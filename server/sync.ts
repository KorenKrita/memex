// L0 sync (DESIGN §4.1, §5.1, §5.2): FIFO leases per (source, collector), manifest, one-transaction commit.
// Ordering rules are the ones prototype/protocol.ts checks against real Postgres (see prototype/README.md):
//   - a lease is granted to the oldest pending request; validity is judged on the server clock inside the
//     commit transaction, after every lock wait; the successor's grant must update the old lease row, so an
//     accepted commit always lands before the next lease;
//   - a commit id is idempotent: a retry returns the stored result;
//   - each collector's copy is a projection; the shared record is `ok` while all copies agree and `conflict`
//     (isolated) when they differ; any change of an existing shared record bumps the session's version;
//   - forgotten sources and canonical records are refused inside the same transaction (§7).
import type { Db, Tx } from "./db.ts";
import { bump, enqueue } from "./db.ts";
import { blockHashes, CHUNK, sourceIdOf, type Harness } from "../client/records.ts";
import { contentHash, isCopyTime } from "../dryrun/dryrun.ts";

export const BARRIER = 1; // global visibility barrier and merge serializer (advisory lock, one-key form)
const COMMIT_NS = 0x6d656d78; // two-key namespace for commit ids
export const LEASE_TTL_MS = Number(process.env.MEMEX_LEASE_TTL_MS ?? 30_000);
export const MAX_COMMIT_RECORDS = 2_000;
export const MAX_COMMIT_BYTES = 8 * 1024 * 1024;

export type SourceDecl = { source: string; harness: Harness; nativeId: string; scope: string; parent?: string | null; meta?: object };
export type Change = { pos: number; id: string; hash: string; raw: string };
/** A reconciliation segment (§5.2): the commit checks records [fromRec, toRec) of the file, bytes [fromByte,
 *  toByte). `prev`: hash of the file's record at fromRec - 1 (null at 0). `eof`: nothing complete follows. */
export type ScanPart = { round: number; key: string; fromRec: number; toRec: number; fromByte: number; toByte: number; prev: string | null;
                         eof: boolean; last?: boolean };
export type Commit = { leaseId: number; commitId: string; length: number; changes: Change[]; scan?: ScanPart; leaf?: string | null };
export type CommitResult =
  | { ok: true; accepted: number; refused: number; length: number; scan?: { state: string; doneRecs: number } }
  | { ok: false; reason: "lease" | "forgotten" | "not-granted" | "bad-request" | "scan-stale" | "scan-restart"; detail?: string };

export class HttpError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export const granted = async (q: Tx, scope: string, purpose: string, dest: string) =>
  (await q`select 1 from grant_rule where scope = ${scope} and purpose = ${purpose} and dest = ${dest}`).length > 0;

/** Register the source once (identity is derived, not trusted) and queue a lease request. */
export async function requestLease(db: Db, d: SourceDecl, collector: string): Promise<number> {
  if (d.source !== sourceIdOf(d.harness, d.nativeId)) throw new HttpError(400, "source id does not match harness and native id");
  return db.begin(async (tx: Tx) => {
    if ((await tx`select 1 from tomb where key = ${"source:" + d.source}`).length) throw new HttpError(410, "source forgotten");
    // Server-side grant (§7): the client's policy only narrows; with no rule here nothing is stored.
    if (!(await granted(tx, d.scope, "sync", "memex"))) throw new HttpError(403, "scope not granted for sync");
    await tx`insert into source (id, harness, native_id, scope, parent_id, meta)
             values (${d.source}, ${d.harness}, ${d.nativeId}, ${d.scope}, ${d.parent ?? null}, ${d.meta ?? {}})
             on conflict (id) do update set parent_id = coalesce(source.parent_id, excluded.parent_id)`;
    const [s] = await tx`select scope from source where id = ${d.source}`;
    if (s.scope !== d.scope) throw new HttpError(409, "scope differs from the registered scope");
    const [r] = await tx`insert into lease (source, collector, ttl_ms) values (${d.source}, ${collector}, ${LEASE_TTL_MS}) returning id`;
    return Number(r.id);
  });
}

/** Advance the (source, collector) queue; report whether `id` holds a valid lease now. */
export async function tryGrant(db: Db, id: number): Promise<{ granted: boolean; leftMs: number }> {
  return db.begin(async (tx: Tx) => {
    const [me] = await tx`select source, collector from lease where id = ${id}`;
    if (!me) return { granted: false, leftMs: 0 };
    await tx`select pg_advisory_xact_lock(hashtext(${me.source}), hashtext(${me.collector}))`;
    await tx`update lease set done = true where source = ${me.source} and collector = ${me.collector}
             and granted_at is not null and not done and expires_at <= clock_timestamp()`;
    // A request nobody polls for 2 ttl is abandoned (client gone): it must not hold the queue.
    await tx`update lease set done = true where source = ${me.source} and collector = ${me.collector}
             and granted_at is null and not done and created_at < clock_timestamp() - make_interval(secs => 2 * ttl_ms / 1000.0)
             and id <> ${id}`;
    const [busy] = await tx`select count(*)::int n from lease where source = ${me.source} and collector = ${me.collector}
                            and granted_at is not null and not done`;
    if (busy.n === 0)
      await tx`update lease set granted_at = clock_timestamp(), expires_at = clock_timestamp() + make_interval(secs => ttl_ms / 1000.0)
               where id = (select id from lease where source = ${me.source} and collector = ${me.collector}
                           and granted_at is null and not done order by id limit 1)`;
    await tx`update lease set created_at = clock_timestamp() where id = ${id} and granted_at is null and not done`; // still polling
    const [r] = await tx`select granted_at is not null and not done and expires_at > clock_timestamp() as mine,
                                greatest(0, floor(extract(epoch from (expires_at - clock_timestamp())) * 1000))::bigint as left_ms
                         from lease where id = ${id}`;
    return { granted: !!r.mine, leftMs: r.mine ? Number(r.left_ms) : 0 };
  });
}

export async function releaseLease(db: Db, id: number) {
  await db`update lease set done = true where id = ${id}`;
}

async function validLease(db: Db, id: number) {
  const [l] = await db`select source, collector from lease where id = ${id} and granted_at is not null and not done
                       and expires_at > clock_timestamp()`;
  return l as { source: string; collector: string } | undefined;
}

// Reconciliation rounds (§5.2). A round fixes its files and each file's bound when it opens; files that appear
// or grow later wait for the next round. Items are handed out in the round's order (newest mtime first), those
// that failed more often later, so a file that keeps failing does not take every segment. Progress moves only
// in an accepted commit (commit() above); a failure here only counts a try.
export const scanConfig = { intervalMs: Number(process.env.MEMEX_SCAN_INTERVAL_MS ?? 3_600_000), maxFiles: 20_000 };
const KINDS = new Set(["auto", "import"]);
const isKey = (k: unknown) => typeof k === "string" && /^[0-9a-f]{64}$/.test(k);

export async function finishRound(tx: Tx, round: number) {
  await tx`update scan_round set finished_at = now() where id = ${round} and finished_at is null
           and not exists (select 1 from scan_item where round = ${round} and state = 'pending')`;
}

/** The collector's open round of this kind, or a new one over `files` (auto: once the interval has passed). */
export async function openRound(db: Db, b: any) {
  const collector = b.collector, kind = b.kind ?? "auto";
  if (typeof collector !== "string" || !collector || collector.length > 128 || !KINDS.has(kind)) throw new HttpError(400, "collector, kind");
  const files = Array.isArray(b.files) ? b.files : [];
  if (files.length > scanConfig.maxFiles) throw new HttpError(400, `at most ${scanConfig.maxFiles} files`);
  for (const f of files)
    if (!isKey(f?.key) || !Number.isSafeInteger(f.size) || f.size < 0 || typeof f.mtime !== "number") throw new HttpError(400, "files");
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(hashtext('scan'), hashtext(${kind + ":" + collector}))`;
    const [cur] = await tx`select id from scan_round where collector = ${collector} and kind = ${kind} and finished_at is null`;
    if (cur) return { round: Number(cur.id), opened: false };
    const [last] = await tx`select finished_at from scan_round where collector = ${collector} and kind = ${kind} order by id desc limit 1`;
    if (kind === "auto" && last && Date.now() - new Date(last.finished_at).getTime() < scanConfig.intervalMs)
      return { round: null, next_at: new Date(new Date(last.finished_at).getTime() + scanConfig.intervalMs).toISOString() };
    const [r] = await tx`insert into scan_round (collector, kind) values (${collector}, ${kind}) returning id`;
    const seen = new Set<string>();
    const rows = [...files].sort((x, y) => y.mtime - x.mtime).filter((f) => !seen.has(f.key) && seen.add(f.key))
      .map((f, ord) => ({ round: r.id, key: f.key, ord, bound: f.size }));
    for (let i = 0; i < rows.length; i += 1000) await tx`insert into scan_item ${tx(rows.slice(i, i + 1000))}`;
    await finishRound(tx, Number(r.id));
    return { round: Number(r.id), opened: true };
  });
}

const counts = async (q: Db | Tx, round: number) => {
  const [c] = await q`select count(*)::int total, (count(*) filter (where state = 'done'))::int done,
                      (count(*) filter (where state = 'skipped'))::int skipped, (count(*) filter (where state = 'pending'))::int pending,
                      (select finished_at from scan_round where id = ${round}) finished_at, (select opened_at from scan_round where id = ${round}) opened_at
                      from scan_item where round = ${round}`;
  return { round, total: c.total, done: c.done, skipped: c.skipped, pending: c.pending, finished: !!c.finished_at,
           opened_at: c.opened_at, finished_at: c.finished_at };
};

/** Pending items of a round, in order, with where their accepted progress stands; and the round's counts. */
export async function nextItems(db: Db, b: any) {
  const round = Number(b.round), limit = Math.min(Math.max(Number.isInteger(b.limit) ? b.limit : 50, 0), 200);
  if (!Number.isSafeInteger(round)) throw new HttpError(400, "round");
  // exclude: keys the caller already tried in this run (its next page holds only others).
  const ex = (Array.isArray(b.exclude) ? b.exclude : []).filter((k: unknown) => typeof k === "string").slice(0, scanConfig.maxFiles);
  const items = await db`select key, bound, done_recs, done_bytes, tries from scan_item where round = ${round} and state = 'pending'
                         and not (key = any(${db.array(ex, "text")})) order by tries, ord limit ${limit}`;
  return { items: items.map((i: any) => ({ key: i.key, bound: Number(i.bound), doneRecs: i.done_recs, doneBytes: Number(i.done_bytes), tries: i.tries })),
           ...(await counts(db, round)) };
}

/** skip: the file cannot be reconciled (not a session, Legacy, child thread, gone): the item is closed with
 *  its reason. fail: a try that did not land (network, lease, a file changing under the read): it stays. */
export async function markItem(db: Db, b: any, how: "skip" | "fail") {
  const round = Number(b.round);
  if (!Number.isSafeInteger(round) || !isKey(b.key)) throw new HttpError(400, "round, key");
  const reason = String(b.reason ?? "").slice(0, 200);
  return db.begin(async (tx: Tx) => {
    if (how === "skip") {
      await tx`update scan_item set state = 'skipped', reason = ${reason} where round = ${round} and key = ${b.key} and state = 'pending'`;
      await finishRound(tx, round);
    } else await tx`update scan_item set tries = tries + 1, reason = ${reason} where round = ${round} and key = ${b.key} and state = 'pending'`;
    return { ok: true };
  });
}

/** Per kind: the open round's progress and the last finished round (§5.2 `/memex status`). */
export async function scanStatus(db: Db, collector: string) {
  const out: Record<string, { current: object | null; last: object | null }> = {};
  for (const kind of KINDS) {
    const [cur] = await db`select id from scan_round where collector = ${collector} and kind = ${kind} and finished_at is null`;
    const [last] = await db`select id from scan_round where collector = ${collector} and kind = ${kind} and finished_at is not null
                            order by id desc limit 1`;
    out[kind] = { current: cur ? await counts(db, Number(cur.id)) : null, last: last ? await counts(db, Number(last.id)) : null };
  }
  return out;
}

/** Manifest of one collector's copy (§5.1): block hashes in file order, plus the last block in full. */
export async function manifest(db: Db, leaseId: number) {
  const l = await validLease(db, leaseId);
  if (!l) throw new HttpError(409, "lease not held");
  const rows = await db`select pos, rec as id, hash from projection where source = ${l.source} and collector = ${l.collector} order by pos`;
  // Blocks by position: block b covers positions [b*CHUNK, (b+1)*CHUNK); a position the copy does not hold (a
  // refused record) is a hole, so the block cannot equal a local block whose record there exists. `length` is
  // one past the last held position.
  const length = rows.length ? rows[rows.length - 1].pos + 1 : 0;
  const byBlock: { id: string; hash: string }[][] = [];
  for (const r of rows) (byBlock[Math.floor(r.pos / CHUNK)] ??= []).push(r);
  const blocks: string[] = [];
  for (let b = 0; b * CHUNK < length; b++) {
    const rs = byBlock[b] ?? [];
    const full = rs.length === Math.min(CHUNK, length - b * CHUNK);
    blocks.push(full ? blockHashes(rs)[0] : "hole");
  }
  const lastStart = Math.max(0, (blocks.length - 1) * CHUNK);
  // The leaf this collector last reported (null: none, or reported as none): a leaf-only change still commits.
  const [v] = await db`select leaf from collector_view where source = ${l.source} and collector = ${l.collector}`;
  return { length, chunk: CHUNK, blocks, last: { start: lastStart, records: rows.filter((r: any) => r.pos >= lastStart) }, leaf: v ? v.leaf : undefined };
}

/** Records of the given blocks, for the slow path (a block hash differs). */
export async function blockRecords(db: Db, leaseId: number, blocks: number[]) {
  const l = await validLease(db, leaseId);
  if (!l) throw new HttpError(409, "lease not held");
  // Each row carries its position: a copy may have holes (a refused record, a segment that moved a record).
  const out: Record<number, { pos: number; id: string; hash: string }[]> = {};
  for (const b of blocks.slice(0, 64))
    out[b] = await db`select pos, rec as id, hash from projection where source = ${l.source} and collector = ${l.collector}
                      and pos >= ${b * CHUNK} and pos < ${(b + 1) * CHUNK} order by pos`;
  return out;
}

/** Re-arm transaction_timeout to the lease's remaining lifetime (or keep the inherited bound), one statement. */
async function rearmTimeout(tx: Tx, leaseId: number | null) {
  await tx`with cap as materialized (select current_setting('transaction_timeout')::interval as inh),
                clr as materialized (select set_config('transaction_timeout', '0', true) as z from cap)
           select set_config('transaction_timeout', ms::text, true)
           from (select least((select floor(extract(epoch from (expires_at - clock_timestamp())) * 1000)::bigint
                               from lease where id = ${leaseId}::bigint),
                              case when cap.inh = interval '0' then null
                                   else greatest(1, floor(extract(epoch from (cap.inh - (clock_timestamp() - now()))) * 1000)::bigint) end
                             ) as ms
                 from clr, cap) s
           where ms is not null`;
}

/** Every declared ancestor of `source` (living or forgotten: a forgotten source keeps its row), cycle-safe. */
export async function ancestorsOf(tx: Tx, source: string): Promise<string[]> {
  return (await tx`with recursive a(id, path) as (
                     select parent_id, array[id] from source where id = ${source} and parent_id is not null
                     union all
                     select s.parent_id, a.path || s.id from a join source s on s.id = a.id
                     where s.parent_id is not null and not s.id = any(a.path))
                   select distinct id from a`).map((x: any) => x.id as string);
}
/** A content tomb's fingerprint of a record version: Pi by content without the parent link (projection's copy
 *  rule, looseOf: Pi re-chains a copied entry past a dropped label), Codex by the whole line. A line that does
 *  not parse (an older database's) by its bytes, so a byte-identical copy still matches and nothing throws. */
const tombHash = (harness: string, raw: string) => {
  let e: any;
  try {
    e = JSON.parse(raw);
  } catch {
    return "raw-" + new Bun.CryptoHasher("sha256").update(raw).digest("hex");
  }
  if (harness === "pi" && e && typeof e === "object" && !Array.isArray(e)) {
    const { parentId: _, ...rest } = e;
    return contentHash(rest);
  }
  return contentHash(e);
};
/** A forgotten source's content tomb for one of its own record versions (forget.ts). */
export const copyTomb = (harness: string, owner: string, rec: string, raw: string) => `copy:${owner}:${rec}:${tombHash(harness, raw)}`;
/** Content tombs a record may be a copy under: its lineage's (`anc`, from ancestorsOf), plus the ownerless
 *  `copy:<rec>:<hash>` a database from before tombs named their source may hold (its owner unknown: still
 *  refusing, never guessed into a lineage). Both fingerprints: tombs written before Pi's left out the parent
 *  link hashed the whole line. */
export const copyTombsOf = (harness: string, anc: string[], rec: string, raw: string) => {
  const hs = new Set([tombHash(harness, raw)]);
  try {
    hs.add(contentHash(JSON.parse(raw)));
  } catch {}
  return [...hs].flatMap((h) => [`copy:${rec}:${h}`, ...anc.map((g) => `copy:${g}:${rec}:${h}`)]);
};
export async function commit(db: Db, c: Commit): Promise<CommitResult> {
  if (!Number.isInteger(c.length) || c.length < 0 || c.changes.length > MAX_COMMIT_RECORDS)
    return { ok: false, reason: "bad-request", detail: `at most ${MAX_COMMIT_RECORDS} changes` };
  const s = c.scan;
  if (s) {
    const ints = [s.round, s.fromRec, s.toRec, s.fromByte, s.toByte];
    if (!ints.every((x) => Number.isSafeInteger(x) && x >= 0) || typeof s.key !== "string" || !s.key || s.key.length > 64
        || s.toRec < s.fromRec || s.toRec - s.fromRec > MAX_COMMIT_RECORDS || s.toByte < s.fromByte || c.length !== s.toRec
        || (s.prev !== null && typeof s.prev !== "string") || typeof s.eof !== "boolean" || c.changes.some((ch) => ch.pos < s.fromRec))
      return { ok: false, reason: "bad-request", detail: "scan range" };
  }
  let bytes = 0;
  for (const ch of c.changes) {
    bytes += ch.raw.length;
    if (!(ch.pos >= 0 && ch.pos < c.length) || typeof ch.id !== "string" || !ch.id) return { ok: false, reason: "bad-request", detail: "change outside the copy" };
    if (new Bun.CryptoHasher("sha256").update(ch.raw).digest("hex") !== ch.hash) return { ok: false, reason: "bad-request", detail: `hash of ${ch.id}` };
    // Every record is one JSON object (the collector refuses other lines): what the server parses later is.
    let obj: unknown;
    try {
      obj = JSON.parse(ch.raw);
    } catch {}
    if (typeof obj !== "object" || obj === null || Array.isArray(obj)) return { ok: false, reason: "bad-request", detail: `${ch.id} is not a JSON object` };
  }
  if (bytes > MAX_COMMIT_BYTES) return { ok: false, reason: "bad-request", detail: "commit too large" };
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${COMMIT_NS}, hashtext(${c.commitId}))`;
    const [prev] = await tx`select result from commit_result where commit_id = ${c.commitId}`;
    if (prev) return prev.result as CommitResult;
    const [l] = await tx`select source, collector, done from lease where id = ${c.leaseId} for update`;
    if (!l || l.done) return { ok: false, reason: "lease" } as const;
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    const [v] = await tx`select floor(extract(epoch from (expires_at - clock_timestamp())) * 1000)::bigint as left_ms
                         from lease where id = ${c.leaseId} and granted_at is not null`;
    const accepted = !!v && Number(v.left_ms) > 0;
    await rearmTimeout(tx, accepted ? c.leaseId : null);
    if (!accepted) return { ok: false, reason: "lease" } as const;
    if ((await tx`select 1 from tomb where key = ${"source:" + l.source}`).length) {
      await tx`update lease set done = true where id = ${c.leaseId}`;
      return { ok: false, reason: "forgotten" } as const;
    }
    const [src] = await tx`select scope, harness from source where id = ${l.source}`;
    if (!(await granted(tx, src.scope, "sync", "memex"))) {
      await tx`update lease set done = true where id = ${c.leaseId}`;
      return { ok: false, reason: "not-granted" } as const;
    }
    // A scan segment counts only from where the item's accepted progress stands, and only while the record it
    // continues from is still the one this collector's copy holds there; otherwise nothing is applied.
    let item: any;
    if (s) {
      [item] = await tx`select i.state, i.done_recs, i.done_bytes, i.bound, i.source from scan_item i join scan_round r on r.id = i.round
                        where i.round = ${s.round} and i.key = ${s.key} and r.collector = ${l.collector} for update of i`;
      const stale = !item || item.state !== "pending" || item.done_recs !== s.fromRec || Number(item.done_bytes) !== s.fromByte;
      // The range stays within the round's bound: growth past it is the next round's (live sync has the tail).
      if (!stale && s.toByte > Number(item.bound)) return { ok: false, reason: "bad-request", detail: "scan range past the round's bound" } as const;
      let restart = false;
      if (!stale && item.source && item.source !== l.source) restart = true;
      else if (!stale && s.fromRec > 0) {
        const [a] = await tx`select hash from projection where source = ${l.source} and collector = ${l.collector} and pos = ${s.fromRec - 1}`;
        restart = !a || a.hash !== s.prev;
      }
      if (stale || restart) {
        if (restart) await tx`update scan_item set done_recs = 0, done_bytes = 0, source = null where round = ${s.round} and key = ${s.key}`;
        const result = { ok: false, reason: stale ? "scan-stale" : "scan-restart" } as const;
        await tx`insert into commit_result (commit_id, result) values (${c.commitId}, ${result})`;
        await tx`update lease set done = true where id = ${c.leaseId}`;
        return result;
      }
    }
    // Refuse forgotten records: this source's own (`canon:source:rec`), and copies of a forgotten fork
    // ancestor's own records, by content (`copy:ancestor:rec:content hash`, forget.ts), so an import after the
    // forget cannot bring them back as this session's own. Only this lineage's tombs count: a session outside it
    // that held the same bytes is not where this record came from. A record that only reuses an id is not a copy.
    const anc = await ancestorsOf(tx, l.source);
    const copyKeys = (ch: Change) => {
      try {
        return copyTombsOf(src.harness, anc, ch.id, ch.raw);
      } catch {
        return [];
      }
    };
    const cand = c.changes.flatMap((ch) => [`canon:${l.source}:${ch.id}`, ...(ch.id === "header" ? [] : copyKeys(ch))]);
    const tombs = new Set<string>((await tx`select key from tomb where key = any(${tx.array(cand, "text")})`).map((r: any) => r.key));
    // Pi: only a possible copy (not newer than this file's fork time, DESIGN §4.1) is refused as one; an own
    // entry that happens to carry a forgotten record's id and content is accepted.
    const headRaw = c.changes.find((ch) => ch.id === "header")?.raw
      ?? (await tx`select b.raw from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                   where r.source = ${l.source} and r.rec = 'header'`)[0]?.raw;
    let head: any;
    try {
      head = headRaw ? JSON.parse(headRaw) : undefined;
    } catch {}
    const maybeCopy = (ch: Change) => {
      if (head?.type !== "session") return true;
      try {
        return isCopyTime(JSON.parse(ch.raw), head);
      } catch {
        return true;
      }
    };
    const keep = c.changes.filter((ch) => !tombs.has(`canon:${l.source}:${ch.id}`) && (ch.id === "header" || !copyKeys(ch).some((k) => tombs.has(k)) || !maybeCopy(ch)));
    // Projection: the copy now has `length` records; rows past it are gone; changed positions are replaced.
    // A scan segment covers only its range: rows past it are the live tail's, unless the file ends there.
    const dropped = s && !s.eof ? [] : await tx`delete from projection where source = ${l.source} and collector = ${l.collector}
                                                 and pos >= ${c.length} returning rec`;
    const touched = new Set<string>(dropped.map((r: any) => r.rec));
    if (keep.length) {
      const ids = keep.map((ch) => ch.id);
      const poss = keep.map((ch) => ch.pos);
      const moved = await tx`delete from projection where source = ${l.source} and collector = ${l.collector}
                             and (pos = any(${tx.array(poss, "int4")}) or rec = any(${tx.array(ids, "text")})) returning rec`;
      for (const r of moved) touched.add(r.rec);
      for (const id of ids) touched.add(id);
      await tx`insert into body ${tx(keep.map((ch) => ({ source: l.source, rec: ch.id, hash: ch.hash, raw: ch.raw })))} on conflict do nothing`;
      await tx`insert into projection ${tx(keep.map((ch) => ({ source: l.source, collector: l.collector, pos: ch.pos, rec: ch.id, hash: ch.hash })))}`;
    }
    if (touched.size) {
      // Merge each touched record across collectors; a record no copy holds any more leaves the shared L0.
      const ids = [...touched];
      const merged = await tx`select r0.rec, count(distinct p.hash)::int n, min(p.hash) h, min(p.pos) pos, r.state, r.hash old_hash
                              from unnest(${tx.array(ids, "text")}::text[]) r0(rec)
                              left join projection p on p.source = ${l.source} and p.rec = r0.rec
                              left join record r on r.source = ${l.source} and r.rec = r0.rec
                              group by r0.rec, r.state, r.hash`;
      // Two kinds of change (§5.5 write-side rule): a new record only makes the projection out of date (it
      // re-projects; nothing already published from other records becomes wrong), while a revised, removed or
      // conflicting record withdraws inputs and moves the session's version, so work built on the old
      // record is refused at its barrier check.
      let added = false, withdrawn = false;
      for (const m of merged) {
        if (m.n === 0) {
          if (m.state) withdrawn = true;
          await tx`delete from record where source = ${l.source} and rec = ${m.rec}`;
          continue;
        }
        const state = m.n === 1 ? "ok" : "conflict";
        const hash = m.n === 1 ? m.h : null;
        if (!m.state) added = state === "ok" ? true : ((withdrawn = true), added);
        else if (m.state !== state || m.old_hash !== hash) withdrawn = true;
        await tx`insert into record (source, rec, state, hash, first_pos) values (${l.source}, ${m.rec}, ${state}, ${hash}, ${m.pos})
                 on conflict (source, rec) do update set state = excluded.state, hash = excluded.hash, first_pos = excluded.first_pos`;
      }
      if (withdrawn) await bump(tx, "s:" + l.source);
      if (added || withdrawn) await enqueue(tx, "project", l.source);
    }
    let scan: { state: string; doneRecs: number } | undefined;
    if (s) {
      // Done when the checked range reaches the round's bound: the bound itself, nothing complete before it
      // (`last`: it falls inside a line still being written), or the end of the file. `last` and `eof` are
      // the collector's reading of its own file (the server never sees the file); what the server holds it
      // to is the range: from the accepted point, within the bound, continuous with the copy (prev).
      const state = s.toByte >= Number(item.bound) || s.last === true || s.eof ? "done" : "pending";
      await tx`update scan_item set done_recs = ${s.toRec}, done_bytes = ${s.toByte}, source = ${l.source}, state = ${state}, reason = null
               where round = ${s.round} and key = ${s.key}`;
      if (state === "done") await finishRound(tx, s.round);
      scan = { state, doneRecs: s.toRec };
    }
    if (c.leaf !== undefined)
      await tx`insert into collector_view values (${l.source}, ${l.collector}, ${c.leaf}, now())
               on conflict (source, collector) do update set leaf = excluded.leaf, at = excluded.at`;
    const result = { ok: true, accepted: keep.length, refused: c.changes.length - keep.length, length: c.length, ...(scan ? { scan } : {}) } as const;
    await tx`insert into commit_result (commit_id, result) values (${c.commitId}, ${result})`;
    await tx`update lease set done = true where id = ${c.leaseId}`;
    return result;
  });
}
