// Sync segments (DESIGN §5.1, §5.2): lease → read the local file → compare with the server's copy → one commit.
// No local state: progress lives on the server, a lost receipt is retried with the same commit id.
//   syncFile     live sync (hooks): the whole file against this collector's copy, bounded per segment.
//   scanSegment  one reconciliation segment: a bounded byte range of a file, from where the round's accepted
//                progress for it stands; it neither chases growth past the round's bound nor drops the tail.
//   scanOnce     one scan call: open or continue this collector's round, run segments until the deadline.
//
// Outbound authorization (§7) comes first: a source outside the allowed scopes makes no request at all.
import { realpathSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { blockHashes, CHUNK, piHeaderId, readHead, readRange, readSession, sha256, sourceIdOf, type Harness, type ReadResult } from "./records.ts";
import type { Policy } from "./policy.ts";
import { allowed, scopeOf } from "./policy.ts";

export type Transport = (method: string, path: string, body?: unknown) => Promise<any>;
export type SyncOutcome =
  | { status: "synced"; sent: number; length: number; refused: number; more: boolean } // more: segment limit hit, call again
  | { status: "unchanged"; length: number }
  | { status: "skipped"; reason: string }
  | { status: "failed"; reason: string };
export type ScanItem = { key: string; bound: number; doneRecs: number; doneBytes: number; tries: number };
export type SegmentOutcome =
  | { status: "advanced"; sent: number; final: boolean; item: ScanItem }
  | { status: "stale" } // another segment moved this item first: take a fresh descriptor
  | { status: "restart" } // the file changed before the checked point: the item starts again from 0
  | { status: "skipped"; reason: string }
  | { status: "failed"; reason: string };

const MAX_CHANGES = 2_000;
const MAX_BYTES = 6 * 1024 * 1024;

/** Identity of a file on this collector for scan rounds: its real path, hashed (the path itself stays here). */
export const fileKey = (path: string) => {
  let p = resolve(path);
  try {
    p = realpathSync(p);
  } catch {}
  return sha256(p);
};

type Head = { sourceId: string; nativeId: string; parentPath?: string; parentNative?: string };
/** The lease request body: the source as the file declares it. Pi names a parent file: only one in the same
 *  scope is opened. Codex names a thread id. The server links a parent only if it is registered in the same
 *  scope; otherwise the copied prefix stays unresolved rather than tied to a session this scope may not read. */
function declOf(policy: Policy, path: string, harness: Harness, scope: string, h: Head, meta?: object) {
  let parentNative = h.parentNative;
  if (h.parentPath) parentNative = scopeOf(policy, h.parentPath) === scope ? piHeaderId(h.parentPath) : undefined;
  const parent = parentNative ? sourceIdOf(harness, parentNative) : null;
  return { source: h.sourceId, harness, nativeId: h.nativeId, scope, parent, meta: meta ?? {} };
}

/** Queue for the lease and wait for it until `deadline`. Returns the lease id and whether it was granted. */
async function acquire(t: Transport, decl: object, collector: string, deadline: number) {
  const lease: number = (await t("POST", "/v1/sync/lease", { ...decl, collector })).lease;
  try {
    for (let i = 0; ; i++) {
      if ((await t("POST", `/v1/sync/lease/${lease}/poll`)).granted) return { lease, granted: true };
      if (Date.now() + 50 >= deadline) return { lease, granted: false };
      await Bun.sleep(Math.min(50 * (i + 1), 500, Math.max(0, deadline - Date.now())));
    }
  } catch (e) {
    await t("POST", `/v1/sync/lease/${lease}/release`).catch(() => {});
    throw e;
  }
}

/** POST a commit; a lost receipt is retried with the same commit id (the server returns the stored result). */
async function sendCommit(t: Transport, lease: number, body: object) {
  for (let i = 0; ; i++) {
    try {
      return await t("POST", `/v1/sync/lease/${lease}/commit`, body);
    } catch (e) {
      if (i === 2) throw e;
    }
  }
}

/** Server rows of the given positions' blocks, by position. */
async function remoteRows(t: Transport, lease: number, blocks: number[]) {
  const remote = new Map<number, { id: string; hash: string }>();
  for (let i = 0; i < blocks.length; i += 64) {
    const got = await t("POST", `/v1/sync/lease/${lease}/blocks`, { blocks: blocks.slice(i, i + 64) });
    for (const [b, rows] of Object.entries(got as Record<string, { id: string; hash: string }[]>))
      rows.forEach((r, j) => remote.set(Number(b) * CHUNK + j, r));
  }
  return remote;
}

export async function syncFile(t: Transport, policy: Policy, path: string, harness: Harness,
                               opts: { leaf?: string | null; collector: string; meta?: object; deadlineMs?: number }): Promise<SyncOutcome> {
  const scope = scopeOf(policy, path);
  if (!allowed(policy, scope, "sync", "memex")) return { status: "skipped", reason: "not-authorized" };
  const deadline = Date.now() + (opts.deadlineMs ?? 60_000);
  const pre = readSession(path, harness);
  if (!pre.ok) return { status: "skipped", reason: pre.reason };
  let lease: number, granted: boolean;
  try {
    ({ lease, granted } = await acquire(t, declOf(policy, path, harness, scope, pre, opts.meta), opts.collector, deadline));
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) }; // e.g. "410 source forgotten"
  }
  // Every way out before a commit lets the lease go (an accepted commit ends it on the server).
  let committed = false;
  try {
    if (!granted) return { status: "failed", reason: "lease-timeout" };
    // Read only after the lease is held (§5.1): the read is the snapshot this commit stands for.
    const cur: ReadResult = readSession(path, harness);
    if (!cur.ok) return { status: "skipped", reason: cur.reason };
    if (cur.sourceId !== pre.sourceId) return { status: "failed", reason: "file-changed-identity" };
    const m = await t("GET", `/v1/sync/lease/${lease}/manifest`);
    const local = cur.records;
    const mine = blockHashes(local);
    // Blocks to compare record by record: every block whose hash differs (slow path covers prefix rewrites).
    const differ: number[] = [];
    for (let b = 0; b < mine.length; b++) if (mine[b] !== m.blocks[b]) differ.push(b);
    const remote = await remoteRows(t, lease, differ);
    const changes: { pos: number; id: string; hash: string; raw: string }[] = [];
    let bytes = 0;
    let hitLimit = false;
    scan: for (const b of differ)
      for (let pos = b * CHUNK; pos < Math.min(local.length, (b + 1) * CHUNK); pos++) {
        const r = remote.get(pos);
        if (r && r.id === local[pos].id && r.hash === local[pos].hash) continue;
        if (changes.length >= MAX_CHANGES || bytes + local[pos].raw.length > MAX_BYTES) {
          hitLimit = true;
          break scan;
        }
        changes.push({ pos, ...local[pos] });
        bytes += local[pos].raw.length;
      }
    // A bounded segment: if not every change fits, commit the copy as the prefix up to the last change sent
    // (the server drops its rows past that); the next segment continues from there.
    if (hitLimit && !changes.length) return { status: "failed", reason: "record-too-large" };
    const length = hitLimit ? changes[changes.length - 1].pos + 1 : local.length;
    // Unchanged: the same records, and no leaf to report or the one the server already has for this collector.
    if (!changes.length && m.length === local.length && (opts.leaf === undefined || opts.leaf === m.leaf))
      return { status: "unchanged", length: local.length };
    const r = await sendCommit(t, lease, { commitId: crypto.randomUUID(), length, changes, leaf: opts.leaf });
    committed = true;
    if (!r.ok) return { status: "failed", reason: r.reason };
    return { status: "synced", sent: changes.length, length: r.length, refused: r.refused, more: hitLimit };
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) };
  } finally {
    if (!committed) await t("POST", `/v1/sync/lease/${lease}/release`).catch(() => {});
  }
}

/** One reconciliation segment of `item` in `round`: records from item.doneRecs (byte item.doneBytes), at most
 *  `maxRecs` of them, never past item.bound. Every record in the range is compared with this collector's copy;
 *  an unchanged range is confirmed with an empty commit, so progress is what the server accepted. */
export async function scanSegment(t: Transport, policy: Policy, path: string, harness: Harness,
                                  opts: { collector: string; round: number; item: ScanItem; maxRecs?: number; deadline?: number; meta?: object }): Promise<SegmentOutcome> {
  const scope = scopeOf(policy, path);
  if (!allowed(policy, scope, "sync", "memex")) return { status: "skipped", reason: "not-authorized" };
  const it = opts.item;
  const maxRecs = Math.min(opts.maxRecs ?? CHUNK, MAX_CHANGES);
  const pre = readHead(path, harness);
  if (!pre.ok) return { status: "skipped", reason: pre.reason };
  let lease: number, granted: boolean;
  try {
    ({ lease, granted } = await acquire(t, declOf(policy, path, harness, scope, pre, opts.meta), opts.collector,
                                        opts.deadline ?? Date.now() + 30_000));
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) };
  }
  let committed = false;
  try {
    if (!granted) return { status: "failed", reason: "lease-timeout" };
    const head = readHead(path, harness);
    if (!head.ok) return { status: "skipped", reason: head.reason };
    if (head.sourceId !== pre.sourceId) return { status: "failed", reason: "file-changed-identity" };
    const got = readRange(path, harness, { from: it.doneBytes, bound: it.bound, first: it.doneRecs, maxBytes: MAX_BYTES });
    if (!got.ok) return got.reason === "missing" ? { status: "skipped", reason: "missing" } : { status: "failed", reason: got.reason };
    const n = Math.min(got.records.length, maxRecs);
    const recs = got.records.slice(0, n);
    // Every complete line read counts when every record in it is sent (blank lines included); else up to the last.
    const toByte = n < got.records.length ? got.ends[n - 1] : got.to;
    const from = it.doneRecs, to = from + n;
    const blocks: number[] = [];
    for (let b = Math.floor(from / CHUNK); n && b <= Math.floor((to - 1) / CHUNK); b++) blocks.push(b);
    const remote = await remoteRows(t, lease, blocks);
    const changes = recs.flatMap((r, j) => {
      const x = remote.get(from + j);
      return x && x.id === r.id && x.hash === r.hash ? [] : [{ pos: from + j, ...r }];
    });
    const scan = { round: opts.round, key: it.key, fromRec: from, toRec: to, fromByte: it.doneBytes, toByte,
                   prev: got.prev === undefined ? "" : got.prev, eof: n === got.records.length && got.eof,
                   last: n === got.records.length && got.last };
    const r = await sendCommit(t, lease, { commitId: crypto.randomUUID(), length: to, changes, scan });
    committed = true;
    if (!r.ok) {
      if (r.reason === "scan-stale") return { status: "stale" };
      if (r.reason === "scan-restart") return { status: "restart" };
      return { status: "failed", reason: r.reason };
    }
    return { status: "advanced", sent: changes.length, final: r.scan.state === "done",
             item: { ...it, doneRecs: to, doneBytes: toByte } };
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) };
  } finally {
    if (!committed) await t("POST", `/v1/sync/lease/${lease}/release`).catch(() => {});
  }
}

export type ScanFile = { path: string; harness: Harness };
/** One scan call (§5.2): open this collector's round of `kind` over the authorized `files` (or continue the
 *  open one), then run segments on at most `limit` pending items until `deadlineMs`. Call again to go on;
 *  a new auto round opens only once the server's interval has passed since the last one finished. */
export async function scanOnce(t: Transport, policy: Policy, files: ScanFile[],
                               opts: { collector: string; kind?: "auto" | "import"; limit?: number; deadlineMs?: number; maxRecs?: number }) {
  const deadline = Date.now() + (opts.deadlineMs ?? 10_000);
  const byKey = new Map<string, ScanFile & { size: number; mtime: number }>();
  for (const f of files) {
    if (!allowed(policy, scopeOf(policy, f.path), "sync", "memex")) continue;
    try {
      const st = statSync(f.path);
      if (st.isFile()) byKey.set(fileKey(f.path), { ...f, size: st.size, mtime: st.mtimeMs });
    } catch {}
  }
  if (!byKey.size) return { round: null, reason: "no-files" } as const;
  const o = await t("POST", "/v1/scan/open", { collector: opts.collector, kind: opts.kind ?? "auto",
                                              files: [...byKey].map(([key, f]) => ({ key, size: f.size, mtime: f.mtime })) });
  if (o.round === null) return { round: null, next_at: o.next_at } as const;
  const round: number = o.round;
  const { items } = await t("POST", "/v1/scan/next", { round, limit: opts.limit ?? 50 });
  for (let it of items as ScanItem[]) {
    if (Date.now() >= deadline) break;
    const f = byKey.get(it.key);
    if (!f) {
      await t("POST", "/v1/scan/skip", { round, key: it.key, reason: "missing" });
      continue;
    }
    for (let restarts = 0; Date.now() < deadline; ) {
      const r = await scanSegment(t, policy, f.path, f.harness, { collector: opts.collector, round, item: it, maxRecs: opts.maxRecs, deadline });
      if (r.status === "advanced") {
        if (r.final) break;
        it = r.item;
      } else if (r.status === "restart" && ++restarts <= 1) it = { ...it, doneRecs: 0, doneBytes: 0 };
      else {
        if (r.status === "skipped") await t("POST", "/v1/scan/skip", { round, key: it.key, reason: r.reason });
        else if (r.status === "failed") await t("POST", "/v1/scan/fail", { round, key: it.key, reason: r.reason });
        break; // stale: another process moved it; the next call takes a fresh descriptor
      }
    }
  }
  const c = await t("POST", "/v1/scan/next", { round, limit: 0 });
  return { round, finished: c.finished as boolean, total: c.total as number, done: c.done as number, skipped: c.skipped as number,
           pending: c.pending as number };
}
