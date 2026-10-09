// memex protocol prototype: the database-side ordering rules of DESIGN.md §4.1, §5.1, §5.2, §5.5.
// Scope: publish barrier, per-collector projections + merge, FIFO sync leases, commit idempotency, artifact
// invalidation. NOT: HTTP/socket delivery, plugins, real sessions, performance.
import type { SQL } from "bun";

type Db = SQL;                                  // pool or reserved connection; both are tagged-template callables
type Tx = any;                                  // transaction handle passed to sql.begin callbacks

const BARRIER = 1;                              // single global visibility barrier (advisory lock key)

// ------------------------------------------------------------------------------------------------ schema

export async function installSchema(db: Db) {
  await db.unsafe(`
    create table if not exists vis_version (key text primary key, v bigint not null default 1);
    create table if not exists occurrence (occ text primary key, view text not null, canon text not null);
    create table if not exists tomb (key text primary key);              -- 'rec:P:r1' | 'view:F' | 'query:Q'
    create table if not exists dest (name text primary key, allowed bool not null);
    create table if not exists artifact (id text primary key, query text not null, dest text not null,
                                         gen_vec jsonb not null);         -- versions it was generated under
    create table if not exists artifact_input (artifact text not null, occ text not null);
    create table if not exists publication (artifact text not null, at timestamptz not null default clock_timestamp());

    create table if not exists body (rec text not null, hash text not null, primary key (rec, hash));
    create table if not exists projection (rec text not null, collector text not null, hash text not null,
                                           primary key (rec, collector));
    create table if not exists shared (rec text primary key, state text not null, hash text);
    create table if not exists lease (id bigserial primary key, source text not null, collector text not null,
                                      ttl_ms int not null, granted_at timestamptz, expires_at timestamptz,
                                      done bool not null default false);
    create table if not exists commit_result (commit_id text primary key, result jsonb not null);
    create table if not exists scan_progress (source text not null, collector text not null, chunks int not null,
                                              primary key (source, collector));
  `);
}

export async function resetData(db: Db) {
  await db.unsafe(`truncate vis_version, occurrence, tomb, dest, artifact, artifact_input, publication,
                   body, projection, shared, lease, commit_result, scan_progress restart identity`);
}

const sessOf = (canon: string) => canon.split(":")[0];

export async function seed(db: Db, s: { occurrences: { occ: string; view: string; canon: string }[]; dests: string[] }) {
  for (const o of s.occurrences)
    await db`insert into occurrence values (${o.occ}, ${o.view}, ${o.canon}) on conflict (occ) do nothing`;
  for (const d of s.dests) await db`insert into dest values (${d}, true) on conflict (name) do nothing`;
}

// ------------------------------------------------------------------------------------------------ dependency vector

type ArtSpec = { id: string; occs: string[]; query: string; dest: string };

/** Keys of the full dependency vector: canonical source sessions, view sessions, query session, destination. */
async function vectorKeys(q: Tx, artifactId: string, opts: PublishOpts = {}): Promise<string[]> {
  const [a] = await q`select query, dest from artifact where id = ${artifactId}`;
  const rows = await q`select o.view, o.canon from artifact_input i join occurrence o on o.occ = i.occ
                       where i.artifact = ${artifactId}`;
  const keys = new Set<string>();
  for (const r of rows) { keys.add("s:" + sessOf(r.canon)); if (!opts.omitViewSessions) keys.add("s:" + r.view); }
  keys.add("s:" + a.query);
  keys.add("d:" + a.dest);
  return [...keys].sort();
}
async function readVector(q: Tx, keys: string[]): Promise<Record<string, number>> {
  const rows = await q`select key, v from vis_version where key in ${q(keys)}`;
  const out: Record<string, number> = Object.fromEntries(keys.map(k => [k, 1]));   // absent key = version 1
  for (const r of rows) out[r.key] = Number(r.v);
  return out;
}
/** Exact eligibility: inputs not tombstoned, canonical mapping unchanged, no conflict, query alive, dest allowed. */
async function exactOk(q: Tx, artifactId: string): Promise<boolean> {
  const [a] = await q`select query, dest from artifact where id = ${artifactId}`;
  if (!a) return false;
  const [bad] = await q`
    select count(*)::int n from artifact_input i join occurrence o on o.occ = i.occ
    left join shared s on s.rec = o.canon
    where i.artifact = ${artifactId}
      and (exists (select 1 from tomb t where t.key = 'rec:' || o.canon or t.key = 'view:' || o.view)
           or coalesce(s.state, 'ok') <> 'ok')`;
  const [inputs] = await q`select count(*)::int n from artifact_input where artifact = ${artifactId}`;
  const [mapped] = await q`select count(*)::int n from artifact_input i join occurrence o on o.occ = i.occ
                           where i.artifact = ${artifactId}`;
  const [qt] = await q`select count(*)::int n from tomb where key = ${"query:" + a.query}`;
  const [d] = await q`select allowed from dest where name = ${a.dest}`;
  return bad.n === 0 && inputs.n === mapped.n && qt.n === 0 && !!d?.allowed;
}

export async function createArtifact(db: Db, a: ArtSpec) {
  await db.begin("isolation level repeatable read", async (tx: Tx) => {
    await tx`insert into artifact values (${a.id}, ${a.query}, ${a.dest}, '{}'::jsonb)`;
    for (const o of a.occs) await tx`insert into artifact_input values (${a.id}, ${o})`;
    const vec = await readVector(tx, await vectorKeys(tx, a.id));
    await tx`update artifact set gen_vec = ${vec} where id = ${a.id}`;
  });
}

/** Generation-vector validity: every key the artifact was generated under is still at that version. */
async function genVecOk(q: Tx, id: string): Promise<boolean> {
  const [a] = await q`select gen_vec from artifact where id = ${id}`;
  if (!a) return false;
  const gen: Record<string, number> = a.gen_vec;
  const now = await readVector(q, Object.keys(gen));
  return Object.entries(gen).every(([k, v]) => now[k] === v);
}

/** Valid = generation vector unchanged AND exact check passes (an advanced key kills it forever). */
export async function isArtifactValid(db: Db, id: string): Promise<boolean> {
  return db.begin("isolation level repeatable read", async (tx: Tx) => (await genVecOk(tx, id)) && (await exactOk(tx, id)));
}

export async function publicationCount(db: Db, id: string) {
  const [r] = await db`select count(*)::int n from publication where artifact = ${id}`;
  return r.n as number;
}

// ------------------------------------------------------------------------------------------------ writers
// Each invalidating change: exclusive barrier lock + state change + advance >=1 key an old vector observes.

/** Optional pause inside a writer's transaction: holding the exclusive lock, change made, not yet committed. */
export type WriterHooks = { beforeCommit?: () => Promise<void> };
async function invalidating(db: Db, fn: (tx: Tx) => Promise<void>, h: WriterHooks = {}) {
  await db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await fn(tx);
    await h.beforeCommit?.();
  });
}
const bump = (tx: Tx, key: string) =>
  tx`insert into vis_version values (${key}, 2) on conflict (key) do update set v = vis_version.v + 1`;

export const forgetRecord = (db: Db, canon: string, h?: WriterHooks) =>
  invalidating(db, async tx => { await tx`insert into tomb values (${"rec:" + canon}) on conflict do nothing`; await bump(tx, "s:" + sessOf(canon)); }, h);
export const hideView = (db: Db, view: string, h?: WriterHooks) =>
  invalidating(db, async tx => { await tx`insert into tomb values (${"view:" + view}) on conflict do nothing`; await bump(tx, "s:" + view); }, h);
export const forgetQuerySession = (db: Db, s: string, h?: WriterHooks) =>
  invalidating(db, async tx => { await tx`insert into tomb values (${"query:" + s}) on conflict do nothing`; await bump(tx, "s:" + s); }, h);
export const revokeDest = (db: Db, d: string, h?: WriterHooks) =>
  invalidating(db, async tx => { await tx`update dest set allowed = false where name = ${d}`; await bump(tx, "d:" + d); }, h);
/** Remap advances the OCCURRENCE's view session (an old vector observes it), not only the new canonical source. */
export const remap = (db: Db, occ: string, canon: string, h?: WriterHooks) =>
  invalidating(db, async tx => {
    const [o] = await tx`update occurrence set canon = ${canon} where occ = ${occ} returning view`;
    await bump(tx, "s:" + o.view);
  }, h);

// ------------------------------------------------------------------------------------------------ publish

export type PublishHooks = {
  duringCheck?: () => Promise<void>;            // inside the step-1 snapshot, after the exact check, before sampling
  afterCheck?: () => Promise<void>;             // step 1 done (check + vector sampled), its txn committed
  beforeLock?: () => Promise<void>;             // about to open step 2
  inLock?: (tx: Tx) => Promise<void>;           // holding the shared lock, before compare
};
export type PublishOpts = { reuseSnapshot?: boolean; omitViewSessions?: boolean; sampleLate?: boolean; lockAndReadOneStatement?: boolean; lockDeadlineMs?: number };

/**
 * Two-step publish (§5.5). Reserves its own connection from `pool` (the deadline may terminate it).
 * Step 1: one REPEATABLE READ snapshot checks generation-vector validity and the exact predicate AND samples
 *         the full dependency vector.
 * Step 2: a NEW READ COMMITTED transaction takes the shared barrier lock in its own statement; the version
 *         read is the NEXT statement, so its snapshot is taken after the lock was granted. `transaction_timeout`
 *         bounds the WHOLE locked transaction server-side (including client pauses between statements); on
 *         expiry Postgres terminates the backend, which releases the lock and discards the publication.
 */
export async function publish(pool: Db, id: string, hooks: PublishHooks = {}, opts: PublishOpts = {}):
  Promise<"published" | "rejected" | "timeout"> {
  const c = await pool.reserve();
  try { return await publishOn(c, id, hooks, opts); } finally { c.release(); }
}
async function publishOn(c: Db, id: string, hooks: PublishHooks, opts: PublishOpts): Promise<"published" | "rejected" | "timeout"> {
  if (opts.reuseSnapshot) return publishBrokenReuseSnapshot(c, id, hooks, opts);
  let keys: string[] = [], vec: Record<string, number> = {}, ok = false;
  await c`begin isolation level repeatable read`;
  try {
    ok = (await genVecOk(c, id)) && (await exactOk(c, id));
    await hooks.duringCheck?.();                                     // another txn commits; our snapshot is fixed
    keys = await vectorKeys(c, id, opts);
    if (!opts.sampleLate) vec = await readVector(c, keys);
    await c`commit`;
    if (opts.sampleLate) vec = await readVector(c, keys);           // broken: versions from a later, separate snapshot
  } catch (e) { await c`rollback`.catch(() => {}); throw e; }
  await hooks.afterCheck?.();
  if (!ok) return "rejected";
  await hooks.beforeLock?.();

  await c`begin isolation level read committed`;
  try {
    await c.unsafe(`set local transaction_timeout = ${Math.max(1, Math.floor(opts.lockDeadlineMs ?? LOCK_DEADLINE_MS))}`);
    let now: Record<string, number>;
    if (opts.lockAndReadOneStatement) {                              // broken: snapshot taken before the lock wait
      now = await readVectorWithLock(c, keys);
      await hooks.inLock?.(c);
    } else {
      await c`select pg_advisory_xact_lock_shared(${BARRIER})`;
      await hooks.inLock?.(c);
      now = await readVector(c, keys);                               // fresh read, after the lock
    }
    if (!keys.every(k => now[k] === vec[k])) { await c`rollback`; return "rejected"; }
    await c`insert into publication (artifact) values (${id})`;       // "hand to transport" stand-in
    await c`commit`;
    return "published";
  } catch (e: any) {
    await c`rollback`.catch(() => {});
    if (e?.errno === "25P04" || e?.code === "ERR_POSTGRES_CONNECTION_CLOSED") return "timeout";   // transaction_timeout
    throw e;
  }
}
const LOCK_DEADLINE_MS = 2000;                                       // prototype default; product value is [待标定]

/** Broken variant helper: take the lock and read versions in ONE statement (its snapshot predates the wait). */
async function readVectorWithLock(q: Tx, keys: string[]): Promise<Record<string, number>> {
  const rows = await q`select l.ok, v.key, v.v from (select pg_advisory_xact_lock_shared(${BARRIER}) is null as ok) l
                       left join vis_version v on v.key in ${q(keys)}`;
  const out: Record<string, number> = Object.fromEntries(keys.map(k => [k, 1]));
  for (const r of rows) if (r.key) out[r.key] = Number(r.v);
  return out;
}

/** Deliberately broken: compares in the step-1 snapshot after taking the lock. Used only to prove the test bites. */
async function publishBrokenReuseSnapshot(c: Db, id: string, hooks: PublishHooks, opts: PublishOpts) {
  await c`begin isolation level repeatable read`;
  try {
    const ok = await exactOk(c, id);
    const keys = await vectorKeys(c, id, opts);
    const vec = await readVector(c, keys);
    await hooks.afterCheck?.();
    if (!ok) { await c`rollback`; return "rejected" as const; }
    await hooks.beforeLock?.();
    await c`select pg_advisory_xact_lock_shared(${BARRIER})`;
    const now = await readVector(c, keys);                           // same old snapshot
    if (!keys.every(k => now[k] === vec[k])) { await c`rollback`; return "rejected" as const; }
    await c`insert into publication (artifact) values (${id})`;
    await c`commit`;
    return "published" as const;
  } catch (e) { await c`rollback`.catch(() => {}); throw e; }
}

// ------------------------------------------------------------------------------------------------ sync leases

/** Enqueue a lease request for (source, collector); FIFO by id. */
export async function requestLease(db: Db, source: string, collector: string, ttlMs: number): Promise<number> {
  const [r] = await db`insert into lease (source, collector, ttl_ms) values (${source}, ${collector}, ${ttlMs}) returning id`;
  return Number(r.id);
}

/**
 * Advance the queue of `id`'s (source, collector) and report whether `id` now holds a valid lease.
 * Under the per-queue advisory lock: expired granted leases leave the queue; if no lease is valid, the OLDEST
 * pending request is granted — whoever calls. So a request whose client died is still granted, expires after its
 * ttl and is reaped; it cannot block the queue forever. A granted lease is reported only while unexpired and
 * not done.
 */
export async function tryGrant(db: Db, id: number): Promise<boolean> {
  return db.begin(async (tx: Tx) => {
    const [me] = await tx`select source, collector from lease where id = ${id}`;
    if (!me) return false;
    await tx`select pg_advisory_xact_lock(hashtext(${me.source}), hashtext(${me.collector}))`;   // two-key form: never collides with BARRIER (one-key)
    await tx`update lease set done = true where source = ${me.source} and collector = ${me.collector}
             and granted_at is not null and not done and expires_at <= clock_timestamp()`;
    const [busy] = await tx`select count(*)::int n from lease where source = ${me.source} and collector = ${me.collector}
                            and granted_at is not null and not done`;
    if (busy.n === 0)
      await tx`update lease set granted_at = clock_timestamp(),
               expires_at = clock_timestamp() + make_interval(secs => ttl_ms / 1000.0)
               where id = (select id from lease where source = ${me.source} and collector = ${me.collector}
                           and granted_at is null and not done order by id limit 1)`;
    const [r] = await tx`select granted_at is not null and not done and expires_at > clock_timestamp() as mine
                         from lease where id = ${id}`;
    return !!r.mine;
  });
}

/** Cancel a lease request or give back a lease (client gave up). Server-side expiry covers crashed clients. */
export async function cancelLease(db: Db, id: number) {
  await db`update lease set done = true where id = ${id}`;
}

export async function acquireLease(db: Db, source: string, collector: string, ttlMs: number): Promise<number> {
  const id = await requestLease(db, source, collector, ttlMs);
  for (let i = 0; i < 2000; i++) { if (await tryGrant(db, id)) return id; await Bun.sleep(5); }
  await cancelLease(db, id);
  throw new Error("lease not granted");
}

// ------------------------------------------------------------------------------------------------ sync commit

export type SyncCommit = { leaseId: number; commitId: string; records: { rec: string; hash: string }[]; scanChunks?: number };
export type SyncResult = { ok: true } | { ok: false; reason: "lease" | "source" };
export type SyncOpts = {
  noMergeLock?: boolean;                       // broken variant: merge without the global barrier
  afterLeaseCheck?: () => Promise<void>;       // holding the lease row lock, before the barrier wait
  afterMergeRead?: () => Promise<void>;        // merge inputs computed, shared state not yet written
};

/**
 * One atomic transaction (§4.1, §5.1):
 *   commit-id lock + short-circuit (a concurrent retry waits, then reads the original result)
 *   → lease row lock → records must belong to the lease's source
 *   → global barrier (serializes the shared merge; also the visibility barrier)
 *   → lease validity on the server clock, checked AFTER every lock wait
 *   → body dedupe → projection update → merge (conflict entry = visibility tightening, bumps the session)
 *   → scan progress → record result → release lease.
 */
export async function syncCommit(db: Db, c: SyncCommit, opts: SyncOpts = {}): Promise<SyncResult> {
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${COMMIT_NS}, hashtext(${c.commitId}))`;
    const [prev] = await tx`select result from commit_result where commit_id = ${c.commitId}`;
    if (prev) return prev.result as SyncResult;
    const [l] = await tx`select source, collector, done from lease where id = ${c.leaseId} for update`;
    if (!l || l.done) return { ok: false, reason: "lease" } as const;   // not recorded: a retry may still be rejected
    if (!c.records.every(r => sessOf(r.rec) === l.source)) return { ok: false, reason: "source" } as const;
    await opts.afterLeaseCheck?.();
    if (!opts.noMergeLock) await tx`select pg_advisory_xact_lock(${BARRIER})`;
    const [v] = await tx`select granted_at is not null and expires_at > clock_timestamp() as valid
                         from lease where id = ${c.leaseId}`;
    if (!v.valid) return { ok: false, reason: "lease" } as const;
    const merged: { rec: string; next: { state: string; hash: string | null }; old: any }[] = [];
    for (const r of c.records) {
      await tx`insert into body values (${r.rec}, ${r.hash}) on conflict do nothing`;   // dedupe stores body once
      await tx`insert into projection values (${r.rec}, ${l.collector}, ${r.hash})
               on conflict (rec, collector) do update set hash = excluded.hash`;      // projection always updated
      const hashes = await tx`select distinct hash from projection where rec = ${r.rec}`;
      const next = hashes.length === 1 ? { state: "ok", hash: hashes[0].hash } : { state: "conflict", hash: null };
      const [old] = await tx`select state, hash from shared where rec = ${r.rec}`;
      merged.push({ rec: r.rec, next, old });
    }
    await opts.afterMergeRead?.();                                   // test hook: both sides computed their merge
    for (const { rec, next, old } of merged) {
      await tx`insert into shared values (${rec}, ${next.state}, ${next.hash})
               on conflict (rec) do update set state = excluded.state, hash = excluded.hash`;
      if (old && (old.state !== next.state || old.hash !== next.hash)) await bump(tx, "s:" + sessOf(rec));
    }
    if (c.scanChunks) await tx`insert into scan_progress values (${l.source}, ${l.collector}, ${c.scanChunks})
                               on conflict (source, collector) do update set chunks = scan_progress.chunks + excluded.chunks`;
    const result = { ok: true } as const;
    await tx`insert into commit_result values (${c.commitId}, ${result})`;
    await tx`update lease set done = true where id = ${c.leaseId}`;
    return result;
  });
}
const COMMIT_NS = 0x6d656d78;                                        // two-key advisory namespace for commit ids

export async function sharedState(db: Db, rec: string): Promise<{ state: "ok"; hash: string } | { state: "conflict" } | { state: "absent" }> {
  const [r] = await db`select state, hash from shared where rec = ${rec}`;
  if (!r) return { state: "absent" };
  return r.state === "ok" ? { state: "ok", hash: r.hash } : { state: "conflict" };
}
export async function scanProgress(db: Db, source: string, collector: string): Promise<number> {
  const [r] = await db`select chunks from scan_progress where source = ${source} and collector = ${collector}`;
  return r ? r.chunks : 0;
}
