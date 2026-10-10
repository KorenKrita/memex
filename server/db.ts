// Schema and connection (DESIGN §4, §5, §7). One Postgres database holds every piece of state; migrations are
// idempotent and run at start-up. Tables by layer:
//   auth      api_key, grant_rule                 who may call, and which (scope, purpose, destination) is allowed
//   L0        source, lease, commit_result, projection, body, record, scan_round/item, tomb, vis_version
//   jobs      job                                 derived work, enqueued in the same transaction as its input
// Derived layers (span, forest, statements) add their own tables in their modules' migrations.
import { SQL } from "bun";

export type Db = SQL;
export type Tx = any;

export function connect(url = process.env.MEMEX_PG_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex", max = 8): Db {
  return new SQL({ url, max, idleTimeout: 30 });
}

const L0 = `
create extension if not exists vector;
create extension if not exists pg_trgm;

create table if not exists api_key (hash text primary key, name text not null, created_at timestamptz not null default now(),
                                    revoked bool not null default false);
create unique index if not exists api_key_name on api_key (name);
-- purpose: sync | snapshot | recall | tool | derive ; dest: memex | llm | embedding | rerank. Default: nothing allowed.
create table if not exists grant_rule (scope text not null, purpose text not null, dest text not null,
                                       primary key (scope, purpose, dest));

create table if not exists source (id text primary key, harness text not null, native_id text not null, scope text not null,
                                   parent_id text, meta jsonb not null default '{}', created_at timestamptz not null default now());
create table if not exists vis_version (key text primary key, v bigint not null default 1);
-- Frozen recall decisions (§6.2), one per real user input of a session, written once. recall_state orders a
-- session's decisions (seq, assigned under its row lock, so commit order = seq order). Withdrawals that hide
-- some of a session's blocks move its version 'r:<session>' (bump() does it in the withdrawing transaction),
-- which every replay response depends on — an empty increment included.
create table if not exists recall_state (source text primary key, seq bigint not null default 0, gen bigint);
create table if not exists recall_block (source text not null, user_rec text not null, query_hash text not null, seq bigint not null,
                                         state text not null, text text not null, deps jsonb not null, sources text[] not null,
                                         refs jsonb not null default '[]', at timestamptz not null default now(), primary key (source, user_rec));
create index if not exists recall_block_deps on recall_block using gin (deps);
alter table recall_block add column if not exists refs jsonb not null default '[]';
-- From the 2b83df9 schema: recall_state.gen was the withdrawal count; it moves into r:<session> (kept above any
-- version a client may hold), and the column stays (unused) rather than being dropped.
alter table recall_state add column if not exists gen bigint;
insert into vis_version select 'r:' || source, gen + 1 from recall_state where gen is not null and gen > 0
  on conflict (key) do update set v = greatest(vis_version.v, excluded.v);
-- Blocks frozen before hints carried their view (refs empty, bare span:/record: handles): never served again
-- (their handles would reopen on whatever view is current); their text stays, nothing is searched again.
with l as (update recall_block set state = 'legacy' where state = 'shown' and refs = '[]' returning source)
insert into vis_version select distinct 'r:' || source, 2 from l on conflict (key) do update set v = vis_version.v + 1;
create table if not exists tomb (key text primary key, at timestamptz not null default now());   -- 'source:S' | 'canon:C'

create table if not exists lease (id bigserial primary key, source text not null, collector text not null, ttl_ms int not null,
                                  created_at timestamptz not null default clock_timestamp(),
                                  granted_at timestamptz, expires_at timestamptz, done bool not null default false);
create index if not exists lease_queue on lease (source, collector, id) where not done;
create table if not exists commit_result (commit_id text primary key, result jsonb not null, at timestamptz not null default now());

-- body: every version ever received; projection: what each collector's copy currently holds, in file order;
-- record: the merged shared L0 (state ok = all collectors agree, conflict = isolated, §4.1).
create table if not exists body (source text not null, rec text not null, hash text not null, raw text not null,
                                 received_at timestamptz not null default now(), primary key (source, rec, hash));
create table if not exists projection (source text not null, collector text not null, pos int not null, rec text not null,
                                       hash text not null, primary key (source, collector, pos));
create unique index if not exists projection_rec on projection (source, collector, rec);
create table if not exists record (source text not null, rec text not null, state text not null, hash text,
                                   first_pos int not null, primary key (source, rec));
drop table if exists scan_progress;
-- Reconciliation rounds (§5.2): per collector and kind (auto | import), a fixed set of files, each with the bound
-- (size at open) the round checks up to and how far the accepted segments have checked it. key is a hash of
-- the file's real path on that collector; source is filled by the first accepted segment.
create table if not exists scan_round (id bigserial primary key, collector text not null, kind text not null,
                                       opened_at timestamptz not null default now(), finished_at timestamptz);
create index if not exists scan_round_by on scan_round (collector, kind, id);
create table if not exists scan_item (round bigint not null references scan_round (id) on delete cascade, key text not null,
                                      ord int not null, bound bigint not null, source text, done_recs int not null default 0,
                                      done_bytes bigint not null default 0, state text not null default 'pending',
                                      tries int not null default 0, reason text, primary key (round, key));
create table if not exists collector_view (source text not null, collector text not null, leaf text, at timestamptz not null default now(),
                                           primary key (source, collector));

-- One pending row per (kind, key). req counts requests; a worker finishes the job only if no request came in
-- while it ran (claimed_req = req), otherwise the job goes back to queued: a request is never absorbed by a run
-- that had already read its input (§5.5).
create table if not exists job (id bigserial primary key, kind text not null, key text not null, payload jsonb not null default '{}',
                                state text not null default 'queued', attempts int not null default 0, not_before timestamptz not null default now(),
                                locked_until timestamptz, error text, created_at timestamptz not null default now(),
                                updated_at timestamptz not null default now(), req bigint not null default 1, claimed_req bigint,
                                claim bigint not null default 0);
create unique index if not exists job_pending on job (kind, key) where state in ('queued', 'running');
alter table job add column if not exists req bigint not null default 1;
alter table job add column if not exists claimed_req bigint;
alter table job add column if not exists claim bigint not null default 0;
`;

const migrations: string[] = [L0];
export function addMigration(sql: string) {
  migrations.push(sql);
}

export async function migrate(db: Db) {
  // One transaction, one migrator at a time (a session-level lock on a pooled connection could be released
  // on another connection).
  await db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(42)`;
    for (const m of migrations) await tx.unsafe(m);
  });
}

/** Bump the visibility version of a key (session `s:<id>`, destination `d:<name>`). Inside the caller's tx.
 *  Sessions holding a frozen recall block made under that key learn of it in the same transaction. */
export const bump = async (tx: Tx, key: string) => {
  await tx`insert into vis_version values (${key}, 2) on conflict (key) do update set v = vis_version.v + 1`;
  if (!key.startsWith("r:"))
    await tx`insert into vis_version select 'r:' || source, 2 from (select distinct source from recall_block where deps ? ${key}) x
             on conflict (key) do update set v = vis_version.v + 1`;
};

/** Enqueue derived work once: a pending job with the same (kind, key) absorbs the request, and a running one
 *  is told to run again (req moves past its claimed_req). */
export const enqueue = (tx: Tx, kind: string, key: string, payload: object = {}, delayMs = 0) =>
  tx`insert into job (kind, key, payload, not_before) values (${kind}, ${key}, ${payload}, now() + make_interval(secs => ${delayMs} / 1000.0))
     on conflict (kind, key) where state in ('queued', 'running')
     do update set payload = excluded.payload, req = job.req + 1, updated_at = now(), not_before = least(job.not_before, excluded.not_before)`;

export type Job = { id: number; kind: string; key: string; payload: any; attempts: number; claimed_req: number; claim: number };
export const JOB_LOCK_MS = Number(process.env.MEMEX_JOB_LOCK_MS ?? 120_000);
const MAX_ATTEMPTS = 8;

/** Claim the next due job of the given kinds (a running job whose lock lapsed counts as due). */
export async function claimJob(db: Db, kinds: string[]): Promise<Job | undefined> {
  const [j] = await db`
    update job set state = 'running', attempts = attempts + 1, claimed_req = req, claim = claim + 1, updated_at = now(),
                   locked_until = clock_timestamp() + make_interval(secs => ${JOB_LOCK_MS} / 1000.0)
    where id = (select id from job where kind in ${db(kinds)} and not_before <= now()
                  and (state = 'queued' or (state = 'running' and locked_until < clock_timestamp()))
                order by not_before, id limit 1 for update skip locked)
    returning id, kind, key, payload, attempts, claimed_req, claim`;
  return j ? { ...j, id: Number(j.id), claimed_req: Number(j.claimed_req), claim: Number(j.claim) } : undefined;
}

/** Finish a claimed job first thing in the caller's publishing transaction: "done" → publish; "rerun" →
 *  requeued, publish nothing; "lost" (lock lapsed, someone else holds it) → publish nothing. */
export async function finishJob(tx: Tx, j: Job): Promise<"done" | "rerun" | "lost"> {
  const [r] = await tx`select req, claim, state from job where id = ${j.id} for update`;
  if (!r || r.state !== "running" || Number(r.claim) !== j.claim) return "lost";
  if (Number(r.req) === j.claimed_req) {
    await tx`update job set state = 'done', locked_until = null, updated_at = now() where id = ${j.id}`;
    return "done";
  }
  // New input arrived after this run read its own: the run's result is stale, do not publish it.
  await tx`update job set state = 'queued', attempts = 0, locked_until = null, updated_at = now() where id = ${j.id}`;
  return "rerun";
}

/** A failed run: back off and retry, or give up after MAX_ATTEMPTS (state failed keeps the error). */
export async function failJob(db: Db, j: Job, err: unknown) {
  const msg = String((err as Error)?.message ?? err).slice(0, 500);
  await db`update job set state = case when attempts >= ${MAX_ATTEMPTS} and req = claimed_req then 'failed' else 'queued' end,
                          error = ${msg}, locked_until = null, updated_at = now(),
                          not_before = clock_timestamp() + make_interval(secs => least(600, 2 ^ attempts))
           where id = ${j.id} and state = 'running' and claim = ${j.claim}`;
}
