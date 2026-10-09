// Schema and connection (DESIGN §4, §5, §7). One Postgres database holds every piece of state; migrations are
// idempotent and run at start-up. Tables by layer:
//   auth      api_key, grant_rule                 who may call, and which (scope, purpose, destination) is allowed
//   L0        source, lease, commit_result, projection, body, record, scan_progress, tomb, vis_version
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
-- purpose: sync | snapshot | recall | tool ; dest: memex | llm | embedding | rerank. Default: nothing allowed.
create table if not exists grant_rule (scope text not null, purpose text not null, dest text not null,
                                       primary key (scope, purpose, dest));

create table if not exists source (id text primary key, harness text not null, native_id text not null, scope text not null,
                                   parent_id text, meta jsonb not null default '{}', created_at timestamptz not null default now());
create table if not exists vis_version (key text primary key, v bigint not null default 1);
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
create table if not exists scan_progress (source text not null, collector text not null, chunks bigint not null default 0,
                                          primary key (source, collector));
create table if not exists collector_view (source text not null, collector text not null, leaf text, at timestamptz not null default now(),
                                           primary key (source, collector));

create table if not exists job (id bigserial primary key, kind text not null, key text not null, payload jsonb not null default '{}',
                                state text not null default 'queued', attempts int not null default 0, not_before timestamptz not null default now(),
                                locked_until timestamptz, error text, created_at timestamptz not null default now(),
                                updated_at timestamptz not null default now());
create unique index if not exists job_pending on job (kind, key) where state in ('queued', 'running');
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

/** Bump the visibility version of a key (session `s:<id>`, destination `d:<name>`). Inside the caller's tx. */
export const bump = (tx: Tx, key: string) =>
  tx`insert into vis_version values (${key}, 2) on conflict (key) do update set v = vis_version.v + 1`;

/** Enqueue derived work once: a pending job with the same (kind, key) absorbs the new request. */
export const enqueue = (tx: Tx, kind: string, key: string, payload: object = {}) =>
  tx`insert into job (kind, key, payload) values (${kind}, ${key}, ${payload})
     on conflict (kind, key) where state in ('queued', 'running') do update set payload = excluded.payload, updated_at = now()`;
