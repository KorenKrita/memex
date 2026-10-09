# memex protocol prototype

Checks the database-side ordering rules of `docs/DESIGN.md` against a real Postgres, using several independent
connections. It covers §4.1 (per-collector projections and merge, conflict isolation), §5.1/§5.2 (FIFO sync
leases, commit idempotency, scan progress) and §5.5 (two-step publish barrier, write-side invariant, locked-section
deadline), plus artifact invalidation by generation vector (§6.2).

It is **not** the product: no plugins, no HTTP server, no real sessions, no models. The Postgres container below
is a throwaway test fixture. The product rule "no service or storage on the user's machine" is about the
runtime plugin, not about a local test database.

## Run

```sh
bun run db:up      # postgres:17-alpine on 127.0.0.1:55432, data on tmpfs (gone when the container stops)
bun test
bun run db:down
```

`MEMEX_PG_URL` overrides the connection string.

## What the tests force

Interleavings come from explicit pause hooks in `publish()` (`duringCheck`, `afterCheck`, `beforeLock`,
`inLock`), in writers (`beforeCommit`: exclusive lock held, change not committed) and in `syncCommit()`
(`afterLeaseCheck`, `afterMergeRead`), from holding the global barrier on a separate connection, from
observing `pg_locks` (backends really waiting on an advisory lock) and from the server clock
(`clock_timestamp()` past a lease's expiry). There are no blind sleeps that decide an outcome.

Every "broken variant caught" test runs a deliberately wrong version of a rule and asserts the bug shows up,
so the matching correct-protocol tests are known to bite: sampling versions in a separate snapshot, reusing the
step-1 snapshot, a vector without view sessions, taking the lock and reading versions in one statement (its
snapshot predates the lock wait), and merging without the barrier. Commit-loss tests: the backend's termination is
observed in `pg_stat_activity` before COMMIT is sent; the "committed, ack lost" case uses the test-only
`afterCommit` seam (real COMMIT, then a simulated closed-connection error; a fault injection, not a real network
failure) and asserts the publication exists.

Mutation runs: `bun /tmp/memex-review/mutate-final.ts` (outside the repo; literal replacements, restores the
file on exit, full log). Deleting or weakening one rule in `protocol.ts` turned the matching tests red for:
fresh read after the lock, each vector component (view / query / destination), remap bumping the occurrence's
session, writer and publisher barrier locks, the locked-section deadline, merge serialization, lease expiry on the
server clock, commit-id short-circuit, conflict bumping the version, generation-vector validity, FIFO grant, no
grant over a valid lease, conflict in the exact check; later rounds (15/15 in the last run): generation-vector
check inside `publish`, `transaction_timeout` instead of `statement_timeout`, granting the queue head on any
poll, re-checking lease expiry after the barrier wait, the commit-id lock, records restricted to the lease's
source, reporting a lease only while unexpired and not done, `uncertain` after COMMIT, the `commitAttempted`
flag, exclusive (not shared) merge lock, the lease-lifetime timer, resetting an inherited timer (publish and
sync), the 64-bit remaining-lifetime cast, and the successor's grant having to touch the expired lease row.

## Prototype choices (not performance conclusions)

- One global advisory lock is both the visibility barrier and the merge serializer. It is the simplest thing
  that is correct; contention under real load has not been measured.
- `transaction_timeout` (Postgres 17, set per locked transaction) is the server-side deadline. It bounds the
  transaction up to COMMIT, including client pauses between statements; on expiry the backend is terminated,
  which releases the lock and discards the publication. It does **not** cover the COMMIT phase itself (PG17
  `CommitTransaction()` disables it before the durable commit), so a publication whose COMMIT started in time
  can finish slightly late. `statement_timeout` is not enough: many short statements can hold the lock
  indefinitely. `publish()` reserves its own pool connection, and first sets the timeout to 0 because setting a
  new value does not reschedule an already-active (inherited) timer.
- When the deadline fires during a statement, Bun receives SQLSTATE `25P04` → `timeout` (confirmed). When it fires
  while the session is idle in transaction (client pause), Bun reports only `ERR_POSTGRES_CONNECTION_CLOSED`,
  indistinguishable from any other disconnect. `publish()` therefore classifies a closed connection by position:
  before COMMIT was attempted = `aborted` (definitely not committed, cause unknown); after = `uncertain`
  (could be "committed, ack lost"; the caller must not assume a rollback).
- Bun 1.4.2 quirk: on a closed reserved connection the `IN ${sql(list)}` helper raises an extra unhandled
  rejection that escapes try/catch; version reads use `= any(${sql.array(keys, "text")})` instead.
- A lease queue advances on any caller's poll: the oldest pending request is granted even if its client is gone,
  then expires after its ttl and is reaped, so a dead head cannot block the queue. `syncCommit()` re-checks
  lease expiry on the server clock after the barrier wait, under the lease row's `FOR UPDATE` lock. That check
  is the cut-off (DESIGN §5.1 "only commit while the lease is valid"). Safety does not depend on a timer: the
  successor's grant must update the expired lease row to reap it, so it waits for this transaction to end; a
  commit accepted at the check always lands before the next lease exists, even if its COMMIT finishes after
  `expires_at`. Interpretation, not yet written into DESIGN: "valid" means valid at the server-side acceptance
  check, not at COMMIT completion. For liveness, the same statement arms `transaction_timeout` with the remaining
  lifetime (after clearing any inherited timer), so a stalled pre-COMMIT transaction cannot hold the queue far
  past expiry. Not a hard millisecond bound; the COMMIT phase is not covered (see above).
- "Published" means a row in `publication`, written inside the locked section. HTTP/socket hand-off and
  cancellation at that boundary are **not** verified here.
