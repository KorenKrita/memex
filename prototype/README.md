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
snapshot predates the lock wait), and merging without the barrier.

Mutation runs (delete one rule from `protocol.ts`, run the suite) turned the matching tests red for: fresh read
after the lock, each vector component (view / query / destination), remap bumping the occurrence's session,
writer and publisher barrier locks, the locked-section deadline, merge serialization, lease expiry on the server
clock, commit-id short-circuit, conflict bumping the version, generation-vector validity, FIFO grant, no grant
over a valid lease, conflict in the exact check, and (second round) generation-vector check inside `publish`,
`transaction_timeout` instead of `statement_timeout`, granting the queue head on any caller's poll, re-checking
lease expiry after the barrier wait, the commit-id lock, records restricted to the lease's source, and reporting
a lease only while unexpired and not done.

## Prototype choices (not performance conclusions)

- One global advisory lock is both the visibility barrier and the merge serializer. It is the simplest thing
  that is correct; contention under real load has not been measured.
- `transaction_timeout` (Postgres 17, set per locked transaction) is the server-side deadline. It bounds the
  whole transaction including client pauses between statements; on expiry the backend is terminated, which
  releases the lock and discards the publication. `statement_timeout` is not enough: many short statements
  can hold the lock indefinitely. `publish()` reserves its own pool connection for this reason.
- A lease queue advances on any caller's poll: the oldest pending request is granted even if its client is gone,
  then expires after its ttl and is reaped, so a dead head cannot block the queue. `syncCommit()` re-checks
  lease expiry after the barrier wait, so a lease that runs out while queued commits nothing.
- "Published" means a row in `publication`, written inside the locked section. HTTP/socket hand-off and
  cancellation at that boundary are **not** verified here.
