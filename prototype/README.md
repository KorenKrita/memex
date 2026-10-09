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
`inLock`), from observing `pg_locks` (a writer really waiting on the barrier) and from the server clock
(`clock_timestamp()` past a lease's expiry). There are no blind sleeps that decide an outcome.

Every "broken variant caught" test runs a deliberately wrong version of a rule and asserts the bug shows up,
so the matching correct-protocol tests are known to bite. Mutation runs (delete one rule from `protocol.ts`,
run the suite) also turned the matching tests red for: fresh read after the lock, each vector component
(view / query / destination), remap bumping the occurrence's session, writer and publisher barrier locks,
the locked-section deadline, merge serialization, lease expiry on the server clock, commit-id short-circuit,
conflict bumping the version, generation-vector validity, FIFO grant, no grant over a valid lease, and
conflict in the exact check.

## Prototype choices (not performance conclusions)

- One global advisory lock is both the visibility barrier and the merge serializer. It is the simplest thing
  that is correct; contention under real load has not been measured.
- `statement_timeout` (set per locked transaction) is the server-side deadline. A timed-out statement aborts
  the transaction and releases the lock, which the deadline test observes by a waiting writer finishing.
- "Published" means a row in `publication`, written inside the locked section. HTTP/socket hand-off and
  cancellation at that boundary are **not** verified here.
