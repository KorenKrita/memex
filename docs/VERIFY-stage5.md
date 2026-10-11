# Stage 5 verification record

What was checked for the deployable package (deploy/, docs/INSTALL.md) and the two plugins, on one machine,
with synthetic data only. No real session was uploaded; nothing ran on server.

Versions: memex e5927c1 and later (this file's commit), Bun 1.4.2, Pi 1.1.0, codex-cli 0.162.0-alpha.2,
PostgreSQL 17.11 (pgvector/pgvector:pg17), vector 0.8.7, pg_trgm 1.6, Docker on macOS.

Scripts used: `eval/smoke/` — `smoke.ts` (sync one fake Pi session, grep it), `load.ts` (N sessions × R
exchanges, sync 8 at a time, wait for projection, 30 greps that must each hit the expected session), `fork.ts` (`parent` / `child`
modes for fork-after-forget), `hang.ts` (fake model endpoint on :18799 that hangs, or answers when
`/tmp/memex-smoke/hang.mode` holds `ok`). All take `MEMEX_URL` and `MEMEX_KEY`.

## Regression

At de816ab: `bunx tsc -p .` exit 0; `bun run test` 429 pass, 1 skip, 0 fail; `bun test dryrun`
79 pass; Pi e2e 2 pass; Codex e2e (`MEMEX_E2E_CODEX=1`, real codex-cli) 1 pass.
After the r4 fix (the commit adding this line): `bun run test` 430 pass, 1 skip, 0 fail (only server code and
its test changed; dryrun and plugin e2e not rerun).

## Results

| Check | How | Result |
| --- | --- | --- |
| Compose up | `deploy/` per INSTALL §1, admin key + grants, `smoke.ts` | sync 3/3 records, grep hit |
| Postgres stops under a running API (1d0c19a) | `docker stop memex-db-1`, then start | API process lives; `/healthz` 503 within 3.7 s; a client sync fails within 10 s; after start the same process syncs again (before the fix the process exited) |
| SIGTERM with a model call hanging (e5927c1) | `hang.ts` hang mode, `MEMEX_IDLE_CLOSE_MS=0`, wait for `/chat/completions`, `docker compose stop -t 20 api` | before: 21.4 s, exit 137 (killed); after: 5.6 s, exit 0 |
| Model recovers after an outage | separate compose project with `hang.ts` as LLM and embedding endpoint; a summarize call hanging; SIGTERM (5.3 s, exit 0); `hang.ts` switched to valid replies; API started | within 4 s every job done: summarize (attempt 2), l3, embed, forest, card; 1 `span_summary` row stored; grep finds the session |
| Database silent on open connections (this commit) | `server/main.test.ts`: API on a TCP proxy that, after a first answer, drops all traffic on open sockets while a request arrives every 500 ms. Real: `docker pause` on the database of a separate compose project, a request every 0.5 s | Under Bun 1.4.2 such queries never settle while new ones arrive (measured directly: 41 queries over 40 s, none settled; an earlier test that stopped sending saw them fail at 30 s, which does not hold under traffic). Now: `/healthz` 503 within 2 s; after that API requests get 503 at once with no query; the process exits 1 after 10 s. Real: 10 immediate 503s, exit and compose restart 11 s after the pause; after unpause `/healthz` ok, and a new session synced and was found by grep in that session |
| Postgres refusing connections while requests keep coming | `server/main.test.ts`, 20 s of refusals with `MEMEX_DB_STUCK_MS=7000` | the process stays (refusals take 5.26 s to fail, an answer); `/healthz` 200 again once connections pass |
| SIGTERM with a health query in flight on a silent database | `server/main.test.ts`, `MEMEX_STOP_MS=300` | the process exits within 1.5 s (before: 28 s, because `close({ timeout: 0 })` waits for queries in flight under Bun 1.4.2) |
| `close()` with the budget used up | `server/main.test.ts`: a worker job holding a `pg_sleep(3)` on the application pool and a `pg_sleep(3)` on the probe pool, `MEMEX_STOP_MS=200`, `close()` called directly (no signal-level exit involved) | returns in about 210 ms, both queries cut (with the probe pool closed by `timeout: 0`, as before: 2 923 ms) |
| Gate without application queries | `server/main.test.ts`: probe stub that never answers, application stub that counts calls | after the probe deadline, 3 `/v1/grep` requests get 503, 0 application queries |
| `/healthz` single-flight and own connection | `server/main.test.ts` | 3 requests against a database that never answers: 3 × 503 within 2.5 s each, 1 query issued; all 8 application connections blocked by a lock: `/healthz` still 200 |
| Worker backoff vs stop | `server/worker.test.ts` | abort during a 2 s backoff ends the loop in < 200 ms (was 1951 ms) |
| Backup / restore (b8a3a86, 521542f) | `deploy/backup.sh`, then INSTALL's restore command from the repo root | restore into a new database succeeds, row counts equal; restore over the live database exits 2, database unchanged; missing file exits 2; dumps mode 600, two runs never share a file; no dump is deleted by the script |
| Forget vs backup | forget a source, re-upload it | 410. A backup taken before the forget brings 3 forgotten bodies back on restore: documented in INSTALL (repeat forgets after a restore) |
| Fork copy after forget, on a restored database | `fork.ts parent` → forget → `pg_dump`/`pg_restore` into a fresh database → API on it → `fork.ts child` | child sync: 4 sent, 2 refused (the copied parent records); `FORKPARENT-SECRET`/`ANSWER` 0 rows in lexical and body; the child's own entry 1 row; projection ok |
| Codex plugin install / remove | isolated `CODEX_HOME`, `codex plugin marketplace add` + install, e2e run, `codex plugin remove` + `marketplace remove` | after removal only an empty `config.toml` and an empty `plugins/cache/memex-local` directory remain |
| Pi extension install / remove | isolated `PI_CODING_AGENT_DIR`, path in `settings.json` `extensions`, RPC `get_commands` | listed: `/memex` present; path removed: no memex command |
| Real configuration untouched | sha1 of `~/.codex/config.toml`, `~/.pi/agent/settings.json` before/after | unchanged |
| Plugins write nothing locally | grep for fs writes in plugins/ and client/ | only `MEMEX_DEBUG_LOG` (opt-in) |
| Synthetic load (b2ac73f) | `load.ts 200 40` (fresh session ids per run; each grep is for a marker held by one record and must hit that session only; the script exits 1 otherwise), onto a database already holding about 32 000 records from earlier runs | 200/200 sessions synced, 16 200 sent (16 000 messages + 200 headers), 0 refused, 3.6 s at 8 concurrent; projection of the last session done 105 s after the sync ended (worker concurrency 1); 30/30 greps found in the expected session, p50 169 ms, p95 180 ms with 48 618 records stored; peak memory api 78 MiB / db 229 MiB (limits 256 / 384); no OOM, no restart; database 260 MB. Earlier runs used a version of the script whose repeated runs reuse session ids and whose checks accepted a hit from any session: their projection and hit figures are withdrawn |
| Shutdown with a query outstanding (f5ffe8a) | `server/main.test.ts`: `pg_sleep(3)` running, `MEMEX_STOP_MS=200` | `close()` 336 ms (was 2 910 ms); the query is cut |
| `/healthz` on a database that stops answering (158c68c) | `server/main.test.ts`, database stub that never answers | 503 after 2 s (was: no answer) |
| Restore target and passwords (2601d2e) | separate compose project, password `a#b/c?d%40 x$y` | API connects (password in `PGPASSWORD`); `restore` rejects `dbname=memex`, `-x`, `Memex`, `memex`, a 64-byte name (exit 2); `MEMEX_PG_DB=memex_r5b` switches the API to the restored copy, and then `backup.sh` dumps it and refuses to restore over it |

## Not verified

- With models configured: the full derivation pipeline under load (summaries, L3, embeddings, forests), its
  throughput, and how long a backlog takes to drain. The load runs had no model configured: after the first
  run, about 15 600 summarize/l3 jobs were queued; each fails after its retries with `MEMEX_LLM_URL not
  configured`. The model-recovery check above covers one session with a fake model, not real models.
- Search / recall latency with summaries and embeddings present (only grep was measured).
- The silent-database handling rests on Bun 1.4.2 behaviour measured here (queries on a silent connection
  do not settle while traffic continues; `close({ timeout: 0 })` waits, a positive timeout cuts). A Bun
  upgrade must rerun `server/main.test.ts`. Requests in flight when the database goes silent fail.
- Observed, not changed: jobs run in `not_before, id` order, so a derivation backlog delays projection of
  newly synced sessions (what grep reads). On the local instance with no model configured (about 24 000
  queued l3 jobs, each failing quickly) a session synced after the pause test was not projected within 80 s.
  With models configured, how fast a backlog is worked off and how much it delays projection are not measured.
- Pi `/tree` and compaction followed by recall in a real Pi; Codex with a shared app-server (several
  threads, one MCP process); a Codex fork's first turn.
- server itself (1.9 GB memory), the shared reverse proxy, and turning off OV's automatic recall.

## Independent reviews

First review (da62c15..2720a7a): `/tmp/memex-review/stage5-215853.md`, 7 findings, all in this range.
Findings 1-3 and 5-7 held and were fixed. Finding 4 held for `/healthz` (no deadline of its own); its claim
that the worker would hang on a silent database also held under continuing traffic (see the third review).

| # | Finding | Status |
| --- | --- | --- |
| 1 | shutdown waited for outstanding queries after the worker budget | fixed f5ffe8a: one `MEMEX_STOP_MS` budget, connections cut at its end |
| 2 | `restore ... 'dbname=memex'` passed the guard and restored into the live database | fixed 2601d2e: plain name `[a-z_][a-z0-9_]{0,62}`, never the live database |
| 3 | special characters in the password broke the URL | fixed 2601d2e: `PGPASSWORD` |
| 4 | `/healthz` and worker queries hung on a database that stops answering | held. `/healthz`: 158c68c (2 s deadline), a5485a2 (single-flight on its own connection). Worker: a watchdog (5d86ac3) was removed in a5485a2 on a mistaken premise (that queries fail at the 30 s idle timeout; true only without further traffic); the third review showed that, and the fix after it refuses requests while the database is silent and exits after 10 s |
| 5 | repeated `load.ts` runs reused ids | fixed b2ac73f: ids per run, counts asserted |
| 6 | grep checks accepted hits from any session | fixed b2ac73f: one hit in the expected session; the final probe must hit too |
| 7 | INSTALL's database switch did nothing under compose | fixed 2601d2e: `MEMEX_PG_DB` |

Second review (2720a7a..beb59ab): `/tmp/memex-review/stage5-r2-223738.md`. It confirmed findings 1-3 and 5-7
as resolved, and 4 as partly resolved, and found two problems in the watchdog:

| # | Finding | Status |
| --- | --- | --- |
| r2-1 | timed-out probes stayed outstanding and accumulated (watchdog and each `/healthz`) | held: one probe in flight however many ask (a5485a2); the fail-stop after r3 judges that same probe by how long it has gone unanswered, never submitting another while it hangs |
| r2-2 | the watchdog, on the shared pool, took a busy pool for a silent database and killed the process | held: the probe has its own 1-connection pool (a5485a2; test: 8 application connections blocked, `/healthz` 200), which the fail-stop after r3 also uses |

Third review (beb59ab..ec6f67e): `/tmp/memex-review/stage5-r3-231425.md`.

| # | Finding | Status |
| --- | --- | --- |
| r3-1 | requests arriving on a silent database keep its connections from timing out; queries accumulate | held (reproduced: 41 queries over 40 s, none settled): API requests are refused before any query once the probe is overdue; the process exits after the probe has gone 10 s unanswered |
| r3-2 | `close({ timeout: 0 })` waits for queries in flight, so shutdown could exceed its budget | held (reproduced: 1 930 ms, query completed; `0.001`: 2 ms, cut): both pools closed with a positive timeout from the shared deadline, and a signal-level forced exit at the end of the budget |

Fourth review (ec6f67e..de816ab): `/tmp/memex-review/stage5-r4-234213.md`. It reproduced the default behaviour
(exit at 11.0 s with requests gated; refusals do not exit; busy pool stays healthy; SIGTERM/SIGINT 308/310 ms;
`close()` 215 ms) and found one problem:

| # | Finding | Status |
| --- | --- | --- |
| r4-1 | `MEMEX_DB_STUCK_MS` above the probe pool's 30 s idle timeout lets the silent probe fail first, reopening the gate and never exiting (60 s: 164 queries pending) | held: the server refuses to start outside 6-25 s (test) |
