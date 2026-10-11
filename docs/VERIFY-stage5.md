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

At 158c68c plus this commit: `bunx tsc -p .` exit 0; `bun run test` 424 pass, 1 skip, 0 fail; `bun test dryrun`
79 pass; Pi e2e 2 pass; Codex e2e (`MEMEX_E2E_CODEX=1`, real codex-cli) 1 pass.

## Results

| Check | How | Result |
| --- | --- | --- |
| Compose up | `deploy/` per INSTALL §1, admin key + grants, `smoke.ts` | sync 3/3 records, grep hit |
| Postgres stops under a running API (1d0c19a) | `docker stop memex-db-1`, then start | API process lives; `/healthz` 503 within 3.7 s; a client sync fails within 10 s; after start the same process syncs again (before the fix the process exited) |
| SIGTERM with a model call hanging (e5927c1) | `hang.ts` hang mode, `MEMEX_IDLE_CLOSE_MS=0`, wait for `/chat/completions`, `docker compose stop -t 20 api` | before: 21.4 s, exit 137 (killed); after: 5.6 s, exit 0 |
| Model recovers after an outage | separate compose project with `hang.ts` as LLM and embedding endpoint; a summarize call hanging; SIGTERM (5.3 s, exit 0); `hang.ts` switched to valid replies; API started | within 4 s every job done: summarize (attempt 2), l3, embed, forest, card; 1 `span_summary` row stored; grep finds the session |
| Database silent on open connections (5d86ac3) | `server/main.test.ts`: TCP proxy that refuses, then drops all traffic on open sockets; real: `docker pause` on the database container | refusing for 12 s: the process stays (the worker backs off); silent: watchdog (one `select 1` every 10 s, 3 s each, never more than one outstanding) exits 1 after 3 misses. Real: the API exited and compose restarted it 40 s after the pause; after unpause `/healthz` ok and a sync succeeded |
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
- The watchdog restarts a process whose database went silent; requests in flight at that moment fail, and
  a database that answers `select 1` but stalls one specific query is not detected.
- Pi `/tree` and compaction followed by recall in a real Pi; Codex with a shared app-server (several
  threads, one MCP process); a Codex fork's first turn.
- server itself (1.9 GB memory), the shared reverse proxy, and turning off OV's automatic recall.

## Independent review (da62c15..2720a7a)

Report: `/tmp/memex-review/stage5-215853.md`. All seven findings held and were in this range.

| # | Finding | Status |
| --- | --- | --- |
| 1 | shutdown waited for outstanding queries after the worker budget | fixed f5ffe8a: one `MEMEX_STOP_MS` budget, connections cut at its end |
| 2 | `restore ... 'dbname=memex'` passed the guard and restored into the live database | fixed 2601d2e: plain name `[a-z_][a-z0-9_]{0,62}`, never the live database |
| 3 | special characters in the password broke the URL | fixed 2601d2e: `PGPASSWORD` |
| 4 | `/healthz` and worker queries hung on a database that stops answering | fixed 158c68c (`/healthz` 2 s) and 5d86ac3 (watchdog exit and restart) |
| 5 | repeated `load.ts` runs reused ids | fixed b2ac73f: ids per run, counts asserted |
| 6 | grep checks accepted hits from any session | fixed b2ac73f: one hit in the expected session; the final probe must hit too |
| 7 | INSTALL's database switch did nothing under compose | fixed 2601d2e: `MEMEX_PG_DB` |
