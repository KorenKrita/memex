# Stage 5 verification record

What was checked for the deployable package (deploy/, docs/INSTALL.md) and the two plugins, on one machine,
with synthetic data only. No real session was uploaded; nothing ran on server.

Versions: memex e5927c1 and later (this file's commit), Bun 1.4.2, Pi 1.1.0, codex-cli 0.162.0-alpha.2,
PostgreSQL 17.11 (pgvector/pgvector:pg17), vector 0.8.7, pg_trgm 1.6, Docker on macOS.

Scripts used: `eval/smoke/` — `smoke.ts` (sync one fake Pi session, grep it), `load.ts` (N sessions × R
exchanges, sync 8 at a time, wait for projection, 30 greps that must each hit), `fork.ts` (`parent` / `child`
modes for fork-after-forget), `hang.ts` (fake model endpoint on :18799 that hangs, or answers when
`/tmp/memex-smoke/hang.mode` holds `ok`). All take `MEMEX_URL` and `MEMEX_KEY`.

## Regression

At e5927c1 plus this file: `bunx tsc -p .` exit 0; `bun run test` 422 pass, 1 skip, 0 fail; `bun test dryrun`
79 pass; Pi e2e 2 pass; Codex e2e (`MEMEX_E2E_CODEX=1`, real codex-cli) 1 pass.

## Results

| Check | How | Result |
| --- | --- | --- |
| Compose up | `deploy/` per INSTALL §1, admin key + grants, `smoke.ts` | sync 3/3 records, grep hit |
| Postgres stops under a running API (1d0c19a) | `docker stop memex-db-1`, then start | API process lives; `/healthz` 503 within 3.7 s; a client sync fails within 10 s; after start the same process syncs again (before the fix the process exited) |
| SIGTERM with a model call hanging (e5927c1) | `hang.ts` hang mode, `MEMEX_IDLE_CLOSE_MS=0`, wait for `/chat/completions`, `docker compose stop -t 20 api` | before: 21.4 s, exit 137 (killed); after: 5.6 s, exit 0. After restart with `ok` mode the job's lock lapsed and it was sent to the model again |
| Worker backoff vs stop | `server/worker.test.ts` | abort during a 2 s backoff ends the loop in < 200 ms (was 1951 ms) |
| Backup / restore (b8a3a86, 521542f) | `deploy/backup.sh`, then INSTALL's restore command from the repo root | restore into a new database succeeds, row counts equal; restore over `memex` exits 2, database unchanged; missing file exits 2; dumps mode 600, two runs never share a file; no dump is deleted by the script |
| Forget vs backup | forget a source, re-upload it | 410. A backup taken before the forget brings 3 forgotten bodies back on restore: documented in INSTALL (repeat forgets after a restore) |
| Fork copy after forget, on a restored database | `fork.ts parent` → forget → `pg_dump`/`pg_restore` into a fresh database → API on it → `fork.ts child` | child sync: 4 sent, 2 refused (the copied parent records); `FORKPARENT-SECRET`/`ANSWER` 0 rows in lexical and body; the child's own entry 1 row; projection ok |
| Codex plugin install / remove | isolated `CODEX_HOME`, `codex plugin marketplace add` + install, e2e run, `codex plugin remove` + `marketplace remove` | after removal only an empty `config.toml` and an empty `plugins/cache/memex-local` directory remain |
| Pi extension install / remove | isolated `PI_CODING_AGENT_DIR`, path in `settings.json` `extensions`, RPC `get_commands` | listed: `/memex` present; path removed: no memex command |
| Real configuration untouched | sha1 of `~/.codex/config.toml`, `~/.pi/agent/settings.json` before/after | unchanged |
| Plugins write nothing locally | grep for fs writes in plugins/ and client/ | only `MEMEX_DEBUG_LOG` (opt-in) |
| Synthetic load | `load.ts 200 40` twice (16 000 records each, second run onto a database holding the first) | sync 3.0 s / 3.4 s, 8 concurrent, 0 failures; second run: projection done 117 ms after the last sync, 30/30 greps hit, p50 57 ms, p95 94 ms; peak memory api 109 MiB / db 189 MiB (limits 256 / 384); no OOM, no restart; database 101 MB after the first run |

## Not verified

- With models configured: the full derivation pipeline under load (summaries, L3, embeddings, forests), its
  throughput, and how long a backlog takes to drain. In the load runs no model was configured: about 15 600
  summarize/l3 jobs stayed queued; each fails after its retries with `MEMEX_LLM_URL not configured`.
- Search / recall latency with summaries and embeddings present (only grep was measured).
- A model endpoint answering correctly after an outage: the fake model's reply is not a valid summary, so the
  restarted job reached the model but ended `summary invalid`.
- Pi `/tree` and compaction followed by recall in a real Pi; Codex with a shared app-server (several
  threads, one MCP process); a Codex fork's first turn.
- server itself (1.9 GB memory), the shared reverse proxy, and turning off OV's automatic recall.
