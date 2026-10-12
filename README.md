# memex

Personal cross-session, cross-harness (Pi + Codex) memory backend. Under construction: delivery phases 1–3
(DESIGN §11), validated on synthetic data only.

- `docs/INSTALL.md` — installing the server and the two plugins, settings, known limits
- `docs/DESIGN.md` — design (private, not in the public repository)
- `docs/RESEARCH-landscape.md` — survey of open-source memory projects and what was adopted
- `dryrun/` — offline projection rules (spans, views, forks, canonical records), validated on local sessions
- `prototype/` — sync protocol model
- `server/` — the service: L0 sync (`sync.ts`), projection (`project.ts`), summaries and embeddings
  (`derive.ts`), search / zoom / grep (`search.ts`), forget (`forget.ts`), HTTP (`http.ts`), entry (`main.ts`)
- `client/` — record encoding, the sync client and the mem_* tools shared by the plugins; `cli.ts` (import)
- `plugins/pi/` — Pi extension; `plugins/codex/` — Codex plugin (hooks, MCP server, `build.ts`)
- `eval/` — question bank tooling

Tests: a Postgres 17 with pgvector and pg_trgm at `MEMEX_PG_URL` (default
`postgres://postgres:memex@127.0.0.1:55433/memex`), then `bun run test` (server, client, plugins; the Pi
end-to-end test needs `pi` on PATH) and `bun test dryrun`. The live Codex test is opt-in: see
`plugins/codex/e2e.test.ts`.

## Not implemented yet (stage 2 search contract, DESIGN §6.4)

- `mem_search` filters `since` / `until` / `repo`, and dates and repository on each result.
- A default of at most 8 results for the agent tool (the API takes `limit`, default 10, max 40).
- Task status on results and the "related sessions" grouping (they come from session cards and the summary
  forest, stage 3).
- `mem_zoom` above span level (session card → forest root → children), stage 3.
- `/memex trace` and `/memex why <id>` (the trace is written; the commands come with the plugins, stage 4).
- Trace retention (14 days [待标定]).
- The trace does not store the query as its own object (DESIGN §6.5): the query text is not kept at all, and
  the trace row is bound to the querying source and deleted with it on forget.
- The trace has no artefact versions (summary / embedding version per candidate) and no injection
  confirmation: `returned` and `pool[].returned` mean "assembled into the response to be delivered" — not
  delivered, not injected into a context; the plugins (stage 4) report what they inject. `pool[].selected`
  means chosen for the rerank set; `rerank_skipped` is why rerank scores were not used and does not say whether
  the request was sent (an endpoint error after sending also sets it).
- Regex `mem_grep` joins every readable record's text and relies on the 5 s statement timeout; its cost at
  real corpus size is not measured.
