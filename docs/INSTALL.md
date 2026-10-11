# Installing memex

Three parts: the server (one per user, remote), the Pi extension and the Codex plugin (one per machine and
host). Nothing is stored on the client: no queue, no cursor, no database. Until the policy allows a session,
nothing of it leaves the machine.

Tested with: Pi 1.1.0, codex-cli 0.162.0-alpha.2, Bun 1.4.2, Postgres 17 with pgvector and pg_trgm.

## 1. Server

Postgres 17 with the `vector` and `pg_trgm` extensions, then:

```sh
MEMEX_PG_URL=postgres://… MEMEX_HOST=127.0.0.1 MEMEX_PORT=8787 \
MEMEX_LLM_URL=…/v1 MEMEX_LLM_MODEL=… MEMEX_LLM_KEY=… \
MEMEX_EMBED_URL=…/v1 MEMEX_EMBED_MODEL=… MEMEX_EMBED_KEY=… \
MEMEX_RERANK_URL=…/rerank MEMEX_RERANK_MODEL=… MEMEX_RERANK_KEY=… \
bun server/main.ts
```

`main.ts` migrates the schema, then serves the API and runs one worker loop. It listens on loopback by
default; put TLS in front (a reverse proxy) before exposing it.

Per-turn recall injects nothing until `MEMEX_RECALL_TAU` (the rerank score bar) is set: it is not calibrated
yet (DESIGN §6.2), so the default is "not ready", and the plugins show nothing.

Administration runs on the server host against the database; there is no HTTP route for it:

```sh
bun server/admin.ts key add laptop                          # prints the key once
bun server/admin.ts grant allow personal sync memex         # purposes: sync snapshot recall tool derive
bun server/admin.ts grant allow personal derive llm         # destinations: memex llm embedding rerank
bun server/admin.ts grant allow personal derive embedding
bun server/admin.ts grant allow personal snapshot memex
bun server/admin.ts grant allow personal recall memex
bun server/admin.ts grant allow personal recall embedding
bun server/admin.ts grant allow personal recall rerank
bun server/admin.ts grant allow personal tool memex
bun server/admin.ts grant list
```

Grants are the server's own authorization. The client policy can only narrow them. A revoke withdraws what
was derived or served under the grant.

## 2. Client settings (both hosts)

**Server**: `MEMEX_URL` and `MEMEX_KEY` when both are set in the environment; otherwise the file
`~/.config/memex/client.json` (another path: `MEMEX_CLIENT`):

```json
{ "url": "https://memex.example", "key": "…" }
```

Use the file for Codex: its hooks and MCP server run with the Codex process's environment, not your shell's.

**Policy**: `~/.config/memex/policy.json` (another path: `MEMEX_POLICY`). It starts empty: no file means no
scope, and nothing is sent. A session belongs to the first scope whose `paths` contain its log file:

```json
{
  "scopes": [
    {
      "name": "personal",
      "paths": ["/Users/me/.pi/agent/sessions", "/Users/me/.codex/sessions"],
      "allow": {
        "sync": ["memex"],
        "snapshot": ["memex"],
        "recall": ["memex", "embedding", "rerank"],
        "tool": ["memex", "embedding", "rerank"]
      }
    }
  ]
}
```

Each purpose is checked on its own. A scope can allow queries (`snapshot`, `recall`, `tool`) without `sync`.
Then nothing of its sessions is uploaded, but they can read what other scopes uploaded. `allow.import`
(`["memex"]`, on top of `sync`) lets `bun client/cli.ts import` upload a scope's history.

## 3. Pi

```sh
pi -e /path/to/memex/plugins/pi/index.ts                    # one run
```

To load it in every session, add the path to `extensions` in `~/.pi/agent/settings.json` (Pi docs:
settings.md, Resources). In a session, `/memex` shows where the server settings come from, the session's
scope and what it allows, and the last sync, recall and scan.

What it does:
- Syncs the session file on `turn_end`, `session_tree`, `session_compact` and `session_shutdown`.
- Scans this machine's authorized Pi logs in the background to reconcile missed syncs: 2 s after start
  (`MEMEX_SCAN_DELAY_MS`; negative turns it off), then again after a turn at most every 10 minutes
  (`MEMEX_SCAN_EVERY_MS`). Each run works through the server-side round within its budget (60 s), trying every
  pending file once; what fails or does not fit is left for the next run. The server opens a new round only
  once its interval (`MEMEX_SCAN_INTERVAL_MS`, 1 h) has passed. The Codex plugin runs the same at
  `SessionStart` and `Stop`.
- Puts the start-of-session snapshot in the system prompt.
- Recalls once per user input. The recall goes on that message in what the model sees (never into the
  session file) and is replayed as frozen on later turns.
- Provides the `mem_search`, `mem_zoom` and `mem_grep` tools.

## 4. Codex

Build the plugin into a self-contained directory and a local marketplace. Codex copies the plugin into its
cache, so the build must not point back into this repo:

```sh
bun plugins/codex/build.ts                    # → dist/codex-marketplace (or give another directory)
codex plugin marketplace add dist/codex-marketplace
codex plugin add memex@memex-local
```

Then two steps only you can take:

1. **Let the tools run without asking** (they only read memex). In `~/.codex/config.toml`:
   ```toml
   [plugins."memex@memex-local".mcp_servers.memex]
   default_tools_approval_mode = "approve"
   ```
2. **Trust the hooks**: in the Codex CLI, open `/hooks` and trust memex's hooks. Codex skips plugin hooks
   until you do, and then nothing syncs and nothing is injected. The MCP tools still work.

The hook command uses the Bun binary that ran the build, by absolute path. Rebuild and reinstall the plugin
after moving or upgrading Bun.

What it does:
- `SessionStart`: syncs, then adds the snapshot as context. A reconciliation run starts in its own detached
  process, so it does not hold up the start; `Stop` starts one too (the server decides whether a round is due).
- `UserPromptSubmit`: syncs the previous exchange, then adds the recall for this prompt. Codex writes it into
  the rollout and replays it itself.
- `Stop`, `PreCompact`, `SessionEnd`: sync. `SessionEnd` does not run reliably in Codex; reconciliation
  catches up.
- MCP server: `mem_search`, `mem_zoom` and `mem_grep` as the thread Codex names in each call
  (`_meta.threadId`), checked against that thread's rollout under `CODEX_HOME/sessions`.

## 5. Known limits

- Synthetic data only so far; the recall bar τ and the other `[待标定]` values in DESIGN are not calibrated.
- Codex Legacy rollouts and Codex subagent/guardian threads are not indexed.
- Codex fork, first turn: the query does not carry the exchange inherited from the parent (DESIGN §6.2).
- Recall budget: a decision the server saved just within its deadline, after the plugin stopped waiting,
  is not shown in that process but may be shown by a later process's replay (DESIGN §6.2).
- Pi per-turn recall runs only when the turn is unambiguous: one input since the agent was last idle, typed
  (not an extension's, not a `/command`, not steering or a follow-up), and the new message's text exactly
  that input. Turns with a second input, or another extension handling or transforming input, get no recall.
  Pi does not tie an input to the message it makes; an extension that delays an input past the end of the run,
  followed by the user typing the same text, can still be taken for the user's (DESIGN §6.2).
- Codex hook: reading the rollout is not bounded by the budget; on a very large file the hook can take longer
  than `MEMEX_CODEX_RECALL_MS`, but it makes no recall request after the budget.
- Not verified in a real host yet: Pi recall after `/tree` navigation or compaction; Codex with one
  app-server serving several threads (MCP identity per call); the first turn of a Codex fork.
- Uninstall: remove the extension path (Pi), `codex plugin remove memex@memex-local` (Codex). This stops new
  uploads only. What was uploaded stays on the server until forgotten (`POST /v1/forget { source }` per
  session, or drop the database).
