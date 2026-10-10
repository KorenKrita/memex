# eval — phase 0 measurements (DESIGN §8, §9, §11)

Scripts for the phase-0 checks. Real session text never enters the repo: samples, model outputs and
probe logs live under `/tmp/memex-eval/` (mode 0600). Model calls use the LLM, embedding and rerank the
user's OpenViking server is configured with; `ov.conf` is piped in over ssh and keys stay in memory.

| Script | What |
|---|---|
| `sample-spans.ts` | Deterministic span sample through `dryRun()` (copies, fork parents, snapshot checks), per-record digests and the §4.3 `<context>` |
| `span-trial.py` | §9 step 2: span summaries per §4.3 + embeddings; `--self-test` checks rendering and validation offline |
| `sample-lexical.ts` | Lexical-index sample through `dryRun()`'s lexical hook: whole files, raw record text in ≤ 2 KB chunks |
| `fts-compare.ts` | `pg_trgm` vs ParadeDB `pg_search` on that sample with a fixed query file |
| `probes/pi-persist-probe.ts` | §11 item 2: Pi lifecycle events vs what is in the session file |
| `probes/codex-hook-probe.sh`, `probes/codex-appserver-probe.ts`, `probes/codex-tui-probe.py` | §11 item 1: Codex hooks per entry point, rollout state at each hook |

## Model endpoints (2026-10-09, read from the OV server config, not changed)

LLM `gpt-6-sol` (temperature 0, `reasoning_effort: none`), embedding `text-embedding-v4` (1024 dims), rerank
`Qwen3-Reranker-0dot6B` (OV reaches it through a shim that only renames `data` → `results`). A one-call
synthetic check returned 200 on all three. LLM and embedding responses carry token usage; rerank returns
none. **Prices are not known here**, so this reports usage, not cost.

## 50-span trial (`/tmp/memex-eval/trial-v2.jsonl`)

Sample: the first 50 of a 400-span sample (seed `memex-eval-1`, code `b0e572f+dirty` = 714b3e7) from all
local Pi and Codex logs; mean input 9,837 B, mean `<context>` 638 B.

- 50/50 valid summaries; 51 calls (one retry: a `verified` item cited a non-tool record), no HTTP errors.
- Tokens: prompt 183,630 (mean 3,601 per call), completion 16,785 (mean 329). Embedding of the 50
  summaries: 4,172 tokens.
- Latency per call p50 3.8 s, max 6.0 s; 4 workers: 56 s for 50 spans.
- Summary size p50 336 B, max 504 B (limit 600 B, no length retry needed).
- `anchors`: 257, of which 5 are not substrings of the input (e.g. `revision=868` built from a JSON field)
  and 4 appear only in `<context>`.
- `verified`: 122 items in 46 spans. A hand check of 12 random items against the cited record: 11
  supported; 1 over-attributed (the record says "experiment ports closed", the port numbers came from other
  records of the span). A record id that exists is not proof; this check is small.
- Not measured: summary quality against a reference, the effect of `<context>`, retries at scale.

## Host probes (synthetic prompts, isolated session dirs / `CODEX_HOME`)

- **Pi 1.1.0**: the session file appears at the first assistant `message_end`; at every `turn_end` (plain
  and tool turns) the whole in-memory branch is already readable in the file; at `message_end` of an
  assistant message that message is not yet in the file. Readability only, not crash durability.
- **Codex 0.162.0-alpha.2**, `codex exec` and `codex app-server` (stdio): SessionStart, UserPromptSubmit,
  PreToolUse, PostToolUse, Stop and SessionEnd all fire; `transcript_path` exists from SessionStart; at
  Stop the rollout already holds the assistant message and token count (only `task_complete` follows).
  Hooks run only when trusted (`[hooks.state."<key>"] trusted_hash`, from `hooks/list`) or with
  `--dangerously-bypass-hook-trust`.
- **Codex interactive TUI** (`probes/codex-tui-probe.py`, pseudo-terminal; one plain and one tool turn, both
  exit 0): the TUI attaches to a shared app-server daemon under `CODEX_HOME`, and hooks run with the
  daemon's start-up environment (an earlier run's hooks wrote to a previous run's log until the daemon was
  stopped). Seconds after submit — plain: SessionStart 1.3, UserPromptSubmit 1.4, Stop 6.3, TUI exit 9.3,
  SessionEnd 69.1; tool: SessionStart 1.6, PreToolUse 4.7, PostToolUse 4.9, Stop 8.0, exit 10.9, SessionEnd
  70.7. At Stop the assistant message is in the rollout. SessionEnd came ~60 s after the TUI exited, before
  the daemon was stopped; why it waits is not established. A hook-based exporter must not depend on
  SessionEnd or on the shell's environment.
- Codex cold-file compression: the binary has it, but feature `local_thread_store_compression` is off
  ("under development") in this build; source (b12ac84) compresses rollouts older than 7 days when on.
- **Codex MCP identity** (`probes/codex-mcp-meta-probe.ts`, 2026-10-10, `codex exec` ×2 in parallel, isolated
  `CODEX_HOME`): every `tools/call` carries `_meta` with `threadId`, `sessionId` (both equal to that run's
  `session_meta.payload.id`), `callId`, `itemId`, `windowId`, `progressToken`, `x-codex-turn-metadata`; the two
  runs each got their own id (one stdio server process per run). `CODEX_THREAD_ID` / `CODEX_SESSION_ID` are
  not in the MCP server's environment. Verified for separate `codex exec` processes only (each its own MCP
  server); a shared app-server serving several threads through one MCP process is not covered. In this
  isolated run (`approval_policy = "never"`) the tool call was refused until the probe server set
  `default_tools_approval_mode = "approve"`.
- **Pi `context` ↔ entries** (`probes/pi-context-map-probe.ts`, Pi 1.1.0, a two-turn print session, both
  prompts the same text): the `context` messages exclude the system message, the projection's include it
  (positions do not line up). Each user message matched exactly one projection entry by (role, timestamp,
  content) — its `message` entry id — including the two same-text inputs (different timestamps). Same
  timestamps, tree moves, compaction and other extensions' edits were not covered by this run.
- **Pi edits and sources** (`probes/pi-context-edit-probe.ts` loaded before the map probe, same session shape;
  logs `/tmp/memex-eval/pi-edit.hQUi`): a message sent by an extension (`sendUserMessage`, follow-up) fires
  `input` with `source: "extension"` (the typed one `interactive`) and no second `before_agent_start`; when an
  earlier extension appends text to the latest user message in `context`, the content match finds no entry
  (`matches: []`) while the message's `timestamp` is unchanged. So the plugin ties a `context` user message to
  its entry by (role, timestamp) unique on both sides, and omits injection otherwise. At the first `context` of
  a run the leaf is the new user entry.

## FTS comparison (`/tmp/memex-eval/fts-v4.json`)

Setup: PostgreSQL 18.6, `pg_search` 0.26.0, `pg_trgm` 1.6 (image
`paradedb/paradedb@sha256:52fc9c95…a217ac`, tmpfs, laptop). Corpus: every lexical record of 150 whole files
(seed `memex-fts-1`) = 44,327 chunks, 54 MB, including tool output, thinking and context text. Queries: 80,
fixed in a file (sha256 `c4b1afe2…`), picked by hand from strings that occur in the corpus — Chinese
words (19), paths (10), identifiers (14), error strings (8), identifier fragments (7), two-term AND (8),
typos of corpus identifiers (8), absent look-alikes (6). Relevance is substring containment (typos: of the
intended spelling), which is complete over this corpus but favours `trgm-ilike` by definition.
R-cap@10 = relevant in top 10 / min(relevant, 10), a capped recall (not standard precision or recall).

| class: hit@10 / R-cap@10 | trgm-ilike | trgm-fuzzy `<%` | jieba `&&&` | source_code `&&&` | ngram(2,3) `&&&` | jieba phrase `###` |
|---|---|---|---|---|---|---|
| zh (19) | 19 / 100% | 18 / 79% | 17 / 69% | 13 / 44% | 19 / 100% | 19 / 99% |
| path (10) | 10 / 100% | 10 / 98% | 10 / 90% | 10 / 87% | 10 / 97% | 10 / 100% |
| ident (14) | 14 / 100% | 14 / 91% | 11 / 71% | 10 / 53% | 14 / 95% | 14 / 97% |
| error (8) | 8 / 100% | 8 / 91% | 8 / 90% | 7 / 69% | 8 / 91% | 8 / 100% |
| ident-part (7) | 7 / 100% | 7 / 86% | 3 / 37% | 5 / 39% | 7 / 94% | 3 / 43% |
| terms AND (8) | 8 / 100% | 7 / 71% | 8 / 95% | 6 / 61% | 8 / 100% | 8 / 100% |
| typo (8) | 0 | **8 / 100%** | 0 | 0 | 0 | 0 |
| neg (6): returned anything | 0 | 1 | 1 | 0 | 0 | 0 |
| range of per-class p50 | 1.1–18 ms | 12–1,106 ms | 1.8–4.3 ms | 1.7–3.2 ms | 2.3–14 ms | 1.5–4.5 ms |

- Build on 54 MB: trigram GIN 3.4 s, 69 MB; bm25 (three tokenizers) 4.2 s, 220 MB; heap 33 MB.
- `trgm-ilike` p95 reaches 850 ms on two-character Chinese words: a pattern under three characters has no
  trigram, so the planner scans the table (44k rows, 360 ms for `罗盘`). Three or more characters use the
  index (`蛤蟆街`: 0.4 ms).
- `<%` is slow because the index condition is loose: for `receipt_rejected` the index returns 3,147 rows and
  the `word_similarity` recheck over whole 2 KB chunks keeps 608 (713 ms). Among the methods tested it was
  the only one to find the 8 typos. Returned results that are not literal matches: `<%` returned
  `finish_mcp_startup…` for the absent `finish_mcp_shutdown_after_lag` (similarity 0.6; arguably wanted for
  a fuzzy search), `jieba` AND returned `蛤蟆街` for the absent `蛤蟆桥` (the tokenizer splits `蛤蟆`).
- `ngram(2,3)` matched Chinese, paths, identifier fragments and AND queries close to substring search at
  2–14 ms; it rejects phrase queries (`###`) in 0.26. `jieba` phrase search is exact for whole words but
  misses identifier fragments.
- Limits: one laptop, one 54 MB sample (the whole local lexical text is 782 MB), queries chosen by one
  person from the corpus itself, no ranking judgement beyond "contains the string", no concurrent load.

## Question bank (DESIGN §8; `qbank/`)

The bank itself is real session text and stays local (`/tmp/memex-eval/qb/`, mode 0700). The repo holds
the tools, the format and a synthetic fixture.

- `qbank/freeze.py OUTDIR [--exclude-file LIST]` — APFS-clones `~/.pi/agent/sessions` and
  `~/.codex/{sessions,archived_sessions}` file by file into one frozen corpus (directories 0700, files
  0600) and hashes every file into `manifest.json`. `LIST` holds corpus-relative paths (`pi/…`, `codex/…`,
  `codex-archived/…`) of sessions that contain the bank itself; they are skipped before copying, nothing is
  deleted, and an entry that matches no source file stops the freeze.
- `qbank/indexable.ts CORPUS` — the files the reference dry-run (`dryrun/dryrun.ts`) indexes.
- `qbank/build.py OUTDIR drafts.jsonl...` — validates hand-written drafts against the frozen corpus and
  writes the two views. Refuses an existing `OUTDIR`.
- `qbank/span-distance.ts OUTDIR` — correction distance on `dryRun()` spans of each question's cut view
  (below); exits 1 when a stated type disagrees or a record maps to no span.
- `qbank/selftest.sh` — runs `build.py` and `span-distance.ts` on `qbank/fixture/` (synthetic, regenerated
  by `fixture/make.py`). Accepted builds: exact public schema, visible map, cut views and 0700/0600 modes;
  session copies that only diverge after the cutoff; seven correction distances (same span in Pi and across
  a Codex hook-context message, adjacent self-correction, next exchange with a sibling-branch user turn in
  between in file order and with a Codex user message starting with `<`, later turn, other session); an
  empty-history question with an empty visible map; a torn multibyte write after the cutoff. Must fail:
  an old claim on a sibling branch, a nonadjacent self-correction under any label, and 15 build cases
  (each for its stated reason, without printing session text).

**Draft fields** (one JSON object per line): `id`, `task_cluster`, `split` (`tune`/`test`), `type`,
`needs_history`, `origin` (`derived` from a real session or `synthetic`), `question`, `current_context`
(what the user's current turn shows; for no-history questions this holds everything needed),
`cutoff` (ISO time), `answer_points`, `forbidden`, `evidence` (`file`, `entry`, `quote`, `use` =
`answer`/`old-claim`/`correction`/`source-only`), `check_status` (`checked` or `checked-with-caveats`),
optional `fact_group` (list) and `notes`. Pi `entry` = record id; Codex `entry` = `str(ordinal)` of a
`response_item` (message, tool call or tool output).

**Visible set of a question** (what the system under test may index for it) is built from the corpus as it
stood at the cutoff: every frozen file is cut to the line prefix before its first record with
`timestamp >= cutoff` (file order decides ties and clocks that go backwards; a Pi prefix holds every branch
written before the cutoff), Pi `<session>/<id>/run-N/` subagent runs are left out, and the result is written
to `public/views/<cutoff>/` (whole files as APFS clones, only files that span the cutoff rewritten). The
reference dry-run then decides indexability on that view — Codex legacy (non-paginated), subagent and
guardian threads (`thread_source`, else `source`; DESIGN §4.1 keeps only the parent link), compressed files,
Pi `subagent-artifacts/*_transcript` and conflicting Pi copies are out — so data written after the cutoff
cannot change what was indexable before it. In the current corpus this is 1,515 files at the latest cutoff.
The views are what a replay must index; that a downstream replay reads only them is not verified yet.

**Checks** (explicit errors, not `assert`): unique ids; every cited file matches the frozen manifest; each
`quote` is a substring of the cited record; history evidence lies inside the visible prefix (by line, not
only by time) of a file indexable at that cutoff; no `task_cluster`, no `fact_group` and no session file
(any evidence use) appears in both splits; correction types carry `old-claim` and `correction` evidence,
with the correction later in the same file or in another file (`*-other-session`). Semantic overlap between
splits that shares no session must still be labelled by hand with `fact_group`.

**Outputs** (directories 0700, files 0600):
- `public/questions.jsonl` — `id`, `question`, `current_context`, `cutoff`, `visible`;
  `public/visible/<cutoff>.json` — corpus path, manifest hash, cutoff and `{path: [sha256 of the frozen
  file, visible_lines]}`; `public/views/<cutoff>/` — the cut corpus. No answers, no evidence list.
- `grader/grading.jsonl` — everything else; each evidence item gets `sha256`, `line`, `timestamp`, `role`.
  `grader/evidence/<sha256>.jsonl` — copies of the cited files.

On the current corpus a build takes ~14 min (36 cutoffs, one dry-run each); `du` reports ~66 GB for the
views, of which ~0.65 GB are rewritten files and the rest APFS clones sharing blocks with the corpus.

**Correction distance** (`span-distance.ts`, correction types only): the old claim and the first correction
are looked up in the reference indexer's records for the question's view (a record on a prefix shared by
several Pi branches has one span per view). In the same file the checker walks the correction's own path
(Pi `parentId` chain, Codex file order) back to the old claim and counts the indexer's `user` records on
it: shared span = `same-span`; none and every record between in one of the two spans =
`correction-self-adjacent-span` (the agent corrects itself); none but another span between =
`nonadjacent-span`, which always fails; an old claim off the correction's path is an error; exactly one = `correction-next-exchange`; two or more =
`correction-later-turn`; different file = `*-other-session`. On `bank-v8` all 14 correction questions agree
with their type. Limit: adjacency is checked as "every record between belongs to one of the two spans";
for a record on a prefix shared by several Pi branches (several spans) this is weaker than ordered
adjacency on one navigation view.

**Current local bank** (2026-10-09, `bank-v8`, questions sha256 `6820fde8…`, grading `4dc52a34…`): 82
questions, 32 task clusters, 37 evidence files, 36 cutoffs. 57 need history, 25 do not (30%); 62 test /
20 tune; 12 cite Codex rollouts; 79 derived, 3 synthetic; 78 `checked`, 4 `checked-with-caveats`.
Types: no-history 25, rare-detail 12, plan-vs-done 10, correction-next-exchange 9, cross-session-continue
6, abstain 4, same-name 3, stale-state 3, referential-short 3, state-update-other-session 2,
correction-later-turn 2, tool-output-detail 1, branch-version-disambiguation 1,
correction-self-adjacent-span 1. Evidence roles: assistant 106, tool result 68, user 34, tool call 1.

Not yet covered: a correction inside the old claim's own span; a real cross-session correction of a wrong
claim (the two other-session questions are state updates); the "unique raw hit + both summary channels
mis-recall" regression (DESIGN §6.3), which can be built from synthetic text with controlled summary
candidates but is not yet. The current corpus was frozen by the earlier `freeze.py` (exclusion by deleting
its own clone, source modes kept); its manifest is unchanged. Every answer was written and checked by one
model against the cited records; the user has not reviewed the bank yet.
