# eval — phase 0 measurements (DESIGN §8, §9, §11)

Scripts for the phase-0 checks. Real session text never enters the repo: samples, model outputs and
probe logs live under `/tmp/memex-eval/` (mode 0600). Model calls use the LLM, embedding and rerank the
user's OpenViking server is configured with; `ov.conf` is piped in over ssh and keys stay in memory.

| Script | What |
|---|---|
| `sample-spans.ts` | Deterministic span sample through `dryRun()` (copies, fork parents, snapshot checks), per-record digests and the §4.3 `<context>` |
| `span-trial.py` | §9 step 2: span summaries per §4.3 + embeddings; `--self-test` checks rendering and validation offline |
| `fts-compare.ts` | `pg_trgm` vs ParadeDB `pg_search` (exploratory, see below) |
| `probes/pi-persist-probe.ts` | §11 item 2: Pi lifecycle events vs what is in the session file |
| `probes/codex-hook-probe.sh`, `probes/codex-appserver-probe.ts` | §11 item 1: Codex hooks per entry point, rollout state at each hook |

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
  `--dangerously-bypass-hook-trust`. **Interactive TUI not tested.**
- Codex cold-file compression: the binary has it, but feature `local_thread_store_compression` is off
  ("under development") in this build; source (b12ac84) compresses rollouts older than 7 days when on.

## FTS comparison (exploratory only)

`fts-compare.ts` ran a substring known-item micro-benchmark on 3,000 sampled digest spans: relevant = spans
containing the query string, so `trgm-ilike` scores 100% by construction. It is **not** a basis for choosing:
it used digest text instead of ~2 KB `lexical_chunks` of raw text, auto-generated queries, and no
hand-checked relevance. Observations worth re-testing: pg_search `ngram(2,3)` matched Chinese and paths
almost as well as substring search at ~1 ms; `jieba` missed many Chinese substrings; `source_code` found
identifier fragments; only trigram similarity (`<%`) handled typos, at ~2 s per query (cause not yet
analysed).
