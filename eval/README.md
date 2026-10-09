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

## FTS comparison (`/tmp/memex-eval/fts-v2.json`)

Setup: PostgreSQL 18.6, `pg_search` 0.26.0, `pg_trgm` 1.6 (image
`paradedb/paradedb@sha256:52fc9c95…a217ac`, tmpfs, laptop). Corpus: every lexical record of 150 whole files
(seed `memex-fts-1`) = 44,327 chunks, 54 MB, including tool output, thinking and context text. Queries: 80,
fixed in a file (sha256 `c4b1afe2…`), picked by hand from strings that occur in the corpus — Chinese
words (19), paths (10), identifiers (14), error strings (8), identifier fragments (7), two-term AND (8),
typos of corpus identifiers (8), absent look-alikes (6). Relevance is substring containment (typos: of the
intended spelling), which is complete over this corpus but favours `trgm-ilike` by definition. P@10 =
relevant in top 10 / min(relevant, 10).

| class | trgm-ilike | trgm-fuzzy `<%` | jieba `&&&` | source_code `&&&` | ngram(2,3) `&&&` | jieba phrase `###` |
|---|---|---|---|---|---|---|
| zh (19) hit / P@10 | 19 / 100% | 18 / 79% | 17 / 69% | 13 / 44% | 19 / 100% | 19 / 99% |
| path (10) | 10 / 100% | 10 / 98% | 10 / 90% | 10 / 87% | 10 / 97% | 10 / 100% |
| ident (14) | 14 / 100% | 14 / 91% | 11 / 71% | 10 / 53% | 14 / 95% | 14 / 97% |
| error (8) | 8 / 100% | 8 / 91% | 8 / 90% | 7 / 69% | 8 / 91% | 8 / 100% |
| ident-part (7) | 7 / 100% | 7 / 86% | 3 / 37% | 5 / 39% | 7 / 94% | 3 / 43% |
| terms AND (8) | 8 / 100% | 7 / 71% | 8 / 95% | 6 / 61% | 8 / 100% | 8 / 100% |
| typo (8) | 0 | **8 / 100%** | 0 | 0 | 0 | 0 |
| neg (6) false hits | 0 | 1 | 1 | 0 | 0 | 0 |
| latency p50 (all) | 1–18 ms | 150–1,070 ms | 2–3 ms | 1–3 ms | 6–12 ms | 2–4 ms |

- Build on 54 MB: trigram GIN 3.1 s, 69 MB; bm25 (three tokenizers) 3.5 s, 231 MB; heap 33 MB.
- `trgm-ilike` p95 reaches 530 ms on two-character Chinese words: a pattern under three characters has no
  trigram, so the planner scans the table (44k rows, 360 ms for `罗盘`). Three or more characters use the
  index (`蛤蟆街`: 0.4 ms).
- `<%` is slow because the index condition is loose: for `receipt_rejected` the index returns 3,147 rows and
  the `word_similarity` recheck over whole 2 KB chunks keeps 608 (713 ms). It is the only method that found
  typos, and it gave one false hit on an absent look-alike identifier.
- `ngram(2,3)` matched Chinese, paths, identifier fragments and AND queries close to substring search at
  6–12 ms; it rejects phrase queries (`###`) in 0.26. `jieba` phrase search is exact for whole words but
  misses identifier fragments; `jieba` AND gave one false hit on an absent Chinese look-alike.
- Limits: one laptop, one 54 MB sample (the whole local lexical text is 782 MB), queries chosen by one
  person from the corpus itself, no ranking judgement beyond "contains the string", no concurrent load.
