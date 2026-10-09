# dryrun — phase 0 local statistics (DESIGN §5.4, §10)

`bun dryrun/dryrun.ts [--per-file] <file-or-dir>...` reads the Pi / Codex session logs you name and prints
JSON: files indexed / skipped (with paths and reason), fork resolution, record counts, bytes left out of span
text by reason, exchanges, spans, `digest_text` bytes, `lexical_chunks` bytes and chunks, and the forest
internal node count (`spans - popcount(spans)` per file). It reads only the given paths, writes nothing,
opens no network connection and calls no model. There are no default directories.

Rules implemented (each pinned by a test in `dryrun.test.ts`, all on synthetic logs; each rule was also
broken on purpose and the suite went red, log `/tmp/memex-review/mutate-dryrun-111432.log`):

- Input boundary: only regular files under the given paths; symlinks are listed in `symlinksSkipped` and
  never followed; overlapping inputs are deduplicated. A Pi fork's parent is read only if it is itself one of
  the inputs. A file that fails to read or parse is reported as `unreadable` and the batch continues; a cut
  last line is `tornLines`, other bad lines `malformedLines`.
- Pi session v3 only. **All branches are counted** (retrieval covers every branch, DESIGN §4.3): the tree is
  cut into segments at branch points and at user messages, so a shared prefix is counted once and two
  branches never share a span. The inferred tail (last entry) is reported only, as `offTail`.
  `subagent-artifacts/*_transcript.jsonl` is skipped without being read, also by the fork pre-pass. A fork
  inherits an entry only when the parent has the same id and content (key-order-independent hash); with
  the parent not given, every entry is `unresolved` (not new evidence).
- Codex Paginated only: `history_mode` other than `paginated` (serde default is Legacy) or any line without
  `ordinal` skips the file. Child threads follow DESIGN §4.1 (`thread_source`, then `source`). Fork boundary
  uses Codex's own rule; an unknown boundary quarantines every record (`unresolved`). Message content is
  classified per fragment using the aligned `content_item_kinds` (`user.*` = user, e.g. `user.goal`);
  unannotated user fragments fall back to Codex's context start markers. `event_msg` repeats response items.
- Removed from all derived text (`excluded`, records and bytes per reason): memex's own injections (Pi
  `sections.memex`, `memex-*` custom messages, Codex `hooks.additional_context` starting with `<memex`,
  `<memex…>` blocks), other memory systems (`<openviking-context>`, `<contextdb-auto-recall>` blocks,
  `memini-*` / `openviking*` / `contextdb*` custom messages), memory tool results (`mem_search/zoom/grep`,
  `openviking_*`; Codex matched to the call by `call_id`; the call itself stays), host compaction / branch
  summaries.
- Kept in L0 and the lexical index but not summary input (`digestExcluded`): model thinking / reasoning,
  harness and extension context (rest of the system prompt, developer messages, Codex context fragments,
  other Pi custom messages, whose `customType` names are listed in `extensionTypes`). These never open an
  exchange.
- Exchange = a real user message and what follows on the same branch. Spans cut only between units; a unit
  is a record, or a tool call plus everything up to the last result of the calls it opened (commentary,
  parallel calls and out-of-order results included). `SPAN_MAX` (12 KB) wins over the 4 KB target: only a
  single unit larger than 12 KB makes an `oversizedSpans` entry.
- `digest_text` keeps tool output head and tail (1 KB each), up to 20 error lines and a locator;
  `lexical_chunks` count all kept text in 2 KB chunks. All sizes are UTF-8 bytes.

Not done here: redaction, canonical mapping across Codex forks (counts only), rollback replay, `.zst` files
(reported as skipped), cost estimation (§9 needs a sample run first). Thresholds are DESIGN's [待标定] values.

Running it on real logs is a separate step: it only reads, but point it at a copy or a chosen subset first.
