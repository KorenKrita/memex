# dryrun — phase 0 local statistics (DESIGN §5.4, §10)

`bun dryrun/dryrun.ts [--per-file] <file-or-dir>...` reads the Pi / Codex session logs you name and prints
JSON: files indexed / skipped (with paths and reason), fork resolution, record counts, bytes left out of span
text by reason, exchanges, views, spans (prefix spans and oversized ones separately), unpaired tool calls,
`digest_text` bytes, `lexical_chunks` bytes and chunks, and forest internal nodes. Two size measures:
`digestBytes` counts each record once; `summaryInputBytes` sums every span, so a prefix span P re-reads its
prefix. Neither includes `<context>` neighbours, internal-node summary input or retries, and spans plus
forest nodes are not a call count (cold sessions build internal nodes on demand, DESIGN §4.4). It reads only the given paths, writes nothing,
opens no network connection and calls no model. There are no default directories.

Rules implemented (each pinned by a test in `dryrun.test.ts`, all on synthetic logs; each rule was also
broken on purpose: a mutation counts only if it compiles, changes the targeted behaviour, turns a named
test red and the suite is green again once restored; latest log `/tmp/memex-review/mutate-dryrun3-115235.log`):

- Input boundary: only regular files under the given paths; symlinks are listed in `symlinksSkipped` and
  never followed; overlapping inputs are deduplicated. Roots resolve to their real path; files are opened
  with `O_NOFOLLOW` and must still be the inode `collect()` saw (passed explicitly, no module state).
  **Not closed:** `O_NOFOLLOW` guards only the last path component; a parent directory swapped for a link
  between its check and `readdir`/`open` can still redirect reads (Node has no openat-style traversal). Missing or unlistable paths go to
  `discoveryErrors` and the rest still runs. A Pi fork's parent is read only if it is itself one of
  the inputs. A file that fails to read or parse is reported as `unreadable` and the batch continues; a cut
  last line is `tornLines`, other bad lines `malformedLines`.
- Pi session v3 only. **All branches are counted** (retrieval covers every branch, DESIGN §4.3). Each
  root-to-tip path is a view; views are partitioned in creation order (first child first), as DESIGN's
  online rule would: a later view reuses every earlier span lying whole on its path; where it forks inside
  an earlier closed span S it gets a prefix span P (cut back to whole units) and then its own spans, and S
  stays for its view. A fork on a span boundary shares directly. Records are counted once however many
  views hold them. Children holding only labels, state or extension context do not make a branch. Forest
  internal nodes: binary carries over each view's span list, identical (left, right) pairs one node
  (DESIGN §4.4). The inferred tail (last entry) is reported only, as `offTail`.
  `subagent-artifacts/*_transcript.jsonl` is skipped without being read, also by the fork pre-pass. A fork
  inherits an entry only when the parent has the same id and content (key-order-independent hash); with
  the parent not given, every entry is `unresolved` (not new evidence).
- Codex Paginated only: `history_mode` other than `paginated` (serde default is Legacy) or any line without
  `ordinal` skips the file. Child threads follow DESIGN §4.1 (`thread_source`, then `source`). Fork boundary
  uses Codex's own rule; an unknown boundary quarantines every record (`unresolved`). Message content is
  classified per fragment using `content_item_kinds` by position (`user.*` = user, e.g. `user.goal`; missing
  positions are unknown);
  unannotated user fragments fall back to Codex's context start markers. `event_msg` repeats response items.
- Removed from all derived text (`excluded`, records and bytes per reason): memex's own injections (Pi
  `sections.memex`, `memex-*` custom messages, Codex `hooks.additional_context` starting with `<memex`,
  `<memex…>` blocks), other memory systems (`<openviking-context>`, `<contextdb-auto-recall>` blocks,
  `memini-*` / `openviking*` / `contextdb*` custom messages), memory tool results (`mem_search/zoom/grep`,
  `openviking_*`; Codex matched to the call by `call_id`, including calls before a fork boundary; the call
  itself stays, the result adds no text), host compaction / branch summaries. Counts are per record: a
  record with several blocks of one kind counts once.
- Kept in L0 and the lexical index but not summary input (`digestExcluded`): model thinking / reasoning
  (memory blocks stripped), harness and extension context (system `content` plus non-memex sections, developer messages, Codex context fragments,
  other Pi custom messages, whose `customType` names are listed in `extensionTypes`). These never open an
  exchange.
- Exchange = a real user message and what follows on the same branch. Spans cut only between units and
  never cross a user message; a unit is a record, or a tool call plus everything up to the last result of
  the calls it opened (commentary, parallel calls and out-of-order results included). A call still waiting
  at the next user message ends there and is counted in `unpairedCalls`. A memory result keeps its call
  company but adds no bytes and never opens a span. `SPAN_MAX` (12 KB) wins over the 4 KB target: only a
  single unit larger than 12 KB makes an `oversizedSpans` entry.
- `digest_text` keeps tool output head and tail (1 KB each), up to 20 error lines and a locator;
  `lexical_chunks` count all kept text in 2 KB chunks. All sizes are UTF-8 bytes.

Not done here: redaction, canonical mapping across Codex forks (counts only), rollback replay, `.zst` files
(reported as skipped), cost estimation (§9 needs a sample run first). Thresholds are DESIGN's [待标定] values.

Exports: `dryRun`, `analyzeFile`, `collect` (and `FileResult`, `AnalyzeOptions`); `codexIsChild` /
`codexForkBoundary` are exported only so tests can check Codex's rules case by case.

Running it on real logs is a separate step: it only reads, but point it at a copy or a chosen subset first.
