# dryrun — phase 0 local statistics (DESIGN §5.4, §10)

`bun dryrun/dryrun.ts [--per-file] <file-or-dir>...` reads the Pi / Codex session logs you name and prints
JSON: files indexed / skipped (with paths and reason), fork resolution, record counts, bytes left out of span
text by reason, exchanges, spans, `digest_text` bytes, `lexical_chunks` bytes and chunks, and the forest
internal node count (`spans - popcount(spans)` per file). It reads only the given paths, writes nothing,
opens no network connection and calls no model. There are no default directories.

Rules implemented (each pinned by a test in `dryrun.test.ts`, all on synthetic logs):

- Pi session v3 only. The view is the inferred tail (last entry, Pi's resume rule), labelled
  `inferred-tail`; entries off that path are counted, not mixed in. `subagent-artifacts/*_transcript.jsonl`
  is skipped without being read. A fork (`parentSession`) inherits an entry only when the parent file is in
  the same run and has the same id and content; otherwise the fork is reported `parent-not-given`.
- Codex Paginated only: `history_mode` other than `paginated` (serde default is Legacy) or any line without
  `ordinal` skips the whole file. Child threads follow DESIGN §4.1 (`thread_source`, then `source`). Fork
  boundary uses Codex's own rule (`forked_from_ordinal_exclusive`, else `history_base` only when it names the
  parent or the file is the original rollout); an unknown boundary quarantines every record (`unresolved`).
  Only `response_item` and inter-agent lines carry content (`event_msg` repeats them).
- Out of span text: thinking / reasoning; memex's own injections (Pi `sections` / `memex-*` custom messages,
  Codex `hooks.additional_context` starting with `<memex`); other memory systems (`<openviking-context>`,
  `<contextdb-auto-recall>`); Codex harness context (user messages whose `content_item_kinds` are all
  non-`user.*`, or that start with a Codex context marker); Pi compaction / branch summaries.
- Exchange = a real user message and everything after it up to the next one. Spans cut only between units
  (an assistant text, or a run of tool calls with their results), targeting 4–12 KB of digest; a single unit
  over 12 KB stays whole and is counted in `oversizedSpans`.
- `digest_text` keeps tool output head and tail (1 KB each), up to 20 error lines and a locator;
  `lexical_chunks` count all kept text in 2 KB chunks. All sizes are UTF-8 bytes.

Not done here: redaction, canonical mapping across Codex forks (counts only), rollback replay, `.zst` files
(reported as skipped), cost estimation (§9 needs a sample run first). Thresholds are DESIGN's [待标定] values.

Running it on real logs is a separate step: it only reads, but point it at a copy or a chosen subset first.
