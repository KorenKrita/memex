# dryrun — phase 0 local statistics (DESIGN §5.4, §10)

`bun dryrun/dryrun.ts [--per-file] <file-or-dir>...` reads the Pi / Codex session logs you name and prints
JSON: files indexed / skipped (with paths and reason), fork resolution, record counts, bytes left out of span
text by reason, exchanges, views, spans (prefix spans and oversized ones separately), unpaired tool calls,
`digest_text` bytes, `lexical_chunks` bytes and chunks, and forest internal nodes. Two size measures:
`digestBytes` counts each record once; `summaryInputBytes` sums every span, so a prefix span P re-reads its
prefix. Neither includes `<context>` neighbours, internal-node summary input or retries, and spans plus
forest nodes are not a call count (cold sessions build internal nodes on demand, DESIGN §5.5). It reads
only the given paths, writes nothing,
opens no network connection and calls no model. There are no default directories.

Rules implemented (tested in `dryrun.test.ts` on synthetic logs only). Mutation checks: a mutation counts
only if it compiles, changes the targeted behaviour, turns a named test red (or hangs the suite past its
cap) and the suite is green again once restored; logs `/tmp/memex-review/mutate-dryrun4-132111.log`, `mutate-dryrun5-141336.log`, `mutate-dryrun6-144322.log` and `mutate-dryrun7-151942.log` (there `parentGeq`, a tie taking the parent's view, survived: it is not equivalent, the seventh review found a case; now tested) and `mutate-dryrun8-153025.log`.
Rules without a listed mutation there are tested but not mutation-checked.

- Input boundary: only regular files under the given paths; symlinks are listed in `symlinksSkipped` and
  never followed; overlapping inputs are deduplicated. Roots resolve to their real path; files are opened
  with `O_NOFOLLOW | O_NONBLOCK` (a FIFO swapped in cannot block) and must still be the inode `collect()`
  saw (passed explicitly, no module state). **Not closed:** `O_NOFOLLOW` guards only the last path
  component; a parent directory swapped for a link between its check and `readdir`/`open` can still
  redirect reads (Node has no openat-style traversal). Missing or unlistable paths go to
  `discoveryErrors` and the rest still runs. A Pi fork's parent is read only if it is itself one of
  the inputs. A file that fails to read or parse is reported as `unreadable` and the batch continues; a cut
  last line is `tornLines`, other bad lines `malformedLines`.
- Pi session v3 only. Duplicate ids or a parent cycle make the file `invalid-tree` (not read further). An
  entry whose `parentId` names no entry in the file is a root, as in Pi's own `getTree()`, and is counted
  in `orphanRoots`. **All branches are counted** (retrieval covers every branch,
  DESIGN §4.3). Each root-to-tip path is a view; views are partitioned depth first, children in file
  (= creation) order. A later view's **source** is the view sharing its longest prefix among this file's
  earlier views (first one on a tie) and, for a resolved fork, its **direct parent's** views only (a tie
  takes this file's: the two can cut the shared prefix differently, as unit boundaries depend on the continuation); an older ancestor holding the same records
  is never the source, since the parent's partition already holds what it took from it. It reuses the
  source's spans lying whole on that prefix and ending on one of its own unit boundaries; where it forks
  inside a source span S it gets a prefix span P (cut back to whole units) and then its own spans, and S
  stays for its view. Spans other views made never change this cut. A fork on a span boundary shares
  directly. **Assumption:** spans are taken as finally cut; the logs
  do not say whether a span was still open when a later branch forked from it, so P counts are what
  DESIGN's rule gives for closed spans, not a replay. Records are counted once however many views hold
  them. Children holding only labels, state or extension context do not make a branch. Forest internal
  nodes: binary carries over each view's span list, identical (left, right) pairs one node (DESIGN §4.4);
  a file whose views all have fewer than 4 spans builds none (short session) and leaves them for a longer
  fork to build and count. The inferred tail (last entry) is reported only, as `offTail`.
  `subagent-artifacts/*_transcript.jsonl` is skipped without being opened. A fork inherits an entry only
  when the parent has the same id and content (key-order-independent hash); inherited entries are not new
  evidence but stay on the child's views and keep the canonical identity of the session they are native
  to (header `id`), through any number of descendants. Records new in a file are native to it, so
  byte-identical records new in two sibling forks stay two. A span is its ordered canonical records: an
  identical span (e.g. the same P cut by two forks) is counted once in the family, whichever file meets
  it first; `prefixSpans` counts a span once if any view uses it as a P, whichever role met it first. Only a file whose own ancestry is resolved (or that is no fork) can be a parent: parent not
  given → every entry `unresolved`; parent invalid, itself unresolved, or a fork chain looping back →
  `unresolved` too.
- Pi session identity is the header `id` (DESIGN §4.1); a Pi file without one is `no-session-id` and is
  never a parent (a path is not an identity). Files with the same header id are copies of one session:
  identical copies are analysed once (first path) and the others are `duplicate-copy` with `copyOf`; a fork
  naming either copy resolves to the same one. Copies that differ need a projection merge this tool does
  not do, so the whole group is `conflicting-copies` and forks of it stay `unresolved`, whatever the file
  names. A pre-pass reads each Pi file once (same open checks) and keeps only its id and content hash; the
  analysis must see the same hash, a file rewritten in between is `unreadable`. Codex files are not
  grouped by id: one thread id may legitimately span several rollout files.
  Cost: linear in entries for a linear session (tested for a 4× size ratio; a cheap per-entry copy is
  not caught); a branching session materializes every view, so work is the sum of view lengths plus
  partitioning (a 4000-view synthetic file is tested to finish in a subprocess deadline). Memory is not
  bounded or tested: each file's views and its family's partition are held while it is analysed.
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
