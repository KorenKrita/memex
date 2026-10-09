// memex import --dry-run (phase 0): read explicitly given Pi / Codex session logs, apply the
// DESIGN §4.1–§4.3 rules locally and report how much would be archived, split and summarized.
// Read-only, no network, no model calls, no default directories. Sizes are UTF-8 bytes.
import { createHash } from "node:crypto";
import { closeSync, constants, fstatSync, lstatSync, openSync, readdirSync, readFileSync, realpathSync } from "node:fs";
import { basename, join, resolve } from "node:path";

// [待标定] values from DESIGN §4.3
const SPAN_MIN = 4096; // target only: a span may close short to stay under SPAN_MAX
const SPAN_MAX = 12288;
const TOOL_HEAD = 1024;
const TOOL_TAIL = 1024;
const TOOL_ERROR_LINES = 20;
const LEXICAL_CHUNK = 2048;

const enc = new TextEncoder();
const bytes = (s: string) => enc.encode(s).length;

// Kinds that become span text. `context` (harness / extension context) and `thinking` are kept in L0
// and the lexical index but are not summary input; neither opens an exchange.
type Kind = "user" | "assistant" | "tool_call" | "tool_result" | "agent" | "context" | "thinking";
interface Rec {
  id: string;
  kind: Kind;
  text: string;
  callId?: string;
  memory?: boolean; // memory tool result: keeps call/result pairing, contributes no text or evidence
  key: string; // record identity: native record id + position inside it
}
const DIGEST_KINDS = new Set<Kind>(["user", "assistant", "tool_call", "tool_result", "agent"]);

type Tally = Record<string, { records: number; bytes: number }>;

export interface FileResult {
  path: string;
  harness?: "pi" | "codex";
  status: "indexed" | "skipped";
  reason?: "legacy" | "child-thread" | "unknown-format" | "unsupported-version" | "compressed" | "empty" | "no-ordinal" | "unreadable";
  error?: string;
  forkStatus?: "none" | "resolved" | "parent-not-given" | "unresolved";
  records: number; // L0 lines with a record identity (Pi entries / Codex ordinals)
  evidence: number; // records contributing summary input
  offTail: number; // Pi entries not on the inferred-tail path (still counted: retrieval covers all branches)
  branchPoints: number; // Pi entries with more than one text-bearing child
  views: number; // branch tips (Pi) / 1 (Codex): each is one navigation view
  prefixSpans: number; // DESIGN §4.3 prefix segments P, created where a later branch forks inside a span
  unpairedCalls: number; // tool calls with no result before the next user message or the end
  inherited: number; // fork prefix: canonical lives in the parent
  unresolved: number; // fork prefix with unknown boundary: archived, quarantined
  tornLines: number; // last line cut mid-write
  malformedLines: number; // complete lines that are not JSON objects
  nonContent: Record<string, number>; // records with no text (state, usage, labels, event_msg duplicates …)
  excluded: Tally; // left out of L0-derived text entirely (memory injections, host summaries)
  digestExcluded: Tally; // kept in lexical index, not summary input (thinking, harness/extension context)
  extensionTypes: Record<string, number>; // Pi custom_message customType histogram (names only)
  exchanges: number;
  spans: number;
  oversizedSpans: number; // spans over SPAN_MAX made of one indivisible unit
  digestBytes: number; // unique records' digest text
  summaryInputBytes: number; // sum over all spans (P re-reads its prefix): L1 summary input, without context
  lexicalBytes: number;
  lexicalChunks: number;
  forestInternalNodes: number;
}

function emptyResult(path: string): FileResult {
  return {
    path,
    status: "indexed",
    records: 0,
    evidence: 0,
    offTail: 0,
    branchPoints: 0,
    views: 0,
    prefixSpans: 0,
    unpairedCalls: 0,
    inherited: 0,
    unresolved: 0,
    tornLines: 0,
    malformedLines: 0,
    nonContent: {},
    excluded: {},
    digestExcluded: {},
    extensionTypes: {},
    exchanges: 0,
    spans: 0,
    oversizedSpans: 0,
    digestBytes: 0,
    summaryInputBytes: 0,
    lexicalBytes: 0,
    lexicalChunks: 0,
    forestInternalNodes: 0,
  };
}

function tally(t: Tally, reason: string, n: number, records = 1) {
  const e = (t[reason] ??= { records: 0, bytes: 0 });
  e.records += records;
  e.bytes += n;
}
const inc = (m: Record<string, number>, k: string, n = 1) => (m[k] = (m[k] ?? 0) + n);

// ---------- text helpers ----------

const OTHER_MEMORY_BLOCKS = [/<openviking-context\b[\s\S]*?<\/openviking-context>/g, /<contextdb-auto-recall\b[\s\S]*?<\/contextdb-auto-recall>/g];
const MEMEX_BLOCK = /<memex[-\w]*\b[\s\S]*?<\/memex[-\w]*>/g;
const MEMEX_TOOL = /(^|__)mem_(search|zoom|grep)$/;
const OTHER_MEMORY_TOOL = /(^|__)openviking_|^mcp__openviking__/;
const OTHER_MEMORY_CUSTOM = /^(memini-|openviking|contextdb)/;

// Removes memory-system blocks from retained text. `excluded` is the current record's own tally
// (see perRecord), so a record counts once per reason however many blocks it holds.
function stripMemory(text: string, excluded: Tally): string {
  let out = text;
  const strip = (re: RegExp, reason: string) => {
    let n = 0;
    out = out.replace(re, (m) => ((n += bytes(m)), ""));
    if (n) tally(excluded, reason, n);
  };
  strip(MEMEX_BLOCK, "memex-injected");
  for (const re of OTHER_MEMORY_BLOCKS) strip(re, "other-memory-injected");
  return out.trim();
}

function perRecord<T>(res: FileResult, fn: (ex: Tally) => T): T {
  const ex: Tally = {};
  const out = fn(ex);
  for (const [k, v] of Object.entries(ex)) tally(res.excluded, k, v.bytes, 1);
  return out;
}

function textOf(v: unknown): string {
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(textOf).filter(Boolean).join("\n");
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (o.type === "image" || o.type === "input_image") return "[image]";
    if (typeof o.text === "string") return o.text;
    if ("content" in o) return textOf(o.content);
    if ("output" in o) return textOf(o.output);
  }
  return "";
}

// Deterministic content hash: object keys sorted recursively, array order kept.
function canon(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canon).join(",")}]`;
  if (v && typeof v === "object")
    return `{${Object.keys(v as object)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canon((v as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  return JSON.stringify(v);
}
const contentHash = (v: unknown) => createHash("sha256").update(canon(v)).digest("hex");

function digestOf(r: Rec): string {
  if (r.kind !== "tool_result") return `${r.kind}: ${r.text}`;
  const n = bytes(r.text);
  if (n <= TOOL_HEAD + TOOL_TAIL) return `${r.kind}: ${r.text}`;
  const buf = enc.encode(r.text);
  const dec = new TextDecoder(); // replacement chars at cut points are fine for a size estimate
  const head = dec.decode(buf.slice(0, TOOL_HEAD));
  const tail = dec.decode(buf.slice(n - TOOL_TAIL));
  const errors = dec
    .decode(buf.slice(TOOL_HEAD, n - TOOL_TAIL))
    .split("\n")
    .filter((l) => /error|fail|exception|traceback|panic/i.test(l))
    .slice(0, TOOL_ERROR_LINES)
    .map((l) => l.slice(0, 300));
  return `${r.kind}: ${head}\n‹tool-output record=${r.id} bytes=${n}›\n${errors.join("\n")}\n${tail}`;
}
const digestSize = (r: Rec) => (DIGEST_KINDS.has(r.kind) && !r.memory ? bytes(digestOf(r)) + 1 : 0);

// ---------- units, spans, views ----------

// A unit is never split: one record, or a tool call together with everything up to the last result of
// the calls it opened (parallel calls, out-of-order results, commentary in between). A real user message
// ends any open unit: a call still waiting there is unpaired and stays in its own exchange.
function unitsOf(path: Rec[], unpaired: Set<string>): Rec[][] {
  const out: Rec[][] = [];
  let cur: Rec[] | undefined;
  const open = new Map<string, string>(); // callId → record key
  const closeOpen = () => {
    for (const k of open.values()) unpaired.add(k);
    open.clear();
    cur = undefined;
  };
  for (const r of path) {
    if (r.kind === "user" && cur) closeOpen();
    if (cur) {
      cur.push(r);
      if (r.kind === "tool_call" && r.callId) open.set(r.callId, r.key);
      if (r.kind === "tool_result" && r.callId) open.delete(r.callId);
      if (open.size === 0) cur = undefined;
      continue;
    }
    if (r.kind === "tool_call" && r.callId) {
      cur = [r];
      open.set(r.callId, r.key);
      out.push(cur);
    } else out.push([r]);
  }
  closeOpen();
  return out;
}

type Span = { id: number; keys: string[]; size: number };

// Partition every view into spans, the way DESIGN §4.3 does it online: views are taken in creation order
// (first child first at every branch point); a later view reuses every earlier span lying whole on its
// path, and where it forks inside an earlier span it gets a prefix span P (cut back to a whole unit, so a
// call never leaves its result) followed by its own spans. Spans never cross a user message.
function partition(views: Rec[][], res: FileResult) {
  const spanAt = new Map<string, Span[]>(); // first record key → spans starting there
  const unpaired = new Set<string>();
  const nodeId = new Map<string, number>(); // forest node (left,right) → id, shared across views
  let nextId = 0;
  const register = (us: Rec[][], prefix: boolean): Span | undefined => {
    const recs = us.flat();
    const size = recs.reduce((a, r) => a + digestSize(r), 0);
    if (size === 0) return undefined; // e.g. a lone memory result: nothing to summarize
    const sp: Span = { id: nextId++, keys: recs.map((r) => r.key), size };
    (spanAt.get(sp.keys[0]) ?? spanAt.set(sp.keys[0], []).get(sp.keys[0])!).push(sp);
    res.spans++;
    res.summaryInputBytes += size;
    if (prefix) res.prefixSpans++;
    if (size > SPAN_MAX) res.oversizedSpans++; // only a lone indivisible unit gets here
    return sp;
  };
  for (const path of views) {
    res.views++;
    const us = unitsOf(path, unpaired);
    const starts: number[] = []; // record offset of each unit
    let off = 0;
    for (const u of us) starts.push(off), (off += u.length);
    const order: number[] = []; // this view's span list
    let ui = 0;
    while (ui < us.length) {
      let full: Span | undefined;
      let partial = 0;
      for (const sp of spanAt.get(us[ui][0].key) ?? []) {
        let m = 0;
        while (m < sp.keys.length && starts[ui] + m < path.length && path[starts[ui] + m].key === sp.keys[m]) m++;
        if (m === sp.keys.length) full = !full || sp.keys.length > full.keys.length ? sp : full;
        else partial = Math.max(partial, m);
      }
      if (full) {
        order.push(full.id);
        const end = starts[ui] + full.keys.length;
        while (ui < us.length && starts[ui] < end) ui++;
        continue;
      }
      let k = 0;
      while (ui + k < us.length && starts[ui + k] + us[ui + k].length <= starts[ui] + partial) k++;
      if (k > 0) {
        const p = register(us.slice(ui, ui + k), true);
        if (p) order.push(p.id);
        ui += k;
        continue;
      }
      // Fresh span: whole units, up to SPAN_MAX, never across a user message.
      let n = 1;
      let size = us[ui].reduce((a, r) => a + digestSize(r), 0);
      while (ui + n < us.length && us[ui + n][0].kind !== "user") {
        const add = us[ui + n].reduce((a, r) => a + digestSize(r), 0);
        if (size + add > SPAN_MAX) break;
        size += add;
        n++;
      }
      const sp = register(us.slice(ui, ui + n), false);
      if (sp) order.push(sp.id);
      ui += n;
    }
    // Forest over this view's span list: binary carries; identical subtrees are one node (DESIGN §4.4).
    for (let level = order; level.length > 1; ) {
      const up: number[] = [];
      for (let i = 0; i + 1 < level.length; i += 2) {
        const key = `${level[i]},${level[i + 1]}`;
        let id = nodeId.get(key);
        if (id === undefined) nodeId.set(key, (id = nextId++));
        up.push(id);
      }
      level = up;
    }
  }
  res.forestInternalNodes = nodeId.size;
  res.unpairedCalls = unpaired.size;
}

// Unique records: evidence, digest and lexical bytes, exclusions. Independent of how views share them.
function account(recs: Rec[], res: FileResult) {
  const evidence = new Set<string>();
  const excludedSeen = new Set<string>();
  for (const r of recs) {
    if (r.kind === "user") res.exchanges++;
    if (r.memory) continue;
    const d = digestSize(r);
    res.digestBytes += d;
    if (d) evidence.add(r.key.slice(0, r.key.lastIndexOf("#")));
    else {
      const reason = r.kind === "thinking" ? "thinking" : "context";
      const key = `${r.key.slice(0, r.key.lastIndexOf("#"))}:${reason}`;
      tally(res.digestExcluded, reason, bytes(r.text), excludedSeen.has(key) ? 0 : 1);
      excludedSeen.add(key);
    }
    const n = bytes(r.text);
    res.lexicalBytes += n;
    res.lexicalChunks += Math.ceil(n / LEXICAL_CHUNK);
  }
  res.evidence = evidence.size;
}

const inSpan = (r: Rec) => DIGEST_KINDS.has(r.kind); // memory results stay: they close their call
type Parsed = { recs: Rec[]; views: Rec[][] };

// ---------- reading ----------

// `want` is the dev:ino seen by collect(); a file swapped after discovery is refused.
function readLines(path: string, res: FileResult, want?: string): Record<string, any>[] {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  let text: string;
  try {
    const st = fstatSync(fd);
    if (!st.isFile() || (want !== undefined && want !== `${st.dev}:${st.ino}`)) throw new Error("file changed since discovery");
    text = readFileSync(fd, "utf8");
  } finally {
    closeSync(fd);
  }
  const lines = text.split("\n");
  const out: Record<string, any>[] = [];
  lines.forEach((line, i) => {
    if (!line.trim()) return;
    let v: unknown;
    try {
      v = JSON.parse(line);
    } catch {
      // Only an unterminated last line is a torn write; anything else is malformed.
      if (i === lines.length - 1 && !text.endsWith("\n")) res.tornLines++;
      else res.malformedLines++;
      return;
    }
    if (v && typeof v === "object" && !Array.isArray(v)) out.push(v as Record<string, any>);
    else res.malformedLines++;
  });
  return out;
}


// ---------- Pi (session v3) ----------

const PI_SUBAGENT = /(^|\/)subagent-artifacts\/[^/]*_transcript\.jsonl$/;

function piRecords(entry: Record<string, any>, res: FileResult): Rec[] {
  return perRecord(res, (ex) => piRecordsInner(entry, res, ex)).map((r, j) => ({ ...r, key: `${entry.id}#${j}` }));
}

type Draft = Omit<Rec, "key">;
function piRecordsInner(entry: Record<string, any>, res: FileResult, ex: Tally): Draft[] {
  const id = String(entry.id);
  const custom = (customType: string, content: unknown): Draft[] => {
    const text = textOf(content);
    if (String(customType).startsWith("memex-")) return tally(ex, "memex-injected", bytes(text)), [];
    if (OTHER_MEMORY_CUSTOM.test(String(customType))) return tally(ex, "other-memory-injected", bytes(text)), [];
    inc(res.extensionTypes, String(customType));
    const kept = stripMemory(text, ex);
    return kept ? [{ id, kind: "context", text: `[${customType}] ${kept}` }] : [];
  };
  switch (entry.type) {
    case "custom_message":
      return custom(entry.customType, entry.content);
    case "compaction":
    case "branch_summary":
      tally(ex, "host-summary", bytes(String(entry.summary ?? "")));
      return [];
    case "message":
      break;
    default:
      inc(res.nonContent, String(entry.type));
      return [];
  }
  const m = entry.message ?? {};
  switch (m.role) {
    case "user": {
      const kept = stripMemory(textOf(m.content), ex);
      return kept ? [{ id, kind: "user", text: kept }] : [];
    }
    case "assistant": {
      const out: Draft[] = [];
      const texts: string[] = [];
      for (const p of Array.isArray(m.content) ? m.content : []) {
        if (p?.type === "text") texts.push(String(p.text ?? ""));
        else if (p?.type === "thinking" && p.thinking) {
          const t = stripMemory(String(p.thinking), ex);
          if (t) out.push({ id, kind: "thinking", text: t });
        }
        else if (p?.type === "toolCall") out.push({ id, kind: "tool_call", callId: p.id, text: `${p.name} ${JSON.stringify(p.arguments ?? {})}` });
      }
      const text = stripMemory(texts.join("\n"), ex);
      return text ? [{ id, kind: "assistant", text }, ...out] : out;
    }
    case "toolResult": {
      const text = textOf(m.content);
      const name = String(m.toolName ?? "");
      const memory = MEMEX_TOOL.test(name) ? "memex" : OTHER_MEMORY_TOOL.test(name) ? "other-memory" : undefined;
      if (memory) {
        // The call stays in the span (the agent did look something up); the recalled text does not.
        tally(ex, `${memory}-tool-result`, bytes(text));
        return [{ id, kind: "tool_result", callId: m.toolCallId, text: "", memory: true }];
      }
      return [{ id, kind: "tool_result", callId: m.toolCallId, text: (m.isError ? "[error] " : "") + stripMemory(text, ex) }];
    }
    case "bashExecution":
      return [
        { id, kind: "tool_call", callId: `${id}:bash`, text: `user-bash ${m.command}` },
        { id, kind: "tool_result", callId: `${id}:bash`, text: `${stripMemory(String(m.output ?? ""), ex)}\n[exit ${m.exitCode ?? "?"}]` },
      ];
    case "custom":
      return custom(m.customType, m.content);
    case "system": {
      // Only memex's own section is removed; the rest of the system prompt is harness context.
      const sections = m.sections && typeof m.sections === "object" ? { ...m.sections } : undefined;
      if (sections && "memex" in sections) {
        tally(ex, "memex-injected", bytes(textOf(sections.memex)));
        delete sections.memex;
      }
      // `content` is the base prompt; sections render after it (pi-ai SystemMessage). Both are kept.
      const text = [textOf(m.content), ...(sections ? Object.values(sections).map(textOf) : [])].filter(Boolean).join("\n");
      const kept = stripMemory(text, ex);
      return kept ? [{ id, kind: "context", text: kept }] : [];
    }
    case "branchSummary":
    case "compactionSummary":
      tally(ex, "host-summary", bytes(String(m.summary ?? "")));
      return [];
    default:
      inc(res.nonContent, `message:${m.role}`);
      return [];
  }
}

function piFile(lines: Record<string, any>[], res: FileResult, parentKeys: (p: string) => Set<string> | undefined): Parsed {
  res.harness = "pi";
  const [header, ...rest] = lines;
  if (header.version !== 3) return skip(res, "unsupported-version");
  const entries = rest.filter((e) => typeof e.id === "string");
  res.malformedLines += rest.length - entries.length;
  res.records = entries.length;
  const byId = new Map(entries.map((e) => [e.id as string, e]));
  const children = new Map<string, Record<string, any>[]>();
  for (const e of entries) {
    if (e.parentId && byId.has(e.parentId) && e.parentId !== e.id) {
      const k = children.get(e.parentId) ?? [];
      k.push(e);
      children.set(e.parentId, k);
    }
  }
  // Inferred tail (Pi's resume rule) is reported only; every branch is counted.
  const onTail = new Set<string>();
  for (let e: Record<string, any> | undefined = entries[entries.length - 1]; e && !onTail.has(e.id); e = byId.get(e.parentId)) onTail.add(e.id);
  res.offTail = entries.length - onTail.size;

  res.forkStatus = "none";
  let inheritedKeys: Set<string> | undefined;
  if (header.parentSession) {
    inheritedKeys = parentKeys(resolve(String(header.parentSession)));
    res.forkStatus = inheritedKeys ? "resolved" : "parent-not-given";
  }
  const recsOf = new Map<string, Rec[]>();
  for (const e of entries) {
    if (res.forkStatus === "parent-not-given") {
      // Copied history keeps the parent's ids; without the parent nothing here can be called new.
      res.unresolved++;
      recsOf.set(e.id, []);
    } else if (inheritedKeys?.has(`${e.id}:${contentHash(e)}`)) {
      res.inherited++;
      recsOf.set(e.id, []);
    } else recsOf.set(e.id, piRecords(e, res));
  }
  // Only span text makes a branch: a child subtree of labels, state or extension context does not.
  const hasText = new Map<string, boolean>();
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i];
    hasText.set(e.id, recsOf.get(e.id)!.some(inSpan) || (children.get(e.id) ?? []).some((k) => hasText.get(k.id)));
  }
  const textKids = (id: string) => (children.get(id) ?? []).filter((k) => hasText.get(k.id));
  for (const e of entries) if (textKids(e.id).length > 1) res.branchPoints++;
  // Views: root → tip paths, in creation order (first child first). Iterative DFS sharing one path array.
  const views: Rec[][] = [];
  const path: Rec[] = [];
  const stack: { e: Record<string, any>; depth: number }[] = [];
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i];
    if ((!byId.has(e.parentId) || e.parentId === e.id) && hasText.get(e.id)) stack.push({ e, depth: 0 });
  }
  const lens: number[] = [];
  while (stack.length) {
    const { e, depth } = stack.pop()!;
    lens.length = depth;
    path.length = depth ? lens[depth - 1] : 0;
    path.push(...recsOf.get(e.id)!.filter(inSpan));
    lens.push(path.length);
    const kids = textKids(e.id);
    if (kids.length === 0) views.push(path.slice());
    for (let k = kids.length - 1; k >= 0; k--) stack.push({ e: kids[k], depth: depth + 1 });
  }
  return { recs: entries.flatMap((e) => recsOf.get(e.id)!), views };
}

function piEntryKeys(path: string, want?: string): Set<string> | undefined {
  const res = emptyResult(path);
  const lines = readLines(path, res, want);
  if (lines[0]?.type !== "session") return undefined;
  return new Set(lines.slice(1).filter((e) => typeof e.id === "string").map((e) => `${e.id}:${contentHash(e)}`));
}

// ---------- Codex (rollout, Paginated only) ----------

// Harness-written user-role fragments (codex-rs/core/src/context/*: matches_text start markers).
const CODEX_CONTEXT_MARKERS = [
  "# AGENTS.md instructions",
  "<user_instructions>",
  "<environment_context>",
  "<turn_aborted>",
  "<subagent_notification>",
  "<user_shell_command>",
  "<codex_internal_context",
  "<goal_context>",
  "<external_",
];
const isMarked = (t: string) => CODEX_CONTEXT_MARKERS.some((m) => t.trimStart().startsWith(m));

// Classify each content fragment. Codex aligns content_item_kinds with content positions
// (models.rs / annotated_content.rs); text markers are only the fallback for unannotated fragments.
function codexFragments(p: Record<string, any>, ex: Tally): { user: string[]; context: string[] } {
  const content: any[] = Array.isArray(p.content) ? p.content : [];
  const kinds: unknown = p.internal_chat_message_metadata_passthrough?.content_item_kinds;
  // Positional; a short array leaves the rest unknown (annotated_content.rs pads with "unknown").
  const aligned = Array.isArray(kinds) ? (kinds as unknown[]).map(String) : undefined;
  const user: string[] = [];
  const context: string[] = [];
  content.forEach((c, i) => {
    const text = textOf(c);
    if (!text) return;
    const k = aligned?.[i];
    const annotated = k !== undefined && k !== "" && k !== "unknown";
    if (annotated && k.startsWith("user.")) user.push(text);
    else if (k === "hooks.additional_context" && /^\s*<memex/.test(text)) tally(ex, "memex-injected", bytes(text));
    else if (annotated || p.role !== "user" || isMarked(text)) context.push(text);
    else user.push(text);
  });
  return { user, context };
}

function codexRecords(line: Record<string, any>, res: FileResult, calls: Map<string, string>): Rec[] {
  return perRecord(res, (ex) => codexRecordsInner(line, res, calls, ex)).map((r, j) => ({ ...r, key: `${line.ordinal}#${j}` }));
}

function codexRecordsInner(line: Record<string, any>, res: FileResult, calls: Map<string, string>, ex: Tally): Draft[] {
  const id = String(line.ordinal);
  if (line.type === "inter_agent_communication") {
    const text = stripMemory(textOf(line.payload), ex);
    return text ? [{ id, kind: "agent", text }] : [];
  }
  if (line.type === "compacted") return tally(ex, "host-summary", bytes(String(line.payload?.message ?? ""))), [];
  if (line.type !== "response_item") return inc(res.nonContent, String(line.type)), []; // event_msg repeats response items
  const p = line.payload ?? {};
  switch (p.type) {
    case "message": {
      if (p.role === "assistant") {
        const text = stripMemory(textOf(p.content), ex);
        return text ? [{ id, kind: "assistant", text }] : [];
      }
      const { user, context } = codexFragments(p, ex);
      const out: Draft[] = [];
      const u = stripMemory(user.join("\n"), ex);
      if (u) out.push({ id, kind: "user", text: u });
      const c = stripMemory(context.join("\n"), ex);
      if (c) out.push({ id, kind: "context", text: c });
      return out;
    }
    case "reasoning": {
      const text = stripMemory([textOf(p.summary), textOf(p.content)].filter(Boolean).join("\n"), ex);
      return text ? [{ id, kind: "thinking", text }] : [];
    }
    case "function_call":
    case "custom_tool_call":
    case "local_shell_call":
    case "tool_search_call":
    case "web_search_call": {
      const name = String(p.name ?? p.type);
      return [{ id, kind: "tool_call", callId: p.call_id, text: `${name} ${p.arguments ?? p.input ?? JSON.stringify(p.action ?? {})}` }];
    }
    case "function_call_output":
    case "custom_tool_call_output":
    case "tool_search_output": {
      const text = textOf(p.output) || textOf(p.tools);
      const name = calls.get(String(p.call_id)) ?? String(p.name ?? "");
      if (MEMEX_TOOL.test(name)) return tally(ex, "memex-tool-result", bytes(text)), [{ id, kind: "tool_result", callId: p.call_id, text: "", memory: true }];
      if (OTHER_MEMORY_TOOL.test(name)) return tally(ex, "other-memory-tool-result", bytes(text)), [{ id, kind: "tool_result", callId: p.call_id, text: "", memory: true }];
      return [{ id, kind: "tool_result", callId: p.call_id, text: stripMemory(text, ex) }];
    }
    case "agent_message": {
      const text = stripMemory(textOf(p.content), ex);
      return text ? [{ id, kind: "agent", text }] : [];
    }
    default:
      inc(res.nonContent, `response_item:${p.type}`);
      return [];
  }
}

export function codexIsChild(meta: Record<string, any>): boolean {
  const ts = meta.thread_source;
  if (ts !== undefined && ts !== null) return ts === "subagent" || ts === "guardian_review";
  const s = meta.source;
  return !!s && typeof s === "object" && ("subagent" in s || s.internal === "guardian");
}

// rollout-<timestamp>-<thread-id>[_<rollout-id>].jsonl → rollout id (thread id when absent)
function rolloutId(path: string): string | undefined {
  const m = basename(path).match(/^rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-([0-9a-f-]{36})(?:_([0-9a-f-]{36}))?\.jsonl$/);
  return m ? (m[2] ?? m[1]) : undefined;
}

// Same rule as codex-rs/rollout/src/metadata.rs forked_from_ordinal_exclusive().
export function codexForkBoundary(meta: Record<string, any>, path: string): number | undefined {
  const parent = meta.forked_from_id;
  if (!parent) return undefined;
  if (typeof meta.forked_from_ordinal_exclusive === "number") return meta.forked_from_ordinal_exclusive;
  const base = meta.history_base;
  if (base && (base.thread_id === parent || rolloutId(path) === meta.id)) return base.end_ordinal_exclusive;
  return undefined;
}

function codexFile(path: string, lines: Record<string, any>[], res: FileResult): Parsed {
  res.harness = "codex";
  const meta = lines[0].payload ?? {};
  if (meta.history_mode !== "paginated") return skip(res, "legacy");
  if (lines.some((l) => typeof l.ordinal !== "number")) return skip(res, "no-ordinal");
  if (codexIsChild(meta)) return skip(res, "child-thread");
  res.records = lines.length;
  const boundary = codexForkBoundary(meta, path);
  res.forkStatus = !meta.forked_from_id ? "none" : boundary === undefined ? "unresolved" : "resolved";
  // Call names from the whole file first: an inherited call can own an output after the boundary.
  const calls = new Map<string, string>();
  for (const l of lines) {
    const p = l.type === "response_item" ? l.payload : undefined;
    if (p && /_call$/.test(String(p.type)) && p.call_id) calls.set(String(p.call_id), String(p.name ?? p.type));
  }
  const recs: Rec[] = [];
  for (const l of lines) {
    if (meta.forked_from_id) {
      // Unknown boundary: every record could be inherited, so the whole file stays quarantined.
      if (boundary === undefined) {
        res.unresolved++;
        continue;
      }
      if (l.ordinal < boundary) {
        res.inherited++;
        continue;
      }
    }
    const rs = codexRecords(l, res, calls);
    recs.push(...rs);
  }
  return { recs, views: [recs.filter(inSpan)] }; // rollback replay is out of scope: one linear view
}

// ---------- driver ----------

function skip(res: FileResult, reason: FileResult["reason"]): Parsed {
  res.status = "skipped";
  res.reason = reason;
  return { recs: [], views: [] };
}

export interface AnalyzeOptions {
  parentKeys?: (p: string) => Set<string> | undefined; // Pi fork parents among the approved inputs
  identity?: string; // dev:ino from collect(); the file must still be that inode
}

export function analyzeFile(path: string, opts: AnalyzeOptions = {}): FileResult {
  const parentKeys = opts.parentKeys ?? (() => undefined);
  const res = emptyResult(path);
  if (PI_SUBAGENT.test(path)) return skip(res, "child-thread"), (res.harness = "pi"), res;
  if (path.endsWith(".zst")) return skip(res, "compressed"), res;
  try {
    const lines = readLines(path, res, opts.identity);
    if (lines.length === 0) return skip(res, "empty"), res;
    let parsed: Parsed;
    if (lines[0].type === "session") parsed = piFile(lines, res, parentKeys);
    else if (lines[0].type === "session_meta") parsed = codexFile(path, lines, res);
    else return skip(res, "unknown-format"), res;
    if (res.status === "indexed") {
      account(parsed.recs, res);
      partition(parsed.views, res);
    }
  } catch (e) {
    const fresh = emptyResult(path);
    Object.assign(res, fresh, { status: "skipped", reason: "unreadable", error: String((e as Error).message ?? e).slice(0, 200) });
  }
  return res;
}

// Regular files under the inputs; symlinks are never followed (reported instead), paths deduplicated.
// Roots are resolved to their real path once; below them every directory must still be its own real path
// when it is listed, and every file is opened with O_NOFOLLOW and checked against the identity seen here.
// Residual: a directory swapped for a link between that check and readdir; closing it needs openat-style
// traversal, which Node does not offer.
export function collect(inputs: string[]): { files: string[]; identity: Map<string, string>; symlinks: string[]; errors: { path: string; error: string }[] } {
  const files = new Set<string>();
  const identity = new Map<string, string>();
  const symlinks = new Set<string>();
  const errors: { path: string; error: string }[] = [];
  const walk = (p: string) => {
    try {
      const st = lstatSync(p);
      if (st.isSymbolicLink()) symlinks.add(p);
      else if (st.isDirectory()) {
        if (realpathSync(p) !== p) throw new Error("directory changed since discovery");
        for (const n of readdirSync(p).sort()) walk(join(p, n));
      } else if (st.isFile() && /\.jsonl(\.zst)?$/.test(p)) {
        files.add(p);
        identity.set(p, `${st.dev}:${st.ino}`);
      }
    } catch (e) {
      errors.push({ path: p, error: String((e as Error).message ?? e).slice(0, 200) });
    }
  };
  for (const i of inputs) {
    let root: string;
    try {
      const abs = resolve(i);
      if (lstatSync(abs).isSymbolicLink()) {
        symlinks.add(abs);
        continue;
      }
      root = realpathSync(abs);
    } catch (e) {
      errors.push({ path: resolve(i), error: String((e as Error).message ?? e).slice(0, 200) });
      continue;
    }
    walk(root);
  }
  return { files: [...files].sort(), identity, symlinks: [...symlinks].sort(), errors };
}

export function dryRun(inputs: string[]) {
  const { files, identity, symlinks, errors } = collect(inputs);
  const approved = new Set(files);
  // Pi forks name their parent file. Only parents among the approved inputs are read, once, on demand.
  const cache = new Map<string, Set<string> | undefined>();
  const parentKeys = (p: string) => {
    if (!approved.has(p) || PI_SUBAGENT.test(p)) return undefined;
    if (!cache.has(p)) {
      try {
        cache.set(p, piEntryKeys(p, identity.get(p)));
      } catch {
        cache.set(p, undefined);
      }
    }
    return cache.get(p);
  };
  const results = files.map((f) => analyzeFile(f, { parentKeys, identity: identity.get(f) }));
  const total = emptyResult("(total)");
  const skipped: Record<string, string[]> = {};
  for (const r of results) {
    if (r.status === "skipped") {
      (skipped[r.reason!] ??= []).push(r.path);
      continue;
    }
    for (const k of ["records", "evidence", "offTail", "branchPoints", "views", "prefixSpans", "unpairedCalls", "inherited", "unresolved", "tornLines", "malformedLines", "exchanges", "spans", "oversizedSpans", "digestBytes", "summaryInputBytes", "lexicalBytes", "lexicalChunks", "forestInternalNodes"] as const)
      total[k] += r[k];
    for (const [k, v] of Object.entries(r.nonContent)) inc(total.nonContent, k, v);
    for (const [k, v] of Object.entries(r.extensionTypes)) inc(total.extensionTypes, k, v);
    for (const t of ["excluded", "digestExcluded"] as const) for (const [k, v] of Object.entries(r[t])) tally(total[t], k, v.bytes, v.records);
  }
  const indexed = results.filter((r) => r.status === "indexed");
  return {
    files: results.length,
    symlinksSkipped: symlinks,
    discoveryErrors: errors,
    indexed: { pi: indexed.filter((r) => r.harness === "pi").length, codex: indexed.filter((r) => r.harness === "codex").length },
    skipped,
    forks: Object.fromEntries(["resolved", "parent-not-given", "unresolved"].map((s) => [s, indexed.filter((r) => r.forkStatus === s).length])),
    total,
    perFile: results,
  };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const perFile = args.includes("--per-file");
  const inputs = args.filter((a) => a !== "--per-file");
  if (inputs.length === 0) {
    console.error("usage: bun dryrun/dryrun.ts [--per-file] <file-or-dir>...   (no default paths)");
    process.exit(2);
  }
  const { perFile: files, ...summary } = dryRun(inputs);
  console.log(JSON.stringify(perFile ? { ...summary, perFile: files } : summary, null, 2));
}
