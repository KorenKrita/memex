// memex import --dry-run (phase 0): read explicitly given Pi / Codex session logs, apply the
// DESIGN §4.1–§4.3 rules locally and report how much would be archived, split and summarized.
// Read-only, no network, no model calls, no default directories. Sizes are UTF-8 bytes.
import { createHash } from "node:crypto";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";

// [待标定] values from DESIGN §4.3
export const SPAN_MIN = 4096; // target only: a span may close short to stay under SPAN_MAX
export const SPAN_MAX = 12288;
export const TOOL_HEAD = 1024;
export const TOOL_TAIL = 1024;
export const TOOL_ERROR_LINES = 20;
export const LEXICAL_CHUNK = 2048;

const enc = new TextEncoder();
const bytes = (s: string) => enc.encode(s).length;

// Kinds that become span text. `context` (harness / extension context) and `thinking` are kept in L0
// and the lexical index but are not summary input; neither opens an exchange.
export type Kind = "user" | "assistant" | "tool_call" | "tool_result" | "agent" | "context" | "thinking";
export interface Rec {
  id: string;
  kind: Kind;
  text: string;
  callId?: string;
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
  branchPoints: number; // Pi entries with more than one child
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
  digestBytes: number;
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

// Removes memory-system blocks from any retained text; each record counts once per reason.
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
export const contentHash = (v: unknown) => createHash("sha256").update(canon(v)).digest("hex");

export function digestOf(r: Rec): string {
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
const digestSize = (r: Rec) => (DIGEST_KINDS.has(r.kind) ? bytes(digestOf(r)) + 1 : 0);

// ---------- units and spans ----------

// A unit is never split: one record, or a tool call together with everything up to the last result of
// the calls still open (parallel calls, out-of-order results, commentary in between). A call whose result
// never arrives keeps its unit open to the end of the segment.
export function units(seg: Rec[]): Rec[][] {
  const out: Rec[][] = [];
  let cur: Rec[] | undefined;
  const open = new Set<string>();
  for (const r of seg) {
    if (cur) {
      cur.push(r);
      if (r.kind === "tool_call" && r.callId) open.add(r.callId);
      if (r.kind === "tool_result" && r.callId) open.delete(r.callId);
      if (open.size === 0) cur = undefined;
      continue;
    }
    if (r.kind === "tool_call" && r.callId) {
      cur = [r];
      open.add(r.callId);
      out.push(cur);
    } else out.push([r]);
  }
  return out;
}

export function spansOf(seg: Rec[]): { recs: Rec[]; size: number; oversized: boolean }[] {
  const spans: { recs: Rec[]; size: number; oversized: boolean }[] = [];
  let cur: { recs: Rec[]; size: number; oversized: boolean } | undefined;
  for (const u of units(seg)) {
    const n = u.reduce((s, r) => s + digestSize(r), 0);
    if (n === 0 && !cur) continue; // context-only units never open a span on their own
    if (cur && cur.size + n > SPAN_MAX) cur = undefined;
    if (!cur) spans.push((cur = { recs: [], size: 0, oversized: false }));
    cur.recs.push(...u);
    cur.size += n;
    if (cur.size > SPAN_MAX) cur.oversized = true; // only possible for a lone oversized unit
  }
  return spans.filter((s) => s.size > 0);
}

// Segments are chains along the conversation tree that never cross a branch point or an exchange start.
function account(segments: Rec[][], res: FileResult) {
  const evidence = new Set<string>();
  for (const seg of segments) {
    for (const s of spansOf(seg)) {
      res.spans++;
      if (s.oversized) res.oversizedSpans++;
    }
    for (const r of seg) {
      if (r.kind === "user") res.exchanges++;
      const d = digestSize(r);
      res.digestBytes += d;
      if (d) evidence.add(r.id);
      else tally(res.digestExcluded, r.kind === "thinking" ? "thinking" : "context", bytes(r.text));
      const n = bytes(r.text);
      res.lexicalBytes += n;
      res.lexicalChunks += Math.ceil(n / LEXICAL_CHUNK);
    }
  }
  res.evidence = evidence.size;
  const popcount = (n: number) => n.toString(2).replace(/0/g, "").length;
  res.forestInternalNodes = res.spans - popcount(res.spans);
}

// ---------- reading ----------

function readLines(path: string, res: FileResult): Record<string, any>[] {
  const text = readFileSync(path, "utf8");
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

export const PI_SUBAGENT = /(^|\/)subagent-artifacts\/[^/]*_transcript\.jsonl$/;

function piRecords(entry: Record<string, any>, res: FileResult): Rec[] {
  const id = String(entry.id);
  const ex = res.excluded;
  const custom = (customType: string, content: unknown): Rec[] => {
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
      const out: Rec[] = [];
      const texts: string[] = [];
      for (const p of Array.isArray(m.content) ? m.content : []) {
        if (p?.type === "text") texts.push(String(p.text ?? ""));
        else if (p?.type === "thinking" && p.thinking) out.push({ id, kind: "thinking", text: String(p.thinking) });
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
        return [{ id, kind: "tool_result", callId: m.toolCallId, text: `[${memory} tool result]` }];
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
      const text = sections ? Object.values(sections).map(textOf).join("\n") : textOf(m.content);
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

function piFile(lines: Record<string, any>[], res: FileResult, parentKeys: (p: string) => Set<string> | undefined): Rec[][] {
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
  const opensExchange = (e: Record<string, any>) => recsOf.get(e.id)!.some((r) => r.kind === "user");
  const isStart = (e: Record<string, any>) => {
    if (!byId.has(e.parentId) || e.parentId === e.id) return true;
    return (children.get(e.parentId)?.length ?? 0) > 1 || opensExchange(e);
  };
  const segments: Rec[][] = [];
  const visited = new Set<string>();
  for (const start of entries) {
    if (!isStart(start) || visited.has(start.id)) continue;
    const seg: Rec[] = [];
    for (let e: Record<string, any> | undefined = start; e && !visited.has(e.id); ) {
      visited.add(e.id);
      seg.push(...recsOf.get(e.id)!);
      const kids: Record<string, any>[] = children.get(e.id) ?? [];
      if (kids.length > 1) res.branchPoints++;
      e = kids.length === 1 && !isStart(kids[0]) ? kids[0] : undefined;
    }
    segments.push(seg);
  }
  return segments;
}

function piEntryKeys(path: string): Set<string> | undefined {
  const res = emptyResult(path);
  const lines = readLines(path, res);
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
function codexFragments(p: Record<string, any>, res: FileResult): { user: string[]; context: string[] } {
  const content: any[] = Array.isArray(p.content) ? p.content : [];
  const kinds: unknown = p.internal_chat_message_metadata_passthrough?.content_item_kinds;
  const aligned = Array.isArray(kinds) && kinds.length === content.length ? (kinds as unknown[]).map(String) : undefined;
  const user: string[] = [];
  const context: string[] = [];
  content.forEach((c, i) => {
    const text = textOf(c);
    if (!text) return;
    const k = aligned?.[i];
    const annotated = k !== undefined && k !== "" && k !== "unknown";
    if (annotated && k.startsWith("user.")) user.push(text);
    else if (k === "hooks.additional_context" && /^\s*<memex/.test(text)) tally(res.excluded, "memex-injected", bytes(text));
    else if (annotated || p.role !== "user" || isMarked(text)) context.push(text);
    else user.push(text);
  });
  return { user, context };
}

function codexRecords(line: Record<string, any>, res: FileResult, calls: Map<string, string>): Rec[] {
  const id = String(line.ordinal);
  const ex = res.excluded;
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
      const { user, context } = codexFragments(p, res);
      const out: Rec[] = [];
      const u = stripMemory(user.join("\n"), ex);
      if (u) out.push({ id, kind: "user", text: u });
      const c = stripMemory(context.join("\n"), ex);
      if (c) out.push({ id, kind: "context", text: c });
      return out;
    }
    case "reasoning": {
      const text = [textOf(p.summary), textOf(p.content)].filter(Boolean).join("\n");
      return text ? [{ id, kind: "thinking", text }] : [];
    }
    case "function_call":
    case "custom_tool_call":
    case "local_shell_call":
    case "tool_search_call":
    case "web_search_call": {
      const name = String(p.name ?? p.type);
      if (p.call_id) calls.set(String(p.call_id), name);
      return [{ id, kind: "tool_call", callId: p.call_id, text: `${name} ${p.arguments ?? p.input ?? JSON.stringify(p.action ?? {})}` }];
    }
    case "function_call_output":
    case "custom_tool_call_output":
    case "tool_search_output": {
      const text = textOf(p.output) || textOf(p.tools);
      const name = calls.get(String(p.call_id)) ?? String(p.name ?? "");
      if (MEMEX_TOOL.test(name)) return tally(ex, "memex-tool-result", bytes(text)), [{ id, kind: "tool_result", callId: p.call_id, text: "[memex tool result]" }];
      if (OTHER_MEMORY_TOOL.test(name)) return tally(ex, "other-memory-tool-result", bytes(text)), [{ id, kind: "tool_result", callId: p.call_id, text: "[other-memory tool result]" }];
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

function codexFile(path: string, lines: Record<string, any>[], res: FileResult): Rec[][] {
  res.harness = "codex";
  const meta = lines[0].payload ?? {};
  if (meta.history_mode !== "paginated") return skip(res, "legacy");
  if (lines.some((l) => typeof l.ordinal !== "number")) return skip(res, "no-ordinal");
  if (codexIsChild(meta)) return skip(res, "child-thread");
  res.records = lines.length;
  const boundary = codexForkBoundary(meta, path);
  res.forkStatus = !meta.forked_from_id ? "none" : boundary === undefined ? "unresolved" : "resolved";
  const segments: Rec[][] = [[]];
  const calls = new Map<string, string>();
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
    if (rs.some((r) => r.kind === "user") && segments[segments.length - 1].length) segments.push([]);
    segments[segments.length - 1].push(...rs);
  }
  return segments;
}

// ---------- driver ----------

function skip(res: FileResult, reason: FileResult["reason"]): Rec[][] {
  res.status = "skipped";
  res.reason = reason;
  return [];
}

export function analyzeFile(path: string, parentKeys: (p: string) => Set<string> | undefined = () => undefined): FileResult {
  const res = emptyResult(path);
  if (PI_SUBAGENT.test(path)) return skip(res, "child-thread"), (res.harness = "pi"), res;
  if (path.endsWith(".zst")) return skip(res, "compressed"), res;
  try {
    const lines = readLines(path, res);
    if (lines.length === 0) return skip(res, "empty"), res;
    let segments: Rec[][];
    if (lines[0].type === "session") segments = piFile(lines, res, parentKeys);
    else if (lines[0].type === "session_meta") segments = codexFile(path, lines, res);
    else return skip(res, "unknown-format"), res;
    if (res.status === "indexed") account(segments, res);
  } catch (e) {
    const fresh = emptyResult(path);
    Object.assign(res, fresh, { status: "skipped", reason: "unreadable", error: String((e as Error).message ?? e).slice(0, 200) });
  }
  return res;
}

// Regular files under the inputs; symlinks are never followed (reported instead), paths deduplicated.
export function collect(inputs: string[]): { files: string[]; symlinks: string[] } {
  const files = new Set<string>();
  const symlinks = new Set<string>();
  const walk = (p: string) => {
    const st = lstatSync(p);
    if (st.isSymbolicLink()) symlinks.add(p);
    else if (st.isDirectory()) for (const n of readdirSync(p).sort()) walk(join(p, n));
    else if (st.isFile() && /\.jsonl(\.zst)?$/.test(p)) files.add(p);
  };
  for (const i of inputs) walk(resolve(i));
  return { files: [...files].sort(), symlinks: [...symlinks].sort() };
}

export function dryRun(inputs: string[]) {
  const { files, symlinks } = collect(inputs);
  const approved = new Set(files);
  // Pi forks name their parent file. Only parents among the approved inputs are read, once, on demand.
  const cache = new Map<string, Set<string> | undefined>();
  const parentKeys = (p: string) => {
    if (!approved.has(p) || PI_SUBAGENT.test(p)) return undefined;
    if (!cache.has(p)) {
      try {
        cache.set(p, piEntryKeys(p));
      } catch {
        cache.set(p, undefined);
      }
    }
    return cache.get(p);
  };
  const results = files.map((f) => analyzeFile(f, parentKeys));
  const total = emptyResult("(total)");
  const skipped: Record<string, string[]> = {};
  for (const r of results) {
    if (r.status === "skipped") {
      (skipped[r.reason!] ??= []).push(r.path);
      continue;
    }
    for (const k of ["records", "evidence", "offTail", "branchPoints", "inherited", "unresolved", "tornLines", "malformedLines", "exchanges", "spans", "oversizedSpans", "digestBytes", "lexicalBytes", "lexicalChunks", "forestInternalNodes"] as const)
      total[k] += r[k];
    for (const [k, v] of Object.entries(r.nonContent)) inc(total.nonContent, k, v);
    for (const [k, v] of Object.entries(r.extensionTypes)) inc(total.extensionTypes, k, v);
    for (const t of ["excluded", "digestExcluded"] as const) for (const [k, v] of Object.entries(r[t])) tally(total[t], k, v.bytes, v.records);
  }
  const indexed = results.filter((r) => r.status === "indexed");
  return {
    files: results.length,
    symlinksSkipped: symlinks,
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
