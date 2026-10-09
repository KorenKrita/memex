// memex import --dry-run (phase 0): read explicitly given Pi / Codex session logs, apply the
// DESIGN §4.1–§4.3 rules locally and report how much would be archived, split and summarized.
// Read-only, no network, no model calls, no default directories. Sizes are UTF-8 bytes.
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { basename, join, resolve } from "node:path";

// [待标定] values from DESIGN §4.3
export const SPAN_MIN = 4096;
export const SPAN_MAX = 12288;
export const TOOL_HEAD = 1024;
export const TOOL_TAIL = 1024;
export const TOOL_ERROR_LINES = 20;
export const LEXICAL_CHUNK = 2048;

const enc = new TextEncoder();
const bytes = (s: string) => enc.encode(s).length;
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

type Kind = "user" | "assistant" | "tool_call" | "tool_result" | "agent" | "ext";
export interface Rec {
  id: string;
  kind: Kind;
  text: string;
  callId?: string;
}

export interface FileResult {
  path: string;
  harness?: "pi" | "codex";
  status: "indexed" | "skipped";
  reason?: "legacy" | "child-thread" | "unknown-format" | "unsupported-version" | "compressed" | "empty" | "no-ordinal";
  leafBasis?: "inferred-tail";
  forkStatus?: "none" | "resolved" | "parent-not-given" | "unresolved";
  records: number; // L0 lines with a record identity (Pi entries / Codex ordinals)
  evidence: number; // records that enter spans
  offPath: number; // Pi entries not on the inferred-tail path (archived, still indexed in later phases)
  inherited: number; // fork prefix: canonical lives in the parent
  unresolved: number; // fork prefix with unknown boundary: archived, quarantined
  tornLines: number;
  excluded: Record<string, number>; // bytes left out of spans, by reason
  exchanges: number;
  spans: number;
  oversizedSpans: number;
  digestBytes: number;
  lexicalBytes: number;
  lexicalChunks: number;
  forestInternalNodes: number;
}

// ---------- shared: text helpers ----------

const OTHER_MEMORY = [/<openviking-context\b[\s\S]*?<\/openviking-context>/g, /<contextdb-auto-recall\b[\s\S]*?<\/contextdb-auto-recall>/g];

function stripOtherMemory(text: string, ex: Record<string, number>): string {
  let out = text;
  for (const re of OTHER_MEMORY) {
    out = out.replace(re, (m) => {
      add(ex, "other-memory-injected", bytes(m));
      return "";
    });
  }
  return out.trim();
}

function add(ex: Record<string, number>, k: string, n: number) {
  if (n > 0) ex[k] = (ex[k] ?? 0) + n;
}

// Collect every string under a `text` key (or the value itself if it is a string).
function textOf(v: unknown): string {
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(textOf).filter(Boolean).join("\n");
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (typeof o.text === "string") return o.text;
    if ("content" in o) return textOf(o.content);
    if ("output" in o) return textOf(o.output);
  }
  return "";
}

export function digestOf(r: Rec): string {
  if (r.kind !== "tool_result") return `${r.kind}: ${r.text}`;
  const n = bytes(r.text);
  if (n <= TOOL_HEAD + TOOL_TAIL) return `${r.kind}: ${r.text}`;
  const buf = enc.encode(r.text);
  const dec = new TextDecoder(); // replacement chars at cut points are fine for a size estimate
  const head = dec.decode(buf.slice(0, TOOL_HEAD));
  const tail = dec.decode(buf.slice(n - TOOL_TAIL));
  const middle = dec.decode(buf.slice(TOOL_HEAD, n - TOOL_TAIL));
  const errors = middle
    .split("\n")
    .filter((l) => /error|fail|exception|traceback|panic/i.test(l))
    .slice(0, TOOL_ERROR_LINES)
    .map((l) => l.slice(0, 300));
  return `${r.kind}: ${head}\n‹tool-output record=${r.id} bytes=${n}›\n${errors.join("\n")}\n${tail}`;
}

// ---------- shared: exchanges and spans ----------

// A unit is never split: an assistant text, or a run of tool calls with their results.
export function units(ex: Rec[]): Rec[][] {
  const out: Rec[][] = [];
  for (const r of ex) {
    const last = out[out.length - 1];
    const inToolRun = last && last.some((x) => x.kind === "tool_call");
    if (r.kind === "tool_result" && inToolRun) last.push(r);
    else if (r.kind === "tool_call" && inToolRun && last[last.length - 1].kind === "tool_call") last.push(r);
    else out.push([r]);
  }
  return out;
}

export function split(records: Rec[], res: FileResult) {
  const exchanges: Rec[][] = [];
  for (const r of records) {
    if (r.kind === "user" || exchanges.length === 0) exchanges.push([]);
    exchanges[exchanges.length - 1].push(r);
  }
  res.exchanges = exchanges.length;
  for (const ex of exchanges) {
    let size = 0;
    let open = false;
    const close = () => {
      if (!open) return;
      res.spans++;
      if (size > SPAN_MAX) res.oversizedSpans++;
      size = 0;
      open = false;
    };
    for (const u of units(ex)) {
      const n = u.reduce((s, r) => s + bytes(digestOf(r)) + 1, 0);
      if (open && size >= SPAN_MIN && size + n > SPAN_MAX) close();
      size += n;
      open = true;
    }
    close();
  }
  for (const r of records) {
    res.digestBytes += bytes(digestOf(r)) + 1;
    const n = bytes(r.text);
    res.lexicalBytes += n;
    res.lexicalChunks += Math.ceil(n / LEXICAL_CHUNK);
  }
  const popcount = (n: number) => n.toString(2).replace(/0/g, "").length;
  res.forestInternalNodes = res.spans - popcount(res.spans);
}

// ---------- lines ----------

function readLines(path: string, res: FileResult): Record<string, any>[] {
  const out: Record<string, any>[] = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      out.push(JSON.parse(line));
    } catch {
      res.tornLines++;
    }
  }
  return out;
}

// ---------- Pi (session v3) ----------

const PI_SUBAGENT = /(^|\/)subagent-artifacts\/[^/]*_transcript\.jsonl$/;

function piRecords(entry: Record<string, any>, ex: Record<string, number>): Rec[] {
  const id = String(entry.id);
  const custom = (customType: string, content: unknown): Rec[] => {
    const text = textOf(content);
    if (String(customType).startsWith("memex-")) return add(ex, "memex-injected", bytes(text)), [];
    const kept = stripOtherMemory(text, ex);
    return kept ? [{ id, kind: "ext", text: `[${customType}] ${kept}` }] : [];
  };
  if (entry.type === "custom_message") return custom(entry.customType, entry.content);
  if (entry.type === "compaction" || entry.type === "branch_summary") {
    add(ex, "host-summary", bytes(String(entry.summary ?? "")));
    return [];
  }
  if (entry.type !== "message") return [];
  const m = entry.message ?? {};
  switch (m.role) {
    case "user": {
      const parts = typeof m.content === "string" ? [{ type: "text", text: m.content }] : (m.content ?? []);
      const text = parts.map((p: any) => (p.type === "text" ? p.text : p.type === "image" ? "[image]" : "")).join("\n");
      const kept = stripOtherMemory(text, ex);
      return kept ? [{ id, kind: "user", text: kept }] : [];
    }
    case "assistant": {
      const out: Rec[] = [];
      const texts: string[] = [];
      for (const p of m.content ?? []) {
        if (p.type === "text") texts.push(p.text);
        else if (p.type === "thinking") add(ex, "thinking", bytes(String(p.thinking ?? "")));
        else if (p.type === "toolCall") out.push({ id, kind: "tool_call", callId: p.id, text: `${p.name} ${JSON.stringify(p.arguments ?? {})}` });
      }
      const text = texts.join("\n").trim();
      return text ? [{ id, kind: "assistant", text }, ...out] : out;
    }
    case "toolResult":
      return [{ id, kind: "tool_result", callId: m.toolCallId, text: (m.isError ? "[error] " : "") + textOf(m.content) }];
    case "bashExecution":
      return [
        { id, kind: "tool_call", text: `user-bash ${m.command}` },
        { id, kind: "tool_result", text: `${m.output ?? ""}\n[exit ${m.exitCode ?? "?"}]` },
      ];
    case "custom":
      return custom(m.customType, m.content);
    case "system":
      // System prompt sections (including memex's own snapshot) are archived in L0, never span text.
      add(ex, "system-prompt", bytes(JSON.stringify(m.sections ?? m.content ?? "")));
      return [];
    case "branchSummary":
    case "compactionSummary":
      add(ex, "host-summary", bytes(String(m.summary ?? "")));
      return [];
    default:
      return [];
  }
}

function piFile(path: string, lines: Record<string, any>[], res: FileResult, parents: Map<string, Set<string>>): Rec[] {
  res.harness = "pi";
  const [header, ...entries] = lines;
  if (header.version !== 3) return skip(res, "unsupported-version");
  const withId = entries.filter((e) => typeof e.id === "string");
  res.records = withId.length;
  const byId = new Map(withId.map((e) => [e.id as string, e]));
  // Offline the leaf the user last looked at is unknown: use Pi's own resume rule (last entry).
  res.leafBasis = "inferred-tail";
  const path_: Record<string, any>[] = [];
  const seen = new Set<string>();
  for (let e = withId[withId.length - 1]; e && !seen.has(e.id); e = byId.get(e.parentId)) {
    seen.add(e.id);
    path_.unshift(e);
  }
  res.offPath = withId.length - path_.length;
  let inheritedKeys: Set<string> | undefined;
  res.forkStatus = "none";
  if (header.parentSession) {
    inheritedKeys = parents.get(resolve(header.parentSession));
    res.forkStatus = inheritedKeys ? "resolved" : "parent-not-given";
  }
  const out: Rec[] = [];
  for (const e of path_) {
    // Inherited only when the parent has the same entry id AND content (DESIGN §4.1).
    if (inheritedKeys?.has(`${e.id}:${sha(JSON.stringify(e))}`)) {
      res.inherited++;
      continue;
    }
    const rs = piRecords(e, res.excluded);
    if (rs.length) res.evidence++;
    out.push(...rs);
  }
  return out;
}

export function piEntryKeys(path: string): Set<string> | undefined {
  const res = emptyResult(path);
  const lines = readLines(path, res);
  if (lines[0]?.type !== "session") return undefined;
  return new Set(lines.slice(1).filter((e) => typeof e.id === "string").map((e) => `${e.id}:${sha(JSON.stringify(e))}`));
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

function codexIsContextUser(p: Record<string, any>): boolean {
  const kinds: string[] | undefined = p.internal_chat_message_metadata_passthrough?.content_item_kinds;
  if (kinds && kinds.length > 0 && kinds.every((k) => !String(k).startsWith("user."))) return true;
  return (p.content ?? []).some((c: any) => typeof c.text === "string" && CODEX_CONTEXT_MARKERS.some((m) => c.text.trimStart().startsWith(m)));
}

function codexRecords(line: Record<string, any>, ex: Record<string, number>): Rec[] {
  const id = String(line.ordinal);
  if (line.type === "inter_agent_communication") {
    const text = textOf(line.payload);
    return text ? [{ id, kind: "agent", text }] : [];
  }
  if (line.type !== "response_item") return []; // event_msg repeats response items; the rest is state
  const p = line.payload ?? {};
  switch (p.type) {
    case "message": {
      const text = textOf(p.content);
      if (p.role === "user") {
        if (codexIsContextUser(p)) return add(ex, "harness-context", bytes(text)), [];
        const kept = stripOtherMemory(text, ex);
        return kept ? [{ id, kind: "user", text: kept }] : [];
      }
      if (p.role === "developer") {
        const hook = (p.internal_chat_message_metadata_passthrough?.content_item_kinds ?? []).includes("hooks.additional_context");
        add(ex, hook && text.trimStart().startsWith("<memex") ? "memex-injected" : "harness-context", bytes(text));
        return [];
      }
      return text ? [{ id, kind: "assistant", text }] : [];
    }
    case "reasoning":
      add(ex, "thinking", bytes(textOf(p.summary) + textOf(p.content)));
      return [];
    case "function_call":
    case "custom_tool_call":
    case "local_shell_call":
    case "tool_search_call":
    case "web_search_call":
      return [{ id, kind: "tool_call", callId: p.call_id, text: `${p.name ?? p.type} ${p.arguments ?? p.input ?? JSON.stringify(p.action ?? {})}` }];
    case "function_call_output":
    case "custom_tool_call_output":
    case "tool_search_output":
      return [{ id, kind: "tool_result", callId: p.call_id, text: textOf(p.output) || textOf(p.tools) }];
    case "agent_message":
      return [{ id, kind: "agent", text: textOf(p.content) }];
    default:
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

function codexFile(path: string, lines: Record<string, any>[], res: FileResult): Rec[] {
  res.harness = "codex";
  const meta = lines[0].payload ?? {};
  if (meta.history_mode !== "paginated") return skip(res, "legacy");
  if (lines.some((l) => typeof l.ordinal !== "number")) return skip(res, "no-ordinal");
  if (codexIsChild(meta)) return skip(res, "child-thread");
  res.records = lines.length;
  const boundary = codexForkBoundary(meta, path);
  res.forkStatus = !meta.forked_from_id ? "none" : boundary === undefined ? "unresolved" : "resolved";
  const out: Rec[] = [];
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
    const rs = codexRecords(l, res.excluded);
    if (rs.length) res.evidence++;
    out.push(...rs);
  }
  return out;
}

// ---------- driver ----------

function emptyResult(path: string): FileResult {
  return {
    path,
    status: "indexed",
    records: 0,
    evidence: 0,
    offPath: 0,
    inherited: 0,
    unresolved: 0,
    tornLines: 0,
    excluded: {},
    exchanges: 0,
    spans: 0,
    oversizedSpans: 0,
    digestBytes: 0,
    lexicalBytes: 0,
    lexicalChunks: 0,
    forestInternalNodes: 0,
  };
}

function skip(res: FileResult, reason: FileResult["reason"]): Rec[] {
  res.status = "skipped";
  res.reason = reason;
  return [];
}

export function analyzeFile(path: string, parents: Map<string, Set<string>> = new Map()): FileResult {
  const res = emptyResult(path);
  if (PI_SUBAGENT.test(path)) return skip(res, "child-thread"), (res.harness = "pi"), res;
  if (path.endsWith(".zst")) return skip(res, "compressed"), res;
  const lines = readLines(path, res);
  if (lines.length === 0) return skip(res, "empty"), res;
  let records: Rec[];
  if (lines[0].type === "session") records = piFile(path, lines, res, parents);
  else if (lines[0].type === "session_meta") records = codexFile(path, lines, res);
  else return skip(res, "unknown-format"), res;
  if (res.status === "indexed") split(records, res);
  return res;
}

export function collect(inputs: string[]): string[] {
  const out: string[] = [];
  const walk = (p: string) => {
    if (statSync(p).isDirectory()) for (const n of readdirSync(p).sort()) walk(join(p, n));
    else if (/\.jsonl(\.zst)?$/.test(p)) out.push(resolve(p));
  };
  for (const i of inputs) walk(i);
  return out;
}

export function dryRun(inputs: string[]) {
  const files = collect(inputs);
  // Pi forks point at their parent file; only parents that are part of this run can resolve them.
  const parents = new Map<string, Set<string>>();
  for (const f of files) {
    if (!f.endsWith(".jsonl")) continue;
    const keys = piEntryKeys(f);
    if (keys) parents.set(f, keys);
  }
  const results = files.map((f) => analyzeFile(f, parents));
  const total = emptyResult("(total)");
  const skipped: Record<string, string[]> = {};
  for (const r of results) {
    if (r.status === "skipped") (skipped[r.reason!] ??= []).push(r.path);
    for (const k of ["records", "evidence", "offPath", "inherited", "unresolved", "tornLines", "exchanges", "spans", "oversizedSpans", "digestBytes", "lexicalBytes", "lexicalChunks", "forestInternalNodes"] as const)
      total[k] += r[k];
    for (const [k, v] of Object.entries(r.excluded)) add(total.excluded, k, v);
  }
  const indexed = results.filter((r) => r.status === "indexed");
  return {
    files: results.length,
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
  const report = dryRun(inputs);
  const { perFile: files, ...summary } = report;
  console.log(JSON.stringify(perFile ? report : summary, null, 2));
  void files;
}
