// Host log → L0 records (DESIGN §4.1, §5.1). Shared by the Pi extension and the Codex plugin: no state, no
// network. A record is one complete JSON line with its native id: Pi `entry.id` (the session header is the
// record `header`), Codex `str(ordinal)` (Paginated only; Legacy files are refused, §4.1 / decision D).
// Only lines that end in "\n" count: the host may still be writing the last one (§5.1).
import { createHash } from "node:crypto";
import { closeSync, fstatSync, openSync, readFileSync, readSync } from "node:fs";

export type Harness = "pi" | "codex";
export type Rec = { id: string; hash: string; raw: string };
export type ReadResult =
  | { ok: true; harness: Harness; nativeId: string; sourceId: string; records: Rec[]; bytes: number; parentPath?: string; parentNative?: string }
  | { ok: false; reason: "missing" | "empty" | "legacy" | "child-thread" | "not-a-session" | "bad-line" | "duplicate-id"; detail?: string };

export const sha256 = (s: string | Buffer) => createHash("sha256").update(s).digest("hex");
/** source_id = sha256(harness, native_session_id): stable across machines and re-imports (§4.1). */
export const sourceIdOf = (harness: Harness, nativeId: string) => sha256(`${harness}\u0000${nativeId}`);

/** Codex child threads (subagent, guardian review) are not indexed on their own (decision A). */
export function codexIsChild(meta: Record<string, any>): boolean {
  const ts = meta.thread_source;
  if (ts !== undefined && ts !== null) return ts === "subagent" || ts === "guardian_review";
  const s = meta.source;
  return !!s && typeof s === "object" && ("subagent" in s || s.internal === "guardian");
}

type Parsed =
  | { ok: true; records: Rec[]; ends: number[]; nativeId?: string; parentPath?: string; parentNative?: string }
  | { ok: false; reason: "legacy" | "child-thread" | "not-a-session" | "bad-line" | "duplicate-id"; detail?: string };
/** Parse complete lines into records. `first` is the record index of the first non-blank line (0: it is the
 *  header). `starts[i]` is the byte offset of line i, for `ends` (byte just past each record's newline). */
function parse(lines: string[], harness: Harness, first: number, starts: number[]): Parsed {
  const recs: Rec[] = [];
  const ends: number[] = [];
  const seen = new Set<string>();
  let nativeId: string | undefined;
  let parentPath: string | undefined; // Pi: the parent file named by the header (its id is read separately)
  let parentNative: string | undefined; // Codex: forked_from_id
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (!raw.trim()) continue;
    const idx = first + recs.length;
    let d: any;
    try {
      d = JSON.parse(raw);
    } catch {
      return { ok: false, reason: "bad-line", detail: `record ${idx}` };
    }
    let id: string;
    if (harness === "pi") {
      if (idx === 0) {
        if (d?.type !== "session" || typeof d.id !== "string") return { ok: false, reason: "not-a-session" };
        nativeId = d.id;
        if (typeof d.parentSession === "string" && d.parentSession) parentPath = d.parentSession;
        id = "header";
      } else if (typeof d?.id === "string") id = d.id;
      else return { ok: false, reason: "bad-line", detail: `record ${idx} has no id` };
    } else {
      if (idx === 0) {
        if (d?.type !== "session_meta") return { ok: false, reason: "not-a-session" };
        const meta = d.payload ?? {};
        if (meta.history_mode !== "paginated") return { ok: false, reason: "legacy" };
        if (codexIsChild(meta)) return { ok: false, reason: "child-thread" };
        nativeId = String(meta.id);
        if (typeof meta.forked_from_id === "string" && meta.forked_from_id) parentNative = meta.forked_from_id;
      }
      if (typeof d?.ordinal !== "number") return { ok: false, reason: "legacy", detail: `record ${idx} has no ordinal` };
      id = String(d.ordinal);
    }
    if (seen.has(id)) return { ok: false, reason: "duplicate-id", detail: id };
    seen.add(id);
    recs.push({ id, hash: sha256(raw), raw });
    ends.push(starts[i] + Buffer.byteLength(raw) + 1);
  }
  return { ok: true, records: recs, ends, nativeId, parentPath, parentNative };
}
const splitLines = (buf: Buffer, at: number) => {
  const lines = buf.toString("utf8").split("\n").slice(0, -1);
  const starts: number[] = [];
  let p = at;
  for (const l of lines) starts.push(p), (p += Buffer.byteLength(l) + 1);
  return { lines, starts };
};

export function readSession(path: string, harness: Harness): ReadResult {
  let buf: Buffer;
  try {
    buf = readFileSync(path);
  } catch {
    return { ok: false, reason: "missing" };
  }
  const end = buf.lastIndexOf(0x0a) + 1; // complete lines only
  const { lines, starts } = splitLines(buf.subarray(0, end), 0);
  if (!lines.some((l) => l.trim())) return { ok: false, reason: "empty" };
  const r = parse(lines, harness, 0, starts);
  if (!r.ok) return r;
  return { ok: true, harness, nativeId: r.nativeId!, sourceId: sourceIdOf(harness, r.nativeId!), records: r.records, bytes: end,
           parentPath: r.parentPath, parentNative: r.parentNative };
}

/** The header record alone (identity and fork parent), without reading the rest of the file. */
export function readHead(path: string, harness: Harness) {
  let fd: number;
  try {
    fd = openSync(path, "r");
  } catch {
    return { ok: false, reason: "missing" } as const;
  }
  try {
    let buf = Buffer.alloc(0);
    for (let at = 0; ; ) {
      const chunk = Buffer.alloc(64 * 1024);
      const n = readSync(fd, chunk, 0, chunk.length, at);
      const nl = chunk.subarray(0, n).indexOf(0x0a);
      buf = Buffer.concat([buf, chunk.subarray(0, nl < 0 ? n : nl + 1)]);
      if (nl >= 0) break;
      if (n === 0 || buf.length > MAX_LINE) return { ok: false, reason: "empty" } as const;
      at += n;
    }
    const { lines, starts } = splitLines(buf, 0);
    if (!lines[0]?.trim()) return { ok: false, reason: "not-a-session" } as const;
    const r = parse(lines, harness, 0, starts);
    if (!r.ok) return r;
    return { ok: true, nativeId: r.nativeId!, sourceId: sourceIdOf(harness, r.nativeId!), header: r.records[0],
             parentPath: r.parentPath, parentNative: r.parentNative } as const;
  } finally {
    closeSync(fd);
  }
}

export const MAX_LINE = 6 * 1024 * 1024;
/** One bounded piece of a file for a scan segment (DESIGN §5.2): the complete lines in [from, min(bound,
 *  from + maxBytes)), whose first record has index `first`. `prev`: hash of the record that ends at `from`
 *  (null when from = 0, undefined when no record ends there: the file was replaced or rewritten before it).
 *  `eof`: no complete line follows `to` in the file as it is now. `last`: no complete line ends between `to`
 *  and the bound (the bound may fall inside a line still being written: that line belongs to a later round). */
export function readRange(path: string, harness: Harness, o: { from: number; bound: number; first: number; maxBytes: number }) {
  let fd: number;
  try {
    fd = openSync(path, "r");
  } catch {
    return { ok: false, reason: "missing" } as const;
  }
  try {
    const size = fstatSync(fd).size;
    const read = (at: number, n: number) => {
      const b = Buffer.alloc(Math.max(0, n));
      const got = readSync(fd, b, 0, b.length, at);
      return b.subarray(0, got);
    };
    let prev: string | null | undefined = null;
    if (o.from > 0) {
      prev = undefined;
      if (o.from <= size && read(o.from - 1, 1)[0] === 0x0a) {
        // Back from `from - 1` to the previous newline: that line is the record before `from`.
        let start = o.from - 1;
        while (start > 0 && o.from - start <= MAX_LINE) {
          const n = Math.min(64 * 1024, start);
          const nl = read(start - n, n).lastIndexOf(0x0a);
          if (nl >= 0) {
            start = start - n + nl + 1;
            break;
          }
          start -= n;
        }
        const raw = read(start, o.from - 1 - start).toString("utf8");
        if (raw.trim()) prev = sha256(raw);
      }
    }
    const stop = Math.min(o.bound, size, o.from + o.maxBytes);
    const buf = read(o.from, stop - o.from);
    const end = buf.lastIndexOf(0x0a) + 1;
    if (!end && stop < Math.min(o.bound, size)) return { ok: false, reason: "record-too-large" } as const;
    const { lines, starts } = splitLines(buf.subarray(0, end), o.from);
    const r = parse(lines, harness, o.first, starts);
    if (!r.ok) return r;
    const to = o.from + end;
    let eof = true;
    for (let at = to; at < size && eof; at += 64 * 1024) if (read(at, 64 * 1024).includes(0x0a)) eof = false;
    const last = stop >= Math.min(o.bound, size);
    return { ok: true, prev, records: r.records, ends: r.ends, to, eof, last } as const;
  } finally {
    closeSync(fd);
  }
}

/** Manifest blocks (§5.1): consecutive records in file order, CHUNK per block; block hash over (id, hash). */
export const CHUNK = 256;
export function blockHashes(recs: { id: string; hash: string }[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < recs.length; i += CHUNK)
    out.push(sha256(recs.slice(i, i + CHUNK).map((r) => `${r.id}\u0000${r.hash}`).join("\n")));
  return out;
}

/** Native id of a Pi session file from its header line only (fork parents, §4.1). */
export function piHeaderId(path: string): string | undefined {
  try {
    const buf = readFileSync(path);
    const nl = buf.indexOf(0x0a);
    const d = JSON.parse(buf.subarray(0, nl < 0 ? buf.length : nl).toString("utf8"));
    return d?.type === "session" && typeof d.id === "string" && d.id ? d.id : undefined;
  } catch {
    return undefined;
  }
}
