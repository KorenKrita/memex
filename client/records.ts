// Host log → L0 records (DESIGN §4.1, §5.1). Shared by the Pi extension and the Codex plugin: no state, no
// network. A record is one complete JSON line with its native id: Pi `entry.id` (the session header is the
// record `header`), Codex `str(ordinal)` (Paginated only; Legacy files are refused, §4.1 / decision D).
// Only lines that end in "\n" count: the host may still be writing the last one (§5.1).
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

export type Harness = "pi" | "codex";
export type Rec = { id: string; hash: string; raw: string };
export type ReadResult =
  | { ok: true; harness: Harness; nativeId: string; sourceId: string; records: Rec[]; bytes: number }
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

export function readSession(path: string, harness: Harness): ReadResult {
  let buf: Buffer;
  try {
    buf = readFileSync(path);
  } catch {
    return { ok: false, reason: "missing" };
  }
  const end = buf.lastIndexOf(0x0a) + 1; // complete lines only
  const text = buf.subarray(0, end).toString("utf8");
  const lines = text.split("\n").slice(0, -1);
  if (!lines.length) return { ok: false, reason: "empty" };
  const recs: Rec[] = [];
  const seen = new Set<string>();
  let nativeId: string | undefined;
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (!raw.trim()) continue;
    let d: any;
    try {
      d = JSON.parse(raw);
    } catch {
      return { ok: false, reason: "bad-line", detail: `line ${i + 1}` };
    }
    let id: string;
    if (harness === "pi") {
      if (i === 0) {
        if (d?.type !== "session" || typeof d.id !== "string") return { ok: false, reason: "not-a-session" };
        nativeId = d.id;
        id = "header";
      } else if (typeof d?.id === "string") id = d.id;
      else return { ok: false, reason: "bad-line", detail: `line ${i + 1} has no id` };
    } else {
      if (i === 0) {
        if (d?.type !== "session_meta") return { ok: false, reason: "not-a-session" };
        const meta = d.payload ?? {};
        if (meta.history_mode !== "paginated") return { ok: false, reason: "legacy" };
        if (codexIsChild(meta)) return { ok: false, reason: "child-thread" };
        nativeId = String(meta.id);
      }
      if (typeof d?.ordinal !== "number") return { ok: false, reason: "legacy", detail: `line ${i + 1} has no ordinal` };
      id = String(d.ordinal);
    }
    if (seen.has(id)) return { ok: false, reason: "duplicate-id", detail: id };
    seen.add(id);
    recs.push({ id, hash: sha256(raw), raw });
  }
  return { ok: true, harness, nativeId: nativeId!, sourceId: sourceIdOf(harness, nativeId!), records: recs, bytes: end };
}

/** Manifest blocks (§5.1): consecutive records in file order, CHUNK per block; block hash over (id, hash). */
export const CHUNK = 256;
export function blockHashes(recs: { id: string; hash: string }[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < recs.length; i += CHUNK)
    out.push(sha256(recs.slice(i, i + CHUNK).map((r) => `${r.id}\u0000${r.hash}`).join("\n")));
  return out;
}
