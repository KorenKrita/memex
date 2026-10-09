// Sample real spans from session logs (DESIGN §9 step 2, §6.3 [待核实] lexical comparison). Local only:
// runs dryRun() (copies, fork parents, snapshot checks as in the dry-run) and writes the sampled spans as
// JSONL to the output path. Keep the output outside the repo: it is real session text.
// Sampling is deterministic for a given snapshot: spans are ranked by sha256(seed + span id).
//
//   bun eval/sample-spans.ts --out /tmp/memex-eval/spans.jsonl --n 400 [--seed s] <file-or-dir>...
//
// Each line: the span id (`<file>#sN`, valid for this partition only), its file, the file's content hash at
// sampling time, prefix flag, every record (id, canonical key, kind, digest text) and the same-path <context>
// records (DESIGN §4.3). A header line carries seed, code version, totals and skip counts.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dryRun, type SpanRecord } from "../dryrun/dryrun.ts";

const args = process.argv.slice(2);
const opt = (name: string, dflt?: string) => {
  const i = args.indexOf(name);
  if (i < 0) return dflt;
  const v = args[i + 1];
  args.splice(i, 2);
  return v;
};
const out = opt("--out");
const n = Number(opt("--n", "400"));
const seed = opt("--seed", "memex-eval-1")!;
if (!out || args.length === 0) throw new Error("usage: --out <path> [--n N] [--seed s] <file-or-dir>...");

type Span = { id: string; file: string; prefix: boolean; records: SpanRecord[]; context: SpanRecord[]; bytes: number };
const rank = (id: string) => createHash("sha256").update(seed + id).digest("hex");
// Keep the n lowest-ranked spans (bounded memory over all logs).
const kept: { r: string; s: Span }[] = [];
let seen = 0;
const r = dryRun(args, (file, sp) => {
  seen++;
  const rk = rank(sp.id);
  if (kept.length >= n && rk >= kept[kept.length - 1].r) return;
  const bytes = sp.records.reduce((a, x) => a + Buffer.byteLength(x.digest) + 1, 0);
  kept.push({ r: rk, s: { id: sp.id, file, prefix: sp.prefix, records: sp.records, context: sp.context, bytes } });
  kept.sort((a, b) => (a.r < b.r ? -1 : 1));
  if (kept.length > n) kept.pop();
});
if (seen !== r.total.spans) throw new Error(`span hook saw ${seen}, dryRun counted ${r.total.spans}`);
const harness = new Map(r.perFile.map((f) => [f.path, f.harness]));
const fileHash = new Map<string, string>();
const hashOf = (f: string) => {
  // hash at write time, not at dryRun's read: a file still being written can differ from what was sampled
  if (!fileHash.has(f)) fileHash.set(f, createHash("sha256").update(readFileSync(f)).digest("hex"));
  return fileHash.get(f)!;
};
let code = "unknown";
try {
  code = execFileSync("git", ["-C", import.meta.dir, "rev-parse", "--short", "HEAD"], { encoding: "utf8" }).trim();
  if (execFileSync("git", ["-C", import.meta.dir, "status", "--porcelain", "--", "../dryrun", "."], { encoding: "utf8" }).trim()) code += "+dirty";
} catch {}
const header = {
  header: true,
  seed,
  code,
  at: new Date().toISOString(),
  files: r.files,
  indexed: r.indexed,
  skipped: Object.fromEntries(Object.entries(r.skipped).map(([k, v]) => [k, (v as string[]).length])),
  forks: r.forks,
  spansCounted: r.total.spans,
  sampled: kept.length,
};
const lines = [JSON.stringify(header), ...kept.map((k) => JSON.stringify({ ...k.s, harness: harness.get(k.s.file), fileSha256: hashOf(k.s.file) }))];
writeFileSync(out, lines.join("\n") + "\n", { mode: 0o600 });
console.log(JSON.stringify({ ...header, out }));
