// Lexical-index sample for the FTS comparison (DESIGN §6.3 [待核实], §11 item 4). Local only: runs dryRun()
// with the lexical hook (same dedup, copies, forks and memory exclusion as the dry-run), keeps every lexical
// record of a deterministic sample of whole files (sha256(seed + path) order), and cuts each record's raw
// text into chunks of at most 2048 UTF-8 bytes on character boundaries, as lexical_chunks would be.
//
//   bun eval/sample-lexical.ts --out /tmp/memex-eval/lexical.jsonl --files 150 [--seed s] <file-or-dir>...
//
// Output (mode 0600, keep it outside the repo): a header line (seed, code, totals, sha256 of the chunk lines),
// then one line per chunk: { file, key, kind, part, text }.
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { collect, dryRun } from "../dryrun/dryrun.ts";

const LEXICAL_CHUNK = 2048; // mirrors dryrun.ts
const args = process.argv.slice(2);
const opt = (name: string, dflt?: string) => {
  const i = args.indexOf(name);
  if (i < 0) return dflt;
  const v = args[i + 1];
  args.splice(i, 2);
  return v;
};
const out = opt("--out");
const nFiles = Number(opt("--files", "150"));
const seed = opt("--seed", "memex-fts-1")!;
if (!out || args.length === 0) throw new Error("usage: --out <path> [--files N] [--seed s] <file-or-dir>...");

const rank = (f: string) => createHash("sha256").update(seed + f).digest("hex");
// Only the lowest-ranked files' text is kept (memory): candidates = 3× the wanted count, since some files
// (skipped, empty, duplicate copies) add no lexical records; the first nFiles that do are chosen.
const candidates = new Set(collect(args).files.sort((a, b) => (rank(a) < rank(b) ? -1 : 1)).slice(0, nFiles * 3));
const byFile = new Map<string, { key: string; kind: string; text: string }[]>();
let lexicalFiles = new Set<string>();
const r = dryRun(args, undefined, (file, rec) => {
  lexicalFiles.add(file);
  if (!candidates.has(file)) return;
  (byFile.get(file) ?? byFile.set(file, []).get(file)!).push({ key: rec.key, kind: rec.kind, text: rec.text });
});
const chosen = [...byFile.keys()].sort((a, b) => (rank(a) < rank(b) ? -1 : 1)).slice(0, nFiles);
if (chosen.length < nFiles) throw new Error(`only ${chosen.length} of ${nFiles} files have lexical records among the candidates`);

const enc = new TextEncoder();
function* chunks(text: string) {
  let cur = "";
  let size = 0;
  for (const ch of text) {
    const n = enc.encode(ch).length;
    if (size + n > LEXICAL_CHUNK) {
      yield cur;
      cur = "";
      size = 0;
    }
    cur += ch;
    size += n;
  }
  if (cur) yield cur;
}
const lines: string[] = [];
for (const file of chosen)
  for (const rec of byFile.get(file)!) {
    let part = 0;
    for (const text of chunks(rec.text.replaceAll("\u0000", ""))) lines.push(JSON.stringify({ file, key: rec.key, kind: rec.kind, part: part++, text }));
  }
let code = "unknown";
try {
  code = execFileSync("git", ["-C", import.meta.dir, "rev-parse", "--short", "HEAD"], { encoding: "utf8" }).trim();
  if (execFileSync("git", ["-C", import.meta.dir, "status", "--porcelain", "--", "../dryrun", "."], { encoding: "utf8" }).trim()) code += "+dirty";
} catch {}
const body = lines.join("\n") + "\n";
const header = {
  header: true,
  seed,
  code,
  at: new Date().toISOString(),
  lexicalFiles: lexicalFiles.size,
  chosenFiles: chosen.length,
  chunks: lines.length,
  bytes: Buffer.byteLength(body),
  sha256: createHash("sha256").update(body).digest("hex"),
  lexicalBytesAll: r.total.lexicalBytes,
};
writeFileSync(out, JSON.stringify(header) + "\n" + body, { mode: 0o600 });
console.log(JSON.stringify({ ...header, out }));
