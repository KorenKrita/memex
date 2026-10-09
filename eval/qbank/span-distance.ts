// Correction distance of a built bank, measured on dryRun() spans (DESIGN §8 "旧陈述 + 更正"). For every
// question with old-claim evidence prints the question's type, the span of the old claim, the span of the
// first correction (or of the first answer when there is no correction), the number of user records
// between them in the same file, and whether the stated type agrees:
//   correction-next-exchange   same file, different span, at most one user record between
//   correction-later-turn      same file, two or more user records between
//   *-other-session            different file
// Exits 1 on any disagreement. Prints record ids and span ids only, no session text.
//
//   bun eval/qbank/span-distance.ts <corpus-dir> <OUTDIR>/grader/grading.jsonl
import { readFileSync, realpathSync } from "node:fs";
import { dryRun } from "../../dryrun/dryrun.ts";

const corpus = realpathSync(process.argv[2]);
const qs = readFileSync(process.argv[3], "utf8").trim().split("\n").map((l) => JSON.parse(l))
  .filter((q) => q.evidence.some((e: any) => e.use === "old-claim"));
const path = (f: string) => `${corpus}/${f.startsWith("codex") ? f : "pi/" + f}`;
const files = [...new Set(qs.flatMap((q) => q.evidence.map((e: any) => path(e.file))))];
const spanOf = new Map<string, string>(); // file|record id -> first span containing it
dryRun(files, (file, sp) => {
  for (const r of sp.records) if (!spanOf.has(`${file}|${r.id}`)) spanOf.set(`${file}|${r.id}`, sp.id.replace(/.*#/, "#"));
});
const userLines = new Map<string, number[]>();
const users = (f: string) => {
  if (!userLines.has(f)) {
    const out: number[] = [];
    readFileSync(f, "utf8").split("\n").forEach((l, n) => {
      try {
        const d = JSON.parse(l);
        const p = d.payload ?? {};
        if (d.type === "message" && d.message?.role === "user") out.push(n);
        else if (d.type === "response_item" && p.type === "message" && p.role === "user") {
          const t = (p.content ?? []).map((c: any) => c.text ?? "").join("");
          if (!t.startsWith("<") && !t.startsWith("# AGENTS")) out.push(n);
        }
      } catch {}
    });
    userLines.set(f, out);
  }
  return userLines.get(f)!;
};
let bad = 0;
for (const q of qs) {
  const o = q.evidence.find((e: any) => e.use === "old-claim");
  const cands = q.evidence.filter((e: any) => e.use === "correction");
  const c = (cands.length ? cands : q.evidence.filter((e: any) => e.use === "answer"))
    .sort((a: any, b: any) => (a.file === o.file ? 0 : 1) - (b.file === o.file ? 0 : 1) || a.line - b.line)[0];
  const so = spanOf.get(`${path(o.file)}|${o.entry}`) ?? "?";
  const sc = spanOf.get(`${path(c.file)}|${c.entry}`) ?? "?";
  let rel: string, between = -1;
  if (c.file !== o.file) rel = "other-session";
  else {
    between = users(path(o.file)).filter((n) => n > o.line && n <= c.line).length;
    rel = so === sc ? "same-span" : between <= 1 ? "next-exchange" : "later-turn";
  }
  const t: string = q.type;
  const ok = !t.startsWith("correction") && t !== "state-update-other-session" ? true : t.endsWith(rel);
  if (!ok) bad++;
  console.log([q.id, t, `${o.entry}${so}`, `${c.entry}${sc}`, between, rel, ok ? "ok" : "MISMATCH"].join("\t"));
}
process.exit(bad ? 1 : 0);
