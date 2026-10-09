// Correction distance of a built bank, measured on dryRun() spans of each question's cut view (DESIGN §8
// "旧陈述 + 更正"). Checked: questions whose type is a correction type (`correction-*`,
// `state-update-other-session`). For each it reports the span of the old claim, the span of the first
// correction (the same file first), the user records between them, and the relation:
//   same-span            same file, same span
//   self-adjacent-span   same file, different span, no user record between (the agent corrects itself)
//   next-exchange        same file, different span, exactly one user record between
//   later-turn           same file, two or more user records between
//   other-session        different file
// The stated type must end with the relation. A record mapped to no span is an error. User records are
// counted in file order between the two lines, which on a branched Pi file can include other branches.
// Exits 1 on any error or disagreement. Prints record ids and span ids only, no session text.
//
//   bun eval/qbank/span-distance.ts <OUTDIR>
import { readFileSync, realpathSync } from "node:fs";
import { dryRun } from "../../dryrun/dryrun.ts";

const out = realpathSync(process.argv[2]);
const CORR = (t: string) => t.startsWith("correction-") || t === "state-update-other-session";
const qs = readFileSync(`${out}/grader/grading.jsonl`, "utf8").trim().split("\n").map((l) => JSON.parse(l)).filter((q) => CORR(q.type));
const view = (q: any) => `${out}/public/views/${q.cutoff.replaceAll(":", "")}`;
const path = (q: any, f: string) => `${view(q)}/${f.startsWith("codex") ? f : "pi/" + f}`;
const spans = new Map<string, Set<string>>(); // view file|record id -> span ids
for (const v of new Set(qs.map(view))) {
  const files = [...new Set(qs.filter((q) => view(q) === v).flatMap((q) => q.evidence.map((e: any) => path(q, e.file))))];
  dryRun(files, (file, sp) => {
    for (const r of sp.records) {
      const k = `${file}|${r.id}`;
      (spans.get(k) ?? spans.set(k, new Set()).get(k)!).add(sp.id.replace(/.*#/, "#"));
    }
  });
}
// A record on a shared prefix of several Pi branches can belong to more than one span (one per view); all are
// kept, and "same span" means the correction's span is one of them.
const span = (q: any, e: any) => {
  const s = spans.get(`${realpathSync(path(q, e.file))}|${e.entry}`);
  if (!s || s.size === 0) throw new Error(`${q.id}: record ${e.entry} maps to no span`);
  return s;
};
const show = (s: Set<string>) => [...s].sort().join(",");
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
  try {
    const o = q.evidence.find((e: any) => e.use === "old-claim");
    const c = q.evidence.filter((e: any) => e.use === "correction")
      .sort((a: any, b: any) => (a.file === o.file ? 0 : 1) - (b.file === o.file ? 0 : 1) || a.line - b.line)[0];
    if (!o || !c) throw new Error(`${q.id}: needs old-claim and correction evidence`);
    const so = span(q, o), sc = span(q, c);
    let rel: string, between = -1;
    if (c.file !== o.file) rel = "other-session";
    else {
      between = users(path(q, o.file)).filter((n) => n > o.line && n <= c.line).length;
      const same = [...sc].some((s) => so.has(s));
      rel = same ? "same-span" : between === 0 ? "self-adjacent-span" : between === 1 ? "next-exchange" : "later-turn";
    }
    const ok = q.type.endsWith(rel);
    if (!ok) bad++;
    console.log([q.id, q.type, `${o.entry}${show(so)}`, `${c.entry}${show(sc)}`, between, rel, ok ? "ok" : "MISMATCH"].join("\t"));
  } catch (e) {
    bad++;
    console.log(String((e as Error).message));
  }
}
console.log(`${qs.length} correction questions, ${bad} errors or mismatches`);
process.exit(bad ? 1 : 0);
