// Correction distance of a built bank, measured on dryRun() spans of each question's cut view (DESIGN §8
// "旧陈述 + 更正"). Checked: questions whose type is a correction type (`correction-*`,
// `state-update-other-session`). For each it takes the old claim and the first correction (the same file
// first) and, in the same file, walks the correction's own path (Pi: its parentId chain; Codex: file order)
// from the old claim to the correction, using the reference indexer's records and kinds:
//   same-span            the two records share a span
//   self-adjacent-span   no user record between, and every record between lies in one of the two spans
//   next-exchange        exactly one user record after the old claim, up to and including the correction
//   later-turn           two or more such user records
//   other-session        different file
// The stated type must end with the relation. Different spans with no user record between that are not
// adjacent fit no type and are reported as `nonadjacent-span`. A record mapped to no span, or an old claim
// off the correction's path, is an error. Exits 1 on any error or disagreement. Prints record ids and span
// ids only, no session text.
//
//   bun eval/qbank/span-distance.ts <OUTDIR>
import { readFileSync, realpathSync } from "node:fs";
import { dryRun } from "../../dryrun/dryrun.ts";

const out = realpathSync(process.argv[2]);
const CORR = (t: string) => t.startsWith("correction-") || t === "state-update-other-session";
const qs = readFileSync(`${out}/grader/grading.jsonl`, "utf8").trim().split("\n").map((l) => JSON.parse(l)).filter((q) => CORR(q.type));
const view = (q: any) => `${out}/public/views/${q.cutoff.replaceAll(":", "")}`;
const path = (q: any, f: string) => `${view(q)}/${f.startsWith("codex") ? f : "pi/" + f}`;
// view file|record id -> the indexer's spans holding it and whether it is a user record. A record on a
// prefix shared by several Pi branches can belong to more than one span (one per view); all are kept.
const recs = new Map<string, { spans: Set<string>; user: boolean }>();
for (const v of new Set(qs.map(view))) {
  const files = [...new Set(qs.filter((q) => view(q) === v).flatMap((q) => q.evidence.map((e: any) => path(q, e.file))))];
  dryRun(files, (file, sp) => {
    for (const r of sp.records) {
      const k = `${file}|${r.id}`;
      const e = recs.get(k) ?? recs.set(k, { spans: new Set(), user: false }).get(k)!;
      e.spans.add(sp.id.replace(/.*#/, "#"));
      if (r.kind === "user") e.user = true;
    }
  });
}
const rec = (f: string, id: string) => recs.get(`${realpathSync(f)}|${id}`);
const show = (s: Set<string>) => [...s].sort().join(",");
// Record ids on the path ending at `id`, root first.
const pathTo = (f: string, id: string): string[] => {
  const lines = readFileSync(f, "utf8").split("\n").flatMap((l) => {
    try {
      return [JSON.parse(l)];
    } catch {
      return [];
    }
  });
  if (!f.includes("/codex")) {
    const parent = new Map(lines.filter((d) => d.id).map((d) => [String(d.id), d.parentId as string | null]));
    const ids: string[] = [];
    for (let x: string | null | undefined = id; x; x = parent.get(x)) ids.push(x);
    return ids.reverse();
  }
  const ids = lines.filter((d) => d.ordinal !== undefined).map((d) => String(d.ordinal));
  return ids.slice(0, ids.indexOf(id) + 1);
};
let bad = 0;
for (const q of qs) {
  try {
    const o = q.evidence.find((e: any) => e.use === "old-claim");
    const c = q.evidence.filter((e: any) => e.use === "correction")
      .sort((a: any, b: any) => (a.file === o.file ? 0 : 1) - (b.file === o.file ? 0 : 1) || a.line - b.line)[0];
    if (!o || !c) throw new Error(`${q.id}: needs old-claim and correction evidence`);
    const fo = path(q, o.file), fc = path(q, c.file);
    const ro = rec(fo, o.entry), rc = rec(fc, c.entry);
    if (!ro || !rc) throw new Error(`${q.id}: record ${ro ? c.entry : o.entry} maps to no span`);
    let rel: string, between = -1;
    if (c.file !== o.file) rel = "other-session";
    else {
      const p = pathTo(fc, c.entry);
      const i = p.indexOf(o.entry);
      if (i < 0) throw new Error(`${q.id}: old claim ${o.entry} is not on the correction's path`);
      const mid = p.slice(i + 1).map((id) => rec(fc, id)).filter((r) => r !== undefined);
      between = mid.filter((r) => r.user).length;
      const own = (r: { spans: Set<string> }) => [...r.spans].some((s) => ro.spans.has(s) || rc.spans.has(s));
      if ([...rc.spans].some((s) => ro.spans.has(s))) rel = "same-span";
      else if (between === 0) rel = mid.slice(0, -1).every(own) ? "self-adjacent-span" : "nonadjacent-span";
      else rel = between === 1 ? "next-exchange" : "later-turn";
    }
    const ok = q.type.endsWith(rel);
    if (!ok) bad++;
    console.log([q.id, q.type, `${o.entry}${show(ro.spans)}`, `${c.entry}${show(rc.spans)}`, between, rel, ok ? "ok" : "MISMATCH"].join("\t"));
  } catch (e) {
    bad++;
    console.log(String((e as Error).message));
  }
}
console.log(`${qs.length} correction questions, ${bad} errors or mismatches`);
process.exit(bad ? 1 : 0);
