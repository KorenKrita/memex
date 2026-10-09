// pg_trgm vs ParadeDB pg_search on real span text (DESIGN §6.3 [待核实], §11 item 4).
//
// Known-item lexical retrieval: queries are cut deterministically from the corpus (a span = the target);
// the relevant set of a query is every span containing it (case-insensitive substring; for two-term
// queries, both). This measures what the raw-text channel is for (paths, identifiers, error strings, rare
// Chinese words); it says nothing about paraphrased questions (the embedding channel's job).
//
//   docker run -d --name memex-fts-test -e POSTGRES_PASSWORD=memex -e POSTGRES_DB=memex \
//     -p 127.0.0.1:55433:5432 --tmpfs /var/lib/postgresql paradedb/paradedb:latest
//   bun eval/fts-compare.ts /tmp/memex-eval/corpus.jsonl /tmp/memex-eval/fts-result.json
import { SQL } from "bun";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";

const [corpusPath, outPath] = process.argv.slice(2);
if (!corpusPath || !outPath) throw new Error("usage: fts-compare.ts <corpus.jsonl> <out.json>");
const sql = new SQL(process.env.MEMEX_FTS_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex");
const K = 10;
const PER_CLASS = 40;

// ---------- corpus ----------
const docs: string[] = readFileSync(corpusPath, "utf8")
  .trim()
  .split("\n")
  .map((l) => JSON.parse(l).text as string)
  .map((t) => t.replaceAll("\u0000", ""));
const lower = docs.map((d) => d.toLowerCase());
const has = (i: number, q: string) => lower[i].includes(q.toLowerCase());
const df = (q: string) => lower.reduce((a, _, i) => a + (has(i, q) ? 1 : 0), 0);

// ---------- queries ----------
type Q = { cls: string; q: string; terms: string[]; relevant: number[]; target?: number; note?: string };
const h = (s: string) => createHash("sha256").update(s).digest("hex");
const order = docs.map((_, i) => i).sort((a, b) => (h(`q${a}`) < h(`q${b}`) ? -1 : 1)); // deterministic target order
const seen = new Set<string>();
const queries: Q[] = [];
const relevantOf = (terms: string[]) => docs.map((_, i) => i).filter((i) => terms.every((t) => has(i, t)));
function addFrom(cls: string, pick: (text: string) => string[], opts: { maxDf?: number; minDf?: number } = {}) {
  let n = 0;
  for (const i of order) {
    if (n >= PER_CLASS) break;
    for (const cand of pick(docs[i])) {
      const key = cls + "\u0000" + cand.toLowerCase();
      if (seen.has(key)) continue;
      const d = df(cand);
      if (d < (opts.minDf ?? 1) || d > (opts.maxDf ?? 5)) continue;
      seen.add(key);
      queries.push({ cls, q: cand, terms: [cand], relevant: relevantOf([cand]), target: i });
      n++;
      break;
    }
  }
}
const uniq = (xs: string[]) => [...new Set(xs)];
// Chinese: runs of 2–6 Han characters inside longer Han text (a word or phrase as one would type it)
addFrom("zh", (t) => uniq([...t.matchAll(/[\p{Script=Han}]{4,}/gu)].flatMap((m) => [m[0].slice(0, 4), m[0].slice(1, 3), m[0].slice(0, 6)]).filter((s) => s.length >= 2)));
// identifiers: camelCase / snake_case / kebab-case tokens
addFrom("ident", (t) => uniq([...t.matchAll(/\b[A-Za-z][A-Za-z0-9]*(?:[A-Z][a-z0-9]+|_[a-z0-9]+|-[a-z0-9]+)+\b/g)].map((m) => m[0]).filter((s) => s.length >= 8)));
// paths: two or more segments with a slash and a file-like tail
addFrom("path", (t) => uniq([...t.matchAll(/(?:~|\.{0,2})?\/?(?:[\w.-]+\/)+[\w.-]+\.[a-z]{1,5}\b/g)].map((m) => m[0]).filter((s) => s.length >= 10)));
// error strings: 3–5 words around error/failed/exception
addFrom("error", (t) =>
  uniq([...t.matchAll(/(?:[\w'`.:-]+ ){0,2}(?:error|Error|failed|Failed|exception|Exception|denied|not found)(?: [\w'`.:-]+){1,2}/g)].map((m) => m[0].trim()).filter((s) => s.length >= 12)),
);
// identifier fragment: an inner camelCase / snake_case part (≥ 6 chars) of a longer identifier
addFrom(
  "ident-part",
  (t) =>
    uniq(
      [...t.matchAll(/\b[a-z][a-z0-9]*(?:[A-Z][a-z0-9]+){2,}\b/g)].flatMap((m) => m[0].split(/(?=[A-Z])/).slice(1).filter((p) => p.length >= 6)),
    ),
  { maxDf: 8 },
);
// two terms from one span, not adjacent: a Chinese word and an identifier
{
  let n = 0;
  for (const i of order) {
    if (n >= PER_CLASS) break;
    const zh = [...docs[i].matchAll(/[\p{Script=Han}]{2,4}/gu)].map((m) => m[0]).filter((s) => df(s) <= 200 && df(s) >= 2);
    const id = [...docs[i].matchAll(/\b[A-Za-z][A-Za-z0-9_]{5,}\b/g)].map((m) => m[0]).filter((s) => df(s) <= 200 && df(s) >= 2);
    if (!zh.length || !id.length) continue;
    const terms = [zh[0], id[Math.floor(id.length / 2)]];
    const rel = relevantOf(terms);
    if (rel.length > 5 || seen.has("mixed" + terms.join("|"))) continue;
    seen.add("mixed" + terms.join("|"));
    queries.push({ cls: "mixed", q: terms.join(" "), terms, relevant: rel, target: i });
    n++;
  }
}
// typo: one inner letter of a rare identifier dropped; relevant = spans containing the original
{
  let n = 0;
  for (const q of queries.filter((x) => x.cls === "ident")) {
    if (n >= PER_CLASS) break;
    const s = q.q;
    const at = Math.floor(s.length / 2);
    const typo = s.slice(0, at) + s.slice(at + 1);
    if (df(typo) > 0) continue;
    queries.push({ cls: "typo", q: typo, terms: [typo], relevant: q.relevant, target: q.target, note: `from ${s}` });
    n++;
  }
}
// negatives: absent strings that look real (identifier with a swapped segment, absent Chinese phrase)
{
  const ids = queries.filter((x) => x.cls === "ident").map((x) => x.q);
  const zhs = queries.filter((x) => x.cls === "zh").map((x) => x.q);
  let n = 0;
  for (let k = 0; k < ids.length && n < PER_CLASS / 2; k++) {
    const a = ids[k].split(/(?=[A-Z_-])/);
    const b = ids[(k + 7) % ids.length].split(/(?=[A-Z_-])/);
    if (a.length < 2 || b.length < 2) continue;
    const neg = a[0] + b[b.length - 1];
    if (df(neg) > 0) continue;
    queries.push({ cls: "negative", q: neg, terms: [neg], relevant: [] });
    n++;
  }
  for (let k = 0; k + 1 < zhs.length && n < PER_CLASS; k += 2) {
    const neg = zhs[k].slice(0, 2) + zhs[k + 1].slice(-2);
    if (neg.length < 4 || df(neg) > 0) continue;
    queries.push({ cls: "negative", q: neg, terms: [neg], relevant: [] });
    n++;
  }
}

// ---------- database ----------
await sql`drop table if exists spans`;
await sql`create table spans (id int primary key, body text not null)`;
for (let i = 0; i < docs.length; i += 200) {
  const rows = docs.slice(i, i + 200).map((body, j) => ({ id: i + j, body }));
  await sql`insert into spans ${sql(rows)}`;
}
await sql`create extension if not exists pg_trgm`;
await sql`create extension if not exists pg_search`;
const t0 = performance.now();
await sql`create index spans_trgm on spans using gin (body gin_trgm_ops)`;
const trgmBuild = performance.now() - t0;
const t1 = performance.now();
await sql.unsafe(`create index spans_bm25 on spans using paradedb (
  id,
  (body::pdb.jieba),
  (body::pdb.chinese_compatible('alias=cc')),
  (body::pdb.source_code('alias=code')),
  (body::pdb.ngram(2,3,'alias=ng'))
) with (key_field = 'id')`);
const bm25Build = performance.now() - t1;
await sql`analyze spans`;
const [sizes] = await sql`select pg_relation_size('spans_trgm') trgm, pg_relation_size('spans_bm25') bm25, pg_total_relation_size('spans') - pg_indexes_size('spans') heap`;

// ---------- methods ----------
// Each returns the top-K span ids for a query. Multi-term queries: every term must match (AND).
type Method = { name: string; run: (q: Q) => Promise<number[]> };
const ilikeAll = (q: Q) => q.terms.map((_, k) => `body ilike '%' || $${k + 1} || '%'`).join(" and ");
const esc = (s: string) => s.replace(/[\\%_]/g, (c) => "\\" + c);
const bm25 = (field: string, op: string) => async (q: Q) => {
  const where = q.terms.map((_, k) => `${field} ${op} $${k + 1}`).join(" and ");
  const rows = await sql.unsafe(`select id from spans where ${where} order by pdb.score(id) desc, id limit ${K}`, q.terms);
  return rows.map((r: any) => r.id);
};
const methods: Method[] = [
  {
    // exact substring, ranked by trigram word similarity (the index serves ILIKE for patterns of ≥ 3 chars)
    name: "trgm-ilike",
    run: async (q) => {
      const rank = q.terms.map((_, k) => `word_similarity($${k + 1}, body)`).join(" + ");
      const rows = await sql.unsafe(`select id from spans where ${ilikeAll(q)} order by ${rank} desc, id limit ${K}`, q.terms.map(esc));
      return rows.map((r: any) => r.id);
    },
  },
  {
    // fuzzy fallback: word similarity above the default threshold (0.6), every term
    name: "trgm-fuzzy",
    run: async (q) => {
      const where = q.terms.map((_, k) => `$${k + 1} <% body`).join(" and ");
      const rank = q.terms.map((_, k) => `word_similarity($${k + 1}, body)`).join(" + ");
      const rows = await sql.unsafe(`select id from spans where ${where} order by ${rank} desc, id limit ${K}`, q.terms);
      return rows.map((r: any) => r.id);
    },
  },
  { name: "jieba-or", run: bm25("body", "|||") },
  { name: "jieba-and", run: bm25("body", "&&&") },
  { name: "cc-and", run: bm25("body::pdb.alias('cc')", "&&&") },
  { name: "code-and", run: bm25("body::pdb.alias('code')", "&&&") },
  { name: "ngram-and", run: bm25("body::pdb.alias('ng')", "&&&") },
];

// ---------- run ----------
const per: Record<string, Record<string, { hit: number; recall: number; mrr: number; fp: number; n: number; ms: number[]; errors: number }>> = {};
const samples: Record<string, { cls: string; q: string; relevant: number; got: Record<string, number[]> }[]> = {};
for (const m of methods) for (const q of queries.slice(0, 5)) await m.run(q).catch(() => {}); // warm up
for (const q of queries) {
  const rel = new Set(q.relevant);
  const got: Record<string, number[]> = {};
  for (const m of methods) {
    const c = ((per[m.name] ??= {})[q.cls] ??= { hit: 0, recall: 0, mrr: 0, fp: 0, n: 0, ms: [], errors: 0 });
    c.n++;
    const t = performance.now();
    let ids: number[] = [];
    try {
      ids = await m.run(q);
    } catch {
      c.errors++;
    }
    c.ms.push(performance.now() - t);
    got[m.name] = ids;
    if (rel.size === 0) {
      c.fp += ids.length > 0 ? 1 : 0; // any result for an absent string is a false hit
      continue;
    }
    const first = ids.findIndex((id) => rel.has(id));
    c.hit += first >= 0 ? 1 : 0;
    c.mrr += first >= 0 ? 1 / (first + 1) : 0;
    c.recall += ids.filter((id) => rel.has(id)).length / Math.min(rel.size, K);
  }
  (samples[q.cls] ??= []).length < 3 && samples[q.cls].push({ cls: q.cls, q: q.q, relevant: q.relevant.length, got });
}
const pct = (xs: number[], p: number) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * p))];
const table: Record<string, Record<string, string>> = {};
for (const [m, byCls] of Object.entries(per))
  for (const [cls, c] of Object.entries(byCls)) {
    const pos = c.n;
    (table[cls] ??= {})[m] =
      cls === "negative"
        ? `falseHit ${c.fp}/${pos} p50 ${pct(c.ms, 0.5).toFixed(1)}ms${c.errors ? ` err ${c.errors}` : ""}`
        : `hit@${K} ${((100 * c.hit) / pos).toFixed(0)}% recall ${((100 * c.recall) / pos).toFixed(0)}% mrr ${(c.mrr / pos).toFixed(2)} p50 ${pct(c.ms, 0.5).toFixed(1)}ms p95 ${pct(c.ms, 0.95).toFixed(1)}ms${c.errors ? ` err ${c.errors}` : ""}`;
  }
const counts = Object.fromEntries(Object.entries(Object.groupBy(queries, (q) => q.cls)).map(([k, v]) => [k, v!.length]));
const result = {
  corpus: { spans: docs.length, bytes: docs.reduce((a, d) => a + Buffer.byteLength(d), 0) },
  build: { trgmMs: Math.round(trgmBuild), bm25Ms: Math.round(bm25Build), ...Object.fromEntries(Object.entries(sizes).map(([k, v]) => [k + "Bytes", Number(v)])) },
  queries: counts,
  table,
  queriesUsed: queries.map(({ cls, q, relevant, note }) => ({ cls, q, relevant: relevant.length, note })),
  samples,
};
writeFileSync(outPath, JSON.stringify(result, null, 2), { mode: 0o600 });
console.log(JSON.stringify({ corpus: result.corpus, build: result.build, queries: counts }, null, 1));
for (const [cls, row] of Object.entries(table)) {
  console.log(`\n[${cls}]`);
  for (const [m, v] of Object.entries(row)) console.log(`  ${m.padEnd(11)} ${v}`);
}
await sql.close();
