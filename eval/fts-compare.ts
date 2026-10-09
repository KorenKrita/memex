// pg_trgm vs ParadeDB pg_search on a lexical-index sample (DESIGN §6.3 [待核实], §11 item 4).
//
// Corpus: eval/sample-lexical.ts output (raw record text in ≤ 2 KB chunks, as lexical_chunks would hold it).
// Queries: a fixed, hand-checked file [{ cls, terms, intended? }]. Relevance:
//   - literal classes (zh, path, ident, error, ident-part, terms): chunks containing every term
//     (case-insensitive substring), complete by construction over this corpus;
//   - typo: chunks containing the `intended` spelling;
//   - neg: no chunk is relevant; any result counts as a false hit.
// Reported per class and method: hit@10 (any relevant in top 10), P@10 = relevant in top 10 / min(|rel|, 10),
// MRR, latency p50/p95. Any query error aborts the run (exit 1): a failed query is not an empty result.
// Tables live in schema fts_v2, created fresh; nothing else in the database is touched.
//
//   bun eval/fts-compare.ts /tmp/memex-eval/lexical.jsonl /tmp/memex-eval/fts-queries.json /tmp/memex-eval/fts-v2.json
import { SQL } from "bun";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";

const [corpusPath, queryPath, outPath] = process.argv.slice(2);
if (!corpusPath || !queryPath || !outPath) throw new Error("usage: fts-compare.ts <lexical.jsonl> <queries.json> <out.json>");
const sql = new SQL(process.env.MEMEX_FTS_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex");
const K = 10;

const corpusText = readFileSync(corpusPath, "utf8");
const lines = corpusText.trim().split("\n").map((l) => JSON.parse(l));
const header = lines[0].header ? lines.shift() : undefined;
const docs: string[] = lines.map((l) => l.text);
const lower = docs.map((d) => d.toLowerCase());
const queryText = readFileSync(queryPath, "utf8");
type Q = { cls: string; terms: string[]; intended?: string; relevant: Set<number> };
const queries: Q[] = JSON.parse(queryText).map((q: any) => {
  const want = (q.cls === "typo" ? [q.intended] : q.terms).map((t: string) => t.toLowerCase());
  const relevant = q.cls === "neg" ? new Set<number>() : new Set(lower.flatMap((d, i) => (want.every((t: string) => d.includes(t)) ? [i] : [])));
  if (q.cls !== "neg" && relevant.size === 0) throw new Error(`query has no relevant chunk: ${JSON.stringify(q)}`);
  if (q.cls === "neg" && lower.some((d) => q.terms.every((t: string) => d.includes(t.toLowerCase())))) throw new Error(`negative query is present: ${q.terms}`);
  return { ...q, relevant };
});

// ---------- database ----------
const [ver] = await sql`select version() v, (select extversion from pg_extension where extname = 'pg_search') pg_search, (select extversion from pg_extension where extname = 'pg_trgm') pg_trgm`;
await sql`drop schema if exists fts_v2 cascade`;
await sql`create schema fts_v2`;
await sql`create table fts_v2.chunks (id int primary key, body text not null)`;
for (let i = 0; i < docs.length; i += 500) await sql`insert into fts_v2.chunks ${sql(docs.slice(i, i + 500).map((body, j) => ({ id: i + j, body })))}`;
const t0 = performance.now();
await sql`create index chunks_trgm on fts_v2.chunks using gin (body gin_trgm_ops)`;
const trgmBuild = performance.now() - t0;
const t1 = performance.now();
await sql.unsafe(`create index chunks_bm25 on fts_v2.chunks using bm25 (
  id,
  (body::pdb.jieba),
  (body::pdb.source_code('alias=code')),
  (body::pdb.ngram(2,3,'alias=ng'))
) with (key_field = 'id')`);
const bm25Build = performance.now() - t1;
await sql`analyze fts_v2.chunks`;
const [sizes] = await sql`select pg_relation_size('fts_v2.chunks_trgm') trgm, pg_relation_size('fts_v2.chunks_bm25') bm25, pg_relation_size('fts_v2.chunks') heap, pg_total_relation_size('fts_v2.chunks') total`;

// ---------- methods ----------
// Each returns the top-K chunk ids. Several terms: every term must match (AND).
const esc = (s: string) => s.replace(/[\\%_]/g, (c) => "\\" + c);
type Method = { name: string; sql: (q: Q) => [string, string[]] };
const methods: Method[] = [
  {
    // exact substring (the trigram index serves ILIKE), ranked by word similarity
    name: "trgm-ilike",
    sql: (q) => {
      const n = q.terms.length; // $1..$n escaped patterns, $n+1..$2n raw terms for ranking
      const where = q.terms.map((_, k) => `body ilike '%' || $${k + 1} || '%'`).join(" and ");
      const rank = q.terms.map((_, k) => `word_similarity($${n + k + 1}, body)`).join(" + ");
      return [`select id from fts_v2.chunks where ${where} order by ${rank} desc, id limit ${K}`, [...q.terms.map(esc), ...q.terms]];
    },
  },
  {
    // fuzzy: word similarity ≥ pg_trgm.word_similarity_threshold (0.6) for every term
    name: "trgm-fuzzy",
    sql: (q) => [`select id from fts_v2.chunks where ${q.terms.map((_, k) => `$${k + 1} <% body`).join(" and ")} order by ${q.terms.map((_, k) => `word_similarity($${k + 1}, body)`).join(" + ")} desc, id limit ${K}`, q.terms],
  },
  ...(
    [
      ["jieba-and", "body", "&&&"],
      ["code-and", "body::pdb.alias('code')", "&&&"],
      ["ngram-and", "body::pdb.alias('ng')", "&&&"],
      ["jieba-phrase", "body", "###"],
      // ngram ### is rejected by pg_search 0.26 ("does not support this query type")
    ] as const
  ).map(([name, field, op]) => ({
    name,
    sql: (q: Q): [string, string[]] => [`select id from fts_v2.chunks where ${q.terms.map((_, k) => `${field} ${op} $${k + 1}`).join(" and ")} order by pdb.score(id) desc, id limit ${K}`, q.terms],
  })),
];

// ---------- run ----------
const run = async (m: Method, q: Q): Promise<number[]> => {
  const [text, params] = m.sql(q);
  try {
    return (await sql.unsafe(text, params)).map((r: any) => r.id as number);
  } catch (e) {
    console.error(JSON.stringify({ method: m.name, query: q.terms, sql: text, error: String((e as Error).message ?? e) }));
    process.exit(1);
  }
};
for (const m of methods) for (const q of queries.slice(0, 3)) await run(m, q); // warm-up, errors abort too
type Cell = { n: number; hit: number; p: number; mrr: number; falseHit: number; ms: number[] };
const per: Record<string, Record<string, Cell>> = {};
const perQuery: any[] = [];
for (const q of queries) {
  const row: any = { cls: q.cls, terms: q.terms, intended: q.intended, relevant: q.relevant.size, methods: {} };
  for (const m of methods) {
    const c = ((per[q.cls] ??= {})[m.name] ??= { n: 0, hit: 0, p: 0, mrr: 0, falseHit: 0, ms: [] });
    const t = performance.now();
    const ids = await run(m, q);
    c.ms.push(performance.now() - t);
    c.n++;
    const rel = ids.filter((id) => q.relevant.has(id)).length;
    const first = ids.findIndex((id) => q.relevant.has(id));
    if (q.cls === "neg") c.falseHit += ids.length ? 1 : 0;
    else {
      c.hit += first >= 0 ? 1 : 0;
      c.p += rel / Math.min(q.relevant.size, K);
      c.mrr += first >= 0 ? 1 / (first + 1) : 0;
    }
    row.methods[m.name] = { returned: ids.length, relevantInTop: rel, firstRank: first >= 0 ? first + 1 : null };
  }
  perQuery.push(row);
}
const pct = (xs: number[], p: number) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * p))];
const table: Record<string, Record<string, string>> = {};
for (const [cls, byM] of Object.entries(per))
  for (const [m, c] of Object.entries(byM))
    (table[cls] ??= {})[m] =
      (cls === "neg" ? `falseHit ${c.falseHit}/${c.n}` : `hit@${K} ${c.hit}/${c.n} P@${K} ${((100 * c.p) / c.n).toFixed(0)}% MRR ${(c.mrr / c.n).toFixed(2)}`) +
      ` p50 ${pct(c.ms, 0.5).toFixed(1)}ms p95 ${pct(c.ms, 0.95).toFixed(1)}ms`;

// ---------- plans for the slow path ----------
const plans: Record<string, string[]> = {};
for (const q of [queries.find((x) => x.cls === "typo")!, queries.find((x) => x.cls === "ident")!]) {
  for (const name of ["trgm-fuzzy", "trgm-ilike"]) {
    const [text, params] = methods.find((m) => m.name === name)!.sql(q);
    plans[`${name} ${q.cls} ${q.terms.join(" ")}`] = (await sql.unsafe(`explain (analyze, buffers) ${text}`, params)).map((r: any) => r["QUERY PLAN"]);
  }
}

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const result = {
  env: { ...ver, image: process.env.MEMEX_FTS_IMAGE },
  corpus: { header, chunks: docs.length, bytes: docs.reduce((a, d) => a + Buffer.byteLength(d), 0), fileSha256: sha(corpusText) },
  queries: { file: queryPath, sha256: sha(queryText), count: queries.length },
  build: { trgmMs: Math.round(trgmBuild), bm25Ms: Math.round(bm25Build), ...Object.fromEntries(Object.entries(sizes).map(([k, v]) => [k + "Bytes", Number(v)])) },
  table,
  perQuery,
  plans,
};
writeFileSync(outPath, JSON.stringify(result, null, 2), { mode: 0o600 });
console.log(JSON.stringify({ env: result.env, corpus: { chunks: result.corpus.chunks, bytes: result.corpus.bytes }, build: result.build }, null, 1));
for (const [cls, row] of Object.entries(table)) {
  console.log(`\n[${cls}]`);
  for (const [m, v] of Object.entries(row)) console.log(`  ${m.padEnd(13)} ${v}`);
}
for (const [k, v] of Object.entries(plans)) console.log(`\n${k}\n  ` + v.join("\n  "));
await sql.close();
