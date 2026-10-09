// Search (DESIGN §6.3, §4.3 path labels, §7 read-side authorization) over published projections and summaries.
//
// Validity: a source's projection is usable while its `projected.deps` (its own s: and its fork parent's
// vector) are all current; a summary additionally needs `input_fp` = its span's. Anything stale is left out
// until the next projection: this is conservative (an append to a live session hides it for one projection
// cycle) and never shows withdrawn evidence. Forgotten canonical records are dropped before any channel cuts.
//
// Authorization, all from server-side grants, per request purpose:
//   - the query is accepted only if grant(query scope, purpose, memex);
//   - a result (and every attachment) comes from a scope X with grant(X, purpose, memex);
//   - the query text goes to the embedding endpoint only if the request lists it and grant(query scope,
//     purpose, embedding); otherwise the vector channel is skipped and the result says so;
//   - rerank needs the destination listed and grant(., purpose, rerank) for the query scope and for every
//     scope whose text would be sent; otherwise `rerank_skipped`.
// The response is `Guarded` (http.ts): its version vector is compared again under the shared barrier
// before it is written, so a forget or revoke that commits first is never answered from the old state.
import { type Db, type Tx, addMigration } from "./db.ts";
import { Guarded } from "./http.ts";
import { BARRIER, HttpError } from "./sync.ts";
import { EMBED_DIM, guardedPost, snapshot, unchanged } from "./derive.ts";
import { canonOf } from "./project.ts";

export const PURPOSES = new Set(["recall", "tool", "snapshot"]);
export const CHANNEL_TOP = 50;
export const GUARANTEED = 8; // [待标定] per-channel top entries that always reach rerank
export const RRF_K = 60; // [待标定]
export const RERANK_CANDIDATES = 40;
export const REPS_PER_UNIT = 3; // [待标定] representations of one unit carried into rerank, per channel
export const RERANK_BUDGET = 64 * 1024; // [待标定] bytes of rerank documents in total
const EXCERPT_SIDE = 600; // [待标定] bytes around a lexical hit
const NEXT_USER = 1024; // [待标定]
const NEXT_ASSISTANT = 400; // [待标定]
const SIGNAL_MAX = 400; // correction / failure lines after the hit, per candidate
const MAX_TERMS = 8;
const SIGNAL = /不对|不是|错了|回滚|撤销|改成|改为|失败|报错|\[error\]|\brevert|\brollback|\bwrong\b|\bfailed\b|\berror\b|\bundo\b/i;

addMigration(`
-- Per search: which stages each candidate survived (no query text, no evidence text). Deleted with the
-- query's source on forget.
create table if not exists search_trace (id bigserial primary key, source text not null, purpose text not null,
                                         at timestamptz not null default now(), data jsonb not null);
create index if not exists search_trace_source on search_trace (source);
`);

export type SearchReq = {
  source: string; // the querying session
  scope: string; // its scope as the plugin knows it; must match the registered one
  purpose: string;
  query: string;
  dests: string[]; // destinations the plugin allows for this query: "embedding", "rerank"
  inContext?: string[]; // native record ids of the querying session still in the host's context
  limit?: number;
};

type Src = { source: string; scope: string; harness: string; deps: Record<string, number> };
type Rep = { span: string; source: string; text: string; keys: string[]; channel: "ft" | "vec"; score: number };
type Cand = {
  key: string; // "record:C" | "unit:U"
  kind: "record" | "unit";
  ranks: { lex?: number; ft?: number; vec?: number };
  fused: number;
  reps: Rep[]; // unit: hit representations (both channels kept)
  hit?: { key: string; source: string; text: string }; // record: the best chunk
  excerpt?: { key: string; text: string };
  attach: { what: "signal" | "next"; key: string; source: string; text: string }[];
  incomplete: string[];
  sources: Set<string>; // every source whose text this candidate carries
};

const esc = (t: string) => t.replace(/[\\%_]/g, (c) => "\\" + c);
const clip = (t: string, n: number) => {
  const b = Buffer.from(t);
  return b.length <= n ? t : b.subarray(0, n).toString().replace(/\uFFFD+$/, "");
};
const terms = (q: string) => [...new Set(q.split(/\s+/).filter(Boolean))].slice(0, MAX_TERMS);

/** Excerpt of `text` around the first occurrence of any term (±EXCERPT_SIDE bytes). */
function around(text: string, ts: string[]) {
  const low = text.toLowerCase();
  let at = -1;
  for (const t of ts) {
    const i = low.indexOf(t.toLowerCase());
    if (i >= 0 && (at < 0 || i < at)) at = i;
  }
  if (at < 0) return null;
  const pre = Buffer.from(text.slice(0, at));
  const start = Math.max(0, pre.length - EXCERPT_SIDE);
  const b = Buffer.from(text);
  return b.subarray(start, Math.min(b.length, pre.length + EXCERPT_SIDE)).toString().replace(/^\uFFFD+|\uFFFD+$/g, "");
}

const granted = async (q: Tx, scope: string, purpose: string, dest: string) =>
  (await q`select 1 from grant_rule where scope = ${scope} and purpose = ${purpose} and dest = ${dest}`).length > 0;

function validate(b: any): SearchReq {
  const s = (v: unknown, n: string, max = 256) => {
    if (typeof v !== "string" || !v || v.length > max) throw new HttpError(400, `${n} must be a non-empty string`);
    return v;
  };
  const purpose = s(b.purpose, "purpose", 16);
  if (!PURPOSES.has(purpose)) throw new HttpError(400, "purpose");
  return {
    source: s(b.source, "source", 64),
    scope: s(b.scope, "scope", 64),
    purpose,
    query: s(b.query, "query", 8192),
    dests: Array.isArray(b.dests) ? b.dests.filter((d: unknown) => d === "embedding" || d === "rerank") : [],
    inContext: Array.isArray(b.inContext) ? b.inContext.filter((x: unknown) => typeof x === "string").slice(0, 10_000) : [],
    limit: Number.isInteger(b.limit) && b.limit > 0 ? Math.min(b.limit, 40) : 10,
  };
}

/** Query scope check, readable and valid sources, in one snapshot. */
async function basis(q: Tx, r: Pick<SearchReq, "source" | "scope" | "purpose" | "inContext">) {
  const [src] = await q`select scope, harness from source where id = ${r.source}`;
  if (src && src.scope !== r.scope) throw new HttpError(409, "scope differs from the registered scope");
  if ((await q`select 1 from tomb where key = ${"source:" + r.source}`).length) throw new HttpError(410, "source forgotten");
  if (!(await granted(q, r.scope, r.purpose, "memex"))) throw new HttpError(403, "this session is not authorized for remote queries");
  const readable = (await q`select scope from grant_rule where purpose = ${r.purpose} and dest = 'memex'`).map((x: any) => x.scope as string);
  const rows = await q`select p.source, p.deps, s.scope, s.harness from projected p join source s on s.id = p.source
                       where p.status = 'ok' and s.scope = any(${q.array(readable, "text")})
                       and not exists (select 1 from tomb t where t.key = 'source:' || p.source)`;
  const keys = [...new Set<string>(rows.flatMap((x: any) => Object.keys(x.deps)))];
  const vs = new Map<string, number>((await q`select key, v from vis_version where key = any(${q.array(keys, "text")})`).map((x: any) => [x.key, Number(x.v)]));
  const valid = new Map<string, Src>();
  for (const x of rows) {
    const deps = x.deps as Record<string, number>;
    if (Object.keys(deps).length && Object.entries(deps).every(([k, v]) => (vs.get(k) ?? 1) === v)) valid.set(x.source, x as Src);
  }
  // Records still in the host's context, as canonical ids (inherited ones map to their origin).
  const ctx = new Set<string>();
  if (src && r.inContext?.length) {
    const [p] = await q`select keys from projected where source = ${r.source}`;
    const inherited = new Map<string, string>((p?.keys ?? []) as [string, string][]);
    for (const id of r.inContext!) ctx.add(inherited.has(id) ? canonOf(src.harness, inherited.get(id)!) : `${r.source}:${id}`);
  }
  // The query's own basis, read with the checks above: its session's version (a forget of the querying
  // session moves it, even if it never reached L0) and the read grant's destination.
  const qk = ["s:" + r.source, "d:memex"];
  const qv = new Map<string, number>((await q`select key, v from vis_version where key = any(${q.array(qk, "text")})`).map((x: any) => [x.key, Number(x.v)]));
  const qDeps = Object.fromEntries(qk.map((k) => [k, qv.get(k) ?? 1])) as Record<string, number>;
  return { valid, ctx, dMemex: qDeps["d:memex"], qDeps };
}

/** Embed the query (guarded by the embedding destination's version), or say why not. */
async function embedQuery(db: Db, r: SearchReq): Promise<{ v?: number[]; skipped?: string }> {
  if (!r.dests.includes("embedding")) return { skipped: "not-requested" };
  const url = process.env.MEMEX_EMBED_URL, model = process.env.MEMEX_EMBED_MODEL, key = process.env.MEMEX_EMBED_KEY;
  if (!url || !model || !key) return { skipped: "not-configured" };
  const c = await snapshot(db, async (q) => {
    const b = await basis(q, r); // the query is still authorized and its session not forgotten
    if (!(await granted(q, r.scope, r.purpose, "embedding"))) return null;
    const [v] = await q`select v from vis_version where key = 'd:embedding'`;
    return { ...b.qDeps, "d:embedding": Number(v?.v ?? 1) };
  });
  if (!c) return { skipped: "not-granted" };
  try {
    const res = await guardedPost(db, c, url.replace(/\/+$/, "") + "/embeddings", key, { model, input: [r.query], dimensions: EMBED_DIM, encoding_format: "float" });
    if (res === "stale") return { skipped: "revoked" };
    const v = res.json?.data?.[0]?.embedding;
    if (res.status !== 200 || !Array.isArray(v) || v.length !== EMBED_DIM) return { skipped: `endpoint-${res.status}` };
    return { v };
  } catch {
    return { skipped: "endpoint-error" };
  }
}

/** The three channels, fusion and rerank inputs, in one snapshot. */
async function recall(q: Tx, r: SearchReq, b: Awaited<ReturnType<typeof basis>>, qv: number[] | undefined) {
  const valid = [...b.valid.keys()];
  const ctx = [...b.ctx];
  const ts = terms(r.query);
  const pats = ts.map((t) => "%" + esc(t) + "%");
  // Term weights: a rare term outweighs a common one (an IDF stand-in; document frequency over the lexical
  // rows and summaries of usable sources, capped so a frequent term costs one bounded count).
  const ws: number[] = [];
  for (const p of pats) {
    const [d] = await q`select (select count(*) from (select 1 from lexical where source = any(${q.array(valid, "text")}) and text ilike ${p} limit 10000) x)::int
                             + (select count(*) from (select 1 from span_summary where ft ilike ${p} limit 10000) y)::int n`;
    ws.push(1 / Math.log(2 + d.n));
  }
  // 1. Lexical: best chunk per canonical record, exact top 50 after every filter.
  const lex = await q`
    select * from (
      select distinct on (canon) canon, key, source, part, text, score from (
        select l.canon, l.key, l.source, l.part, l.text,
               (select coalesce(sum(w), 0) from unnest(${q.array(pats, "text")}::text[], ${q.array(ws, "float8")}::float8[]) p(p, w) where l.text ilike p.p) score
        from lexical l
        where l.source = any(${q.array(valid, "text")}) and l.text ilike any(${q.array(pats, "text")})
          and not (l.canon = any(${q.array(ctx, "text")}))
          and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon)) x
      order by canon, score desc, key, part) y
    order by score desc, canon limit ${CHANNEL_TOP}`;
  // Summary representations usable now: current input, valid source, no forgotten support record, not all
  // support records still in context.
  const repFilter = (alias: string) => q`
    ${q.unsafe(alias)}.input_fp = sp.input_fp and sp.source = any(${q.array(valid, "text")})
    and not exists (select 1 from unnest(sp.canons) c join tomb t on t.key = 'canon:' || c)
    and not (sp.canons <@ ${q.array(ctx, "text")}::text[] and cardinality(sp.canons) > 0)`;
  // 2. Summary full text (summary + anchors): units ranked by their best representation.
  const ft = await q`
    with reps as (
      select ss.unit, ss.span, sp.source, ss.text, sp.keys,
             (select coalesce(sum(w), 0) from unnest(${q.array(pats, "text")}::text[], ${q.array(ws, "float8")}::float8[]) p(p, w) where ss.ft ilike p.p)
               + 0.01 * similarity(ss.ft, ${r.query}) score
      from span_summary ss join span sp on sp.id = ss.span
      where ss.ft ilike any(${q.array(pats, "text")}) and ${repFilter("ss")}),
    ranked as (select *, row_number() over (partition by unit order by score desc, span) rn, max(score) over (partition by unit) best from reps)
    select * from ranked where rn <= ${REPS_PER_UNIT} and unit in (
      select unit from ranked where rn = 1 order by best desc, unit limit ${CHANNEL_TOP})
    order by best desc, unit, rn`;
  // 3. Vector: exact cosine over every usable representation (no approximate index: grouped top-K stays exact).
  const vec = qv
    ? await q`
    with reps as (
      select ss.unit, ss.span, sp.source, ss.text, sp.keys, 1 - (ss.embedding <=> ${"[" + qv.join(",") + "]"}::vector) score
      from span_summary ss join span sp on sp.id = ss.span
      where ss.embedding is not null and ss.embed_model = ${process.env.MEMEX_EMBED_MODEL ?? ""} and ${repFilter("ss")}),
    ranked as (select *, row_number() over (partition by unit order by score desc, span) rn, max(score) over (partition by unit) best from reps)
    select * from ranked where rn <= ${REPS_PER_UNIT} and unit in (
      select unit from ranked where rn = 1 order by best desc, unit limit ${CHANNEL_TOP})
    order by best desc, unit, rn`
    : [];

  // Fusion: candidate keys by type; the first GUARANTEED of each channel always enter, RRF fills to 40.
  const cands = new Map<string, Cand>();
  const get = (key: string, kind: Cand["kind"]) => {
    let c = cands.get(key);
    if (!c) cands.set(key, (c = { key, kind, ranks: {}, fused: 0, reps: [], attach: [], incomplete: [], sources: new Set() }));
    return c;
  };
  lex.forEach((x: any, i: number) => {
    const c = get("record:" + x.canon, "record");
    c.ranks.lex = i + 1;
    c.hit = { key: x.key, source: x.source, text: x.text };
    c.sources.add(x.source);
  });
  for (const [name, rows] of [["ft", ft], ["vec", vec]] as const) {
    const order: string[] = [];
    for (const x of rows as any[]) {
      const c = get("unit:" + x.unit, "unit");
      if (!order.includes(c.key)) order.push(c.key);
      c.ranks[name] = order.indexOf(c.key) + 1;
      if (!c.reps.some((p) => p.span === x.span && p.channel === name))
        c.reps.push({ span: x.span, source: x.source, text: x.text, keys: x.keys, channel: name, score: Number(x.score) });
      c.sources.add(x.source);
    }
  }
  for (const c of cands.values()) c.fused = Object.values(c.ranks).reduce((a, k) => a + 1 / (RRF_K + k!), 0);
  const chosen: Cand[] = [];
  for (const ch of ["lex", "ft", "vec"] as const)
    for (const c of [...cands.values()].filter((c) => c.ranks[ch]! <= GUARANTEED).sort((a, b) => a.ranks[ch]! - b.ranks[ch]!))
      if (!chosen.includes(c)) chosen.push(c);
  for (const c of [...cands.values()].sort((a, b) => b.fused - a.fused || (a.key < b.key ? -1 : 1))) {
    if (chosen.length >= RERANK_CANDIDATES) break;
    if (!chosen.includes(c)) chosen.push(c);
  }
  chosen.sort((a, b) => b.fused - a.fused || (a.key < b.key ? -1 : 1));

  // Rerank inputs: excerpts, signals after the hit inside its span, next-exchange starts from every view.
  const lexText = async (keys: string[]) => {
    const rows = await q`select key, source, string_agg(text, '' order by part) text from lexical
                         where key = any(${q.array(keys, "text")}) and source = any(${q.array(valid, "text")}) group by key, source`;
    return new Map<string, { source: string; text: string }>(rows.map((x: any) => [x.key, { source: x.source, text: x.text }]));
  };
  const seenNext = new Set<string>();
  for (const c of chosen) {
    let support: string[]; // record keys, in span order, the hit's position = `from`
    let from = 0;
    if (c.kind === "record") {
      c.excerpt = { key: c.hit!.key, text: around(c.hit!.text, ts) ?? clip(c.hit!.text, 2 * EXCERPT_SIDE) };
      const [sp] = await q`select keys from span where keys @> array[${c.hit!.key}]::text[] and source = any(${q.array(valid, "text")}) limit 1`;
      support = sp?.keys ?? [c.hit!.key];
      from = Math.max(0, support.indexOf(c.hit!.key));
    } else {
      const best = c.reps[0];
      support = best.keys;
      const texts = await lexText(support);
      // Only a vector hit: the support chunk with most query-term overlap; otherwise the representation alone.
      let pick: { key: string; text: string } | undefined;
      let n = 0;
      for (const k of support) {
        const t = texts.get(k)?.text ?? "";
        const m = ts.filter((x) => t.toLowerCase().includes(x.toLowerCase())).length;
        if (m > n) (n = m), (pick = { key: k, text: around(t, ts)! });
      }
      if (pick) (c.excerpt = pick), (from = support.indexOf(pick.key));
    }
    const texts = await lexText(support.slice(from + 1));
    let budget = SIGNAL_MAX;
    for (const k of support.slice(from + 1)) {
      const t = texts.get(k);
      if (!t) continue;
      for (const line of t.text.split("\n"))
        if (SIGNAL.test(line) && budget > 0) {
          const s = clip(line, budget);
          if (s.length < line.length) c.incomplete.push("signal-clipped");
          budget -= Buffer.byteLength(s);
          c.attach.push({ what: "signal", key: k, source: t.source, text: s });
          c.sources.add(t.source);
        }
    }
    // Next exchange after the last support record, on every valid view that has it (dedup by record version).
    const last = support.at(-1)!;
    const views = await q`select v.source, v.keys, v.recs from view v where v.keys @> array[${last}]::text[] and v.source = any(${q.array(valid, "text")})`;
    const want: string[] = [];
    for (const v of views) {
      const i = (v.keys as string[]).indexOf(last);
      const recs = v.recs as { key: string; kind: string }[];
      const u = recs.findIndex((x, j) => j > i && x.kind === "user");
      if (u < 0 || seenNext.has(recs[u].key)) continue;
      seenNext.add(recs[u].key);
      want.push(recs[u].key);
      const a = recs.findIndex((x, j) => j > u && x.kind === "assistant");
      if (a >= 0 && (recs.slice(u + 1, a).every((x) => x.kind !== "user"))) want.push(recs[a].key);
    }
    const nt = await lexText(want);
    for (const k of want) {
      const t = nt.get(k);
      if (!t) continue;
      const max = views.some((v: any) => (v.recs as any[]).find((x) => x.key === k)?.kind === "user") ? NEXT_USER : NEXT_ASSISTANT;
      const s = clip(t.text, max);
      if (s.length < t.text.length) c.incomplete.push("next-clipped");
      c.attach.push({ what: "next", key: k, source: t.source, text: s });
      c.sources.add(t.source);
    }
  }
  return { chosen, channels: { lex: lex.length, ft: new Set(ft.map((x: any) => x.unit)).size, vec: new Set((vec as any[]).map((x) => x.unit)).size } };
}

/** The document a candidate sends to rerank: hit representations and excerpt first, then attachments. */
const docBase = (c: Cand) =>
  [...new Set(c.reps.map((p) => p.text))].map((t) => `[summary] ${t}`).concat(c.excerpt ? `[excerpt] ${c.excerpt.text}` : c.hit ? `[excerpt] ${clip(c.hit.text, 2 * EXCERPT_SIDE)}` : "")
    .filter(Boolean).join("\n");
const docAttach = (c: Cand) => c.attach.map((a) => `[${a.what === "next" ? "next exchange" : "later in span"}] ${a.text}`).join("\n");

/** Fit the documents in the budget: drop attachments from the lowest-ranked candidate up (incomplete). */
function fit(chosen: Cand[]): string[] | null {
  const base = chosen.map(docBase);
  let total = base.reduce((a, d) => a + Buffer.byteLength(d), 0);
  if (total > RERANK_BUDGET) return null;
  const att = chosen.map(docAttach);
  total += att.reduce((a, d) => a + Buffer.byteLength(d) + 1, 0);
  for (let i = chosen.length - 1; i >= 0 && total > RERANK_BUDGET; i--) {
    if (!att[i]) continue;
    total -= Buffer.byteLength(att[i]) + 1;
    att[i] = "";
    chosen[i].attach = [];
    chosen[i].incomplete.push("attachments-dropped");
  }
  return base.map((b, i) => (att[i] ? b + "\n" + att[i] : b));
}

/** Path label of evidence `keys` against the target source's view (§4.2, §4.3). */
async function pathLabel(q: Tx, source: string, keys: string[]) {
  const views = await q`select ord, tip, keys from view where source = ${source} order by ord`;
  if (!views.length || !keys.length) return { label: "unlocated", basis: "unknown" as const };
  const leaves = [...new Set((await q`select leaf from collector_view where source = ${source} and leaf is not null`).map((x: any) => x.leaf))];
  let v: any, basis: "observed" | "inferred-tail" | "unknown";
  if (leaves.length > 1) basis = "unknown";
  else if (leaves.length === 1 && (v = views.find((x: any) => x.tip === leaves[0]))) basis = "observed";
  else {
    const [t] = await q`select rec from record where source = ${source} order by first_pos desc limit 1`;
    v = views.find((x: any) => x.tip === t?.rec) ?? views.at(-1);
    basis = "inferred-tail";
  }
  if (views.length === 1) return { label: keys.every((k) => views[0].keys.includes(k)) ? "on-path" : "unlocated", basis: basis === "unknown" ? "inferred-tail" : basis };
  if (basis === "unknown") return { label: "branched", basis };
  const on = keys.filter((k) => v.keys.includes(k)).length;
  return { label: on === keys.length ? "on-path" : on === 0 ? "off-path" : "mixed", basis, tip: v.tip as string };
}

export const searchHooks = { beforeTrace: async () => {} };

export async function search(db: Db, body: unknown): Promise<Guarded> {
  const r = validate(body);
  await snapshot(db, (q) => basis(q, r)); // the query's own authorization, before its text goes anywhere
  const emb = await embedQuery(db, r);
  let tries = 0;
  const run = async (): Promise<Guarded> => {
    if (++tries > 3) throw new HttpError(503, "state kept changing; retry");
    const s = await snapshot(db, async (q) => {
      const b = await basis(q, r);
      const rc = await recall(q, r, b, emb.v);
      const scopes = (c: Cand) => [...c.sources].map((x) => b.valid.get(x)!.scope);
      // Rerank authorization: the query scope and every scope whose text would be sent.
      let rerankSkip: string | undefined;
      const rerankScopes = new Set<string>([r.scope, ...rc.chosen.flatMap(scopes)]);
      if (!r.dests.includes("rerank")) rerankSkip = "not-requested";
      else if (!process.env.MEMEX_RERANK_URL) rerankSkip = "not-configured";
      else for (const sc of rerankScopes) if (!(await granted(q, sc, r.purpose, "rerank"))) rerankSkip = "not-granted";
      const [dr] = await q`select v from vis_version where key = 'd:rerank'`;
      const labels = new Map<string, any>();
      for (const c of rc.chosen) {
        const src = c.kind === "record" ? c.hit!.source : c.reps[0].source;
        const keys = c.kind === "record" ? [c.hit!.key] : c.reps[0].keys;
        labels.set(c.key, { ...(await pathLabel(q, src, keys)), reps: await Promise.all(c.reps.map(async (p) => (await pathLabel(q, p.source, p.keys)).label)) });
      }
      return { b, rc, rerankSkip, dRerank: Number(dr?.v ?? 1), labels };
    });
    const { b, rc, labels } = s;
    let rerankSkip = s.rerankSkip;
    // Every source carried by a candidate, plus the read grant's destination version.
    const deps: Record<string, number> = { ...b.qDeps };
    for (const c of rc.chosen) for (const x of c.sources) Object.assign(deps, b.valid.get(x)!.deps);
    let scores: (number | null)[] = rc.chosen.map(() => null);
    if (!rerankSkip && rc.chosen.length) {
      const docs = fit(rc.chosen);
      if (!docs) rerankSkip = "base-over-budget";
      else {
        try {
          const res = await guardedPost(db, { ...deps, "d:rerank": s.dRerank }, process.env.MEMEX_RERANK_URL!, process.env.MEMEX_RERANK_KEY ?? "", {
            model: process.env.MEMEX_RERANK_MODEL,
            query: "Find records that state the specific fact, decision, step or event needed to answer: " + r.query,
            documents: docs,
          });
          const out = res === "stale" ? null : res.status === 200 ? (res.json?.results ?? res.json?.data) : null;
          if (!Array.isArray(out)) rerankSkip = res === "stale" ? "state-changed" : `endpoint-${res.status}`;
          else for (const x of out) if (Number.isInteger(x.index) && x.index < scores.length) scores[x.index] = Number(x.relevance_score ?? x.score);
          if (!rerankSkip && scores.some((x) => x === null || Number.isNaN(x))) (rerankSkip = "endpoint-incomplete"), (scores = scores.map(() => null));
        } catch {
          rerankSkip = "endpoint-error";
        }
      }
    }
    // After rerank: off-path evidence and a session's consecutive places are down-weighted (§6.3).
    const base = rc.chosen.map((c, i) => ({ c, i, score: rerankSkip ? c.fused : scores[i]! }));
    base.sort((x, y) => y.score - x.score || x.i - y.i);
    const seen = new Map<string, number>();
    const final = base.map((x) => {
      const src = x.c.kind === "record" ? x.c.hit!.source : x.c.reps[0].source;
      const n = seen.get(src) ?? 0;
      seen.set(src, n + 1);
      return { ...x, adj: x.score * (labels.get(x.c.key).label === "off-path" ? 0.8 : 1) * Math.pow(0.9, n) };
    }).sort((x, y) => y.adj - x.adj || x.i - y.i);
    const results = final.slice(0, r.limit).map((x) => {
      const c = x.c;
      const L = labels.get(c.key);
      return {
        key: c.key,
        source: c.kind === "record" ? c.hit!.source : c.reps[0].source,
        scope: b.valid.get(c.kind === "record" ? c.hit!.source : c.reps[0].source)!.scope,
        path: { label: L.label, view_basis: L.basis, tip: L.tip ?? null },
        score: x.adj,
        rerank_score: rerankSkip ? null : scores[x.i],
        reps: c.reps.map((p, j) => ({ span: p.span, channel: p.channel, text: p.text, path: L.reps[j] })),
        excerpt: c.excerpt ?? null,
        attachments: c.attach,
        incomplete: [...new Set(c.incomplete)],
        injectable: !rerankSkip && !c.incomplete.length,
        // Historical statements, not the current state (§6.3).
        stated: true,
      };
    });
    const trace = rc.chosen.map((c, i) => ({ key: c.key, ranks: c.ranks, fused: Number(c.fused.toFixed(5)), rerank: scores[i], final: final.findIndex((x) => x.c === c) + 1, attached: c.attach.length, incomplete: [...new Set(c.incomplete)] }));
    // The trace is published like any derived artefact: under the shared barrier, only while the query's
    // basis and every candidate's still hold (a forget of the querying session deletes its traces).
    await searchHooks.beforeTrace();
    const t = await db.begin(async (tx: Tx) => {
      await tx`select pg_advisory_xact_lock_shared(${BARRIER})`;
      if (!(await unchanged(tx, deps))) return null;
      const [row] = await tx`insert into search_trace (source, purpose, data) values (${r.source}, ${r.purpose},
                             ${{ channels: rc.channels, embedding_skipped: emb.skipped ?? null, rerank_skipped: rerankSkip ?? null, candidates: trace }}) returning id`;
      return row;
    });
    if (!t) return run();
    return new Guarded(deps, {
      results,
      embedding_skipped: emb.skipped ?? null,
      rerank_skipped: rerankSkip ?? null,
      trace: Number(t.id),
    }, run);
  };
  return run();
}

// ---------- zoom and grep (§6.4) ----------

const ZOOM_PAGE = 16 * 1024; // [待标定] bytes of record text per page
const GREP_MAX = 50;

type ToolReq = { source: string; scope: string; purpose: string };
function toolReq(b: any): ToolReq {
  const r = validate({ ...b, query: "-" });
  return { source: r.source, scope: r.scope, purpose: r.purpose };
}
const depsOf = (b: Awaited<ReturnType<typeof basis>>, sources: string[]) => {
  const d: Record<string, number> = { "d:memex": b.dMemex };
  for (const s of sources) Object.assign(d, b.valid.get(s)!.deps);
  return d;
};
/** Record texts of `keys` (all chunks joined), without forgotten canonical records. */
async function texts(q: Tx, keys: string[], valid: string[]) {
  const rows = await q`select key, source, canon, kind, string_agg(text, '' order by part) text from lexical l
                       where key = any(${q.array(keys, "text")}) and source = any(${q.array(valid, "text")})
                       and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon)
                       group by key, source, canon, kind`;
  return new Map<string, any>(rows.map((x: any) => [x.key, x]));
}

/** mem_zoom: a span (or unit, or record) with its records' text, paged, and its neighbours on a view. */
export async function zoom(db: Db, body: any): Promise<Guarded> {
  const r = toolReq(body);
  const id = typeof body.id === "string" ? body.id : "";
  const offset = Number.isInteger(body.offset) && body.offset >= 0 ? body.offset : 0;
  const limit = Number.isInteger(body.limit) && body.limit > 0 ? Math.min(body.limit, ZOOM_PAGE) : ZOOM_PAGE;
  const run = async (): Promise<Guarded> => {
    const out = await snapshot(db, async (q) => {
      const b = await basis(q, r);
      const valid = [...b.valid.keys()];
      if (id.startsWith("record:")) {
        const canon = id.slice(7);
        const [row] = await q`select key from lexical l where canon = ${canon} and source = any(${q.array(valid, "text")})
                              and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon) order by key limit 1`;
        if (!row) throw new HttpError(404, "not found or not readable");
        const t = (await texts(q, [row.key], valid)).get(row.key);
        const page = Buffer.from(t.text).subarray(offset, offset + limit).toString();
        return { deps: depsOf(b, [t.source]), body: { id, source: t.source, kind: t.kind, bytes: Buffer.byteLength(t.text), offset, text: page } };
      }
      const sid = id.replace(/^(span|unit):/, "");
      const [sp] = await q`select id, source, keys, canons from span where id = ${sid} and source = any(${q.array(valid, "text")})`;
      if (!sp || (await q`select 1 from tomb where key = any(${q.array((sp.canons as string[]).map((c) => "canon:" + c), "text")}) limit 1`).length)
        throw new HttpError(404, "not found or not readable");
      // The view: the leaf the caller names, else the result's view basis (same rule as search labels).
      const views = await q`select ord, tip, spans, span_ids from view where source = ${sp.source} and span_ids @> array[${sid}]::text[] order by ord`;
      const v = views.find((x: any) => x.tip === body.leaf) ?? views.at(-1);
      const order = v ? (v.spans as any[]).filter((x) => x.id).map((x) => x.id as string) : [sid];
      const i = order.indexOf(sid);
      const t = await texts(q, sp.keys, valid);
      let skip = offset, left = limit, more = false;
      const records = [];
      for (const k of sp.keys as string[]) {
        const x = t.get(k);
        if (!x) continue;
        const buf = Buffer.from(x.text);
        if (skip >= buf.length) {
          skip -= buf.length;
          continue;
        }
        const part = buf.subarray(skip, skip + left);
        records.push({ key: k, canon: x.canon, kind: x.kind, bytes: buf.length, from: skip, text: part.toString() });
        left -= part.length;
        skip = 0;
        if (left <= 0) {
          more = true;
          break;
        }
      }
      const summary = (await q`select text from span_summary ss join span s on s.id = ss.span where ss.span = ${sid} and ss.input_fp = s.input_fp`)[0]?.text ?? null;
      return {
        deps: depsOf(b, [sp.source, ...[...t.values()].map((x) => x.source)]),
        body: {
          id: "span:" + sid, source: sp.source, view_tip: v?.tip ?? null, summary, records, more, next_offset: more ? offset + limit : null,
          before: i > 0 ? "span:" + order[i - 1] : null, after: i >= 0 && i < order.length - 1 ? "span:" + order[i + 1] : null,
        },
      };
    });
    return new Guarded(out.deps, out.body, run);
  };
  return run();
}

/** mem_grep: exact substring (or POSIX regex) over the raw text; record positions and a short snippet. */
export async function grep(db: Db, body: any): Promise<Guarded> {
  const r = toolReq(body);
  const pattern = typeof body.pattern === "string" && body.pattern && body.pattern.length <= 512 ? body.pattern : null;
  if (!pattern) throw new HttpError(400, "pattern must be a non-empty string");
  const regex = body.regex === true;
  const run = async (): Promise<Guarded> => {
    const out = await snapshot(db, async (q) => {
      await q`select set_config('statement_timeout', '5000ms', true)`;
      const b = await basis(q, r);
      const valid = [...b.valid.keys()];
      const rows = await (regex
        ? q`select distinct on (key) key, source, canon, kind, part, text from lexical l
            where source = any(${q.array(valid, "text")}) and text ~ ${pattern}
            and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon) order by key, part limit ${GREP_MAX}`
        : q`select distinct on (key) key, source, canon, kind, part, text from lexical l
            where source = any(${q.array(valid, "text")}) and text like ${"%" + esc(pattern) + "%"}
            and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon) order by key, part limit ${GREP_MAX}`).catch((e: any) => {
        if (/regular expression|canceling statement/.test(String(e?.message))) throw new HttpError(400, "pattern invalid or too slow");
        throw e;
      });
      const hits = [];
      for (const x of rows) {
        const m = regex ? new RegExp(pattern).exec(x.text) : null;
        const at = regex ? (m?.index ?? 0) : x.text.indexOf(pattern);
        const pre = Buffer.byteLength(x.text.slice(0, at));
        const buf = Buffer.from(x.text);
        hits.push({
          record: "record:" + x.canon, key: x.key, source: x.source, kind: x.kind,
          offset: x.part * 2048 + at, // character offset in the record's text (chunks are 2,048 characters)
          snippet: buf.subarray(Math.max(0, pre - 120), pre + 200).toString().replace(/^\uFFFD+|\uFFFD+$/g, ""),
        });
      }
      return { deps: depsOf(b, [...new Set<string>(rows.map((x: any) => x.source))]), body: { hits, truncated: rows.length >= GREP_MAX } };
    });
    return new Guarded(out.deps, out.body, run);
  };
  return run();
}
