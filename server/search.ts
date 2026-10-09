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
import { createHash } from "node:crypto";
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
  leaf?: string; // the querying session's current leaf (native id): its own results are labelled against it
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
  // `path`: the attachment's own label against the result's reference view (set with the result's labels).
  attach: { what: "signal" | "next"; key: string; source: string; text: string; path?: string }[];
  incomplete: string[];
  sources: Set<string>; // every source whose text this candidate carries
};

const esc = (t: string) => t.replace(/[\\%_]/g, (c) => "\\" + c);
const clip = (t: string, n: number) => {
  const b = Buffer.from(t);
  return b.length <= n ? t : b.subarray(0, n).toString().replace(/\uFFFD+$/, "");
};
const terms = (q: string) => [...new Set(q.split(/\s+/).filter(Boolean))].slice(0, MAX_TERMS);

/** Character offset of the first occurrence of any term, or -1. */
function firstHit(text: string, ts: string[]) {
  const low = text.toLowerCase();
  let at = -1;
  for (const t of ts) {
    const i = low.indexOf(t.toLowerCase());
    if (i >= 0 && (at < 0 || i < at)) at = i;
  }
  return at;
}
/** Excerpt of `text` around the first occurrence of any term (±EXCERPT_SIDE bytes). */
function around(text: string, ts: string[]) {
  const at = firstHit(text, ts);
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
    leaf: typeof b.leaf === "string" && b.leaf && b.leaf.length <= 256 ? b.leaf : undefined,
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
      select sp.unit, ss.span, sp.source, ss.text, sp.keys,
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
      select sp.unit, ss.span, sp.source, ss.text, sp.keys, 1 - (ss.embedding <=> ${"[" + qv.join(",") + "]"}::vector) score
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

  // Rerank inputs (§6.3), per candidate: each rerank document is scored on its own, so every candidate gets
  // its own corrections; repetition is only removed inside one candidate (by record version).
  const lexText = async (keys: string[]) => {
    const rows = await q`select key, source, string_agg(text, '' order by part) text from lexical
                         where key = any(${q.array(keys, "text")}) and source = any(${q.array(valid, "text")}) group by key, source`;
    return new Map<string, { source: string; text: string }>(rows.map((x: any) => [x.key, { source: x.source, text: x.text }]));
  };
  for (const c of chosen) {
    // Evidence positions: [record keys in order, index of the hit (-1: no located hit, the whole set counts)].
    const ev: [string[], number][] = [];
    const hitAt = new Map<string, number>(); // record key → character offset of the hit inside it
    if (c.kind === "record") {
      const k = c.hit!.key;
      c.excerpt = { key: k, text: around(c.hit!.text, ts) ?? clip(c.hit!.text, 2 * EXCERPT_SIDE) };
      const full = (await lexText([k])).get(k)?.text ?? c.hit!.text;
      hitAt.set(k, Math.max(0, firstHit(full, ts)));
      // Every span that holds this record version (fork copies share the key), not one of them.
      const sps = await q`select keys from span where keys @> array[${k}]::text[] and source = any(${q.array(valid, "text")})`;
      for (const sp of sps) ev.push([sp.keys, (sp.keys as string[]).indexOf(k)]);
      if (!ev.length) ev.push([[k], 0]);
    } else {
      const seenRep = new Set<string>();
      for (const p of c.reps) {
        if (seenRep.has(p.span)) continue;
        seenRep.add(p.span);
        ev.push([p.keys, -1]);
      }
      // An excerpt: the support record with most query-term overlap over all hit representations.
      const all = [...new Set(c.reps.flatMap((p) => p.keys))];
      const texts = await lexText(all);
      let pick: { key: string; text: string } | undefined;
      let n = 0;
      for (const k of all) {
        const t = texts.get(k)?.text ?? "";
        const m = ts.filter((x) => t.toLowerCase().includes(x.toLowerCase())).length;
        if (m > n) (n = m), (pick = { key: k, text: around(t, ts)! });
      }
      if (pick) {
        c.excerpt = pick;
        hitAt.set(pick.key, Math.max(0, firstHit(texts.get(pick.key)!.text, ts)));
      }
    }
    // Correction signals after the hit: in the hit record after the hit position, and in later records of
    // each evidence span (with no located hit, every record of the support set).
    const later = new Map<string, number>(); // record key → character offset from which to scan
    for (const [keys, i] of ev)
      keys.forEach((k, j) => {
        const from = j === i || (i < 0 && hitAt.has(k)) ? hitAt.get(k) ?? 0 : j > i ? 0 : -1;
        if (from >= 0 && (!later.has(k) || later.get(k)! > from)) later.set(k, from);
      });
    const lt = await lexText([...later.keys()]);
    let budget = SIGNAL_MAX;
    for (const [k, from] of later) {
      const t = lt.get(k);
      if (!t) continue;
      for (const line of t.text.slice(from).split("\n")) {
        if (!SIGNAL.test(line)) continue;
        if (budget <= 0) {
          c.incomplete.push("signals-dropped");
          break;
        }
        const s = clip(line, budget);
        if (s.length < line.length) c.incomplete.push("signal-clipped");
        budget -= Buffer.byteLength(s);
        c.attach.push({ what: "signal", key: k, source: t.source, text: s });
        c.sources.add(t.source);
      }
    }
    // Next exchange after each evidence span's end, on every valid view that holds that record (a user's
    // "不对，改成 B" is most often the next message, and it may differ per fork).
    const ends = [...new Set(ev.map(([keys]) => keys.at(-1)!))];
    const views = await q`select v.keys, v.recs from view v where v.keys && ${q.array(ends, "text")}::text[] and v.source = any(${q.array(valid, "text")})`;
    const want = new Map<string, "user" | "assistant">();
    for (const v of views) {
      const recs = v.recs as { key: string; kind: string }[];
      for (const e of ends) {
        const i = (v.keys as string[]).indexOf(e);
        if (i < 0) continue;
        const u = recs.findIndex((x, j) => j > i && x.kind === "user");
        if (u < 0) continue;
        want.set(recs[u].key, "user");
        const a = recs.findIndex((x, j) => j > u && x.kind === "assistant");
        if (a >= 0 && recs.slice(u + 1, a).every((x) => x.kind !== "user")) want.set(recs[a].key, "assistant");
      }
    }
    const nt = await lexText([...want.keys()]);
    for (const [k, kind] of want) {
      const t = nt.get(k);
      if (!t) continue;
      const s = clip(t.text, kind === "user" ? NEXT_USER : NEXT_ASSISTANT);
      if (s.length < t.text.length) c.incomplete.push("next-clipped");
      c.attach.push({ what: "next", key: k, source: t.source, text: s });
      c.sources.add(t.source);
    }
  }
  // Trace (§6.5): each channel's first 10 by key, and every candidate of the pool (bounded by the channel
  // limits) with its per-channel ranks, the representations it was hit through, its fused rank and whether it
  // was selected for rerank. Keys and numbers only.
  const top = (rows: any[], f: (x: any) => string) => [...new Set(rows.map(f))].slice(0, 10);
  const fusedOrder = [...cands.values()].sort((a, b) => b.fused - a.fused || (a.key < b.key ? -1 : 1));
  return {
    chosen,
    channels: { lex: lex.length, ft: new Set(ft.map((x: any) => x.unit)).size, vec: new Set((vec as any[]).map((x) => x.unit)).size },
    top: { lex: top(lex as any[], (x) => "record:" + x.canon), ft: top(ft as any[], (x) => "unit:" + x.unit), vec: top(vec as any[], (x) => "unit:" + x.unit) },
    pool: fusedOrder.map((c, i) => ({
      key: c.key, ranks: c.ranks, fused: Number(c.fused.toFixed(5)), fused_rank: i + 1,
      reps: c.reps.map((p) => `${p.channel}:${p.span}`), ...(c.hit ? { hit: c.hit.key } : {}),
      selected: chosen.includes(c), // chosen for the rerank set (rerank_skipped: why scores were not used, not whether sent)
    })),
  };
}

/** The document a candidate sends to rerank: hit representations and excerpt first, then attachments. */
const docBase = (c: Cand) =>
  [...new Set(c.reps.map((p) => p.text))].map((t) => `[summary] ${t}`).concat(c.excerpt ? `[excerpt] ${c.excerpt.text}` : c.hit ? `[excerpt] ${clip(c.hit.text, 2 * EXCERPT_SIDE)}` : "")
    .filter(Boolean).join("\n");
const docAttach = (c: Cand) => c.attach.map((a) => `[${a.what === "next" ? "next exchange" : "later in span"}, ${a.path ?? "unlocated"}] ${a.text}`).join("\n");

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

type ViewBasis = "observed" | "inferred-tail" | "unknown";
export type TargetView = {
  views: { ord: number; tip: string; keys: string[] }[];
  v?: { tip: string; keys: string[] }; // the view labels are taken against; absent when basis is unknown
  basis: ViewBasis;
  at: string | null; // when an observed leaf was reported
  reason?: string; // why the basis is unknown
};

/**
 * The target session's view (§4.2): the leaf its own plugin reported most recently (observed), else the host's
 * resume rule (Pi: the last entry written → inferred-tail). A reported leaf that names no projected view gives
 * `unknown` (it is not replaced by the tail: that would label against a path the user is not on).
 */
async function targetView(q: Tx, source: string, leaf?: { tip: string; at: string | null }): Promise<TargetView> {
  const views = (await q`select ord, tip, keys from view where source = ${source} order by ord`) as TargetView["views"];
  const [rep] = leaf ? [leaf] : await q`select leaf as tip, at from collector_view where source = ${source} and leaf is not null order by at desc limit 1`;
  if (rep) {
    const at = rep.at ? new Date(rep.at).toISOString() : null;
    const v = views.find((x) => x.tip === rep.tip);
    return v ? { views, v, basis: "observed", at } : { views, basis: "unknown", at, reason: "leaf-not-projected" };
  }
  if (!views.length) return { views, basis: "unknown", at: null, reason: "no-view" };
  const [t] = await q`select rec from record where source = ${source} order by first_pos desc limit 1`;
  return { views, v: views.find((x) => x.tip === t?.rec) ?? views.at(-1)!, basis: "inferred-tail", at: null };
}

/**
 * The version a view key pins: a fingerprint of the target source's whole read basis (its own `s:` version and
 * every fork ancestor's it was projected from). Any change to the text or partition behind the view moves it,
 * including an in-place revision with the same tip and a revision in a parent the view inherits.
 */
const viewVersion = (b: { valid: Map<string, Src> }, src: string) => {
  const d = b.valid.get(src)!.deps;
  const flat = Object.keys(d).filter((k) => k.startsWith("s:")).sort().map((k) => `${k}=${d[k]}`).join(",");
  return createHash("sha256").update(flat).digest("hex").slice(0, 16);
};

/** Path label of evidence `keys` against a target view (§4.3). */
export function labelOf(t: TargetView, keys: string[]): string {
  // Any supporting record that no view of the target holds: the evidence cannot be placed.
  if (!keys.length || keys.some((k) => !t.views.some((v) => v.keys.includes(k)))) return "unlocated";
  // No usable view (unknown basis): no on/off-path conclusion, even with a single view.
  if (!t.v) return "branched";
  const on = keys.filter((k) => t.v!.keys.includes(k)).length;
  return on === keys.length ? "on-path" : on === 0 ? "off-path" : "mixed";
}

/** A candidate's label from its representations' labels: never stronger than the weakest (§4.3). */
export function mergeLabels(ls: string[]): string {
  if (!ls.length) return "unlocated";
  for (const weak of ["unlocated", "branched"]) if (ls.includes(weak)) return weak;
  return ls.every((x) => x === ls[0]) ? ls[0] : "mixed";
}

export const searchHooks = { beforeTrace: async () => {} };

export async function search(db: Db, body: unknown): Promise<Guarded> {
  const r = validate(body);
  await snapshot(db, (q) => basis(q, r)); // the query's own authorization, before its text goes anywhere
  const t0 = performance.now();
  const emb = await embedQuery(db, r);
  const tEmbed = performance.now() - t0;
  let tries = 0;
  const run = async (): Promise<Guarded> => {
    if (++tries > 3) throw new HttpError(503, "state kept changing; retry");
    const t1 = performance.now();
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
      // Labels against each target session's view (the querying session's own: the leaf it sends).
      const tv = new Map<string, TargetView>();
      const viewOf = async (src: string) => {
        if (!tv.has(src)) tv.set(src, await targetView(q, src, src === r.source && r.leaf ? { tip: r.leaf, at: null } : undefined));
        return tv.get(src)!;
      };
      const labels = new Map<string, any>();
      for (const c of rc.chosen) {
        // One reference view per result: a record's source, a unit's own span's source (not whichever
        // representation ranked first). Every representation is labelled against that same view.
        const unitSrc = c.kind === "unit" ? (await q`select source from span where id = ${c.key.slice(5)}`)[0]?.source : undefined;
        const src: string = c.kind === "record" ? c.hit!.source : unitSrc && b.valid.has(unitSrc) ? unitSrc : c.reps[0].source;
        const t = await viewOf(src);
        const reps = c.reps.map((p) => labelOf(t, p.keys));
        // A unit's label merges all its representations (the excerpt never upgrades it); a record's is its own.
        const label = c.kind === "record" ? labelOf(t, [c.hit!.key]) : mergeLabels(reps);
        // Each attachment on its own (a correction may sit on another branch than the hit).
        for (const a of c.attach) a.path = labelOf(t, [a.key]);
        const vk = { source: src, tip: t.v?.tip ?? null, basis: t.basis, version: viewVersion(b, src), at: t.at };
        labels.set(c.key, { label, basis: t.basis, reason: t.reason ?? null, view_key: vk, reps, src });
      }
      // Index lag (§6.5): derived work not yet published when this search read the index.
      const lag = Object.fromEntries((await q`select kind, count(*)::int n from job where state in ('queued', 'running') group by kind`).map((x: any) => [x.kind, x.n]));
      return { b, rc, rerankSkip, dRerank: Number(dr?.v ?? 1), labels, lag };
    });
    const { b, rc, labels } = s;
    const tRecall = performance.now() - t1;
    const t2 = performance.now();
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
    const tRerank = performance.now() - t2;
    // After rerank: off-path evidence and a session's consecutive places are down-weighted (§6.3).
    const base = rc.chosen.map((c, i) => ({ c, i, score: rerankSkip ? c.fused : scores[i]! }));
    base.sort((x, y) => y.score - x.score || x.i - y.i);
    const seen = new Map<string, number>();
    const final = base.map((x) => {
      const src = labels.get(x.c.key).src as string;
      const n = seen.get(src) ?? 0;
      seen.set(src, n + 1);
      return { ...x, adj: x.score * (labels.get(x.c.key).label === "off-path" ? 0.8 : 1) * Math.pow(0.9, n) };
    }).sort((x, y) => y.adj - x.adj || x.i - y.i);
    const results = final.slice(0, r.limit).map((x) => {
      const c = x.c;
      const L = labels.get(c.key);
      return {
        key: c.key,
        // The result's target session: its reference source (the session its view_key names).
        source: L.src,
        scope: b.valid.get(L.src)!.scope,
        path: { label: L.label, view_basis: L.basis, view_key: L.view_key, ...(L.reason ? { reason: L.reason } : {}) },
        score: x.adj,
        rerank_score: rerankSkip ? null : scores[x.i],
        reps: c.reps.map((p, j) => ({ span: p.span, source: p.source, channel: p.channel, text: p.text, path: L.reps[j] })),
        excerpt: c.excerpt ?? null,
        attachments: c.attach,
        incomplete: [...new Set(c.incomplete)],
        injectable: !rerankSkip && !c.incomplete.length,
        // Historical statements, not the current state (§6.3).
        stated: true,
      };
    });
    // Rerank stage, merged into the pool entries: rerank rank (by score), adjusted rank, attachments.
    const rerankRank = new Map<Cand, number>();
    if (!rerankSkip) [...rc.chosen.keys()].sort((x, y) => scores[y]! - scores[x]! || x - y).forEach((i, n) => rerankRank.set(rc.chosen[i], n + 1));
    const byKey = new Map(rc.chosen.map((c, i) => [c.key, { c, i }]));
    const pool = rc.pool.map((p) => {
      const h = byKey.get(p.key);
      if (!h) return p;
      return { ...p, selected_pos: h.i + 1, rerank: scores[h.i], rerank_rank: rerankRank.get(h.c) ?? null, final_rank: final.findIndex((x) => x.c === h.c) + 1,
               returned: final.findIndex((x) => x.c === h.c) < r.limit!, attached: h.c.attach.length, incomplete: [...new Set(h.c.incomplete)] };
    });
    // The trace is published like any derived artefact: under the shared barrier, only while the query's
    // basis and every candidate's still hold (a forget of the querying session deletes its traces).
    await searchHooks.beforeTrace();
    const t = await db.begin(async (tx: Tx) => {
      await tx`select pg_advisory_xact_lock_shared(${BARRIER})`;
      if (!(await unchanged(tx, deps))) return null;
      const [row] = await tx`insert into search_trace (source, purpose, data) values (${r.source}, ${r.purpose},
                             ${{
                               channels: rc.channels, top: rc.top, pool,
                               embedding_skipped: emb.skipped ?? null, rerank_skipped: rerankSkip ?? null,
                               returned: results.map((x) => x.key), attempt: tries, index_lag: s.lag,
                               ms: { embed: Math.round(tEmbed), recall: Math.round(tRecall), rerank: Math.round(tRerank), total: Math.round(performance.now() - t0) },
                             }}) returning id`;
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
  const d: Record<string, number> = { ...b.qDeps }; // the querying session's own version and read grant, always
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

const cont = (b: Buffer, i: number) => i < b.length && (b[i] & 0xc0) === 0x80;
/**
 * A page of `limit` bytes over records laid end to end, starting at byte `offset`. Cuts land on UTF-8
 * character boundaries (an offset inside a character moves forward, an end inside one moves back); `next` is
 * the offset actually reached, so pages never split or repeat a character.
 */
function page(recs: { key: string; buf: Buffer }[], offset: number, limit: number) {
  const parts: { key: string; bytes: number; from: number; text: string }[] = [];
  let pos = 0, left = limit, next: number | null = null;
  for (const { key, buf } of recs) {
    const end = pos + buf.length;
    if (offset >= end || !buf.length) {
      pos = end;
      continue;
    }
    let from = Math.max(0, offset - pos);
    while (cont(buf, from)) from++;
    if (left <= 0) {
      next = pos + from;
      break;
    }
    let to = Math.min(buf.length, from + left);
    while (to > from && cont(buf, to)) to--;
    if (to === from && from < buf.length) {
      // Not even one character fits: the page ends here (a limit under 4 bytes still makes progress).
      if (!parts.length) while (to < buf.length && (to === from || cont(buf, to))) to++;
      else {
        next = pos + from;
        break;
      }
    }
    parts.push({ key, bytes: buf.length, from, text: buf.subarray(from, to).toString() });
    left -= to - from;
    if (to < buf.length) {
      next = pos + to;
      break;
    }
    pos = end;
  }
  return { parts, more: next !== null, next };
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
      // The view: the search result's `view_key` as returned, or `leaf` for the querying session's own current
      // view. A key pins a version (viewVersion): when the source has changed since, the request is refused
      // (409 view-stale) for spans and records alike, never answered with the current text or neighbours.
      let vkIn: { source: string; tip: string | null; basis: ViewBasis } | null = null;
      if (body.view_key !== undefined) {
        const k = body.view_key;
        if (!k || typeof k !== "object" || typeof k.source !== "string" || (typeof k.tip !== "string" && k.tip !== null)
            || !["observed", "inferred-tail", "unknown"].includes(k.basis) || typeof k.version !== "string") throw new HttpError(400, "view_key");
        if (!b.valid.has(k.source)) throw new HttpError(404, "not found or not readable");
        if (k.version !== viewVersion(b, k.source)) throw new HttpError(409, "view-stale");
        vkIn = { source: k.source, tip: k.tip, basis: k.basis };
      } else if (body.version !== undefined) {
        // A grep hit's pin: the version of the source it was found in (`in`); checked like a view key's.
        if (typeof body.version !== "string" || typeof body.in !== "string") throw new HttpError(400, "version needs in");
        if (!b.valid.has(body.in)) throw new HttpError(404, "not found or not readable");
        if (body.version !== viewVersion(b, body.in)) throw new HttpError(409, "view-stale");
      } else if (typeof body.leaf === "string") {
        if (!b.valid.has(r.source)) throw new HttpError(404, "not found or not readable");
        vkIn = { source: r.source, tip: body.leaf, basis: "observed" };
      }
      if (id.startsWith("record:")) {
        const canon = id.slice(7);
        // One canonical record is one host entry; it may hold several records (#0 text, #1 a tool call ...).
        // Fork copies hold the same canonical record and may keep different revisions: `in` (from a grep hit or
        // a view key) picks the copy; it must be readable, never silently replaced by another one.
        // With a view key the record must be on one of its source's views (the text may be owned by a fork
        // parent: the view's record keys name the version it shows).
        const inSrc = typeof body.in === "string" ? body.in : null;
        if (inSrc && !b.valid.has(inSrc)) throw new HttpError(404, "not found or not readable");
        const vviews = vkIn ? await q`select tip, keys from view where source = ${vkIn.source}` : [];
        const onView: string[] | null = vkIn ? [...new Set<string>(vviews.flatMap((v: any) => v.keys as string[]))] : null;
        const [row] = await q`select source from lexical l where canon = ${canon} and source = any(${q.array(inSrc ? [inSrc] : valid, "text")})
                              and (${onView === null} or key = any(${q.array(onView ?? [], "text")}))
                              and not exists (select 1 from tomb t where t.key = 'canon:' || l.canon) order by source limit 1`;
        if (!row) throw new HttpError(404, "not found or not readable");
        const keys: string[] = (await q`select distinct key from lexical where canon = ${canon} and source = ${row.source}`).map((x: any) => x.key);
        keys.sort((x: string, y: string) => Number(x.slice(x.lastIndexOf("#") + 1)) - Number(y.slice(y.lastIndexOf("#") + 1)));
        const t = await texts(q, keys, [row.source]);
        const pg = page(keys.map((k) => ({ key: k, buf: Buffer.from(t.get(k)?.text ?? "") })), offset, limit);
        // The key's view, resolved like a span's: its tip must name a view of its source; the record may be
        // off that view (its text is still served, the label says where it is).
        let vout: object = {};
        if (vkIn) {
          const v: any = vkIn.tip === null ? undefined : vviews.find((x: any) => x.tip === vkIn!.tip);
          const vb: ViewBasis = v && vkIn.basis !== "unknown" ? vkIn.basis : "unknown";
          vout = {
            view_key: { source: vkIn.source, tip: v?.tip ?? null, basis: vb, version: viewVersion(b, vkIn.source) }, view_basis: vb,
            ...(v ? { path: labelOf({ views: vviews.map((x: any, i: number) => ({ ord: i, tip: x.tip, keys: x.keys })), v, basis: vb, at: null }, keys) }
                  : { reason: vkIn.tip === null ? "view-unknown" : "view-not-found" }),
          };
        }
        return {
          deps: depsOf(b, [row.source, ...(vkIn ? [vkIn.source] : [])]),
          body: { id, source: row.source, ...vout, records: pg.parts.map((x) => ({ ...x, kind: t.get(x.key)?.kind })), more: pg.more, next_offset: pg.next },
        };
      }
      const sid = id.replace(/^(span|unit):/, "");
      const [sp] = await q`select id, source, keys, canons from span where id = ${sid} and source = any(${q.array(valid, "text")})`;
      if (!sp || (await q`select 1 from tomb where key = any(${q.array((sp.canons as string[]).map((c) => "canon:" + c), "text")}) limit 1`).length)
        throw new HttpError(404, "not found or not readable");
      let vsrc: string = sp.source, v: any, vb: ViewBasis, reason: string | null = null;
      if (vkIn) {
        vsrc = vkIn.source; // may be a fork child whose view holds this parent-owned span
        const views = await q`select ord, tip, spans from view where source = ${vsrc} and span_ids @> array[${sid}]::text[]`;
        v = vkIn.tip === null ? undefined : views.find((x: any) => x.tip === vkIn.tip);
        vb = v && vkIn.basis !== "unknown" ? vkIn.basis : "unknown";
        if (!v) reason = vkIn.tip === null ? "view-unknown" : "view-not-found";
      } else {
        const t = await targetView(q, sp.source);
        const views = await q`select ord, tip, spans from view where source = ${sp.source} and span_ids @> array[${sid}]::text[]`;
        v = t.v && views.find((x: any) => x.tip === t.v!.tip);
        vb = t.basis;
        reason = t.reason ?? null;
        if (t.v && !v) (vb = "unknown"), (reason = "span-off-view");
      }
      const order = v ? (v.spans as any[]).filter((x) => x.id).map((x) => x.id as string) : [sid];
      const i = order.indexOf(sid);
      const t = await texts(q, sp.keys, valid);
      const pg = page((sp.keys as string[]).filter((k) => t.has(k)).map((k) => ({ key: k, buf: Buffer.from(t.get(k).text) })), offset, limit);
      const records = pg.parts.map((x) => ({ ...x, canon: t.get(x.key).canon, kind: t.get(x.key).kind }));
      const more = pg.more;
      const summary = (await q`select text from span_summary ss join span s on s.id = ss.span where ss.span = ${sid} and ss.input_fp = s.input_fp`)[0]?.text ?? null;
      return {
        deps: depsOf(b, [sp.source, vsrc, ...[...t.values()].map((x) => x.source)]),
        body: {
          id: "span:" + sid, source: sp.source, view_key: { source: vsrc, tip: v?.tip ?? null, basis: vb, version: viewVersion(b, vsrc) },
          view_basis: vb, ...(reason ? { reason } : {}), summary, records, more, next_offset: pg.next,
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
      // Matching runs on each record's whole text (its chunks joined), in Postgres only: one regex engine
      // (ARE), its time bounded by the statement timeout. Positions come back as UTF-8 byte offsets in the
      // record text, the unit mem_zoom pages in. A literal pattern is located through the chunk index first;
      // a match across a chunk boundary is found by joining each chunk with the start of the next.
      const rows = await (regex
        ? q`with t as (select key, source, canon, kind, string_agg(text, '' order by part) text from lexical l
                       where source = any(${q.array(valid, "text")})
                       and not exists (select 1 from tomb tb where tb.key = 'canon:' || l.canon)
                       group by key, source, canon, kind),
                 m as (select distinct on (key) key, source, canon, kind, text, regexp_instr(text, ${pattern}) at from t
                       where text ~ ${pattern} order by key, source)
            select key, source, canon, kind, octet_length(left(text, at - 1)) pre,
                   octet_length(coalesce(regexp_substr(text, ${pattern}), '')) len,
                   substr(text, greatest(1, at - 60), 60 + least(char_length(coalesce(regexp_substr(text, ${pattern}), '')), 200) + 100 - greatest(0, 61 - at)) snippet
            from m order by key limit ${GREP_MAX}`
        : q`with c as (select l.key, l.source from lexical l
                       left join lexical n on n.source = l.source and n.key = l.key and n.part = l.part + 1
                       where l.source = any(${q.array(valid, "text")})
                       and (l.text like ${"%" + esc(pattern) + "%"} or (n.text is not null and position(${pattern} in right(l.text, ${pattern.length - 1}) || left(n.text, ${pattern.length - 1})) > 0))
                       and not exists (select 1 from tomb tb where tb.key = 'canon:' || l.canon)
                       group by l.key, l.source),
                 t as (select distinct on (l.key) l.key, l.source, l.canon, l.kind, string_agg(l.text, '' order by l.part) text
                       from lexical l join c on c.key = l.key and c.source = l.source group by l.key, l.source, l.canon, l.kind order by l.key, l.source)
            select key, source, canon, kind, octet_length(left(text, strpos(text, ${pattern}) - 1)) pre, octet_length(${pattern}) len,
                   substr(text, greatest(1, strpos(text, ${pattern}) - 60), 60 + char_length(${pattern}) + 100 - greatest(0, 61 - strpos(text, ${pattern}))) snippet
            from t order by key limit ${GREP_MAX}`).catch((e: any) => {
        if (/regular expression|canceling statement/.test(String(e?.message))) throw new HttpError(400, "pattern invalid or too slow");
        throw e;
      });
      // Coordinates of mem_zoom on `record:<canon>` with `in: <source>`: the host entry's records (#0, #1 …)
      // laid end to end, in bytes. The hit's offset adds the lengths of the entry's earlier records.
      const before = rows.length
        ? await q`select h.key, coalesce(sum(octet_length(l.text)), 0)::bigint n
                  from unnest(${q.array(rows.map((x: any) => x.key), "text")}, ${q.array(rows.map((x: any) => x.source), "text")}) h(key, source)
                  left join lexical l on l.source = h.source and l.canon = (select canon from lexical where key = h.key and source = h.source limit 1)
                   and split_part(l.key, '#', 2)::int < split_part(h.key, '#', 2)::int
                  group by h.key`
        : [];
      const lead = new Map<string, number>(before.map((x: any) => [x.key, Number(x.n)]));
      const hits = rows.map((x: any) => ({
        record: "record:" + x.canon, in: x.source, version: viewVersion(b, x.source), key: x.key, kind: x.kind,
        offset: lead.get(x.key)! + Number(x.pre), length: Number(x.len), // UTF-8 bytes, mem_zoom's record coordinates
        snippet: x.snippet,
      }));
      return { deps: depsOf(b, [...new Set<string>(rows.map((x: any) => x.source))]), body: { hits, truncated: rows.length >= GREP_MAX } };
    });
    return new Guarded(out.deps, out.body, run);
  };
  return run();
}
