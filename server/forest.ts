// L2 binary summary forest (DESIGN §4.4, §5.5).
// Structure: the leaves of a view are its closed spans in order (prefix segments P included); the forest is
// the binary-carry decomposition of that sequence (13 leaves: [1–8][9–12][13]). Only adjacent trees of the
// same size merge, so appending leaves only adds nodes, and two views that share their first k leaves share
// every subtree inside them. Fewer than MIN_LEAVES leaves: no internal nodes, the leaves are listed.
// Identity: a node id binds what its summary is made from — for a leaf, the span, its input fingerprint and
// the summary text hash; for an internal node, its two children's ids, the merge prompt version and the
// model. The text of a node is written once; a different input is a different node.
// Structure is separate from readiness: a node exists as soon as all its leaves have current summaries;
// its text comes from one `merge` job per node (one model call, so a large tree never holds a job lock for
// long). A missing leaf summary leaves its place empty: nothing merges across it.
// Hot/cold: merges start only for sessions active within MEMEX_FOREST_HOT_DAYS (by the records' own
// timestamps, not import time) or read through the forest; others get structure only, and a read expands
// what is ready (children of a node without text) and asks for the merges in the background.
// Publication: a merge sends text to the model and stores a result, so it follows the barrier of derive.ts:
// an exact check of every input in one snapshot (a child whose stored version vector is no longer current
// is checked again down to its leaves, never just relabelled), the guarded send, and a locked comparison of
// the vector before the write.
import { type Db, type Job, type Tx, addMigration, enqueue, finishJob } from "./db.ts";
import { BARRIER } from "./sync.ts";
import { checkSpan, env, guardedPost, parseJson, publish, skip, snapshot, unchanged } from "./derive.ts";
import { canonOf } from "./project.ts";

export const FOREST_VERSION = "forest-merge-v1";
/** Test seam: runs in runForest's transaction right after it takes the barrier. */
export const forestHooks = { underBarrier: async (_tx: Tx) => {} };
/** Test seam: runs in runForest between its snapshot and its write. */
export const snapshotHook = { afterSnapshot: async () => {}, beforeMergeCommit: async (_node: string) => {} };
export const MIN_LEAVES = 4; // DESIGN §4.4 [待标定 4]
const NODE_MAX = 1200; // UTF-8 bytes [待标定]
const MAX_TRIES = 3;
const HOLD_MS = () => Number(process.env.MEMEX_FOREST_HOLD_MS ?? 5_000); // the barrier held shared while writing structure
const hotDays = () => Number(process.env.MEMEX_FOREST_HOT_DAYS ?? 30);

addMigration(`
create table if not exists forest_node (id text primary key, size int not null, kids jsonb not null, kid_ids text[] not null,
                                        spans text[] not null, text text, text_hash text, model text, prompt_v text, deps jsonb,
                                        at timestamptz);
create index if not exists forest_node_kids on forest_node using gin (kid_ids);
create index if not exists forest_node_spans on forest_node using gin (spans);
create table if not exists forest_access (source text primary key, at timestamptz not null default now());
`);

const sha = (s: string) => new Bun.CryptoHasher("sha256").update(s).digest("hex");
type Kid = { node: string } | { span: string; fp: string; hash: string };
const kidId = (k: Kid) => ("node" in k ? k.node : "fl_" + sha(`${k.span}\u0000${k.fp}\u0000${k.hash}`).slice(0, 32));
const nodeId = (model: string, a: Kid, b: Kid) => "fn_" + sha([FOREST_VERSION, model, kidId(a), kidId(b)].join("\u0000")).slice(0, 32);

/** Roots of the forest over n leaves as [start, size]; below MIN_LEAVES every leaf is its own root. */
export function roots(n: number): [number, number][] {
  if (n < MIN_LEAVES) return Array.from({ length: n }, (_, i) => [i, 1]);
  const out: [number, number][] = [];
  let start = 0;
  for (let bit = 2 ** 30; bit >= 1; bit /= 2)
    if (n & bit) {
      out.push([start, bit]);
      start += bit;
    }
  return out;
}

type Planned = { id: string; start: number; size: number; kids: [Kid, Kid]; spans: string[] };
export const leavesOf = (v: { spans: any[]; closed_end: number }) => v.spans.filter((x: any) => x.id && x.end <= v.closed_end).map((x: any) => x.id as string);

/** The nodes of one view whose leaves all have a current summary (no model call). */
async function plan(q: Tx, model: string, leaves: string[]): Promise<Planned[]> {
  if (leaves.length < MIN_LEAVES) return [];
  const rows = await q`select s.id, s.input_fp, ss.text_hash from span s
                       left join span_summary ss on ss.span = s.id and ss.input_fp = s.input_fp
                       where s.id = any(${q.array(leaves, "text")})`;
  const byId = new Map<string, any>(rows.map((r: any) => [r.id, r]));
  const out: Planned[] = [];
  const build = (start: number, size: number): Kid | null => {
    if (size === 1) {
      const r = byId.get(leaves[start]);
      return r?.text_hash ? { span: r.id, fp: r.input_fp, hash: r.text_hash } : null;
    }
    const a = build(start, size / 2);
    const b = build(start + size / 2, size / 2);
    if (!a || !b) return null;
    const id = nodeId(model, a, b);
    out.push({ id, start, size, kids: [a, b], spans: leaves.slice(start, start + size) });
    return { node: id };
  };
  for (const [s, z] of roots(leaves.length)) build(s, z);
  return out;
}

/** Job `forest` for one source: write the structure of all its views, queue the merges it can take now. */
export async function runForest(db: Db, job: Job) {
  const source = job.key;
  const model = env("MEMEX_LLM_MODEL");
  const p = await snapshot(db, async (q) => {
    const all = new Map<string, Planned>();
    for (const v of await q`select spans, closed_end from view where source = ${source} order by ord`)
      for (const n of await plan(q, model, leavesOf(v as any))) all.set(n.id, n);
    // Activity by the records' own timestamps (an import of old history is not recent activity).
    const [h] = await q`select coalesce(max(substring(b.raw from '"timestamp":"([0-9]{4}-[0-9T:.+Z-]+)"')::timestamptz)
                                 > now() - make_interval(days => ${hotDays()}), false)
                               or exists (select 1 from forest_access a where a.source = ${source}) hot
                        from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                        where r.source = ${source} and r.state = 'ok'`;
    const nodes = [...all.values()];
    const rows = await q`select id, text is not null written, deps, model from forest_node where id = any(${q.array(nodes.map((n) => n.id), "text")})`;
    const stored = new Map<string, any>(rows.map((r: any) => [r.id, r]));
    // New rows (or old rows missing their configuration); merges: a written node whose vector moved is
    // checked again (no model call) whether hot or not; an unwritten one whose children are written, if hot.
    const fresh = nodes.filter((n) => !stored.get(n.id)?.model && !stored.get(n.id)?.written)
      .map((n) => ({ id: n.id, size: n.size, kids: n.kids, kid_ids: n.kids.map(kidId), spans: n.spans }));
    const merge: string[] = [];
    for (const n of nodes) {
      const r = stored.get(n.id);
      if (r?.written) {
        if (!(await unchanged(q, r.deps))) merge.push(n.id);
      } else if (h?.hot) merge.push(n.id);
      // (Every unwritten node of a hot session is queued, whether its children looked written or not: a
      // child published concurrently can miss a parent this run inserts (its wake-up looked before the
      // insert). A merge whose children are not ready ends without a call; the child's publication, or this
      // job's next run, queues it again.)
    }
    // Session cards (§4.4): a hot view whose forest is complete gets its card (the job finds out whether
    // the stored card was made from the same input). A cold view of 4+ leaves is never complete before a
    // read asks for its merges (which makes it hot); `hot` here also keeps short cold views card-less.
    const cards: string[] = [];
    if (h?.hot && !merge.length)
      for (const v of await q`select tip, spans, closed_end from view where source = ${source} order by ord`)
        if (leavesOf(v as any).length && (await forestView(q, leavesOf(v as any), () => true)).complete) cards.push(JSON.stringify([source, v.tip]));
    return { fresh, merge, cards, spans: [...new Set(nodes.flatMap((n) => n.spans))] };
  });
  await snapshotHook.afterSnapshot();
  // Under the barrier only set-based statements, with the whole held interval bounded (as guardedPost):
  // what is new, what is ready and what moved were read in the snapshot above.
  return db.begin(async (tx: Tx) => {
    // transaction_timeout (PostgreSQL 17) bounds the whole transaction, not each statement: past it the
    // server ends the session, which rolls back and releases the barrier (the pool reconnects).
    await tx`select set_config('transaction_timeout', ${HOLD_MS() + "ms"}, true)`;
    await tx`select pg_advisory_xact_lock_shared(${BARRIER})`; // against forget, which deletes nodes of its spans
    await forestHooks.underBarrier(tx);
    const fin = await finishJob(tx, job);
    if (fin !== "done") return fin;
    const [have] = await tx`select count(*)::int n from span where id = any(${tx.array(p.spans, "text")})`;
    if (have.n !== p.spans.length) return fin; // re-projected or forgotten meanwhile; that queues this job again
    if (p.fresh.length)
      await tx`insert into forest_node (id, size, kids, kid_ids, spans, model, prompt_v)
               select x->>'id', (x->>'size')::int, x->'kids', array(select jsonb_array_elements_text(x->'kid_ids')),
                      array(select jsonb_array_elements_text(x->'spans')), ${model}, ${FOREST_VERSION}
               from jsonb_array_elements(${p.fresh as any}::jsonb) x
               on conflict (id) do update set model = excluded.model, prompt_v = excluded.prompt_v
                 where forest_node.model is null and forest_node.text is null`;
    // (A row stored before the configuration was recorded: the id, planned now from the same inputs under
    // this model and prompt, says which configuration it is for.)
    for (const [kind, keys] of [["merge", p.merge], ["card", p.cards]] as const)
      if (keys.length)
        await tx`insert into job (kind, key) select ${kind}, x from unnest(${tx.array(keys, "text")}::text[]) x
                 on conflict (kind, key) where state in ('queued', 'running')
                 do update set req = job.req + 1, updated_at = now()`;
    return fin;
  });
}

type KidCheck = { ok: true; deps: Record<string, number>; text: string } | { ok: false; reason: string; scopes?: string[] };

/** Exact check of one child in the caller's snapshot: a leaf through checkSpan plus its summary; a node by
 *  its stored vector, or — when that vector moved — by checking its own children again. */
async function checkKid(q: Tx, k: Kid): Promise<KidCheck> {
  if ("span" in k) {
    const r = await checkSpan(q, k.span, "llm");
    if (!r.ok) return r;
    if (r.span.input_fp !== k.fp) return { ok: false, reason: "record-changed" };
    const [s] = await q`select text from span_summary where span = ${k.span} and input_fp = ${k.fp} and text_hash = ${k.hash}`;
    return s ? { ok: true, deps: r.deps, text: s.text } : { ok: false, reason: "not-ready" };
  }
  const [n] = await q`select text, deps, kids from forest_node where id = ${k.node}`;
  if (!n || n.text === null) return { ok: false, reason: "not-ready" };
  if (await unchanged(q, n.deps)) return { ok: true, deps: n.deps, text: n.text };
  const deps: Record<string, number> = {};
  for (const kk of n.kids as Kid[]) {
    const r = await checkKid(q, kk);
    if (!r.ok) return r;
    Object.assign(deps, r.deps);
  }
  return { ok: true, deps, text: n.text };
}

export const FOREST_SYSTEM =
  "You merge the summaries of two consecutive parts of one coding-agent session into one summary of both, " +
  "for a long-term memory index used to navigate that session. Part 1 comes before part 2.\n" +
  "Keep what the user asked, decided and corrected, results that were verified, and what is still open; drop " +
  "repetition. Do not add facts the parts do not contain. Write in the language of the parts.\n" +
  `Return one JSON object: {"summary": string}, summary at most ${NODE_MAX} UTF-8 bytes (Chinese characters are 3 bytes each).`;

/** Job `merge` for one node: its text from its children's (once), or a re-checked vector for written text. */
export async function runMerge(db: Db, job: Job) {
  const url = env("MEMEX_LLM_URL").replace(/\/+$/, "") + "/chat/completions";
  const model = env("MEMEX_LLM_MODEL");
  const key = env("MEMEX_LLM_KEY");
  let problem = "";
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const [n] = await q`select text, text_hash, deps, kids, model, prompt_v, spans from forest_node where id = ${job.key}`;
      if (!n) return { ok: false as const, reason: "gone" };
      // The id names the model and prompt it was planned for; another configuration plans its own nodes.
      if (n.model !== model || n.prompt_v !== FOREST_VERSION) {
        const srcs = await q`select distinct source from view where span_ids @> array[${n.spans[0]}]::text[]`;
        return { ok: false as const, reason: "config-changed", replan: srcs.map((r: any) => r.source as string) };
      }
      if (n.text !== null && (await unchanged(q, n.deps))) return { ok: false as const, reason: "current" };
      const texts: string[] = [];
      const deps: Record<string, number> = {};
      for (const k of n.kids as Kid[]) {
        const r = await checkKid(q, k);
        if (!r.ok) return r;
        texts.push(r.text);
        Object.assign(deps, r.deps);
      }
      return { ok: true as const, written: n.text !== null, hash: n.text_hash as string | null, texts, deps };
    });
    // Not granted: remembered, so allowing it queues the merge again; anything else (a child not ready, a
    // withdrawn input, a node forgotten) ends here — the next summary or projection queues it again.
    if (!c.ok && "replan" in c)
      return db.begin(async (tx: Tx) => {
        const fin = await finishJob(tx, job);
        for (const s of c.replan as string[]) await enqueue(tx, "forest", s);
        return fin;
      });
    if (!c.ok) return skip(db, job, c.reason, (c as any).scopes ?? []);
    if (c.written)
      return publish(db, job, c.deps, (tx) => tx`update forest_node set deps = ${c.deps} where id = ${job.key} and text_hash = ${c.hash}`);
    const messages = [
      { role: "system", content: FOREST_SYSTEM },
      { role: "user", content: c.texts.map((t, j) => `<part n="${j + 1}">\n${t}\n</part>`).join("\n") },
    ];
    if (problem) messages.push({ role: "user", content: "Your previous answer was invalid: " + problem + ". Return a corrected JSON object." });
    const r = await guardedPost(db, c.deps, url, key, { model, messages, temperature: 0, reasoning_effort: "none", response_format: { type: "json_object" } });
    if (r === "stale") continue;
    if (r.status !== 200) throw new Error(`llm http ${r.status}`);
    let text: unknown;
    try {
      text = parseJson(r.json?.choices?.[0]?.message?.content ?? "")?.summary;
    } catch {}
    if (typeof text !== "string" || !text.trim()) {
      problem = "no summary string";
      continue;
    }
    if (Buffer.byteLength(text) > NODE_MAX) {
      problem = `summary is ${Buffer.byteLength(text)} bytes, more than ${NODE_MAX}`;
      continue;
    }
    const t = text;
    return publish(db, job, c.deps, async (tx) => {
      const done = await tx`update forest_node set text = ${t}, text_hash = ${sha(t)}, model = ${model}, prompt_v = ${FOREST_VERSION},
                                   deps = ${c.deps}, at = now() where id = ${job.key} and text is null returning id`;
      // Written once; the parents that were waiting for it can merge now.
      if (done.length) {
        for (const p of await tx`select id from forest_node where kid_ids @> array[${job.key}]::text[] and text is null`) await enqueue(tx, "merge", p.id);
        // The views holding it may be complete now: their forest job queues their session cards.
        const [n] = await tx`select spans from forest_node where id = ${job.key}`;
        for (const v of await tx`select distinct source from view where span_ids @> array[${n.spans[0]}]::text[]`) await enqueue(tx, "forest", v.source);
      }
      await snapshotHook.beforeMergeCommit(job.key);
    });
  }
  throw new Error(problem ? `merge invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

export type ForestItem = { start: number; size: number; node?: string; span?: string; text: string | null };

/** The canonical sources a span summary was made from (records and <context>), from its keys. */
const genSources = (harness: string, span: { records: any[]; context: any[] }) =>
  [...new Set([...span.records, ...span.context].map((r: any) => canonOf(harness, r.key).split(":")[0]))];

/** Read one view's forest in the caller's snapshot (no model call, no write). `readable(source)` says whether
 *  a generation source may be shown to this reader: a leaf summary is used only if every source it was made
 *  from (records and <context>) is readable and none of its records is forgotten; a node only if every leaf
 *  under it is. `start/size` select a subtree (a node being expanded); default: the whole view. Returns the
 *  items (a node with current text, else its children, down to leaves), the sources used, and whether every
 *  item was a ready node or a root leaf. */
export async function forestView(q: Tx, leaves: string[], readable: (source: string) => boolean, sel?: { start: number; size: number }) {
  const model = env("MEMEX_LLM_MODEL");
  const rows = await q`select s.id, s.records, s.context, ss.text, src.harness from span s join source src on src.id = s.source
                       join span_summary ss on ss.span = s.id and ss.input_fp = s.input_fp
                       where s.id = any(${q.array(leaves, "text")})
                         and not exists (select 1 from unnest(s.canons) c join tomb t on t.key = 'canon:' || c)`;
  const sums = new Map<string, { text: string; from: string[] }>();
  for (const x of rows) {
    const from = genSources(x.harness, x);
    if (from.every(readable)) sums.set(x.id, { text: x.text, from });
  }
  const planned = new Map((await plan(q, model, leaves)).map((n) => [`${n.start}:${n.size}`, n]));
  // A node id is planned from the leaves' current input and summary hashes, so a stored node with that id
  // was made from exactly these leaves: readable while all of them are (its stored vector, with the model
  // destination, is for publication, not for reading).
  const nodeRows = await q`select id, text, spans from forest_node where id = any(${q.array([...planned.values()].map((n) => n.id), "text")}) and text is not null`;
  const usable = new Map<string, string>();
  for (const x of nodeRows) if ((x.spans as string[]).every((sp) => sums.has(sp))) usable.set(x.id, x.text);
  let complete = true;
  const items: ForestItem[] = [];
  const used = new Set<string>();
  const addFrom = (from: number, n: number) => leaves.slice(from, from + n).forEach((sp) => sums.get(sp)?.from.forEach((x) => used.add(x)));
  const expand = (start: number, size: number, top: boolean) => {
    if (size === 1) {
      const sm = sums.get(leaves[start]);
      if (!sm || !top) complete = false;
      if (sm) addFrom(start, 1);
      items.push({ start, size, span: leaves[start], text: sm?.text ?? null });
      return;
    }
    const n = planned.get(`${start}:${size}`);
    const text = n && usable.get(n.id);
    if (n && text) {
      addFrom(start, size);
      return void items.push({ start, size, node: n.id, text });
    }
    complete = false;
    expand(start, size / 2, false);
    expand(start + size / 2, size / 2, false);
  };
  if (sel) {
    // Children of the selected node (its own text is what the caller already has).
    if (sel.size > 1) for (const h of [0, sel.size / 2]) expand(sel.start + h, sel.size / 2, true);
  } else for (const [st, z] of roots(leaves.length)) expand(st, z, true);
  return { items, used: [...used], complete, leafText: (sp: string) => sums.get(sp)?.text ?? null };
}

/** Where node `id` sits on a view's leaves, if it is one of that view's planned nodes. */
export async function nodeOn(q: Tx, leaves: string[], id: string) {
  const n = (await plan(q, env("MEMEX_LLM_MODEL"), leaves)).find((x) => x.id === id);
  return n ? { start: n.start, size: n.size } : null;
}

/** Remember a read that was not fully ready, and ask for its merges in the background (after the read). */
export async function wantForest(db: Db, source: string) {
  await db.begin(async (tx: Tx) => {
    await tx`insert into forest_access values (${source}) on conflict (source) do update set at = now()`;
    await enqueue(tx, "forest", source);
  });
}

/** The forest of one view (tests and internal use): every source counts as readable. */
export async function readForest(db: Db, source: string, tip: string) {
  const r = await snapshot(db, async (q) => {
    const [v] = await q`select spans, closed_end from view where source = ${source} and tip = ${tip}`;
    if (!v) return null;
    const [pr] = await q`select deps from projected where source = ${source}`;
    if (!pr || !(await unchanged(q, pr.deps))) return { stale: true, complete: false, items: [] as ForestItem[] };
    const f = await forestView(q, leavesOf(v as any), () => true);
    return { stale: false, complete: f.complete, items: f.items };
  });
  if (r && !r.complete) await wantForest(db, source);
  return r;
}

// ---------- session card (§4.4) ----------
// One card per view (source, tip), made by the model from the view's forest roots and its most recent span
// summaries. The view key names which card; the input fingerprint (texts, model, prompt) says whether the
// stored card was made from what the view shows now. A card not current is not shown: the reader gets a
// deterministic degraded card from the ready summaries instead, never a model call.

export const CARD_VERSION = "session-card-v1";
const CARD_MAX = 1536; // UTF-8 bytes of overview, DESIGN §4.4 [待标定 ≤1.5KB]
const RECENT = 3; // most recent span summaries next to the roots [待标定]

addMigration(`
create table if not exists session_card (source text not null, tip text not null, input_fp text not null, card jsonb not null,
                                         model text not null, prompt_v text not null, deps jsonb not null,
                                         at timestamptz not null default now(), primary key (source, tip));
`);

const clipBytes = (t: string, n: number) => {
  const b = Buffer.from(t);
  if (b.length <= n) return t;
  let i = n;
  while (i > 0 && (b[i] & 0xc0) === 0x80) i--; // never cut inside a character
  return b.subarray(0, i).toString();
};

type CardInput = { roots: { id: string; text: string }[]; recent: { id: string; text: string }[]; complete: boolean; leaves: string[]; used: string[] };

/** What a view's card is made from, in the caller's snapshot; `readable` as for forestView. */
async function cardInput(q: Tx, leaves: string[], readable: (s: string) => boolean): Promise<CardInput> {
  const f = await forestView(q, leaves, readable);
  const roots = f.items.filter((i) => i.text !== null).map((i) => ({ id: i.node ? "node:" + i.node : "span:" + i.span, text: i.text! }));
  const recent = leaves.slice(-RECENT).map((sp) => ({ id: "span:" + sp, text: f.leafText(sp) }));
  return {
    roots, recent: recent.filter((r) => r.text !== null) as { id: string; text: string }[],
    complete: f.complete && recent.every((r) => r.text !== null), leaves, used: f.used,
  };
}
const cardFp = (model: string, i: CardInput) => sha(JSON.stringify([CARD_VERSION, model, i.roots, i.recent]));

/** cwd and time range of a source, from its own records (not import time). */
async function cardMeta(q: Tx, source: string) {
  const [m] = await q`select min(t) first_at, max(t) last_at, (array_agg(cwd) filter (where cwd is not null))[1] cwd from (
                        select substring(b.raw from '"timestamp":"([0-9]{4}-[0-9T:.+Z-]+)"')::timestamptz t,
                               coalesce(b.raw::jsonb->>'cwd', b.raw::jsonb->'payload'->>'cwd') cwd
                        from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                        where r.source = ${source} and r.state = 'ok') x`;
  return { cwd: m?.cwd ?? null, first_at: m?.first_at ?? null, last_at: m?.last_at ?? null };
}

export const CARD_SYSTEM =
  "You write the card of one coding-agent session for a long-term memory index, from summaries of its parts " +
  "in order (<part>) and of its most recent exchanges (<recent>). The card is read at the start of later " +
  "sessions to recall what this one was about.\n" +
  "Keep what the user wanted and decided, results that were verified, and what is still open. Do not add facts " +
  "the summaries do not contain. Write in the language of the summaries.\n" +
  'Return one JSON object: {"title": string (at most 80 bytes), "overview": string, "results": [string], ' +
  `"open_threads": [string]}; overview at most ${CARD_MAX} UTF-8 bytes (Chinese characters are 3 bytes each), ` +
  "results and open_threads at most 5 items of at most 200 bytes each.";

/** Job `card` for one view: key JSON [source, tip]. */
export async function runCard(db: Db, job: Job) {
  const [source, tip] = JSON.parse(job.key) as [string, string];
  const url = env("MEMEX_LLM_URL").replace(/\/+$/, "") + "/chat/completions";
  const model = env("MEMEX_LLM_MODEL");
  const key = env("MEMEX_LLM_KEY");
  let problem = "";
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const [v] = await q`select spans, closed_end from view where source = ${source} and tip = ${tip}`;
      if (!v) return { ok: false as const, reason: "gone" };
      const [pr] = await q`select deps from projected where source = ${source}`;
      if (!pr || !(await unchanged(q, pr.deps))) return { ok: false as const, reason: "projection-stale" };
      const input = await cardInput(q, leavesOf(v as any), () => true);
      if (!input.complete) return { ok: false as const, reason: "not-ready" };
      const fp = cardFp(model, input);
      // Exact check of every text that is sent (nodes and leaves, down to their records), for the model.
      const deps: Record<string, number> = { ...pr.deps };
      for (const it of [...input.roots, ...input.recent]) {
        const k: Kid = it.id.startsWith("node:") ? { node: it.id.slice(5) } : await leafKid(q, it.id.slice(5));
        const r = await checkKid(q, k);
        if (!r.ok) return r;
        Object.assign(deps, r.deps);
      }
      const [old] = await q`select input_fp, deps from session_card where source = ${source} and tip = ${tip}`;
      return { ok: true as const, input, fp, deps, current: old?.input_fp === fp, meta: await cardMeta(q, source) };
    });
    if (!c.ok) return skip(db, job, c.reason, (c as any).scopes ?? []);
    const write = (card: object | null) => async (tx: Tx) => {
      if (!(await tx`select 1 from view where source = ${source} and tip = ${tip}`).length) return;
      if (card === null) await tx`update session_card set deps = ${c.deps} where source = ${source} and tip = ${tip} and input_fp = ${c.fp}`;
      else
        await tx`insert into session_card (source, tip, input_fp, card, model, prompt_v, deps)
                 values (${source}, ${tip}, ${c.fp}, ${card as any}, ${model}, ${CARD_VERSION}, ${c.deps})
                 on conflict (source, tip) do update set input_fp = excluded.input_fp, card = excluded.card, model = excluded.model,
                   prompt_v = excluded.prompt_v, deps = excluded.deps, at = now()`;
    };
    if (c.current) return publish(db, job, c.deps, write(null));
    const body = [...c.input.roots.map((r) => `<part>\n${r.text}\n</part>`), ...c.input.recent.map((r) => `<recent>\n${r.text}\n</recent>`)].join("\n");
    const messages = [{ role: "system", content: CARD_SYSTEM }, { role: "user", content: body }];
    if (problem) messages.push({ role: "user", content: "Your previous answer was invalid: " + problem + ". Return a corrected JSON object." });
    const r = await guardedPost(db, c.deps, url, key, { model, messages, temperature: 0, reasoning_effort: "none", response_format: { type: "json_object" } });
    if (r === "stale") continue;
    if (r.status !== 200) throw new Error(`llm http ${r.status}`);
    let out: any;
    try {
      out = parseJson(r.json?.choices?.[0]?.message?.content ?? "");
    } catch {}
    const strs = (x: any) => Array.isArray(x) && x.every((y) => typeof y === "string") && x.length <= 5 && x.every((y: string) => Buffer.byteLength(y) <= 200);
    if (!out || typeof out.title !== "string" || typeof out.overview !== "string" || !strs(out.results) || !strs(out.open_threads)) {
      problem = "keys title, overview, results, open_threads with the stated types and limits";
      continue;
    }
    if (Buffer.byteLength(out.overview) > CARD_MAX || Buffer.byteLength(out.title) > 80) {
      problem = `overview at most ${CARD_MAX} bytes, title at most 80`;
      continue;
    }
    const card = { title: out.title, overview: out.overview, results: out.results, open_threads: out.open_threads, ...c.meta,
                   source_span_ids: c.input.leaves };
    return publish(db, job, c.deps, write(card));
  }
  throw new Error(problem ? `card invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

/** A leaf's current identity (span, input, summary hash) for the exact check. */
async function leafKid(q: Tx, span: string): Promise<Kid> {
  const [r] = await q`select s.input_fp, ss.text_hash from span s join span_summary ss on ss.span = s.id and ss.input_fp = s.input_fp where s.id = ${span}`;
  return { span, fp: r?.input_fp ?? "", hash: r?.text_hash ?? "" };
}

/** The card of a view for reading, in the caller's snapshot: the stored card if it was made from exactly what
 *  the view shows this reader now, else a degraded card from the ready summaries. No model call. */
export async function readCard(q: Tx, source: string, leaves: string[], tip: string, readable: (s: string) => boolean) {
  const input = await cardInput(q, leaves, readable);
  const meta = await cardMeta(q, source);
  const [row] = await q`select card, input_fp from session_card where source = ${source} and tip = ${tip} and model = ${env("MEMEX_LLM_MODEL")} and prompt_v = ${CARD_VERSION}`;
  if (row && row.input_fp === cardFp(env("MEMEX_LLM_MODEL"), input)) return { card: { ...row.card, ...meta }, degraded: false, used: input.used };
  const overview = clipBytes(input.roots.map((r) => r.text).join("\n"), CARD_MAX);
  return {
    card: { title: null, overview, results: [], open_threads: [], ...meta, source_span_ids: leaves },
    degraded: true, used: input.used,
  };
}
