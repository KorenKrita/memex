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

export const FOREST_VERSION = "forest-merge-v1";
export const MIN_LEAVES = 4; // DESIGN §4.4 [待标定 4]
const NODE_MAX = 1200; // UTF-8 bytes [待标定]
const MAX_TRIES = 3;
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
const leavesOf = (v: { spans: any[]; closed_end: number }) => v.spans.filter((x: any) => x.id && x.end <= v.closed_end).map((x: any) => x.id as string);

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
    return { nodes: [...all.values()], hot: !!h?.hot };
  });
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock_shared(${BARRIER})`; // against forget, which deletes nodes of its spans
    const fin = await finishJob(tx, job);
    if (fin !== "done") return fin;
    const spans = [...new Set(p.nodes.flatMap((n) => n.spans))];
    const [have] = await tx`select count(*)::int n from span where id = any(${tx.array(spans, "text")})`;
    if (have.n !== spans.length) return fin; // re-projected or forgotten meanwhile; that queues this job again
    for (const n of p.nodes)
      await tx`insert into forest_node (id, size, kids, kid_ids, spans)
               values (${n.id}, ${n.size}, ${n.kids as any}, ${tx.array(n.kids.map(kidId), "text")}, ${tx.array(n.spans, "text")})
               on conflict (id) do nothing`;
    const ids = p.nodes.map((n) => n.id);
    const rows = await tx`select id, text is not null written, deps from forest_node where id = any(${tx.array(ids, "text")})`;
    const written = new Map<string, any>(rows.map((r: any) => [r.id, r]));
    for (const n of p.nodes) {
      const r = written.get(n.id)!;
      // A written node whose vector moved: checked again (no model call), whether the session is hot or not.
      if (r.written) {
        if (!(await unchanged(tx, r.deps))) await enqueue(tx, "merge", n.id);
        continue;
      }
      const kidsReady = n.kids.every((k) => !("node" in k) || written.get(k.node)?.written);
      if (p.hot && kidsReady) await enqueue(tx, "merge", n.id);
    }
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
      const [n] = await q`select text, text_hash, deps, kids from forest_node where id = ${job.key}`;
      if (!n) return { ok: false as const, reason: "gone" };
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
      if (done.length) for (const p of await tx`select id from forest_node where kid_ids @> array[${job.key}]::text[] and text is null`) await enqueue(tx, "merge", p.id);
    });
  }
  throw new Error(problem ? `merge invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

export type ForestItem = { start: number; size: number; node?: string; span?: string; text: string | null };

/** The forest of one view for reading (zoom, session card): each root as its node when its text is current,
 *  otherwise expanded into its children, down to leaf summaries. Never calls a model: a view not fully
 *  ready is remembered as accessed and its merges are queued in the background. */
export async function readForest(db: Db, source: string, tip: string) {
  const model = env("MEMEX_LLM_MODEL");
  const r = await snapshot(db, async (q) => {
    const [v] = await q`select spans, closed_end from view where source = ${source} and tip = ${tip}`;
    if (!v) return null;
    const [pr] = await q`select deps from projected where source = ${source}`;
    if (!pr || !(await unchanged(q, pr.deps))) return { stale: true, complete: false, items: [] as ForestItem[] };
    const leaves = leavesOf(v as any);
    const sums = new Map<string, string>(
      (await q`select s.id, ss.text from span s join span_summary ss on ss.span = s.id and ss.input_fp = s.input_fp
               where s.id = any(${q.array(leaves, "text")})
                 and not exists (select 1 from unnest(s.canons) c join tomb t on t.key = 'canon:' || c)`).map((x: any) => [x.id, x.text]),
    );
    const planned = new Map((await plan(q, model, leaves)).map((n) => [`${n.start}:${n.size}`, n]));
    // Read-side validity (as search.ts): the source's projection is current, and the node id is planned from
    // the leaves' current input and summary hashes, so a matching node was made from exactly these leaves;
    // it is readable while none of them is forgotten. Its stored vector (with the model destination) is for
    // publication, not for reading.
    const rows = await q`select id, text, spans from forest_node where id = any(${q.array([...planned.values()].map((n) => n.id), "text")}) and text is not null`;
    const usable = new Map<string, string>();
    for (const x of rows) if ((x.spans as string[]).every((sp) => sums.has(sp))) usable.set(x.id, x.text);
    let complete = true;
    const items: ForestItem[] = [];
    const expand = (start: number, size: number, root: boolean) => {
      if (size === 1) {
        const text = sums.get(leaves[start]) ?? null;
        if (text === null || !root) complete = false;
        items.push({ start, size, span: leaves[start], text });
        return;
      }
      const n = planned.get(`${start}:${size}`);
      const text = n && usable.get(n.id);
      if (n && text) return void items.push({ start, size, node: n.id, text });
      complete = false;
      expand(start, size / 2, false);
      expand(start + size / 2, size / 2, false);
    };
    for (const [s, z] of roots(leaves.length)) expand(s, z, true);
    return { stale: false, complete, items };
  });
  if (r && !r.complete)
    await db.begin(async (tx: Tx) => {
      await tx`insert into forest_access values (${source}) on conflict (source) do update set at = now()`;
      await enqueue(tx, "forest", source);
    });
  return r;
}
