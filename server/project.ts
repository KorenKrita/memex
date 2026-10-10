// Deterministic projection (DESIGN §4.3, §5.5): L0 of one source → navigation views, spans, lexical index.
// Runs the dry-run's parsing and partition rules (dryrun/dryrun.ts) on the merged records, so the server
// splits exactly as the measured reference does. No model calls here: summarize jobs are queued for new
// closed spans, and the lexical index is usable as soon as this job publishes.
//
// Rules on top of the dry-run:
//  - Only state=ok records are read. A conflict record becomes a placeholder line with its agreed tree
//    position (Pi parentId) and no text, so it is neither evidence nor lost structure; if the versions
//    disagree on position, or the header is in conflict, the source is blocked (nothing derived).
//  - Fork parent: source.parent_id (sent by the client only when the parent is in the same scope). A parent
//    not yet projected leaves the copied prefix quarantined (the dry-run's "parent-not-given"); its own
//    projection re-queues the child. A parent in another scope, forgotten or blocked counts as invalid.
//  - Span stability: spans are closed once a later user message follows them on their view, or when the
//    source has been idle for IDLE_CLOSE_MS. Closed spans feed the next run as the prior partition and are
//    never re-cut; the open tail is re-cut on each run (its summaries are only queued once closed).
//  - Retrieval unit (§4.3): a prefix span P cut from a span S that some view still uses is an additional
//    representation of S; anything else is its own unit.
import { createHash } from "node:crypto";
import { type Db, type Job, type Tx, bump, enqueue, finishJob } from "./db.ts";
import { BARRIER } from "./sync.ts";
import { parentInfo, projectLines, type ParentInfo, type PriorView, type ProjectedView, type SpanRecord } from "../dryrun/dryrun.ts";
import { addMigration } from "./db.ts";
import { sourceIdOf, type Harness } from "../client/records.ts";

export const RULE_VERSION = "span-v1"; // part of every span id: a rule change makes new spans, never reuses old ones
const idleCloseMs = () => Number(process.env.MEMEX_IDLE_CLOSE_MS ?? 10 * 60_000);
const LEXICAL_CHUNK = 2048;

addMigration(`
create table if not exists span (id text primary key, source text not null, prefix bool not null, cut text, unit text not null,
                                 keys text[] not null, records jsonb not null, context jsonb not null,
                                 created_at timestamptz not null default now());
-- Fingerprint of everything a summary of this span reads (record and context keys carry content hashes).
alter table span add column if not exists input_fp text not null default '';
create index if not exists span_source on span (source);
create index if not exists span_unit on span (unit);
-- One row per navigation view of a source, in creation order; closed_end = record offset up to which the
-- partition is final.
create table if not exists view (source text not null, ord int not null, tip text not null, keys text[] not null, recs jsonb not null,
                                 spans jsonb not null, span_ids text[] not null, closed_end int not null, primary key (source, ord));
create index if not exists view_spans on view using gin (span_ids);
-- Inherited-key map of a projected source (child forks look up canonical keys here), and its block state.
-- keys null: the source is projected but cannot be a fork parent (its own ancestry is unresolved).
create table if not exists projected (source text primary key, status text not null, keys jsonb, res jsonb not null,
                                      at timestamptz not null default now());
create table if not exists lexical (source text not null, key text not null, part int not null, rec text not null, kind text not null,
                                    text text not null, primary key (source, key, part));
create index if not exists lexical_trgm on lexical using gin (text gin_trgm_ops);
-- The version vector this projection was built from (its own s:, and its fork parent's vector).
alter table projected add column if not exists deps jsonb not null default '{}';
-- Canonical ids (tomb key without 'canon:') of a lexical row's record and of a span's records, so search
-- can drop forgotten evidence before it cuts a channel; key indexes for excerpts and successor lookups.
alter table lexical add column if not exists canon text not null default '';
alter table span add column if not exists canons text[] not null default '{}';
-- Backfill after an upgrade: a projection written before these columns existed is projected again (its
-- rows stay out of search until then: projected.deps '{}' never counts as current).
insert into job (kind, key)
  select 'project', p.source from projected p
  where (p.deps = '{}'::jsonb or exists (select 1 from lexical l where l.source = p.source and l.canon = '')
         or exists (select 1 from span s where s.source = p.source and s.canons = '{}'))
    and not exists (select 1 from job j where j.kind = 'project' and j.key = p.source and j.state in ('queued', 'running'))
    and not exists (select 1 from tomb t where t.key = 'source:' || p.source);
create index if not exists lexical_key on lexical (key);
create index if not exists view_keys on view using gin (keys);
`);

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
export const spanIdOf = (recs: { key: string }[]) => "sp_" + sha(RULE_VERSION + "\u0000" + recs.map((r) => r.key).join("\u0000")).slice(0, 32);
/** A record key `native/rec:hash#j` → canonical id `sourceId:rec` (the tomb key without 'canon:'). */
export function canonOf(harness: string, key: string) {
  const base = key.slice(0, key.lastIndexOf("#"));
  const slash = base.indexOf("/");
  const rest = base.slice(slash + 1);
  return `${sourceIdOf(harness as Harness, base.slice(0, slash))}:${rest.slice(0, rest.lastIndexOf(":"))}`;
}
export const inputFpOf = (s: { records: SpanRecord[]; context: SpanRecord[] }) =>
  sha(JSON.stringify([RULE_VERSION, s.records.map((r) => [r.key, r.kind, r.digest]), s.context.map((r) => [r.key, r.kind, r.digest])]));

type Loaded = { lines: Record<string, any>[]; blocked?: string; idle: boolean; idleMs: number };

/** L0 of a source as JSONL objects in file order: ok records parsed, conflicts as placeholders. */
async function load(q: Tx, source: string, harness: string): Promise<Loaded> {
  const rows = await q`select r.rec, r.state, b.raw, extract(epoch from (now() - (select max(received_at) from body where source = ${source}))) * 1000 as idle_ms
                       from record r left join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                       where r.source = ${source} order by r.first_pos`;
  const lines: Record<string, any>[] = [];
  const idleMs = rows.length ? Number(rows[0].idle_ms) : Infinity;
  const idle = idleMs >= idleCloseMs();
  for (const r of rows) {
    if (r.state === "ok") {
      lines.push(JSON.parse(r.raw));
      continue;
    }
    // Conflict: keep its place in the tree if every version agrees on it, without any text.
    const versions = await q`select distinct b.raw from projection p join body b on b.source = p.source and b.rec = p.rec and b.hash = p.hash
                             where p.source = ${source} and p.rec = ${r.rec}`;
    const parsed = versions.map((v: any) => JSON.parse(v.raw));
    if (r.rec === "header" || (harness === "codex" && lines.length === 0)) return { lines: [], blocked: "header-conflict", idle, idleMs };
    if (harness === "pi") {
      const parents = new Set(parsed.map((p: any) => p.parentId ?? null));
      if (parents.size !== 1) return { lines: [], blocked: `tree-conflict:${r.rec}`, idle, idleMs };
      lines.push({ type: "memex_conflict", id: r.rec, parentId: [...parents][0], timestamp: parsed[0].timestamp });
    } else lines.push({ type: "memex_conflict", ordinal: parsed[0].ordinal });
  }
  return { lines, idle, idleMs };
}

type Result = { status: "ok" | "blocked" | "empty"; reason?: string; views: ProjectedView[]; spans: Map<string, Parameters<NonNullable<Parameters<typeof projectLines>[2]["onSpan"]>>[0]>; lexical: { key: string; rec: string; kind: string; text: string }[]; keys: [string, string][] | null };

/** Analyse one source (and, through `parent`, read its fork parent's stored projection). */
async function analyse(q: Tx, source: string): Promise<Result & { harness: string; scope: string; idle: boolean; idleMs: number; parentId: string | null }> {
  const [src] = await q`select harness, scope, parent_id from source where id = ${source}`;
  // A forgotten parent is skipped: forget removed its own records from this L0, and what it had inherited
  // keeps its origin through the nearest living ancestor (the copies match that ancestor's records).
  for (const seen = new Set([source]); src.parent_id && (await q`select 1 from tomb where key = ${"source:" + src.parent_id}`).length; ) {
    if (seen.has(src.parent_id)) {
      src.parent_id = null; // a cycle of declared parents: no usable ancestor
      break;
    }
    seen.add(src.parent_id);
    src.parent_id = (await q`select parent_id from source where id = ${src.parent_id}`)[0]?.parent_id ?? null;
  }
  const empty = { views: [], spans: new Map(), lexical: [], keys: null };
  const { lines, blocked, idle, idleMs } = await load(q, source, src.harness);
  const base = { harness: src.harness, scope: src.scope, idle, idleMs, parentId: src.parent_id };
  if (blocked) return { ...base, ...empty, status: "blocked", reason: blocked };
  if (!lines.length) return { ...base, ...empty, status: "empty" };
  // Fork parent (Pi): the header names it by file path, the server by source id.
  let parent: ParentInfo | "invalid" | undefined;
  if (src.harness === "pi" && lines[0].type === "session") {
    // Unknown or no living ancestor: a root session (records whose parent entry was forgotten are orphan roots).
    if (!src.parent_id) delete lines[0].parentSession;
    else {
      lines[0].parentSession = "/memex/source/" + src.parent_id;
      const [p] = await q`select s.scope, pr.status, pr.keys from source s left join projected pr on pr.source = s.id where s.id = ${src.parent_id}`;
      if (!p || !p.status) parent = undefined; // not projected yet: quarantined until it is
      else if (p.scope !== src.scope || p.status !== "ok" || !p.keys) parent = "invalid";
      else {
        const vs = await q`select keys, spans from view where source = ${src.parent_id} order by ord`;
        parent = parentInfo(p.keys, vs.map((v: any) => ({ keys: v.keys, spans: v.spans })), spanIdOf);
      }
    }
  }
  const prior: PriorView[] = (await q`select keys, spans, closed_end from view where source = ${source} order by ord`).map((v: any) => ({
    keys: v.keys.slice(0, v.closed_end),
    spans: v.spans.filter((s: any) => s.end <= v.closed_end),
  }));
  const spans: Result["spans"] = new Map();
  const lexical: Result["lexical"] = [];
  const p = projectLines("/memex/source/" + source, lines, {
    idOf: spanIdOf,
    prior: prior.filter((v) => v.keys.length),
    parent: () => parent,
    onSpan: (s) => spans.set(s.id, s),
    onLexical: (r) => lexical.push({ key: r.key, rec: r.id, kind: r.kind, text: r.text }),
  });
  if (p.res.status !== "indexed") return { ...base, ...empty, status: "blocked", reason: p.res.reason };
  return { ...base, status: "ok", views: p.views, spans, lexical, keys: p.parent ? [...p.parent.keys] : null };
}

const chunks = (text: string) => {
  const out: string[] = [];
  const t = text.replaceAll("\u0000", "");
  // Never between the two halves of a surrogate pair (a lone half is not text: it would be stored as U+FFFD).
  for (let i = 0, j; i < t.length; i = j) {
    j = Math.min(t.length, i + LEXICAL_CHUNK);
    if (j < t.length && /[\uD800-\uDBFF]/.test(t[j - 1])) j--;
    out.push(t.slice(i, j));
  }
  return out.length ? out : [""];
};

/** Offset up to which a view's partition is final: before its last user message, or all of it when idle. */
function closedEnd(v: ProjectedView, idle: boolean): number {
  if (idle) return v.recs.length;
  let lastUser = -1;
  v.recs.forEach((r, i) => r.kind === "user" && (lastUser = i));
  let end = 0;
  for (const s of v.spans) if (s.end <= Math.max(0, lastUser)) end = s.end;
  return end;
}

/** Job `project` for one source, in one read-committed transaction holding the barrier exclusively, then
 *  its job row (the same order as sync.commit). Every change to this source's inputs (sync commit, forget,
 *  the parent's projection) also takes the barrier, so the reads are consistent, and a change committed
 *  before shows up as req > claimed_req ("rerun"). A projection that withdraws anything already published
 *  as input (a span, a canonical mapping, a context) moves the source's version (§5.5 write-side rule), so
 *  model requests and results built from the old projection are refused at their barrier check. */
export async function runProject(db: Db, job: Job): Promise<"done" | "rerun" | "lost"> {
  const source = job.key;
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    const fin = await finishJob(tx, job);
    if (fin !== "done") return fin;
    if (!(await tx`select 1 from source where id = ${source}`).length) return fin;
    if ((await tx`select 1 from tomb where key = ${"source:" + source}`).length) return fin; // forgotten: nothing is derived again
    const a = await analyse(tx, source);
    let withdrawn = false;
    const [before] = await tx`select status, keys, deps from projected where source = ${source}`;
    await tx`insert into projected (source, status, keys, res) values (${source}, ${a.status}, ${a.keys}, ${{ reason: a.reason ?? null }})
             on conflict (source) do update set status = excluded.status, keys = excluded.keys, res = excluded.res, at = now()`;
    const wasClosed = new Set<string>(
      (await tx`select spans, closed_end from view where source = ${source}`).flatMap((v: any) => v.spans.filter((x: any) => x.id && x.end <= v.closed_end).map((x: any) => x.id)),
    );
    await tx`delete from view where source = ${source}`;
    if (a.status !== "ok") {
      await tx`delete from lexical where source = ${source}`;
    } else {
      const rows = a.views.map((v, ord) => {
        const ce = closedEnd(v, a.idle);
        return {
          source, ord, tip: v.recs.at(-1)?.id ?? "", keys: v.recs.map((r) => r.key), recs: v.recs, spans: v.spans,
          span_ids: v.spans.flatMap((s) => (s.id ? [s.id] : [])), closed_end: ce,
        };
      });
      for (const r of rows)
        await tx`insert into view (source, ord, tip, keys, recs, spans, span_ids, closed_end)
                 values (${r.source}, ${r.ord}, ${r.tip}, ${tx.array(r.keys, "text")}, ${r.recs}, ${r.spans}, ${tx.array(r.span_ids, "text")}, ${r.closed_end})`;
      // Spans: new ids only (ids are content-addressed). Unit: P is a representation of the S it was cut from.
      const used = new Set(rows.flatMap((r) => r.span_ids));
      const closed = new Set(rows.flatMap((r) => r.spans.filter((s) => s.id && s.end <= r.closed_end).map((s) => s.id!)));
      // Summaries are queued once a span is closed on some view (inherited parent spans are the parent's).
      const own = new Set<string>();
      const refresh = new Set<string>(); // closed spans whose summary input changed
      for (const [id, s] of a.spans) {
        // The unit is global: S may still be used by a view of another source (a fork parent).
        const cutUsed = !!s.cut && (used.has(s.cut) || (await tx`select 1 from view where span_ids @> array[${s.cut}]::text[] limit 1`).length > 0);
        const unit = s.prefix && cutUsed ? s.cut! : id;
        const keys = s.records.map((r: SpanRecord) => r.key);
        const fp = inputFpOf(s);
        const canons = keys.map((k: string) => canonOf(a.harness, k));
        const [old] = await tx`select input_fp, unit, canons from span where id = ${id}`;
        if (!old) {
          await tx`insert into span (id, source, prefix, cut, unit, keys, records, context, input_fp, canons)
                   values (${id}, ${source}, ${s.prefix}, ${s.cut ?? null}, ${unit}, ${tx.array(keys, "text")}, ${s.records}, ${s.context}, ${fp},
                           ${tx.array(canons, "text")})`;
          own.add(id);
        } else if (old.input_fp !== fp || old.unit !== unit || JSON.stringify(old.canons) !== JSON.stringify(canons)) {
          await tx`update span set records = ${s.records}, context = ${s.context}, input_fp = ${fp}, unit = ${unit},
                   canons = ${tx.array(canons, "text")} where id = ${id}`;
          if (old.input_fp !== fp) (withdrawn = true), refresh.add(id);
        }
      }
      // Spans kept from an earlier partition (not re-emitted) written before `canons` existed: fill them in.
      for (const r of await tx`select id, keys from span where source = ${source} and canons = '{}' and cardinality(keys) > 0`)
        await tx`update span set canons = ${tx.array((r.keys as string[]).map((k) => canonOf(a.harness, k)), "text")} where id = ${r.id}`;
      // An open tail closes by idling, with no new commit to wake us: look again when it would have.
      if (rows.some((r) => r.closed_end < r.keys.length)) await enqueue(tx, "project", source, {}, Math.max(0, idleCloseMs() - a.idleMs) + 1000);
      for (const id of closed)
        // Also a parent's span that closes here first (its tail goes on in this child): idempotent if the
        // parent already had it summarized.
        if (!wasClosed.has(id) || refresh.has(id)) {
          await enqueue(tx, "summarize", id);
          await enqueue(tx, "l3", id); // L3 reads the raw user records, not the summary: never waits for it
        }
      // Spans of this source no view uses any more and nothing else depends on: drop them.
      const gone = await tx`delete from span s where s.source = ${source} and not exists (select 1 from view v where v.span_ids @> array[s.id])
                            and not exists (select 1 from span u where u.unit = s.id and u.id <> s.id) returning id`;
      if (gone.length) withdrawn = true;
      // Lexical index: rebuild for this source (cheap relative to summaries; keeps forget/revision exact).
      await tx`delete from lexical where source = ${source}`;
      const lex = a.lexical.flatMap((r) => chunks(r.text).map((text, part) => ({ source, key: r.key, part, rec: r.rec, kind: r.kind, text, canon: canonOf(a.harness, r.key) })));
      for (let i = 0; i < lex.length; i += 500) await tx`insert into lexical ${tx(lex.slice(i, i + 500))}`;
    }
    // A changed canonical mapping or block state withdraws what was derived from the old one. The map only
    // growing (new records mapped, every earlier entry the same) changes nothing already published.
    const oldMap = new Map<string, string>((before?.keys ?? []) as [string, string][]);
    const newMap = new Map<string, string>((a.keys ?? []) as [string, string][]);
    const remapped = !!before && (before.status !== a.status || (before.keys === null) !== (a.keys === null)
      || [...oldMap].some(([k, v]) => newMap.get(k) !== v));
    const grown = !!before && newMap.size > oldMap.size;
    if (withdrawn || remapped) {
      await bump(tx, "s:" + source);
      // L3 statements whose vector holds this source: looked at again (renewed if their input is the same,
      // read again otherwise) — a version moved by a change elsewhere must not hide them for good.
      for (const r of await tx`select distinct span from l3_stmt where deps ? ${"s:" + source}`) await enqueue(tx, "l3", r.span);
    }
    // Read-side validity (search.ts): what this projection was built from. It holds while every entry is
    // current; any later commit, forget or parent change makes it stale until the next projection.
    const [sv] = await tx`select v from vis_version where key = ${"s:" + source}`;
    const deps: Record<string, number> = { ["s:" + source]: Number(sv?.v ?? 1) };
    if (a.parentId) Object.assign(deps, (await tx`select deps from projected where source = ${a.parentId}`)[0]?.deps ?? {});
    await tx`update projected set deps = ${deps} where source = ${source}`;
    const same = (x: any, y: any) => JSON.stringify(Object.entries(x ?? {}).sort()) === JSON.stringify(Object.entries(y ?? {}).sort());
    // Children that fork from this source re-project when its projection, its basis or its map changes
    // (new parent records can take over records a child had as its own: the child's remap moves its version).
    if (!before || remapped || grown || !same(before.deps, deps))
      for (const c of await tx`select id from source where parent_id = ${source}`) await enqueue(tx, "project", c.id);
    return fin;
  });
}
