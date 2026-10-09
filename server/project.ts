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
import { type Db, type Job, type Tx, enqueue, finishJob } from "./db.ts";
import { parentInfo, projectLines, type ParentInfo, type PriorView, type ProjectedView, type SpanRecord } from "../dryrun/dryrun.ts";
import { addMigration } from "./db.ts";

export const RULE_VERSION = "span-v1"; // part of every span id: a rule change makes new spans, never reuses old ones
const idleCloseMs = () => Number(process.env.MEMEX_IDLE_CLOSE_MS ?? 10 * 60_000);
const LEXICAL_CHUNK = 2048;

addMigration(`
create table if not exists span (id text primary key, source text not null, prefix bool not null, cut text, unit text not null,
                                 keys text[] not null, records jsonb not null, context jsonb not null,
                                 created_at timestamptz not null default now());
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
`);

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
export const spanIdOf = (recs: { key: string }[]) => "sp_" + sha(RULE_VERSION + "\u0000" + recs.map((r) => r.key).join("\u0000")).slice(0, 32);

type Loaded = { lines: Record<string, any>[]; blocked?: string; idle: boolean };

/** L0 of a source as JSONL objects in file order: ok records parsed, conflicts as placeholders. */
async function load(q: Tx, source: string, harness: string): Promise<Loaded> {
  const rows = await q`select r.rec, r.state, b.raw, extract(epoch from (now() - (select max(received_at) from body where source = ${source}))) * 1000 as idle_ms
                       from record r left join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                       where r.source = ${source} order by r.first_pos`;
  const lines: Record<string, any>[] = [];
  let idle = true;
  for (const r of rows) {
    idle = Number(r.idle_ms) >= idleCloseMs();
    if (r.state === "ok") {
      lines.push(JSON.parse(r.raw));
      continue;
    }
    // Conflict: keep its place in the tree if every version agrees on it, without any text.
    const versions = await q`select distinct b.raw from projection p join body b on b.source = p.source and b.rec = p.rec and b.hash = p.hash
                             where p.source = ${source} and p.rec = ${r.rec}`;
    const parsed = versions.map((v: any) => JSON.parse(v.raw));
    if (r.rec === "header" || (harness === "codex" && lines.length === 0)) return { lines: [], blocked: "header-conflict", idle };
    if (harness === "pi") {
      const parents = new Set(parsed.map((p: any) => p.parentId ?? null));
      if (parents.size !== 1) return { lines: [], blocked: `tree-conflict:${r.rec}`, idle };
      lines.push({ type: "memex_conflict", id: r.rec, parentId: [...parents][0], timestamp: parsed[0].timestamp });
    } else lines.push({ type: "memex_conflict", ordinal: parsed[0].ordinal });
  }
  return { lines, idle };
}

type Result = { status: "ok" | "blocked" | "empty"; reason?: string; views: ProjectedView[]; spans: Map<string, Parameters<NonNullable<Parameters<typeof projectLines>[2]["onSpan"]>>[0]>; lexical: { key: string; rec: string; kind: string; text: string }[]; keys: [string, string][] | null };

/** Analyse one source (and, through `parent`, read its fork parent's stored projection). */
async function analyse(q: Tx, source: string): Promise<Result & { harness: string; scope: string; idle: boolean; parentId: string | null }> {
  const [src] = await q`select harness, scope, parent_id from source where id = ${source}`;
  const empty = { views: [], spans: new Map(), lexical: [], keys: null };
  const { lines, blocked, idle } = await load(q, source, src.harness);
  const base = { harness: src.harness, scope: src.scope, idle, parentId: src.parent_id };
  if (blocked) return { ...base, ...empty, status: "blocked", reason: blocked };
  if (!lines.length) return { ...base, ...empty, status: "empty" };
  // Fork parent (Pi): the header names it by file path, the server by source id.
  let parent: ParentInfo | "invalid" | undefined;
  if (src.harness === "pi" && lines[0].type === "session") {
    if (!src.parent_id) delete lines[0].parentSession; // unknown or other-scope parent: a root session
    else {
      lines[0].parentSession = "/memex/source/" + src.parent_id;
      const [p] = await q`select s.scope, pr.status, pr.keys, (select count(*) from tomb where key = ${"source:" + src.parent_id})::int forgotten
                          from source s left join projected pr on pr.source = s.id where s.id = ${src.parent_id}`;
      if (!p || !p.status) parent = undefined; // not projected yet: quarantined until it is
      else if (p.scope !== src.scope || p.forgotten || p.status !== "ok" || !p.keys) parent = "invalid";
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
  for (let i = 0; i < t.length; i += LEXICAL_CHUNK) out.push(t.slice(i, i + LEXICAL_CHUNK));
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

/** Job `project` for one source, in one read-committed transaction that first locks its job row. Every
 *  change to this source's inputs (sync commit, forget, the parent's projection) enqueues this job in its
 *  own transaction, so it waits on that row lock and cannot commit while this run reads: the reads are
 *  consistent, and a change committed before the lock shows up as req > claimed_req ("rerun"). */
export async function runProject(db: Db, job: Job): Promise<"done" | "rerun" | "lost"> {
  const source = job.key;
  return db.begin(async (tx: Tx) => {
    const fin = await finishJob(tx, job);
    if (fin !== "done") return fin;
    if (!(await tx`select 1 from source where id = ${source}`).length) return fin;
    const a = await analyse(tx, source);
    const [before] = await tx`select status, keys from projected where source = ${source}`;
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
      for (const [id, s] of a.spans) {
        const unit = s.prefix && s.cut && used.has(s.cut) ? s.cut : id;
        const keys = s.records.map((r: SpanRecord) => r.key);
        const ins = await tx`insert into span (id, source, prefix, cut, unit, keys, records, context)
                             values (${id}, ${source}, ${s.prefix}, ${s.cut ?? null}, ${unit}, ${tx.array(keys, "text")}, ${s.records}, ${s.context})
                             on conflict (id) do nothing returning id`;
        if (ins.length) own.add(id);
      }
      for (const id of closed) if (!wasClosed.has(id) && (own.has(id) || (await tx`select 1 from span where id = ${id} and source = ${source}`).length)) await enqueue(tx, "summarize", id);
      // Spans of this source no view uses any more and nothing else depends on: drop them.
      await tx`delete from span s where s.source = ${source} and not exists (select 1 from view v where v.span_ids @> array[s.id])
               and not exists (select 1 from span u where u.unit = s.id and u.id <> s.id)`;
      // Lexical index: rebuild for this source (cheap relative to summaries; keeps forget/revision exact).
      await tx`delete from lexical where source = ${source}`;
      const lex = a.lexical.flatMap((r) => chunks(r.text).map((text, part) => ({ source, key: r.key, part, rec: r.rec, kind: r.kind, text })));
      for (let i = 0; i < lex.length; i += 500) await tx`insert into lexical ${tx(lex.slice(i, i + 500))}`;
    }
    // Children that fork from this source re-project when its projection changes.
    if (!before || before.status !== a.status || JSON.stringify(before.keys) !== JSON.stringify(a.keys))
      for (const c of await tx`select id from source where parent_id = ${source}`) await enqueue(tx, "project", c.id);
    return fin;
  });
}
