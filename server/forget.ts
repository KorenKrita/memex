// Session-granularity forget (DESIGN §7). One transaction under the exclusive barrier:
//   1. tombs: `source:S` and `canon:S:rec` for every record S ever held, so sync refuses them again;
//   2. fork descendants (parent_id chain): their copies of S's records (same record id and content, the
//      dry-run's match rule) are tombed under the descendant's own canonical id and removed from its L0;
//      the descendant's version moves and it is projected again (a forgotten parent counts as no parent,
//      so its own records stay visible and nothing copied from S can come back as its own);
//   3. everything stored for S: L0, projection, spans (summaries and vectors cascade), lexical rows, views,
//      search traces, skipped work and queued jobs; its version moves, so in-flight model calls and
//      responses built from it are refused at their barrier check (§5.5).
// The host's own log files and existing backups are not touched.
import { type Db, type Tx, bump, enqueue } from "./db.ts";
import { BARRIER } from "./sync.ts";
import { contentHash } from "../dryrun/dryrun.ts";
import "./project.ts";
import "./derive.ts";
import "./search.ts";

export async function forgetSource(db: Db, source: string): Promise<{ records: number; descendants: number; copies: number }> {
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await tx`insert into tomb values (${"source:" + source}) on conflict do nothing`;
    const recs = await tx`select distinct p.rec, b.raw from projection p join body b on b.source = p.source and b.rec = p.rec and b.hash = p.hash
                          where p.source = ${source}
                          union select r.rec, b.raw from record r left join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                          where r.source = ${source}`;
    const ids = [...new Set<string>(recs.map((r: any) => r.rec))];
    if (ids.length) await tx`insert into tomb select 'canon:' || ${source} || ':' || x from unnest(${tx.array(ids, "text")}::text[]) x on conflict do nothing`;
    // Copies in fork descendants, matched by record id + content.
    const match = new Set<string>();
    for (const r of recs) if (r.raw) match.add(`${r.rec}\u0000${contentHash(JSON.parse(r.raw))}`);
    const desc = await tx`with recursive d(id) as (select id from source where parent_id = ${source}
                            union select s.id from source s join d on s.parent_id = d.id) select id from d`;
    let copies = 0;
    for (const { id } of desc) {
      const rows = await tx`select distinct p.rec, b.raw from projection p join body b on b.source = p.source and b.rec = p.rec and b.hash = p.hash
                            where p.source = ${id}`;
      const hit = [...new Set<string>(rows.filter((r: any) => r.rec !== "header" && match.has(`${r.rec}\u0000${contentHash(JSON.parse(r.raw))}`)).map((r: any) => r.rec))];
      if (hit.length) {
        copies += hit.length;
        await tx`insert into tomb select 'canon:' || ${id} || ':' || x from unnest(${tx.array(hit, "text")}::text[]) x on conflict do nothing`;
        for (const t of ["projection", "record", "body"]) await tx`delete from ${tx(t)} where source = ${id} and rec = any(${tx.array(hit, "text")})`;
      }
      await bump(tx, "s:" + id);
      await enqueue(tx, "project", id);
    }
    // Everything stored for the source itself.
    const spans = (await tx`select id from span where source = ${source}`).map((r: any) => r.id as string);
    await tx`delete from job where state in ('queued', 'failed') and ((kind = 'project' and key = ${source}) or key = any(${tx.array(spans, "text")}))`;
    await tx`delete from derive_skip where span = any(${tx.array(spans, "text")})`;
    for (const t of ["lexical", "view", "projected", "span", "search_trace", "collector_view", "scan_progress", "projection", "record", "body"])
      await tx`delete from ${tx(t)} where source = ${source}`;
    await tx`update lease set done = true where source = ${source}`;
    await tx`update source set meta = '{}' where id = ${source}`;
    await bump(tx, "s:" + source);
    return { records: ids.length, descendants: desc.length, copies };
  });
}
