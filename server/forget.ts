// Session-granularity forget (DESIGN §7). One transaction: the exclusive barrier first, then rows (the
// same order as every writer: sync.commit takes the barrier before it touches its lease row).
//   1. tombs for what S itself contributed: `source:S`; `canon:S:rec` for every record S ever held; and for
//      every version of a record native to S (not one S inherited from its own fork parent), a content
//      tomb `copy:<rec>:<content hash>`. A fork copy keeps the parent's record id and content (the dry-run's
//      match rule), so the content tomb finds copies in any descendant, now or imported later, without
//      touching a descendant's own record that merely reuses an id, nor what S itself inherited from above;
//   2. fork descendants: rows matching a content tomb leave their L0; their version moves and they are
//      projected again (a forgotten parent counts as no parent; what remains is their own and what they
//      inherited from further up, which keeps its origin through the next living ancestor's projection);
//   3. everything stored for S: L0, spans (summaries and vectors cascade), lexical rows, views, traces,
//      skipped work and queued jobs; its version moves, so in-flight model calls and responses built from
//      it are refused at their barrier check (§5.5).
// The host's own log files and existing backups are not touched.
import { type Db, type Tx, bump, enqueue } from "./db.ts";
import { BARRIER } from "./sync.ts";
import { contentHash } from "../dryrun/dryrun.ts";
import "./project.ts";
import "./derive.ts";
import "./search.ts";

export const copyTomb = (rec: string, raw: string) => `copy:${rec}:${contentHash(JSON.parse(raw))}`;

export async function forgetSource(db: Db, source: string): Promise<{ records: number; descendants: number; copies: number }> {
  return db.begin(async (tx: Tx) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await tx`insert into tomb values (${"source:" + source}) on conflict do nothing`;
    // Every version S ever stored (body keeps each received version).
    const versions = await tx`select rec, raw from body where source = ${source}`;
    const ids = [...new Set<string>(versions.map((r: any) => r.rec))];
    if (ids.length) await tx`insert into tomb select 'canon:' || ${source} || ':' || x from unnest(${tx.array(ids, "text")}::text[]) x on conflict do nothing`;
    // Records S inherited: its projection maps them ("id:content hash") to an ancestor's key; S's own keys
    // start with its native id. A version the projection does not map (e.g. a superseded revision) counts
    // as S's own: that can only tomb more of S, never something of an ancestor S did not hold as a copy.
    const [pr] = await tx`select keys from projected where source = ${source}`;
    const [src] = await tx`select native_id from source where id = ${source}`;
    const inherited = new Set<string>(((pr?.keys ?? []) as [string, string][]).filter(([, k]) => !k.startsWith(src.native_id + "/")).map(([m]) => m));
    const own = versions.filter((r: any) => r.rec !== "header" && !inherited.has(`${r.rec}:${contentHash(JSON.parse(r.raw))}`));
    const tombs = [...new Set<string>(own.map((r: any) => copyTomb(r.rec, r.raw)))];
    if (tombs.length) await tx`insert into tomb select x from unnest(${tx.array(tombs, "text")}::text[]) x on conflict do nothing`;
    const match = new Set(tombs);
    // Fork descendants: drop their copies, move their version, project them again.
    const desc = await tx`with recursive d(id) as (select id from source where parent_id = ${source}
                            union select s.id from source s join d on s.parent_id = d.id) select id from d`;
    let copies = 0;
    for (const { id } of desc) {
      const rows = await tx`select rec, hash, raw from body where source = ${id} and rec <> 'header'`;
      const hit = rows.filter((r: any) => match.has(copyTomb(r.rec, r.raw)));
      if (hit.length) {
        copies += new Set(hit.map((r: any) => r.rec)).size;
        for (const r of hit) {
          await tx`delete from projection where source = ${id} and rec = ${r.rec} and hash = ${r.hash}`;
          await tx`delete from record where source = ${id} and rec = ${r.rec} and hash = ${r.hash}`;
          await tx`delete from body where source = ${id} and rec = ${r.rec} and hash = ${r.hash}`;
        }
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
    // Lease rows are not touched: a commit holds its lease row before it waits for the barrier, so taking
    // them here (after the barrier) would deadlock. Its commit finds the source tomb under the barrier.
    await tx`update source set meta = '{}' where id = ${source}`;
    await bump(tx, "s:" + source);
    return { records: ids.length, descendants: desc.length, copies };
  });
}
