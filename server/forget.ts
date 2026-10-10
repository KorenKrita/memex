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
import "./forest.ts";
import "./snapshot.ts";
import "./l3.ts";

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
    // Forest nodes made from it: over any of its spans, or with it anywhere in their generation vector
    // (a span's <context>, a fork child's inherited records, spans long since re-cut away). A node not yet
    // written has no text; its structure over the source's spans goes too.
    await tx`delete from forest_node where spans && ${tx.array(spans, "text")}::text[] or deps ? ${"s:" + source}`;
    // Likewise a descendant's span summary made with it in the vector (its <context>, inherited records):
    // the span stays, its summary is made again from what is left.
    // Queued again: it may be made lawfully from what remains (another view closes it, other context).
    for (const r of await tx`delete from span_summary where deps ? ${"s:" + source} returning span`) await enqueue(tx, "summarize", r.span);
    await tx`delete from forest_access where source = ${source}`;
    // L3: its statements (evidence of this source), their relations, and its extraction marks.
    // Also statements of other sessions read with it (its records in their <context> or inherited prefix): they
    // go, and their spans are read again from what is left.
    const l3 = await tx`delete from l3_stmt where src = ${source} or deps ? ${"s:" + source} returning id, span`;
    const l3ids = l3.map((r: any) => r.id as string);
    if (l3ids.length) await tx`delete from l3_rel where a = any(${tx.array(l3ids, "text")}) or b = any(${tx.array(l3ids, "text")})`;
    const l3spans = [...new Set<string>(l3.map((r: any) => r.span as string))];
    await tx`delete from l3_seen where src = ${source} or span = any(${tx.array(l3spans, "text")})`;
    for (const sp of l3spans) await enqueue(tx, "l3", sp);
    await tx`delete from session_card where source = ${source} or deps ? ${"s:" + source}`;
    // Start snapshots: the source's own, and every block made from it in anyone else's (text and all).
    await tx`delete from start_snapshot where source = ${source}`;
    await tx`delete from snapshot_refresh where source = ${source}`;
    // Frozen recall: the source's own decisions and state; in other sessions, blocks made from it lose their
    // text (their vectors keep s:<source>, so the bump below moves each holder's generation).
    await tx`delete from recall_block where source = ${source}`;
    await tx`delete from recall_state where source = ${source}`;
    await tx`update recall_block set text = '', state = 'withdrawn' where deps ? ${"s:" + source}`;
    await tx`update start_snapshot set blocks = (select coalesce(jsonb_agg(x), '[]') from jsonb_array_elements(blocks) x where not (x->'deps' ? ${"s:" + source}))
             where blocks @? ${'$[*].deps."s:' + source + '"'}::jsonpath`;
    for (const t of ["lexical", "view", "projected", "span", "search_trace", "collector_view", "scan_progress", "projection", "record", "body"])
      await tx`delete from ${tx(t)} where source = ${source}`;
    // Lease rows are not touched: a commit holds its lease row before it waits for the barrier, so taking
    // them here (after the barrier) would deadlock. Its commit finds the source tomb under the barrier.
    await tx`update source set meta = '{}' where id = ${source}`;
    await bump(tx, "s:" + source);
    return { records: ids.length, descendants: desc.length, copies };
  });
}
