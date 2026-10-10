// Projection (DESIGN §4.3, §5.5) over real Postgres: L0 commits → views, spans, lexical rows.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { claimJob, connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { runProject } from "./project.ts";
import { drain as drainAll, handlers } from "./worker.ts";
// L3 jobs are tested in l3.test.ts; here they would only add model calls.
const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const drain = (db: any, kinds = KINDS, max?: number) => drainAll(db, kinds, max);
import { sourceIdOf } from "../client/records.ts";

let db: Db;
beforeAll(async () => {
  db = connect();
  await migrate(db);
});
afterAll(async () => {
  await db.close();
});
beforeEach(async () => {
  stampOf.clear(); // a fixture's times are its own: an entry built in another test is not a copy here
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical restart identity cascade`);
  await db`insert into grant_rule values ('personal', 'sync', 'memex'), ('work', 'sync', 'memex')`;
  process.env.MEMEX_IDLE_CLOSE_MS = String(10 * 60_000);
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
// Fixture clock (DESIGN §4.1: a fork child's entry not newer than its header is a copy): the same entry built
// again gets the same time (a copy keeps the parent's); a fork header takes the next tick, so entries built
// after it are the child's own.
let tickN = 0;
const stampOf = new Map<string, string>();
const tick = () => new Date(Date.parse(T) + ++tickN * 1000).toISOString();
const stamp = (...k: unknown[]) => {
  const key = JSON.stringify([T, ...k]);
  if (!stampOf.has(key)) stampOf.set(key, tick());
  return stampOf.get(key)!;
};
const header = (id: string, parentSession?: string) =>
  JSON.stringify({ type: "session", version: 3, id, timestamp: parentSession ? stamp("fork", id, parentSession) : T, cwd: "/x", ...(parentSession ? { parentSession } : {}) });
const user = (id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: stamp("user", id, parent, text), message: { role: "user", content: [{ type: "text", text }] } });
const asst = (id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: stamp("assistant", id, parent, text), message: { role: "assistant", content: [{ type: "text", text }] } });

let commitN = 0;
/** Commit the whole file content as one collector's copy (positions = line order). */
async function put(native: string, lines: string[], o: { collector?: string; scope?: string; parent?: string | null } = {}) {
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope: o.scope ?? "personal", parent: o.parent ?? null };
  const l = await requestLease(db, decl, o.collector ?? "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  const changes = lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw }));
  const r = await commit(db, { leaseId: l, commitId: `c${commitN++}`, length: lines.length, changes });
  expect(r.ok).toBe(true);
  return decl.source;
}
const views = async (s: string) => (await db`select tip, span_ids, closed_end, keys from view where source = ${s} order by ord`) as any[];
const summarizeJobs = async () => (await db`select key from job where kind = 'summarize' order by id`).map((r: any) => r.key);

const big = (n: number) => "x".repeat(n);

test("a linear session: one view, spans per exchange, lexical rows; the open tail is not summarized", async () => {
  const s = await put("a", [header("a"), user("u1", null, "first question about 蛤蟆街"), asst("a1", "u1", "answer one"), user("u2", "a1", "second"), asst("a2", "u2", "answer two")]);
  await drain(db);
  const v = await views(s);
  expect(v.length).toBe(1);
  expect(v[0].tip).toBe("a2");
  expect(v[0].span_ids.length).toBe(2);
  expect(v[0].closed_end).toBe(2); // [u1 a1] closed by u2; [u2 a2] still open
  expect(await summarizeJobs()).toEqual([v[0].span_ids[0]]);
  const [lx] = await db`select rec from lexical where source = ${s} and text like ${"%蛤蟆街%"}`;
  expect(lx.rec).toBe("u1");
});

test("growth after an idle close appends a continuation; closed spans keep their ids", async () => {
  const lines = [header("b"), user("u1", null, "q"), asst("a1", "u1", "r1")];
  const s = await put("b", lines);
  process.env.MEMEX_IDLE_CLOSE_MS = "0";
  await drain(db);
  const [v1] = await views(s);
  expect(v1.span_ids.length).toBe(1);
  expect(v1.closed_end).toBe(2);
  expect(await summarizeJobs()).toEqual(v1.span_ids);
  await put("b", [...lines, asst("a2", "a1", "more of the same exchange")]);
  await drain(db);
  const [v2] = await views(s);
  expect(v2.span_ids[0]).toBe(v1.span_ids[0]); // not re-cut into [u1 a1 a2]
  expect(v2.span_ids.length).toBe(2);
  expect((await summarizeJobs()).length).toBe(2);
});

test("a fork inside a closed span: the old view keeps S, the new one gets P + its own span; P is a representation of S", async () => {
  const base = [header("c"), user("u1", null, "q " + big(100)), asst("a1", "u1", "r1"), asst("a2", "a1", "r2"), user("u3", "a2", "next")];
  const s = await put("c", base);
  await drain(db);
  const [old] = await views(s);
  const S = old.span_ids[0];
  await put("c", [...base, asst("b2", "a1", "other branch")]);
  await drain(db);
  const vs = await views(s);
  expect(vs.length).toBe(2);
  expect(vs[0].span_ids[0]).toBe(S);
  const P = vs[1].span_ids[0];
  expect(P).not.toBe(S);
  const [p] = await db`select prefix, cut, unit, keys from span where id = ${P}`;
  expect(p).toMatchObject({ prefix: true, cut: S, unit: S });
  expect(p.keys.length).toBe(2);
  const units = await db`select distinct unit from span where source = ${s}`;
  expect(units.length).toBe(3); // S, [u3], [b2]: forking added one unit (the new branch), not two
});

test("a record in conflict between two collectors: no text, tree kept, other records still projected", async () => {
  const lines = [header("d"), user("u1", null, "q"), asst("a1", "u1", "secret version A"), user("u2", "a1", "after")];
  const s = await put("d", lines, { collector: "m1" });
  await put("d", [lines[0], lines[1], asst("a1", "u1", "secret version B"), lines[3]], { collector: "m2" });
  await drain(db);
  const v = await views(s);
  expect(v.length).toBe(1);
  expect(v[0].tip).toBe("u2");
  const lx = await db`select text from lexical where source = ${s} and text like ${"%secret%"}`;
  expect(lx.length).toBe(0);
});

test("fork parent: before the parent exists the copies are quarantined (the child's own entry is not); once projected, inherited records stay the parent's", async () => {
  const P = sourceIdOf("pi", "par");
  const parentLines = [header("par"), user("u1", null, "parent question"), asst("a1", "u1", "parent answer")];
  const child = [header("kid", "/somewhere/par.jsonl"), user("u1", null, "parent question"), asst("a1", "u1", "parent answer"), user("k1", "a1", "child question")];
  const C = await put("kid", child, { parent: P });
  await drain(db);
  expect((await db`select rec from lexical where source = ${C}`).map((x: any) => x.rec)).toEqual(["k1"]); // copies quarantined, k1 its own
  await put("par", parentLines);
  await drain(db); // parent projects, re-queues the child
  const lx = (await db`select rec from lexical where source = ${C} order by rec`).map((r: any) => r.rec);
  expect(lx).toEqual(["k1"]); // only the child's own record is its evidence
  const [cv] = await views(C);
  const [pv] = await views(P);
  expect(cv.keys.slice(0, 2)).toEqual(pv.keys); // the child's view walks through the parent's records
});

test("fork parent in another scope is never linked: its copies stay out, the child's own entry is kept", async () => {
  const P = await put("par2", [header("par2"), user("u1", null, "work secret"), asst("a1", "u1", "w")], { scope: "work" });
  const C = await put("kid2", [header("kid2", "/x.jsonl"), user("u1", null, "work secret"), asst("a1", "u1", "w"), user("k1", "a1", "mine")], { parent: P });
  await drain(db);
  expect((await db`select rec, text from lexical where source = ${C}`).map((x: any) => x.rec)).toEqual(["k1"]);
  expect(JSON.stringify(await db`select text from lexical where source = ${C}`)).not.toContain("work secret");
});

test("a commit during a projection run is not lost: the run publishes nothing and the job runs again", async () => {
  const lines = [header("e"), user("u1", null, "q")];
  const s = await put("e", lines);
  const job = (await claimJob(db, ["project"]))!;
  await put("e", [...lines, asst("a1", "u1", "late answer")]); // arrives after the claim, before publishing
  expect(await runProject(db, job)).toBe("rerun");
  expect((await views(s)).length).toBe(0);
  await drain(db);
  expect((await views(s))[0].tip).toBe("a1");
});

test("appending exchanges does not move the session version; a revision of a closed span does", async () => {
  const lines = [header("ap"), user("u1", null, "q1"), asst("a1", "u1", "r1"), user("u2", "a1", "q2"), asst("a2", "u2", "r2")];
  const s = await put("ap", lines);
  await drain(db);
  const ver = async () => Number((await db`select v from vis_version where key = ${"s:" + s}`)[0]?.v ?? 1);
  const v0 = await ver();
  const spans0 = (await db`select id from span where source = ${s}`).map((r: any) => r.id).sort();
  // Two more exchanges: the open tail [u2 a2] closes, new spans appear; nothing published is withdrawn.
  await put("ap", [...lines, user("u3", "a2", "q3"), asst("a3", "u3", "r3"), user("u4", "a3", "q4"), asst("a4", "u4", "r4")]);
  await drain(db);
  expect(await ver()).toBe(v0);
  const spans1 = (await db`select id from span where source = ${s}`).map((r: any) => r.id);
  for (const id of spans0) expect(spans1).toContain(id); // earlier spans kept as they were
  // A revision inside the first closed span withdraws it.
  const rev = [...lines, user("u3", "a2", "q3"), asst("a3", "u3", "r3"), user("u4", "a3", "q4"), asst("a4", "u4", "r4")];
  rev[2] = asst("a1", "u1", "r1 revised");
  await put("ap", rev);
  await drain(db);
  expect(await ver()).toBeGreaterThan(v0);
});

test("a child's copies the parent has not synced yet: withdrawn until it catches up, then inherited", async () => {
  const P = sourceIdOf("pi", "rp");
  const pl = [header("rp"), user("x0", null, "q0"), asst("y0", "x0", "r0"), user("u1", "y0", "q1"), asst("a1", "u1", "r1")];
  await put("rp", pl.slice(0, 3)); // the parent has its first exchange only
  const C = await put("rk", [header("rk", "/x/rp.jsonl"), ...pl.slice(1), user("k1", "a1", "child")], { parent: P });
  await drain(db);
  const ver = async () => Number((await db`select v from vis_version where key = ${"s:" + C}`)[0]?.v ?? 1);
  const v0 = await ver();
  const own = (await db`select keys from projected where source = ${C}`)[0].keys as [string, string][];
  expect(own.find(([k]) => k.startsWith("u1:"))).toBeUndefined(); // a copy (not newer than the fork), never the child's own
  expect(own.find(([k]) => k.startsWith("k1:"))![1].startsWith("rk/")).toBe(true);
  await put("rp", pl);
  await drain(db);
  const now = (await db`select keys from projected where source = ${C}`)[0].keys as [string, string][];
  expect(now.find(([k]) => k.startsWith("u1:"))![1].startsWith("rp/")).toBe(true); // now the parent's
  expect(await ver()).toBeGreaterThan(v0);
});
