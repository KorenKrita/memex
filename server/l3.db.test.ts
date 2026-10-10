// L3 extraction and pair judgment over real Postgres and a fake model endpoint (DESIGN §4.5).
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { hooks, revokeGrant } from "./derive.ts";
import { forgetSource } from "./forget.ts";
import { checkExtract, l3Hooks, l3View } from "./l3.ts";
import { drain, runOne } from "./worker.ts";
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let requests: { sys: string; content: string }[] = [];
// The fake extractor: every citable record of the user whose text has PREF becomes a statement.
type Rec = { id: string; kind: string; citable: boolean; text: string };
let extract = (recs: Rec[]): any[] =>
  recs
    .filter((r) => r.citable && r.kind === "user" && r.text.includes("PREF"))
    .map((r) => ({ record_id: r.id, text: r.text, scope: r.text.includes("REPO") ? "repo" : "global", retraction: r.text.includes("RETRACT"), effective_from: null, effective_phrase: null }));
// The fake judge: "NEW" over the same TOPIC replaces; RETRACT retracts; otherwise unrelated.
let judge = (a: string, b: string) => {
  const topic = (t: string) => /TOPIC(\w+)/.exec(t)?.[1];
  if (!topic(a) || topic(a) !== topic(b)) return { same_object: false, relation: "unrelated" };
  if (a.includes("RETRACT") || b.includes("RETRACT")) return { same_object: true, relation: "retracts" };
  return { same_object: true, relation: "replaces" };
};
let srv: ReturnType<typeof Bun.serve>;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = await req.json();
      if (new URL(req.url).pathname.endsWith("/embeddings")) return Response.json({ data: [{ embedding: Array.from({ length: 1024 }, (_, i) => (i === 0 ? 1 : 0)) }] });
      const sys = body.messages[0].content as string;
      const content = body.messages[1].content as string;
      requests.push({ sys, content });
      const reply = (o: object) => Response.json({ choices: [{ message: { content: JSON.stringify(o) } }] });
      if (sys.startsWith("You extract")) {
        const recs = [...content.matchAll(/<record id="([^"]*)" kind="(\w+)"([^>]*)>\n([\s\S]*?)\n<\/record>/g)].map((m) => ({
          id: m[1], kind: m[2], citable: m[3].includes('citable="yes"'), text: m[4].replace(/^\w+: /, ""),
        }));
        return reply({ statements: extract(recs) });
      }
      if (sys.startsWith("You compare")) {
        const [a, b] = [...content.matchAll(/<statement[^>]*>\n([\s\S]*?)\n<\/statement>/g)].map((m) => m[1]);
        return reply(judge(a, b));
      }
      return reply({ summary: "S", user_asked: [], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] });
    },
  });
  Object.assign(process.env, {
    MEMEX_LLM_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k",
    MEMEX_EMBED_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
  });
});
afterAll(async () => {
  srv.stop(true);
  await db.close();
});
const DEFAULT_EXTRACT = extract, DEFAULT_JUDGE = judge;
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, forest_node, forest_access,
                   session_card, start_snapshot, snapshot_refresh, l3_stmt, l3_seen, l3_rel restart identity cascade`);
  for (const [s, p, d] of [["personal", "sync", "memex"], ["personal", "derive", "llm"], ["personal", "derive", "embedding"], ["personal", "snapshot", "memex"]])
    await db`insert into grant_rule values (${s}, ${p}, ${d})`;
  process.env.MEMEX_IDLE_CLOSE_MS = "0";
  requests = [];
  extract = DEFAULT_EXTRACT;
  judge = DEFAULT_JUDGE;
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const header = (id: string, parentSession?: string) =>
  JSON.stringify({ type: "session", version: 3, id, timestamp: "2026-01-01T00:00:00Z", cwd: "/w/" + id, ...(parentSession ? { parentSession } : {}) });
const msg = (role: string, id: string, parent: string | null, text: string, ts: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: ts, message: { role, content: [{ type: "text", text }] } });
let n = 0;
/** A session of (user, assistant) exchanges; `turns[i]` = [user text, assistant text, timestamp]. */
async function put(native: string, turns: [string, string, string][], o: { repo?: string | null; parent?: string | null; parentNative?: string; prefix?: string[] } = {}) {
  const lines = [header(native, o.parentNative ? `/x/${o.parentNative}.jsonl` : undefined), ...(o.prefix ?? [])];
  let prev: string | null = o.prefix?.length ? JSON.parse(o.prefix[o.prefix.length - 1]).id : null;
  turns.forEach(([u, a, ts], i) => {
    lines.push(msg("user", `${native}u${i}`, prev, u, ts), msg("assistant", `${native}a${i}`, `${native}u${i}`, a, ts));
    prev = `${native}a${i}`;
  });
  const repo = o.repo === undefined ? "repo-a" : o.repo;
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope: "personal", parent: o.parent ?? null, meta: repo ? { repo } : {} };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  expect((await commit(db, { leaseId: l, commitId: `n${n++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) })).ok).toBe(true);
  return { source: decl.source, lines };
}
const view = async (t = Date.now(), purpose = "snapshot", dest = "memex") =>
  (await db.begin("isolation level repeatable read read only", (q: any) => l3View(q, t, purpose, dest))) as { view: any[]; next: number | null };
const texts = async (t?: number) => (await view(t)).view.map((s) => s.text).sort();
const T0 = "2026-01-01T01:00:00Z", T1 = "2026-01-02T01:00:00Z", T2 = "2026-01-03T01:00:00Z";

test("only the user's own words become statements; assistant text is never promoted; stated_at is the record's time", async () => {
  await put("s1", [["PREF TOPICpm use pnpm", "PREF TOPICpm you should use yarn", T0]]);
  await drain(db);
  const v = await view();
  expect(v.view.map((s) => s.text)).toEqual(["PREF TOPICpm use pnpm"]);
  expect(+new Date(v.view[0].stated_at)).toBe(Date.parse(T0));
  expect(v.view[0].scope).toBe("global");
  // The assistant's record is shown but not citable.
  const ex = requests.find((r) => r.sys.startsWith("You extract"))!;
  expect(ex.content).toMatch(/kind="assistant">/);
  expect(ex.content).not.toMatch(/kind="assistant"[^>]*citable/);
});

test("an answer citing a record the user did not write is refused: nothing is stored", async () => {
  extract = (recs) => recs.filter((r) => r.kind === "assistant").map((r) => ({ record_id: r.id, text: r.text, scope: "global", retraction: false, effective_from: null, effective_phrase: null }));
  await put("s1", [["hello", "PREF TOPICpm use yarn", T0]]);
  await drain(db);
  expect(await texts()).toEqual([]);
  expect(requests.filter((r) => r.sys.startsWith("You extract")).length).toBe(3); // asked to correct, then gave up
  const [j] = await db`select state, error from job where kind = 'l3'`; // retried later with backoff, as any failed job
  expect(j.state).toBe("queued");
  expect(j.error).toBe("extraction invalid after 3 tries");
});

test("a repo statement needs a repo identity: without one it is dropped, never made global", async () => {
  await put("s1", [["PREF REPO TOPICfmt use tabs here", "ok", T0]], { repo: null });
  await put("s2", [["PREF REPO TOPICfmt use spaces here", "ok", T0]], { repo: "repo-b" });
  await drain(db);
  const v = (await view()).view;
  expect(v.map((s) => [s.text, s.scope])).toEqual([["PREF REPO TOPICfmt use spaces here", "repo:repo-b"]]);
});

test("a fork copy of the user's record makes no second statement", async () => {
  const p = await put("par", [["PREF TOPICpm use pnpm", "ok", T0]]);
  await drain(db);
  await put("kid", [["child question", "ok", T1]], { parent: p.source, parentNative: "par", prefix: p.lines.slice(1) });
  await drain(db);
  expect((await db`select count(*)::int n from l3_stmt`)[0].n).toBe(1);
  expect(await texts()).toEqual(["PREF TOPICpm use pnpm"]);
});

test("every pair in a scope is judged; a later replacement wins; any import order gives the same view", async () => {
  const sessions: [string, [string, string, string][]][] = [
    ["a", [["PREF TOPICpm use npm", "ok", T0]]],
    ["b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]],
    ["c", [["PREF TOPICed use vim", "ok", T2]]],
  ];
  const results: string[][] = [];
  for (const order of [[0, 1, 2], [2, 1, 0], [1, 2, 0]]) {
    await db.unsafe(`truncate source, vis_version, tomb, lease, commit_result, body, projection, record, scan_progress, collector_view, job, span, view,
                     projected, lexical, span_summary, derive_skip, forest_node, forest_access, session_card, l3_stmt, l3_seen, l3_rel restart identity cascade`);
    for (const i of order) {
      await put(...sessions[i]);
      await drain(db);
    }
    expect((await db`select count(*)::int n from l3_rel`)[0].n).toBe(3); // 3 statements → 3 pairs, unrelated ones too
    results.push(await texts());
  }
  expect(results[0]).toEqual(["PREF TOPICed use vim", "PREF TOPICpm NEW use pnpm"]);
  expect(results[1]).toEqual(results[0]);
  expect(results[2]).toEqual(results[0]);
  // Before B was said, A held.
  expect(await texts(Date.parse(T0) + 1000)).toEqual(["PREF TOPICpm use npm"]);
});

test("two statements published at once still get their pair", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db, ["project", "summarize", "embed"]); // spans closed; the two l3 jobs wait
  let release!: () => void;
  const gate = new Promise<void>((ok) => (release = ok));
  let first = true;
  l3Hooks.afterPlanRead = async () => {
    if (!first) return;
    first = false;
    // The first publisher has read its scope; the second one runs now and would read before the first commits.
    const other = runOne(db, ["l3"]);
    await Bun.sleep(300);
    release();
    void other;
  };
  try {
    const one = runOne(db, ["l3"]);
    await gate;
    await one;
    await drain(db, ["l3"]);
  } finally {
    l3Hooks.afterPlanRead = async () => {};
  }
  await drain(db);
  expect((await db`select count(*)::int n from l3_rel`)[0].n).toBe(1);
  expect(await texts()).toEqual(["PREF TOPICpm NEW use pnpm"]);
});

test("a revoke between the check and the send: nothing is sent, nothing stored", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db, ["project", "summarize", "embed"]);
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    await revokeGrant(db, "personal", "derive", "llm");
  };
  await drain(db, ["l3"]);
  expect(requests.filter((r) => r.sys.startsWith("You extract")).length).toBe(0);
  expect(await texts()).toEqual([]);
});

test("forget takes the statements, their relations and extraction marks; the older statement shows again", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  const b = await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db);
  expect(await texts()).toEqual(["PREF TOPICpm NEW use pnpm"]);
  await forgetSource(db, b.source);
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
  expect((await db`select count(*)::int n from l3_rel`)[0].n).toBe(0);
  expect((await db`select count(*)::int n from l3_stmt where src = ${b.source}`)[0].n).toBe(0); // gone from storage, not only hidden
  expect((await db`select count(*)::int n from l3_seen where src = ${b.source}`)[0].n).toBe(0);
});

test("a revised record's statement is no longer readable, and does not suppress", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db);
  await put("b", [["never mind", "ok", T1]]); // the same record id, other content
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
});

test("a read scope not granted hides its statements", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db);
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
  await db`delete from grant_rule where purpose = 'snapshot'`;
  expect(await texts()).toEqual([]);
});

test("effective_from needs a phrase copied from the record; then it is in effect from that time on", async () => {
  extract = (recs) =>
    recs.filter((r) => r.citable && r.kind === "user" && r.text.includes("PREF")).map((r) => ({
      record_id: r.id, text: r.text, scope: "global", retraction: false,
      effective_from: "2026-01-10T00:00:00+08:00", effective_phrase: r.text.includes("from Jan 10") ? "from Jan 10" : "next week",
    }));
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]); // invented phrase: refused
  await put("b", [["PREF TOPICed from Jan 10 use helix", "ok", T1]]);
  await drain(db);
  const all = await db`select text, effective_from from l3_stmt`;
  expect(all.map((r: any) => r.text)).toEqual(["PREF TOPICed from Jan 10 use helix"]);
  expect(await texts(Date.parse("2026-01-05T00:00:00Z"))).toEqual([]);
  expect(await texts(Date.parse("2026-01-09T16:00:00Z"))).toEqual(["PREF TOPICed from Jan 10 use helix"]);
  expect((await view(Date.parse("2026-01-05T00:00:00Z"))).next).toBe(Date.parse("2026-01-09T16:00:00Z"));
});

test("a record already extracted is not citable again", () => {
  const c = { id: "u1", src: "S", rec: "u1", hash: "h", stated_at: 0, repo: null, auth: "personal", digest: "user: PREF x", citable: false };
  const r = checkExtract({ statements: [{ record_id: "u1", text: "PREF x", scope: "global", retraction: false, effective_from: null, effective_phrase: null }] }, new Map([["u1", c]]), "m");
  expect(r.problems.length).toBe(1);
  expect(r.stmts).toEqual([]);
});

test("two statements of one extraction in one scope get their pair", async () => {
  extract = (recs) =>
    recs.filter((r) => r.citable && r.kind === "user").flatMap((r) => ["PREF TOPICpm use npm", "PREF TOPICpm NEW use pnpm"].map((text) => ({
      record_id: r.id, text, scope: "global", retraction: false, effective_from: null, effective_phrase: null,
    })));
  await put("a", [["about package managers", "ok", T0]]);
  await drain(db);
  expect((await db`select kind from l3_rel`).map((r: any) => r.kind)).toEqual(["contradicts"]); // said at once: no order between them
  expect(await texts()).toEqual(["PREF TOPICpm NEW use pnpm", "PREF TOPICpm use npm"]);
});

test("a record claimed by another extraction meanwhile keeps the first set", async () => {
  const a = await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db, ["project", "summarize", "embed"]);
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    const [r] = await db`select rec, hash from record where source = ${a.source} and rec = 'au0'`;
    await db`insert into l3_seen values (${a.source}, ${r.rec}, ${r.hash})`;
  };
  await drain(db, ["l3"]);
  expect((await db`select count(*)::int n from l3_stmt`)[0].n).toBe(0);
});

test("a later retraction retracts even when the judge calls it a replacement: what the retracted one replaced is back", async () => {
  // The retraction is about B only (R–A unrelated); for R–B the judge names "replaces".
  judge = (a, b) => ((a + b).includes("RETRACT") && !(a + b).includes("NEW") ? { same_object: false, relation: "unrelated" } : { same_object: true, relation: "replaces" });
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await put("r", [["PREF RETRACT TOPICpm forget the pnpm thing", "ok", T2]]);
  await drain(db);
  const kinds = new Map((await db`select a, b, kind from l3_rel`).map((r: any) => [r.kind, r]));
  expect(kinds.has("retracts")).toBe(true);
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
});

test("not the same object: unrelated, whatever relation the judge names", async () => {
  judge = () => ({ same_object: false, relation: "replaces" });
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("b", [["PREF TOPICed use vim", "ok", T1]]);
  await drain(db);
  expect((await db`select kind from l3_rel`).map((r: any) => r.kind)).toEqual(["unrelated"]);
  expect(await texts()).toEqual(["PREF TOPICed use vim", "PREF TOPICpm use npm"]);
});
