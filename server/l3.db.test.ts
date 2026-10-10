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
/** Set by a test: the fake model replies only once it has settled (a change landing while a request is out). */
let before: Promise<unknown> | null = null;
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
      requests.push({ sys, content: body.messages.slice(1).map((m: any) => m.content).join("\n") });
      const reply = async (o: object) => {
        if (before) await before.finally(() => (before = null));
        return Response.json({ choices: [{ message: { content: JSON.stringify(o) } }] });
      };
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
  before = null;
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
  let calls = 0;
  let other: Promise<unknown> | null = null;
  l3Hooks.afterPlanRead = async () => {
    if (++calls > 1) return; // the second publisher planning (only possible without the scope lock)
    // The first publisher has read its scope and not committed. Run the second until it either waits for the
    // first's scope lock or has read its own scope (then it could miss the first's statement).
    other = runOne(db, ["l3"]);
    for (let i = 0; i < 400; i++) {
      const [w] = await db`select count(*)::int n from pg_locks where locktype = 'advisory' and not granted`;
      if (w.n > 0 || calls > 1) return;
      await Bun.sleep(5);
    }
    throw new Error("the second publisher neither waited nor planned");
  };
  try {
    await runOne(db, ["l3"]);
    expect(other).not.toBeNull();
    await other;
  } finally {
    l3Hooks.afterPlanRead = async () => {};
  }
  expect((await db`select count(*)::int n from l3_stmt`)[0].n).toBe(2);
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
    const [sp] = await db`select id, input_fp from span where source = ${a.source}`;
    await db`insert into l3_seen (src, rec, rec_hash, span, input_fp) values (${a.source}, ${r.rec}, ${r.hash}, ${sp.id}, ${sp.input_fp})`;
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

// ---------- user card in the start snapshot ----------
import { startSnapshot, BUDGET, FRAMING } from "./snapshot.ts";
import { USER_MAX } from "./l3.ts";
const snap = async (x: object = {}) => (await startSnapshot(db, { source: sourceIdOf("pi", "me"), scope: "personal", repo: "repo-a", ...x })).body as any;

test("the snapshot carries the user card after the framing: global statements in effect now, each with its source", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  const b = await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await put("c", [["PREF REPO TOPICfmt tabs here", "ok", T1]]);
  await drain(db);
  const k = requests.length;
  const s = await snap();
  expect(requests.length).toBe(k); // a read never calls a model
  expect(s.text.startsWith(FRAMING + "\n\n## About the user")).toBe(true);
  const [rec] = await db`select rec from l3_stmt where src = ${b.source}`;
  expect(s.text).toContain(`- PREF TOPICpm NEW use pnpm [record:${b.source}:${rec.rec}]`);
  expect(s.text).not.toContain("use npm"); // replaced
  expect(s.text).not.toContain("tabs here"); // a repo statement: not on the user card
});

test("no statement in effect: no user card block", async () => {
  await put("a", [["hello", "ok", T0]]);
  await drain(db);
  expect((await snap()).text).not.toContain("About the user");
});

test("frozen: a new statement does not change a stored snapshot; a refresh takes it in", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db);
  const s1 = await snap();
  await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db);
  expect((await snap()).text).toBe(s1.text);
  const r = await snap({ refresh_id: "r1" });
  expect(r.text).toContain("use pnpm");
  expect(r.text).not.toContain("use npm");
});

test("forgetting a statement's source drops the user card block as a whole", async () => {
  const a = await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("e", [["PREF TOPICed use vim", "ok", T1]]);
  await drain(db);
  expect((await snap()).text).toContain("use npm");
  await forgetSource(db, a.source);
  const s = await snap();
  expect(s.text).not.toContain("About the user");
  expect(JSON.stringify(await db`select blocks from start_snapshot`)).not.toContain("use npm");
});

test("forgetting the source of a statement that suppresses another drops the block too (what it hid would be stale)", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  const b = await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await put("e", [["PREF TOPICed use vim", "ok", T1]]);
  await drain(db);
  expect((await snap()).text).toContain("use pnpm");
  await forgetSource(db, b.source);
  expect((await snap()).text).not.toContain("About the user");
});

test("a statement whose scope is not readable for snapshots is not on the card; revoking it later drops the block", async () => {
  await db`insert into grant_rule values ('work', 'sync', 'memex'), ('work', 'derive', 'llm')`;
  const decl = { source: sourceIdOf("pi", "w"), harness: "pi" as const, nativeId: "w", scope: "work", parent: null, meta: { repo: "repo-a" } };
  const lines = [header("w"), msg("user", "wu0", null, "PREF TOPICsec work secret rule", T0), msg("assistant", "wa0", "wu0", "ok", T0)];
  const l = await requestLease(db, decl, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "w1", length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db);
  expect((await snap()).text).not.toContain("work secret");
  await db`insert into grant_rule values ('work', 'snapshot', 'memex')`;
  expect((await snap({ refresh_id: "r1" })).text).toContain("work secret");
  await db`delete from grant_rule where scope = 'work' and purpose = 'snapshot'`;
  expect((await snap()).text).not.toContain("work secret");
});

test("the user card stays within its budget, whole lines only, newest first", async () => {
  const turns: [string, string, string][] = Array.from({ length: 12 }, (_, i) => [`PREF TOPICt${i} ${"中".repeat(80)} item${i}`, "ok", `2026-01-0${1 + (i % 9)}T0${i % 10}:00:00Z`]);
  for (let i = 0; i < 12; i++) await put("u" + i, [turns[i]]);
  await drain(db);
  const s = await snap();
  const block = s.text.split("\n\n").find((x: string) => x.startsWith("## About the user"))!;
  expect(Buffer.byteLength(block)).toBeLessThanOrEqual(USER_MAX);
  expect(Buffer.from(block).toString()).toBe(block);
  const lines = block.split("\n").slice(1);
  expect(lines.length).toBeGreaterThan(1);
  expect(lines.length).toBeLessThan(12);
  expect(lines.every((l: string) => /^- PREF .* \[record:[0-9a-f]+:[^\]]+\]$/.test(l))).toBe(true);
  expect(s.bytes).toBeLessThanOrEqual(BUDGET);
});

test("forgetting a retraction (shown nowhere, yet hiding a statement) drops the user card block", async () => {
  await put("b", [["PREF TOPICpm use pnpm", "ok", T0]]);
  const r = await put("r", [["PREF RETRACT TOPICpm forget the pnpm thing", "ok", T1]]);
  await put("e", [["PREF TOPICed use vim", "ok", T1]]);
  await drain(db);
  const s1 = await snap();
  expect(s1.text).toContain("use vim");
  expect(s1.text).not.toContain("use pnpm");
  await forgetSource(db, r.source);
  expect((await snap()).text).not.toContain("About the user"); // its text would hide pnpm wrongly now
});

test("user card lines are newest first", async () => {
  await put("a", [["PREF TOPICx1 first", "ok", T0]]);
  await put("b", [["PREF TOPICx2 third", "ok", T2]]);
  await put("c", [["PREF TOPICx3 second", "ok", T1]]);
  await drain(db);
  const block = (await snap()).text.split("\n\n").find((x: string) => x.startsWith("## About the user"))!;
  expect(block.split("\n").slice(1).map((l: string) => l.split(" ")[3])).toEqual(["third", "second", "first"]);
});

test("a revised record's statement is not on a freshly built card", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db, ["project", "summarize", "embed", "l3"]);
  await put("a", [["something else", "ok", T0]]); // revised, not drained: no re-extraction yet
  expect((await snap()).text).not.toContain("use npm");
});

// ---------- generation dependencies: a statement is made from the whole input, not only its cited record ----------

test("the context a statement was read with is forgotten: the statement goes, though its own record is unchanged", async () => {
  extract = (recs) =>
    recs.filter((r) => r.citable && r.kind === "user" && r.text.includes("PREF")).map((r) => ({
      record_id: r.id, text: r.text + " ctx[" + recs.filter((x) => !x.citable && x.kind === "user").map((x) => x.text).join(",") + "]",
      scope: "global", retraction: false, effective_from: null, effective_phrase: null,
    }));
  const p = await put("par", [["convention is tabs", "ok", T0]]);
  await drain(db);
  await put("kid", [["PREF TOPICfmt follow that", "ok", T1]], { parent: p.source, parentNative: "par", prefix: p.lines.slice(1) });
  await drain(db);
  expect(await texts()).toEqual(["PREF TOPICfmt follow that ctx[convention is tabs]"]);
  await forgetSource(db, p.source);
  expect(await texts()).toEqual([]); // not before it is made again
  expect((await db`select count(*)::int n from l3_stmt where text like '%tabs%'`)[0].n).toBe(0); // gone from storage too
  await drain(db);
  expect((await texts()).some((t) => t.includes("tabs"))).toBe(false);
});

test("the context a statement was read with is revised: the old statement is unreadable and the record is read again", async () => {
  extract = (recs) =>
    recs.filter((r) => r.citable && r.kind === "user" && r.text.includes("PREF")).map((r) => ({
      record_id: r.id, text: r.text + " ctx[" + recs.filter((x) => x.id !== r.id && x.kind === "user").map((x) => x.text).join(",") + "]",
      scope: "global", retraction: false, effective_from: null, effective_phrase: null,
    }));
  await put("s", [["convention is tabs", "ok", T0], ["PREF TOPICfmt follow that", "ok", T1]]);
  await drain(db);
  const before = await texts();
  expect(before.length).toBe(1);
  expect(before[0]).toContain("tabs");
  await put("s", [["convention is spaces", "ok", T0], ["PREF TOPICfmt follow that", "ok", T1]]); // the earlier record revised
  expect((await texts()).some((t) => t.includes("tabs"))).toBe(false);
  await drain(db);
  const after = await texts();
  expect(after.length).toBe(1);
  expect(after[0]).toContain("spaces");
  expect((await db`select count(*)::int n from l3_stmt where text like '%tabs%'`)[0].n).toBe(0); // the stale one is gone
});

test("spans closed before L3 existed are extracted after an upgrade, once; starting again adds nothing", async () => {
  await put("old", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db, ["project", "summarize", "embed"]);
  await db`delete from job where kind = 'l3'`; // as before the upgrade: no L3 job was ever queued
  await migrate(db);
  await drain(db);
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
  const k = requests.length;
  await migrate(db);
  await drain(db);
  expect(requests.length).toBe(k);
  expect((await db`select count(*)::int n from l3_stmt`)[0].n).toBe(1);
});

import { zoom } from "./search.ts";
test("a user card line's record: handle opens the user's words with mem_zoom", async () => {
  await db`insert into grant_rule values ('personal', 'tool', 'memex')`;
  await put("a", [["PREF TOPICpm use pnpm", "ok", T0]]);
  await drain(db);
  const h = /\[(record:[^\]]+)\]/.exec((await snap()).text)![1];
  const z = (await zoom(db, { source: sourceIdOf("pi", "me"), scope: "personal", purpose: "tool", id: h })).body as any;
  expect(z.records.map((x: any) => x.text)).toEqual(["PREF TOPICpm use pnpm"]);
});

test("a forget while the extraction request is out: nothing is stored", async () => {
  const a = await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db, ["project", "summarize", "embed"]);
  const orig = extract;
  extract = (recs) => {
    extract = orig;
    before = forgetSource(db, a.source); // lands after the send, before the reply
    return orig(recs);
  };
  await drain(db, ["l3"]);
  expect(requests.some((r) => r.sys.startsWith("You extract"))).toBe(true);
  expect((await db`select count(*)::int n from l3_stmt`)[0].n).toBe(0);
});

test("a forget while the pair request is out: no relation is stored", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  const b = await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db, ["project", "summarize", "embed", "l3"]);
  const orig = judge;
  judge = (x, y) => {
    judge = orig;
    before = forgetSource(db, b.source);
    return orig(x, y);
  };
  await drain(db, ["l3pair"]);
  expect(requests.some((r) => r.sys.startsWith("You compare"))).toBe(true);
  expect((await db`select count(*)::int n from l3_rel`)[0].n).toBe(0);
  expect(await texts()).toEqual(["PREF TOPICpm use npm"]);
});

test("crossing an effective boundary: a new snapshot shows the new decision, a stored one stays as it was", async () => {
  const soon = new Date(Date.now() + 1500);
  extract = (recs) =>
    recs.filter((r) => r.citable && r.kind === "user" && r.text.includes("PREF")).map((r) => ({
      record_id: r.id, text: r.text, scope: "global", retraction: false,
      effective_from: r.text.includes("from then") ? soon.toISOString() : null, effective_phrase: r.text.includes("from then") ? "from then" : null,
    }));
  const now = new Date(Date.now() - 60_000).toISOString();
  await put("a", [["PREF TOPICpm use npm", "ok", now]]);
  await put("b", [["PREF TOPICpm NEW from then use pnpm", "ok", now.replace(/:00\.\d+Z$/, ":30.000Z")]]);
  await drain(db);
  const s1 = await snap();
  expect(s1.text).toContain("use npm");
  expect(s1.text).not.toContain("use pnpm");
  await Bun.sleep(1600);
  expect((await snap()).text).toBe(s1.text); // frozen
  const r = await snap({ refresh_id: "after" });
  expect(r.text).toContain("use pnpm");
  expect(r.text).not.toContain("use npm");
});

test("a revoke while the pair request is out: no relation is stored", async () => {
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await put("b", [["PREF TOPICpm NEW use pnpm", "ok", T1]]);
  await drain(db, ["project", "summarize", "embed", "l3"]);
  const orig = judge;
  judge = (x, y) => {
    judge = orig;
    before = revokeGrant(db, "personal", "derive", "llm");
    return orig(x, y);
  };
  await drain(db, ["l3pair"]);
  expect(requests.some((r) => r.sys.startsWith("You compare"))).toBe(true);
  expect((await db`select count(*)::int n from l3_rel`)[0].n).toBe(0);
});

test("retry feedback never echoes the model's output", async () => {
  let k = 0;
  extract = (recs) => {
    k++;
    if (k === 1) return [{ record_id: "SECRET_FROM_CONTEXT", text: "x", scope: "SECRET_SCOPE", retraction: false, effective_from: null, effective_phrase: null }];
    return [];
  };
  await put("a", [["PREF TOPICpm use npm", "ok", T0]]);
  await drain(db);
  const ex = requests.filter((r) => r.sys.startsWith("You extract"));
  expect(ex.length).toBeGreaterThan(1);
  expect(JSON.stringify(requests)).not.toContain("SECRET_");
});

test("retry feedback is dropped once the input it was about moved", async () => {
  // Appending does not move a session's version; revising an earlier record (the later span's <context>) does.
  await put("a", [["context one", "ok", T0], ["PREF TOPICpm use npm", "ok", T1]]);
  await drain(db, ["project", "summarize", "embed"]);
  let k = 0;
  extract = (recs) => {
    if (!recs.some((r) => r.citable && r.text.includes("PREF"))) return [];
    if (++k === 1) {
      before = put("a", [["context two", "ok", T0], ["PREF TOPICpm use npm", "ok", T1]]).then(() => drain(db, ["project"]));
      return [{ record_id: "nope", text: "x", scope: "global", retraction: false, effective_from: null, effective_phrase: null }];
    }
    return [];
  };
  await drain(db, ["l3"]);
  const ex = requests.filter((r) => r.sys.startsWith("You extract") && r.content.includes("PREF"));
  expect(ex.length).toBeGreaterThan(1);
  expect(ex[1].content).toContain("context two");
  expect(ex[1].content).not.toContain("previous answer");
});

test("an unrelated revision elsewhere in the session: the statement is readable again once projected, without a model call", async () => {
  // Three exchanges, each its own span; the statement is in the first, the third is revised.
  const turns: [string, string, string][] = [["PREF TOPICpm use pnpm", "ok", T0], ["middle", "ok", T1], ["last", "ok", T2]];
  await put("a", turns);
  await drain(db);
  expect(await texts()).toEqual(["PREF TOPICpm use pnpm"]);
  const k = requests.filter((r) => r.sys.startsWith("You extract")).length;
  await put("a", [turns[0], turns[1], ["last revised", "ok", T2]]);
  await drain(db);
  const st = (await db`select span, input_fp, deps from l3_stmt`)[0];
  expect(await texts()).toEqual(["PREF TOPICpm use pnpm"]);
  // Only the revised part was read again; the statement itself was not extracted again.
  const asked = requests.filter((r) => r.sys.startsWith("You extract") && /citable="yes">\n\w+: PREF/.test(r.content));
  expect(asked.length).toBe(1);
  void k;
});

test("the user card over a revoked scope stays dropped when it is granted again", async () => {
  const { allowGrant } = await import("./derive.ts");
  await db`insert into grant_rule values ('work', 'sync', 'memex'), ('work', 'derive', 'llm')`;
  const decl = { source: sourceIdOf("pi", "w"), harness: "pi" as const, nativeId: "w", scope: "work", parent: null, meta: { repo: "repo-a" } };
  const lines = [header("w"), msg("user", "wu0", null, "PREF TOPICsec work rule", T0), msg("assistant", "wa0", "wu0", "ok", T0)];
  const l = await requestLease(db, decl, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "w2", length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await allowGrant(db, "work", "snapshot", "memex");
  await drain(db);
  expect((await snap()).text).toContain("work rule");
  await revokeGrant(db, "work", "snapshot", "memex");
  await allowGrant(db, "work", "snapshot", "memex");
  expect((await snap()).text).not.toContain("work rule");
  expect((await snap({ refresh_id: "r" })).text).toContain("work rule");
});
