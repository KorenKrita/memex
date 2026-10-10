// Model pipeline (DESIGN §5.5, §7) over real Postgres and a local fake model endpoint that can be paused.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { claimJob, connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { allowGrant, revokeGrant, runSummarize, runEmbed, EMBED_DIM } from "./derive.ts";
import { drain as drainAll, handlers, runOne as runOneAll } from "./worker.ts";
// L3 jobs are tested in l3.test.ts; here they would only add model calls.
const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const drain = (db: any, kinds = KINDS, max?: number) => drainAll(db, kinds, max);
const runOne = (db: any, kinds = KINDS) => runOneAll(db, kinds);
import { sourceIdOf } from "../client/records.ts";

let db: Db;
// Fake OpenAI-compatible server: records every request, can hold replies until released.
let requests: { path: string; body: any }[] = [];
let hold: Promise<void> | null = null;
let summaryText = "fake summary";
let srv: ReturnType<typeof Bun.serve>;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      requests.push({ path, body });
      if (hold) await hold;
      if (path.endsWith("/chat/completions")) {
        const input = body.messages[1].content as string;
        const tool = /<record id="([^"]+)" kind="tool_result">/.exec(input.slice(input.indexOf("<input>")));
        const s = { summary: summaryText, user_asked: ["q"], assistant_chose: [], verified: tool ? [{ claim: "ran", record_id: tool[1] }] : [], tasks: [], kind: ["chat"], anchors: [] };
        return Response.json({ choices: [{ message: { content: JSON.stringify(s) } }] });
      }
      return Response.json({ data: [{ embedding: Array.from({ length: EMBED_DIM }, (_, i) => (i === 0 ? 1 : 0)) }] });
    },
  });
  Object.assign(process.env, {
    MEMEX_LLM_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k-llm",
    MEMEX_EMBED_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k-emb",
  });
});
afterAll(async () => {
  srv.stop(true);
  await db.close();
});
beforeEach(async () => {
  stampOf.clear(); // a fixture's times are its own: an entry built in another test is not a copy here
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip restart identity cascade`);
  await db`insert into grant_rule values ('personal', 'sync', 'memex'), ('personal', 'derive', 'llm'), ('personal', 'derive', 'embedding'), ('work', 'sync', 'memex')`;
  process.env.MEMEX_IDLE_CLOSE_MS = String(10 * 60_000);
  requests = [];
  hold = null;
  summaryText = "fake summary";
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
const header = (id: string, parentSession?: string) =>
  JSON.stringify({ type: "session", version: 3, id, timestamp: parentSession ? stamp("fork", id, parentSession) : T, cwd: "/x", ...(parentSession ? { parentSession } : {}) });
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
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: stamp(role, id, parent, text), message: { role, content: [{ type: "text", text }] } });
let commitN = 0;
async function put(native: string, lines: string[], scope = "personal") {
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope, parent: null };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  const r = await commit(db, { leaseId: l, commitId: `d${commitN++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(r.ok).toBe(true);
  return decl.source;
}
const session = (n: string, secret = "alpha") => [header(n), msg("user", "u1", null, `question ${secret}`), msg("assistant", "a1", "u1", "answer"), msg("user", "u2", "a1", "next")];
const summaries = async () => (await db`select span, text, embedding is not null as emb from span_summary`) as any[];
const chats = () => requests.filter((r) => r.path.endsWith("/chat/completions"));

test("a closed span is summarized and embedded; the open tail is not sent", async () => {
  await put("a", session("a"));
  await drain(db);
  const s = await summaries();
  expect(s.length).toBe(1);
  expect(s[0]).toMatchObject({ text: "fake summary", emb: true });
  expect(chats().length).toBe(1);
  expect(chats()[0].body.messages[1].content).toContain("question alpha");
  expect(chats()[0].body.messages[1].content).not.toContain("next"); // [u2] is the open tail
});

test("no derive grant: zero model requests; allowing it later queues the work again", async () => {
  await db`delete from grant_rule where purpose = 'derive'`;
  await put("b", session("b"));
  await drain(db);
  expect(requests.length).toBe(0);
  expect((await db`select reason from derive_skip`).map((r: any) => r.reason)).toEqual(["not-granted:llm"]);
  await allowGrant(db, "personal", "derive", "llm");
  await allowGrant(db, "personal", "derive", "embedding");
  await drain(db);
  expect(chats().length).toBe(1);
  expect((await summaries())[0].emb).toBe(true);
});

test("a revision committed before the model call: the old text is never sent", async () => {
  await put("c", session("c", "old-secret"));
  await runOne(db, ["project"]);
  // The record changes in L0; the projection has not run again yet, so the stored span is stale.
  const lines = session("c", "new-text");
  await put("c", lines);
  const job = (await claimJob(db, ["summarize"]))!;
  await runSummarize(db, job);
  expect(requests.length).toBe(0);
  await drain(db); // re-projection makes the new span, which is summarized
  expect(chats().every((r) => !JSON.stringify(r.body).includes("old-secret"))).toBe(true);
  expect(chats().some((r) => JSON.stringify(r.body).includes("new-text"))).toBe(true);
});

test("forget during the model call: the reply is not published", async () => {
  const s = await put("d", session("d"));
  await runOne(db, ["project"]);
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const job = (await claimJob(db, ["summarize"]))!;
  const run = runSummarize(db, job);
  while (!chats().length) await Bun.sleep(5);
  await db.begin(async (tx: any) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await tx`insert into tomb values (${"source:" + s})`;
    await tx`insert into vis_version values (${"s:" + s}, 2) on conflict (key) do update set v = vis_version.v + 1`;
  });
  release();
  await run;
  expect((await summaries()).length).toBe(0);
});

test("revoke during the model call: not published, and nothing more is sent", async () => {
  await put("e", session("e"));
  await runOne(db, ["project"]);
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const job = (await claimJob(db, ["summarize"]))!;
  const run = runSummarize(db, job);
  while (!chats().length) await Bun.sleep(5);
  await revokeGrant(db, "personal", "derive", "llm");
  release();
  await run;
  hold = null;
  await drain(db);
  expect((await summaries()).length).toBe(0);
  expect(chats().length).toBe(1);
});

test("a change in another session does not stop publication", async () => {
  await put("f", session("f"));
  await runOne(db, ["project"]);
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const job = (await claimJob(db, ["summarize"]))!;
  const run = runSummarize(db, job);
  while (!chats().length) await Bun.sleep(5);
  await put("other", session("other"));
  release();
  expect(await run).toBe("done");
  expect((await summaries()).length).toBe(1);
});

test("a late embedding of an older summary does not land on the newer one", async () => {
  await put("g", session("g"));
  await drain(db, ["project", "summarize"]);
  const embedJob = (await claimJob(db, ["embed"]))!;
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const run = runEmbed(db, embedJob);
  while (!requests.some((r) => r.path.endsWith("/embeddings"))) await Bun.sleep(5);
  // Meanwhile the summary is replaced (e.g. prompt upgrade) without touching any version.
  await db`update span_summary set text = 'newer', text_hash = 'other', embedding = null`;
  release();
  await run;
  expect((await summaries())[0]).toMatchObject({ text: "newer", emb: false });
});

test("an idle tail closes without any new commit", async () => {
  process.env.MEMEX_IDLE_CLOSE_MS = "300";
  await put("h", [header("h"), msg("user", "u1", null, "only question"), msg("assistant", "a1", "u1", "reply")]);
  await drain(db);
  expect(chats().length).toBe(0); // open
  await Bun.sleep(1600); // the re-projection was scheduled for idle + 1 s
  await drain(db);
  expect(chats().length).toBe(1);
  expect((await summaries()).length).toBe(1);
});

test("model errors keep no provider text in the job row", async () => {
  await put("i", session("i"));
  await runOne(db, ["project"]);
  process.env.MEMEX_LLM_URL = `http://127.0.0.1:${srv.port}/bad`;
  const prev = srv;
  srv = Bun.serve({ port: 0, fetch: () => new Response("upstream says: secret key sk-123 and session text", { status: 500 }) });
  process.env.MEMEX_LLM_URL = `http://127.0.0.1:${srv.port}/v1`;
  await runOne(db, ["summarize"]);
  const [j] = await db`select error from job where kind = 'summarize'`;
  expect(j.error).toBe("llm http 500");
  srv.stop(true);
  srv = prev;
  process.env.MEMEX_LLM_URL = `http://127.0.0.1:${srv.port}/v1`;
});

test("a revoke between the check and the send: nothing is sent", async () => {
  const { hooks } = await import("./derive.ts");
  await put("j", session("j"));
  await runOne(db, ["project"]);
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    await revokeGrant(db, "personal", "derive", "llm");
  };
  await drain(db);
  expect(requests.length).toBe(0);
  expect((await db`select reason from derive_skip`).map((r: any) => r.reason)).toEqual(["not-granted:llm"]);
});

test("an open span is never sent, even if a summarize job names it", async () => {
  const s = await put("k", session("k"));
  await drain(db);
  const [v] = await db`select spans, closed_end from view where source = ${s}`;
  const open = v.spans.find((x: any) => x.id && x.end > v.closed_end).id;
  await db`insert into job (kind, key) values ('summarize', ${open})`;
  requests = [];
  await drain(db);
  expect(requests.length).toBe(0);
});

// ---- regressions from the 2a review ----

test("a child's copies the parent has not synced yet: nothing built from them is sent; inherited once it catches up", async () => {
  const P = sourceIdOf("pi", "rp");
  const pl = [header("rp"), msg("user", "x0", null, "q0"), msg("assistant", "y0", "x0", "r0"), msg("user", "u1", "y0", "q1"), msg("assistant", "a1", "u1", "r1"),
              msg("user", "u2", "a1", "q2 child-native-for-now"), msg("assistant", "a2", "u2", "r2")];
  await put("rp", pl.slice(0, 3)); // the parent has synced only its first exchange so far
  const kid = [header("rk", "/x/rp.jsonl"), ...pl.slice(1), msg("user", "k1", "a2", "child turn")];
  const decl = { source: sourceIdOf("pi", "rk"), harness: "pi" as const, nativeId: "rk", scope: "personal", parent: P };
  const l = await requestLease(db, decl, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "rk0", length: kid.length, changes: kid.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db, ["project"]);
  // u1..a2 are copies (not newer than the fork) the parent has not synced yet: withdrawn, never the child's own —
  // so nothing built from them is sent before the parent catches up (DESIGN §4.1; no remap race to guard).
  expect((await db`select id from span where keys[1] like 'rk/u2:%' or keys[1] like 'rk/u1:%'`).length).toBe(0);
  await drain(db);
  expect(chats().filter((r) => JSON.stringify(r.body).includes("child-native-for-now")).length).toBe(0);
  expect((await db`select id from span where keys[1] like 'rk/k1:%'`).length).toBe(1); // the child's own still is
  await put("rp", pl); // the parent catches up: u1..a2 are the parent's records, inherited by the child
  await drain(db);
  expect((await db`select id from span where keys[1] like 'rp/u2:%'`).length).toBe(1);
});

test("an unchanged input is not summarized again", async () => {
  await put("same", session("same"));
  await drain(db);
  expect(chats().length).toBe(1);
  const [sp] = await db`select span, deps from span_summary`;
  const k = "s:" + sourceIdOf("pi", "same");
  await db.begin(async (tx: any) => {
    await tx`insert into vis_version values (${k}, 2) on conflict (key) do update set v = vis_version.v + 1`;
    // As every version move comes with a projection (a stale projection proves no span closed).
    await tx`insert into job (kind, key) values ('project', ${sourceIdOf("pi", "same")}) on conflict (kind, key) where state in ('queued', 'running') do update set not_before = now()`;
  });
  await drain(db, ["project"]);
  await db`insert into job (kind, key) values ('summarize', ${sp.span}) on conflict (kind, key) where state in ('queued', 'running') do nothing`;
  await drain(db);
  expect(chats().length).toBe(1);
  const [row] = await db`select deps from span_summary`;
  expect(row.deps[k]).toBe((sp.deps[k] ?? 1) + 1); // the vector was renewed
});

test("a summary made from an old <context> is not embedded", async () => {
  await put("ctx", [header("ctx"), msg("user", "u1", null, "first"), msg("assistant", "a1", "u1", "r1"), msg("user", "u2", "a1", "second"), msg("assistant", "a2", "u2", "r2"), msg("user", "u3", "a2", "third")]);
  await drain(db, ["project", "summarize"]);
  const before = (await db`select count(*)::int n from span_summary`)[0].n;
  expect(before).toBe(2);
  requests = [];
  // u1 is revised: [u2 a2] keeps its records (same span id) but its <context> changes.
  await put("ctx", [header("ctx"), msg("user", "u1", null, "first REVISED"), msg("assistant", "a1", "u1", "r1"), msg("user", "u2", "a1", "second"), msg("assistant", "a2", "u2", "r2"), msg("user", "u3", "a2", "third")]);
  await drain(db, ["project"]);
  await drain(db, ["embed"]);
  const embedded = requests.filter((r) => r.path.endsWith("/embeddings")).length;
  expect(embedded).toBe(0); // both summaries depend on the old u1 (as record or as context)
  await drain(db);
  const sent = chats().map((r) => r.body.messages[1].content as string);
  expect(sent.length).toBe(2);
  expect(sent.every((t) => !t.includes(">\nfirst\n<"))).toBe(true);
});

test("allow racing a skip: the skipped work is not lost", async () => {
  const { hooks } = await import("./derive.ts");
  await db`delete from grant_rule where purpose = 'derive'`;
  await put("race", session("race"));
  await runOne(db, ["project"]);
  hooks.beforeSkip = async () => {
    hooks.beforeSkip = async () => {};
    await allowGrant(db, "personal", "derive", "llm");
    await allowGrant(db, "personal", "derive", "embedding");
  };
  await drain(db);
  expect(chats().length).toBe(1);
});

test("a child's prefix cut from the parent's span is a representation of that span", async () => {
  const P = sourceIdOf("pi", "up");
  const pl = [header("up"), msg("user", "u1", null, "q " + "y".repeat(100)), msg("assistant", "a1", "u1", "r1"), msg("assistant", "a2", "a1", "r2"), msg("user", "u3", "a2", "next")];
  await put("up", pl);
  await drain(db, ["project"]);
  const [S] = (await db`select id from span where source = ${P} and keys[1] like '%/u1:%'`).map((r: any) => r.id);
  const kid = [header("uk", "/x/up.jsonl"), pl[1], pl[2], msg("assistant", "b2", "a1", "child branch")];
  const decl = { source: sourceIdOf("pi", "uk"), harness: "pi" as const, nativeId: "uk", scope: "personal", parent: P };
  const l = await requestLease(db, decl, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "uk0", length: kid.length, changes: kid.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db, ["project"]);
  const prefix = await db`select id, unit from span where prefix and cut = ${S}`;
  expect(prefix.length).toBe(1);
  expect(prefix[0].unit).toBe(S);
});
