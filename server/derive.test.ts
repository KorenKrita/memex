// Model pipeline (DESIGN §5.5, §7) over real Postgres and a local fake model endpoint that can be paused.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { claimJob, connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { allowGrant, revokeGrant, runSummarize, runEmbed, EMBED_DIM } from "./derive.ts";
import { drain, runOne } from "./worker.ts";
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
const header = (id: string) => JSON.stringify({ type: "session", version: 3, id, timestamp: T, cwd: "/x" });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: T, message: { role, content: [{ type: "text", text }] } });
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
