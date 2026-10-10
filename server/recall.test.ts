// Per-turn recall: frozen decisions (DESIGN §6.2) over real Postgres and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { EMBED_DIM, revokeGrant } from "./derive.ts";
import { Guarded, serve, addApiKey, httpHooks, transport } from "./http.ts";
import { recall, blocks, recallHooks, tokens, TOKEN_BUDGET, FRAMING, queryOf, clip } from "./recall.ts";
import { zoom } from "./search.ts";
import { forgetSource } from "./forget.ts";
import { drain as drainAll, handlers } from "./worker.ts";
// L3 jobs are tested in l3.test.ts; here they would only add model calls.
const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const drain = (db: any, kinds = KINDS, max?: number) => drainAll(db, kinds, max);
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let calls: { path: string; body: any }[] = [];
let rerankDelay = 0;
let embedDelay = 0;
let rerankStatus = 200;
let score = (d: string): number => (d.includes("ANSWER") ? 0.9 : 0.1);
let srv: ReturnType<typeof Bun.serve>;
let embedHook: null | (() => Promise<void>) = null;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      calls.push({ path, body });
      if (path.endsWith("/embeddings") && embedDelay) await Bun.sleep(embedDelay);
      if (path.endsWith("/embeddings") && embedHook) await embedHook();
      if (path.endsWith("/rerank")) {
        if (rerankDelay) await Bun.sleep(rerankDelay);
        if (rerankStatus !== 200) return new Response("no", { status: rerankStatus });
        return Response.json({ data: body.documents.map((d: string, index: number) => ({ index, relevance_score: score(d) })) });
      }
      if (path.endsWith("/chat/completions")) {
        const sys = body.messages[0].content as string;
        if (sys.startsWith("You write the card")) return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "T", overview: "card", results: [], open_threads: [] }) } }] });
        if (sys.startsWith("You merge")) return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "merged" }) } }] });
        const s = /kind="user">\n?([^<]*)/.exec((body.messages[1].content as string).split("<input>")[1] ?? "")?.[1]?.trim() ?? "s";
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: s, user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] }) } }] });
      }
      return Response.json({ data: body.input.map(() => ({ embedding: Array.from({ length: EMBED_DIM }, (_, i) => (i === 0 ? 1 : 0)) })) });
    },
  });
  const base = `http://127.0.0.1:${srv.port}/v1`;
  Object.assign(process.env, {
    MEMEX_LLM_URL: base, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k",
    MEMEX_EMBED_URL: base, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
    MEMEX_RERANK_URL: base + "/rerank", MEMEX_RERANK_MODEL: "fake-rerank", MEMEX_RERANK_KEY: "k",
  });
});
afterAll(async () => {
  srv.stop(true);
  await db.close();
  delete process.env.MEMEX_RECALL_TAU;
  delete process.env.MEMEX_RECALL_MS;
});
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, search_trace,
                   forest_node, forest_access, session_card, recall_state, recall_block restart identity cascade`);
  for (const [s, p, d] of [["personal", "sync", "memex"], ["personal", "derive", "llm"], ["personal", "derive", "embedding"],
                           ["personal", "recall", "memex"], ["personal", "recall", "embedding"], ["personal", "recall", "rerank"],
                           ["personal", "tool", "memex"]])
    await db`insert into grant_rule values (${s}, ${p}, ${d})`;
  process.env.MEMEX_IDLE_CLOSE_MS = "0";
  process.env.MEMEX_RECALL_TAU = "0.5";
  delete process.env.MEMEX_RECALL_MS;
  calls = [];
  rerankDelay = 0;
  embedDelay = 0;
  rerankStatus = 200;
  score = (d) => (d.includes("ANSWER") ? 0.9 : 0.1);
  recallHooks.beforeSave = async () => {};
  embedHook = null;
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
const header = (id: string, extra = {}) => JSON.stringify({ type: "session", version: 3, id, timestamp: T, cwd: "/x", ...extra });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: T, message: { role, content: [{ type: "text", text }] } });
let n = 0;
async function put(native: string, ex: [string, string][]) {
  const lines = [header(native)];
  let prev: string | null = null;
  ex.forEach(([u, a], i) => {
    lines.push(msg("user", `u${i}`, prev, u), msg("assistant", `a${i}`, `u${i}`, a));
    prev = `a${i}`;
  });
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope: "personal", parent: null };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  expect((await commit(db, { leaseId: l, commitId: `r${n++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) })).ok).toBe(true);
  return decl.source;
}
/** Commit raw lines (branches: entries with any parents) with an optional leaf. */
async function putRaw(native: string, lines: string[], leaf?: string, parent: string | null = null) {
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope: "personal", parent };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  expect((await commit(db, { leaseId: l, commitId: `r${n++}`, length: lines.length, leaf, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) })).ok).toBe(true);
  return decl.source;
}
const me = sourceIdOf("pi", "me");
/** A previous-exchange reference as the host sends it: the record id and the hash of the line it uploaded. */
const ref = (raw: string) => ({ rec: JSON.parse(raw).id, hash: sha(raw) });
const R = (user_rec: string, input: string, x: object = {}) => ({ source: me, scope: "personal", user_rec, input, dests: ["embedding", "rerank"], ...x });
const ask = async (b: object) => {
  const r = await recall(db, b);
  return (r instanceof Guarded ? r.body : r) as any;
};
const replay = async (cursor?: string) => (await blocks(db, { source: me, scope: "personal", cursor })).body as any;
const reranks = () => calls.filter((c) => c.path.endsWith("/rerank")).length;

// ---------- frozen and bound ----------

test("a decision is made once per user record; a retry replays it without a model call, new history does not change it", async () => {
  await put("a", [["where is the port", "ANSWER the port is zx-7781"]]);
  await drain(db);
  const d1 = await ask(R("u1", "the port zx-7781?"));
  expect(d1.state).toBe("shown");
  expect(d1.text).toContain("zx-7781");
  const k = reranks();
  await put("b", [["more on the port", "ANSWER port zx-7781 moved to zx-9"]]);
  await drain(db);
  const d2 = await ask(R("u1", "the port zx-7781?"));
  expect(reranks()).toBe(k);
  expect(d2.text).toBe(d1.text);
});

test("the same user record with another query is refused", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  await expect(ask(R("u1", "something else"))).rejects.toThrow("another query");
});

test("an empty decision is stored and replayed too", async () => {
  await put("a", [["q", "nothing relevant here zx-7781"]]); // rerank scores it 0.1 < τ
  await drain(db);
  const d = await ask(R("u1", "zx-7781"));
  expect(d.state).toBe("empty");
  const k = reranks();
  expect((await ask(R("u1", "zx-7781"))).state).toBe("empty");
  expect(reranks()).toBe(k);
  expect((await db`select state from recall_block`).map((r: any) => r.state)).toEqual(["empty"]);
});

test("slash commands and empty input are skipped; nothing stored", async () => {
  expect((await ask(R("u1", "/memex refresh"))).state).toBe("skipped");
  expect((await ask(R("u2", "   "))).state).toBe("skipped");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("the query takes the input and the same branch's last exchange, the reply cut to 800 bytes on a character boundary", () => {
  const q = queryOf("就第二个", "选哪个方案", "中".repeat(400));
  expect(q.startsWith("就第二个\nprevious user: 选哪个方案\nprevious reply: ")).toBe(true);
  expect(Buffer.byteLength(q.split("previous reply: ")[1])).toBe(798); // 266 characters of 3 bytes
  expect(clip("中中", 4)).toBe("中");
});

// ---------- the bar and the hint lines ----------

test("τ not configured: not ready, no search, nothing stored", async () => {
  delete process.env.MEMEX_RECALL_TAU;
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  expect((await ask(R("u1", "zx-7781"))).state).toBe("not-ready");
  expect(reranks()).toBe(0);
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("no rerank (failed, not granted) → nothing injected, never a fallback score", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  rerankStatus = 500;
  expect(await ask(R("u1", "zx-7781"))).toEqual({ state: "unavailable", reason: "endpoint-500" });
  rerankStatus = 200;
  await db`delete from grant_rule where purpose = 'recall' and dest = 'rerank'`;
  expect(await ask(R("u2", "zx-7781"))).toEqual({ state: "unavailable", reason: "not-granted" });
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0); // not decisions: a later retry may decide
});

test("a non-finite score or one under τ is not injected", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  score = () => 0.49;
  expect((await ask(R("u1", "zx-7781"))).state).toBe("empty");
  score = () => NaN; // JSON turns it into null: the rerank result is incomplete
  expect((await ask(R("u2", "zx-7781"))).state).toBe("unavailable");
});

test("at most 3 hint lines, block within the token budget framing included; each handle opens with mem_zoom", async () => {
  for (let i = 0; i < 6; i++) await put("s" + i, [[`ANSWER topic zx-7781 number ${i} ${"长".repeat(150)}`, `reply ${i}`]]);
  await drain(db);
  const d = await ask(R("u1", "zx-7781"));
  expect(d.state).toBe("shown");
  expect(d.text.startsWith(FRAMING)).toBe(true);
  const lines = d.text.split("\n").filter((l: string) => /^\[hint:\d+\.\d+\] /.test(l));
  expect(lines.length).toBe(d.text.split("\n").length - 3); // framing (2 lines) and the end tag, the rest hints
  expect(lines.length).toBeGreaterThan(0);
  expect(lines.length).toBeLessThanOrEqual(3);
  expect(tokens(d.text)).toBeLessThanOrEqual(TOKEN_BUDGET());
  for (const l of lines) {
    const id = /^\[([^\]]+)\]/.exec(l)![1];
    const z = (await zoom(db, { source: me, scope: "personal", purpose: "tool", id })).body as any;
    // The frozen result: the hint's own summary text is in what the handle opens.
    const shown = l.split(" · ")[1].replace(" → mem_zoom", "").slice(0, 30);
    expect(JSON.stringify(z)).toContain(shown);
  }
});

test("an incomplete rerank input is not injected, however high its score", async () => {
  // The next user message after the hit is longer than the rerank input keeps: marked next-clipped.
  await put("a", [["q", "ANSWER zx-7781"], ["x".repeat(3000), "r"]]);
  await drain(db);
  score = () => 0.99;
  const d = await ask(R("u1", "zx-7781"));
  const { search } = await import("./search.ts");
  const sr = (await search(db, { source: me, scope: "personal", purpose: "recall", query: "zx-7781", dests: ["embedding", "rerank"] })).body as any;
  const rec = sr.results.find((x: any) => x.key.startsWith("record:"));
  expect(rec.incomplete).toContain("next-clipped"); // the fixture does what it says
  const [row] = await db`select refs from recall_block where user_rec = 'u1'`;
  expect(JSON.stringify(row?.refs ?? [])).not.toContain(rec.key.slice(7)); // the clipped record is not among the frozen hints
});

test("lines that would pass the token budget are left out (budget over the whole block)", async () => {
  // Three passing hints of ~240 bytes of CJK each (~160 tokens): with the framing, over 800 only past ~4.
  for (let i = 0; i < 3; i++) await put("s" + i, [[`ANSWER zx-7781 ${i} ${"长".repeat(150)}`, `reply ${i}`]]);
  await drain(db);
  process.env.MEMEX_RECALL_TOKENS = "500"; // a small budget: room for the framing and one line only
  try {
    const d = await ask(R("u1", "zx-7781"));
    expect(Buffer.byteLength(d.text)).toBeLessThanOrEqual(500);
    expect(d.text.split("\n").filter((l: string) => /^\[hint:\d+\.\d+\] /.test(l)).length).toBe(1);
  } finally {
    delete process.env.MEMEX_RECALL_TOKENS;
  }
});

test("a slow embedding endpoint is bounded by the same deadline", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "300";
  embedDelay = 800;
  try {
    const t = Date.now();
    expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
    expect(Date.now() - t).toBeLessThan(600);
  } finally {
    embedDelay = 0;
  }
});

test("forget while a holder's block shows it: the holder's generation moves even though its vector names only the forgotten source", async () => {
  // The decision's vector is {s:me, d:*, s:a}; forget(a) bumps s:a only after deleting a's rows — the generation
  // must still move for the holder.
  const a = await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const v = async () => Number((await db`select v from vis_version where key = ${"r:" + me}`)[0]?.v ?? 1);
  const g0 = await v();
  await forgetSource(db, a);
  expect(await v()).toBeGreaterThan(g0);
});

// ---------- withdrawal and the cursor ----------

test("replay: all blocks first, then only new ones after the cursor", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const r1 = await replay();
  expect(r1.replace).toBe(true);
  expect(r1.blocks.map((b: any) => b.user_rec)).toEqual(["u1"]);
  const r2 = await replay(r1.cursor);
  expect(r2.replace).toBe(false);
  expect(r2.blocks).toEqual([]);
  await ask(R("u2", "zx-7781 again"));
  const r3 = await replay(r2.cursor);
  expect(r3.replace).toBe(false);
  expect(r3.blocks.map((b: any) => b.user_rec)).toEqual(["u2"]);
});

test("a withdrawal after saving replaces the set even with nothing new; an unrelated change does not", async () => {
  const a = await put("a", [["q", "ANSWER zx-7781"]]);
  await put("other", [["unrelated", "words"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const r1 = await replay();
  await put("other", [["unrelated changed", "words"]]); // a revision elsewhere
  expect((await replay(r1.cursor)).replace).toBe(false);
  await put("a", [["q changed", "ANSWER zx-7781"]]); // a revision of the source the block shows
  const r2 = await replay(r1.cursor);
  expect(r2.replace).toBe(true);
  expect(r2.blocks).toEqual([]);
  void a;
});

test("forget: the block's text is gone from storage and the holder replaces its set", async () => {
  const a = await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const r1 = await replay();
  await forgetSource(db, a);
  const r2 = await replay(r1.cursor);
  expect(r2.replace).toBe(true);
  expect(r2.blocks).toEqual([]);
  expect(JSON.stringify(await db`select text from recall_block`)).not.toContain("zx-7781");
  expect((await ask(R("u1", "zx-7781"))).state).toBe("withdrawn"); // the binding stays decided: no new search
});

test("a revoke of the rerank destination withdraws decisions made with it", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const r1 = await replay();
  await revokeGrant(db, "personal", "recall", "rerank");
  const r2 = await replay(r1.cursor);
  expect(r2.replace).toBe(true);
  expect(r2.blocks).toEqual([]);
});

test("a withdrawal between search and save: nothing saved", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  recallHooks.beforeSave = async () => {
    await put("a", [["q changed", "ANSWER zx-7781"]]);
  };
  expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("concurrent decisions of one session get distinct ordered seqs: a cursor misses none", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  const r0 = await replay();
  await Promise.all(Array.from({ length: 6 }, (_, i) => ask(R("c" + i, "zx-7781 " + i))));
  const r1 = await replay(r0.cursor);
  expect(r1.blocks.map((b: any) => b.user_rec).sort()).toEqual(["c0", "c1", "c2", "c3", "c4", "c5"]);
  const seqs = (await db`select seq from recall_block order by seq`).map((r: any) => Number(r.seq));
  expect(seqs).toEqual([1, 2, 3, 4, 5, 6]);
});

test("concurrent retries of one binding save one decision", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  const all = await Promise.all(Array.from({ length: 5 }, () => ask(R("u1", "zx-7781"))));
  expect(new Set(all.map((x) => x.text)).size).toBe(1);
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(1);
});

test("an incremental replay read just before a withdrawal is not delivered as 'nothing changed'", async () => {
  const a = await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  const r1 = await replay();
  const api = await serve(db, { port: 0 });
  const key = await addApiKey(db, "t");
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await put("a", [["q changed", "ANSWER zx-7781"]]); // after the empty increment was computed, before delivery
  };
  try {
    const r = await transport(`http://127.0.0.1:${api.port}`, key)("POST", "/v1/recall/blocks", { source: me, scope: "personal", cursor: r1.cursor });
    expect(fired).toBeGreaterThan(1); // recomputed
    expect(r.replace).toBe(true);
    expect(r.blocks).toEqual([]);
  } finally {
    httpHooks.beforeGuard = async () => {};
    await api.stop();
  }
  void a;
});

test("the querying session revised while its query embedding is out: nothing frozen from the old query", async () => {
  // The query's context is the session's last exchange; revising it moves s:me while the embedding is out.
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["old question", "old reply"]]);
  await drain(db);
  embedHook = async () => {
    embedHook = null;
    await put("me", [["revised question", "old reply"]]);
  };
  const d = await ask(R("u9", "zx-7781", { prev_user: "old question", prev_assistant: "old reply",
    prev_user_ref: ref(msg("user", "u0", null, "old question")), prev_assistant_ref: ref(msg("assistant", "a0", "u0", "old reply")) }));
  expect(d.state).toBe("query-stale");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

// ---------- the deadline ----------

test("past the budget: no decision, nothing stored, the next input unaffected", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "300";
  rerankDelay = 600;
  const t = Date.now();
  expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
  expect(Date.now() - t).toBeLessThan(550);
  await Bun.sleep(400); // the late reply arrives; nothing may be written for it
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
  rerankDelay = 0;
  process.env.MEMEX_RECALL_MS = "2500";
  expect((await ask(R("u2", "zx-7781"))).state).toBe("shown");
  expect((await db`select user_rec from recall_block`).map((r: any) => r.user_rec)).toEqual(["u2"]);
});

test("the save waiting for the barrier past the deadline: rolled back, nothing stored", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "600";
  const { BARRIER } = await import("./sync.ts");
  // After the search, a writer takes the barrier exclusively and keeps it past the deadline.
  let blocker: Promise<unknown> = Promise.resolve();
  recallHooks.beforeSave = async () => {
    let holding!: () => void;
    const isHeld = new Promise<void>((r) => (holding = r));
    blocker = db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      holding();
      await Bun.sleep(900);
    });
    await isHeld;
  };
  expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
  await blocker;
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("an expired request is never sent: the endpoint receives nothing", async () => {
  const { guardedPost } = await import("./derive.ts");
  const k = calls.length;
  await expect(guardedPost(db, {}, process.env.MEMEX_RERANK_URL!, "k", { x: 1 }, Date.now() - 1)).rejects.toThrow("deadline");
  // Expired while waiting for the barrier: still nothing sent.
  const { BARRIER } = await import("./sync.ts");
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let holding!: () => void;
  const isHeld = new Promise<void>((r) => (holding = r));
  const blocker = db.begin(async (tx: any) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    holding();
    await held;
  });
  await isHeld;
  const p = guardedPost(db, {}, process.env.MEMEX_RERANK_URL!, "k", { x: 1 }, Date.now() + 200);
  setTimeout(release, 400);
  await expect(p).rejects.toThrow();
  await blocker;
  await Bun.sleep(100);
  expect(calls.length).toBe(k);
});

test("trace publication waiting for the barrier is bounded by the deadline", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "600";
  const { searchHooks } = await import("./search.ts");
  const { BARRIER } = await import("./sync.ts");
  let blocker: Promise<unknown> = Promise.resolve();
  searchHooks.beforeTrace = async () => {
    let holding!: () => void;
    const isHeld = new Promise<void>((r) => (holding = r));
    blocker = db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      holding();
      await Bun.sleep(1000);
    });
    await isHeld;
  };
  try {
    const t = Date.now();
    expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
    expect(Date.now() - t).toBeLessThan(900);
    await blocker;
    await Bun.sleep(50);
    expect((await db`select count(*)::int n from search_trace`)[0].n).toBe(0);
    expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
  } finally {
    searchHooks.beforeTrace = async () => {};
  }
});

test("long inputs: the composed query stays within what search accepts", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  const d = await ask(R("u1", "zx-7781 " + "x".repeat(8180), { prev_user: "y".repeat(8192), prev_assistant: "z".repeat(60000) }));
  expect(["shown", "empty"]).toContain(d.state);
});

test("a hint opens the frozen evidence on the frozen view after the session moves on", async () => {
  // The evidence in a fork child that later branches elsewhere: the hint keeps the view it was found under.
  const a = await put("a", [["ANSWER zx-7781 first", "r0"], ["second", "r1"]]);
  await drain(db);
  const d = await ask(R("u1", "zx-7781"));
  expect(d.state).toBe("shown");
  const l = d.text.split("\n").find((x: string) => x.startsWith("[hint:"))!;
  const id = /^\[([^\]]+)\]/.exec(l)![1];
  const [row] = await db`select refs from recall_block`;
  const z1 = (await zoom(db, { source: me, scope: "personal", purpose: "tool", id })).body as any;
  const { at: _at, ...frozen } = row.refs[0].view_key;
  expect(z1.view_key).toMatchObject(frozen);
  expect(JSON.stringify(z1)).toContain("ANSWER zx-7781 first");
  // The source revised: the frozen view key no longer holds — refused, never answered with the new text.
  await put("a", [["ANSWER zx-7781 first", "r0"], ["second changed", "r1"]]);
  await drain(db);
  await expect(zoom(db, { source: me, scope: "personal", purpose: "tool", id })).rejects.toThrow();
  void a;
});

test("the deadline passing during save: nothing stored", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "400";
  recallHooks.beforeSave = async () => {
    await Bun.sleep(450);
  };
  expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("a hint opens the frozen view after the session switches branch (no withdrawal)", async () => {
  // a: u0 → a0 → (u1 → a1 "ANSWER on the first branch") | (u2 → a2 elsewhere, later the leaf)
  const h = header("br");
  const l = [h, msg("user", "u0", null, "start"), msg("assistant", "a0", "u0", "ok"),
             msg("user", "u1", "a0", "ANSWER zx-7781 on the first branch"), msg("assistant", "a1", "u1", "r1")];
  await putRaw("br", l, "a1");
  await drain(db);
  const d = await ask(R("u1", "zx-7781"));
  expect(d.state).toBe("shown");
  const id = /^\[([^\]]+)\]/.exec(d.text.split("\n").find((x: string) => x.startsWith("[hint:"))!)![1];
  const [row] = await db`select refs from recall_block`;
  const { at: _a, ...frozen } = row.refs[0].view_key;
  // A new branch from a0 becomes the leaf: appended records only, no withdrawal.
  await putRaw("br", [...l, msg("user", "u2", "a0", "other branch"), msg("assistant", "a2", "u2", "r2")], "a2");
  await drain(db);
  const z = (await zoom(db, { source: me, scope: "personal", purpose: "tool", id })).body as any;
  expect(z.view_key).toMatchObject(frozen); // the frozen tip, not the new leaf's
  expect(z.view_key.tip).toBe("a1");
  expect(JSON.stringify(z)).toContain("on the first branch");
});

test("the first read waiting on a locked table is bounded by the deadline: no model call, nothing stored later", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "400";
  let holding!: () => void;
  const isHeld = new Promise<void>((r) => (holding = r));
  const blocker = db.begin(async (tx: any) => {
    await tx`lock table recall_block in access exclusive mode`;
    holding();
    await Bun.sleep(1000);
  });
  await isHeld;
  const k = calls.length;
  const t = Date.now();
  expect((await ask(R("u1", "zx-7781"))).state).toBe("timeout");
  expect(Date.now() - t).toBeLessThan(800);
  await blocker;
  await Bun.sleep(100);
  expect(calls.length).toBe(k);
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("upgrade from the 2b83df9 schema: the withdrawal count carries over; old bare-handle blocks are not served", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  await ask(R("u1", "zx-7781"));
  // As the older schema left it: a gen count (2 withdrawals seen by a client holding "seq.3"), and a block
  // without refs whose text has a bare span handle.
  await db`update recall_state set gen = 2`;
  await db`update recall_block set refs = '[]', text = '<memex-recall>\n[span:sp_x] on-path · old → mem_zoom\n</memex-recall>'`;
  await db`delete from vis_version where key = ${"r:" + me}`;
  await migrate(db);
  const v = Number((await db`select v from vis_version where key = ${"r:" + me}`)[0].v);
  expect(v).toBeGreaterThan(2); // above any generation an old client could hold (its cursor said 2)
  const r = await replay("1.2");
  expect(r.replace).toBe(true);
  expect(r.blocks).toEqual([]); // the legacy block is not served
  expect((await ask(R("u1", "zx-7781"))).state).toBe("withdrawn"); // its binding stays decided; no new search
  expect((await db`select text from recall_block`)[0].text).toContain("sp_x"); // kept, not rewritten
});

test("the querying session revised before the search starts: refused at its first read, no model call", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["old question", "old reply"]]);
  await drain(db);
  // Revise between recall's pin and search: the pin no longer holds at search's authorization read.
  const { recallHooks: rh } = await import("./recall.ts");
  rh.beforeSearch = async () => {
    await put("me", [["revised question", "old reply"]]);
  };
  try {
    const k = calls.length;
    expect((await ask(R("u9", "zx-7781", { prev_user: "old question", prev_user_ref: ref(msg("user", "u0", null, "old question")) }))).state).toBe("query-stale");
    expect(calls.length).toBe(k);
  } finally {
    rh.beforeSearch = async () => {};
  }
});

// ---------- review round 3 ----------

/** The querying session as a fork child of `fpar` whose previous exchange is inherited from it. */
async function forkMe() {
  const P = sourceIdOf("pi", "fpar");
  const pl = [header("fpar"), msg("user", "p0", null, "parent question"), msg("assistant", "p1", "p0", "parent reply")];
  await putRaw("fpar", pl);
  await putRaw("me", [header("me", { parentSession: "/x/fpar.jsonl" }), pl[1], pl[2]], undefined, P);
  return { P, pl };
}

test("a fork child's recall with an unchanged inherited context succeeds", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { pl } = await forkMe();
  await drain(db);
  const d = await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_assistant: "parent reply", prev_user_ref: ref(pl[1]), prev_assistant_ref: ref(pl[2]) }));
  expect(d.state).toBe("shown");
});

test("the fork ancestor holding the inherited context revised during the call: nothing frozen", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { pl } = await forkMe();
  await drain(db);
  embedHook = async () => {
    embedHook = null;
    await putRaw("fpar", [pl[0], msg("user", "p0", null, "parent question revised"), pl[2]]);
  };
  const d = await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_assistant: "parent reply", prev_user_ref: ref(pl[1]), prev_assistant_ref: ref(pl[2]) }));
  expect(d.state).toBe("query-stale");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
});

test("the same user record with two different queries at once: one decides, the other is refused", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  // Both pass the first read before either saves.
  let waiting = 0;
  let gate!: () => void;
  const open = new Promise<void>((r) => (gate = r));
  recallHooks.beforeSave = async () => {
    if (++waiting === 2) gate();
    await open;
  };
  const rs = await Promise.allSettled([ask(R("u1", "zx-7781 first")), ask(R("u1", "zx-7781 second"))]);
  const ok = rs.filter((x) => x.status === "fulfilled");
  const bad = rs.filter((x) => x.status === "rejected") as PromiseRejectedResult[];
  expect(ok.length).toBe(1);
  expect(bad.length).toBe(1);
  expect(String(bad[0].reason)).toContain("another query");
  expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(1);
});

test("waiting for a pool connection past the deadline: the call ends in time and nothing is saved later", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "600";
  const one = connect(undefined, 1);
  let blocker: Promise<unknown> = Promise.resolve();
  recallHooks.beforeSave = async () => {
    let holding!: () => void;
    const isHeld = new Promise<void>((r) => (holding = r));
    blocker = one.begin(async () => {
      holding();
      await Bun.sleep(1200);
    });
    await isHeld;
  };
  try {
    const t = Date.now();
    const r = await recall(one, R("u1", "zx-7781"));
    expect((r instanceof Guarded ? r.body : r) as any).toMatchObject({ state: "timeout" });
    expect(Date.now() - t).toBeLessThan(1000);
    await blocker;
    await Bun.sleep(200);
    expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(0);
  } finally {
    await one.close();
  }
});

test("saved in time but the answer blocked: the call times out, the decision stays for a retry", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  process.env.MEMEX_RECALL_MS = "800";
  const one = connect(undefined, 1);
  const { recallHooks: rh } = await import("./recall.ts");
  let blocker: Promise<unknown> = Promise.resolve();
  rh.afterSave = async () => {
    let holding!: () => void;
    const isHeld = new Promise<void>((r) => (holding = r));
    blocker = one.begin(async () => {
      holding();
      await Bun.sleep(1200);
    });
    await isHeld;
  };
  try {
    const t = Date.now();
    const r = await recall(one, R("u1", "zx-7781"));
    expect((r instanceof Guarded ? r.body : r) as any).toMatchObject({ state: "timeout" });
    expect(Date.now() - t).toBeLessThan(1100);
    await blocker;
    expect((await db`select count(*)::int n from recall_block`)[0].n).toBe(1);
    rh.afterSave = async () => {};
    const k = reranks();
    expect((await ask(R("u1", "zx-7781"))).state).toBe("shown"); // recovered by the retry, no new search
    expect(reranks()).toBe(k);
  } finally {
    rh.afterSave = async () => {};
    await one.close();
  }
});

// ---------- review stage 3: the previous exchange's provenance ----------

const outbound = () => calls.map((c) => JSON.stringify(c.body)).join("\n");

test("the previous exchange is used only with a reference that matches its current record", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { pl } = await forkMe();
  await drain(db);
  const d = await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_assistant: "parent reply", prev_user_ref: ref(pl[1]), prev_assistant_ref: ref(pl[2]) }));
  expect(d.state).toBe("shown");
  expect(outbound()).toContain("parent question");
  expect(outbound()).toContain("parent reply");
});

test("a previous exchange without a reference is not sent anywhere", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await forkMe();
  await drain(db);
  await ask(R("u9", "zx-7781", { prev_user: "UNPROVEN_TEXT", prev_assistant: "UNPROVEN_REPLY" }));
  expect(outbound()).not.toContain("UNPROVEN");
});

test("the fork parent forgotten before the recall: its inherited exchange is not sent", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { P, pl } = await forkMe();
  await drain(db);
  await forgetSource(db, P);
  await drain(db);
  calls = [];
  await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_assistant: "parent reply", prev_user_ref: ref(pl[1]), prev_assistant_ref: ref(pl[2]) }));
  expect(outbound()).not.toContain("parent question");
  expect(outbound()).not.toContain("parent reply");
});

test("the session's own previous exchange revised before the recall: the host's old text is not sent", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["OLD_QUESTION", "old reply"]]);
  await drain(db);
  const old = msg("user", "u0", null, "OLD_QUESTION");
  await put("me", [["new question", "old reply"]]);
  await drain(db);
  calls = [];
  expect((await ask(R("u9", "zx-7781", { prev_user: "OLD_QUESTION", prev_user_ref: ref(old) }))).state).toBe("query-stale");
  expect(outbound()).not.toContain("OLD_QUESTION");
  expect(calls.length).toBe(0);
});

test("a recall block's source scope revoked then granted again: the block stays withdrawn", async () => {
  const { allowGrant } = await import("./derive.ts");
  await db`insert into grant_rule values ('work', 'sync', 'memex'), ('work', 'derive', 'llm'), ('work', 'derive', 'embedding'), ('work', 'recall', 'memex'), ('work', 'recall', 'rerank'), ('work', 'recall', 'embedding')`;
  const decl = { source: sourceIdOf("pi", "w"), harness: "pi" as const, nativeId: "w", scope: "work", parent: null };
  const lines = [header("w"), msg("user", "u0", null, "q"), msg("assistant", "a0", "u0", "ANSWER zx-7781")];
  const l = await requestLease(db, decl, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "w1", length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db);
  expect((await ask(R("u1", "zx-7781"))).state).toBe("shown");
  await revokeGrant(db, "work", "recall", "memex");
  expect((await ask(R("u1", "zx-7781"))).state).toBe("withdrawn");
  await allowGrant(db, "work", "recall", "memex");
  expect((await ask(R("u1", "zx-7781"))).state).toBe("withdrawn");
  expect((await replay()).blocks.map((b: any) => b.text).join("")).not.toContain("zx-7781");
});

test("over HTTP, a decision delivered past the recall deadline is not shown; a retry reads the saved decision", async () => {
  const { BARRIER } = await import("./sync.ts");
  process.env.MEMEX_RECALL_MS = "600";
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  const api = await serve(db, { port: 0 });
  const key = await addApiKey(db, "t");
  let release!: () => void;
  const hold = new Promise<void>((ok) => (release = ok));
  let held: Promise<unknown> | null = null;
  recallHooks.beforeSave = async () => {}; // saved in time
  httpHooks.beforeGuard = async () => {
    httpHooks.beforeGuard = async () => {};
    // A writer holds the barrier exclusively past the deadline, while the answer waits to be delivered.
    let got!: () => void;
    const taken = new Promise<void>((ok) => (got = ok));
    held = db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      got();
      await hold;
    });
    await taken;
    setTimeout(release, 1200);
  };
  try {
    const t0 = Date.now();
    const r = await transport(`http://127.0.0.1:${api.port}`, key)("POST", "/v1/recall", R("u1", "zx-7781")).catch((e: any) => ({ error: String(e) }));
    expect(Date.now() - t0).toBeLessThan(1100);
    expect((r as any).state === "shown").toBe(false);
    await held;
    delete process.env.MEMEX_RECALL_MS;
    const again = await ask(R("u1", "zx-7781"));
    expect(again.state).toBe("shown"); // the timely decision stays
  } finally {
    httpHooks.beforeGuard = async () => {};
    release();
    await api.stop();
  }
});

test("over HTTP, an event-loop stall after the response is prepared: no recall content past the deadline", async () => {
  process.env.MEMEX_RECALL_MS = "800";
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  const api = await serve(db, { port: 0 });
  const key = await addApiKey(db, "t");
  let t0 = 0;
  httpHooks.beforePiece = async () => {
    httpHooks.beforePiece = async () => {};
    while (Date.now() - t0 < 900) {} // a synchronous stall past the deadline
  };
  try {
    t0 = Date.now();
    // Past the deadline the stream is broken off: the client gets an error or a cut body, never the content.
    const text = await fetch(`http://127.0.0.1:${api.port}/v1/recall`, { method: "POST", headers: { authorization: "Bearer " + key, "content-type": "application/json" }, body: JSON.stringify(R("u1", "zx-7781")) })
      .then((res) => res.text()).catch(() => "");
    expect(text).not.toContain("zx-7781");
  } finally {
    httpHooks.beforePiece = async () => {};
    await api.stop();
  }
});

test("the fork parent's inherited record revised before the recall, the child not projected again yet: its copy is not sent", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { pl } = await forkMe();
  await drain(db);
  await putRaw("fpar", [pl[0], msg("user", "p0", null, "PARENT_REVISED"), pl[2]]); // committed, not projected
  calls = [];
  await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_user_ref: ref(pl[1]) }));
  expect(outbound()).not.toContain("parent question");
});

test("the fork parent's inherited record revised and both re-projected: the child's old copy is withdrawn, not its own", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const { pl } = await forkMe();
  await drain(db);
  await putRaw("fpar", [pl[0], msg("user", "p0", null, "PARENT_REVISED"), pl[2]]);
  await drain(db); // parent and child both projected again
  const me = sourceIdOf("pi", "me");
  expect((await db`select count(*)::int n from lexical where text like '%parent question%'`)[0].n).toBe(0);
  // The parent's unchanged reply stays inherited; no span any view uses (summary input) holds the withdrawn question.
  expect((await db`select count(*)::int n from span s where records::text like '%parent question%'
                   and exists (select 1 from view v where v.span_ids @> array[s.id])`)[0].n).toBe(0);
  calls = [];
  const d = await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_user_ref: ref(pl[1]) }));
  expect(d.state).toBe("query-stale");
  expect(outbound()).not.toContain("parent question");
});

test("the fork parent known only in its revised version, the child imported later with the old copy: not the child's own", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const P = sourceIdOf("pi", "fpar");
  const old = [header("fpar"), msg("user", "p0", null, "parent question"), msg("assistant", "p1", "p0", "parent reply")];
  await putRaw("fpar", [old[0], msg("user", "p0", null, "PARENT_REVISED"), old[2]]);
  await drain(db);
  await putRaw("me", [header("me", { parentSession: "/x/fpar.jsonl" }), old[1], old[2], msg("user", "k1", "p1", "CHILD_OWN_go")], undefined, P);
  await drain(db);
  expect((await db`select count(*)::int n from lexical where text like '%parent question%'`)[0].n).toBe(0);
  expect((await db`select count(*)::int n from lexical where text like '%CHILD_OWN_go%'`)[0].n).toBe(1);
  calls = [];
  await ask(R("u9", "zx-7781", { prev_user: "parent question", prev_user_ref: ref(old[1]) }));
  expect(outbound()).not.toContain("parent question");
});

// ---------- re-review 2: the sent text is the referenced record's ----------

test("a valid reference paired with other text: the text is not sent", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["current question", "current reply"]]);
  await drain(db);
  calls = [];
  const d = await ask(R("u9", "zx-7781", { prev_user: "WITHDRAWN_OTHER_TEXT", prev_user_ref: ref(msg("user", "u0", null, "current question")) }));
  expect(d.state).toBe("query-stale");
  expect(outbound()).not.toContain("WITHDRAWN_OTHER_TEXT");
  expect(calls.length).toBe(0);
});

test("references of the wrong role, or an assistant reply not following the referenced user message: refused", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["first", "reply one"], ["second", "reply two"]]);
  await drain(db);
  const u0 = msg("user", "u0", null, "first"), a0 = msg("assistant", "a0", "u0", "reply one"), a1 = msg("assistant", "a1", "u1", "reply two");
  expect((await ask(R("u9", "zx-7781", { prev_user: "reply one", prev_user_ref: ref(a0) }))).state).toBe("query-stale");
  expect((await ask(R("u8", "zx-7781", { prev_user: "first", prev_user_ref: ref(u0), prev_assistant: "reply two", prev_assistant_ref: ref(a1) }))).state).toBe("query-stale");
  // An earlier exchange of the same branch is not the previous one; the most recent is.
  expect((await ask(R("u7", "zx-7781", { prev_user: "first", prev_user_ref: ref(u0), prev_assistant: "reply one", prev_assistant_ref: ref(a0) }))).state).toBe("query-stale");
  expect((await ask(R("u7", "zx-7781", { prev_assistant: "reply one", prev_assistant_ref: ref(a0) }))).state).toBe("query-stale");
  const u1 = msg("user", "u1", "a0", "second");
  calls = [];
  expect((await ask(R("u6", "zx-7781", { prev_user: "second", prev_user_ref: ref(u1), prev_assistant: "reply two", prev_assistant_ref: ref(a1) }))).state).toBe("shown");
  expect(outbound()).toContain("second");
  expect(outbound()).toContain("reply two");
});

// ---------- review round 3: the previous exchange is the querying branch's most recent ----------

test("a previous exchange from another branch than the request's leaf: refused, one reference or two", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  // Branch L: u0 → a0 → u1 → a1; branch R: u0 → a0 → u2 → a2.
  const L = [header("me"), msg("user", "u0", null, "root"), msg("assistant", "a0", "u0", "root reply"),
    msg("user", "u1", "a0", "OFF_BRANCH_QUESTION"), msg("assistant", "a1", "u1", "OFF_BRANCH_REPLY"),
    msg("user", "u2", "a0", "right question"), msg("assistant", "a2", "u2", "right reply")];
  await putRaw("me", L, "a2");
  await drain(db);
  calls = [];
  for (const x of [
    { prev_user: "OFF_BRANCH_QUESTION", prev_user_ref: ref(L[3]), prev_assistant: "OFF_BRANCH_REPLY", prev_assistant_ref: ref(L[4]) },
    { prev_user: "OFF_BRANCH_QUESTION", prev_user_ref: ref(L[3]) },
    { prev_assistant: "OFF_BRANCH_REPLY", prev_assistant_ref: ref(L[4]) },
  ])
    expect((await ask(R("u9" + Object.keys(x).length, "zx-7781", { leaf: "a2", ...x }))).state).toBe("query-stale");
  expect(outbound()).not.toContain("OFF_BRANCH");
  // The leaf's own most recent exchange is used.
  const d = await ask(R("u8", "zx-7781", { leaf: "a2", prev_user: "right question", prev_user_ref: ref(L[5]), prev_assistant: "right reply", prev_assistant_ref: ref(L[6]) }));
  expect(d.state).toBe("shown");
  expect(outbound()).toContain("right reply");
  // A leaf the projection does not know: no branch, nothing sent.
  calls = [];
  expect((await ask(R("u7", "zx-7781", { leaf: "nope", prev_user: "right question", prev_user_ref: ref(L[5]) }))).state).toBe("query-stale");
  expect(calls.length).toBe(0);
});

test("the current input already synced: the previous exchange is the one before it, not the input itself", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await put("me", [["first", "reply one"], ["zx-7781 now", "pending"]]);
  await drain(db);
  const u0 = msg("user", "u0", null, "first"), a0 = msg("assistant", "a0", "u0", "reply one");
  expect((await ask(R("u1", "zx-7781 now", { prev_user: "first", prev_user_ref: ref(u0), prev_assistant: "reply one", prev_assistant_ref: ref(a0) }))).state).toBe("shown");
});

test("a Codex session's previous exchange with valid references: sent", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  const cid = "22222222-2222-2222-2222-222222222222";
  const cm = (ordinal: number, role: string, text: string) =>
    JSON.stringify({ timestamp: T, ordinal, type: "response_item", payload: { type: "message", role, content: [{ type: role === "assistant" ? "output_text" : "input_text", text }] } });
  const lines = [JSON.stringify({ timestamp: T, ordinal: 0, type: "session_meta", payload: { id: cid, history_mode: "paginated", source: "cli", cwd: "/x" } }),
    cm(1, "user", "CODEX_PREV_QUESTION"), cm(2, "assistant", "CODEX_PREV_REPLY")];
  const decl = { source: sourceIdOf("codex", cid), harness: "codex" as const, nativeId: cid, scope: "personal", parent: null };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  expect((await commit(db, { leaseId: l, commitId: `r${n++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: String(pos), hash: sha(raw), raw })) })).ok).toBe(true);
  await drain(db);
  calls = [];
  const cref = (raw: string) => ({ rec: String(JSON.parse(raw).ordinal), hash: sha(raw) });
  const d = await ask({ ...R("9", "zx-7781"), source: decl.source, prev_user: "CODEX_PREV_QUESTION", prev_user_ref: cref(lines[1]), prev_assistant: "CODEX_PREV_REPLY", prev_assistant_ref: cref(lines[2]) });
  expect(d.state).toBe("shown");
  expect(outbound()).toContain("CODEX_PREV_QUESTION");
  expect(outbound()).toContain("CODEX_PREV_REPLY");
});
