// Per-turn recall: frozen decisions (DESIGN §6.2) over real Postgres and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { EMBED_DIM, revokeGrant } from "./derive.ts";
import { Guarded } from "./http.ts";
import { recall, blocks, recallHooks, tokens, TOKEN_BUDGET, FRAMING, queryOf, clip } from "./recall.ts";
import { zoom } from "./search.ts";
import { forgetSource } from "./forget.ts";
import { drain } from "./worker.ts";
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let calls: { path: string; body: any }[] = [];
let rerankDelay = 0;
let embedDelay = 0;
let rerankStatus = 200;
let score = (d: string): number => (d.includes("ANSWER") ? 0.9 : 0.1);
let srv: ReturnType<typeof Bun.serve>;
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
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
const header = (id: string) => JSON.stringify({ type: "session", version: 3, id, timestamp: T, cwd: "/x" });
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
const me = sourceIdOf("pi", "me");
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
  expect((await ask(R("u1", "zx-7781"))).state).toBe("empty");
  rerankStatus = 200;
  await db`delete from grant_rule where purpose = 'recall' and dest = 'rerank'`;
  expect((await ask(R("u2", "zx-7781"))).state).toBe("empty");
});

test("a non-finite score or one under τ is not injected", async () => {
  await put("a", [["q", "ANSWER zx-7781"]]);
  await drain(db);
  score = () => 0.49;
  expect((await ask(R("u1", "zx-7781"))).state).toBe("empty");
  score = () => NaN; // JSON turns it into null: the rerank result is incomplete
  expect((await ask(R("u2", "zx-7781"))).state).toBe("empty");
});

test("at most 3 hint lines, block within the token budget framing included; each handle opens with mem_zoom", async () => {
  for (let i = 0; i < 6; i++) await put("s" + i, [[`ANSWER topic zx-7781 number ${i} ${"长".repeat(150)}`, `reply ${i}`]]);
  await drain(db);
  const d = await ask(R("u1", "zx-7781"));
  expect(d.state).toBe("shown");
  expect(d.text.startsWith(FRAMING)).toBe(true);
  const lines = d.text.split("\n").filter((l: string) => /^\[(span|record):/.test(l));
  expect(lines.length).toBe(d.text.split("\n").length - 3); // framing (2 lines) and the end tag, the rest hints
  expect(lines.length).toBeGreaterThan(0);
  expect(lines.length).toBeLessThanOrEqual(3);
  expect(tokens(d.text)).toBeLessThanOrEqual(TOKEN_BUDGET());
  for (const l of lines) {
    const id = /^\[([^\]]+)\]/.exec(l)![1];
    const z = (await zoom(db, { source: me, scope: "personal", purpose: "tool", id })).body as any;
    expect(z).toBeTruthy();
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
  expect(d.text).not.toContain(`[${rec.key}]`);
});

test("lines that would pass the token budget are left out (budget over the whole block)", async () => {
  // Three passing hints of ~240 bytes of CJK each (~160 tokens): with the framing, over 800 only past ~4.
  for (let i = 0; i < 3; i++) await put("s" + i, [[`ANSWER zx-7781 ${i} ${"长".repeat(150)}`, `reply ${i}`]]);
  await drain(db);
  process.env.MEMEX_RECALL_TOKENS = "300"; // a small budget: room for the framing and one line only
  try {
    const d = await ask(R("u1", "zx-7781"));
    expect(tokens(d.text)).toBeLessThanOrEqual(300);
    expect(d.text.split("\n").filter((l: string) => /^\[(span|record):/.test(l)).length).toBe(1);
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
  const [g0] = await db`select gen from recall_state where source = ${me}`;
  await forgetSource(db, a);
  const [g1] = await db`select gen from recall_state where source = ${me}`;
  expect(Number(g1.gen)).toBeGreaterThan(Number(g0.gen));
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
