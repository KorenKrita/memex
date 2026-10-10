// Pi plugin: its handlers driven by a fake host over real session files, the real HTTP server and Postgres,
// and a fake model endpoint. The host is faked; plugins/pi/e2e.test.ts runs the real `pi`.
import { afterAll, afterEach, beforeAll, beforeEach, expect, test } from "bun:test";
import { appendFileSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "../../server/db.ts";
import { EMBED_DIM } from "../../server/derive.ts";
import { addApiKey, serve } from "../../server/http.ts";
import { drain as drainAll, handlers } from "../../server/worker.ts";
import { sourceIdOf } from "../../client/records.ts";
import memex from "./index.ts";

const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const drain = (db: Db) => drainAll(db, KINDS);

let db: Db;
let model: ReturnType<typeof Bun.serve>;
let api: { port: number; stop: () => Promise<void> };
let dir: string;
let calls: { path: string; body: any }[] = [];
// Relevant: a document naming zx-7781, for an input (the query's first line) asking about the port.
const score = (q: string, d: string) => (d.includes("zx-7781") && q.split("\n")[0].includes("port") ? 0.9 : 0.1);

beforeAll(async () => {
  db = connect();
  await migrate(db);
  model = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      calls.push({ path, body });
      if (path.endsWith("/rerank")) return Response.json({ data: body.documents.map((d: string, index: number) => ({ index, relevance_score: score(body.query, d) })) });
      if (path.endsWith("/chat/completions")) {
        const sys = body.messages[0].content as string;
        if (sys.startsWith("You write the card")) return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "Port work", overview: "card about zx-7781", results: [], open_threads: [] }) } }] });
        if (sys.startsWith("You merge")) return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "merged" }) } }] });
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "port zx-7781", user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] }) } }] });
      }
      return Response.json({ data: body.input.map(() => ({ embedding: Array.from({ length: EMBED_DIM }, (_, i) => (i === 0 ? 1 : 0)) })) });
    },
  });
  const base = `http://127.0.0.1:${model.port}/v1`;
  Object.assign(process.env, {
    MEMEX_LLM_URL: base, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k",
    MEMEX_EMBED_URL: base, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
    MEMEX_RERANK_URL: base + "/rerank", MEMEX_RERANK_MODEL: "fake-rerank", MEMEX_RERANK_KEY: "k",
  });
  api = await serve(db, { port: 0 });
});
afterAll(async () => {
  await api.stop();
  model.stop(true);
  await db.close();
  for (const k of ["MEMEX_URL", "MEMEX_KEY", "MEMEX_POLICY", "MEMEX_RECALL_TAU"]) delete process.env[k];
});

const ALL = { sync: ["memex"], snapshot: ["memex"], recall: ["memex", "embedding", "rerank"], tool: ["memex", "embedding", "rerank"] };
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_round, scan_item, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, search_trace,
                   forest_node, forest_access, session_card, start_snapshot, snapshot_refresh, recall_state, recall_block restart identity cascade`);
  for (const p of ["sync", "derive", "snapshot", "recall", "tool"])
    for (const d of ["memex", "llm", "embedding", "rerank"]) await db`insert into grant_rule values ('personal', ${p}, ${d})`;
  dir = mkdtempSync(join(tmpdir(), "memex-pi-"));
  mkdirSync(join(dir, "sessions"));
  policy({ personal: ALL });
  Object.assign(process.env, { MEMEX_URL: `http://127.0.0.1:${api.port}`, MEMEX_KEY: await addApiKey(db, "t"), MEMEX_IDLE_CLOSE_MS: "0", MEMEX_RECALL_TAU: "0.5", MEMEX_SCAN_DELAY_MS: "-1" });
  calls = [];
});
function policy(allow: Record<string, object>) {
  const scopes = Object.entries(allow).map(([name, a]) => ({ name, paths: [join(dir, name === "personal" ? "sessions" : name)], allow: a }));
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes }));
  process.env.MEMEX_POLICY = join(dir, "policy.json");
}

// ---------- a fake host: a session file the plugin reads, the session manager over it ----------
let clock = Date.parse("2026-01-01T00:00:00Z");
type Entry = { type: string; id: string; parentId: string | null; timestamp: string; message?: any };
const hosts: { emit: (ev: string, e?: any) => Promise<any> }[] = [];
function host(native: string) {
  const file = join(dir, "sessions", `${native}.jsonl`);
  writeFileSync(file, JSON.stringify({ type: "session", version: 3, id: native, timestamp: new Date(clock).toISOString(), cwd: dir }) + "\n");
  const entries: Entry[] = [];
  let leaf: string | null = null;
  const on = new Map<string, ((e: any, ctx: any) => any)[]>();
  const tools = new Map<string, any>();
  const commands = new Map<string, any>();
  const notes: string[] = [];
  const byId = (id: string | null) => entries.find((e) => e.id === id);
  const branch = () => {
    const out: Entry[] = [];
    for (let e = byId(leaf); e; e = byId(e.parentId)) out.unshift(e);
    return out;
  };
  const ctx = {
    cwd: dir,
    ui: { notify: (m: string) => notes.push(m), setStatus: () => {} },
    sessionManager: {
      getSessionFile: () => file, getSessionId: () => native, getLeafId: () => leaf, getEntry: byId,
      getBranch: branch, buildContextEntries: branch,
    },
  };
  const pi: any = {
    on: (ev: string, f: any) => on.set(ev, [...(on.get(ev) ?? []), f]),
    registerTool: (t: any) => tools.set(t.name, t),
    registerCommand: (n: string, c: any) => commands.set(n, c),
  };
  memex(pi);
  const emit = async (ev: string, e: any = {}) => {
    let r: any;
    for (const f of on.get(ev) ?? []) r = (await f({ type: ev, ...e }, ctx)) ?? r;
    return r;
  };
  const add = (message: any, persist = true) => {
    const e: Entry = { type: "message", id: `e${entries.length}`, parentId: leaf, timestamp: new Date(clock).toISOString(), message: { ...message, timestamp: clock } };
    clock += 1000;
    entries.push(e);
    leaf = e.id;
    if (persist) appendFileSync(file, JSON.stringify(e) + "\n");
    return e;
  };
  const text = (t: string) => [{ type: "text", text: t }];
  hosts.push({ emit });
  void emit("session_start", { reason: "startup" });
  return {
    file, ctx, tools, commands, notes, emit, entries,
    setLeaf: (id: string | null) => (leaf = id),
    source: sourceIdOf("pi", native),
    /** One run: input → before_agent_start → user entry → context → reply → turn_end. Returns the system
     *  prompt sections and the messages sent to the model. */
    async run(input: string, reply: string, o: { source?: string } = {}) {
      await emit("input", { text: input, source: o.source ?? "interactive" });
      const sections: Record<string, string> = {};
      await emit("before_agent_start", { prompt: input, systemPrompt: "", systemPromptOptions: { sections } });
      const u = add({ role: "user", content: text(input) }, false); // in memory first: the file has it at turn end
      const messages = branch().map((e) => e.message);
      const r = await emit("context", { messages });
      add({ role: "assistant", content: text(reply) }, false);
      appendFileSync(file, branch().slice(-2).map((e) => JSON.stringify(e)).join("\n") + "\n");
      await emit("turn_end", { messageEntryId: leaf });
      return { sections, sent: r?.messages ?? messages, user: u };
    },
  };
}
afterEach(async () => {
  for (const h of hosts.splice(0)) await h.emit("session_shutdown", {});
});
const flush = async (h: ReturnType<typeof host>) => {
  await h.emit("session_shutdown", {});
  await drain(db);
};
const sentText = (msgs: any[]) => msgs.map((m) => (Array.isArray(m.content) ? m.content.map((c: any) => c.text).join("") : m.content)).join("\n");

// ---------- sync ----------

test("a turn syncs the session; nothing at all leaves an unauthorized session", async () => {
  const h = host("s1");
  await h.run("hello", "hi");
  await flush(h);
  expect((await db`select count(*)::int n from record where source = ${h.source}`)[0].n).toBe(3); // header + 2

  policy({ other: ALL }); // the session file is outside every scope
  const reqs: string[] = [];
  const orig = globalThis.fetch;
  globalThis.fetch = (async (u: any, i: any) => (reqs.push(String(u)), orig(u, i))) as any;
  try {
    const h2 = host("s2");
    const r = await h2.run("secret zx-7781", "reply");
    await flush(h2);
    await h2.tools.get("mem_search").execute("c", { query: "zx" }, undefined, undefined, h2.ctx);
    expect(r.sections).toEqual({});
    expect(reqs).toEqual([]);
  } finally {
    globalThis.fetch = orig;
  }
  expect((await db`select count(*)::int n from source where id = ${sourceIdOf("pi", "s2")}`)[0].n).toBe(0);
});

test("no server configured: the plugin does nothing and says so", async () => {
  delete process.env.MEMEX_URL;
  const h = host("s3");
  const r = await h.run("hello", "hi");
  expect(r.sections).toEqual({});
  await h.commands.get("memex").handler("", h.ctx);
  expect(h.notes.at(-1)).toContain("not configured");
  const t = await h.tools.get("mem_grep").execute("c", { pattern: "x" }, undefined, undefined, h.ctx);
  expect(t.content[0].text).toContain("not configured");
});

// ---------- start-of-session snapshot ----------

test("the snapshot is asked once and the same text set on every run", async () => {
  const a = host("old");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  await drain(db);
  const h = host("new");
  const r1 = await h.run("first", "one");
  expect(r1.sections.memex).toContain("[memex]");
  await flush(h);
  await drain(db); // the snapshot is stored: newer history does not change it
  const r2 = await h.run("second", "two");
  expect(r2.sections.memex).toBe(r1.sections.memex);
  expect((await db`select count(*)::int n from start_snapshot where source = ${h.source}`)[0].n).toBe(1);
});

// ---------- per-turn recall ----------

test("recall: a hint is attached to the latest user message, then replayed frozen on the same message", async () => {
  const a = host("past");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now");
  const r1 = await h.run("the port zx-7781?", "ok");
  const last = r1.sent.at(-1);
  expect(last.role).toBe("user");
  expect(sentText([last])).toContain("<memex-recall>");
  expect(sentText([last])).toContain("[hint:");
  await flush(h);
  const r2 = await h.run("thanks a lot", "bye");
  // The first user message keeps its block; the new one (no hit) has none.
  const users = r2.sent.filter((m: any) => m.role === "user");
  expect(sentText([users[0]])).toBe(sentText([last]));
  expect(sentText([users[1]])).not.toContain("memex-recall");
  expect((await db`select count(*)::int n from recall_block where source = ${h.source}`)[0].n).toBe(2);
});

test("recall carries the previous exchange as references the server accepts", async () => {
  const a = host("past2");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now2");
  await h.run("tell me about deploys", "deploys use zx-7781 ports");
  await flush(h);
  calls = [];
  await h.run("and that port?", "ok");
  const [row] = await db`select state, query_hash from recall_block where source = ${h.source} and user_rec = 'e2'`;
  expect(row.state).toBe("shown"); // not query-stale: the references matched
  const q = calls.find((c) => c.path.endsWith("/embeddings"))!.body.input[0] as string;
  expect(q).toContain("previous user: tell me about deploys");
  expect(q).toContain("previous reply: deploys use zx-7781 ports");
});

test("an extension's input, a /command and an ambiguous timestamp are not recalled", async () => {
  const a = host("past3");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now3");
  const r = await h.run("the port zx-7781?", "ok", { source: "extension" });
  expect(sentText(r.sent)).not.toContain("memex-recall");
  await h.run("/memex", "ok");
  expect((await db`select count(*)::int n from recall_block where source = ${h.source}`)[0].n).toBe(0);
  // Two context user messages with one timestamp: neither is tied to an entry.
  await h.emit("input", { text: "zx-7781 again", source: "interactive" });
  const msgs = [{ role: "user", content: [{ type: "text", text: "a zx-7781" }], timestamp: 5 }, { role: "user", content: [{ type: "text", text: "b zx-7781" }], timestamp: 5 }];
  const out = await h.emit("context", { messages: msgs });
  expect(out).toBeUndefined();
});

test("recall not ready (τ unset): nothing injected, nothing stored", async () => {
  delete process.env.MEMEX_RECALL_TAU;
  const a = host("past4");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now4");
  const r = await h.run("the port zx-7781?", "ok");
  expect(sentText(r.sent)).not.toContain("memex-recall");
  await h.commands.get("memex").handler("", h.ctx);
  expect(h.notes.at(-1)).toContain("not-ready");
});

test("server down: past blocks are left out, the run goes on", async () => {
  const a = host("past5");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now5");
  await h.run("the port zx-7781?", "ok");
  await flush(h);
  process.env.MEMEX_URL = "http://127.0.0.1:1"; // read at load: a new host instance sees it
  const h2 = host("now5"); // same session reloaded (same file), server unreachable
  const r = await h2.run("more", "ok");
  expect(sentText(r.sent)).not.toContain("memex-recall");
});

// ---------- tools ----------

test("tools: search, zoom a hit, grep; identity from the session, not the model", async () => {
  const a = host("past6");
  await a.run("where is the port", "ANSWER the port is zx-7781");
  await flush(a);
  const h = host("now6");
  await h.run("hi", "hello");
  await flush(h);
  const call = async (n: string, p: object) => JSON.parse((await h.tools.get(n).execute("c", p, undefined, undefined, h.ctx)).content[0].text);
  const s = await call("mem_search", { query: "zx-7781" });
  expect(s.results.length).toBeGreaterThan(0);
  const z = await call("mem_zoom", { id: s.results[0].id, view_key: s.results[0].view_key });
  expect(JSON.stringify(z)).toContain("zx-7781");
  const g = await call("mem_grep", { pattern: "zx-7781" });
  expect(g.hits.length).toBeGreaterThan(0);
  const zg = await call("mem_zoom", { id: g.hits[0].record, in: g.hits[0].in, version: g.hits[0].version });
  expect(JSON.stringify(zg)).toContain("zx-7781");
  // A tool-only revoke on the client: no request.
  policy({ personal: { ...ALL, tool: [] } });
  const h2 = host("now6b");
  const t = await h2.tools.get("mem_search").execute("c", { query: "zx" }, undefined, undefined, h2.ctx);
  expect(t.content[0].text).toContain("not authorized");
});

test("a session file not written yet, under a symlinked directory, is in its scope (Pi writes the file after the first reply)", async () => {
  const { symlinkSync } = await import("node:fs");
  const { loadPolicy, scopeOf } = await import("../../client/policy.ts");
  symlinkSync(join(dir, "sessions"), join(dir, "link"));
  const p = loadPolicy(); // scope "personal": paths [<dir>/sessions]
  expect(scopeOf(p, join(dir, "link", "not-yet.jsonl"))).toBe("personal");
  expect(scopeOf(p, join(dir, "link", "sub", "not-yet.jsonl"))).toBe("personal");
  expect(scopeOf(p, join(dir, "elsewhere", "x.jsonl"))).toBe("unauthorized");
});
