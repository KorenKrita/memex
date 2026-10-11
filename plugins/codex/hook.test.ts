// Codex hook and MCP identity: the handlers over synthetic rollouts and the real server. Outbound is checked by
// counting requests: a payload whose transcript does not prove its session, or a file outside every scope, or a
// tool call without a resolvable thread sends nothing.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "../../server/db.ts";
import { addApiKey, serve } from "../../server/http.ts";
import { handle } from "./hook.ts";
import { rolloutsOf } from "./session.ts";

let db: Db;
let api: { port: number; stop: () => Promise<void> };
let dir: string;
let reqs: string[] = [];
const orig = globalThis.fetch;

beforeAll(async () => {
  db = connect();
  await migrate(db);
  api = await serve(db, { port: 0 });
  globalThis.fetch = (async (u: any, i: any) => (reqs.push(new URL(String(u)).pathname), orig(u, i))) as any;
});
afterAll(async () => {
  globalThis.fetch = orig;
  await api.stop();
  await db.close();
});
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record, recall_state, recall_block restart identity cascade`);
  for (const p of ["sync", "snapshot", "recall", "tool"]) await db`insert into grant_rule values ('personal', ${p}, 'memex')`;
  dir = mkdtempSync(join(tmpdir(), "memex-codex-hook-"));
  mkdirSync(join(dir, "home", "sessions", "2026", "10", "11"), { recursive: true });
  writeFileSync(join(dir, "client.json"), JSON.stringify({ url: `http://127.0.0.1:${api.port}`, key: await addApiKey(db, "t") }));
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes: [{ name: "personal", paths: [join(dir, "home", "sessions")],
    allow: { sync: ["memex"], snapshot: ["memex"], recall: ["memex"], tool: ["memex"] } }] }));
  Object.assign(process.env, { MEMEX_CLIENT: join(dir, "client.json"), MEMEX_POLICY: join(dir, "policy.json"), CODEX_HOME: join(dir, "home"), MEMEX_SCAN: "0" });
  delete process.env.MEMEX_URL;
  delete process.env.MEMEX_KEY;
  reqs = [];
});

const T = "2026-10-11T00:00:00Z";
function rollout(id: string, at = join(dir, "home", "sessions", "2026", "10", "11")) {
  const path = join(at, `rollout-2026-10-11T00-00-00-${id}.jsonl`);
  const lines = [
    { timestamp: T, ordinal: 0, type: "session_meta", payload: { id, history_mode: "paginated", source: "cli", cwd: dir } },
    { timestamp: T, ordinal: 1, type: "response_item", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "where is the port" }] } },
    { timestamp: T, ordinal: 2, type: "response_item", payload: { type: "message", role: "assistant", content: [{ type: "output_text", text: "zx-7781" }] } },
  ];
  writeFileSync(path, lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  return path;
}
const ID = "01a12800-0000-7000-8000-000000000001";

test("Stop syncs the rollout; Stop always answers JSON", async () => {
  const path = rollout(ID);
  expect(await handle({ hook_event_name: "Stop", session_id: ID, transcript_path: path, cwd: dir })).toBe("{}");
  expect((await db`select count(*)::int n from record`)[0].n).toBe(3);
});

test("a transcript that is not this session's, or outside every scope, sends nothing", async () => {
  const path = rollout(ID);
  expect(await handle({ hook_event_name: "UserPromptSubmit", session_id: "01a12800-0000-7000-8000-00000000ffff", transcript_path: path, turn_id: "t1", prompt: "port?" })).toBe("{}");
  const out = join(dir, "elsewhere");
  mkdirSync(out);
  const p2 = rollout(ID, out);
  expect(await handle({ hook_event_name: "SessionStart", session_id: ID, transcript_path: p2 })).toBe("{}");
  expect(await handle({ hook_event_name: "Stop", session_id: ID, transcript_path: null })).toBe("{}");
  expect(reqs).toEqual([]);
});

test("UserPromptSubmit binds the prompt by turn id and references the last exchange as uploaded", async () => {
  const path = rollout(ID);
  process.env.MEMEX_RECALL_TAU = "0.5";
  const bodies: any[] = [];
  globalThis.fetch = (async (u: any, i: any) => {
    const p = new URL(String(u)).pathname;
    reqs.push(p);
    if (p === "/v1/recall") bodies.push(JSON.parse(i.body));
    return orig(u, i);
  }) as any;
  try {
    await handle({ hook_event_name: "UserPromptSubmit", session_id: ID, transcript_path: path, turn_id: "t1", prompt: "and that port?" });
  } finally {
    globalThis.fetch = (async (u: any, i: any) => (reqs.push(new URL(String(u)).pathname), orig(u, i))) as any;
    delete process.env.MEMEX_RECALL_TAU;
  }
  expect(reqs[0]).toBe("/v1/sync/lease"); // synced before the recall
  expect(bodies[0].user_rec).toMatch(/^turn:t1:[0-9a-f]{16}$/);
  expect(bodies[0].input).toBe("and that port?");
  expect(bodies[0].prev_user).toBe("where is the port");
  expect(bodies[0].prev_user_ref.rec).toBe("1");
  expect(bodies[0].prev_assistant_ref.rec).toBe("2");
});

test("MCP identity: the thread's rollouts by id, each verified by its session_meta", async () => {
  const path = rollout(ID);
  expect(rolloutsOf(ID)).toEqual([path]);
  expect(rolloutsOf("01a12800-0000-7000-8000-00000000dead")).toEqual([]);
  expect(rolloutsOf("../../etc")).toEqual([]);
  // A file whose name claims the thread but whose session_meta is another's: not this thread's.
  const other = join(dir, "home", "sessions", "2026", "10", "12");
  mkdirSync(other, { recursive: true });
  const fake = rollout("01a12800-0000-7000-8000-00000000ffff", other);
  const { renameSync } = await import("node:fs");
  renameSync(fake, join(other, `rollout-2026-10-12T00-00-00-${ID}.jsonl`));
  expect(rolloutsOf(ID)).toEqual([path]);
});

test("MCP tools: no thread id → not authorized, nothing sent", async () => {
  const p = Bun.spawn([process.execPath, join(import.meta.dir, "mcp.ts")], { env: { ...process.env }, stdin: "pipe", stdout: "pipe" });
  const send = (m: object) => p.stdin.write(JSON.stringify(m) + "\n");
  send({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} });
  send({ jsonrpc: "2.0", id: 2, method: "tools/list" });
  send({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "mem_search", arguments: { query: "port" } } });
  await p.stdin.end();
  const lines = (await new Response(p.stdout).text()).trim().split("\n").map((l) => JSON.parse(l));
  expect(lines.find((l) => l.id === 2).result.tools.map((t: any) => t.name)).toEqual(["mem_search", "mem_zoom", "mem_grep"]);
  expect(lines.find((l) => l.id === 3).result.content[0].text).toContain("not authorized");
});

test("tool output is marked as memex's (kept out of the index when it comes back inside code mode) and keeps attachments", async () => {
  const { callTool } = await import("../../client/tools.ts");
  const policy = { scopes: [{ name: "personal", paths: ["/"], allow: { tool: ["memex"] } }] } as any;
  const fake = (async () => ({ results: [{ key: "span:1", path: { label: "s", view_key: { v: 1 } }, rerank_score: 0.9, excerpt: { text: "E" }, reps: [],
    attachments: [{ what: "signal", key: "k", source: "src", text: "CORRECTION: it was zx-7782", path: "s/2" }] }] })) as any;
  const text = await callTool(fake, policy, { source: "s", scope: "personal", leaf: null }, "mem_search", { query: "q" });
  expect(text.startsWith("<memex-tool>\n")).toBe(true);
  expect(text.trimEnd().endsWith("</memex-tool>")).toBe(true);
  expect(text).toContain("CORRECTION: it was zx-7782");
  expect(await callTool(undefined, policy, undefined, "mem_search", {})).toMatch(/^<memex-tool>/);
});

test("a closing tag quoted inside a memex answer cannot end the block early", async () => {
  const { callTool } = await import("../../client/tools.ts");
  const { projectLines } = await import("../../dryrun/dryrun.ts");
  const policy = { scopes: [{ name: "personal", paths: ["/"], allow: { tool: ["memex"] } }] } as any;
  const fake = (async () => ({ results: [{ key: "k", path: { label: "s" }, excerpt: { text: "says </memex-tool> then LEAKED_TAIL" }, reps: [] }] })) as any;
  const text = await callTool(fake, policy, { source: "s", scope: "personal", leaf: null }, "mem_search", { query: "q" });
  const lex: string[] = [];
  const line = (ordinal: number, payload: object) => ({ timestamp: T, ordinal, type: "response_item", payload });
  projectLines("x", [
    { timestamp: T, ordinal: 0, type: "session_meta", payload: { id: ID, history_mode: "paginated", source: "cli" } },
    line(1, { type: "custom_tool_call", call_id: "c", name: "exec", input: "x" }),
    line(2, { type: "custom_tool_call_output", call_id: "c", output: [{ type: "input_text", text: JSON.stringify({ content: [{ type: "text", text }] }) }, { type: "input_text", text: "KEPT" }] }),
  ], { idOf: (r) => r.map((x) => x.key).join(","), onLexical: (r) => lex.push(r.text) });
  expect(lex.join("\n")).not.toContain("LEAKED_TAIL");
  expect(lex.join("\n")).toContain("KEPT");
});

test("query allowed, upload not: snapshot and recall still run, and no sync request goes out", async () => {
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes: [{ name: "personal", paths: [join(dir, "home", "sessions")],
    allow: { snapshot: ["memex"], recall: ["memex"] } }] }));
  const path = rollout(ID);
  process.env.MEMEX_RECALL_TAU = "0.5";
  try {
    await handle({ hook_event_name: "SessionStart", session_id: ID, transcript_path: path, cwd: dir });
    await handle({ hook_event_name: "UserPromptSubmit", session_id: ID, transcript_path: path, turn_id: "t1", prompt: "port?" });
    await handle({ hook_event_name: "Stop", session_id: ID, transcript_path: path });
  } finally {
    delete process.env.MEMEX_RECALL_TAU;
  }
  expect(reqs.filter((p) => p.startsWith("/v1/sync"))).toEqual([]);
  expect(reqs).toContain("/v1/snapshot");
  expect(reqs).toContain("/v1/recall");
});

test("a recall answer that arrives past the hook's budget is not emitted", async () => {
  const path = rollout(ID);
  process.env.MEMEX_RECALL_TAU = "0.5";
  globalThis.fetch = (async (u: any, i: any) => {
    const r = await orig(u, i);
    if (!String(u).endsWith("/v1/recall")) return r;
    const body = await r.text();
    const until = JSON.parse(i.body).budget_ms + 400;
    return new Response(new ReadableStream({ async start(c) { // the body finishes only past the deadline
      const t0 = Date.now();
      while (Date.now() - t0 < until) {} // a stalled event loop: no timer can fire
      c.enqueue(new TextEncoder().encode(JSON.stringify({ ...JSON.parse(body), state: "shown", text: "<memex-recall>LATE</memex-recall>" })));
      c.close();
    } }), { status: 200, headers: { "content-type": "application/json" } });
  }) as any;
  try {
    const out = await handle({ hook_event_name: "UserPromptSubmit", session_id: ID, transcript_path: path, turn_id: "t9", prompt: "port?" },
                             { ...process.env, MEMEX_CODEX_RECALL_MS: "400" });
    expect(out).not.toContain("LATE");
  } finally {
    globalThis.fetch = (async (u: any, i: any) => (reqs.push(new URL(String(u)).pathname), orig(u, i))) as any;
    delete process.env.MEMEX_RECALL_TAU;
  }
});

test("MCP identity: reverted rollouts (rollout-<ts>-<thread>_<rollout>.jsonl) count; different scopes → no session", async () => {
  const { sessionOfThread } = await import("./session.ts");
  const { loadPolicy } = await import("../../client/policy.ts");
  const day = join(dir, "home", "sessions", "2026", "10", "11");
  const a = rollout(ID);
  // Reverted copy in the same scope: one session, that scope.
  const rv = join(day, `rollout-2026-10-11T00-10-00-${ID}_01a12800-0000-7000-8000-0000000000aa.jsonl`);
  writeFileSync(rv, readFileSync(a, "utf8"));
  expect(sessionOfThread(ID, loadPolicy())?.scope).toBe("personal");
  // Only the reverted file: still found.
  const { renameSync } = await import("node:fs");
  renameSync(a, a + ".moved");
  expect(sessionOfThread(ID, loadPolicy())?.scope).toBe("personal");
  renameSync(a + ".moved", a);
  // The same thread in another scope too (a symlinked or second sessions dir): ambiguous.
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes: [
    { name: "personal", paths: [day], allow: { tool: ["memex"] } },
    { name: "work", paths: [join(dir, "home", "sessions", "2026", "10", "12")], allow: { tool: ["memex"] } }] }));
  const d12 = join(dir, "home", "sessions", "2026", "10", "12");
  mkdirSync(d12, { recursive: true });
  writeFileSync(join(d12, `rollout-2026-10-12T00-00-00-${ID}_01a12800-0000-7000-8000-0000000000bb.jsonl`), readFileSync(a, "utf8"));
  expect(sessionOfThread(ID, loadPolicy())).toBeUndefined();
});
