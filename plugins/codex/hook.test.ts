// Codex hook and MCP identity: the handlers over synthetic rollouts and the real server. Outbound is checked by
// counting requests: a payload whose transcript does not prove its session, or a file outside every scope, or a
// tool call without a resolvable thread sends nothing.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "../../server/db.ts";
import { addApiKey, serve } from "../../server/http.ts";
import { handle } from "./hook.ts";
import { rolloutOf } from "./session.ts";

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

test("MCP identity: the thread's rollout by id, verified by its session_meta; none or two → no session", async () => {
  const path = rollout(ID);
  expect(rolloutOf(ID)).toBe(path);
  expect(rolloutOf("01a12800-0000-7000-8000-00000000dead")).toBeUndefined();
  expect(rolloutOf("../../etc")).toBeUndefined();
  // A second file claiming the same id (another day): ambiguous.
  const other = join(dir, "home", "sessions", "2026", "10", "12");
  mkdirSync(other, { recursive: true });
  rollout(ID, other);
  expect(rolloutOf(ID)).toBeUndefined();
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
