// Pi plugin end to end: the real `pi -p` (isolated agent dir, a fake OpenAI-compatible model that records what
// it is sent) loading plugins/pi/index.ts, against the real HTTP server and Postgres.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "../../server/db.ts";
import { EMBED_DIM } from "../../server/derive.ts";
import { addApiKey, serve } from "../../server/http.ts";
import { drain as drainAll, handlers } from "../../server/worker.ts";

const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const PI = Bun.which("pi");

let db: Db;
let model: ReturnType<typeof Bun.serve>;
let api: { port: number; stop: () => Promise<void> };
let dir: string;
let env: Record<string, string>;
const chats: any[] = []; // what the host model was sent
let script: (body: any) => { text?: string; tool?: { name: string; args: object } } = () => ({ text: "ok" });

const sse = (chunks: object[]) =>
  new Response(chunks.map((c) => `data: ${JSON.stringify(c)}\n\n`).join("") + "data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } });

beforeAll(async () => {
  db = connect();
  await migrate(db);
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_round, scan_item, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, search_trace,
                   forest_node, forest_access, session_card, start_snapshot, snapshot_refresh, recall_state, recall_block restart identity cascade`);
  for (const p of ["sync", "derive", "snapshot", "recall", "tool"])
    for (const d of ["memex", "llm", "embedding", "rerank"]) await db`insert into grant_rule values ('personal', ${p}, ${d})`;
  model = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body: any = await req.json();
      if (path.startsWith("/host/")) {
        chats.push(body);
        const r = script(body);
        const base = { id: "c", object: "chat.completion.chunk", created: 0, model: "fake" };
        if (r.tool)
          return sse([{ ...base, choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: r.tool.name, arguments: JSON.stringify(r.tool.args) } }] } }] },
                      { ...base, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] }]);
        return sse([{ ...base, choices: [{ index: 0, delta: { role: "assistant", content: r.text ?? "ok" } }] },
                    { ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } }]);
      }
      // The memex server's models.
      if (path.endsWith("/rerank"))
        return Response.json({ data: body.documents.map((d: string, index: number) => ({ index, relevance_score: d.includes("zx-7781") && body.query.split("\n")[0].includes("port") ? 0.9 : 0.1 })) });
      if (path.endsWith("/chat/completions")) {
        const sys = body.messages[0].content as string;
        if (sys.startsWith("You write the card")) return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "Port work", overview: "the service port is zx-7781", results: [], open_threads: [] }) } }] });
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
    MEMEX_IDLE_CLOSE_MS: "0", MEMEX_RECALL_TAU: "0.5",
  });
  api = await serve(db, { port: 0 });

  dir = mkdtempSync(join(tmpdir(), "memex-pi-e2e-"));
  const agent = join(dir, "agent");
  mkdirSync(agent);
  mkdirSync(join(dir, "sessions"));
  writeFileSync(join(agent, "models.json"), JSON.stringify({
    providers: { fake: { baseUrl: `http://127.0.0.1:${model.port}/host/v1`, api: "openai-completions", apiKey: "k", models: [{ id: "fake-model" }] } },
  }));
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes: [{ name: "personal", paths: [join(dir, "sessions")],
    allow: { sync: ["memex"], snapshot: ["memex"], recall: ["memex", "embedding", "rerank"], tool: ["memex", "embedding", "rerank"] } }] }));
  env = {
    ...(process.env as Record<string, string>), PI_CODING_AGENT_DIR: agent, MEMEX_URL: `http://127.0.0.1:${api.port}`,
    MEMEX_KEY: await addApiKey(db, "e2e"), MEMEX_POLICY: join(dir, "policy.json"), MEMEX_SCAN_DELAY_MS: "-1",
  };
});
afterAll(async () => {
  await api?.stop();
  model?.stop(true);
  await db?.close();
});

/** One print-mode run in a new (or the given) session; returns its exit code and output. */
async function pi(prompt: string, o: { session?: string; fork?: string } = {}) {
  const args = [PI!, "-ne", "-nc", "-ns", "-e", process.env.E2E_EXT ?? join(import.meta.dir, "index.ts"), ...(process.env.E2E_PROBE ? ["-e", process.env.E2E_PROBE] : []), "--provider", "fake", "--model", "fake-model",
                "--session-dir", join(dir, "sessions"), ...(o.session ? ["--session", o.session] : []), ...(o.fork ? ["--fork", o.fork] : []), "-p", prompt];
  const p = Bun.spawn(args, { cwd: dir, env, stdout: "pipe", stderr: "pipe" });
  const [out, err, code] = [await new Response(p.stdout).text(), await new Response(p.stderr).text(), await p.exited];
  return { code, out, err };
}
const textOf = (m: any) => (typeof m.content === "string" ? m.content : (m.content ?? []).map((c: any) => c.text ?? "").join(""));

test.skipIf(!PI)("real pi: sync at turn end, snapshot in the system prompt, a recall hint on the user message, mem_search works", async () => {
  // A past session, synced by the plugin.
  script = () => ({ text: "ANSWER the service port is zx-7781" });
  const r0 = await pi("where is the service port");
  expect(r0.code, r0.err).toBe(0);
  const files = readdirSync(join(dir, "sessions"));
  expect(files.length).toBe(1);
  expect((await db`select count(*)::int n from source`)[0].n).toBe(1); // synced before exit
  await drainAll(db, KINDS);

  // A new session: snapshot in the system prompt, recall on the user message, then a tool call.
  chats.length = 0;
  let step = 0;
  script = () => (step++ === 0 ? { tool: { name: "mem_search", args: { query: "service port zx-7781" } } } : { text: "done" });
  const r1 = await pi("which port did the service use?");
  expect(r1.code, r1.err).toBe(0);
  const first = chats[0];
  const system = first.messages.filter((m: any) => m.role === "system" || m.role === "developer").map(textOf).join("\n");
  expect(system).toContain("<memex>");
  expect(system).toContain("[memex] Background from earlier sessions");
  const user = first.messages.filter((m: any) => m.role === "user").map(textOf).at(-1);
  expect(user).toContain("which port did the service use?");
  expect(user).toContain("<memex-recall>");
  const toolMsg = chats[1].messages.find((m: any) => m.role === "tool");
  expect(textOf(toolMsg)).toContain("zx-7781");
  expect((await db`select count(*)::int n from source`)[0].n).toBe(2);
  expect((await db`select count(*)::int n from recall_block where state = 'shown'`)[0].n).toBe(1);
}, 120_000);

test.skipIf(!PI)("real pi: a resumed session replays its frozen block on the same message; a fork syncs as a child", async () => {
  // The session of the previous test that got a recall block: resume it, a second prompt.
  const [row] = await db`select s.native_id, b.user_rec, b.text from recall_block b join source s on s.id = b.source where b.state = 'shown'`;
  const file = readdirSync(join(dir, "sessions")).find((f) => f.includes(row.native_id))!;
  chats.length = 0;
  script = () => ({ text: "fine" });
  const r = await pi("thanks a lot", { session: join(dir, "sessions", file) });
  expect(r.code, r.err).toBe(0);
  const users = chats[0].messages.filter((m: any) => m.role === "user").map(textOf);
  expect(users[0]).toContain("which port did the service use?");
  expect(users[0]).toContain(row.text); // the frozen block, byte for byte, on its own message
  expect(users.at(-1)).not.toContain("<memex-recall>"); // no hit for this one
  const sys = (m: any[]) => m.filter((x: any) => x.role === "system" || x.role === "developer").map(textOf).join("\n");
  expect(sys(chats[0].messages)).toContain("<memex>");

  // Fork it: a new session file with a parent; the plugin syncs it with the parent link.
  const before = (await db`select count(*)::int n from source`)[0].n;
  const f = await pi("continue in a fork", { fork: join(dir, "sessions", file) });
  expect(f.code, f.err).toBe(0);
  const rows = await db`select parent_id from source`;
  expect((await db`select count(*)::int n from source`)[0].n).toBe(before + 1);
  expect(rows.some((x: any) => x.parent_id)).toBe(true);
}, 120_000);
