// Codex plugin end to end: the built plugin installed into an isolated CODEX_HOME, the real `codex exec` (and
// `exec resume`), the real memex HTTP server and Postgres, fake memex models. Opt-in:
//   MEMEX_E2E_CODEX=1 MEMEX_E2E_CODEX_KEY=<key> [MEMEX_E2E_CODEX_BASE=…/v1] [MEMEX_E2E_CODEX_MODEL=…] bun test plugins/codex/e2e.test.ts
// Hooks run with --dangerously-bypass-hook-trust (the trust review is the user's, not testable here).
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "../../server/db.ts";
import { EMBED_DIM } from "../../server/derive.ts";
import { addApiKey, serve } from "../../server/http.ts";
import { drain as drainAll, handlers } from "../../server/worker.ts";
import { build } from "./build.ts";

const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const CODEX = Bun.which("codex");
// A live integration: runs only when asked for (MEMEX_E2E_CODEX=1), against a Responses endpoint given in the
// environment (no credential defaults here).
const BASE = process.env.MEMEX_E2E_CODEX_BASE ?? "http://127.0.0.1:8317/v1";
const KEY = process.env.MEMEX_E2E_CODEX_KEY ?? "";
const MODEL = process.env.MEMEX_E2E_CODEX_MODEL ?? "gpt-6.1-sol";
const up = process.env.MEMEX_E2E_CODEX === "1" && CODEX && KEY
  ? await fetch(BASE + "/models", { headers: { authorization: `Bearer ${KEY}` }, signal: AbortSignal.timeout(2000) }).then((r) => r.ok, () => false)
  : false;

let db: Db;
let model: ReturnType<typeof Bun.serve>;
let api: { port: number; stop: () => Promise<void> };
let dir: string, home: string, cwd: string;
let env: Record<string, string>;
const embeds: string[] = [];

beforeAll(async () => {
  if (!up) return;
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
      if (path.endsWith("/rerank"))
        return Response.json({ data: body.documents.map((d: string, index: number) => ({ index, relevance_score: /zx-?7781/i.test(d) && /port/i.test(body.query.split("\n")[0]) ? 0.9 : 0.1 })) });
      if (path.endsWith("/chat/completions")) {
        const sys = body.messages[0].content as string;
        if (sys.startsWith("You write the card")) return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "Port work", overview: "the service port is zx-7781", results: [], open_threads: [] }) } }] });
        if (sys.startsWith("You merge")) return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "merged" }) } }] });
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "service port zx-7781", user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] }) } }] });
      }
      embeds.push(...body.input);
      return Response.json({ data: body.input.map(() => ({ embedding: Array.from({ length: EMBED_DIM }, (_, i) => (i === 0 ? 1 : 0)) })) });
    },
  });
  const base = `http://127.0.0.1:${model.port}/v1`;
  Object.assign(process.env, {
    MEMEX_LLM_URL: base, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k", MEMEX_EMBED_URL: base, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
    MEMEX_RERANK_URL: base + "/rerank", MEMEX_RERANK_MODEL: "fake-rerank", MEMEX_RERANK_KEY: "k", MEMEX_IDLE_CLOSE_MS: "0", MEMEX_RECALL_TAU: "0.5",
  });
  api = await serve(db, { port: 0 });

  dir = mkdtempSync(join(tmpdir(), "memex-codex-e2e-"));
  home = join(dir, "home");
  cwd = join(dir, "cwd");
  mkdirSync(home);
  mkdirSync(cwd);
  const market = join(dir, "market");
  await build(market);
  writeFileSync(join(home, "config.toml"), `model_provider = "e2e"
model = "${MODEL}"
model_reasoning_effort = "low"
approval_policy = "never"
sandbox_mode = "read-only"

[model_providers.e2e]
name = "e2e"
wire_api = "responses"
base_url = "${BASE}"
experimental_bearer_token = "${KEY}"
supports_websockets = false

[projects."${cwd}"]
trust_level = "trusted"
`);
  // The plugin's settings, as the user would write them: the server and the policy, in files (hooks do not see
  // the shell's environment).
  writeFileSync(join(dir, "client.json"), JSON.stringify({ url: `http://127.0.0.1:${api.port}`, key: await addApiKey(db, "e2e") }));
  writeFileSync(join(dir, "policy.json"), JSON.stringify({ scopes: [{ name: "personal", paths: [join(home, "sessions")],
    allow: { sync: ["memex"], snapshot: ["memex"], recall: ["memex", "embedding", "rerank"], tool: ["memex", "embedding", "rerank"] } }] }));
  env = { PATH: process.env.PATH!, HOME: process.env.HOME!, CODEX_HOME: home, MEMEX_CLIENT: join(dir, "client.json"), MEMEX_POLICY: join(dir, "policy.json"),
          MEMEX_DEBUG_LOG: join(dir, "hook.jsonl") };
  const run = (args: string[]) => Bun.spawnSync([CODEX!, ...args], { env, cwd }).exitCode;
  expect(run(["plugin", "marketplace", "add", market])).toBe(0);
  expect(run(["plugin", "add", "memex@memex-local"])).toBe(0);
  writeFileSync(join(home, "config.toml"), readFileSync(join(home, "config.toml"), "utf8") + `
[plugins."memex@memex-local".mcp_servers.memex]
default_tools_approval_mode = "approve"
`);
});
afterAll(async () => {
  await api?.stop();
  model?.stop(true);
  await db?.close();
});

async function codex(args: string[]) {
  const p = Bun.spawn([CODEX!, "exec", "--dangerously-bypass-hook-trust", "--skip-git-repo-check", ...args], { env, cwd, stdout: "pipe", stderr: "pipe" });
  const [out, err, code] = [await new Response(p.stdout).text(), await new Response(p.stderr).text(), await p.exited];
  return { code, out: out + err };
}
const rollouts = () => {
  const out: string[] = [];
  const walk = (d: string) => {
    for (const n of readdirSync(d, { withFileTypes: true })) n.isDirectory() ? walk(join(d, n.name)) : n.name.endsWith(".jsonl") && out.push(join(d, n.name));
  };
  walk(join(home, "sessions"));
  return out;
};
const linesOf = (f: string) => readFileSync(f, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l));

test.skipIf(!up)("real codex: hooks sync, snapshot and recall go into the rollout, mem_search by thread id", async () => {
  const r0 = await codex(["Reply with exactly: the service port is zx-7781"]);
  expect(r0.code, r0.out).toBe(0);
  const first = rollouts();
  expect(first.length).toBe(1);
  expect((await db`select count(*)::int n from source`)[0].n).toBe(1); // synced by Stop
  await drainAll(db, KINDS);

  // A new session: SessionStart puts the snapshot in; UserPromptSubmit puts the recall in, before the prompt.
  const r1 = await codex(["Which port does the service use? Call mem_search with query \"service port\" first, then answer."]);
  expect(r1.code, r1.out).toBe(0);
  const f1 = rollouts().find((f) => !first.includes(f))!;
  const ls = linesOf(f1);
  const dev = ls.filter((l) => l.type === "response_item" && l.payload?.role === "developer").map((l) => JSON.stringify(l.payload.content));
  expect(dev.some((t) => t.includes("<memex>") && t.includes("[memex] Background from earlier sessions"))).toBe(true);
  expect(dev.some((t) => t.includes("<memex-recall>"))).toBe(true);
  // The tool ran as this thread and found the earlier session (any calling form: direct or through code mode).
  const outputs = ls.filter((l) => l.type === "response_item" && /_output$/.test(String(l.payload?.type))).map((l) => JSON.stringify(l.payload));
  expect(outputs.some((o) => o.includes("zx-7781") && o.includes("view_key")), outputs.join("\n").slice(0, 2000)).toBe(true);
  expect((await db`select count(*)::int n from recall_block where state = 'shown' and user_rec like 'turn:%'`)[0].n).toBe(1);

  // Resumed: the previous exchange is referenced and accepted (not query-stale); the turn's prompt is the input.
  await drainAll(db, KINDS); // the worker's projection of this session (a lagging one: query-stale, no injection)
  embeds.length = 0;
  const id = ls[0].payload.id;
  const r2 = await codex(["resume", id, "And that port again, what was it?"]);
  expect(r2.code, r2.out).toBe(0);
  const q = embeds.find((e) => e.startsWith("And that port again"));
  expect(q).toContain("previous user: Which port does the service use?");
  // One decision per prompt (the first session's, empty; this one's; the resumed one's), all bound by turn id.
  expect((await db`select count(*)::int n from recall_block where user_rec like 'turn:%'`)[0].n).toBe(3);
  const log = readFileSync(join(dir, "hook.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l));
  expect(log.at(-1).state).not.toBe("query-stale");
  expect(log.at(-1).prev).not.toBeNull();
}, 300_000);
