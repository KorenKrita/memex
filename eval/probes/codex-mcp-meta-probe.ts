// Codex MCP identity probe (stage 4): a minimal stdio MCP server whose one tool logs the `_meta` Codex sends
// with each tools/call (keys and id values only, never arguments' text) next to MEMEX_PROBE_LOG. Run under an
// isolated CODEX_HOME with a synthetic prompt that calls the tool; compare `_meta.threadId` / `sessionId` with
// the rollout's session_meta.payload.id. Usage: bun eval/probes/codex-mcp-meta-probe.ts (as an MCP command).
import { appendFileSync } from "node:fs";

const log = process.env.MEMEX_PROBE_LOG ?? "/tmp/memex-eval/codex-mcp-meta.jsonl";
const send = (m: object) => process.stdout.write(JSON.stringify(m) + "\n");
let buf = "";
process.stdin.on("data", (d) => {
  buf += d;
  let i: number;
  while ((i = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, i);
    buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    const m = JSON.parse(line);
    if (m.method === "initialize")
      send({ jsonrpc: "2.0", id: m.id, result: { protocolVersion: m.params?.protocolVersion ?? "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "memex-probe", version: "0" } } });
    else if (m.method === "tools/list")
      send({ jsonrpc: "2.0", id: m.id, result: { tools: [{ name: "probe_ping", description: "Returns pong. Call it when asked to ping.", inputSchema: { type: "object", properties: {} } }] } });
    else if (m.method === "tools/call") {
      const meta = m.params?._meta ?? null;
      appendFileSync(log, JSON.stringify({ t: Date.now(), env: { CODEX_THREAD_ID: process.env.CODEX_THREAD_ID ?? null, CODEX_SESSION_ID: process.env.CODEX_SESSION_ID ?? null },
        metaKeys: meta ? Object.keys(meta).sort() : null, threadId: meta?.threadId ?? null, sessionId: meta?.sessionId ?? null, pid: process.pid }) + "\n");
      send({ jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text: "pong" }] } });
    } else if (m.id !== undefined) send({ jsonrpc: "2.0", id: m.id, result: {} });
  }
});
