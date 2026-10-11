// memex for Codex: the agent's tools (DESIGN §6.4) as a stdio MCP server. The session is the thread Codex names
// in each call's `_meta.threadId` (verified against its rollout's session_meta id, then the policy scope of that
// file); a call without it, or whose rollout cannot be found or is outside every scope, sends nothing.
import { fromEnv } from "../../client/api.ts";
import { loadPolicy, scopeOf, type Policy } from "../../client/policy.ts";
import { readHead } from "../../client/records.ts";
import { callTool, TOOLS } from "../../client/tools.ts";
import { rolloutOf } from "./session.ts";

const send = (m: object) => process.stdout.write(JSON.stringify(m) + "\n");

async function call(params: any): Promise<string> {
  const t = fromEnv();
  let policy: Policy;
  try {
    policy = loadPolicy();
  } catch (e) {
    return `memex: policy: ${String((e as Error)?.message ?? e)}`;
  }
  const id = params?._meta?.threadId;
  const path = typeof id === "string" ? rolloutOf(id) : undefined;
  const h = path ? readHead(path, "codex") : undefined;
  const s = path && h?.ok ? { source: h.sourceId, scope: scopeOf(policy, path), leaf: null } : undefined;
  return callTool(t, policy, s, params?.name, params?.arguments ?? {});
}

if (import.meta.main) {
  let buf = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (d: string) => {
    buf += d;
    for (let i: number; (i = buf.indexOf("\n")) >= 0; ) {
      const line = buf.slice(0, i);
      buf = buf.slice(i + 1);
      if (!line.trim()) continue;
      let m: any;
      try {
        m = JSON.parse(line);
      } catch {
        continue;
      }
      if (m.method === "initialize")
        send({ jsonrpc: "2.0", id: m.id, result: { protocolVersion: m.params?.protocolVersion ?? "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "memex", version: "0.1.0" } } });
      else if (m.method === "tools/list") send({ jsonrpc: "2.0", id: m.id, result: { tools: TOOLS } });
      else if (m.method === "tools/call")
        call(m.params).then((text) => send({ jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text }] } }));
      else if (m.id !== undefined) send({ jsonrpc: "2.0", id: m.id, result: {} });
    }
  });
}
