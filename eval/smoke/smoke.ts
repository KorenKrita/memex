// Synthetic smoke run against a deployed memex (MEMEX_URL, MEMEX_KEY): sync a fake Pi session, read it back.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fromEnv } from "../../client/api.ts";
import { syncFile } from "../../client/sync.ts";
import { sourceIdOf } from "../../client/records.ts";

const dir = process.env.SMOKE_DIR ?? "/tmp/memex-smoke/sessions";
mkdirSync(dir, { recursive: true });
const name = process.argv[2] ?? "smoke-a";
const p = join(dir, `${name}.jsonl`);
const M = (id: string, parent: string | null, role: string, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: "2026-10-11T00:00:01Z", message: { role, content: [{ type: "text", text }] } });
if (process.argv[3] !== "keep")
  writeFileSync(p, [JSON.stringify({ type: "session", version: 3, id: name, timestamp: "2026-10-11T00:00:00Z", cwd: "/tmp" }),
    M("u1", null, "user", "SMOKE where does the zx-7781 port go"), M("a1", "u1", "assistant", "SMOKE ANSWER zx-7781 is the relay port")].join("\n") + "\n");
const policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], tool: ["memex"] } }] };
const t = fromEnv()!;
const out: any = { sync: await syncFile(t, policy as any, p, "pi", { collector: "smoke:pi" }) };
out.grep = await t("POST", "/v1/grep", { source: sourceIdOf("pi", name), scope: "personal", purpose: "tool", pattern: "zx-7781", dests: ["memex"] })
  .then((r: any) => ({ hits: r.hits.length }), (e: Error) => ({ error: e.message }));
console.log(JSON.stringify(out));
