// Fork after forget, against a deployed memex: parent P synced (by "parent" mode), or a child forked from it
// (copied prefix + its own entry) synced now ("child" mode). Prints what grep finds of each text.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fromEnv } from "../../client/api.ts";
import { syncFile } from "../../client/sync.ts";
import { sourceIdOf } from "../../client/records.ts";

const dir = "/tmp/memex-smoke/fork-sessions";
mkdirSync(dir, { recursive: true });
const M = (id: string, parent: string | null, role: string, text: string, ts: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: ts, message: { role, content: [{ type: "text", text }] } });
const pPath = join(dir, "fk-parent.jsonl"), cPath = join(dir, "fk-child.jsonl");
const prefix = [M("u0", null, "user", "FORKPARENT-SECRET where", "2026-10-11T00:00:01Z"), M("a0", "u0", "assistant", "FORKPARENT-ANSWER", "2026-10-11T00:00:02Z")];
const policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], tool: ["memex"] } }] };
const t = fromEnv()!;
const mode = process.argv[2];
if (mode === "parent") {
  writeFileSync(pPath, [JSON.stringify({ type: "session", version: 3, id: "fk-parent", timestamp: "2026-10-11T00:00:00Z", cwd: "/tmp" }), ...prefix].join("\n") + "\n");
  console.log(JSON.stringify({ parent: await syncFile(t, policy as any, pPath, "pi", { collector: "fork:pi" }), source: sourceIdOf("pi", "fk-parent") }));
} else {
  writeFileSync(cPath, [JSON.stringify({ type: "session", version: 3, id: "fk-child", timestamp: "2026-10-11T00:00:03Z", cwd: "/tmp", parentSession: pPath }),
    ...prefix, M("k1", "a0", "user", "FORKCHILD-OWN text", "2026-10-11T00:00:04Z")].join("\n") + "\n");
  const child = await syncFile(t, policy as any, cPath, "pi", { collector: "fork:pi" });
  await Bun.sleep(4000); // the worker projects it
  const grep = async (pattern: string) => (await t("POST", "/v1/grep", { source: sourceIdOf("pi", "fk-child"), scope: "personal", purpose: "tool", pattern, dests: ["memex"] })).hits.length;
  console.log(JSON.stringify({ child, parentSecret: await grep("FORKPARENT-SECRET"), childOwn: await grep("FORKCHILD-OWN") }));
}
