// Synthetic load: N fake Pi sessions of R exchanges each, synced in parallel batches; then grep latency.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fromEnv } from "../../client/api.ts";
import { syncFile } from "../../client/sync.ts";
import { sourceIdOf } from "../../client/records.ts";

const N = Number(process.argv[2] ?? 200), R = Number(process.argv[3] ?? 40), P = 8;
const dir = "/tmp/memex-smoke/sessions2";
mkdirSync(dir, { recursive: true });
const words = "port relay deploy cache index forget lease shard token vector summary branch rollout hook budget".split(" ");
const say = (i: number, j: number) => Array.from({ length: 60 }, (_, k) => words[(i * 7 + j * 3 + k) % words.length]).join(" ") + ` id-${i}-${j}`;
const M = (id: string, parent: string | null, role: string, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: "2026-10-11T00:00:01Z", message: { role, content: [{ type: "text", text }] } });
const files: string[] = [];
for (let i = 0; i < N; i++) {
  const lines = [JSON.stringify({ type: "session", version: 3, id: `load2-${i}`, timestamp: "2026-10-11T00:00:00Z", cwd: "/tmp" })];
  let prev: string | null = null;
  for (let j = 0; j < R; j++) {
    lines.push(M(`u${j}`, prev, "user", "Q " + say(i, j)), M(`a${j}`, `u${j}`, "assistant", "A " + say(j, i)));
    prev = `a${j}`;
  }
  const p = join(dir, `load2-${i}.jsonl`);
  writeFileSync(p, lines.join("\n") + "\n");
  files.push(p);
}
const policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], tool: ["memex"] } }] };
const t = fromEnv()!;
const t0 = Date.now();
let failed = 0;
for (let i = 0; i < files.length; i += P)
  await Promise.all(files.slice(i, i + P).map(async (p) => {
    const r: any = await syncFile(t, policy as any, p, "pi", { collector: "load:pi", deadlineMs: 60_000 });
    if (r.status !== "synced" && r.status !== "unchanged") failed++;
  }));
const syncMs = Date.now() - t0;
const lat: number[] = [];
let missed = 0; // each query names one record of the session: it must be found
// grep reads projected views: wait until the worker has projected the last session synced.
const probe = { source: sourceIdOf("pi", `load2-${N - 1}`), scope: "personal", purpose: "tool", pattern: `id-${N - 1}-3`, dests: ["memex"] };
const w0 = Date.now();
while (!((await t("POST", "/v1/grep", probe)) as any).hits?.length && Date.now() - w0 < 300_000) await Bun.sleep(1000);
const projectMs = Date.now() - w0;
for (let k = 0; k < 30; k++) {
  const s = Date.now();
  const r: any = await t("POST", "/v1/grep", { source: sourceIdOf("pi", `load2-${k}`), scope: "personal", purpose: "tool", pattern: `id-${k}-3`, dests: ["memex"] });
  lat.push(Date.now() - s);
  if (!r.hits?.length) missed++;
}
lat.sort((a, b) => a - b);
console.log(JSON.stringify({ sessions: N, records: N * R * 2, failed, sync_ms: syncMs, wait_projected_ms: projectMs, grep_missed: missed, grep_p50: lat[15], grep_p95: lat[28] }));
