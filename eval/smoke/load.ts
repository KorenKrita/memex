// Synthetic load: N fake Pi sessions of R exchanges each, synced 8 at a time; then 30 greps, each for a marker
// that only one record of one session holds, which must be found in that session.
//   bun eval/smoke/load.ts [N] [R]        (MEMEX_URL, MEMEX_KEY; every run uses fresh session ids)
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fromEnv } from "../../client/api.ts";
import { syncFile } from "../../client/sync.ts";
import { sourceIdOf } from "../../client/records.ts";

const N = Number(process.argv[2] ?? 200), R = Number(process.argv[3] ?? 40), P = 8;
const run = Date.now().toString(36);
const dir = `/tmp/memex-smoke/load-${run}`;
mkdirSync(dir, { recursive: true });
const words = "port relay deploy cache index forget lease shard token vector summary branch rollout hook budget".split(" ");
const say = (i: number, j: number) => Array.from({ length: 60 }, (_, k) => words[(i * 7 + j * 3 + k) % words.length]).join(" ");
const mark = (i: number, j: number, role: string) => `mk${run}x${i}x${j}x${role}`; // unique to one record
const id = (i: number) => `load-${run}-${i}`;
const M = (rid: string, parent: string | null, role: string, text: string) =>
  JSON.stringify({ type: "message", id: rid, parentId: parent, timestamp: "2026-10-11T00:00:01Z", message: { role, content: [{ type: "text", text }] } });
const files: string[] = [];
for (let i = 0; i < N; i++) {
  const lines = [JSON.stringify({ type: "session", version: 3, id: id(i), timestamp: "2026-10-11T00:00:00Z", cwd: "/tmp" })];
  let prev: string | null = null;
  for (let j = 0; j < R; j++) {
    lines.push(M(`u${j}`, prev, "user", `Q ${say(i, j)} ${mark(i, j, "u")}`), M(`a${j}`, `u${j}`, "assistant", `A ${say(j, i)} ${mark(i, j, "a")}`));
    prev = `a${j}`;
  }
  const p = join(dir, `${id(i)}.jsonl`);
  writeFileSync(p, lines.join("\n") + "\n");
  files.push(p);
}
const policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], tool: ["memex"] } }] };
const t = fromEnv()!;
const t0 = Date.now();
const sync = { synced: 0, other: 0, sent: 0, refused: 0 };
for (let i = 0; i < files.length; i += P)
  await Promise.all(files.slice(i, i + P).map(async (p) => {
    const r: any = await syncFile(t, policy as any, p, "pi", { collector: "load:pi", deadlineMs: 60_000 });
    if (r.status === "synced" && !r.more) sync.synced++;
    else sync.other++;
    sync.sent += r.sent ?? 0;
    sync.refused += r.refused ?? 0;
  }));
const syncMs = Date.now() - t0;
const grep = (i: number, j: number) =>
  t("POST", "/v1/grep", { source: sourceIdOf("pi", id(i)), scope: "personal", purpose: "tool", pattern: mark(i, j, "a"), dests: ["memex"] }) as Promise<any>;
const found = (r: any, i: number) => r.hits?.length === 1 && r.hits[0].in === sourceIdOf("pi", id(i));
// grep reads projected views: wait until the last session synced is projected.
const w0 = Date.now();
let lastProjected = false;
while (!(lastProjected = found(await grep(N - 1, R - 1), N - 1)) && Date.now() - w0 < 300_000) await Bun.sleep(1000);
const projectMs = Date.now() - w0;
const lat: number[] = [];
let missed = 0;
for (let k = 0; k < 30; k++) {
  const i = Math.floor((k * N) / 30), j = (k * 7) % R; // spread over the sessions and their records
  const s = Date.now();
  const r = await grep(i, j);
  lat.push(Date.now() - s);
  if (!found(r, i)) missed++;
}
lat.sort((a, b) => a - b);
const out = { run, sessions: N, records: N * R * 2, sync, sync_ms: syncMs, last_projected: lastProjected, wait_projected_ms: projectMs, grep_missed: missed, grep_p50: lat[15], grep_p95: lat[28] };
console.log(JSON.stringify(out));
if (sync.other || sync.sent !== N * R * 2 + N || sync.refused || missed || !lastProjected) process.exit(1);
