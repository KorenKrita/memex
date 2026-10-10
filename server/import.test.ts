// History import (DESIGN §5.4) end to end over HTTP against a real Postgres (MEMEX_PG_URL, a throwaway
// database: tests truncate it).
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "./db.ts";
import { addApiKey, serve, transport } from "./http.ts";
import { importDryRun, importFiles, importPlan } from "../client/cli.ts";
import type { Transport } from "../client/sync.ts";
import type { Policy } from "../client/policy.ts";

let db: Db, server: Awaited<ReturnType<typeof serve>>, key: string, base: string;
let piDir: string, codexDir: string, syncOnly: string, policy: Policy;

beforeAll(async () => {
  db = connect();
  await migrate(db);
  server = await serve(db, { port: 0 });
  base = `http://127.0.0.1:${server.port}`;
});
afterAll(async () => {
  await server.stop();
  await db.close();
});
beforeEach(async () => {
  const dir = mkdtempSync(join(tmpdir(), "memex-import-"));
  (piDir = join(dir, "pi")), (codexDir = join(dir, "codex")), (syncOnly = join(dir, "live"));
  for (const d of [piDir, codexDir, syncOnly]) mkdirSync(d);
  policy = { scopes: [{ name: "personal", paths: [piDir, codexDir], allow: { sync: ["memex"], import: ["memex"] } },
                      { name: "live", paths: [syncOnly], allow: { sync: ["memex"] } }] };
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_round, scan_item, collector_view, job restart identity cascade`);
  key = await addApiKey(db, "test");
  await db`insert into grant_rule values ('personal', 'sync', 'memex'), ('live', 'sync', 'memex')`;
});

const day = 86_400_000;
function pi(d: string, name: string, n: number, ageDays: number) {
  const p = join(d, name + ".jsonl");
  const lines = [JSON.stringify({ type: "session", version: 3, id: "s-" + name, timestamp: "2026-01-01T00:00:00Z", cwd: "/x" })];
  for (let i = 0; i < n; i++)
    lines.push(JSON.stringify({ type: "message", id: "e" + i, parentId: i ? "e" + (i - 1) : null, timestamp: "2026-01-01T00:00:01Z",
                                message: { role: "user", content: [{ type: "text", text: "text " + i }] } }));
  writeFileSync(p, lines.join("\n") + "\n");
  const t = new Date(Date.now() - ageDays * day);
  utimesSync(p, t, t);
  return p;
}
function codex(name: string, n: number, ageDays: number) {
  const p = join(codexDir, `rollout-${name}.jsonl`);
  const lines = [JSON.stringify({ type: "session_meta", ordinal: 0, payload: { id: "c-" + name, history_mode: "paginated", cwd: "/x" } })];
  for (let i = 1; i <= n; i++)
    lines.push(JSON.stringify({ type: "response_item", ordinal: i, payload: { type: "message", role: "user", content: [{ type: "input_text", text: "q " + i }] } }));
  writeFileSync(p, lines.join("\n") + "\n");
  const t = new Date(Date.now() - ageDays * day);
  utimesSync(p, t, t);
  return p;
}
const sources = async () => (await db`select harness, native_id from source order by native_id`).map((r: any) => `${r.harness}:${r.native_id}`);

test("the plan covers only scopes allowed to import, sorted by harness, filtered by --since", async () => {
  pi(piDir, "old", 3, 40);
  pi(piDir, "new", 3, 2);
  codex("c1", 2, 1);
  pi(syncOnly, "live", 3, 1);
  writeFileSync(join(piDir, "notes.txt"), "not a log\n");
  const all = importPlan(policy);
  expect(all.pi.map((f) => f.path.split("/").pop()).sort()).toEqual(["new.jsonl", "old.jsonl"]);
  expect(all.codex.map((f) => f.path.split("/").pop())).toEqual(["rollout-c1.jsonl"]);
  const recent = importPlan(policy, { since: Date.now() - 30 * day });
  expect(recent.pi.map((f) => f.path.split("/").pop())).toEqual(["new.jsonl"]);
});

test("the plan reads the whole header: a Codex header with timestamp first is found; symlinked dirs are not followed", async () => {
  const p = join(codexDir, "rollout-ts.jsonl");
  writeFileSync(p, JSON.stringify({ timestamp: "2026-01-01T00:00:00Z", type: "session_meta", ordinal: 0,
                                    payload: { id: "c-ts", history_mode: "paginated", cwd: "/x", note: "x".repeat(2000) } }) + "\n");
  const outside = mkdtempSync(join(tmpdir(), "memex-import-outside-"));
  pi(outside, "linked", 2, 1);
  symlinkSync(outside, join(piDir, "link"));
  const plan = importPlan(policy);
  expect(plan.codex.map((f) => f.path.split("/").pop())).toEqual(["rollout-ts.jsonl"]);
  expect(plan.pi).toEqual([]);
});

test("an import round sends only what import allows now, and an item left out of a later plan stays pending", async () => {
  const a = pi(piDir, "ia", 3, 1);
  pi(piDir, "ib", 3, 2);
  const plan = importPlan(policy);
  // The policy loses import for this scope after the plan was made: nothing is sent for it.
  const narrowed: Policy = { scopes: [{ ...policy.scopes[0], allow: { sync: ["memex"] } }, policy.scopes[1]] };
  const calls: string[] = [];
  const t: Transport = async (m, path, body) => (calls.push(path), transport(base, key)(m, path, body));
  expect(await importFiles(t, narrowed, plan, { machine: "mac" })).toEqual({});
  expect(calls).toEqual([]);
  // A round over both files, then a run with only one of them: the other is not closed as missing.
  const r = await importFiles(transport(base, key), policy, { pi: [plan.pi[0], plan.pi[1]], codex: [] }, { machine: "mac", maxRecs: 1, maxIdle: 1 });
  expect(r.pi?.total).toBe(2);
  await importFiles(transport(base, key), policy, { pi: plan.pi.filter((f) => f.path === a), codex: [] }, { machine: "mac" });
  expect((await db`select count(*)::int n from scan_item where reason = 'missing' or reason = 'not-listed'`)[0].n).toBe(0);
});

test("dry run: local statistics, no request", async () => {
  pi(piDir, "a", 4, 1);
  codex("c2", 3, 1);
  const calls: string[] = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async (u: any) => (calls.push(String(u)), new Response("{}"))) as typeof fetch;
  let r;
  try {
    r = importDryRun(importPlan(policy));
  } finally {
    globalThis.fetch = realFetch;
  }
  expect(calls).toEqual([]);
  expect(r).toMatchObject({ files: 2 });
  expect(r.stats.indexed).toEqual({ pi: 1, codex: 1 });
});

test("import runs to completion per harness and is idempotent; the sync-only scope is never sent", async () => {
  pi(piDir, "a", 600, 3);
  pi(piDir, "b", 5, 1);
  codex("c3", 4, 2);
  pi(syncOnly, "live", 3, 1);
  const lines: string[] = [];
  const r = await importFiles(transport(base, key), policy, importPlan(policy), { machine: "mac", log: (s) => lines.push(s), maxRecs: 256 });
  expect(r.pi).toMatchObject({ finished: true, total: 2, done: 2 });
  expect(r.codex).toMatchObject({ finished: true, total: 1, done: 1 });
  expect(await sources()).toEqual(["codex:c-c3", "pi:s-a", "pi:s-b"]);
  expect((await db`select count(*)::int n from projection where collector = 'mac:pi'`)[0].n).toBe(601 + 6);
  expect(lines.some((l) => l.includes("pi"))).toBe(true);
  const before = (await db`select count(*)::int n from body`)[0].n;
  const again = await importFiles(transport(base, key), policy, importPlan(policy), { machine: "mac" });
  expect(again.pi).toMatchObject({ finished: true, done: 2 });
  expect((await db`select count(*)::int n from body`)[0].n).toBe(before);
});

test("a file that keeps failing does not hang the import: it stops and reports what is left", async () => {
  pi(piDir, "ok", 3, 2);
  pi(piDir, "bad", 3, 1);
  const t: Transport = async (m, path, body: any) => {
    if (path === "/v1/sync/lease" && body?.nativeId === "s-bad") throw new Error("503 busy");
    if (path === "/v1/sync/batch/lease") {
      const r = await transport(base, key)(m, path, body);
      return { results: r.results.map((x: any, i: number) => (body.requests[i].nativeId === "s-bad" ? { error: "503 busy" } : x)) };
    }
    return transport(base, key)(m, path, body);
  };
  const r = await importFiles(t, policy, importPlan(policy), { machine: "mac", maxIdle: 3 });
  expect(r.pi).toMatchObject({ finished: false, done: 1, pending: 1 });
});
