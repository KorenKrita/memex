// L0 sync end to end over HTTP against a real Postgres (MEMEX_PG_URL, a throwaway database: tests truncate it).
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync, appendFileSync, mkdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "./db.ts";
import { addApiKey, serve, transport } from "./http.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { syncFile } from "../client/sync.ts";
import { readSession, sourceIdOf } from "../client/records.ts";
import type { Policy } from "../client/policy.ts";

let db: Db, server: Awaited<ReturnType<typeof serve>>, key: string, base: string;
const dir = mkdtempSync(join(tmpdir(), "memex-sync-"));
const allowDir = join(dir, "allowed");
const denyDir = join(dir, "denied");
mkdirSync(allowDir);
mkdirSync(denyDir);
const policy: Policy = { scopes: [{ name: "personal", paths: [allowDir], allow: { sync: ["memex"] } }] };

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
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_round, scan_item, collector_view, job restart identity cascade`);
  key = await addApiKey(db, "test");
  await db`insert into grant_rule values ('personal', 'sync', 'memex')`;
});

const M = (id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: "2026-01-01T00:00:01Z", message: { role: "user", content: [{ type: "text", text }] } });
function piFile(d: string, name: string, n: number, sid = "s-" + name) {
  const p = join(d, name + ".jsonl");
  const lines = [JSON.stringify({ type: "session", version: 3, id: sid, timestamp: "2026-01-01T00:00:00Z", cwd: "/x" })];
  for (let i = 0; i < n; i++) lines.push(M("e" + i, i ? "e" + (i - 1) : null, "text " + i));
  writeFileSync(p, lines.join("\n") + "\n");
  return p;
}
const counting = () => {
  const calls: string[] = [];
  const f = (async (u: any, i: any) => (calls.push(String(u)), fetch(u, i))) as typeof fetch;
  return { calls, t: () => transport(base, key, f) };
};
const records = async (source: string) => (await db`select rec, state from record where source = ${source} order by first_pos`).map((r: any) => r.rec);

test("unauthorized source: zero requests", async () => {
  const c = counting();
  const p = piFile(denyDir, "d", 3);
  expect(await syncFile(c.t(), policy, p, "pi", { collector: "m1" })).toEqual({ status: "skipped", reason: "not-authorized" });
  expect(await syncFile(c.t(), { scopes: [] }, piFile(allowDir, "a0", 2), "pi", { collector: "m1" })).toMatchObject({ status: "skipped" });
  expect(c.calls).toEqual([]);
});

test("sync is idempotent; appended lines go up; a partial last line waits", async () => {
  const p = piFile(allowDir, "a", 5);
  const t = transport(base, key);
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", sent: 6, length: 6 });
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toEqual({ status: "unchanged", length: 6 });
  appendFileSync(p, M("e5", "e4", "more") + "\n" + M("e6", "e5", "half").slice(0, 20));
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", sent: 1, length: 7 });
  const src = sourceIdOf("pi", "s-a");
  expect(await records(src)).toEqual(["header", "e0", "e1", "e2", "e3", "e4", "e5"]);
  const [b] = await db`select count(*)::int n from body where source = ${src}`;
  expect(b.n).toBe(7);
});

test("a leaf change with the file unchanged is reported; an unchanged leaf with an unchanged file sends no commit", async () => {
  const p = piFile(allowDir, "lf", 3);
  const t = transport(base, key);
  const src = sourceIdOf("pi", "s-lf");
  expect(await syncFile(t, policy, p, "pi", { collector: "m1", leaf: "e2" })).toMatchObject({ status: "synced" });
  const leaf = async () => (await db`select leaf from collector_view where source = ${src} and collector = 'm1'`)[0]?.leaf;
  expect(await leaf()).toBe("e2");
  // Navigated back in the tree: no new entry, only the leaf moved.
  expect(await syncFile(t, policy, p, "pi", { collector: "m1", leaf: "e0" })).toMatchObject({ status: "synced", sent: 0 });
  expect(await leaf()).toBe("e0");
  const before = (await db`select count(*)::int n from commit_result`)[0].n;
  expect(await syncFile(t, policy, p, "pi", { collector: "m1", leaf: "e0" })).toEqual({ status: "unchanged", length: 4 });
  expect((await db`select count(*)::int n from commit_result`)[0].n).toBe(before);
});

test("a sync that gives up after its lease was granted lets the lease go", async () => {
  const p = piFile(allowDir, "rl", 2);
  const src = sourceIdOf("pi", "s-rl");
  // The file turns into another session between the first read and the read under the lease.
  let flip = false;
  const base0 = transport(base, key);
  const t: typeof base0 = async (method, path, body) => {
    const r = await base0(method, path, body);
    if (!flip && path.endsWith("/poll") && r.granted) {
      flip = true;
      writeFileSync(p, readFileSync(p, "utf8").replace('"id":"s-rl"', '"id":"s-other"'));
    }
    return r;
  };
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toEqual({ status: "failed", reason: "file-changed-identity" });
  await Bun.sleep(100); // the release is sent, not waited for
  expect((await db`select count(*)::int n from lease where source = ${src} and not done`)[0].n).toBe(0);
});

test("same-length rewrite of the first record is found (slow path), body keeps both versions", async () => {
  const p = piFile(allowDir, "b", 600);
  const t = transport(base, key);
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  const lines = (await Bun.file(p).text()).split("\n");
  lines[1] = lines[1].replace("text 0", "TEXT 0");
  writeFileSync(p, lines.join("\n"));
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", sent: 1 });
  const src = sourceIdOf("pi", "s-b");
  const [r] = await db`select hash from record where source = ${src} and rec = 'e0'`;
  expect(r.hash).toBe(readSession(p, "pi").ok && (readSession(p, "pi") as any).records[1].hash);
  const [n] = await db`select count(*)::int n from body where source = ${src} and rec = 'e0'`;
  expect(n.n).toBe(2);
});

test("two collectors disagree: the record is isolated as conflict and the session version moves", async () => {
  const a = piFile(allowDir, "c1", 2, "same");
  const t = transport(base, key);
  await syncFile(t, policy, a, "pi", { collector: "m1" });
  const b = join(allowDir, "c2.jsonl");
  writeFileSync(b, (await Bun.file(a).text()).replace("text 1", "text X"));
  await syncFile(t, policy, b, "pi", { collector: "m2" });
  const src = sourceIdOf("pi", "same");
  const rows = await db`select rec, state from record where source = ${src} order by first_pos`;
  expect(rows.map((r: any) => `${r.rec}:${r.state}`)).toEqual(["header:ok", "e0:ok", "e1:conflict"]);
  const [v] = await db`select v from vis_version where key = ${"s:" + src}`;
  expect(Number(v.v)).toBeGreaterThan(1);
});

test("expired lease cannot commit; the queue moves on; a commit id retried returns the stored result", async () => {
  process.env.MEMEX_LEASE_TTL_MS = "30000";
  const decl = { source: sourceIdOf("pi", "q"), harness: "pi" as const, nativeId: "q", scope: "personal" };
  const l1 = await requestLease(db, decl, "m1");
  const l2 = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l2)).granted).toBe(false); // FIFO: l1 first
  expect((await tryGrant(db, l1)).granted).toBe(true);
  await db`update lease set expires_at = clock_timestamp() - interval '1 second' where id = ${l1}`;
  const raw = JSON.stringify({ type: "session", version: 3, id: "q" });
  const ch = [{ pos: 0, id: "header", hash: new Bun.CryptoHasher("sha256").update(raw).digest("hex"), raw }];
  expect((await db`select granted_at is not null and not done and expires_at > clock_timestamp() as v from lease where id = ${l1}`)[0].v).toBe(false);
  expect(await tryGrant(db, l1)).toEqual({ granted: false, leftMs: 0 }); // an expired lease is never reported as held
  expect(await commit(db, { leaseId: l1, commitId: "c1", length: 1, changes: ch })).toEqual({ ok: false, reason: "lease" });
  expect((await tryGrant(db, l2)).granted).toBe(true);
  const ok = await commit(db, { leaseId: l2, commitId: "c2", length: 1, changes: ch });
  expect(ok).toMatchObject({ ok: true, accepted: 1 });
  expect(await commit(db, { leaseId: l2, commitId: "c2", length: 1, changes: ch })).toEqual(ok);
});

test("forgotten source and canonical records are refused", async () => {
  const p = piFile(allowDir, "f", 2);
  const t = transport(base, key);
  const src = sourceIdOf("pi", "s-f");
  await db`insert into tomb (key) values (${"canon:" + src + ":e1"})`;
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", refused: 1 });
  expect(await records(src)).toEqual(["header", "e0"]);
  await db`insert into tomb (key) values (${"source:" + src})`;
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "failed" });
});

test("API key required; a bad hash or a source id that does not match is refused", async () => {
  expect((await fetch(base + "/v1/sync/lease", { method: "POST", body: "{}" })).status).toBe(401);
  const t = transport(base, key);
  await expect(t("POST", "/v1/sync/lease", { source: "x".repeat(64), harness: "pi", nativeId: "n", scope: "s", collector: "c" })).rejects.toThrow("400");
});

test("server grant: a client that skips its own policy stores nothing; revoking mid-lease refuses the commit", async () => {
  const p = piFile(allowDir, "g", 2);
  const open: Policy = { scopes: [{ name: "other", paths: [allowDir], allow: { sync: ["memex"] } }] };
  const t = transport(base, key);
  expect(await syncFile(t, open, p, "pi", { collector: "m1" })).toMatchObject({ status: "failed", reason: "403 scope not granted for sync" });
  const decl = { source: sourceIdOf("pi", "g2"), harness: "pi" as const, nativeId: "g2", scope: "personal" };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  await db`delete from grant_rule`;
  const raw = JSON.stringify({ type: "session", version: 3, id: "g2" });
  const ch = [{ pos: 0, id: "header", hash: new Bun.CryptoHasher("sha256").update(raw).digest("hex"), raw }];
  expect(await commit(db, { leaseId: l, commitId: "g", length: 1, changes: ch })).toEqual({ ok: false, reason: "not-granted" });
  const [n] = await db`select (select count(*) from body)::int b, (select count(*) from record)::int r`;
  expect(n).toEqual({ b: 0, r: 0 });
});

test("a record the copy no longer holds leaves the merged L0 and re-queues projection", async () => {
  const p = piFile(allowDir, "t", 4);
  const t = transport(base, key);
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  const src = sourceIdOf("pi", "s-t");
  await db`update job set state = 'done'`;
  const lines = (await Bun.file(p).text()).split("\n");
  writeFileSync(p, lines.slice(0, 3).join("\n") + "\n"); // header, e0, e1
  expect(await syncFile(t, policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", sent: 0, length: 3 });
  expect(await records(src)).toEqual(["header", "e0", "e1"]);
  const [j] = await db`select count(*)::int n from job where kind = 'project' and key = ${src} and state = 'queued'`;
  expect(j.n).toBe(1);
});

test("Pi fork parent is sent only when the parent file is in the same scope", async () => {
  const t = transport(base, key);
  const par = piFile(allowDir, "par", 1, "par-id");
  const kid = join(allowDir, "kid.jsonl");
  const head = (p: string, id: string) => JSON.stringify({ type: "session", version: 3, id, timestamp: "2026-01-01T00:00:00Z", cwd: "/x", parentSession: p });
  writeFileSync(kid, head(par, "kid-id") + "\n");
  await syncFile(t, policy, kid, "pi", { collector: "m1" });
  const outside = piFile(denyDir, "par-out", 1, "par-out-id");
  const kid2 = join(allowDir, "kid2.jsonl");
  writeFileSync(kid2, head(outside, "kid2-id") + "\n");
  await syncFile(t, policy, kid2, "pi", { collector: "m1" });
  const rows = [...(await db`select native_id, parent_id from source`)].sort((a: any, b: any) => (a.native_id < b.native_id ? -1 : 1));
  expect(rows).toEqual([
    { native_id: "kid-id", parent_id: sourceIdOf("pi", "par-id") },
    { native_id: "kid2-id", parent_id: null }, // the parent file outside the scope is never opened
  ]);
});

test("the session version moves on withdrawals only: an append does not, a revision or a removal does", async () => {
  const p = piFile(allowDir, "v", 3);
  const t = transport(base, key);
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  const src = sourceIdOf("pi", "s-v");
  const ver = async () => Number((await db`select v from vis_version where key = ${"s:" + src}`)[0]?.v ?? 1);
  const v0 = await ver();
  appendFileSync(p, M("e3", "e2", "appended") + "\n");
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  expect(await ver()).toBe(v0); // nothing published from the old records is wrong now
  expect((await db`select count(*)::int n from job where kind = 'project' and key = ${src} and state = 'queued'`)[0].n).toBe(1);
  const lines = (await Bun.file(p).text()).split("\n");
  lines[2] = lines[2].replace("text 1", "TEXT 1"); // same length: an in-place revision
  writeFileSync(p, lines.join("\n"));
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  const v1 = await ver();
  expect(v1).toBeGreaterThan(v0);
  writeFileSync(p, lines.slice(0, 3).join("\n") + "\n"); // e1 onwards removed
  await syncFile(t, policy, p, "pi", { collector: "m1" });
  expect(await ver()).toBeGreaterThan(v1);
});

test("sync keeps its deadline when the lease release hangs (the server's lease expires on its own)", async () => {
  const p = piFile(allowDir, "rel", 2);
  const f = (async (u: any, i: any) => (String(u).endsWith("/release") ? new Promise<Response>(() => {}) : fetch(u, i))) as typeof fetch;
  const at = Date.now();
  const tr = transport(base, key, f);
  // Fail the poll: the commit path never runs, the release in finally does.
  const failing = (async (m: any, path: any, body: any, o: any) => {
    if (String(path).endsWith("/poll")) throw new Error("poll down");
    return tr(m, path, body, o);
  }) as any;
  const r = await syncFile(failing, policy, p, "pi", { collector: "m1", deadlineMs: 600 });
  expect(r.status).toBe("failed");
  expect(Date.now() - at).toBeLessThan(1_000);
});

test("a request whose deadline has passed is not sent at all", async () => {
  const c = counting();
  await expect(c.t()("POST", "/v1/recall", {}, { deadline: Date.now() - 1 })).rejects.toThrow("timeout");
  expect(c.calls).toEqual([]);
});
