// Reconciliation rounds (DESIGN §5.2) end to end over HTTP against a real Postgres (MEMEX_PG_URL, a throwaway
// database: tests truncate it).
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, migrate, type Db } from "./db.ts";
import { addApiKey, serve, transport } from "./http.ts";
import { scanConfig } from "./sync.ts";
import { fileKey, scanOnce, scanSegment, syncFile, type ScanItem, type Transport } from "../client/sync.ts";
import { readSession, sourceIdOf } from "../client/records.ts";
import type { Policy } from "../client/policy.ts";

let db: Db, server: Awaited<ReturnType<typeof serve>>, key: string, base: string;
const dir = mkdtempSync(join(tmpdir(), "memex-scan-"));
const policy: Policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], import: ["memex"] } }] };

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
  scanConfig.intervalMs = 3_600_000;
});

const M = (id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: "2026-01-01T00:00:01Z", message: { role: "user", content: [{ type: "text", text }] } });
let n = 0;
function piFile(name: string, count: number) {
  const p = join(dir, name + ".jsonl");
  const lines = [JSON.stringify({ type: "session", version: 3, id: "s-" + name, timestamp: "2026-01-01T00:00:00Z", cwd: "/x" })];
  for (let i = 0; i < count; i++) lines.push(M("e" + i, i ? "e" + (i - 1) : null, "text " + i));
  writeFileSync(p, lines.join("\n") + "\n");
  return p;
}
const grow = (p: string, from: number, count: number) => {
  for (let i = from; i < from + count; i++) appendFileSync(p, M("e" + i, "e" + (i - 1), "text " + i) + "\n");
};
const t = () => transport(base, key);
const proj = async (name: string) =>
  (await db`select rec from projection where source = ${sourceIdOf("pi", "s-" + name)} and collector = 'm1' order by pos`).map((r: any) => r.rec);
const commits = async () => (await db`select count(*)::int n from commit_result`)[0].n as number;
/** Open a round over `paths` and return its id and the first items (a raw protocol step, no segment run). */
async function open(paths: string[], kind = "auto") {
  const files = paths.map((p) => ({ key: fileKey(p), size: readFileSync(p).length, mtime: ++n }));
  const o = await t()("POST", "/v1/scan/open", { collector: "m1", kind, files });
  const nx = await t()("POST", "/v1/scan/next", { round: o.round, limit: 50 });
  return { round: o.round as number, items: nx.items as ScanItem[] };
}
const item = async (round: number, p: string) =>
  (await db`select state, done_recs, done_bytes, tries, reason from scan_item where round = ${round} and key = ${fileKey(p)}`)[0];
const seg = (tr: Transport, p: string, round: number, it: ScanItem, maxRecs = 256) =>
  scanSegment(tr, policy, p, "pi", { collector: "m1", round, item: it, maxRecs });

test("a segment does not chase growth past its bound and does not cut the tail live sync uploaded", async () => {
  const p = piFile("g", 599); // 600 records with the header
  const { round, items } = await open([p]);
  expect(items).toHaveLength(1);
  expect(await seg(t(), p, round, items[0])).toMatchObject({ status: "advanced", sent: 256, final: false });
  grow(p, 599, 100);
  expect(await syncFile(t(), policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", length: 700 });
  for (let i = 0; i < 5; i++) {
    const [it] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
    if (!it) break;
    expect(await seg(t(), p, round, it)).toMatchObject({ status: "advanced" });
  }
  expect(await item(round, p)).toMatchObject({ state: "done", done_recs: 600 });
  expect(await proj("g")).toHaveLength(700);
  expect((await db`select finished_at from scan_round where id = ${round}`)[0].finished_at).not.toBeNull();
});

test("an unchanged range advances once with an empty commit; a stale descriptor does not advance it again", async () => {
  const p = piFile("u", 599);
  await syncFile(t(), policy, p, "pi", { collector: "m1" });
  const { round, items } = await open([p]);
  const before = await commits();
  const r = await seg(t(), p, round, items[0]);
  expect(r).toMatchObject({ status: "advanced", sent: 0, final: false });
  expect(await commits()).toBe(before + 1);
  expect(await item(round, p)).toMatchObject({ state: "pending", done_recs: 256 });
  expect(await seg(t(), p, round, items[0])).toEqual({ status: "stale" });
  expect(await item(round, p)).toMatchObject({ done_recs: 256 });
});

test("a lost receipt is retried with the same commit id and advances the item once", async () => {
  const p = piFile("l", 599);
  const { round, items } = await open([p]);
  let lost = false;
  const tr: Transport = async (m, path, body) => {
    const r = await t()(m, path, body);
    if (!lost && path.endsWith("/commit")) {
      lost = true;
      throw new Error("connection reset");
    }
    return r;
  };
  expect(await seg(tr, p, round, items[0])).toMatchObject({ status: "advanced", sent: 256 });
  expect(lost).toBe(true);
  expect(await item(round, p)).toMatchObject({ done_recs: 256 });
  expect(await proj("l")).toHaveLength(256);
});

test("two processes holding the same descriptor: one segment is credited, the other is stale", async () => {
  const p = piFile("c", 599);
  const { round, items } = await open([p]);
  const rs = await Promise.all([seg(t(), p, round, items[0]), seg(t(), p, round, items[0])]);
  expect(rs.map((r) => r.status).sort()).toEqual(["advanced", "stale"]);
  expect(await item(round, p)).toMatchObject({ done_recs: 256 });
});

test("a rewrite before the scanned point restarts the item; the round then uploads the revision", async () => {
  const p = piFile("w", 599);
  await syncFile(t(), policy, p, "pi", { collector: "m1" });
  const { round, items } = await open([p]);
  expect(await seg(t(), p, round, items[0])).toMatchObject({ status: "advanced" });
  const lines = readFileSync(p, "utf8").split("\n");
  lines[255] = lines[255].replace("text 254", "TEXT 254"); // pos 255: the record the next segment is anchored on
  writeFileSync(p, lines.join("\n"));
  const [it] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
  expect(await seg(t(), p, round, it)).toEqual({ status: "restart" });
  expect(await item(round, p)).toMatchObject({ state: "pending", done_recs: 0 });
  for (let i = 0; i < 5 && (await item(round, p)).state === "pending"; i++) {
    const [x] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
    await seg(t(), p, round, x);
  }
  expect(await item(round, p)).toMatchObject({ state: "done" });
  const [h] = await db`select b.raw from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                       where r.source = ${sourceIdOf("pi", "s-w")} and r.rec = 'e254'`;
  expect(h.raw).toContain("TEXT 254");
});

test("a record removed from the prefix: after the round, the copy is the file, position by position", async () => {
  const p = piFile("del", 599);
  await syncFile(t(), policy, p, "pi", { collector: "m1" });
  const lines = readFileSync(p, "utf8").split("\n");
  lines.splice(11, 1); // e10 goes; e11.. move up one
  writeFileSync(p, lines.join("\n"));
  const { round } = await open([p]);
  for (let i = 0; i < 10 && (await item(round, p)).state === "pending"; i++) {
    const [x] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
    await seg(t(), p, round, x);
  }
  expect(await item(round, p)).toMatchObject({ state: "done" });
  const local = (readSession(p, "pi") as any).records.map((r: any, pos: number) => `${pos}:${r.id}:${r.hash}`);
  const remote = (await db`select pos, rec, hash from projection where source = ${sourceIdOf("pi", "s-del")} and collector = 'm1' order by pos`)
    .map((r: any) => `${r.pos}:${r.rec}:${r.hash}`);
  expect(remote).toEqual(local);
});

test("a refused record leaves a hole: segments past it still compare by position, the round finishes, nothing is resent twice", async () => {
  const p = piFile("hole", 599);
  const src = sourceIdOf("pi", "s-hole");
  await db`insert into tomb (key) values (${"canon:" + src + ":e300"})`;
  expect(await syncFile(t(), policy, p, "pi", { collector: "m1" })).toMatchObject({ status: "synced", refused: 1 });
  const { round } = await open([p]);
  const sent: number[] = [];
  for (let i = 0; i < 10 && (await item(round, p)).state === "pending"; i++) {
    const [x] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
    const r: any = await seg(t(), p, round, x);
    sent.push(r.sent ?? -1);
  }
  expect(await item(round, p)).toMatchObject({ state: "done", done_recs: 600 });
  expect(sent).toEqual([0, 1, 0]); // only the refused record is offered again (and refused again)
  const remote = (await db`select pos, rec from projection where source = ${src} and collector = 'm1' order by pos`).map((r: any) => `${r.pos}:${r.rec}`);
  expect(remote).toHaveLength(599);
  expect(remote).not.toContain("301:e300");
  expect(remote.at(-1)).toBe("599:e598");
});

test("a cold file rewritten in place (mtime kept, same length) is found by the next round", async () => {
  const p = piFile("cold", 10);
  await syncFile(t(), policy, p, "pi", { collector: "m1" });
  scanConfig.intervalMs = 0;
  expect(await scanOnce(t(), policy, [{ path: p, harness: "pi" }], { collector: "m1" })).toMatchObject({ finished: true });
  const st = statSync(p);
  writeFileSync(p, readFileSync(p, "utf8").replace("text 0", "TEXT 0"));
  utimesSync(p, st.atime, st.mtime); // mtime kept
  expect(await scanOnce(t(), policy, [{ path: p, harness: "pi" }], { collector: "m1" })).toMatchObject({ finished: true });
  const [h] = await db`select b.raw from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                       where r.source = ${sourceIdOf("pi", "s-cold")} and r.rec = 'e0'`;
  expect(h.raw).toContain("TEXT 0");
});

test("a hot file that keeps failing does not starve a cold one", async () => {
  const hot = piFile("hot", 5);
  const cold = piFile("cold2", 5);
  const hotSrc = sourceIdOf("pi", "s-hot");
  const tr: Transport = async (m, path, body: any) => {
    if (path === "/v1/sync/lease" && body?.source === hotSrc) throw new Error("503 busy");
    if (path === "/v1/sync/batch/lease") {
      const r = await t()(m, path, body);
      return { results: r.results.map((x: any, i: number) => (body.requests[i].source === hotSrc ? { error: "503 busy" } : x)) };
    }
    return t()(m, path, body);
  };
  const files = [{ path: hot, harness: "pi" as const }, { path: cold, harness: "pi" as const }];
  // The hot file is newer (scanned first in this round's order) and fails every time.
  writeFileSync(hot, readFileSync(hot));
  let r: any;
  for (let i = 0; i < 3; i++) r = await scanOnce(tr, policy, files, { collector: "m1", limit: 1 });
  expect((await db`select state from scan_item where key = ${fileKey(cold)}`)[0].state).toBe("done");
  const [h] = await db`select state, tries, reason from scan_item where key = ${fileKey(hot)}`;
  expect(h).toMatchObject({ state: "pending", reason: "503 busy" });
  expect(h.tries).toBeGreaterThan(0);
  expect(r.finished).toBe(false);
});

test("rounds: one at a time per collector and kind, new files wait for the next one, unsupported files are skipped", async () => {
  const a = piFile("ra", 3);
  const bad = join(dir, "bad.jsonl");
  writeFileSync(bad, JSON.stringify({ type: "not-a-session" }) + "\n");
  const first = await scanOnce(t(), policy, [{ path: a, harness: "pi" }, { path: bad, harness: "pi" }], { collector: "m1" });
  expect(first).toMatchObject({ finished: true, total: 2, done: 1, skipped: 1 });
  expect((await db`select reason from scan_item where key = ${fileKey(bad)}`)[0].reason).toBe("not-a-session");
  // Inside the interval: no new auto round, a new file is not scanned yet.
  const b = piFile("rb", 3);
  expect(await scanOnce(t(), policy, [{ path: a, harness: "pi" }, { path: b, harness: "pi" }], { collector: "m1" }))
    .toMatchObject({ round: null });
  expect(await proj("rb")).toHaveLength(0);
  // An import round is not held back by the auto interval.
  expect(await scanOnce(t(), policy, [{ path: b, harness: "pi" }], { collector: "m1", kind: "import" })).toMatchObject({ finished: true, done: 1 });
  expect(await proj("rb")).toHaveLength(4);
  const st = await t()("GET", "/v1/scan/status?collector=m1");
  expect(st.auto.last).toMatchObject({ total: 2, done: 1, skipped: 1 });
  expect(st.import.last).toMatchObject({ total: 1, done: 1 });
});

test("a bound inside a line that is completed later: the item finishes, the line waits for the next round", async () => {
  const p = piFile("half", 3);
  appendFileSync(p, M("e3", "e2", "half line").slice(0, 25));
  const { round, items } = await open([p]);
  appendFileSync(p, M("e3", "e2", "half line").slice(25) + "\n" + M("e4", "e3", "more") + "\n");
  let r: any;
  for (let i = 0; i < 4; i++) {
    const [it] = (await t()("POST", "/v1/scan/next", { round, limit: 50 })).items as ScanItem[];
    if (!it) break;
    r = await seg(t(), p, round, it);
  }
  expect(await item(round, p)).toMatchObject({ state: "done", done_recs: 4 });
  expect(await proj("half")).toHaveLength(4);
});

test("forget closes the source's pending item; the round finishes and the source's segments are refused", async () => {
  const p = piFile("fg", 599);
  const { round, items } = await open([p]);
  expect(await seg(t(), p, round, items[0])).toMatchObject({ status: "advanced", final: false });
  await t()("POST", "/v1/forget", { source: sourceIdOf("pi", "s-fg") });
  expect(await item(round, p)).toMatchObject({ state: "skipped", reason: "forgotten" });
  expect((await db`select finished_at from scan_round where id = ${round}`)[0].finished_at).not.toBeNull();
  expect(await seg(t(), p, round, { ...items[0], doneRecs: 256 })).toMatchObject({ status: "failed" });
  expect(await proj("fg")).toHaveLength(0);
});

test("an unauthorized file in the list makes no request for it", async () => {
  const other = mkdtempSync(join(tmpdir(), "memex-scan-deny-"));
  mkdirSync(join(other, "x"));
  const p = join(other, "x", "d.jsonl");
  writeFileSync(p, readFileSync(piFile("dsrc", 2)));
  const calls: string[] = [];
  const tr: Transport = async (m, path, body) => (calls.push(path), t()(m, path, body));
  await scanOnce(tr, policy, [{ path: p, harness: "pi" }], { collector: "m1" });
  expect(calls).toEqual([]);
});

test("scanAll goes on past one call's item limit until the round is finished", async () => {
  const { scanAll } = await import("../client/sync.ts");
  const files = Array.from({ length: 5 }, (_, i) => ({ path: piFile(`many${i}`, 2), harness: "pi" as const }));
  const r: any = await scanAll(t(), policy, files, { collector: "m-all", limit: 2, deadlineMs: 30_000 });
  expect(r.finished).toBe(true);
  expect((await db`select count(*)::int n from scan_item i join scan_round r on r.id = i.round where r.collector = 'm-all' and i.state = 'done'`)[0].n).toBe(5);
});
