// Start-of-session snapshot (DESIGN §6.1) over real Postgres and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant } from "./sync.ts";
import { revokeGrant } from "./derive.ts";
import { startSnapshot, BUDGET, FRAMING, TOOLS } from "./snapshot.ts";
import { forgetSource } from "./forget.ts";
import { drain } from "./worker.ts";
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let requests: any[] = [];
let overview = (parts: string[]) => parts.join(" | ");
let srv: ReturnType<typeof Bun.serve>;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      requests.push(body);
      if (path.endsWith("/embeddings")) return Response.json({ data: [{ embedding: Array.from({ length: 1024 }, (_, i) => (i === 0 ? 1 : 0)) }] });
      const sys = body.messages[0].content as string;
      const content = body.messages[1].content as string;
      if (sys.startsWith("You write the card")) {
        const parts = [...content.matchAll(/<part>\n([\s\S]*?)\n<\/part>/g)].map((m) => m[1]);
        return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "card " + parts[0]?.slice(0, 20), overview: overview(parts), results: [], open_threads: [] }) } }] });
      }
      if (sys.startsWith("You merge")) {
        const parts = [...content.matchAll(/<part n="\d">\n([\s\S]*?)\n<\/part>/g)].map((m) => m[1]);
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: parts.join(" + ") }) } }] });
      }
      const input = content.split("<input>")[1] ?? "";
      const words = [...input.matchAll(/kind="user">\s*(?:user: )?([^<]*)</g)].map((m) => m[1].trim()).join(" ");
      return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: "S(" + words + ")", user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] }) } }] });
    },
  });
  Object.assign(process.env, {
    MEMEX_LLM_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k",
    MEMEX_EMBED_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
  });
});
afterAll(async () => {
  srv.stop(true);
  await db.close();
});
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, forest_node, forest_access,
                   session_card, start_snapshot, snapshot_refresh restart identity cascade`);
  for (const [s, p, d] of [["personal", "sync", "memex"], ["personal", "derive", "llm"], ["personal", "derive", "embedding"], ["personal", "snapshot", "memex"],
                           ["work", "sync", "memex"], ["work", "derive", "llm"]])
    await db`insert into grant_rule values (${s}, ${p}, ${d})`;
  process.env.MEMEX_IDLE_CLOSE_MS = "0";
  requests = [];
  overview = (parts) => parts.join(" | ");
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = () => new Date().toISOString();
const header = (id: string) => JSON.stringify({ type: "session", version: 3, id, timestamp: T(), cwd: "/w/" + id });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: T(), message: { role, content: [{ type: "text", text }] } });
let n = 0;
async function put(native: string, words: string[], repo: string | null = "repo-a", scope = "personal") {
  const lines = [header(native)];
  let prev: string | null = null;
  words.forEach((w, i) => {
    lines.push(msg("user", `u${i}`, prev, w), msg("assistant", `a${i}`, `u${i}`, "r" + i));
    prev = `a${i}`;
  });
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope, parent: null, meta: repo ? { repo } : {} };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  expect((await commit(db, { leaseId: l, commitId: `n${n++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) })).ok).toBe(true);
  return decl.source;
}
const me = sourceIdOf("pi", "me");
const snap = async (x: object = {}) => (await startSnapshot(db, { source: me, scope: "personal", repo: "repo-a", ...x })).body as any;

test("first request builds and stores: framing, current repo's cards, other repos' recent titles, tool hints", async () => {
  const a = await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await put("sb", ["beta0"], "repo-b");
  await put("sc", ["gamma0"], null); // no repo: never "the current repo"
  await drain(db);
  const s = await snap();
  expect(s.gen).toBe(1);
  expect(s.text.startsWith(FRAMING)).toBe(true);
  expect(s.text.endsWith(TOOLS)).toBe(true);
  expect(s.text).toContain("S(alpha0) + S(alpha1) + S(alpha2) + S(alpha3)");
  expect(s.text).toContain(`[session:${a}]`);
  expect(s.text).toMatch(/^- .*repo-b/m);
  expect(s.text).toMatch(/^- .*S\(gamma0\)/m); // listed as another repo's (or no repo's) title line
  expect(s.text).not.toContain("## card S(gamma0)");
});

test("served unchanged afterwards: new history and a card made later do not change it; refresh does, once per id", async () => {
  await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  const s1 = await snap();
  await put("sd", ["delta0"]);
  await drain(db);
  overview = () => "a newer card";
  await db`delete from session_card`;
  await drain(db);
  const s2 = await snap();
  expect(s2.text).toBe(s1.text);
  expect(s2.gen).toBe(1);
  const r1 = await snap({ refresh_id: "r1" });
  expect(r1.gen).toBe(2);
  expect(r1.text).toContain("S(delta0)");
  const r1again = await snap({ refresh_id: "r1" }); // a retried refresh: the same snapshot
  expect(r1again.gen).toBe(2);
  expect(r1again.text).toBe(r1.text);
  expect((await snap()).gen).toBe(2);
});

test("concurrent first requests save one snapshot", async () => {
  await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  const all = await Promise.all(Array.from({ length: 6 }, () => snap()));
  expect(new Set(all.map((x) => x.text)).size).toBe(1);
  expect(all.every((x) => x.gen === 1)).toBe(true);
  expect((await db`select count(*)::int n from start_snapshot`)[0].n).toBe(1);
});

test("a forgotten session's block is gone as a whole, from storage too; the rest stays frozen", async () => {
  const a = await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await put("se", ["eps0", "eps1", "eps2", "eps3"]);
  await drain(db);
  const s1 = await snap();
  expect(s1.text).toContain("S(alpha0)");
  expect(s1.text).toContain("S(eps0)");
  await forgetSource(db, a);
  const s2 = await snap();
  expect(s2.text).not.toContain("alpha");
  expect(s2.text).toContain("S(eps0)");
  expect(s2.gen).toBe(1); // not rebuilt: no new history taken in
  expect(JSON.stringify(await db`select blocks from start_snapshot`)).not.toContain("alpha");
});

test("a revision of a session in the snapshot drops its block until a refresh", async () => {
  await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  expect((await snap()).text).toContain("S(alpha1)");
  await put("sa", ["alpha0", "alpha1 changed", "alpha2", "alpha3"]);
  const s = await snap();
  expect(s.text).not.toContain("alpha");
  expect(s.dropped).toBe(1);
  await drain(db);
  expect((await snap()).text).not.toContain("alpha"); // still frozen
  expect((await snap({ refresh_id: "r" })).text).toContain("S(alpha1 changed)");
});

test("a revoked read scope hides its blocks; the querying session not authorized → refused", async () => {
  await put("sw", ["work0", "work1", "work2", "work3"], "repo-a", "work");
  await db`insert into grant_rule values ('work', 'snapshot', 'memex')`;
  await drain(db);
  expect((await snap()).text).toContain("S(work0)");
  await db`delete from grant_rule where scope = 'work' and purpose = 'snapshot'`;
  expect((await snap()).text).not.toContain("work0");
  await revokeGrant(db, "personal", "snapshot", "memex");
  await expect(snap()).rejects.toThrow("not authorized");
});

test("budget: at most 6 KB of UTF-8 in total; whole blocks only, never a cut character", async () => {
  overview = (parts) => "中".repeat(500) + parts.join(""); // 1500+ bytes per card overview
  for (let i = 0; i < 8; i++) await put("b" + i, [`w${i}a`, `w${i}b`, `w${i}c`, `w${i}d`]);
  await drain(db);
  const s = await snap();
  expect(s.bytes).toBeLessThanOrEqual(BUDGET);
  expect(Buffer.byteLength(s.text)).toBe(s.bytes);
  expect(Buffer.from(s.text).toString()).toBe(s.text);
  expect(s.text.match(/^## /gm)!.length).toBeLessThanOrEqual(3);
  expect(s.text.endsWith(TOOLS)).toBe(true);
});

test("a read never calls a model; degraded cards are asked for in the background", async () => {
  process.env.MEMEX_FOREST_HOT_DAYS = "-1";
  try {
    await put("cold", ["c0", "c1", "c2"]);
    await drain(db);
  } finally {
    delete process.env.MEMEX_FOREST_HOT_DAYS;
  }
  const k = requests.length;
  const s = await snap();
  expect(requests.length).toBe(k);
  expect(s.text).toContain("(summary pending)");
  expect((await db`select 1 from forest_access`).length).toBe(1);
});

test("two first requests built at once: the one saved second does not replace the first", async () => {
  await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  const { snapshotHooks } = await import("./snapshot.ts");
  let gate!: () => void;
  const open = new Promise<void>((r) => (gate = r));
  let waiting = 0;
  snapshotHooks.beforeSave = async () => {
    if (waiting++ === 0) await open; // the first builder waits; the second saves first
  };
  try {
    const first = snap();
    while (waiting === 0) await Bun.sleep(5);
    // Different history before the second builds: if it replaced the first save, the texts would differ.
    await put("sz", ["zeta0"]);
    await drain(db);
    const second = await snap();
    gate();
    const f = await first;
    expect(f.text).toBe(second.text); // the first request serves what was stored, not its own build
    expect(f.gen).toBe(1);
  } finally {
    snapshotHooks.beforeSave = async () => {};
  }
});

test("a withdrawal between building and saving: the built snapshot is not saved", async () => {
  const a = await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  const { snapshotHooks } = await import("./snapshot.ts");
  let once = false;
  snapshotHooks.beforeSave = async () => {
    if (once) return;
    once = true;
    await put("sa", ["alpha0", "alpha1 changed", "alpha2", "alpha3"]); // a revision: its version moves
  };
  try {
    const s = await snap();
    expect(s.text).not.toContain("S(alpha1)"); // rebuilt without the withdrawn card
    expect(JSON.stringify(await db`select blocks from start_snapshot`)).not.toContain("S(alpha1)");
  } finally {
    snapshotHooks.beforeSave = async () => {};
  }
  void a;
});

test("budget: a block that does not fit is left out while a later smaller one still fits", async () => {
  // Three current-repo cards of ~1.6 KB (overview at its cap) and many short title lines of another repo:
  // 3 cards + framing + tools ≈ 5.3 KB; the titles fill what is left, then stop — the budget is the limit.
  overview = () => "中".repeat(510); // 1530 bytes, under the card cap
  for (let i = 0; i < 3; i++) await put("big" + i, [`b${i}a`, `b${i}b`, `b${i}c`, `b${i}d`]);
  for (let i = 0; i < 40; i++) await put("o" + i, [`o${i}`], "repo-b");
  await drain(db);
  const s = await snap();
  expect(s.bytes).toBeLessThanOrEqual(BUDGET);
  expect(s.text.match(/^## /gm)?.length).toBe(3);
  const titles = s.text.match(/^- /gm)?.length ?? 0;
  expect(titles).toBeGreaterThan(0);
  expect(titles).toBeLessThan(40); // not all fit
  expect(s.text.endsWith(TOOLS)).toBe(true);
});

test("budget: a block too large for what is left is skipped, a smaller one after it still goes in", async () => {
  // Other repos' title lines, newest first: the newest one's title is very long (a degraded card's title is
  // the first line of its overview: one long leaf summary), the older one short.
  // Three current-repo cards take ~5 KB first; what is left fits a short line, not a ~1.6 KB one.
  overview = () => "中".repeat(480);
  for (let i = 0; i < 3; i++) await put("big" + i, [`b${i}a`, `b${i}b`, `b${i}c`, `b${i}d`]);
  await put("small", ["s0"], "repo-b");
  await Bun.sleep(5);
  const L = await put("long", ["long0"], "repo-b");
  await drain(db);
  // Its summary made very long (as a future prompt might allow), its card not made yet: a degraded title
  // from that summary's first line, too long for the budget.
  await db`update span_summary ss set text = ${"L".repeat(7000)} from span s where s.id = ss.span and s.source = ${L}`;
  await db`delete from session_card where source = ${L}`;
  const s = await snap();
  expect(s.bytes).toBeLessThanOrEqual(BUDGET);
  expect(s.text).not.toContain("LLLL");
  expect(s.text).toContain("S(s0)");
});

test("an old refresh retried after a newer one does not refresh again", async () => {
  await put("sa", ["alpha0", "alpha1", "alpha2", "alpha3"]);
  await drain(db);
  await snap();
  const a = await snap({ refresh_id: "A" });
  const b = await snap({ refresh_id: "B" });
  expect([a.gen, b.gen]).toEqual([2, 3]);
  await put("sn", ["newer0"]);
  await drain(db);
  const again = await snap({ refresh_id: "A" });
  expect(again.gen).toBe(3);
  expect(again.text).not.toContain("newer0");
});

test("a snapshot asks in the background only for the cards it shows, not for every cold session it looked at", async () => {
  process.env.MEMEX_FOREST_HOT_DAYS = "-1";
  try {
    const shown = await put("shown", ["s0", "s1", "s2"]); // current repo: shown (degraded)
    const old = await put("old", ["o0", "o1", "o2"], "repo-b");
    await db`update body set raw = replace(raw, ${T().slice(0, 4)}, '2020') where source = ${old}`; // older than 48 h, other repo
    await drain(db);
    await db`delete from forest_access`;
    const s = await snap();
    expect(s.text).toContain(`[session:${shown}]`);
    expect(s.text).not.toContain(`[session:${old}]`);
    const wanted = (await db`select source from forest_access`).map((r: any) => r.source);
    expect(wanted).toEqual([shown]);
  } finally {
    delete process.env.MEMEX_FOREST_HOT_DAYS;
  }
});
