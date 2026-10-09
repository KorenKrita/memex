// Search (DESIGN §6.3, §4.3, §7) over real Postgres, HTTP via node:http, and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import net from "node:net";
import { connect, migrate, bump, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { EMBED_DIM, revokeGrant } from "./derive.ts";
import { addApiKey, Guarded, httpHooks, route, serve, transport } from "./http.ts";
import { search, zoom, grep, GUARANTEED } from "./search.ts";
import { forgetSource } from "./forget.ts";
import { drain } from "./worker.ts";
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let requests: { path: string; body: any }[] = [];
let srv: ReturnType<typeof Bun.serve>;
let api: Awaited<ReturnType<typeof serve>>;
let key: string;
// Fake endpoint: summaries echo the first user line of the input; embeddings are a bag of marker words;
// rerank scores documents containing "ANSWER" high.
const MARKERS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];
const vecOf = (t: string) => {
  const v = new Array<number>(EMBED_DIM).fill(0);
  MARKERS.forEach((m, i) => t.toLowerCase().includes(m) && (v[i] = 1));
  if (!v.some((x) => x !== 0)) v[EMBED_DIM - 1] = 1;
  return v;
};
let summaryOf = (input: string) => /kind="user">\n?([^<]*)/.exec(input.slice(input.indexOf("<input>")))?.[1]?.trim() ?? "s";
let rerankStatus = 200;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      requests.push({ path, body });
      if (path.endsWith("/chat/completions")) {
        const s = { summary: summaryOf(body.messages[1].content), user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] };
        return Response.json({ choices: [{ message: { content: JSON.stringify(s) } }] });
      }
      if (path.endsWith("/rerank")) {
        if (rerankStatus !== 200) return new Response("no", { status: rerankStatus });
        return Response.json({ data: body.documents.map((d: string, index: number) => ({ index, relevance_score: d.includes("ANSWER") ? 0.9 : 0.1 })) });
      }
      return Response.json({ data: body.input.map((t: string) => ({ embedding: vecOf(t) })) });
    },
  });
  const base = `http://127.0.0.1:${srv.port}/v1`;
  Object.assign(process.env, {
    MEMEX_LLM_URL: base, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k",
    MEMEX_EMBED_URL: base, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k",
    MEMEX_RERANK_URL: base + "/rerank", MEMEX_RERANK_MODEL: "fake-rerank", MEMEX_RERANK_KEY: "k",
  });
  api = await serve(db, { port: 0 });
});
afterAll(async () => {
  srv.stop(true);
  await api.stop();
  await db.close();
});
const ALL = [
  ["personal", "sync", "memex"], ["personal", "derive", "llm"], ["personal", "derive", "embedding"],
  ["personal", "tool", "memex"], ["personal", "tool", "embedding"], ["personal", "tool", "rerank"],
  ["work", "sync", "memex"], ["work", "derive", "llm"], ["work", "derive", "embedding"],
];
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, search_trace restart identity cascade`);
  for (const [s, p, d] of ALL) await db`insert into grant_rule values (${s}, ${p}, ${d})`;
  process.env.MEMEX_IDLE_CLOSE_MS = "0"; // every tail closes: everything is summarized
  requests = [];
  rerankStatus = 200;
  summaryOf = (input) => /kind="user">\n?([^<]*)/.exec(input.slice(input.indexOf("<input>")))?.[1]?.trim() ?? "s";
  httpHooks.beforeDeliver = async () => {};
  httpHooks.beforeGuard = async () => {};
  key = await addApiKey(db, "t");
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
const header = (id: string, extra = {}) => JSON.stringify({ type: "session", version: 3, id, timestamp: T, cwd: "/x", ...extra });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: T, message: { role, content: [{ type: "text", text }] } });
let commitN = 0;
async function put(native: string, lines: string[], scope = "personal", parent: string | null = null, leaf?: string) {
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope, parent };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  const r = await commit(db, { leaseId: l, commitId: `s${commitN++}`, length: lines.length, leaf, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(r.ok).toBe(true);
  return decl.source;
}
/** A linear session: exchanges [user, assistant] with ids u<i>/a<i>. */
const linear = (n: string, ex: [string, string][]) => {
  const out = [header(n)];
  let prev: string | null = null;
  ex.forEach(([u, a], i) => {
    out.push(msg("user", `u${i}`, prev, u), msg("assistant", `a${i}`, `u${i}`, a));
    prev = `a${i}`;
  });
  return out;
};
const Q = (query: string, extra: object = {}) => ({ source: sourceIdOf("pi", "asker"), scope: "personal", purpose: "tool", query, dests: ["embedding", "rerank"], ...extra });
const body = async (b: object) => (await search(db, b)).body as any;
const http = () => transport(`http://127.0.0.1:${api.port}`, key);

test("three channels; a lexical-only answer reaches rerank through its guaranteed slot", async () => {
  // 40 sessions on the same topic ("alpha" in summaries and vectors), one holding the rare token in a reply.
  for (let i = 0; i < 40; i++) await put(`t${i}`, linear(`t${i}`, [[`alpha topic ${i}`, `noise ${i}`]]));
  await put("ans", linear("ans", [["unrelated beta", "ANSWER the port is zx-7781"]]));
  await drain(db);
  const r = await body(Q("alpha zx-7781"));
  expect(r.rerank_skipped).toBeNull();
  expect(r.embedding_skipped).toBeNull();
  expect(r.results[0].excerpt.text).toContain("zx-7781");
  expect(r.results[0].key.startsWith("record:")).toBe(true);
  const [t] = await db`select data from search_trace`;
  expect(t.data.channels.lex).toBe(41); // every user message says alpha; the rare token ranks first
  expect(t.data.channels.ft).toBe(40);
  expect(t.data.candidates.length).toBe(40);
  const rare = t.data.candidates.find((c: any) => c.ranks.lex === 1);
  expect(rare.ranks.ft).toBeUndefined();
  expect(rare.final).toBe(1);
  expect(GUARANTEED).toBeGreaterThan(0);
});

test("guaranteed slots: the answer only the lexical channel ranks first still reaches rerank", async () => {
  // 45 same-topic sessions fill both summary channels; the answer's summary does not match the query and
  // its raw text has only the rare token, so RRF alone ranks it after 40 two-channel candidates.
  for (let i = 0; i < 45; i++) await put(`t${i}`, linear(`t${i}`, [[`alpha topic ${i}`, `noise ${i}`]]));
  await put("ans", linear("ans", [["unrelated beta", "ANSWER the port is zx-7781"]]));
  await drain(db);
  const r = await body(Q("alpha zx-7781"));
  const [t] = await db`select data from search_trace`;
  const rare = t.data.candidates.find((c: any) => c.ranks.lex === 1);
  expect(rare).toBeTruthy();
  const rrfRank = t.data.candidates.filter((c: any) => c.fused > rare.fused).length + 1;
  expect(rrfRank).toBeGreaterThan(8);
  expect(r.results[0].excerpt.text).toContain("zx-7781");
});

test("no grant for the querying session: refused before any text leaves", async () => {
  await db`delete from grant_rule where scope = 'personal' and purpose = 'tool'`;
  await expect(search(db, Q("alpha"))).rejects.toThrow("not authorized");
  expect(requests.length).toBe(0);
  const res = await fetch(`http://127.0.0.1:${api.port}/v1/search`, { method: "POST", headers: { authorization: `Bearer ${key}` }, body: JSON.stringify(Q("alpha")) });
  expect(res.status).toBe(403);
});

test("results only from readable scopes; embedding and rerank each need their own grant", async () => {
  await put("p1", linear("p1", [["alpha personal", "ANSWER p"]]));
  await put("w1", linear("w1", [["alpha work", "ANSWER w"]]), "work");
  await drain(db);
  requests = [];
  let r = await body(Q("alpha"));
  expect(r.results.map((x: any) => x.scope)).toEqual(expect.arrayContaining(["personal"]));
  expect(r.results.every((x: any) => x.scope === "personal")).toBe(true);
  // Revoke the query's embedding grant: no query text to the embedding endpoint, vector channel skipped.
  await revokeGrant(db, "personal", "tool", "embedding");
  requests = [];
  r = await body(Q("alpha"));
  expect(r.embedding_skipped).toBe("not-granted");
  expect(requests.filter((x) => x.path.endsWith("/embeddings")).length).toBe(0);
  // Rerank not requested by the plugin: skipped, not injectable.
  requests = [];
  r = await body(Q("alpha", { dests: [] }));
  expect(r.rerank_skipped).toBe("not-requested");
  expect(r.results.every((x: any) => !x.injectable)).toBe(true);
  expect(requests.filter((x) => x.path.endsWith("/rerank")).length).toBe(0);
});

test("rerank is skipped when any candidate's scope lacks the rerank grant", async () => {
  await db`insert into grant_rule values ('work', 'tool', 'memex')`;
  await put("p1", linear("p1", [["alpha personal", "x"]]));
  await put("w1", linear("w1", [["alpha work", "y"]]), "work");
  await drain(db);
  requests = [];
  const r = await body(Q("alpha"));
  expect(r.results.some((x: any) => x.scope === "work")).toBe(true);
  expect(r.rerank_skipped).toBe("not-granted");
  expect(requests.filter((x) => x.path.endsWith("/rerank")).length).toBe(0);
});

test("forgotten canonical records and stale projections are filtered before the channel cut", async () => {
  const s = await put("f1", linear("f1", [["alpha secret-one", "r"], ["beta", "r2"]]));
  await drain(db);
  await db`insert into tomb values (${"canon:" + s + ":u0"})`;
  const has = (r: any) => JSON.stringify(r.results).includes("secret-one");
  let r = await body(Q("secret-one"));
  expect(has(r)).toBe(false);
  // A newer L0 commit makes the projection stale: nothing from it until it is projected again.
  await db`delete from tomb`;
  await put("f1", linear("f1", [["alpha secret-one", "r"], ["beta", "r2"], ["gamma", "r3"]]));
  r = await body(Q("secret-one"));
  expect(r.results.length).toBe(0);
  await drain(db);
  r = await body(Q("secret-one"));
  expect(has(r)).toBe(true);
});

test("records still in the asker's context are excluded, older ones of the same session are not", async () => {
  const asker = linear("asker", [["alpha old fact", "o"], ["alpha recent", "n"]]);
  await put("asker", asker);
  await drain(db);
  const r = await body(Q("alpha", { inContext: ["u1", "a1"], dests: [] }));
  const ex = r.results.map((x: any) => x.excerpt?.text ?? x.reps[0]?.text).join("|");
  expect(ex).toContain("old fact");
  expect(ex).not.toContain("recent");
});

test("a correction in the next exchange is attached to the hit", async () => {
  await put("c1", linear("c1", [["set the port to 8080 alpha", "ok 8080"], ["不对，改成 9090", "done 9090"]]));
  await drain(db);
  const r = await body(Q("port 8080"));
  const hit = r.results.find((x: any) => (x.excerpt?.text ?? "").includes("8080") || x.reps.some((p: any) => p.text.includes("8080")));
  expect(hit.attachments.some((a: any) => a.what === "next" && a.text.includes("改成 9090"))).toBe(true);
  const sent = requests.filter((x) => x.path.endsWith("/rerank")).at(-1)!.body.documents.join("\n");
  expect(sent).toContain("改成 9090");
});

test("over the rerank budget: attachments are dropped and marked incomplete", async () => {
  // Base documents (two 590-byte summaries collapse to one, a short excerpt) fit; with the next-exchange
  // attachments (1,000 + 400 bytes each) 40 candidates exceed 64 KB.
  summaryOf = (input) => (input.includes("<input>") && /alpha \d+/.test(input.slice(input.indexOf("<input>"))) ? "alpha " + "y".repeat(590) : "z");
  for (let i = 0; i < 40; i++) await put(`b${i}`, linear(`b${i}`, [[`alpha ${i}`, "r"], [`n${i} ${"x".repeat(990)}`, "w".repeat(400)]]));
  await drain(db);
  const r = await body(Q("alpha", { limit: 40 }));
  expect(r.rerank_skipped).toBeNull();
  const sent = requests.filter((x) => x.path.endsWith("/rerank")).at(-1)!.body.documents as string[];
  expect(sent.reduce((a, d) => a + Buffer.byteLength(d), 0)).toBeLessThanOrEqual(64 * 1024);
  const [t] = await db`select data from search_trace`;
  expect(t.data.candidates.length).toBe(40);
  // Attachments go from the lowest-ranked candidates up; those are marked incomplete and not injectable.
  const dropped = t.data.candidates.filter((c: any) => c.incomplete.includes("attachments-dropped"));
  expect(dropped.length).toBeGreaterThan(0);
  expect(sent.reduce((a, d) => a + Buffer.byteLength(d), 0)).toBeGreaterThan(60 * 1024); // only what was needed
  const out = r.results.filter((x: any) => x.incomplete.includes("attachments-dropped"));
  expect(out.length).toBe(dropped.length);
  // Lowest initial-recall ranks lose attachments first: every dropped one ranks below every kept one.
  const first = t.data.candidates.findIndex((c: any) => c.incomplete.includes("attachments-dropped"));
  expect(t.data.candidates.slice(first).every((c: any) => c.attached === 0)).toBe(true);
  expect(t.data.candidates.slice(0, first).some((c: any) => c.attached > 0)).toBe(true);
  expect(out.every((x: any) => !x.injectable && x.attachments.length === 0)).toBe(true);
  expect(r.results.filter((x: any) => !x.incomplete.length).every((x: any) => x.injectable)).toBe(true);
  expect(sent.filter((d) => d.includes("[next exchange]")).length).toBeGreaterThan(0);
  expect(sent.filter((d) => d.includes("[next exchange]")).length).toBeLessThan(40);
});

test("both summary channels' representations of a unit reach rerank; a fork prefix labels on-path", async () => {
  // Parent: S = [u0 a0 t a1] closed; the child forks inside S, so P = [u0 a0] is S's extra representation.
  const P = sourceIdOf("pi", "par");
  const pl = [header("par"), msg("user", "u0", null, "alpha prefix fact"), msg("assistant", "a0", "u0", "first part"), msg("assistant", "a1", "a0", "beta later ANSWER")];
  summaryOf = (input) => (input.includes("beta later") ? "S whole: alpha and beta" : input.includes("first part") ? "P only: alpha prefix" : "other");
  await put("par", pl);
  await drain(db);
  const kid = [header("kid", { parentSession: "/x/par.jsonl" }), pl[1], pl[2], msg("assistant", "b1", "a0", "child branch"), msg("user", "k1", "b1", "gamma child")];
  await put("kid", kid, "personal", P, "k1");
  await drain(db);
  const units = await db`select unit, count(*)::int n from span_summary group by unit having count(*) > 1`;
  expect(units.length).toBe(1);
  const r = await body(Q("alpha"));
  const u = r.results.find((x: any) => x.key === "unit:" + units[0].unit);
  expect(u.reps.map((p: any) => p.text)).toEqual(expect.arrayContaining(["S whole: alpha and beta", "P only: alpha prefix"]));
  const sent = requests.filter((x) => x.path.endsWith("/rerank")).at(-1)!.body.documents.join("\n");
  expect(sent).toContain("P only: alpha prefix");
  // Labels are against the result session's view: S's parent view sees S whole (on-path there).
  expect(u.path.label).toBe("on-path");
});

test("a forget committed between the check and delivery: the response is recomputed without it", async () => {
  const s = await put("g1", linear("g1", [["alpha gone-soon", "r"]]));
  await drain(db);
  let fired = false;
  httpHooks.beforeDeliver = async () => {
    if (fired) return;
    fired = true;
    // A writer needs the exclusive barrier: it must wait until the response is handed over.
    const w = db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      await tx`insert into tomb values (${"source:" + s})`;
      await bump(tx, "s:" + s);
    });
    const raced = await Promise.race([w.then(() => "writer"), new Promise((ok) => setTimeout(() => ok("held"), 300))]);
    expect(raced).toBe("held");
  };
  const r = await http()("POST", "/v1/search", Q("gone-soon"));
  expect(r.results.length).toBeGreaterThan(0); // delivered before the forget
  await new Promise((ok) => setTimeout(ok, 50));
  const r2 = await http()("POST", "/v1/search", Q("gone-soon"));
  expect(r2.results.length).toBe(0);
});

test("a forget that commits after the snapshot but before delivery is not answered from the old state", async () => {
  const s = await put("h1", linear("h1", [["alpha stale-answer", "r"]]));
  await drain(db);
  const g = await search(db, Q("stale-answer"));
  expect((g.body as any).results.length).toBeGreaterThan(0);
  await db.begin(async (tx: any) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await tx`insert into tomb values (${"source:" + s})`;
    await bump(tx, "s:" + s);
  });
  // The deps compared at delivery no longer hold; the recomputed body has nothing from h1.
  const { unchanged } = await import("./derive.ts");
  expect(await db.begin((tx: any) => unchanged(tx, g.deps))).toBe(false);
  expect(((await g.again()).body as any).results.length).toBe(0);
});

test("base documents over the budget: rerank is skipped, no candidate is dropped", async () => {
  summaryOf = (input) => "alpha " + "y".repeat(590);
  for (let i = 0; i < 40; i++) await put(`o${i}`, linear(`o${i}`, [[`${"q".repeat(1500)} alpha ${i} ${"q".repeat(1500)}`, "r"]]));
  await drain(db);
  requests = [];
  const r = await body(Q("alpha", { limit: 40 }));
  expect(r.rerank_skipped).toBe("base-over-budget");
  expect(requests.filter((x) => x.path.endsWith("/rerank")).length).toBe(0);
  expect(r.results.length).toBe(40);
  expect(r.results.every((x: any) => !x.injectable)).toBe(true);
});

test("over HTTP: a forget committed after the result was computed is not delivered from the old state", async () => {
  const s = await put("d1", linear("d1", [["alpha deliver-check", "r"]]));
  await drain(db);
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      await tx`insert into tomb values (${"source:" + s})`;
      await bump(tx, "s:" + s);
    });
  };
  const r = await http()("POST", "/v1/search", Q("deliver-check"));
  expect(fired).toBe(2); // the first delivery found its vector stale and recomputed
  expect(JSON.stringify(r)).not.toContain("deliver-check");
});

// ---------- zoom / grep ----------

const T0 = (extra: object = {}) => ({ source: sourceIdOf("pi", "asker"), scope: "personal", purpose: "tool", ...extra });

test("grep: exact substring with record positions; scope and forget filter it; regex errors are 400", async () => {
  const s = await put("g1", linear("g1", [["see /etc/memex.conf line", "r"], ["other", "the path /etc/memex.conf again"]]));
  await put("w1", linear("w1", [["/etc/memex.conf at work", "r"]]), "work");
  await drain(db);
  let r = (await grep(db, T0({ pattern: "/etc/memex.conf" }))).body as any;
  expect(r.hits.length).toBe(2);
  expect(r.hits.every((h: any) => h.source === s && h.snippet.includes("/etc/memex.conf"))).toBe(true);
  r = (await grep(db, T0({ pattern: "memex\\.conf a", regex: true }))).body as any;
  expect(r.hits.length).toBe(1);
  await expect(grep(db, T0({ pattern: "(", regex: true }))).rejects.toThrow("pattern invalid");
  await db`insert into tomb values (${"canon:" + s + ":u0"})`;
  r = (await grep(db, T0({ pattern: "/etc/memex.conf" }))).body as any;
  expect(r.hits.map((h: any) => h.record)).toEqual(["record:" + s + ":a1"]);
  r = (await grep(db, T0({ pattern: "memex\\.conf", regex: true }))).body as any;
  expect(r.hits.map((h: any) => h.record)).toEqual(["record:" + s + ":a1"]);
  // Not authorized to query: refused.
  await db`delete from grant_rule where scope = 'personal' and purpose = 'tool'`;
  await expect(grep(db, T0({ pattern: "x" }))).rejects.toThrow("not authorized");
});

test("zoom: a span's records with paging and neighbours on its view; a record by canonical id", async () => {
  const s = await put("z1", linear("z1", [["first " + "f".repeat(300), "r1"], ["second", "r2"], ["third", "r3"]]));
  await drain(db);
  const spans = (await db`select id, keys from span where source = ${s}`) as any[];
  const mid = spans.find((x) => x.keys[0].startsWith("z1/u1:"))!;
  let r = (await zoom(db, T0({ id: "span:" + mid.id }))).body as any;
  expect(r.records.map((x: any) => x.text)).toEqual(["second", "r2"]);
  expect(r.before).toBeTruthy();
  expect(r.after).toBeTruthy();
  const first = (await zoom(db, T0({ id: r.before }))).body as any;
  expect(first.records[0].text.startsWith("first")).toBe(true);
  expect(first.before).toBeNull();
  // Paging inside a span.
  const p1 = (await zoom(db, T0({ id: r.before, limit: 100 }))).body as any;
  expect(p1.more).toBe(true);
  expect(Buffer.byteLength(p1.records.map((x: any) => x.text).join(""))).toBe(100);
  const p2 = (await zoom(db, T0({ id: r.before, limit: 100, offset: p1.next_offset }))).body as any;
  expect(p2.records[0].text).toBe("f".repeat(100));
  // A record by canonical id.
  const rec = (await zoom(db, T0({ id: "record:" + s + ":a2" }))).body as any;
  expect(rec.text).toBe("r3");
  // Forgetting one of the span's records makes the span unreadable.
  await db`insert into tomb values (${"canon:" + s + ":u1"})`;
  await expect(zoom(db, T0({ id: "span:" + mid.id }))).rejects.toThrow("not found or not readable");
});

test("zoom over HTTP: another scope's span is not readable", async () => {
  const w = await put("wz", linear("wz", [["work secret", "r"]]), "work");
  await drain(db);
  const [sp] = await db`select id from span where source = ${w}`;
  const res = await fetch(`http://127.0.0.1:${api.port}/v1/zoom`, { method: "POST", headers: { authorization: `Bearer ${key}` }, body: JSON.stringify(T0({ id: "span:" + sp.id })) });
  expect(res.status).toBe(404);
  expect(await res.text()).not.toContain("work secret");
});

test("zoom over HTTP: a forget committed before delivery is not answered from the old state", async () => {
  const s = await put("zf", linear("zf", [["zoom-forget-me", "r"]]));
  await drain(db);
  const [sp] = await db`select id from span where source = ${s} limit 1`;
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await db.begin(async (tx: any) => {
      await tx`select pg_advisory_xact_lock(${BARRIER})`;
      await tx`insert into tomb values (${"source:" + s})`;
      await bump(tx, "s:" + s);
    });
  };
  const res = await fetch(`http://127.0.0.1:${api.port}/v1/zoom`, { method: "POST", headers: { authorization: `Bearer ${key}` }, body: JSON.stringify(T0({ id: "span:" + sp.id })) });
  expect(fired).toBe(1); // the first delivery found its vector stale; recomputing finds nothing readable
  expect(res.status).toBe(404);
  expect(await res.text()).not.toContain("zoom-forget-me");
});

// ---------- forget (§7) ----------

const hasText = async (t: string) => {
  const g = (await grep(db, T0({ pattern: t }))).body as any;
  const s = (await search(db, Q(t, { dests: [] }))).body as any;
  return g.hits.length > 0 || JSON.stringify(s.results).includes(t);
};

test("forget: nothing of the session is searchable, stored, or accepted again", async () => {
  const s = await put("fg", linear("fg", [["forget-me-token alpha", "reply"]]));
  await put("keep", linear("keep", [["keep-me-token alpha", "reply"]]));
  await drain(db);
  await search(db, Q("forget-me-token", { source: s })); // a trace bound to the forgotten session
  expect(await hasText("forget-me-token")).toBe(true);
  const r = await http()("POST", "/v1/forget", { source: s });
  expect(r.records).toBe(3);
  expect(await hasText("forget-me-token")).toBe(false);
  expect(await hasText("keep-me-token")).toBe(true);
  for (const t of ["body", "record", "projection", "lexical", "view", "projected", "span", "search_trace"])
    expect((await db.unsafe(`select count(*)::int n from ${t} where source = $1`, [s]))[0].n).toBe(0);
  expect((await db`select count(*)::int n from span_summary ss where not exists (select 1 from span where id = ss.span)`)[0].n).toBe(0);
  // Sync refuses it again; the worker derives nothing more.
  await expect(requestLease(db, { source: s, harness: "pi", nativeId: "fg", scope: "personal" }, "m1")).rejects.toThrow("forgotten");
  requests = [];
  await db`insert into job (kind, key) values ('project', ${s})`; // a stray queued projection
  await drain(db);
  expect(requests.length).toBe(0);
  expect((await db`select count(*)::int n from projected where source = ${s}`)[0].n).toBe(0);
});

test("forget a fork parent: the child keeps its own records, the copied prefix is gone and stays gone", async () => {
  const P = sourceIdOf("pi", "fp");
  const pl = [header("fp"), msg("user", "u0", null, "parent-only-secret"), msg("assistant", "a0", "u0", "parent reply")];
  await put("fp", pl);
  await drain(db);
  const kid = [header("fk", { parentSession: "/x/fp.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "child-own-fact"), msg("assistant", "k2", "k1", "child reply")];
  const K = await put("fk", kid, "personal", P);
  await drain(db);
  expect(await hasText("parent-only-secret")).toBe(true);
  expect(await hasText("child-own-fact")).toBe(true);
  const r = await forgetSource(db, P);
  expect(r.copies).toBe(2);
  const mark = requests.length;
  await drain(db);
  expect(await hasText("parent-only-secret")).toBe(false);
  expect(await hasText("child-own-fact")).toBe(true);
  // The child's collector syncs the same file again: the copies are refused, not revived as the child's own.
  const l = await requestLease(db, { source: K, harness: "pi", nativeId: "fk", scope: "personal", parent: P }, "m1");
  await tryGrant(db, l);
  const c = await commit(db, { leaseId: l, commitId: "again", length: kid.length, changes: kid.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(c).toMatchObject({ ok: true, refused: 2 });
  await drain(db);
  expect(await hasText("parent-only-secret")).toBe(false);
  expect(await hasText("child-own-fact")).toBe(true);
  // Summaries sent after the forget never carry the parent's text.
  const after = requests.slice(mark).filter((x) => x.path.endsWith("/chat/completions"));
  expect(after.length).toBeGreaterThan(0); // the child's spans were summarized again
  expect(after.every((x) => !JSON.stringify(x.body).includes("parent-only-secret"))).toBe(true);
});

test("forget racing a search delivery: the response computed before it is recomputed", async () => {
  const s = await put("fr", linear("fr", [["race-token alpha", "r"]]));
  await drain(db);
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await forgetSource(db, s);
  };
  const r = await http()("POST", "/v1/search", Q("race-token", { dests: [] }));
  expect(fired).toBe(2);
  expect(JSON.stringify(r)).not.toContain("race-token");
});

test("forget tombs every native record: a fork imported later cannot revive them through its prefix", async () => {
  const P = sourceIdOf("pi", "lp");
  const pl = [header("lp"), msg("user", "u0", null, "late-parent-secret"), msg("assistant", "a0", "u0", "r")];
  await put("lp", pl);
  await drain(db);
  await forgetSource(db, P);
  // A child that forked before the forget is imported only now. Its copies resolve to canon:P:u0/a0.
  const tombs = (await db`select key from tomb where key like ${"canon:" + P + ":%"}`).map((r: any) => r.key).sort();
  expect(tombs).toEqual(["canon:" + P + ":a0", "canon:" + P + ":u0", "canon:" + P + ":header"].sort());
  const kid = [header("lk", { parentSession: "/x/lp.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "late-child-own")];
  await put("lk", kid, "personal", P);
  await drain(db);
  expect(await hasText("late-parent-secret")).toBe(false);
  expect(await hasText("late-child-own")).toBe(true);
});

// ---------- delivery boundary (§5.5 "对外响应") ----------

// A guarded test route with a body far larger than the socket buffers.
route("POST", /^\/v1\/test-big$/, async () => {
  const g: Guarded = new Guarded({ "d:memex": 1 }, { blob: "z".repeat(32 * 1024 * 1024) }, async () => g);
  return g;
});
const rawPost = async (path: string) => {
  const sock = net.connect(api.port, "127.0.0.1");
  await new Promise((ok) => sock.once("connect", ok));
  sock.write(`POST ${path} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${key}\r\nContent-Length: 2\r\n\r\n{}`);
  sock.pause();
  return sock;
};
const writerWait = async () => {
  const t0 = Date.now();
  await db.begin(async (tx: any) => void (await tx`select pg_advisory_xact_lock(${BARRIER})`));
  return Date.now() - t0;
};

test("a client that does not read: the barrier is held until hand-over; the deadline cancels the response", async () => {
  const sock = await rawPost("/v1/test-big");
  await Bun.sleep(300);
  const waited = await writerWait();
  expect(waited).toBeGreaterThan(3_000); // not released when Bun merely had the bytes
  expect(waited).toBeLessThan(7_000); // the 5 s deadline released it
  // Cancelled: the connection ends; the client never gets the whole body.
  let got = 0;
  sock.on("data", (d) => (got += d.length));
  const closed = new Promise((ok) => sock.once("close", ok));
  sock.resume();
  await Promise.race([closed, Bun.sleep(3_000)]);
  expect(got).toBeLessThan(32 * 1024 * 1024);
  sock.destroy();
}, 20_000);

test("a client that resumes reading: the hand-over completes and releases the barrier", async () => {
  const sock = await rawPost("/v1/test-big");
  await Bun.sleep(300);
  let got = 0;
  sock.on("data", (d) => (got += d.length));
  const w = writerWait();
  await Bun.sleep(1_000);
  sock.resume();
  const waited = await w;
  expect(waited).toBeGreaterThan(900); // held while the client did not read
  expect(waited).toBeLessThan(4_000); // released once the body was taken, before the deadline
  await Bun.sleep(300);
  expect(got).toBeGreaterThan(32 * 1024 * 1024);
  sock.destroy();
}, 20_000);

test("a client that disconnects: the barrier is released without waiting for the deadline", async () => {
  const sock = await rawPost("/v1/test-big");
  await Bun.sleep(300);
  const w = writerWait();
  await Bun.sleep(500);
  sock.destroy();
  const waited = await w;
  expect(waited).toBeLessThan(2_500);
}, 20_000);

// ---------- forget by canonical source ----------

test("forget a child: a grandchild keeps the grandparent's evidence and its own; only the child's records go", async () => {
  const P = sourceIdOf("pi", "gp");
  const pl = [header("gp"), msg("user", "u0", null, "grandparent-fact"), msg("assistant", "a0", "u0", "r")];
  await put("gp", pl);
  await drain(db);
  const kl = [header("ch", { parentSession: "/x/gp.jsonl" }), pl[1], pl[2], msg("user", "c1", "a0", "kid-fact"), msg("assistant", "c2", "c1", "r")];
  const K = await put("ch", kl, "personal", P);
  await drain(db);
  const gl = [header("gc", { parentSession: "/x/ch.jsonl" }), ...kl.slice(1), msg("user", "g1", "c2", "grandchild-fact"), msg("assistant", "g2", "g1", "r")];
  await put("gc", gl, "personal", K);
  await drain(db);
  for (const t of ["grandparent-fact", "kid-fact", "grandchild-fact"]) expect(await hasText(t)).toBe(true);
  await forgetSource(db, K);
  await drain(db);
  expect(await hasText("kid-fact")).toBe(false);
  expect(await hasText("grandparent-fact")).toBe(true);
  expect(await hasText("grandchild-fact")).toBe(true);
  // The grandparent's records keep their origin in the grandchild (not relabelled as its own).
  const g = (await grep(db, T0({ pattern: "grandparent-fact" }))).body as any;
  expect(g.hits.map((h: any) => h.record)).toEqual(["record:" + P + ":u0"]);
  // The grandparent's records are not tombed, under its id or by content.
  expect((await db`select count(*)::int n from tomb where key like ${"canon:" + P + ":%"}`)[0].n).toBe(0);
  // A new fork of the grandparent, imported now, keeps the copied prefix (not refused as forgotten).
  const nl = [header("nf", { parentSession: "/x/gp.jsonl" }), pl[1], pl[2], msg("user", "n1", "a0", "new-fork-own")];
  const l = await requestLease(db, { source: sourceIdOf("pi", "nf"), harness: "pi", nativeId: "nf", scope: "personal", parent: P }, "m1");
  await tryGrant(db, l);
  const c = await commit(db, { leaseId: l, commitId: "nf0", length: nl.length, changes: nl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(c).toMatchObject({ ok: true, refused: 0 });
});

test("forget a parent whose record was revised: the child's copy of the old version goes too", async () => {
  const P = sourceIdOf("pi", "rv");
  const old = [header("rv"), msg("user", "u0", null, "old-version-secret"), msg("assistant", "a0", "u0", "r")];
  await put("rv", old);
  await drain(db);
  const kl = [header("rk2", { parentSession: "/x/rv.jsonl" }), old[1], old[2], msg("user", "k1", "a0", "kid-own-2"), msg("assistant", "k2", "k1", "r")];
  await put("rk2", kl, "personal", P);
  await drain(db);
  // The parent's file now holds a revised u0 (the child's copy keeps the old one).
  await put("rv", [old[0], msg("user", "u0", null, "new-version"), old[2]]);
  await drain(db);
  await forgetSource(db, P);
  await drain(db);
  expect(await hasText("old-version-secret")).toBe(false);
  expect(await hasText("kid-own-2")).toBe(true);
});

test("forget a parent: a child's own record that reuses a parent record id is kept", async () => {
  const P = sourceIdOf("pi", "sid");
  const pl = [header("sid"), msg("user", "u0", null, "parent-text-x"), msg("assistant", "a0", "u0", "r")];
  await put("sid", pl);
  await drain(db);
  // The child's own entry happens to have id "x9", as does a later parent entry with other content.
  const kl = [header("sk", { parentSession: "/x/sid.jsonl" }), pl[1], pl[2], msg("user", "x9", "a0", "child-own-x9"), msg("assistant", "x10", "x9", "r")];
  const K = await put("sk", kl, "personal", P);
  await drain(db);
  await put("sid", [...pl, msg("user", "x9", "a0", "parent-later-x9")]);
  await drain(db);
  await forgetSource(db, P);
  await drain(db);
  expect(await hasText("child-own-x9")).toBe(true);
  expect(await hasText("parent-later-x9")).toBe(false);
  // The child syncs again: its own x9 is accepted, not refused as a forgotten ancestor id.
  const l = await requestLease(db, { source: K, harness: "pi", nativeId: "sk", scope: "personal", parent: P }, "m1");
  await tryGrant(db, l);
  const c = await commit(db, { leaseId: l, commitId: "sk-again", length: kl.length, changes: kl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(c).toMatchObject({ ok: true, refused: 2 }); // only the two copies of the parent's records
  await drain(db);
  expect(await hasText("child-own-x9")).toBe(true);
  expect(await hasText("parent-text-x")).toBe(false);
});
