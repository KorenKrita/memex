// Search (DESIGN §6.3, §4.3, §7) over real Postgres, HTTP via node:http, and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import net from "node:net";
import { connect, migrate, bump, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { EMBED_DIM, revokeGrant } from "./derive.ts";
import { addApiKey, Guarded, httpHooks, route, serve, transport } from "./http.ts";
import { search, zoom, grep, GUARANTEED, searchHooks, mergeLabels, labelOf } from "./search.ts";
import { forgetSource } from "./forget.ts";
import { drain as drainAll, handlers } from "./worker.ts";
// L3 jobs are tested in l3.test.ts; here they would only add model calls.
const KINDS = Object.keys(handlers).filter((k) => !k.startsWith("l3"));
const drain = (db: any, kinds = KINDS, max?: number) => drainAll(db, kinds, max);
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
      if (path.endsWith("/chat/completions") && body.messages[0].content.startsWith("You write the card"))
        return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "T", overview: "card", results: [], open_threads: [] }) } }] });
      if (path.endsWith("/chat/completions") && body.messages[0].content.startsWith("You merge")) {
        const parts = [...(body.messages[1].content as string).matchAll(/<part n="\d">\n([\s\S]*?)\n<\/part>/g)].map((m) => m[1]);
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: parts.join(" + ") }) } }] });
      }
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
  delete process.env.MEMEX_FOREST_HOT_DAYS;
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
  stampOf.clear(); // a fixture's times are its own: an entry built in another test is not a copy here
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, search_trace,
                   forest_node, forest_access, session_card restart identity cascade`);
  for (const [s, p, d] of ALL) await db`insert into grant_rule values (${s}, ${p}, ${d})`;
  process.env.MEMEX_IDLE_CLOSE_MS = "0"; // every tail closes: everything is summarized
  process.env.MEMEX_FOREST_HOT_DAYS = "100000"; // the fixtures' timestamps are old: count them as recent
  requests = [];
  rerankStatus = 200;
  summaryOf = (input) => /kind="user">\n?([^<]*)/.exec(input.slice(input.indexOf("<input>")))?.[1]?.trim() ?? "s";
  httpHooks.beforeDeliver = async () => {};
  httpHooks.beforeGuard = async () => {};
  searchHooks.beforeTrace = async () => {};
  (await import("./derive.ts")).hooks.afterCheck = async () => {};
  key = await addApiKey(db, "t");
});

/** The trace's rerank inputs, in the order they were sent. */
const inputs = (d: any) => d.pool.filter((c: any) => c.selected).sort((a: any, b: any) => a.selected_pos - b.selected_pos);
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const T = "2026-01-01T00:00:00Z";
// Fixture clock (DESIGN §4.1: a fork child's entry not newer than its header is a copy): the same entry built
// again gets the same time (a copy keeps the parent's); a fork header takes the next tick, so entries built
// after it are the child's own.
let tickN = 0;
const stampOf = new Map<string, string>();
const tick = () => new Date(Date.parse(T) + ++tickN * 1000).toISOString();
const stamp = (...k: unknown[]) => {
  const key = JSON.stringify([T, ...k]);
  if (!stampOf.has(key)) stampOf.set(key, tick());
  return stampOf.get(key)!;
};
const header = (id: string, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ type: "session", version: 3, id, timestamp: extra.parentSession ? stamp("fork", id, extra.parentSession) : T, cwd: "/x", ...extra });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: stamp(role, id, parent, text), message: { role, content: [{ type: "text", text }] } });
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
  expect(inputs(t.data).length).toBe(40);
  const rare = inputs(t.data).find((c: any) => c.ranks.lex === 1);
  expect(rare.ranks.ft).toBeUndefined();
  expect(rare.final_rank).toBe(1);
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
  const rare = inputs(t.data).find((c: any) => c.ranks.lex === 1);
  expect(rare).toBeTruthy();
  const rrfRank = inputs(t.data).filter((c: any) => c.fused > rare.fused).length + 1;
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
  // A newer L0 commit that only appends: the old projection stays readable (nothing in it was withdrawn).
  await db`delete from tomb`;
  await put("f1", linear("f1", [["alpha secret-one", "r"], ["beta", "r2"], ["gamma", "r3"]]));
  r = await body(Q("secret-one"));
  expect(has(r)).toBe(true);
  // A revision withdraws: nothing from the old projection until it is projected again.
  await drain(db);
  await put("f1", linear("f1", [["alpha secret-one revised", "r"], ["beta", "r2"], ["gamma", "r3"]]));
  r = await body(Q("secret-one"));
  expect(r.results.length).toBe(0);
  await drain(db);
  r = await body(Q("secret-one"));
  expect(JSON.stringify(r.results)).toContain("secret-one revised");
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
  expect(inputs(t.data).length).toBe(40);
  // Attachments go from the lowest-ranked candidates up; those are marked incomplete and not injectable.
  const dropped = inputs(t.data).filter((c: any) => c.incomplete.includes("attachments-dropped"));
  expect(dropped.length).toBeGreaterThan(0);
  expect(sent.reduce((a, d) => a + Buffer.byteLength(d), 0)).toBeGreaterThan(60 * 1024); // only what was needed
  const out = r.results.filter((x: any) => x.incomplete.includes("attachments-dropped"));
  expect(out.length).toBe(dropped.length);
  // Lowest initial-recall ranks lose attachments first: every dropped one ranks below every kept one.
  const first = inputs(t.data).findIndex((c: any) => c.incomplete.includes("attachments-dropped"));
  expect(inputs(t.data).slice(first).every((c: any) => c.attached === 0)).toBe(true);
  expect(inputs(t.data).slice(0, first).some((c: any) => c.attached > 0)).toBe(true);
  expect(out.every((x: any) => !x.injectable && x.attachments.length === 0)).toBe(true);
  expect(r.results.filter((x: any) => !x.incomplete.length).every((x: any) => x.injectable)).toBe(true);
  expect(sent.filter((d) => d.includes("[next exchange,")).length).toBeGreaterThan(0);
  expect(sent.filter((d) => d.includes("[next exchange,")).length).toBeLessThan(40);
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
  expect((((await g.again()) as any).body as any).results.length).toBe(0);
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
  expect(r.hits.every((h: any) => h.in === s && h.snippet.includes("/etc/memex.conf"))).toBe(true);
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
  expect(rec.records.map((x: any) => x.text)).toEqual(["r3"]);
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
  // Naming the other scope's source explicitly does not make its records readable either.
  const r2 = await fetch(`http://127.0.0.1:${api.port}/v1/zoom`, { method: "POST", headers: { authorization: `Bearer ${key}` },
    body: JSON.stringify(T0({ id: "record:" + w + ":a0", in: w })) });
  expect(r2.status).toBe(404);
  expect(await r2.text()).not.toContain("work secret");
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
  // The child's own entry happens to have id "x9", as does a later parent entry with other content. The child's
  // entries are written after the fork (its header's time), as Pi writes them; copies keep their earlier time.
  const late = (raw: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: "2026-01-02T00:00:00Z" });
  const kl = [header("sk", { parentSession: "/x/sid.jsonl" }), pl[1], pl[2], late(msg("user", "x9", "a0", "child-own-x9")), late(msg("assistant", "x10", "x9", "r"))];
  const K = await put("sk", kl, "personal", P);
  await drain(db);
  await put("sid", [...pl, late(msg("user", "x9", "a0", "parent-later-x9"))]);
  await drain(db);
  expect(await hasText("child-own-x9")).toBe(true); // an id collision is not a copy
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

test("a child's copy of a parent version the server never held, then the parent forgotten: the copy stays withdrawn", async () => {
  const P = sourceIdOf("pi", "rvp");
  const u0 = msg("user", "u0", null, "q"), a0 = msg("assistant", "a0", "u0", "old-withdrawn-copy");
  await put("rvp", [header("rvp"), u0, msg("assistant", "a0", "u0", "revised-current")]); // only the revised version
  await drain(db);
  const late = (raw: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: "2026-01-02T00:00:00Z" });
  const kl = [header("rvk", { parentSession: "/x/rvp.jsonl", timestamp: "2026-01-01T12:00:00Z" }), u0, a0, late(msg("user", "k1", "a0", "kid-new-entry"))];
  const K = await put("rvk", kl, "personal", P);
  await drain(db);
  expect(await hasText("old-withdrawn-copy")).toBe(false);
  expect(await hasText("kid-new-entry")).toBe(true);
  await forgetSource(db, P);
  await drain(db);
  expect(await hasText("old-withdrawn-copy")).toBe(false);
  expect(await hasText("revised-current")).toBe(false);
  expect(await hasText("kid-new-entry")).toBe(true);
  // The child syncs the same file again after the forget: still not its own.
  const l = await requestLease(db, { source: K, harness: "pi", nativeId: "rvk", scope: "personal", parent: P }, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "rvk-again", length: kl.length, changes: kl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db);
  expect(await hasText("old-withdrawn-copy")).toBe(false);
  expect(await hasText("kid-new-entry")).toBe(true);
});

// ---------- review round 5: copy identity through the whole ancestry ----------

/** A → B → C where A holds only the revised u0/a0, B a matching copy of it, C a copy of the old version.
 *  Copies keep the parent's times (T); each child's own entries are newer than its fork (its header's time). */
async function threeGen(order: "c-before" | "c-after") {
  const at = (raw: string, ts: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: ts });
  const u0 = msg("user", "u0", null, "q"), a0new = msg("assistant", "a0", "u0", "gen-revised"), a0old = msg("assistant", "a0", "u0", "gen-old-copy");
  const A = await put("ga", [header("ga"), u0, a0new]);
  await drain(db);
  const B = await put("gb", [header("gb", { parentSession: "/x/ga.jsonl", timestamp: "2026-01-02T00:00:00Z" }), u0, a0new,
    at(msg("user", "b1", "a0", "gen-b-own"), "2026-01-02T01:00:00Z")], "personal", A);
  await drain(db);
  const cl = [header("gc", { parentSession: "/x/gb.jsonl", timestamp: "2026-01-03T00:00:00Z" }), u0, a0old, at(msg("user", "c1", "a0", "gen-c-own"), "2026-01-03T01:00:00Z")];
  if (order === "c-before") {
    await put("gc", cl, "personal", B);
    await drain(db);
    expect(await hasText("gen-old-copy")).toBe(false);
  }
  await forgetSource(db, A);
  await drain(db);
  if (order === "c-after") {
    await put("gc", cl, "personal", B);
    await drain(db);
  }
  return { A, B };
}

for (const order of ["c-before", "c-after"] as const)
  test(`a grandparent forgotten (${order === "c-before" ? "grandchild imported before" : "after"}): the grandchild's withdrawn copy stays withdrawn`, async () => {
    await threeGen(order);
    expect(await hasText("gen-old-copy")).toBe(false);
    expect(await hasText("gen-revised")).toBe(false);
    expect(await hasText("gen-b-own")).toBe(true);
    expect(await hasText("gen-c-own")).toBe(true);
  });

test("only the middle fork forgotten: the living grandparent's evidence and the grandchild's own stay", async () => {
  const at = (raw: string, ts: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: ts });
  const u0 = msg("user", "u0", null, "mid-root-q"), a0 = msg("assistant", "a0", "u0", "mid-root-reply");
  const A = await put("ma", [header("ma"), u0, a0]);
  await drain(db);
  const B = await put("mb", [header("mb", { parentSession: "/x/ma.jsonl", timestamp: "2026-01-02T00:00:00Z" }), u0, a0, at(msg("user", "b1", "a0", "mid-b-own"), "2026-01-02T01:00:00Z")], "personal", A);
  await drain(db);
  await put("mc", [header("mc", { parentSession: "/x/mb.jsonl", timestamp: "2026-01-03T00:00:00Z" }), u0, a0, at(msg("user", "c1", "a0", "mid-c-own"), "2026-01-03T01:00:00Z")], "personal", B);
  await drain(db);
  await forgetSource(db, B);
  await drain(db);
  expect(await hasText("mid-root-reply")).toBe(true); // A's own, alive
  expect(await hasText("mid-b-own")).toBe(false);
  expect(await hasText("mid-c-own")).toBe(true);
});

test("a child's own entry newer than the fork with the same id and content as a parent entry: its own through projection, forget and sync", async () => {
  const at = (raw: string, ts: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: ts });
  const P = sourceIdOf("pi", "xp");
  const u0 = msg("user", "u0", null, "xq"), a0 = msg("assistant", "a0", "u0", "xr");
  const same = at(msg("user", "z9", "a0", "exact-same-own"), "2026-01-05T00:00:00Z");
  await put("xp", [header("xp"), u0, a0]);
  await drain(db);
  const kl = [header("xk", { parentSession: "/x/xp.jsonl", timestamp: "2026-01-02T00:00:00Z" }), u0, a0, same];
  const K = await put("xk", kl, "personal", P);
  await drain(db);
  await put("xp", [header("xp"), u0, a0, same]); // the parent later holds the very same entry
  await drain(db);
  const own = async () => ((await db`select keys from projected where source = ${K}`)[0].keys as [string, string][]).filter(([m]) => m.startsWith("z9:")).map(([, k]) => k.split("/")[0]);
  expect(await own()).toEqual(["xk"]);
  await forgetSource(db, P);
  await drain(db);
  expect(await hasText("exact-same-own")).toBe(true);
  const l = await requestLease(db, { source: K, harness: "pi", nativeId: "xk", scope: "personal", parent: P }, "m1");
  await tryGrant(db, l);
  await commit(db, { leaseId: l, commitId: "xk-again", length: kl.length, changes: kl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db);
  expect(await hasText("exact-same-own")).toBe(true);
  expect(await own()).toEqual(["xk"]);
});

for (const order of ["C before the forget", "C after the forget"])
  test(`A → B → C, B forgotten (${order}): C's copies of A stay A's, never refused or removed for B's copies of them`, async () => {
    const A = sourceIdOf("pi", "ra"), B = sourceIdOf("pi", "rb");
    const u0 = msg("user", "u0", null, "r3-a-question"), a0 = msg("assistant", "a0", "u0", "r3-a-reply");
    await put("ra", [header("ra"), u0, a0]);
    await drain(db);
    // B holds A's records as copies; it saw them before A's projection mapped them (so B's own mapping may lack them).
    await put("rb", [header("rb", { parentSession: "/x/ra.jsonl" }), u0, a0, msg("user", "b1", "a0", "r3-b-own")], "personal", A);
    await drain(db);
    await db`update projected set keys = '[]'::jsonb where source = ${B}`; // as if B's copies were unmapped (withdrawn)
    const cl = [header("rc", { parentSession: "/x/rb.jsonl" }), u0, a0, msg("user", "c1", "a0", "r3-c-own")];
    let C = sourceIdOf("pi", "rc");
    if (order === "C before the forget") C = await put("rc", cl, "personal", B);
    await forgetSource(db, B);
    expect((await db`select count(*)::int n from tomb where key like ${"copy:" + B + ":a0:%"}`)[0].n).toBe(0); // a copy is no contribution of B
    expect((await db`select count(*)::int n from tomb where key like ${"copy:" + B + ":b1:%"}`)[0].n).toBe(1);
    if (order === "C after the forget") {
      const l = await requestLease(db, { source: C, harness: "pi", nativeId: "rc", scope: "personal", parent: B }, "m1");
      await tryGrant(db, l);
      const r = await commit(db, { leaseId: l, commitId: "rc-after", length: cl.length, changes: cl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
      expect((r as any).refused).toBe(0);
    }
    await drain(db);
    expect((await db`select count(*)::int n from body where source = ${C} and rec = 'a0'`)[0].n).toBe(1);
    const owners = ((await db`select keys from projected where source = ${C}`)[0].keys as [string, string][]).filter(([m]) => /^(u0|a0):/.test(m)).map(([, k]) => k.split("/")[0]);
    expect(owners).toEqual(["ra", "ra"]); // inherited through the forgotten B from the living A
    expect(await hasText("r3-c-own")).toBe(true);
    expect(await hasText("r3-b-own")).toBe(false);
  });

test("an unlinked fork is no usable parent: its own entry stays readable in it, its fork's copy of that entry is withdrawn", async () => {
  // B's header names a parent the collector never linked (parent: null); b is B's own (newer than B's fork).
  const u0 = msg("user", "u0", null, "ul-root");
  const B = await put("ub", [header("ub", { parentSession: "/x/absent.jsonl" }), u0, msg("user", "b", "u0", "ul-b-own")], "personal", null);
  await drain(db);
  expect(await hasText("ul-b-own")).toBe(true);
  expect((await db`select res from projected where source = ${B}`)[0].res.parent_ok).toBe(false);
  // C forks B after b: b is a copy in C, B no usable parent — withdrawn, not inherited, not C's own.
  const C = await put("uc", [header("uc", { parentSession: "/x/ub.jsonl" }), u0, msg("user", "b", "u0", "ul-b-own"), msg("user", "c", "b", "ul-c-own")], "personal", B);
  await drain(db);
  const recs = (await db`select recs from view where source = ${C}`).flatMap((v: any) => (v.recs as { id: string }[]).map((x) => x.id));
  expect(recs).not.toContain("b");
  expect(recs).not.toContain("u0");
  expect(recs).toContain("c");
});

for (const dir of ["earlier", "later"])
  test(`forget of a fork whose header was revised (fork time moved ${dir}): the current header decides which versions were its own`, async () => {
    const A = sourceIdOf("pi", "fa"), B = sourceIdOf("pi", "fb");
    const at = (raw: string, ts: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: ts });
    const e = at(msg("user", "e", null, "fh-entry"), "2026-01-03T00:00:00Z");
    await put("fa", [header("fa"), e]);
    await drain(db);
    const h = (ts: string) => JSON.stringify({ type: "session", version: 3, id: "fb", timestamp: ts, cwd: "/x", parentSession: "/x/fa.jsonl" });
    // First header, then the revised one: e is a copy under the first, B's own under the second, or the other way round.
    const [first, now] = dir === "earlier" ? ["2026-01-04T00:00:00Z", "2026-01-02T00:00:00Z"] : ["2026-01-02T00:00:00Z", "2026-01-04T00:00:00Z"];
    await put("fb", [h(first), e], "personal", A);
    await drain(db);
    await put("fb", [h(now), e], "personal", A);
    await drain(db);
    const ownNow = dir === "earlier"; // e (Jan 3) newer than the current fork time (Jan 2): B's own
    await forgetSource(db, B);
    expect((await db`select count(*)::int n from tomb where key like ${"copy:" + B + ":e:%"}`)[0].n).toBe(ownNow ? 1 : 0);
  });

for (const parentSynced of [false, true])
  test(`a fork whose header names a parent the collector did not link (parent ${parentSynced ? "synced in another scope" : "absent"}): copies withdrawn, its own kept`, async () => {
    const u0 = msg("user", "u0", null, "unlinked-copy-q"), a0 = msg("assistant", "a0", "u0", "unlinked-copy-r");
    if (parentSynced) await put("hp", [header("hp"), u0, a0], "work");
    await drain(db);
    // parent: null, as client/sync.ts declares it when the parent file is absent or outside this scope.
    const C = await put("hc", [header("hc", { parentSession: "/x/hp.jsonl" }), u0, a0, msg("user", "k1", "a0", "unlinked-own")], "personal", null);
    await drain(db);
    expect(await hasText("unlinked-own")).toBe(true);
    expect(JSON.stringify(await db`select text from lexical where source = ${C}`)).not.toContain("unlinked-copy");
    const keys = ((await db`select keys from projected where source = ${C}`)[0].keys ?? []) as [string, string][];
    expect(keys.map(([m]) => m.split(":")[0]).sort()).toEqual(["k1"]);
  });

test("a fork whose declared parent is not usable (in dispute): its own entries searchable, copies withdrawn; inherited once the parent is usable", async () => {
  const P = sourceIdOf("pi", "dp");
  const u0 = msg("user", "u0", null, "disp-copy-q"), a0 = msg("assistant", "a0", "u0", "disp-copy-r");
  const base = [header("dp"), u0, a0];
  await put("dp", [...base, msg("user", "x1", "a0", "one")]);
  const l = await requestLease(db, { source: P, harness: "pi", nativeId: "dp", scope: "personal", parent: null }, "m2");
  await tryGrant(db, l);
  const other = [...base, msg("user", "x1", "u0", "two")]; // where x1 hangs is in dispute: the parent is blocked
  await commit(db, { leaseId: l, commitId: "dp-m2", length: other.length, changes: other.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db);
  expect((await db`select status from projected where source = ${P}`)[0].status).toBe("blocked");
  const C = await put("dk", [header("dk", { parentSession: "/x/dp.jsonl" }), u0, a0, msg("user", "k1", "a0", "disp-kid-own")], "personal", P);
  await drain(db);
  expect(await hasText("disp-kid-own")).toBe(true);
  expect(JSON.stringify(await db`select text from lexical where source = ${C}`)).not.toContain("disp-copy");
  // A fork of this fork: it is no usable parent (its own ancestry unresolved), so the grandchild's copies stay out.
  const G = await put("dg", [header("dg", { parentSession: "/x/dk.jsonl" }), u0, a0, msg("user", "k1", "a0", "disp-kid-own"), msg("user", "g1", "k1", "disp-grand-own")], "personal", C);
  await drain(db);
  expect(JSON.stringify(await db`select text from lexical where source = ${G}`)).not.toMatch(/disp-copy|disp-kid-own/);
  expect(await hasText("disp-grand-own")).toBe(true);
  // The dispute resolved: the parent is usable, the child's copies are inherited.
  const l2 = await requestLease(db, { source: P, harness: "pi", nativeId: "dp", scope: "personal", parent: null }, "m2");
  await tryGrant(db, l2);
  const fixed = [...base, msg("user", "x1", "a0", "one")];
  await commit(db, { leaseId: l2, commitId: "dp-m2b", length: fixed.length, changes: fixed.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  await drain(db);
  const owners = ((await db`select keys from projected where source = ${C}`)[0].keys as [string, string][]).filter(([m]) => /^(u0|a0):/.test(m)).map(([, k]) => k.split("/")[0]);
  expect(owners).toEqual(["dp", "dp"]);
});

test("an unrelated session with the very same entry is forgotten: a fork's copy inherited from its own parent stays, also when synced again", async () => {
  // P and Q are independent roots that hold the same entry bytes (same id, content and time). C forks Q.
  const P = sourceIdOf("pi", "np"), Q = sourceIdOf("pi", "nq");
  const u0 = msg("user", "u0", null, "shared-bytes-q"), a0 = msg("assistant", "a0", "u0", "shared-bytes-r");
  await put("np", [header("np"), u0, a0]);
  await put("nq", [header("nq"), u0, a0]);
  await drain(db);
  const kl = [header("nc", { parentSession: "/x/nq.jsonl" }), u0, a0, msg("user", "k1", "a0", "nc-own")];
  const C = await put("nc", kl, "personal", Q);
  await drain(db);
  const owner = async () => ((await db`select keys from projected where source = ${C}`)[0]?.keys as [string, string][] ?? []).filter(([m]) => m.startsWith("a0:")).map(([, k]) => k.split("/")[0]);
  expect(await owner()).toEqual(["nq"]);
  await forgetSource(db, P);
  await drain(db);
  expect(await owner()).toEqual(["nq"]); // Q, C's own parent, still holds it
  expect((await db`select count(*)::int n from record where source = ${C} and rec = 'a0'`)[0].n).toBe(1);
  // C syncs again (a new collector copy, a restart): its copy of Q's entry is accepted.
  const l = await requestLease(db, { source: C, harness: "pi", nativeId: "nc", scope: "personal", parent: Q }, "m2");
  await tryGrant(db, l);
  const r = await commit(db, { leaseId: l, commitId: "nc-again", length: kl.length, changes: kl.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(r.ok).toBe(true);
  expect((r as any).refused ?? 0).toBe(0);
  await drain(db);
  expect(await owner()).toEqual(["nq"]);
  expect(await hasText("shared-bytes-r")).toBe(true);
});

test("a Pi fork that drops a label re-chains the next entry's parent: that copy is still inherited, not withdrawn", async () => {
  const P = sourceIdOf("pi", "lbp");
  const u0 = msg("user", "u0", null, "lbl-question"), lb = JSON.stringify({ type: "label", id: "l1", parentId: "u0", timestamp: T, targetId: "u0", label: "x" });
  const a0 = msg("assistant", "a0", "l1", "lbl-shared-reply");
  await put("lbp", [header("lbp"), u0, lb, a0]);
  await drain(db);
  const a0c = JSON.stringify({ ...JSON.parse(a0), parentId: "u0" }); // Pi's createBranchedSession re-chains past the label
  await put("lbk", [header("lbk", { parentSession: "/x/lbp.jsonl" }), u0, a0c], "personal", P);
  await drain(db);
  // The child maps its copy to the parent's record (the same evidence), not withdrawn and not its own.
  const [pk] = await db`select keys from projected where source = ${sourceIdOf("pi", "lbk")}`;
  expect((pk.keys as [string, string][]).filter(([m]) => m.startsWith("a0:")).map(([, k]) => k.split("/")[0])).toEqual(["lbp"]);
});

test("forget concurrent with a real commit that holds its lease row: no deadlock, the commit is refused", async () => {
  const s = await put("dl", linear("dl", [["deadlock-check", "r"]]));
  await drain(db);
  const lines = linear("dl", [["deadlock-check", "r"], ["more", "r2"]]);
  const l = await requestLease(db, { source: s, harness: "pi", nativeId: "dl", scope: "personal" }, "m1");
  await tryGrant(db, l);
  // Hold the lease row as commit does before it waits for the barrier, while forget runs.
  let release!: () => void;
  const gate = new Promise<void>((ok) => (release = ok));
  const holder = db.begin(async (tx: any) => {
    await tx`select 1 from lease where id = ${l} for update`;
    await gate;
  });
  await Bun.sleep(50);
  const f = forgetSource(db, s);
  const outcome = await Promise.race([f.then(() => "forgot"), Bun.sleep(2_000).then(() => "blocked")]);
  release();
  await holder;
  expect(outcome).toBe("forgot"); // forget does not wait on lease rows
  await f;
  const c = await commit(db, { leaseId: l, commitId: "dl1", length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(c).toMatchObject({ ok: false, reason: "forgotten" });
  expect((await db`select count(*)::int n from body where source = ${s}`)[0].n).toBe(0);
});

// ---------- the querying session's own lifecycle ----------

test("forget the querying session before each send: no query text leaves, no trace, no answer", async () => {
  const { hooks } = await import("./derive.ts");
  await put("hist", linear("hist", [["alpha history", "ANSWER"]]));
  const A = await put("asker", linear("asker", [["asking now", "r"]]));
  await drain(db);
  const forgetAsker = async () => void (await forgetSource(db, A));
  // 1. Before the embedding request (and every later model request: each sees the moved version).
  requests = [];
  let n = 0;
  hooks.afterCheck = async () => void (n++ === 0 && (await forgetAsker()));
  await expect(search(db, Q("alpha secret-query-1"))).rejects.toThrow("forgotten");
  expect(requests.filter((x) => JSON.stringify(x.body).includes("secret-query-1")).length).toBe(0);
  expect((await db`select count(*)::int n from search_trace`)[0].n).toBe(0);
});

test("forget the querying session before rerank: rerank is not sent", async () => {
  const { hooks } = await import("./derive.ts");
  await put("hist2", linear("hist2", [["alpha history", "ANSWER"]]));
  const A = await put("asker", linear("asker", [["asking now", "r"]]));
  await drain(db);
  requests = [];
  let n = 0;
  // The embedding request passes; the second guarded send (rerank) finds the query's session forgotten.
  hooks.afterCheck = async () => void (n++ === 1 && (await forgetSource(db, A)));
  await expect(search(db, Q("alpha secret-query-2"))).rejects.toThrow("forgotten");
  expect(requests.filter((x) => x.path.endsWith("/rerank")).length).toBe(0);
  expect((await db`select count(*)::int n from search_trace`)[0].n).toBe(0);
});

test("forget the querying session before its trace is written: no trace survives", async () => {
  await put("hist3", linear("hist3", [["alpha history", "ANSWER"]]));
  const A = await put("asker", linear("asker", [["asking now", "r"]]));
  await drain(db);
  searchHooks.beforeTrace = async () => {
    searchHooks.beforeTrace = async () => {};
    await forgetSource(db, A);
  };
  await expect(search(db, Q("alpha", { dests: [] }))).rejects.toThrow("forgotten");
  expect((await db`select count(*)::int n from search_trace`)[0].n).toBe(0);
});

test("forget a querying session that never reached L0, before delivery: not answered", async () => {
  await put("hist4", linear("hist4", [["alpha history-four", "ANSWER"]]));
  await drain(db);
  const A = sourceIdOf("pi", "never-synced");
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await forgetSource(db, A); // works for an unknown source: tomb + version move
  };
  const res = await fetch(`http://127.0.0.1:${api.port}/v1/search`, { method: "POST", headers: { authorization: `Bearer ${key}` },
    body: JSON.stringify(Q("history-four", { source: A, dests: [] })) });
  expect(res.status).toBe(410);
  expect(await res.text()).not.toContain("history-four");
});

test("zoom and grep: forgetting the querying session before delivery is not answered (hit, no hit, never synced)", async () => {
  const H = await put("hist5", linear("hist5", [["alpha history-five", "ANSWER-five"]]));
  await drain(db);
  const [sp] = await db`select id from span where source = ${H} limit 1`;
  // Each case its own querying session, so the race is the one exercised: the result is computed while the
  // session is live, the forget lands between that and delivery (the hook ran), and nothing is delivered.
  const cases: [string, boolean, object][] = [
    ["synced, zoom", true, { id: "span:" + sp.id }],
    ["synced, grep hit", true, { pattern: "history-five" }],
    ["synced, grep no hit", true, { pattern: "nothing-matches-this" }],
    ["never synced, zoom", false, { id: "span:" + sp.id }],
    ["never synced, grep hit", false, { pattern: "history-five" }],
    ["never synced, grep no hit", false, { pattern: "nothing-matches-this" }],
  ];
  let n = 0;
  for (const [name, synced, b] of cases) {
    const native = "asker5-" + n++;
    const A = synced ? await put(native, linear(native, [["asking", "r"]])) : sourceIdOf("pi", native);
    if (synced) await drain(db);
    let fired = 0;
    httpHooks.beforeGuard = async () => {
      if (fired++) return;
      await forgetSource(db, A);
    };
    const path = "id" in b ? "zoom" : "grep";
    const res = await fetch(`http://127.0.0.1:${api.port}/v1/${path}`, { method: "POST", headers: { authorization: `Bearer ${key}` },
      body: JSON.stringify(T0({ ...b, source: A })) });
    const txt = await res.text();
    expect([name, fired > 0, res.status]).toEqual([name, true, 410]);
    expect(txt).not.toContain("five");
  }
});

// ---------- corrections reach every rerank document that needs them ----------

const lastRerank = () => requests.filter((x) => x.path.endsWith("/rerank")).at(-1)!.body.documents as string[];

test("two candidates followed by the same correction: each rerank document carries it", async () => {
  // One exchange, two records that both match; the user corrects in the next exchange.
  const lines = [header("tw"), msg("user", "u0", null, "use port 8080 alpha"), msg("assistant", "a0", "u0", "set port 8080 in conf"),
    msg("user", "u1", "a0", "不对，改成 9090"), msg("assistant", "a1", "u1", "done")];
  await put("tw", lines);
  await drain(db);
  const r = await body(Q("port 8080"));
  const docs = lastRerank().filter((d) => d.includes("8080"));
  expect(docs.length).toBeGreaterThanOrEqual(2);
  for (const d of docs) expect(d).toContain("改成 9090");
  void r;
});

test("a record hit in a fork copy: the correction on the fork's own next exchange is attached too", async () => {
  const P = sourceIdOf("pi", "fx");
  const pl = [header("fx"), msg("user", "u0", null, "deploy target is host-a7"), msg("assistant", "a0", "u0", "ok host-a7")];
  await put("fx", [...pl, msg("user", "u1", "a0", "thanks"), msg("assistant", "a1", "u1", "np")]);
  await drain(db);
  // A fork continues from a0 with a correction that the parent never has.
  await put("fy", [header("fy", { parentSession: "/x/fx.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "不对，host-b9 才对"), msg("assistant", "k2", "k1", "改了")], "personal", P);
  await drain(db);
  await body(Q("host-a7"));
  const docs = lastRerank().filter((d) => d.includes("host-a7"));
  expect(docs.length).toBeGreaterThan(0);
  for (const d of docs) expect(d).toContain("host-b9");
});

test("a failure line later in the hit record itself is attached", async () => {
  const lines = [header("sf"), msg("user", "u0", null, "run the migration"),
    msg("assistant", "a0", "u0", "running migrate-v3 now\nstep two\n[error] migrate-v3 failed: relation missing")];
  await put("sf", lines);
  await drain(db);
  await body(Q("migrate-v3"));
  const docs = lastRerank().filter((d) => d.includes("running migrate-v3"));
  expect(docs.length).toBeGreaterThan(0);
  for (const d of docs) expect(d).toContain("[later in span, on-path] [error] migrate-v3 failed");
});

test("signals over their budget: the candidate is marked incomplete, not silently trimmed", async () => {
  const many = Array.from({ length: 40 }, (_, i) => `[error] failure number ${i} ${"e".repeat(30)}`).join("\n");
  await put("sb", [header("sb"), msg("user", "u0", null, "build sb-token"), msg("assistant", "a0", "u0", "building sb-token\n" + many)]);
  await drain(db);
  const r = await body(Q("sb-token"));
  const hit = r.results.find((x: any) => (x.excerpt?.text ?? "").includes("sb-token"));
  expect(hit.incomplete).toContain("signals-dropped");
  expect(hit.injectable).toBe(false);
});

test("a unit hit through two representations: corrections after both support sets are attached", async () => {
  const P = sourceIdOf("pi", "ur");
  const pl = [header("ur"), msg("user", "u0", null, "alpha choose db"), msg("assistant", "a0", "u0", "postgres"), msg("assistant", "a1", "a0", "and redis"),
    msg("user", "u1", "a1", "不对，去掉 redis"), msg("assistant", "a2", "u1", "ok")];
  summaryOf = (input) => (input.includes("and redis") ? "S: alpha db postgres and redis" : input.includes("postgres") ? "P: alpha db postgres" : "other");
  await put("ur", pl);
  await drain(db);
  // The fork branches inside S after a0 and corrects differently.
  await put("us", [header("us", { parentSession: "/x/ur.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "不对，换成 mysql"), msg("assistant", "k2", "k1", "ok")], "personal", P);
  await drain(db);
  await body(Q("alpha"));
  const doc = lastRerank().find((d) => d.includes("P: alpha db postgres") && d.includes("S: alpha db postgres and redis"));
  expect(doc).toBeTruthy();
  expect(doc).toContain("去掉 redis");
  expect(doc).toContain("换成 mysql");
});

test("a record hit in a record that both S and its fork prefix P hold: corrections after either end are attached", async () => {
  const P = sourceIdOf("pi", "rp2");
  const pl = [header("rp2"), msg("user", "u0", null, "choose-db-token please"), msg("assistant", "a0", "u0", "postgres"), msg("assistant", "a1", "a0", "and redis"),
    msg("user", "u1", "a1", "不对，去掉 redis"), msg("assistant", "a2", "u1", "ok")];
  await put("rp2", pl);
  await drain(db);
  await put("rq2", [header("rq2", { parentSession: "/x/rp2.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "不对，换成 mysql"), msg("assistant", "k2", "k1", "ok")], "personal", P);
  await drain(db);
  expect((await db`select count(*)::int n from span where keys[1] like 'rp2/u0:%'`)[0].n).toBe(2); // S and P
  await body(Q("choose-db-token", { dests: ["rerank"] }));
  // The record candidate (lexical, the user message) and the unit candidates whose summaries quote it.
  const docs = lastRerank().filter((d) => d.includes("[excerpt] choose-db-token"));
  expect(docs.length).toBeGreaterThan(0);
  for (const d of docs) {
    expect(d).toContain("去掉 redis");
    expect(d).toContain("换成 mysql");
  }
});

test("units follow the span's current unit, not the one stored with an older summary", async () => {
  await put("um", linear("um", [["alpha unit-move", "r"]]));
  await drain(db);
  const [sp] = await db`select id from span limit 1`;
  // The summary row still names an old unit (as after a re-projection moved the span into another unit).
  await db`update span_summary set unit = 'stale-unit' where span = ${sp.id}`;
  const r = await body(Q("unit-move", { dests: [] }));
  expect(r.results.some((x: any) => x.key === "unit:" + sp.id)).toBe(true);
  expect(r.results.some((x: any) => x.key === "unit:stale-unit")).toBe(false);
});

test("an upgraded database (columns added later, empty): migrate re-projects, search works without a new sync", async () => {
  const s0 = await put("mg", linear("mg", [["alpha upgrade-token", "r"]]));
  await drain(db);
  // As written by the previous version: no read basis, no canonical ids.
  await db`update projected set deps = '{}'`;
  await db`update lexical set canon = ''`;
  await db`update span set canons = '{}'`;
  expect(((await search(db, Q("upgrade-token", { dests: [] }))).body as any).results.length).toBe(0); // hidden, not wrong
  await migrate(db);
  expect((await db`select count(*)::int n from job where kind = 'project' and key = ${s0} and state = 'queued'`)[0].n).toBe(1);
  await drain(db);
  expect((await db`select count(*)::int n from lexical where canon = ''`)[0].n).toBe(0);
  expect((await db`select count(*)::int n from span where canons = '{}'`)[0].n).toBe(0);
  expect(JSON.stringify(((await search(db, Q("upgrade-token", { dests: [] }))).body as any).results)).toContain("upgrade-token");
});

test("an upgrade from projections that kept a revised parent's copy as the child's own: unreadable at migrate, before any worker", async () => {
  const P = sourceIdOf("pi", "upp");
  const u0 = msg("user", "u0", null, "q"), a0 = msg("assistant", "a0", "u0", "upgrade-old-copy");
  await put("upp", [header("upp"), u0, msg("assistant", "a0", "u0", "upgrade-revised")]);
  await drain(db);
  const late = (raw: string) => JSON.stringify({ ...JSON.parse(raw), timestamp: "2026-01-02T00:00:00Z" });
  const C = await put("upk", [header("upk", { parentSession: "/x/upp.jsonl" }), u0, a0, late(msg("user", "k1", "a0", "upgrade-kid-own"))], "personal", P);
  await drain(db);
  // As the previous version left it: the copy indexed as the child's own, the projection current.
  const [k] = await db`select key from lexical where source = ${C} order by key limit 1`;
  await db`insert into lexical (source, key, part, rec, kind, text, canon) values (${C}, ${"upk/a0:old#0"}, 0, 'a0', 'assistant', 'upgrade-old-copy', ${C + ":a0"})`;
  expect(k).toBeDefined();
  expect(await hasText("upgrade-old-copy")).toBe(true);
  // A database that already ran fork-copy-v2..v6 (their marks only): v7 still runs, once.
  await db`delete from migration_mark where key = 'fork-copy-v7'`;
  await db`insert into migration_mark values ('fork-copy-v2'), ('fork-copy-v3'), ('fork-copy-v4'), ('fork-copy-v5'), ('fork-copy-v6') on conflict do nothing`;
  await migrate(db);
  expect(await hasText("upgrade-old-copy")).toBe(false); // before any worker ran
  const v = (await db`select v from vis_version where key = ${"s:" + C}`)[0].v;
  await migrate(db); // once only
  expect((await db`select v from vis_version where key = ${"s:" + C}`)[0].v).toBe(v);
  await drain(db);
  expect(await hasText("upgrade-old-copy")).toBe(false);
  expect(await hasText("upgrade-kid-own")).toBe(true);
});

for (const kind of ["the key escaped", "a value jsonb cannot hold"])
test(`an upgrade from projections that kept an unlinked fork's copy as its own (v6 already ran; ${kind}): unreadable at migrate, before any worker`, async () => {
  const u0 = msg("user", "u0", null, "q"), a0 = msg("assistant", "a0", "u0", "unlinked-old-copy");
  // A fork whose parent file the collector did not link (parent: null), its header naming the parent.
  // Its header spells the key with a JSON escape (the same "parentSession" to any JSON reader), or carries a
  // valid JSON value PostgreSQL's jsonb rejects (an escaped NUL): the migration neither misses it nor fails.
  const named = header("ulk", { parentSession: "/x/absent.jsonl" });
  const h = kind === "the key escaped" ? named.replace('"parentSession"', '"parent\\u0053ession"') : named.replace(/}$/, ',"x":"\\u0000"}');
  expect(JSON.parse(h).parentSession).toBe("/x/absent.jsonl");
  const C = await put("ulk", [h, u0, a0, msg("user", "k1", "a0", "unlinked-kid-own")], "personal", null);
  await drain(db);
  // As the previous version left it: the copy indexed as the child's own.
  await db`insert into lexical (source, key, part, rec, kind, text, canon) values (${C}, ${"ulk/a0:old#0"}, 0, 'a0', 'assistant', 'unlinked-old-copy', ${C + ":a0"})`;
  expect(await hasText("unlinked-old-copy")).toBe(true);
  await db`delete from migration_mark where key = 'fork-copy-v7'`;
  await db`insert into migration_mark values ('fork-copy-v2'), ('fork-copy-v3'), ('fork-copy-v4'), ('fork-copy-v5'), ('fork-copy-v6') on conflict do nothing`;
  // A root session (no parent named) is not moved.
  const R = await put("ulr", [header("ulr"), msg("user", "r0", null, "root-text")]);
  await drain(db);
  const rv = (await db`select v from vis_version where key = ${"s:" + R}`)[0]?.v ?? null;
  await migrate(db);
  expect(await hasText("unlinked-old-copy")).toBe(false); // before any worker ran
  expect((await db`select v from vis_version where key = ${"s:" + R}`)[0]?.v ?? null).toBe(rv);
  const v = (await db`select v from vis_version where key = ${"s:" + C}`)[0].v;
  await migrate(db); // once only
  expect((await db`select v from vis_version where key = ${"s:" + C}`)[0].v).toBe(v);
  await drain(db);
  expect(await hasText("unlinked-old-copy")).toBe(false);
  expect(await hasText("unlinked-kid-own")).toBe(true);
});

test("a fresh database: the entry point creates every table before serving", async () => {
  const { start } = await import("./main.ts");
  const name = "memex_fresh_" + Date.now();
  await db.unsafe(`create database ${name}`);
  const url = (process.env.MEMEX_PG_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex").replace(/\/[^/]*$/, "/" + name);
  const prev = process.env.MEMEX_PG_URL;
  process.env.MEMEX_PG_URL = url;
  try {
    await db.unsafe(`select 1`);
    const fresh = connect(url);
    await fresh.unsafe("create extension if not exists vector");
    await fresh.unsafe("create extension if not exists pg_trgm");
    await fresh.close();
    const s = await start({ port: 0 });
    const tables = (await s.db`select tablename from pg_tables where schemaname = 'public'`).map((r: any) => r.tablename);
    for (const t of ["span", "span_summary", "search_trace", "lexical", "projected", "tomb"]) expect(tables).toContain(t);
    const k = await addApiKey(s.db, "fresh");
    const r = await fetch(`http://127.0.0.1:${s.port}/v1/grep`, { method: "POST", headers: { authorization: `Bearer ${k}` }, body: JSON.stringify(T0({ pattern: "x" })) });
    expect(r.status).toBe(403); // no grants yet: refused, not a missing table
    await s.close();
  } finally {
    if (prev === undefined) delete process.env.MEMEX_PG_URL;
    else process.env.MEMEX_PG_URL = prev;
    await db.unsafe(`drop database if exists ${name} with (force)`);
  }
});

// ---------- view keys (§4.2, §6.4) ----------

/** One file, two branches: view a2 = S[u0 a0 a1] + [u1 a2]; view b2 = P[u0 a0] + [b1] + [v1 b2] (written last). */
async function branched(leaf?: string) {
  const l1 = [header("br"), msg("user", "u0", null, "alpha choose db"), msg("assistant", "a0", "u0", "postgres"), msg("assistant", "a1", "a0", "and redis brtoken-off"),
    msg("user", "u1", "a1", "next q"), msg("assistant", "a2", "u1", "ok")];
  const src = await put("br", l1);
  await drain(db);
  await put("br", [...l1, msg("assistant", "b1", "a0", "only postgres brtoken-on"), msg("user", "v1", "b1", "go on"), msg("assistant", "b2", "v1", "fine")], "personal", null, leaf);
  await drain(db);
  const sp = async (first: string, n: number) => (await db`select id from span where source = ${src} and keys[1] like ${"br/" + first + ":%"} and cardinality(keys) = ${n}`)[0].id as string;
  return { src, S: await sp("u0", 3), P: await sp("u0", 2), B1: await sp("b1", 1), U1: await sp("u1", 2) };
}
const rec = (r: any, text: string) => r.results.find((x: any) => x.kind !== "unit" && (x.excerpt?.text ?? "").includes(text));

test("no reported leaf: results carry the inferred-tail view key; the other branch is off-path", async () => {
  const { src } = await branched();
  const r = await body(Q("brtoken", { dests: [] }));
  const on = rec(r, "brtoken-on"), off = rec(r, "brtoken-off");
  expect(on.path).toMatchObject({ label: "on-path", view_basis: "inferred-tail", view_key: { source: src, tip: "b2", basis: "inferred-tail" } });
  expect(on.path.view_key.version).toMatch(/^[0-9a-f]{16}$/);
  expect(off.path.label).toBe("off-path");
});

test("the session's plugin reported a leaf: observed basis with its time, labels against that branch", async () => {
  await branched("a2");
  const r = await body(Q("brtoken", { dests: [] }));
  const off = rec(r, "brtoken-off");
  expect(off.path).toMatchObject({ label: "on-path", view_basis: "observed", view_key: { tip: "a2", basis: "observed" } });
  expect(Date.parse(off.path.view_key.at)).toBeGreaterThan(Date.now() - 60_000);
  expect(rec(r, "brtoken-on").path.label).toBe("off-path");
});

test("a reported leaf that names no view: unknown basis, branched, not replaced by the tail or down-weighted", async () => {
  await branched("zz-not-a-record");
  const r = await body(Q("brtoken", { dests: [] }));
  for (const t of ["brtoken-on", "brtoken-off"]) {
    const x = rec(r, t);
    expect(x.path).toMatchObject({ label: "branched", view_basis: "unknown", reason: "leaf-not-projected", view_key: { tip: null } });
  }
});

test("the querying session's own results are labelled against the leaf it sends", async () => {
  const { src } = await branched();
  const r = await body(Q("brtoken", { dests: [], source: src, leaf: "a2" }));
  expect(rec(r, "brtoken-off").path).toMatchObject({ label: "on-path", view_basis: "observed", view_key: { tip: "a2" } });
});

test("a unit's label merges its representations; an on-path excerpt does not upgrade it", async () => {
  const { S } = await branched();
  const r = await body(Q("alpha choose", { dests: [] }));
  const u = r.results.find((x: any) => x.key === "unit:" + S);
  expect(u).toBeTruthy();
  // On the tail view b2: P's records are all on-path, S's a1 is not → S mixed → the unit is mixed.
  expect(u.reps.map((p: any) => p.path).sort()).toEqual(["mixed", "on-path"]);
  expect(u.path.label).toBe("mixed");
  // With the session observed on a2 both representations are on-path.
  await db`insert into collector_view values (${sourceIdOf("pi", "br")}, 'c9', 'a2', now())`;
  const r2 = await body(Q("alpha choose", { dests: [] }));
  expect(r2.results.find((x: any) => x.key === "unit:" + S).path.label).toBe("on-path");
});

test("zoom takes the search result's view_key as returned; a key it cannot find is reported, not replaced", async () => {
  const { src, S, P, B1, U1 } = await branched();
  const on = rec(await body(Q("brtoken", { dests: [] })), "brtoken-on");
  const vk = on.path.view_key;
  // Along b2: P → b1 → (v1 b2), with the key passed through unchanged.
  let z = (await zoom(db, T0({ id: "span:" + P, view_key: vk }))).body as any;
  expect(z).toMatchObject({ view_key: { source: src, tip: "b2", basis: "inferred-tail" }, view_basis: "inferred-tail", before: null, after: "span:" + B1 });
  expect(z.stale).toBeUndefined();
  // S is not on b2: no neighbours, the reason given (it is not quietly zoomed along a2).
  z = (await zoom(db, T0({ id: "span:" + S, view_key: vk }))).body as any;
  expect(z).toMatchObject({ view_key: { tip: null }, view_basis: "unknown", reason: "view-not-found", before: null, after: null });
  expect(z.records.length).toBe(3);
  // A leaf reported later does not move a zoom that names its key: still along b2.
  await db`insert into collector_view values (${src}, 'c9', 'a2', now())`;
  z = (await zoom(db, T0({ id: "span:" + P, view_key: vk }))).body as any;
  expect(z).toMatchObject({ view_key: { tip: "b2" }, after: "span:" + B1 });
  // Without a key: the target session's own rule, now the observed leaf a2: S → (u1 a2).
  z = (await zoom(db, T0({ id: "span:" + S }))).body as any;
  expect(z).toMatchObject({ view_key: { tip: "a2" }, view_basis: "observed", after: "span:" + U1 });
  // An unknown key (basis unknown, tip null) stays unknown.
  z = (await zoom(db, T0({ id: "span:" + P, view_key: { ...vk, tip: null, basis: "unknown" } }))).body as any;
  expect(z).toMatchObject({ view_basis: "unknown", reason: "view-unknown", after: null });
  // The querying session's own leaf (mem_zoom's leaf_id): its view.
  z = (await zoom(db, T0({ id: "span:" + P, source: src, leaf: "b2" }))).body as any;
  expect(z).toMatchObject({ view_key: { source: src, tip: "b2" }, after: "span:" + B1 });
});

test("zoom view_key: growth elsewhere keeps the key, a revision makes it stale; an unreadable or bad key is refused", async () => {
  const { P, B1 } = await branched();
  const vk = rec(await body(Q("brtoken", { dests: [] })), "brtoken-on").path.view_key;
  const l1 = [header("br"), msg("user", "u0", null, "alpha choose db"), msg("assistant", "a0", "u0", "postgres"), msg("assistant", "a1", "a0", "and redis brtoken-off"),
    msg("user", "u1", "a1", "next q"), msg("assistant", "a2", "u1", "ok"), msg("assistant", "b1", "a0", "only postgres brtoken-on"), msg("user", "v1", "b1", "go on"),
    msg("assistant", "b2", "v1", "fine")];
  // The other branch grows: nothing the key's view shows was withdrawn; the key still pins it.
  await put("br", [...l1, msg("user", "u2", "a2", "more on a"), msg("assistant", "a3", "u2", "sure")]);
  await drain(db);
  expect(((await zoom(db, T0({ id: "span:" + P, view_key: vk }))).body as any)).toMatchObject({ view_key: { tip: "b2" }, after: "span:" + B1 });
  // A fresh search now infers the grown branch's tail; P is not on it.
  const vk2 = rec(await body(Q("brtoken", { dests: [] })), "brtoken-on").path.view_key;
  expect(vk2.tip).toBe("a3");
  expect(((await zoom(db, T0({ id: "span:" + P, view_key: vk2 }))).body as any)).toMatchObject({ view_basis: "unknown", reason: "view-not-found" });
  // An in-place revision of a record on the key's view: stale.
  const rev = [...l1, msg("user", "u2", "a2", "more on a"), msg("assistant", "a3", "u2", "sure")];
  rev[2] = msg("assistant", "a0", "u0", "postgres, revised");
  await put("br", rev);
  await drain(db);
  await expect(zoom(db, T0({ id: "span:" + P, view_key: vk2 }))).rejects.toThrow("view-stale");
  const vk3 = rec(await body(Q("brtoken", { dests: [] })), "brtoken-on").path.view_key;
  // The revised prefix is a new span (content-addressed); the old id is gone, not served from old text.
  await expect(zoom(db, T0({ id: "span:" + P, view_key: { ...vk3, tip: "b2" } }))).rejects.toThrow("not found");
  const P2 = { id: (await db`select span_ids from view where source = ${sourceIdOf("pi", "br")} and tip = 'b2'`)[0].span_ids[0] as string };
  expect(((await zoom(db, T0({ id: "span:" + P2.id, view_key: { ...vk3, tip: "b2" } }))).body as any).view_key.tip).toBe("b2");
  await expect(zoom(db, T0({ id: "span:" + P2.id, view_key: { ...vk3, source: sourceIdOf("pi", "nobody") } }))).rejects.toThrow("not found");
  await expect(zoom(db, T0({ id: "span:" + P, view_key: { tip: "b2" } }))).rejects.toThrow("view_key");
});

test("zoom along a fork child's view: a span stored under the parent navigates in the child's order", async () => {
  const Pp = sourceIdOf("pi", "fz");
  const pl = [header("fz"), msg("user", "u0", null, "alpha fz-shared"), msg("assistant", "a0", "u0", "x"), msg("user", "u1", "a0", "parent next"), msg("assistant", "a1", "u1", "y")];
  await put("fz", pl);
  await drain(db);
  const C = await put("fc", [header("fc", { parentSession: "/x/fz.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "child next"), msg("assistant", "k2", "k1", "z")], "personal", Pp, "k2");
  await drain(db);
  const [shared] = await db`select id from span where source = ${Pp} and keys[1] like 'fz/u0:%'`;
  const [childNext] = await db`select id from span where source = ${C} and keys[1] like 'fc/k1:%'`;
  const [parentNext] = await db`select id from span where source = ${Pp} and keys[1] like 'fz/u1:%'`;
  const vk = (await body(Q("child next", { dests: [] }))).results.find((x: any) => x.source === C).path.view_key;
  expect(vk).toMatchObject({ source: C, tip: "k2" });
  let z = (await zoom(db, T0({ id: "span:" + shared.id, view_key: vk }))).body as any;
  expect(z).toMatchObject({ source: Pp, view_key: { source: C, tip: "k2" }, after: "span:" + childNext.id });
  z = (await zoom(db, T0({ id: "span:" + shared.id }))).body as any;
  expect(z).toMatchObject({ view_key: { source: Pp }, after: "span:" + parentNext.id });
});

test("label merge: unanimous stays, disagreement is mixed, an unlocated or branched representation wins", () => {
  expect(mergeLabels(["on-path", "on-path"])).toBe("on-path");
  expect(mergeLabels(["off-path", "off-path"])).toBe("off-path");
  expect(mergeLabels(["on-path", "off-path"])).toBe("mixed");
  expect(mergeLabels(["on-path", "unlocated"])).toBe("unlocated");
  expect(mergeLabels(["on-path", "branched"])).toBe("branched");
  expect(mergeLabels([])).toBe("unlocated");
});

// ---------- grep and zoom over whole record text (§6.4) ----------

test("grep finds a literal across a chunk boundary and gives its byte offset in the record", async () => {
  const pre = "中".repeat(2040); // 2,040 characters, 6,120 bytes: the token straddles the 2,048-character chunk cut
  await put("gx", linear("gx", [["q", pre + "straddle-token-xyz tail"]]));
  await drain(db);
  expect((await db`select count(*)::int n from lexical where key like 'gx/a0:%'`)[0].n).toBe(2);
  const g = (await grep(db, T0({ pattern: "straddle-token-xyz" }))).body as any;
  expect(g.hits.length).toBe(1);
  expect(g.hits[0]).toMatchObject({ offset: 6120, length: 18 });
  expect(g.hits[0].snippet).toContain("straddle-token-xyz");
  // The offset is what zoom pages in: a page starting there begins with the token.
  const z = (await zoom(db, T0({ id: g.hits[0].record, offset: g.hits[0].offset, limit: 18 }))).body as any;
  expect(z.records[0].text).toBe("straddle-token-xyz");
});

test("grep regex: one engine (Postgres), across chunks, with the match's own position and length", async () => {
  // The 2,048-character cut falls inside "port=8080" (chunk 0 ends with "por").
  await put("gy", linear("gy", [["q", "x".repeat(2036) + " xport=1 port=8080;"]]));
  await drain(db);
  expect((await db`select text from lexical where key like 'gy/a0:%' and part = 0`)[0].text.endsWith("por")).toBe(true);
  // \m is an ARE word-start: JS would not understand it (a second engine would disagree or throw).
  const g = (await grep(db, T0({ pattern: "\\mport=[0-9]+", regex: true }))).body as any;
  expect(g.hits.length).toBe(1);
  expect(g.hits[0]).toMatchObject({ offset: 2045, length: 9 }); // not "xport" (no word start there)
  expect(g.hits[0].snippet).toContain("port=8080");
});

test("grep → zoom: the hit's offset is where zoom of the returned record shows the match (multi-record entry)", async () => {
  const s0 = await put("gz", [header("gz"), msg("user", "u0", null, "hi"),
    JSON.stringify({ type: "message", id: "a0", parentId: "u0", timestamp: T, message: { role: "assistant", content: [
      { type: "text", text: "前文中文" }, { type: "toolCall", id: "c1", name: "bash", arguments: { cmd: "ls /tmp/gz-dir" } }] } })]);
  await drain(db);
  for (const [pattern, regex] of [["gz-dir", false], ["bash \\{", true], ["中文", false]] as const) {
    const g = (await grep(db, T0({ pattern, regex }))).body as any;
    expect(g.hits.length).toBe(1);
    const h = g.hits[0];
    const z = (await zoom(db, T0({ id: h.record, in: h.in, offset: h.offset, limit: h.length }))).body as any;
    expect([pattern, z.records.map((x: any) => x.text).join("")]).toEqual([pattern, pattern.replace("\\", "")]);
  }
  void s0;
});

test("zoom record: every record of the host entry, in order; pages cut on character boundaries", async () => {
  const s0 = await put("zr", [header("zr"), msg("user", "u0", null, "hi"),
    JSON.stringify({ type: "message", id: "a0", parentId: "u0", timestamp: T, message: { role: "assistant", content: [
      { type: "text", text: "前文中文" }, { type: "toolCall", id: "c1", name: "bash", arguments: { cmd: "ls /tmp" } }] } })]);
  await drain(db);
  const all = (await zoom(db, T0({ id: "record:" + s0 + ":a0" }))).body as any;
  expect(all.records.map((x: any) => x.text)).toEqual(["前文中文", 'bash {"cmd":"ls /tmp"}']);
  expect(all.records.map((x: any) => x.kind)).toEqual(["assistant", "tool_call"]);
  // Pages of 4 bytes over "前文中文" (3 bytes each): every page holds whole characters, none lost or repeated.
  let off = 0, got = "";
  for (let i = 0; i < 20; i++) {
    const p = (await zoom(db, T0({ id: "record:" + s0 + ":a0", offset: off, limit: 4 }))).body as any;
    for (const x of p.records) expect(x.text).not.toContain("\uFFFD");
    got += p.records.map((x: any) => x.text).join("|");
    if (!p.more) break;
    expect(p.next_offset).toBeGreaterThan(off);
    off = p.next_offset;
  }
  expect(got.replaceAll("|", "")).toBe("前文中文" + 'bash {"cmd":"ls /tmp"}');
  // An offset inside a character starts at the next whole one.
  const mid = (await zoom(db, T0({ id: "record:" + s0 + ":a0", offset: 1, limit: 6 }))).body as any;
  expect(mid.records[0].text).toBe("文中");
});

test("an emoji on a chunk boundary is stored whole", async () => {
  await put("em", linear("em", [["q", "a".repeat(2047) + "😀 after"]]));
  await drain(db);
  const t = (await db`select string_agg(text, '' order by part) t from lexical where key like 'em/a0:%'`)[0].t;
  expect(t).toBe("a".repeat(2047) + "😀 after");
});

test("the trace records every stage: channel tops, fused order, the cut, the returned order and timings, no text", async () => {
  for (let i = 0; i < 45; i++) await put("tr" + i, linear("tr" + i, [["alpha trace topic " + i, "reply tracetok " + i]]));
  await drain(db);
  const r = await body(Q("alpha tracetok"));
  const [t] = await db`select data from search_trace order by id desc limit 1`;
  const d = t.data;
  expect(d.top.lex.length).toBe(10);
  expect(d.top.ft.length).toBe(10);
  expect(d.top.vec.length).toBe(10);
  // Every candidate of the pool, in fused order, including those the cut left out (past the 10th of them too).
  expect(d.pool.map((c: any) => c.fused_rank)).toEqual(d.pool.map((_: any, i: number) => i + 1));
  for (let i = 1; i < d.pool.length; i++) expect(d.pool[i - 1].fused).toBeGreaterThanOrEqual(d.pool[i].fused);
  const cut = d.pool.filter((c: any) => !c.selected);
  expect(cut.length).toBeGreaterThan(10);
  expect(inputs(d).length).toBe(40);
  for (const c of cut) expect(c.rerank_rank).toBeUndefined();
  // Survivors carry every later stage; the returned ones match the response.
  for (const c of inputs(d)) expect(typeof c.rerank_rank === "number" && c.final_rank >= 1).toBe(true);
  expect(d.pool.filter((c: any) => c.returned).sort((a: any, b: any) => a.final_rank - b.final_rank).map((c: any) => c.key)).toEqual(r.results.map((x: any) => x.key));
  // Unit candidates name the representations they were hit through.
  expect(d.pool.some((c: any) => c.key.startsWith("unit:") && c.reps.length > 0 && /^(ft|vec):sp_/.test(c.reps[0]))).toBe(true);
  expect(d.returned).toEqual(r.results.map((x: any) => x.key));
  for (const k of ["embed", "recall", "rerank", "total"]) expect(typeof d.ms[k]).toBe("number");
  expect(d.attempt).toBe(1);
  expect(d.index_lag).toEqual({});
  // Pending derived work shows up as lag.
  await put("tr-late", linear("tr-late", [["alpha late", "tracetok late"]]));
  await body(Q("alpha tracetok", { dests: [] }));
  expect((await db`select data from search_trace order by id desc limit 1`)[0].data.index_lag.project).toBe(1);
  // Keys and numbers only: neither the query nor any evidence text.
  const j = JSON.stringify(d);
  expect(j).not.toContain("tracetok");
  expect(j).not.toContain("alpha");
});

test("grep → zoom across fork copies: a revised parent record withdraws the child's inherited copy; `in` picks a readable copy, never another", async () => {
  const P = sourceIdOf("pi", "rv");
  const v1 = msg("assistant", "a0", "u0", "old revision rvtoken");
  await put("rv", [header("rv"), msg("user", "u0", null, "q"), v1, msg("assistant", "a1", "a0", "shared rvshared")]);
  await drain(db);
  const C = await put("rw", [header("rw", { parentSession: "/x/rv.jsonl" }), msg("user", "u0", null, "q"), v1,
    msg("assistant", "a1", "a0", "shared rvshared"), msg("user", "k1", "a1", "go")], "personal", P);
  await drain(db);
  // An inherited record is stored once, under its owner: one hit, expanded from the source the hit names;
  // naming another (readable) source does not fall back to the owner.
  const sh = (await grep(db, T0({ pattern: "rvshared" }))).body as any;
  expect(sh.hits.map((h: any) => h.in)).toEqual([P]);
  expect(((await zoom(db, T0({ id: sh.hits[0].record, in: sh.hits[0].in }))).body as any).source).toBe(P);
  await expect(zoom(db, T0({ id: sh.hits[0].record, in: C }))).rejects.toThrow("not found");
  // The parent's a0 is revised in place: the child's copy was inherited, so it is withdrawn with the parent's old
  // version (user decision 2026-10-10) — never the child's own record, not searchable, not readable.
  await put("rv", [header("rv"), msg("user", "u0", null, "q"), msg("assistant", "a0", "u0", "new revision changed"), msg("assistant", "a1", "a0", "shared rvshared")]);
  await drain(db);
  const g = (await grep(db, T0({ pattern: "old revision" }))).body as any;
  expect(g.hits).toEqual([]);
  await expect(zoom(db, T0({ id: "record:" + C + ":a0", in: C }))).rejects.toThrow("not found");
  // The child's own records after the copied prefix stay readable.
  expect(((await grep(db, T0({ pattern: "go" }))).body as any).hits.some((h: any) => h.in === C)).toBe(true);
  // A copy that is not readable is refused, not replaced by a readable one.
  await expect(zoom(db, T0({ id: sh.hits[0].record, in: sourceIdOf("pi", "nope") }))).rejects.toThrow("not found");
});

test("a canonical record is stored under one source only (zoom by canonical id is unambiguous)", async () => {
  const P = sourceIdOf("pi", "cu");
  const pl = [header("cu"), msg("user", "u0", null, "q"), msg("assistant", "a0", "u0", "x")];
  await put("cu", pl);
  await drain(db);
  await put("cv", [header("cv", { parentSession: "/x/cu.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "go")], "personal", P);
  await put("cw", [header("cw", { parentSession: "/x/cu.jsonl" }), pl[1], pl[2], msg("user", "k2", "a0", "go2")], "personal", P);
  await drain(db);
  expect((await db`select canon from lexical group by canon having count(distinct source) > 1`).length).toBe(0);
});

test("a single-view session with a reported leaf that names no record: branched, never on-path", async () => {
  const s0 = await put("sv", linear("sv", [["alpha svtoken", "r"]]), "personal", null, "not-a-record");
  await drain(db);
  const r = await body(Q("svtoken", { dests: [] }));
  expect(r.results.length).toBeGreaterThan(0);
  for (const x of r.results.filter((x: any) => x.source === s0))
    expect(x.path).toMatchObject({ label: "branched", view_basis: "unknown", reason: "leaf-not-projected" });
});

test("labels: one supporting record outside every view makes the evidence unlocated, not mixed", () => {
  const t = { views: [{ ord: 0, tip: "a", keys: ["k1", "k2"] }, { ord: 1, tip: "b", keys: ["k1", "k3"] }], v: { tip: "a", keys: ["k1", "k2"] }, basis: "observed" as const, at: null };
  expect(labelOf(t, ["k1", "k2"])).toBe("on-path");
  expect(labelOf(t, ["k3"])).toBe("off-path");
  expect(labelOf(t, ["k2", "k3"])).toBe("mixed");
  expect(labelOf(t, ["k1", "gone"])).toBe("unlocated");
  expect(labelOf(t, [])).toBe("unlocated");
  expect(labelOf({ ...t, v: undefined, basis: "unknown" as const }, ["k1"])).toBe("branched");
  expect(labelOf({ views: [t.views[0]], basis: "unknown" as const, at: null }, ["k1"])).toBe("branched");
});

test("a unit's representations are labelled against one reference view, the one its view_key names", async () => {
  // Parent S = [u0 a0 a1]; a child forks after a0 (P = [u0 a0] belongs to the same unit). The parent's
  // own observed leaf is on its branch; every representation's label must come from the view in view_key.
  const P = sourceIdOf("pi", "rf");
  const pl = [header("rf"), msg("user", "u0", null, "alpha rftoken fact"), msg("assistant", "a0", "u0", "first part"), msg("assistant", "a1", "a0", "later part")];
  summaryOf = (input) => (input.includes("later part") ? "S rftoken whole" : input.includes("first part") ? "P rftoken prefix" : "other");
  await put("rf", pl, "personal", null, "a1");
  await drain(db);
  await put("rk", [header("rk", { parentSession: "/x/rf.jsonl" }), pl[1], pl[2], msg("assistant", "b1", "a0", "child branch"), msg("user", "k1", "b1", "go")], "personal", P, "not-projected");
  await drain(db);
  // The child's own basis is unknown; the unit belongs to the parent, whose view is the reference.
  const r = await body(Q("rftoken", { dests: [] }));
  const u = r.results.find((x: any) => x.reps?.length >= 2);
  expect(u).toBeTruthy();
  const vk = u.path.view_key;
  const views = await db`select tip, keys from view where source = ${vk.source}`;
  const ref = views.find((v: any) => v.tip === vk.tip)!;
  for (const p of u.reps) {
    const [sp] = await db`select keys from span where id = ${p.span}`;
    const want = labelOf({ views: views.map((v: any, i: number) => ({ ord: i, tip: v.tip, keys: v.keys })), v: ref, basis: vk.basis, at: null }, sp.keys);
    expect([p.span, p.path]).toEqual([p.span, want]);
  }
  expect(u.path).toMatchObject({ label: "on-path", view_key: { source: P, tip: "a1", basis: "observed" } });
});

// ---------- a view_key pins a version: expanding it after a change is refused, not answered with new text ----------

test("zoom with an old view_key after an in-place revision (same tip): refused as stale, span and record", async () => {
  const s0 = await put("pv", linear("pv", [["alpha pvtoken original", "reply"]]));
  await drain(db);
  const r = await body(Q("pvtoken", { dests: [] }));
  const hit = r.results.find((x: any) => x.key.startsWith("record:"));
  const unit = r.results.find((x: any) => x.key.startsWith("unit:"));
  expect(hit && unit).toBeTruthy();
  // Revised in place: same ids, same tip (a0), different text.
  await put("pv", [header("pv"), msg("user", "u0", null, "alpha pvtoken REVISED"), msg("assistant", "a0", "u0", "reply")]);
  await drain(db);
  expect((await db`select tip from view where source = ${s0}`).map((x: any) => x.tip)).toEqual([hit.path.view_key.tip]);
  for (const x of [hit, unit]) {
    const res = await zoom(db, T0({ id: x.key, view_key: x.path.view_key })).then((z) => z.body as any, (e) => e);
    expect([x.key, res.status, String(res.message)]).toEqual([x.key, 409, "view-stale"]);
  }
  // Without a key the current text is served, as asked.
  expect(((await zoom(db, T0({ id: hit.key }))).body as any).records[0].text).toContain("REVISED");
});

test("zoom with an old view_key after the fork parent's record was revised: refused as stale", async () => {
  const P = sourceIdOf("pi", "pp");
  const pl = [header("pp"), msg("user", "u0", null, "alpha pptoken inherited"), msg("assistant", "a0", "u0", "x")];
  await put("pp", pl);
  await drain(db);
  const C = await put("pc", [header("pc", { parentSession: "/x/pp.jsonl" }), pl[1], pl[2], msg("user", "k1", "a0", "child pptoken own"), msg("assistant", "k2", "k1", "y")], "personal", P);
  await drain(db);
  const own = (await body(Q("pptoken own", { dests: [] }))).results.find((x: any) => x.source === C && x.key.startsWith("record:"));
  expect(own.path.view_key.source).toBe(C);
  // The parent revises a record the child inherited: the child's view changes, the child's own file does not.
  await put("pp", [header("pp"), msg("user", "u0", null, "alpha pptoken inherited, revised"), pl[2]]);
  await drain(db);
  const res = await zoom(db, T0({ id: own.key, view_key: own.path.view_key })).then((z) => z.body as any, (e) => e);
  expect([res.status, String(res.message)]).toEqual([409, "view-stale"]);
});

test("an explicit view_key must be whole: basis and version are required and typed", async () => {
  const s0 = await put("pw", linear("pw", [["alpha pwtoken", "reply"]]));
  await drain(db);
  const vk = (await body(Q("pwtoken", { dests: [] }))).results[0].path.view_key;
  for (const bad of [{ ...vk, version: undefined }, { ...vk, version: 1 }, { ...vk, basis: undefined }, { ...vk, basis: "made-up" }, { ...vk, tip: 3 }])
    await expect(zoom(db, T0({ id: "record:" + s0 + ":u0", view_key: bad }))).rejects.toThrow("view_key");
});

test("zoom a record under a view_key of another session: refused (the key does not cover it)", async () => {
  const A = await put("ka", linear("ka", [["alpha katoken", "r"]]));
  const B = await put("kb", linear("kb", [["alpha kbtoken", "r"]]));
  await drain(db);
  const vkA = (await body(Q("katoken", { dests: [] }))).results.find((x: any) => x.source === A).path.view_key;
  await expect(zoom(db, T0({ id: "record:" + B + ":u0", view_key: vkA }))).rejects.toThrow("not found");
  expect(((await zoom(db, T0({ id: "record:" + A + ":u0", view_key: vkA }))).body as any).records[0].text).toContain("katoken");
});

test("corrections are labelled on their own: a fix only on the other branch is attached and marked off-path", async () => {
  // Shared prefix u0 a0, then two branches; the user corrects on a2's branch only. The view is b2 (tail).
  const l = [header("cb"), msg("user", "u0", null, "alpha cbtoken pick postgres"), msg("assistant", "a0", "u0", "ok postgres"),
    msg("user", "u1", "a0", "不对，改成 mysql"), msg("assistant", "a1", "u1", "改了"),
    msg("user", "w1", "a0", "thanks, go on"), msg("assistant", "w2", "w1", "fine")];
  await put("cb", l);
  await drain(db);
  const r = await body(Q("cbtoken"));
  const hit = r.results.find((x: any) => x.key.startsWith("record:") && (x.excerpt?.text ?? "").includes("cbtoken"));
  expect(hit.path).toMatchObject({ label: "on-path", view_key: { tip: "w2" } });
  const fix = hit.attachments.find((a: any) => a.text.includes("改成 mysql"));
  const ok = hit.attachments.find((a: any) => a.text.includes("thanks, go on"));
  expect(fix.path).toBe("off-path");
  expect(ok.path).toBe("on-path");
  // The rerank document names each attachment's path, so the candidate's on-path does not cover the fix.
  const doc = lastRerank().find((d) => d.includes("改成 mysql"))!;
  expect(doc).toContain("[next exchange, off-path] 不对，改成 mysql");
  expect(doc).toContain("[next exchange, on-path] thanks, go on");
});

// ---------- grep pins a version; record zoom honours the key's tip; result source = reference source ----------

test("grep → zoom with the hit's version: exact while unchanged, refused as stale after an in-place revision", async () => {
  const s0 = await put("gv", linear("gv", [["q", "before gv-needle after"]]));
  await drain(db);
  const h = ((await grep(db, T0({ pattern: "gv-needle" }))).body as any).hits[0];
  expect(typeof h.version).toBe("string");
  const z = (await zoom(db, T0({ id: h.record, in: h.in, version: h.version, offset: h.offset, limit: h.length }))).body as any;
  expect(z.records.map((x: any) => x.text).join("")).toBe("gv-needle");
  await put("gv", [header("gv"), msg("user", "u0", null, "q"), msg("assistant", "a0", "u0", "much longer text in front now gv-needle")]);
  await drain(db);
  const res = await zoom(db, T0({ id: h.record, in: h.in, version: h.version, offset: h.offset, limit: h.length })).then((x) => x.body as any, (e) => e);
  expect([res.status, String(res.message)]).toEqual([409, "view-stale"]);
  // A version needs the source it belongs to.
  await expect(zoom(db, T0({ id: h.record, version: h.version }))).rejects.toThrow("version");
  void s0;
});

test("zoom a record with a real key whose tip names no view: reported unknown, not echoed as observed", async () => {
  const { src } = await branched();
  const off = rec(await body(Q("brtoken", { dests: [] })), "brtoken-off");
  const vk = { ...off.path.view_key, tip: "no-such-tip", basis: "observed" };
  const z = (await zoom(db, T0({ id: off.key, view_key: vk }))).body as any;
  expect(z).toMatchObject({ view_key: { source: src, tip: null, basis: "unknown" }, view_basis: "unknown", reason: "view-not-found" });
  expect(z.records[0].text).toContain("brtoken-off"); // the text is still readable
  // With the real tip: the off-path record expands, along the key's view.
  const z2 = (await zoom(db, T0({ id: off.key, view_key: off.path.view_key }))).body as any;
  expect(z2).toMatchObject({ view_key: { tip: off.path.view_key.tip, basis: off.path.view_key.basis }, view_basis: off.path.view_key.basis });
  // A current key whose basis says unknown: the record still expands, with no on/off-path conclusion.
  const z3 = (await zoom(db, T0({ id: off.key, view_key: { ...off.path.view_key, basis: "unknown" } }))).body as any;
  expect(z3).toMatchObject({ view_basis: "unknown", path: "branched" });
  expect(z3.records[0].text).toContain("brtoken-off");
});

test("a unit result's source and scope are its reference source; representations keep their own", async () => {
  const P = sourceIdOf("pi", "us1");
  const pl = [header("us1"), msg("user", "u0", null, "alpha ustoken fact"), msg("assistant", "a0", "u0", "first part"), msg("assistant", "a1", "a0", "later part")];
  // The child's prefix representation ranks first in both channels.
  summaryOf = (input) => (input.includes("later part") ? "S ustoken" : input.includes("first part") ? "P ustoken ustoken ustoken prefix" : "other");
  await put("us1", pl);
  await drain(db);
  const C = await put("us2", [header("us2", { parentSession: "/x/us1.jsonl" }), pl[1], pl[2], msg("assistant", "b1", "a0", "child branch"), msg("user", "k1", "b1", "go")], "personal", P);
  await drain(db);
  const r = await body(Q("ustoken prefix", { dests: [] }));
  const u = r.results.find((x: any) => x.reps?.length >= 2);
  expect(u).toBeTruthy();
  expect(u.source).toBe(u.path.view_key.source);
  expect(u.reps.map((p: any) => p.source).sort()).toEqual([C, P].sort());
});

// ---------- forest through mem_zoom (§4.4, §6.4) ----------

const zb = async (x: object) => (await zoom(db, T0(x))).body as any;
const lin = (n: string, words: string[]) => linear(n, words.map((w, i) => [w, "r" + i]));

test("zoom navigates session → forest roots → node children → span → records, on one view", async () => {
  const s = await put("nav", lin("nav", ["w0", "w1", "w2", "w3", "w4"]));
  await drain(db);
  const top = await zb({ id: "session:" + s });
  expect(top.complete).toBe(true);
  expect(top.items.map((i: any) => i.leaves)).toEqual([[0, 4], [4, 5]]);
  expect(top.items[0].id).toMatch(/^node:/);
  const n = await zb({ id: top.items[0].id, view_key: top.view_key });
  expect(n.items.map((i: any) => i.leaves)).toEqual([[0, 2], [2, 4]]);
  const n2 = await zb({ id: n.items[0].id, view_key: top.view_key });
  expect(n2.items.map((i: any) => i.id.slice(0, 5))).toEqual(["span:", "span:"]);
  const sp = await zb({ id: n2.items[0].id, view_key: top.view_key });
  expect(sp.records.map((r: any) => r.text).join(" ")).toContain("w0");
  expect(sp.after).toBe(n2.items[1].id);
  // A node without the view key it belongs to is refused, not looked up on some branch.
  await expect(zoom(db, T0({ id: top.items[0].id }))).rejects.toThrow("view_key");
});

test("zoom forest on two branches: the shared prefix is the same node; nodes of one branch are not found on the other", async () => {
  const base = lin("fb", ["w0", "w1", "w2", "w3", "w4", "w5"]);
  const alt = [msg("user", "x4", "a3", "z4"), msg("assistant", "y4", "x4", "q"), msg("user", "x5", "y4", "z5"), msg("assistant", "y5", "x5", "q")];
  const s = await put("fb", [...base, ...alt]);
  await drain(db);
  const views = await db`select tip from view where source = ${s} order by ord`;
  const tops: any[] = [];
  for (const v of views) tops.push(await zb({ id: "session:" + s, leaf: undefined, view_key: { source: s, tip: v.tip, basis: "observed", version: (await zb({ id: "session:" + s })).view_key.version } }));
  const [A, B] = tops;
  expect(A.items[0].id).toBe(B.items[0].id); // [w0..w3] shared
  const onlyA = A.items.find((i: any) => i.id.startsWith("node:") && !B.items.some((j: any) => j.id === i.id));
  expect(onlyA).toBeTruthy();
  await expect(zoom(db, T0({ id: onlyA.id, view_key: B.view_key }))).rejects.toThrow("not found");
});

test("zoom forest never calls a model; a cold or partial view is expanded from what is ready", async () => {
  const s = await put("cz", lin("cz", ["w0", "w1", "w2", "w3"]));
  process.env.MEMEX_FOREST_HOT_DAYS = "-1"; // cold
  try {
    await drain(db);
  } finally {
    process.env.MEMEX_FOREST_HOT_DAYS = "100000";
  }
  const n = requests.length;
  const top = await zb({ id: "session:" + s });
  expect(requests.length).toBe(n);
  expect(top.complete).toBe(false);
  expect(top.items.map((i: any) => i.summary)).toEqual(["user: w0", "user: w1", "user: w2", "user: w3"]);
  expect((await db`select 1 from forest_access where source = ${s}`).length).toBe(1); // asked for, in the background
  await drain(db);
  expect((await zb({ id: "session:" + s })).items.length).toBe(1);
});

test("zoom forest: an unknown leaf or a stale key is not replaced by the current tail", async () => {
  const s = await put("uk", lin("uk", ["w0", "w1", "w2", "w3"]));
  await drain(db);
  const top = await zb({ id: "session:" + s });
  const u = await zb({ id: "session:" + s, view_key: { ...top.view_key, tip: "nope" } });
  expect(u).toMatchObject({ view_basis: "unknown", reason: "view-not-found", items: [] });
  await put("uk", lin("uk", ["w0", "w1 changed", "w2", "w3"]));
  await drain(db);
  await expect(zoom(db, T0({ id: top.items[0].id, view_key: top.view_key }))).rejects.toThrow("view-stale");
});

test("zoom forest hides summaries made from a source not readable now: a fork child's mixed node while the parent's projection is stale", async () => {
  const pl = lin("wp", ["ptext0", "ptext1", "ptext2"]);
  const P = await put("wp", pl);
  const C = await put("wc", [header("wc", { parentSession: "/x/wp.jsonl" }), ...pl.slice(1), msg("user", "c3", "a2", "kid3"), msg("assistant", "d3", "c3", "r")], "personal", P);
  await drain(db);
  const top = await zb({ id: "session:" + C });
  expect(JSON.stringify(top)).toContain("ptext0");
  // The parent withdraws something (its projection is stale until projected again); the child is still
  // valid — its own projection names only its version and the parent's old one, so it is stale too, or not:
  // either way no parent text may come out while the parent is not readable.
  const rev = [...pl];
  rev[1] = msg("user", "u0", null, "ptext0 changed");
  await put("wp", rev);
  const after = await zoom(db, T0({ id: "session:" + C })).then((g) => JSON.stringify(g.body), (e) => String(e));
  expect(after).not.toContain("ptext0");
});

test("zoom forest: a guessed node id of an unreadable session is not served", async () => {
  const W = await put("ws", lin("ws", ["hidden0", "hidden1", "hidden2", "hidden3"]), "work");
  await drain(db);
  const [n] = await db`select id from forest_node where text is not null limit 1`;
  expect(n).toBeTruthy();
  await expect(zoom(db, T0({ id: "session:" + W }))).rejects.toThrow("not found");
  await expect(zoom(db, T0({ id: "node:" + n.id, view_key: { source: W, tip: "a3", basis: "observed", version: "x" } }))).rejects.toThrow("not found");
});

test("zoom forest: a forgotten parent's text is not served through the child's forest", async () => {
  const pl = lin("fp", ["ptext0", "ptext1", "ptext2"]);
  const P = await put("fp", pl);
  const C = await put("fc", [header("fc", { parentSession: "/x/fp.jsonl" }), ...pl.slice(1), msg("user", "c3", "a2", "kid3"), msg("assistant", "d3", "c3", "r")], "personal", P);
  await drain(db);
  expect(JSON.stringify(await zb({ id: "session:" + C }))).toContain("ptext0");
  await forgetSource(db, P);
  const read = () => zoom(db, T0({ id: "session:" + C })).then((g) => JSON.stringify(g.body), (e) => String(e));
  expect(await read()).not.toContain("ptext"); // the child is stale until projected again: not found
  await drain(db);
  const r = await read();
  expect(r).not.toContain("ptext");
  expect(r).toContain("kid3");
});

test("zoom forest over HTTP: the session's read grant revoked between the read and its delivery → no forest or card text", async () => {
  const s = await put("rv", lin("rv", ["rvsecret0", "rvsecret1", "rvsecret2", "rvsecret3"]));
  await drain(db);
  let fired = 0;
  httpHooks.beforeGuard = async () => {
    if (fired++) return;
    await revokeGrant(db, "personal", "tool", "memex");
  };
  const r = await http()("POST", "/v1/zoom", T0({ id: "session:" + s })).then((x) => JSON.stringify(x), (e) => String(e));
  expect(fired).toBeGreaterThan(0);
  expect(r).not.toContain("rvsecret");
});

test("zoom session: carries its card (the stored one when current, else degraded), no model call in the read", async () => {
  const s = await put("zc", lin("zc", ["w0", "w1", "w2", "w3"]));
  await drain(db);
  const n = requests.length;
  const top = await zb({ id: "session:" + s });
  expect(requests.length).toBe(n);
  expect(top).toMatchObject({ card_degraded: false, card: { title: "T", overview: "card", cwd: "/x" } });
  await db`delete from session_card`;
  const d = await zb({ id: "session:" + s });
  expect(d.card_degraded).toBe(true);
  expect(d.card.overview).toContain("user: w0");
});

test("zoom session: a complete forest with a degraded card asks for the card in the background (cold, 3 leaves)", async () => {
  process.env.MEMEX_FOREST_HOT_DAYS = "-1";
  let s: string;
  try {
    s = await put("dc", lin("dc", ["w0", "w1", "w2"]));
    await drain(db);
  } finally {
    process.env.MEMEX_FOREST_HOT_DAYS = "100000";
  }
  expect((await db`select count(*)::int n from session_card`)[0].n).toBe(0);
  const n = requests.length;
  const top = await zb({ id: "session:" + s! });
  expect(requests.length).toBe(n);
  expect(top).toMatchObject({ complete: true, card_degraded: true });
  process.env.MEMEX_FOREST_HOT_DAYS = "-1"; // still cold by its records: the read is what makes it wanted
  try {
    await drain(db);
  } finally {
    process.env.MEMEX_FOREST_HOT_DAYS = "100000";
  }
  expect((await zb({ id: "session:" + s! })).card_degraded).toBe(false);
});

// ---------- review stage 3 #6: forest zoom is paged ----------

test("a cold session's forest is paged by bytes; continuations neither repeat nor skip leaves, even when merges land between pages", async () => {
  const words = Array.from({ length: 64 }, (_, i) => `w${i}` + "x".repeat(600));
  summaryOf = (input) => (/kind="user">\n?([^<]*)/.exec(input.slice(input.indexOf("<input>")))?.[1]?.trim() ?? "s").slice(0, 590);
  process.env.MEMEX_FOREST_HOT_DAYS = "-1"; // cold: no merges, every leaf shown
  let s: string;
  try {
    s = await put("pg", lin("pg", [...words, "tail"]));
    await drain(db);
  } finally {
    process.env.MEMEX_FOREST_HOT_DAYS = "100000";
  }
  const p1 = await zb({ id: "session:" + s });
  expect(Buffer.byteLength(JSON.stringify(p1.items))).toBeLessThan(20 * 1024);
  expect(p1.more).toBe(true);
  // Merges complete between the pages (the read asked for them).
  await drain(db);
  expect((await db`select count(*)::int n from forest_node where text is not null`)[0].n).toBeGreaterThan(0); // they did land
  const seen: [number, number][] = p1.items.map((i: any) => i.leaves);
  let next = p1.next_offset;
  for (let k = 0; next !== null && k < 20; k++) {
    const p = await zb({ id: "session:" + s, view_key: p1.view_key, offset: next });
    seen.push(...p.items.map((i: any) => i.leaves));
    next = p.next_offset;
  }
  expect(next).toBeNull();
  // A contiguous cover of every leaf: none repeated or skipped.
  const total = p1.items.length && (await db`select spans, closed_end from view where source = ${s}`).map((v: any) => v.spans.filter((x: any) => x.id && x.end <= v.closed_end).length)[0];
  let at = 0;
  for (const [a, b] of seen) {
    expect(a).toBe(at);
    at = b;
  }
  expect(at).toBe(total);
  expect(total).toBeGreaterThan(60);
});

test("a continuation needs the view key; a revised view refuses it", async () => {
  const s = await put("pr", lin("pr", ["a0", "a1", "a2", "a3", "a4"]));
  await drain(db);
  const p1 = await zb({ id: "session:" + s });
  await expect(zoom(db, T0({ id: "session:" + s, offset: 2 }))).rejects.toThrow("view_key");
  await put("pr", lin("pr", ["a0 changed", "a1", "a2", "a3", "a4"]));
  await drain(db);
  await expect(zoom(db, T0({ id: "session:" + s, view_key: p1.view_key, offset: 2 }))).rejects.toThrow();
});

test("forest pages follow the requested limit (one item at least, for progress)", async () => {
  const words = Array.from({ length: 16 }, (_, i) => `v${i}` + "x".repeat(600));
  summaryOf = (input) => (/kind="user">\n?([^<]*)/.exec(input.slice(input.indexOf("<input>")))?.[1]?.trim() ?? "s").slice(0, 590);
  process.env.MEMEX_FOREST_HOT_DAYS = "-1";
  let s: string;
  try {
    s = await put("pl", lin("pl", [...words, "tail"]));
    await drain(db);
  } finally {
    process.env.MEMEX_FOREST_HOT_DAYS = "100000";
  }
  const p = await zb({ id: "session:" + s, limit: 1024 });
  expect(p.items.length).toBeLessThanOrEqual(2);
  expect(p.more).toBe(true);
  const one = await zb({ id: "session:" + s, limit: 10 });
  expect(one.items.length).toBe(1);
});
