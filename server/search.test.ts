// Search (DESIGN §6.3, §4.3, §7) over real Postgres, HTTP via node:http, and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { connect, migrate, bump, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { EMBED_DIM, revokeGrant } from "./derive.ts";
import { addApiKey, httpHooks, serve, transport } from "./http.ts";
import { search, GUARANTEED } from "./search.ts";
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
