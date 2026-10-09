// L2 summary forest (DESIGN §4.4, §5.5) over real Postgres and a fake model endpoint.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { claimJob, connect, migrate, type Db } from "./db.ts";
import { commit, requestLease, tryGrant, BARRIER } from "./sync.ts";
import { revokeGrant } from "./derive.ts";
import { readForest, roots, runMerge } from "./forest.ts";
import { forgetSource } from "./forget.ts";
import { drain } from "./worker.ts";
import { sourceIdOf } from "../client/records.ts";

let db: Db;
let requests: { path: string; body: any }[] = [];
let hold: Promise<void> | null = null;
let embedDown = false;
let srv: ReturnType<typeof Bun.serve>;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = await req.json();
      requests.push({ path, body });
      if (path.endsWith("/embeddings")) {
        if (embedDown) return new Response("down", { status: 503 });
        return Response.json({ data: [{ embedding: Array.from({ length: 1024 }, (_, i) => (i === 0 ? 1 : 0)) }] });
      }
      if (body.messages[0].content.startsWith("You write the card")) {
        if (hold) await hold;
        const parts = [...(body.messages[1].content as string).matchAll(/<(part|recent)>\n([\s\S]*?)\n<\/\1>/g)].map((m) => `${m[1]}:${m[2]}`);
        return Response.json({ choices: [{ message: { content: JSON.stringify({ title: "T", overview: parts.join(" | "), results: [], open_threads: [] }) } }] });
      }
      if (body.messages[0].content.startsWith("You merge")) {
        if (hold) await hold;
        // A merged summary: the part texts joined, so tests can see exactly what went in.
        const parts = [...(body.messages[1].content as string).matchAll(/<part n="\d">\n([\s\S]*?)\n<\/part>/g)].map((m) => m[1]);
        return Response.json({ choices: [{ message: { content: JSON.stringify({ summary: parts.join(" + ") }) } }] });
      }
      // A span summary: the user's words in <input> (each test exchange has a distinct token).
      const input = (body.messages[1].content as string).split("<input>")[1] ?? "";
      const words = [...input.matchAll(/kind="user">\s*(?:user: )?([^<]*)</g)].map((m) => m[1].trim()).join(" ");
      const s = { summary: "S(" + words + ")", user_asked: ["q"], assistant_chose: [], verified: [], tasks: [], kind: ["chat"], anchors: [] };
      return Response.json({ choices: [{ message: { content: JSON.stringify(s) } }] });
    },
  });
  Object.assign(process.env, {
    MEMEX_LLM_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_LLM_MODEL: "fake-llm", MEMEX_LLM_KEY: "k-llm",
    MEMEX_EMBED_URL: `http://127.0.0.1:${srv.port}/v1`, MEMEX_EMBED_MODEL: "fake-emb", MEMEX_EMBED_KEY: "k-emb",
  });
});
afterAll(async () => {
  srv.stop(true);
  await db.close();
});
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule, source, vis_version, tomb, lease, commit_result, body, projection, record,
                   scan_progress, collector_view, job, span, view, projected, lexical, span_summary, derive_skip, forest_node, forest_access, session_card
                   restart identity cascade`);
  await db`insert into grant_rule values ('personal', 'sync', 'memex'), ('personal', 'derive', 'llm'), ('personal', 'derive', 'embedding')`;
  process.env.MEMEX_IDLE_CLOSE_MS = String(10 * 60_000);
  delete process.env.MEMEX_FOREST_HOT_DAYS;
  requests = [];
  hold = null;
  embedDown = false;
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
let T = new Date().toISOString(); // recent: a hot session
const header = (id: string, parentSession?: string) => JSON.stringify({ type: "session", version: 3, id, timestamp: T, cwd: "/x", ...(parentSession ? { parentSession } : {}) });
const msg = (role: string, id: string, parent: string | null, text: string) =>
  JSON.stringify({ type: "message", id, parentId: parent, timestamp: T, message: { role, content: [{ type: "text", text }] } });
let commitN = 0;
async function put(native: string, lines: string[], parent: string | null = null) {
  const decl = { source: sourceIdOf("pi", native), harness: "pi" as const, nativeId: native, scope: "personal", parent };
  const l = await requestLease(db, decl, "m1");
  expect((await tryGrant(db, l)).granted).toBe(true);
  const r = await commit(db, { leaseId: l, commitId: `f${commitN++}`, length: lines.length, changes: lines.map((raw, pos) => ({ pos, id: pos === 0 ? "header" : JSON.parse(raw).id, hash: sha(raw), raw })) });
  expect(r.ok).toBe(true);
  return decl.source;
}
/** A linear session of n closed exchanges (one more opens the tail): exchange i says `t<i>`. */
const linear = (native: string, n: number, from = 0, reply = "r") => {
  const out = [header(native)];
  let prev: string | null = null;
  for (let i = 0; i <= n; i++) {
    out.push(msg("user", `u${i}`, prev, `t${from + i}`), msg("assistant", `a${i}`, `u${i}`, `${reply}${i}`));
    prev = `a${i}`;
  }
  return out;
};
const merges = () => requests.filter((r) => r.body.messages?.[0].content.startsWith("You merge"));
const nodes = async () => (await db`select id, size, text from forest_node order by size, id`) as any[];
const tipOf = async (s: string) => (await db`select tip from view where source = ${s} order by ord limit 1`)[0].tip as string;
const shape = (items: { size: number }[]) => items.map((i) => i.size);

test("roots: binary carry, nothing below 4 leaves", () => {
  expect(roots(13).map((r) => r[1])).toEqual([8, 4, 1]);
  expect(roots(14).map((r) => r[1])).toEqual([8, 4, 2]);
  expect(roots(16).map((r) => r[1])).toEqual([16]);
  expect(roots(3).map((r) => r[1])).toEqual([1, 1, 1]);
});

test("13 → 14 → 16 leaves: roots [8,4,1] [8,4,2] [16]; nodes only added (10, 11, 15), never rewritten", async () => {
  const s = await put("g", linear("g", 13));
  await drain(db);
  let f = (await readForest(db, s, await tipOf(s)))!;
  expect(f).toMatchObject({ stale: false, complete: true });
  expect(shape(f.items)).toEqual([8, 4, 1]);
  expect(f.items[0].text).toBe(Array.from({ length: 8 }, (_, i) => `S(t${i})`).join(" + "));
  const n13 = await nodes();
  expect(n13.length).toBe(10);
  expect(merges().length).toBe(10);
  await put("g", linear("g", 14));
  await drain(db);
  f = (await readForest(db, s, await tipOf(s)))!;
  expect(shape(f.items)).toEqual([8, 4, 2]);
  expect((await nodes()).length).toBe(11);
  expect(merges().length).toBe(11); // only [13–14]
  await put("g", linear("g", 16));
  await drain(db);
  f = (await readForest(db, s, await tipOf(s)))!;
  expect(shape(f.items)).toEqual([16]);
  const n16 = await nodes();
  expect(n16.length).toBe(15);
  expect(merges().length).toBe(15);
  for (const n of n13) expect(n16).toContainEqual(n); // same ids, same text
});

test("fewer than 4 leaves: no nodes, the leaves are listed", async () => {
  const s = await put("small", linear("small", 3));
  await drain(db);
  const f = (await readForest(db, s, await tipOf(s)))!;
  expect(shape(f.items)).toEqual([1, 1, 1]);
  expect(f.items.map((i) => i.text)).toEqual(["S(t0)", "S(t1)", "S(t2)"]);
  expect((await nodes()).length).toBe(0);
  expect(merges().length).toBe(0);
});

test("a missing leaf summary: nothing merges across it; the read expands what is ready", async () => {
  const s = await put("hole", linear("hole", 4));
  // Leaf 1 (t1) is not summarized: its job is held back.
  await drain(db, ["project"]);
  const t1 = (await db`select id from span where source = ${s} and keys[1] like 'hole/u1:%'`)[0].id;
  await db`update job set not_before = now() + interval '1 hour' where kind = 'summarize' and key = ${t1}`;
  await drain(db);
  const ns = await nodes();
  expect(ns.length).toBe(1); // only [t2 t3]; [1–4] and [t0 t1] wait for t1
  expect(ns[0].text).toBe("S(t2) + S(t3)");
  const f = (await readForest(db, s, await tipOf(s)))!;
  expect(f.complete).toBe(false);
  expect(f.items.map((i) => [i.size, i.text])).toEqual([[1, "S(t0)"], [1, null], [2, "S(t2) + S(t3)"]]);
  await db`update job set not_before = now() where key = ${t1}`;
  await drain(db);
  const g = (await readForest(db, s, await tipOf(s)))!;
  expect(g.complete).toBe(true);
  expect(g.items.map((i) => i.text)).toEqual(["S(t0) + S(t1) + S(t2) + S(t3)"]);
});

test("branches share the subtrees before the fork; each carries on its own after it", async () => {
  // Exchanges t0..t5 closed; then a second branch from a3 with its own t6', t7'.
  const base = linear("br", 6);
  const s = await put("br", base);
  await drain(db);
  const before = new Set((await nodes()).map((n) => n.id));
  const alt = [msg("user", "x4", "a3", "z4"), msg("assistant", "y4", "x4", "q"), msg("user", "x5", "y4", "z5"), msg("assistant", "y5", "x5", "q"),
               msg("user", "x6", "y5", "z6"), msg("assistant", "y6", "x6", "q")];
  await put("br", [...base, ...alt]);
  await drain(db);
  const views = await db`select tip from view where source = ${s} order by ord`;
  expect(views.length).toBe(2);
  const fa = (await readForest(db, s, "a6"))!;
  const fb = (await readForest(db, s, "y6"))!;
  expect(shape(fa.items)).toEqual([4, 2]);
  expect(shape(fb.items)).toEqual([4, 2]);
  expect(fa.items[0].node).toBe(fb.items[0].node); // [t0..t3] shared
  expect(fa.items[1].node).not.toBe(fb.items[1].node);
  expect(fb.items[1].text).toBe("S(z4) + S(z5)");
  for (const id of before) expect((await nodes()).map((n) => n.id)).toContain(id);
});

test("a fork inside a span: the prefix P is a leaf of the new branch; nodes over it are its own", async () => {
  // Exchange t1 has two assistant replies in a row on branch A; branch B forks after the first one.
  const l = [header("pf"), msg("user", "u0", null, "t0"), msg("assistant", "a0", "u0", "r"), msg("user", "u1", "a0", "t1"), msg("assistant", "a1", "u1", "r"),
    msg("assistant", "a1b", "a1", "more"), msg("user", "u2", "a1b", "t2"), msg("assistant", "a2", "u2", "r"), msg("user", "u3", "a2", "t3"), msg("assistant", "a3", "u3", "r"),
    msg("user", "u4", "a3", "t4"), msg("assistant", "a4", "u4", "r")];
  const s = await put("pf", [...l, msg("user", "v2", "a1", "w2"), msg("assistant", "b2", "v2", "r"), msg("user", "v3", "b2", "w3"), msg("assistant", "b3", "v3", "r"),
    msg("user", "v4", "b3", "w4"), msg("assistant", "b4", "v4", "r")]);
  await drain(db);
  const fa = (await readForest(db, s, "a4"))!;
  const fb = (await readForest(db, s, "b4"))!;
  expect(fa.complete && fb.complete).toBe(true);
  expect(fa.items[0].text).toBe("S(t0) + S(t1) + S(t2) + S(t3)");
  expect(fb.items[0].text).toBe("S(t0) + S(t1) + S(w2) + S(w3)");
  // [t0 t1] on A covers S, on B the prefix P: different leaves, different nodes.
  const leafPairs = await db`select id, spans from forest_node where size = 2 and text like 'S(t0) + S(t1)'`;
  expect(leafPairs.length).toBe(2);
});

test("an appended exchange keeps every node; a revision makes its nodes unreadable and they are rebuilt", async () => {
  const s = await put("rv", linear("rv", 4));
  await drain(db);
  const n0 = merges().length;
  expect(n0).toBe(3);
  await put("rv", linear("rv", 5));
  await drain(db);
  expect(merges().length).toBe(n0); // 5 leaves: [1–4][5], nothing to merge
  // Revise exchange t2's user text (in place, same id).
  const rev = linear("rv", 5);
  rev[5] = msg("user", "u2", "a1", "t2 revised");
  await put("rv", rev);
  // Before re-projection and re-summarizing, the old [1–4] is not readable (the projection is stale).
  expect((await readForest(db, s, "a5"))!.stale).toBe(true);
  await drain(db);
  const f = (await readForest(db, s, "a5"))!;
  expect(f.items[0].text).toBe("S(t0) + S(t1) + S(t2 revised) + S(t3)");
  expect(merges().length).toBe(n0 + 2); // [t2' t3] and [1–4]; [t0 t1] reused
});

test("a cold session: structure only, zero merge calls; a read expands leaves and asks for the merges", async () => {
  const old = T;
  T = "2020-01-01T00:00:00Z"; // imported history
  try {
    const s = await put("cold", linear("cold", 4));
    await drain(db);
    expect(merges().length).toBe(0);
    expect((await db`select count(*)::int n from forest_node where text is null`)[0].n).toBe(3);
    const f = (await readForest(db, s, await tipOf(s)))!;
    expect(f.complete).toBe(false);
    expect(f.items.map((i) => i.text)).toEqual(["S(t0)", "S(t1)", "S(t2)", "S(t3)"]); // no model call in the read
    expect(merges().length).toBe(0);
    await drain(db); // the background work it asked for
    expect(merges().length).toBe(3);
    expect((await readForest(db, s, await tipOf(s)))!.items.map((i) => i.size)).toEqual([4]);
  } finally {
    T = old;
  }
});

test("the embedding service down does not hold the forest back", async () => {
  embedDown = true;
  const s = await put("emb", linear("emb", 4));
  await drain(db);
  expect((await readForest(db, s, await tipOf(s)))!.complete).toBe(true);
});

/** Run the merge of the root [1–4] with the model held, doing `during` while the request is out. */
async function holdRootMerge(s: string, during: () => Promise<void>) {
  await coldThenStructure();
  // Children first.
  const pairs = (await db`select id from forest_node where size = 2`).map((r: any) => r.id);
  for (const id of pairs) await runMerge(db, (await claimJobFor("merge", id))!);
  const [root] = await db`select id from forest_node where size = 4`;
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const job = (await claimJobFor("merge", root.id))!;
  const n = merges().length;
  const run = runMerge(db, job);
  while (merges().length === n) await Bun.sleep(5);
  await during();
  release();
  await run;
  hold = null;
  return root.id as string;
}
/** Structure without merges (the session counts as cold), so the test runs each merge itself. */
async function coldThenStructure() {
  process.env.MEMEX_FOREST_HOT_DAYS = "-1";
  try {
    await drain(db);
  } finally {
    delete process.env.MEMEX_FOREST_HOT_DAYS;
  }
}
async function claimJobFor(kind: string, key: string) {
  await db`insert into job (kind, key) values (${kind}, ${key}) on conflict (kind, key) where state in ('queued', 'running') do nothing`;
  await db`update job set not_before = now() + interval '1 hour' where state = 'queued' and not (kind = ${kind} and key = ${key})`;
  await db`update job set not_before = now() where state = 'queued' and kind = ${kind} and key = ${key}`;
  const j = await claimJob(db, [kind]);
  await db`update job set not_before = now() where state = 'queued'`;
  expect(j?.key).toBe(key);
  return j;
}

test("forget during the merge call: not published; the source's nodes are gone", async () => {
  const s = await put("fg", linear("fg", 4));
  const root = await holdRootMerge(s, async () => void (await forgetSource(db, s)));
  expect((await db`select count(*)::int n from forest_node where id = ${root}`)[0].n).toBe(0);
  expect((await nodes()).length).toBe(0);
});

test("a revision during the merge call: the reply is not published", async () => {
  const s = await put("rd", linear("rd", 4));
  const root = await holdRootMerge(s, async () => {
    const rev = linear("rd", 4);
    rev[3] = msg("user", "u1", "a0", "t1 changed");
    await put("rd", rev);
  });
  expect((await db`select text from forest_node where id = ${root}`)[0]?.text ?? null).toBe(null);
});

test("revoke of the model destination during the merge call: not published, nothing more sent", async () => {
  const s = await put("rk", linear("rk", 4));
  const root = await holdRootMerge(s, () => revokeGrant(db, "personal", "derive", "llm"));
  const n = merges().length;
  await drain(db);
  expect((await db`select text from forest_node where id = ${root}`)[0].text).toBe(null);
  expect(merges().length).toBe(n);
});

/** Merge `node` once, running `between` after its exact check and before its guarded send. */
async function betweenCheckAndSend(node: string, between: () => Promise<unknown>) {
  const { hooks } = await import("./derive.ts");
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    await between();
  };
  const n = merges().length;
  try {
    await runMerge(db, (await claimJobFor("merge", node))!);
  } finally {
    hooks.afterCheck = async () => {};
  }
  return merges().length - n;
}

test("between the check and the send — forget, revoke, revision, canonical remap: zero merge requests", async () => {
  // forget
  let s = await put("cs1", linear("cs1", 4));
  await coldThenStructure();
  let [pair] = await db`select id from forest_node where size = 2 order by id limit 1`;
  expect(await betweenCheckAndSend(pair.id, () => forgetSource(db, s))).toBe(0);
  // revoke of the destination (other content: forget's content tombs match identical copies anywhere)
  s = await put("cs2", linear("cs2", 4, 10, "q"));
  await coldThenStructure();
  [pair] = await db`select f.id from forest_node f where size = 2 and f.spans[1] in (select id from span where source = ${s} and keys[1] like 'cs2/u0:%')`;
  expect(await betweenCheckAndSend(pair.id, () => revokeGrant(db, "personal", "derive", "llm"))).toBe(0);
  await db`insert into grant_rule values ('personal', 'derive', 'llm') on conflict do nothing`;
  // revision of a leaf's record
  s = await put("cs3", linear("cs3", 4, 20, "w"));
  await coldThenStructure();
  [pair] = await db`select f.id from forest_node f where size = 2 and f.spans[1] in (select id from span where source = ${s} and keys[1] like 'cs3/u0:%')`;
  const rev = linear("cs3", 4, 20, "w");
  rev[1] = msg("user", "u0", null, "t0 changed");
  expect(await betweenCheckAndSend(pair.id, () => put("cs3", rev))).toBe(0);
});

test("canonical remap between the check and the send: the child's mixed merge is not sent", async () => {
  // The child holds records the parent has not synced yet (its own for now); the parent catches up.
  const pl = linear("rm", 4);
  const P = await put("rm", pl.slice(0, 3));
  await drain(db);
  const C = await put("rn", [header("rn", "/x/rm.jsonl"), ...pl.slice(1), msg("user", "k1", "a4", "kid")], P);
  await coldThenStructure();
  // A pair over the child's records that the parent will take over (u1..a2 are the child's own for now).
  const [pair] = await db`select f.id from forest_node f where size = 2 and f.text is null
                            and exists (select 1 from span sp where sp.id = any(f.spans) and sp.source = ${C} and sp.keys[1] like 'rn/u1:%')`;
  expect(pair).toBeTruthy();
  const sent = await betweenCheckAndSend(pair.id, async () => {
    await put("rm", pl);
    await drain(db, ["project"]);
  });
  expect(sent).toBe(0);
  expect((await db`select text from forest_node where id = ${pair.id}`)[0]?.text ?? null).toBe(null);
});

test("forget of the parent while the child's mixed root is with the model: not published", async () => {
  const pl = linear("mp", 3); // parent t0..t2 closed
  const P = await put("mp", pl);
  const C = await put("mc", [header("mc", "/x/mp.jsonl"), ...pl.slice(1, 7), msg("user", "k3", "a2", "c3"), msg("assistant", "k3a", "k3", "r"), msg("user", "k4", "k3a", "c4")], P);
  const root = await holdRootMerge(C, async () => void (await forgetSource(db, P)));
  expect((await db`select text from forest_node where id = ${root}`)[0]?.text ?? null).toBe(null);
  expect(JSON.stringify(await db`select text from forest_node`)).not.toContain("S(t0)");
});

test("a fork child's mixed node: forgetting the parent removes it; the rebuild holds no parent text", async () => {
  const pl = linear("fp", 3); // parent t0..t2 closed, t3 open
  const P = await put("fp", pl);
  const kid = [header("fk", "/x/fp.jsonl"), ...pl.slice(1, 7), msg("user", "k3", "a2", "c3"), msg("assistant", "k3a", "k3", "r"), msg("user", "k4", "k3a", "c4"), msg("assistant", "k4a", "k4", "r")];
  const C = await put("fk", kid, P);
  await drain(db);
  const f = (await readForest(db, C, "k4a"))!;
  expect(f.items[0].text).toBe("S(t0) + S(t1) + S(t2) + S(c3)");
  await forgetSource(db, P);
  await drain(db);
  const all = (await nodes()).map((n) => n.text ?? "").join("|");
  expect(all).not.toContain("S(t0)");
  const g = (await readForest(db, C, "k4a"))!;
  expect(JSON.stringify(g)).not.toContain("S(t0)");
});

test("a leaf summary replaced (same span and input): a new node; the old text is not read", async () => {
  const s = await put("rs", linear("rs", 4));
  await drain(db);
  const [sp] = await db`select s.id from span s where s.source = ${s} and s.keys[1] like 'rs/u1:%'`;
  // As a new summary of the same input would (another run of the model): new text, new hash.
  await db`update span_summary set text = 'S(other)', text_hash = 'h-other' where span = ${sp.id}`;
  await db`insert into job (kind, key) values ('forest', ${s})`;
  await drain(db);
  const f = (await readForest(db, s, await tipOf(s)))!;
  expect(f.items[0].text).toBe("S(t0) + S(other) + S(t2) + S(t3)");
});

test("a forgotten record (tomb, before any cleanup): its leaf and every node over it are not read", async () => {
  const s = await put("tb", linear("tb", 4));
  await drain(db);
  const [sp] = await db`select canons from span where source = ${s} and keys[1] like 'tb/u1:%'`;
  await db`insert into tomb values (${"canon:" + sp.canons[0]})`;
  const f = (await readForest(db, s, await tipOf(s)))!;
  expect(JSON.stringify(f)).not.toContain("S(t1)");
  expect(f.items.map((i) => i.text)).toEqual(["S(t0)", null, "S(t2) + S(t3)"]);
});

test("a node's text is written once: a second merge finishing later does not replace it", async () => {
  const s = await put("wo", linear("wo", 4));
  await coldThenStructure();
  const [pair] = await db`select id from forest_node where size = 2 order by id limit 1`;
  let release!: () => void;
  hold = new Promise((r) => (release = r));
  const n = merges().length;
  const run = runMerge(db, (await claimJobFor("merge", pair.id))!);
  while (merges().length === n) await Bun.sleep(5);
  await db`update forest_node set text = 'first', text_hash = 'h1', deps = '{}' where id = ${pair.id}`;
  release();
  await run;
  hold = null;
  expect((await db`select text from forest_node where id = ${pair.id}`)[0].text).toBe("first");
});

test("a written node whose leaf input changed (context, same span) is not renewed while the old summary stands", async () => {
  const s = await put("cx", linear("cx", 4));
  await drain(db);
  const [pair] = await db`select id, deps from forest_node where size = 2 and text like 'S(t2)%'`;
  // Revise t1: the span [t2] keeps its records, its <context> changes.
  const rev = linear("cx", 4);
  rev[3] = msg("user", "u1", "a0", "t1 again");
  await put("cx", rev);
  await drain(db, ["project"]);
  // Summaries not redone yet: the old [t2] summary row still has the old input.
  await db`update job set not_before = now() + interval '1 hour' where kind = 'summarize'`;
  await runMerge(db, (await claimJobFor("merge", pair.id))!);
  expect((await db`select deps from forest_node where id = ${pair.id}`)[0].deps).toEqual(pair.deps);
});

test("summaries already current (no model call): the forest is still built from them", async () => {
  const s = await put("up", linear("up", 4));
  await drain(db);
  await db`truncate forest_node`;
  await db`delete from job where kind in ('forest', 'merge')`;
  for (const r of await db`select span from span_summary`) await db`insert into job (kind, key) values ('summarize', ${r.span})`;
  const n = requests.filter((r) => r.body.messages?.[0].content.startsWith("You summarize")).length;
  await drain(db);
  expect(requests.filter((r) => r.body.messages?.[0].content.startsWith("You summarize")).length).toBe(n);
  expect((await readForest(db, s, await tipOf(s)))!.items.map((i) => i.size)).toEqual([4]);
});

test("a fork child made only of the parent's summarized spans reads the parent's nodes (same leaves, same ids)", async () => {
  const pl = linear("pp", 4);
  const P = await put("pp", pl);
  await drain(db);
  // The child repeats the parent's closed exchanges and opens its own tail: no new closed span.
  const C = await put("pc", [header("pc", "/x/pp.jsonl"), ...pl.slice(1, 9), msg("user", "c1", "a3", "own tail")], P);
  await drain(db);
  const f = (await readForest(db, C, "c1"))!;
  expect(f.items.map((i) => i.text)).toEqual(["S(t0) + S(t1) + S(t2) + S(t3)"]);
});

test("a parent's open tail closed on the child's view: summarized and in the child's forest without the parent moving", async () => {
  const pl = linear("op", 2); // parent: t0, t1 closed, t2 open
  const P = await put("op", pl);
  const kid = [header("oc", "/x/op.jsonl"), ...pl.slice(1), msg("user", "k3", "a2", "c3"), msg("assistant", "k3a", "k3", "r"), msg("user", "k4", "k3a", "c4"), msg("assistant", "k4a", "k4", "r")];
  const C = await put("oc", kid, P);
  await drain(db);
  const f = (await readForest(db, C, "k4a"))!;
  expect(f.items.map((i) => i.text)).toEqual(["S(t0) + S(t1) + S(t2) + S(c3)"]);
});

test("a hot fork child of a cold parent, over the parent's summarized spans only: its forest is merged without a read", async () => {
  const old = T;
  T = "2020-01-01T00:00:00Z";
  const pl = linear("cp", 4);
  let P: string;
  try {
    P = await put("cp", pl);
    await drain(db);
  } finally {
    T = old;
  }
  expect(merges().length).toBe(0); // the cold parent: structure only
  await put("hc", [header("hc", "/x/cp.jsonl"), ...pl.slice(1, 9), msg("user", "c1", "a3", "own tail")], P!);
  await drain(db);
  expect(merges().length).toBe(3);
  expect((await readForest(db, sourceIdOf("pi", "hc"), "c1"))!.complete).toBe(true);
});

test("a node planned under one model or prompt is not written by a merge running under another", async () => {
  const s = await put("mm", linear("mm", 4));
  await coldThenStructure();
  const [pair] = await db`select id from forest_node where size = 2 order by id limit 1`;
  process.env.MEMEX_LLM_MODEL = "fake-llm-b";
  try {
    const n = merges().length;
    await runMerge(db, (await claimJobFor("merge", pair.id))!);
    expect(merges().length).toBe(n); // nothing sent for a node whose id names another model
    expect((await db`select text from forest_node where id = ${pair.id}`)[0].text).toBe(null);
    await drain(db); // the structure is planned again under the current model
  } finally {
    process.env.MEMEX_LLM_MODEL = "fake-llm";
  }
  const done = await db`select model from forest_node where text is not null`;
  expect(done.length).toBe(3);
  expect(done.every((r: any) => r.model === "fake-llm-b")).toBe(true);
});

test("forget removes every node made from the source: through <context> only, and over spans re-cut away", async () => {
  const pl = linear("fq", 4); // parent t0..t3 closed
  const P = await put("fq", pl);
  const C = await put("fr", [header("fr", "/x/fq.jsonl"), ...pl.slice(1, 9), msg("user", "c4", "a3", "c4"), msg("assistant", "d4", "c4", "r"),
    msg("user", "c5", "d4", "c5"), msg("assistant", "d5", "c5", "r"), msg("user", "c6", "d5", "tail")], P);
  await drain(db);
  // [c4 c5]: both spans the child's own; c4's <context> is the parent's last exchange.
  const [own] = await db`select id from forest_node where text = 'S(c4) + S(c5)'`;
  expect(own).toBeTruthy();
  // The child revises c5: [c4 c5'] is a new node, the old one stays stored (unreadable) for now.
  const C2 = [header("fr", "/x/fq.jsonl"), ...pl.slice(1, 9), msg("user", "c4", "a3", "c4"), msg("assistant", "d4", "c4", "r"),
    msg("user", "c5", "d4", "c5 v2"), msg("assistant", "d5", "c5", "r"), msg("user", "c6", "d5", "tail")];
  await put("fr", C2, P);
  await drain(db);
  expect((await db`select 1 from forest_node where text = 'S(c4) + S(c5)'`).length).toBe(1);
  await forgetSource(db, P);
  expect((await db`select 1 from forest_node where id = ${own.id}`).length).toBe(0);
  const left = await db`select text, deps from forest_node where text is not null`;
  expect(left.filter((r: any) => ("s:" + P) in (r.deps ?? {})).length).toBe(0);
  expect((await db`select count(*)::int n from span_summary where deps ? ${"s:" + P}`)[0].n).toBe(0);
  await drain(db);
  expect(JSON.stringify(await readForest(db, C, "c6"))).not.toContain("S(t");
  await forgetSource(db, C);
  expect((await db`select count(*)::int n from forest_node where text is not null`)[0].n).toBe(0);
});

test("a written root whose vector moved with no real change: one run re-checks down to the leaves and renews it", async () => {
  const s = await put("rr", linear("rr", 4));
  await drain(db);
  const [root] = await db`select id, deps from forest_node where size = 4`;
  const k = "s:" + s;
  await db.begin(async (tx: any) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    await tx`insert into vis_version values (${k}, 2) on conflict (key) do update set v = vis_version.v + 1`;
    await tx`insert into job (kind, key) values ('project', ${s}) on conflict (kind, key) where state in ('queued', 'running') do update set not_before = now()`; // as every bump comes with a projection
  });
  await drain(db, ["project"]);
  const n = requests.length;
  expect(await runMerge(db, (await claimJobFor("merge", root.id))!)).toBe("done");
  expect(requests.length).toBe(n); // no model call
  const [now] = await db`select deps from forest_node where id = ${root.id}`;
  expect(now.deps[k]).toBe(root.deps[k] + 1);
  // The children were not touched: their vectors are still the old ones.
  expect((await db`select deps from forest_node where size = 2`).every((r: any) => r.deps[k] === root.deps[k])).toBe(true);
});

test("closing evidence from a child's view that is no longer current: the parent's open span is not sent", async () => {
  const pl = linear("ep", 2); // parent: t2 open
  const P = await put("ep", pl);
  const kid = [header("ec", "/x/ep.jsonl"), ...pl.slice(1), msg("user", "k3", "a2", "c3"), msg("assistant", "k3a", "k3", "r")];
  await put("ec", kid, P);
  await drain(db, ["project"]);
  const [t2] = await db`select id from span where source = ${P} and keys[1] like 'ep/u2:%'`;
  // The child is cut back to the parent's records (c3 gone), not projected again yet.
  await put("ec", kid.slice(0, pl.length), P);
  const { runSummarize } = await import("./derive.ts");
  const n = requests.length;
  await runSummarize(db, (await claimJobFor("summarize", t2.id))!);
  expect(requests.length).toBe(n);
  expect((await db`select 1 from span_summary where span = ${t2.id}`).length).toBe(0);
});

test("forget of the fork whose view closed a parent's span: the summary is made again from a sibling's view", async () => {
  const pl = linear("sp", 2); // parent: t2 open
  const P = await put("sp", pl);
  const kid = (n: string, w: string) => [header(n, "/x/sp.jsonl"), ...pl.slice(1), msg("user", n + "3", "a2", w), msg("assistant", n + "3a", n + "3", "r")];
  const A = await put("sa", kid("sa", "ca"), P);
  const B = await put("sb", kid("sb", "cb"), P);
  await drain(db);
  const [t2] = await db`select id from span where source = ${P} and keys[1] like 'sp/u2:%'`;
  const [s0] = await db`select deps from span_summary where span = ${t2.id}`;
  const closer = [A, B].find((x) => ("s:" + x) in s0.deps)!;
  await forgetSource(db, closer);
  await drain(db);
  const [s1] = await db`select text, deps from span_summary where span = ${t2.id}`;
  expect(s1?.text).toBe("S(t2)");
  expect(("s:" + closer) in s1.deps).toBe(false);
});

test("unwritten nodes stored before the configuration was recorded: completed in a bounded number of jobs", async () => {
  const s = await put("lg", linear("lg", 4));
  await coldThenStructure();
  await db`update forest_node set model = null, prompt_v = null where text is null`;
  await db`insert into forest_access values (${s})`; // read once: merges wanted
  await db`insert into job (kind, key) values ('forest', ${s}) on conflict (kind, key) where state in ('queued', 'running') do nothing`;
  const ran = await drain(db, undefined, 50);
  expect(ran).toBeLessThan(50);
  expect((await readForest(db, s, await tipOf(s)))!.items.map((i) => i.size)).toEqual([4]);
});

test("a node planned under another prompt version is not written by the current merge", async () => {
  const s = await put("pv", linear("pv", 4));
  await coldThenStructure();
  await db`update forest_node set prompt_v = 'forest-merge-v0'`;
  const ids = (await db`select id from forest_node`).map((r: any) => r.id);
  const n = merges().length;
  for (const id of ids) await runMerge(db, (await claimJobFor("merge", id))!);
  expect(merges().length).toBe(n);
  expect((await db`select count(*)::int n from forest_node where text is not null`)[0].n).toBe(0);
});

test("a large forest (128 leaves) is written in one bounded step; a held exclusive barrier makes it give up, not wait", async () => {
  const s = await put("big", linear("big", 128));
  await coldThenStructure();
  expect((await nodes()).length).toBe(127);
  await db`truncate forest_node`;
  await db`insert into job (kind, key) values ('forest', ${s}) on conflict (kind, key) where state in ('queued', 'running') do nothing`;
  process.env.MEMEX_FOREST_HOLD_MS = "300";
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let locked!: () => void;
  const isLocked = new Promise<void>((r) => (locked = r));
  const blocker = db.begin(async (tx: any) => {
    await tx`select pg_advisory_xact_lock(${BARRIER})`;
    locked();
    await held;
  });
  await isLocked;
  const t0 = performance.now();
  const { runOne } = await import("./worker.ts");
  try {
    await runOne(db, ["forest"]);
    expect(performance.now() - t0).toBeLessThan(3000);
    expect((await nodes()).length).toBe(0);
  } finally {
    delete process.env.MEMEX_FOREST_HOLD_MS;
    release();
    await blocker;
  }
  await db`update job set not_before = now() where kind = 'forest'`;
  await coldThenStructure();
  expect((await nodes()).length).toBe(127);
});

test("the barrier held by a structure write is bounded as a whole: statements each short but long together give way to forget", async () => {
  const s = await put("hl", linear("hl", 4));
  await drain(db, ["project", "summarize", "embed"]);
  const { forestHooks } = await import("./forest.ts");
  const { runOne } = await import("./worker.ts");
  process.env.MEMEX_FOREST_HOLD_MS = "400";
  let inside!: () => void;
  const isInside = new Promise<void>((r) => (inside = r));
  forestHooks.underBarrier = async (tx: any) => {
    inside();
    await tx`select pg_sleep(0.25)`; // each below the deadline
    await tx`select pg_sleep(0.25)`;
    await tx`select pg_sleep(0.25)`;
  };
  try {
    const run = runOne(db, ["forest"]);
    await isInside;
    const t0 = performance.now();
    await forgetSource(db, s);
    expect(performance.now() - t0).toBeLessThan(400 + 300);
    await run;
  } finally {
    forestHooks.underBarrier = async () => {};
    delete process.env.MEMEX_FOREST_HOLD_MS;
  }
  expect((await nodes()).length).toBe(0);
  expect((await db`select 1 ok`)[0].ok).toBe(1); // the pool is usable after the server ended that session
});

test("a parent planned while its last child is being written is still merged (no lost wake-up)", async () => {
  const s = await put("lw", linear("lw", 4));
  await coldThenStructure();
  // Only the pairs stored: the root will be inserted by a planner racing the last pair.
  await db`delete from forest_node where size = 4`;
  const pairs = (await db`select id from forest_node where size = 2 order by id`).map((r: any) => r.id);
  await runMerge(db, (await claimJobFor("merge", pairs[0]))!);
  await db`insert into forest_access values (${s})`;
  // Planner: its snapshot sees pairs[1] unwritten; before it inserts the root, pairs[1] is published.
  const { snapshotHook } = await import("./forest.ts");
  snapshotHook.afterSnapshot = async () => {
    snapshotHook.afterSnapshot = async () => {};
    await runMerge(db, (await claimJobFor("merge", pairs[1]))!);
  };
  try {
    await db`insert into job (kind, key) values ('forest', ${s}) on conflict (kind, key) where state in ('queued', 'running') do nothing`;
    await runForestOnce(s);
  } finally {
    snapshotHook.afterSnapshot = async () => {};
  }
  await drain(db);
  expect((await readForest(db, s, await tipOf(s)))!.items.map((i) => i.size)).toEqual([4]);
});
async function runForestOnce(s: string) {
  const { runForest } = await import("./forest.ts");
  await runForest(db, (await claimJobFor("forest", s))!);
}

test("an ancestor of the closing view withdraws after the check, before the views below are projected again: nothing is sent", async () => {
  // G: g0..g1 closed, g2 open. P forks G and goes on (p3 open). C forks P and closes p3.
  const gl = linear("ga", 2);
  const G = await put("ga", gl);
  const pl = [header("pa", "/x/ga.jsonl"), ...gl.slice(1), msg("user", "p3", "a2", "pp3"), msg("assistant", "p3a", "p3", "r"),
    msg("user", "p4", "p3a", "pp4"), msg("assistant", "p4a", "p4", "r")];
  const P = await put("pa", pl, G);
  const C = await put("ca", [header("ca", "/x/pa.jsonl"), ...pl.slice(1), msg("user", "c5", "p4a", "cc5"), msg("assistant", "c5a", "c5", "r")], P);
  await drain(db, ["project"]);
  // P's own span [p4 p4a]: closed only on C's view. Its records and its <context> ([p3 p3a]) are P's: G is
  // only behind C's projection (C ← P ← G).
  const [p3] = await db`select id, context from span where source = ${P} and keys[1] like 'pa/p4:%'`;
  expect(p3).toBeTruthy();
  expect(JSON.stringify(p3.context)).not.toContain('"ga/');
  const { hooks, runSummarize } = await import("./derive.ts");
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    // G's first exchange is revised: G's version moves now; P and C are projected again only later.
    const rev = [...gl];
    rev[1] = msg("user", "u0", null, "t0 changed");
    await put("ga", rev);
  };
  const n = requests.length;
  try {
    await runSummarize(db, (await claimJobFor("summarize", p3.id))!);
  } finally {
    hooks.afterCheck = async () => {};
  }
  // The first attempt is refused at the send; the retry checks again, finds C's projection stale, stops.
  expect(requests.length).toBe(n);
});

test("a child published concurrently with the planner inserting its parent (commit after the insert): the parent is still merged", async () => {
  // 8 leaves: root R over [A B]; A written; B unwritten and its last child unwritten.
  const s = await put("lx", linear("lx", 8));
  await coldThenStructure();
  const [R] = await db`select id, kids from forest_node where size = 8`;
  const [A, B] = (R.kids as any[]).map((k) => k.node as string);
  const [, B2] = ((await db`select kids from forest_node where id = ${B}`)[0].kids as any[]).map((k) => k.node as string);
  for (const id of [...(await db`select id from forest_node where size = 2`).map((r: any) => r.id).filter((x: string) => x !== B2)]) await runMerge(db, (await claimJobFor("merge", id))!);
  await runMerge(db, (await claimJobFor("merge", A))!);
  await db`delete from forest_node where id = ${R.id}`; // R is inserted by the racing planner
  await db`delete from job where kind in ('merge', 'forest')`;
  await db`insert into forest_access values (${s})`;
  const { snapshotHook, runForest } = await import("./forest.ts");
  // The planner's snapshot sees B2 and B unwritten (so B is not queued by it, R is new). Then B2 and B are
  // merged; B's publish looks for parents (R not inserted yet) and pauses before its commit.
  let resume!: () => void;
  const paused = new Promise<void>((r) => (resume = r));
  let atPause!: () => void;
  const reachedPause = new Promise<void>((r) => (atPause = r));
  snapshotHook.beforeMergeCommit = async (node) => {
    if (node !== B) return;
    snapshotHook.beforeMergeCommit = async () => {};
    atPause();
    await paused;
  };
  let mergeB: Promise<unknown> | undefined;
  try {
    // The planner's snapshot happens while B is still unwritten (B's merge has not started).
    snapshotHook.afterSnapshot = async () => {
      snapshotHook.afterSnapshot = async () => {};
      await runMerge(db, (await claimJobFor("merge", B2))!);
      mergeB = runMerge(db, (await claimJobFor("merge", B))!);
      await reachedPause; // B has looked for parents (none: R not inserted yet), not committed
      setTimeout(resume, 300); // B commits later on its own (the planner may wait for B's job row meanwhile)
    };
    await db`insert into job (kind, key) values ('forest', ${s})`;
    await runForest(db, (await claimJobFor("forest", s))!);
    // 3. R's merge, if queued, runs now and cannot see B yet.
    const { runOne } = await import("./worker.ts");
    while (await runOne(db, ["merge"]));
  } finally {
    snapshotHook.afterSnapshot = async () => {};
    resume();
    await mergeB;
    snapshotHook.beforeMergeCommit = async () => {};
  }
  await drain(db);
  expect((await db`select text is not null w from forest_node where id = ${R.id}`)[0].w).toBe(true);
});

test("a span check refused only because its view's projection is momentarily stale: summaries and vectors recover on their own", async () => {
  const lines = linear("st", 4); // 4 closed, tail [u4 a4] open
  const s = await put("st", lines);
  await drain(db, ["project"]);
  // A revision in the open tail: the version moves; the closed spans' inputs do not change.
  const rev = [...lines];
  rev[rev.length - 2] = msg("user", "u4", "a3", "t4 changed");
  await put("st", rev);
  const [first] = await db`select id from span where source = ${s} and keys[1] like 'st/u0:%'`;
  const { runSummarize } = await import("./derive.ts");
  const n = requests.length;
  await runSummarize(db, (await claimJobFor("summarize", first.id))!);
  expect(requests.length).toBe(n);
  await drain(db);
  // The refused check came back as a later job, not as a finished one.
  const [later] = await db`select not_before > now() later from job where kind = 'summarize' and key = ${first.id} and state = 'queued'`;
  expect(later?.later).toBe(true);
  await db`update job set not_before = now() where state = 'queued'`;
  await drain(db);
  expect((await db`select count(*)::int n from span_summary where embedding is not null`)[0].n).toBe(4);
  expect((await readForest(db, s, await tipOf(s)))!.items.map((i) => i.size)).toEqual([4]);
});

test("an embed refused only because the projection is momentarily stale: the vector is made later", async () => {
  const lines = linear("se", 4);
  const s = await put("se", lines);
  // Summaries now; the embed jobs are held back.
  await drain(db, ["project", "summarize"]);
  await db`update job set not_before = now() + interval '1 hour' where kind = 'embed'`;
  const rev = [...lines];
  rev[rev.length - 2] = msg("user", "u4", "a3", "t4 changed");
  await put("se", rev);
  const [first] = await db`select id from span where source = ${s} and keys[1] like 'se/u0:%'`;
  const { runEmbed } = await import("./derive.ts");
  await runEmbed(db, (await claimJobFor("embed", first.id))!);
  expect((await db`select embedding is null e from span_summary where span = ${first.id}`)[0].e).toBe(true);
  await drain(db, ["project"]);
  await db`update job set not_before = now() where state = 'queued'`;
  await drain(db);
  expect((await db`select count(*)::int n from span_summary where embedding is not null`)[0].n).toBe(4);
});

test("a merge publish holding the barrier is bounded as a whole: forget gets through, nothing is published", async () => {
  const s = await put("ph", linear("ph", 4));
  await coldThenStructure();
  const [pair] = await db`select id from forest_node where size = 2 order by id limit 1`;
  const { hooks } = await import("./derive.ts");
  process.env.MEMEX_PUBLISH_HOLD_MS = "400";
  let inside!: () => void;
  const isInside = new Promise<void>((r) => (inside = r));
  hooks.inPublish = async (tx: any) => {
    hooks.inPublish = async () => {};
    inside();
    await tx`select pg_sleep(0.25)`; // each below the deadline, together above it
    await tx`select pg_sleep(0.25)`;
    await tx`select pg_sleep(0.25)`;
  };
  let run: Promise<unknown> | undefined;
  try {
    run = runMerge(db, (await claimJobFor("merge", pair.id))!).catch((e) => e);
    await isInside;
    const t0 = performance.now();
    await forgetSource(db, s);
    expect(performance.now() - t0).toBeLessThan(400 + 300);
  } finally {
    hooks.inPublish = async () => {};
    delete process.env.MEMEX_PUBLISH_HOLD_MS;
    await run;
  }
  expect((await db`select count(*)::int n from forest_node where text is not null`)[0].n).toBe(0);
  expect((await db`select 1 ok`)[0].ok).toBe(1);
});

// ---------- session cards ----------
const cardCalls = () => requests.filter((r) => r.body.messages?.[0].content.startsWith("You write the card"));
const cards = async () => (await db`select source, tip, input_fp, card from session_card order by tip`) as any[];
const { readCard } = await import("./forest.ts");
const { snapshot } = await import("./derive.ts");
const cardOf = (s: string, tip: string) =>
  snapshot(db, async (q) => {
    const [v] = await q`select spans, closed_end from view where source = ${s} and tip = ${tip}`;
    const { leavesOf } = await import("./forest.ts");
    return readCard(q, s, leavesOf(v), tip, () => true);
  });

test("card: made once from the complete view's roots and recent summaries; the same input is not sent again", async () => {
  const s = await put("ca", linear("ca", 5));
  await drain(db);
  const c = await cards();
  expect(c.length).toBe(1);
  expect(c[0].card.overview).toBe("part:S(t0) + S(t1) + S(t2) + S(t3) | part:S(t4) | recent:S(t2) | recent:S(t3) | recent:S(t4)");
  expect(c[0].card.cwd).toBe("/x");
  expect(cardCalls().length).toBe(1);
  const r = await cardOf(s, await tipOf(s));
  expect(r.degraded).toBe(false);
  // Something else triggers the forest again: the card is not made again.
  await db`insert into job (kind, key) values ('forest', ${s})`;
  await drain(db);
  expect(cardCalls().length).toBe(1);
});

test("card: an appended exchange makes a new card; until then the stored one is not shown for the new input", async () => {
  const s = await put("cb", linear("cb", 4));
  await drain(db);
  expect(cardCalls().length).toBe(1);
  await put("cb", linear("cb", 5));
  await drain(db, ["project", "summarize", "embed"]);
  // The view's tip moved: a different view; its card is degraded until made.
  const r = await cardOf(s, await tipOf(s));
  expect(r.degraded).toBe(true);
  expect(r.card.overview).toContain("S(t4)");
  await drain(db);
  expect(cardCalls().length).toBe(2);
  expect((await cardOf(s, await tipOf(s))).degraded).toBe(false);
});

test("card: a missing summary → degraded card from what is ready, no card job sent; filled in later", async () => {
  const s = await put("cc", linear("cc", 4));
  await drain(db, ["project"]);
  const [t1] = await db`select id from span where source = ${s} and keys[1] like 'cc/u1:%'`;
  await db`update job set not_before = now() + interval '1 hour' where kind = 'summarize' and key = ${t1.id}`;
  await drain(db);
  expect(cardCalls().length).toBe(0);
  const r = await cardOf(s, await tipOf(s));
  expect(r.degraded).toBe(true);
  expect(r.card.overview).toBe("S(t0)\nS(t2) + S(t3)");
  await db`update job set not_before = now() where key = ${t1.id}`;
  await drain(db);
  expect(cardCalls().length).toBe(1);
});

test("card: two branches get two cards, each from its own view only", async () => {
  const base = linear("cd", 6);
  const alt = [msg("user", "x4", "a3", "z4"), msg("assistant", "y4", "x4", "q"), msg("user", "x5", "y4", "z5"), msg("assistant", "y5", "x5", "q"),
               msg("user", "x6", "y5", "z6"), msg("assistant", "y6", "x6", "q")];
  await put("cd", [...base, ...alt]);
  await drain(db);
  const c = await cards();
  expect(c.length).toBe(2);
  const a = c.find((x) => x.tip === "a6")!, b = c.find((x) => x.tip === "y6")!;
  expect(a.card.overview).not.toContain("z4");
  expect(b.card.overview).not.toContain("t4");
  expect(b.card.overview).toContain("S(z4) + S(z5)");
});

test("card: a cold session gets no card call; the read is degraded", async () => {
  const old = T;
  T = "2020-01-01T00:00:00Z";
  try {
    const s = await put("ce", linear("ce", 4));
    await drain(db);
    expect(cardCalls().length).toBe(0);
    expect((await cardOf(s, await tipOf(s))).degraded).toBe(true);
  } finally {
    T = old;
  }
});

test("card: forget during the card call → not published; a revision during it → not published", async () => {
  for (const [name, change] of [
    ["cf", async (s: string) => void (await forgetSource(db, s))],
    ["cg", async () => {
      const rev = linear("cg", 4, 100, "g");
      rev[3] = msg("user", "u1", "a0", "t1 changed");
      await put("cg", rev);
    }],
  ] as const) {
    // Distinct content: the first case's forget leaves content tombs that would also drop identical records.
    const s = await put(name, name === "cg" ? linear(name, 4, 100, "g") : linear(name, 4));
    await drain(db);
    await db`delete from session_card where source = ${s}`; // as if not made yet
    let release!: () => void;
    hold = new Promise((r) => (release = r));
    const { runCard } = await import("./forest.ts");
    const n = cardCalls().length;
    const run = runCard(db, (await claimJobFor("card", JSON.stringify([s, await tipOf(s)])))!);
    let done = false;
    run.then(() => (done = true));
    while (cardCalls().length === n && !done) await Bun.sleep(5);
    expect(cardCalls().length).toBe(n + 1);
    await change(s);
    release();
    await run;
    hold = null;
    expect((await db`select 1 from session_card where source = ${s}`).length).toBe(0);
  }
});

test("card: a parent's forget removes the child's card made from the parent's text", async () => {
  const pl = linear("cp2", 3);
  const P = await put("cp2", pl);
  const C = await put("cq2", [header("cq2", "/x/cp2.jsonl"), ...pl.slice(1, 7), msg("user", "k3", "a2", "c3"), msg("assistant", "k3a", "k3", "r"), msg("user", "k4", "k3a", "c4")], P);
  await drain(db);
  expect((await db`select card from session_card where source = ${C}`)[0].card.overview).toContain("S(t0)");
  await forgetSource(db, P);
  expect((await db`select count(*)::int n from session_card where card::text like '%S(t0)%'`)[0].n).toBe(0);
});

test("card: a stored card whose input no longer matches the view (a leaf summary redone) is not shown", async () => {
  const s = await put("ch", linear("ch", 4));
  await drain(db);
  const tip = await tipOf(s);
  expect((await cardOf(s, tip)).degraded).toBe(false);
  // Same view, a leaf summary replaced: the forest shows other text; the old card is not its card.
  const [sp] = await db`select id from span where source = ${s} and keys[1] like 'ch/u3:%'`;
  await db`update span_summary set text = 'S(other)', text_hash = 'h-x' where span = ${sp.id}`;
  const r = await cardOf(s, tip);
  expect(r.degraded).toBe(true);
  expect(r.card.overview).toContain("S(other)");
});

test("card: an incomplete view is not sent to the model even if its card job runs", async () => {
  const s = await put("ci", linear("ci", 4));
  await drain(db, ["project"]);
  const [t1] = await db`select id from span where source = ${s} and keys[1] like 'ci/u1:%'`;
  await db`update job set not_before = now() + interval '1 hour' where kind = 'summarize' and key = ${t1.id}`;
  await drain(db);
  const { runCard } = await import("./forest.ts");
  await runCard(db, (await claimJobFor("card", JSON.stringify([s, await tipOf(s)])))!);
  expect(cardCalls().length).toBe(0);
});

test("card: a cold session's card job is not queued by the forest", async () => {
  const old = T;
  T = "2020-01-01T00:00:00Z";
  try {
    await put("cj", linear("cj", 3)); // short: no nodes, so "complete" without any merge
    await drain(db);
    // A read asks for the merges (so the session counts as read); the merges, then the card follow.
    expect((await db`select count(*)::int n from job where kind = 'card'`)[0].n).toBe(0);
  } finally {
    T = old;
  }
});

test("card: a revoked destination between the check and the send → nothing sent", async () => {
  const s = await put("ck", linear("ck", 4));
  await drain(db);
  await db`delete from session_card`;
  const { hooks } = await import("./derive.ts");
  hooks.afterCheck = async () => {
    hooks.afterCheck = async () => {};
    await revokeGrant(db, "personal", "derive", "llm");
  };
  const { runCard } = await import("./forest.ts");
  try {
    await runCard(db, (await claimJobFor("card", JSON.stringify([s, await tipOf(s)])))!);
  } finally {
    hooks.afterCheck = async () => {};
  }
  expect(cardCalls().length).toBe(1); // only the first card, before the delete
});

test("card: a written card's input text withdrawn after the check (a leaf's record forgotten) → no card for it", async () => {
  // Inputs checked down to their records: a canon tomb (forget in progress) refuses the send.
  const s = await put("cl", linear("cl", 4));
  await drain(db);
  await db`delete from session_card`;
  const [sp] = await db`select canons from span where source = ${s} and keys[1] like 'cl/u0:%'`;
  await db`insert into tomb values (${"canon:" + sp.canons[0]})`;
  const n = cardCalls().length;
  const { runCard } = await import("./forest.ts");
  await runCard(db, (await claimJobFor("card", JSON.stringify([s, await tipOf(s)])))!);
  expect(cardCalls().length).toBe(n);
});
