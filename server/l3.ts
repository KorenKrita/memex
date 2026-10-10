// L3 explicit statements (DESIGN §4.5): what the user said, stored as immutable statements with relations
// between them; "in effect" is computed at a time `as_of`, never stored as a state.
//
// - A statement is eligible at t when it had been said (stated_at ≤ t) and its effective time had come
//   ((effective_from ?? stated_at) ≤ t). Its effective time is its own: a retroactive "last week already B"
//   is not in effect before it was said.
// - Relations: replaces(a, b), retracts(a, b) (a newer statement withdraws b), contradicts, unrelated. The
//   model judges the kind; the direction comes from effective times here (orient), equal times → contradicts.
// - A statement suppresses another (replaces / retracts) only while it has power: eligible, readable, and not
//   itself retracted by a statement with power. Being replaced does not take power away, so A → B → C does not
//   bring A back; a retraction of B does (B no longer stands, so what B replaced stands again).
// - Relations only count within one scope (global, or one repository); unreadable statements neither show nor
//   suppress; pairs not judged yet suppress nothing (both show).

export type Stmt = { id: string; scope: string; stated_at: number; effective_from: number | null; retraction?: boolean };
export type RelKind = "replaces" | "retracts" | "contradicts" | "unrelated";
export type Rel = { a: string; b: string; kind: RelKind };

const effective = (s: Stmt) => s.effective_from ?? s.stated_at;
/** The first time `s` is eligible. */
const eligibleFrom = (s: Stmt) => Math.max(s.stated_at, effective(s));

/** A judged relation in its fixed form: replaces/retracts point from the later-effective statement to the
 *  earlier one (no order between them → contradicts); symmetric kinds keep the ids in sorted order. */
export function orient(x: Stmt, y: Stmt, kind: RelKind): Rel {
  if (kind === "replaces" || kind === "retracts") {
    const ex = effective(x), ey = effective(y);
    if (ex !== ey) return ex > ey ? { a: x.id, b: y.id, kind } : { a: y.id, b: x.id, kind };
    kind = "contradicts";
  }
  return x.id < y.id ? { a: x.id, b: y.id, kind } : { a: y.id, b: x.id, kind };
}

/** The statements in effect at `t`. */
export function viewAt(stmts: Stmt[], rels: Rel[], t: number, readable: (s: Stmt) => boolean = () => true): Stmt[] {
  const byId = new Map(stmts.map((s) => [s.id, s]));
  const against = new Map<string, Rel[]>(); // b → relations that would suppress b
  for (const r of rels) {
    if (r.kind !== "replaces" && r.kind !== "retracts") continue;
    const a = byId.get(r.a), b = byId.get(r.b);
    if (!a || !b || a.scope !== b.scope) continue;
    against.set(r.b, [...(against.get(r.b) ?? []), r]);
  }
  const memo = new Map<string, boolean>();
  // A cycle of retractions (only possible with hand-made relations) gives its members no power.
  const power = (id: string, seen = new Set<string>()): boolean => {
    if (memo.has(id)) return memo.get(id)!;
    const s = byId.get(id)!;
    if (seen.has(id)) return false;
    seen.add(id);
    const p = eligibleFrom(s) <= t && readable(s) && !(against.get(id) ?? []).some((r) => r.kind === "retracts" && power(r.a, seen));
    seen.delete(id);
    memo.set(id, p);
    return p;
  };
  return stmts.filter((s) => !s.retraction && power(s.id) && !(against.get(s.id) ?? []).some((r) => power(r.a)));
}

/** The next time after `now` the view can change by time alone (a statement becoming eligible), or null. */
export function nextBoundary(stmts: Stmt[], now: number): number | null {
  let next: number | null = null;
  for (const s of stmts) {
    const e = eligibleFrom(s);
    if (e > now && (next === null || e < next)) next = e;
  }
  return next;
}

// ---------- storage, extraction, pair judgment (jobs `l3`, `l3pair`) ----------
//
// - Extraction reads the span's raw records (not its L1 summary): only the user's own records can be cited;
//   assistant records and <context> help understanding and never become statements. One extraction per
//   canonical user record and content version (`l3_seen`, claimed when publishing): a fork copy of the same
//   record, or a record seen from two spans, makes no second set.
// - A statement's id binds its evidence (canonical record, record hash) and its content; it is never changed.
//   stated_at is the record's own timestamp; effective_from only with an explicit time phrase from the record.
//   Semantic scope: global, or repo:<the source's repo identity> — a repo statement without one is dropped,
//   never made global. Authorization scope (the source's) is kept apart.
// - Pairs: every pair within one semantic scope is judged (the model also says whether both are about the
//   same thing); planning runs under that scope's lock, so two statements published at once still get their
//   pair. Unjudged pairs suppress nothing (viewAt).
// - Readable = the canonical record still holds that content, nothing of it is tombed, the scope is granted
//   for the reader's purpose; forget removes statements, relations and extraction marks of the source.
import { type Db, type Job, type Tx, addMigration, enqueue, finishJob } from "./db.ts";
import { checkSpan, env, guardedPost, parseJson, publish, skip, snapshot } from "./derive.ts";
import { canonOf } from "./project.ts";

addMigration(`
create table if not exists l3_stmt (id text primary key, src text not null, rec text not null, rec_hash text not null, text text not null,
                                    scope text not null, auth_scope text not null, stated_at timestamptz not null, effective_from timestamptz,
                                    retraction bool not null, span text not null, model text not null, prompt_v text not null,
                                    at timestamptz not null default now());
create index if not exists l3_stmt_scope on l3_stmt (scope);
create index if not exists l3_stmt_src on l3_stmt (src);
create table if not exists l3_seen (src text not null, rec text not null, rec_hash text not null, primary key (src, rec, rec_hash));
create table if not exists l3_rel (key text primary key, a text not null, b text not null, kind text not null, model text not null,
                                   prompt_v text not null, at timestamptz not null default now());
`);

export const EXTRACT_VERSION = "l3-extract-v1";
export const PAIR_VERSION = "l3-pair-v1";
const STMT_MAX = 300; // UTF-8 bytes per statement [待标定]
const STMTS_MAX = 8; // per span
const MAX_TRIES = 3;
const tzOffset = () => process.env.MEMEX_TZ_OFFSET ?? "+08:00"; // the user's local time zone (DESIGN §4.5)
export const l3Hooks = { afterPlanRead: async () => {} };

const sha = (s: string) => new Bun.CryptoHasher("sha256").update(s).digest("hex");
const bytes = (t: string) => Buffer.byteLength(t);
const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
export const pairKey = (x: string, y: string) => (x < y ? `${x}|${y}` : `${y}|${x}`);
/** Epoch ms → local ISO time with the configured offset. */
function local(ms: number) {
  const m = /^([+-])(\d\d):(\d\d)$/.exec(tzOffset());
  const off = m ? (m[1] === "-" ? -1 : 1) * (Number(m[2]) * 60 + Number(m[3])) : 0;
  return new Date(ms + off * 60_000).toISOString().slice(0, 19) + (m ? tzOffset() : "Z");
}

export const EXTRACT_SYSTEM =
  "You extract what the user explicitly stated as a lasting preference, convention or decision, from one piece of a coding session. " +
  "Only records with citable=\"yes\" (the user's own words) can be cited. Never turn assistant suggestions, plans or questions into the user's statements; " +
  "<context> and other records only help you understand. Skip one-off task instructions that say nothing about later work. " +
  "Each statement: record_id (the citable record), text (one self-contained sentence in the user's language, at most 300 bytes), " +
  "scope (\"global\" for the user in general, \"repo\" when it is about the current repository), " +
  "retraction (true when the user withdraws something said earlier, text naming what), " +
  "effective_from and effective_phrase: only when the record states when it takes effect (\"from next Monday\", \"since last week\"): " +
  "effective_phrase copied verbatim from the record, effective_from an ISO time with offset resolved against the record's stated_at; otherwise both null. " +
  "Return JSON {\"statements\": [...]}; an empty list when nothing qualifies.";

export const PAIR_SYSTEM =
  "You compare two statements a user made. same_object: whether both are about the same thing (the same setting, tool, habit or rule). " +
  "relation: \"replaces\" (one changes or overrides the other), \"retracts\" (one withdraws the other), \"contradicts\" (they conflict and " +
  "neither clearly overrides), or \"unrelated\". Do not decide which is newer; only the kind. Return JSON {\"same_object\": bool, \"relation\": \"...\"}.";

type Cand = { id: string; src: string; rec: string; hash: string; stated_at: number; repo: string | null; auth: string; digest: string };
type NewStmt = { id: string; cand: Cand; text: string; scope: string; effective_from: number | null; retraction: boolean };

/** The span's user records with their canonical evidence, in the caller's snapshot; `citable` = not extracted yet. */
async function candidates(q: Tx, harness: string, records: any[]) {
  const out: (Cand & { citable: boolean })[] = [];
  for (const r of records.filter((r) => r.kind === "user")) {
    const canon = canonOf(harness, r.key);
    const i = canon.indexOf(":");
    const src = canon.slice(0, i), rec = canon.slice(i + 1);
    const [row] = await q`select r.hash, b.raw, s.scope, s.meta->>'repo' repo from record r join body b on b.source = r.source and b.rec = r.rec and b.hash = r.hash
                          join source s on s.id = r.source where r.source = ${src} and r.rec = ${rec} and r.state = 'ok'`;
    if (!row) continue;
    const ts = Date.parse(JSON.parse(row.raw).timestamp ?? "");
    if (!Number.isFinite(ts)) continue; // no time it was said: nothing it could be in effect from
    const seen = (await q`select 1 from l3_seen where src = ${src} and rec = ${rec} and rec_hash = ${row.hash}`).length > 0;
    out.push({ id: r.id, src, rec, hash: row.hash, stated_at: ts, repo: row.repo ?? null, auth: row.scope, digest: r.digest, citable: !seen });
  }
  return out;
}

function renderExtract(span: { records: any[]; context: any[] }, cands: Map<string, Cand & { citable: boolean }>) {
  const rec = (r: any) => {
    const c = cands.get(r.id);
    const attrs = c ? ` stated_at="${local(c.stated_at)}" citable="${c.citable ? "yes" : "no"}"` : "";
    return `<record id="${esc(r.id)}" kind="${r.kind}"${attrs}>\n${esc(r.digest)}\n</record>`;
  };
  return [...(span.context.length ? [`<context>\n${span.context.map(rec).join("\n")}\n</context>`] : []), `<input>\n${span.records.map(rec).join("\n")}\n</input>`].join("\n");
}

/** Problems with an extraction answer, and the statements it makes (repo statements without a repo dropped). */
export function checkExtract(s: any, cands: Map<string, Cand & { citable: boolean }>, model: string): { problems: string[]; stmts: NewStmt[] } {
  const problems: string[] = [];
  if (!s || typeof s !== "object" || Array.isArray(s) || !Array.isArray(s.statements)) return { problems: ["not a JSON object with a statements list"], stmts: [] };
  if (s.statements.length > STMTS_MAX) problems.push(`more than ${STMTS_MAX} statements`);
  const stmts: NewStmt[] = [];
  for (const x of s.statements) {
    const c = cands.get(x?.record_id);
    if (!c || !c.citable) {
      problems.push(`cites ${JSON.stringify(x?.record_id)}, not a citable user record of <input>`);
      continue;
    }
    if (typeof x.text !== "string" || !x.text.trim() || bytes(x.text) > STMT_MAX) problems.push(`text not a non-empty string of at most ${STMT_MAX} bytes`);
    if (x.scope !== "global" && x.scope !== "repo") problems.push(`scope ${JSON.stringify(x.scope)} not global or repo`);
    if (typeof x.retraction !== "boolean") problems.push("retraction not a boolean");
    let eff: number | null = null;
    if ((x.effective_from ?? null) === null) {
      if ((x.effective_phrase ?? null) !== null) problems.push("effective_phrase without effective_from");
    } else if (typeof x.effective_phrase !== "string" || !x.effective_phrase || !c.digest.includes(x.effective_phrase))
      problems.push("effective_from without an effective_phrase copied from the record");
    else if (typeof x.effective_from !== "string" || !/(Z|[+-]\d\d:\d\d)$/.test(x.effective_from) || !Number.isFinite(Date.parse(x.effective_from)))
      problems.push("effective_from not an ISO time with an offset");
    else eff = Date.parse(x.effective_from);
    if (problems.length) continue;
    if (x.scope === "repo" && !c.repo) continue; // about a repository we cannot name: not made global
    const scope = x.scope === "global" ? "global" : "repo:" + c.repo;
    const text = x.text.trim();
    const id = "st_" + sha([EXTRACT_VERSION, model, c.src, c.rec, c.hash, text, scope, eff ?? "", x.retraction].join("\u0000")).slice(0, 32);
    stmts.push({ id, cand: c, text, scope, effective_from: eff, retraction: x.retraction });
  }
  return { problems, stmts };
}

/** Job `l3` for one span. */
export async function runL3(db: Db, job: Job) {
  const url = env("MEMEX_LLM_URL").replace(/\/+$/, "") + "/chat/completions";
  const model = env("MEMEX_LLM_MODEL");
  const key = env("MEMEX_LLM_KEY");
  let problems: string[] = [];
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const r = await checkSpan(q, job.key, "llm");
      if (!r.ok) return r;
      const cands = await candidates(q, (r.span as any).harness, r.span.records);
      return { ...r, cands: new Map(cands.map((x) => [x.id, x])) };
    });
    if (!c.ok) return skip(db, job, c.reason, c.scopes);
    if (![...c.cands.values()].some((x) => x.citable)) return db.begin((tx: Tx) => finishJob(tx, job)); // nothing new to read
    const messages = [{ role: "system", content: EXTRACT_SYSTEM }, { role: "user", content: renderExtract(c.span, c.cands) }];
    if (problems.length) messages.push({ role: "user", content: "Your previous answer was invalid: " + problems.slice(0, 4).join("; ") + ". Return a corrected JSON object." });
    const r = await guardedPost(db, c.deps, url, key, { model, messages, temperature: 0, reasoning_effort: "none", response_format: { type: "json_object" } });
    if (r === "stale") continue;
    if (r.status !== 200) throw new Error(`llm http ${r.status}`);
    let s: any;
    try {
      s = parseJson(r.json?.choices?.[0]?.message?.content ?? "");
    } catch {
      problems = ["not JSON"];
      continue;
    }
    const chk = checkExtract(s, c.cands, model);
    problems = chk.problems;
    if (problems.length) continue;
    return publish(db, job, c.deps, async (tx) => {
      // Claim each record's extraction; one claimed meanwhile (a fork copy, another span) keeps the first set.
      const mine = new Set<string>();
      for (const x of c.cands.values())
        if (x.citable && (await tx`insert into l3_seen values (${x.src}, ${x.rec}, ${x.hash}) on conflict do nothing returning 1`).length) mine.add(x.id);
      const stmts = chk.stmts.filter((x) => mine.has(x.cand.id));
      await planAndInsert(tx, stmts, job.key, model);
    });
  }
  throw new Error(problems.length ? `extraction invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

/** Insert new statements and queue every pair they make in their scopes, under those scopes' locks. */
async function planAndInsert(tx: Tx, stmts: NewStmt[], span: string, model: string) {
  const scopes = [...new Set(stmts.map((x) => x.scope))].sort();
  for (const s of scopes) await tx`select pg_advisory_xact_lock(hashtextextended(${"l3:" + s}, 0))`;
  for (const x of stmts)
    await tx`insert into l3_stmt (id, src, rec, rec_hash, text, scope, auth_scope, stated_at, effective_from, retraction, span, model, prompt_v)
             values (${x.id}, ${x.cand.src}, ${x.cand.rec}, ${x.cand.hash}, ${x.text}, ${x.scope}, ${x.cand.auth}, ${new Date(x.cand.stated_at)},
                     ${x.effective_from === null ? null : new Date(x.effective_from)}, ${x.retraction}, ${span}, ${model}, ${EXTRACT_VERSION})
             on conflict do nothing`;
  for (const s of scopes) {
    const all = (await tx`select id from l3_stmt where scope = ${s}`).map((r: any) => r.id as string);
    await l3Hooks.afterPlanRead();
    const fresh = new Set(stmts.filter((x) => x.scope === s).map((x) => x.id));
    for (const a of fresh) for (const b of all) if (b !== a && !(fresh.has(b) && b < a)) await enqueue(tx, "l3pair", pairKey(a, b));
  }
}

/** Why statement `s` cannot be read for (purpose, dest) now, or null. */
async function unreadable(q: Tx, s: any, purpose: string, dest: string): Promise<string | null> {
  if ((await q`select 1 from tomb where key = any(${q.array(["source:" + s.src, `canon:${s.src}:${s.rec}`], "text")}) limit 1`).length) return "forgotten";
  if (!(await q`select 1 from record where source = ${s.src} and rec = ${s.rec} and state = 'ok' and hash = ${s.rec_hash}`).length) return "record-changed";
  if (!(await q`select 1 from grant_rule where scope = ${s.auth_scope} and purpose = ${purpose} and dest = ${dest}`).length) return `not-granted:${dest}`;
  return null;
}

const asStmt = (r: any): Stmt => ({ id: r.id, scope: r.scope, stated_at: +new Date(r.stated_at), effective_from: r.effective_from ? +new Date(r.effective_from) : null, retraction: r.retraction });

/** Job `l3pair` for one pair key. */
export async function runL3Pair(db: Db, job: Job) {
  const url = env("MEMEX_LLM_URL").replace(/\/+$/, "") + "/chat/completions";
  const model = env("MEMEX_LLM_MODEL");
  const key = env("MEMEX_LLM_KEY");
  const [x, y] = job.key.split("|");
  let problems: string[] = [];
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const rows = await q`select * from l3_stmt where id = any(${q.array([x, y], "text")}) order by id`;
      if (rows.length !== 2) return { ok: false as const, gone: true };
      if ((await q`select 1 from l3_rel where key = ${job.key} and model = ${model} and prompt_v = ${PAIR_VERSION}`).length) return { ok: false as const, gone: true };
      for (const r of rows) {
        const why = await unreadable(q, r, "derive", "llm");
        if (why) return { ok: false as const, reason: why, scopes: [...new Set(rows.map((r: any) => r.auth_scope as string))] };
      }
      const keys: string[] = [...new Set<string>(rows.map((r: any) => "s:" + r.src)), "d:llm"];
      const vs = new Map<string, number>((await q`select key, v from vis_version where key = any(${q.array(keys, "text")})`).map((r: any) => [r.key, Number(r.v)]));
      return { ok: true as const, rows, deps: Object.fromEntries(keys.map((k) => [k, vs.get(k) ?? 1])) };
    });
    if (!c.ok) {
      if ("gone" in c) return db.begin((tx: Tx) => finishJob(tx, job)); // forgotten, or judged already
      return skip(db, job, c.reason!, c.scopes as string[]);
    }
    const show = (r: any, n: number) =>
      `<statement n="${n}" stated_at="${local(+new Date(r.stated_at))}"${r.effective_from ? ` effective_from="${local(+new Date(r.effective_from))}"` : ""}${r.retraction ? ' retraction="yes"' : ""}>\n${esc(r.text)}\n</statement>`;
    const messages = [{ role: "system", content: PAIR_SYSTEM }, { role: "user", content: c.rows.map((r: any, n: number) => show(r, n + 1)).join("\n") }];
    if (problems.length) messages.push({ role: "user", content: "Your previous answer was invalid: " + problems.join("; ") + ". Return a corrected JSON object." });
    const r = await guardedPost(db, c.deps, url, key, { model, messages, temperature: 0, reasoning_effort: "none", response_format: { type: "json_object" } });
    if (r === "stale") continue;
    if (r.status !== 200) throw new Error(`llm http ${r.status}`);
    let s: any;
    try {
      s = parseJson(r.json?.choices?.[0]?.message?.content ?? "");
    } catch {
      problems = ["not JSON"];
      continue;
    }
    const kinds: RelKind[] = ["replaces", "retracts", "contradicts", "unrelated"];
    problems = [];
    if (typeof s?.same_object !== "boolean") problems.push("same_object not a boolean");
    if (!kinds.includes(s?.relation)) problems.push("relation not one of " + kinds.join(", "));
    if (problems.length) continue;
    const [p, q2] = c.rows.map(asStmt);
    let kind: RelKind = s.same_object ? s.relation : "unrelated";
    let rel = orient(p, q2, kind);
    // The kind of suppression follows the later statement: a retraction retracts, anything else replaces.
    if (rel.kind === "replaces" || rel.kind === "retracts") {
      const later = rel.a === p.id ? p : q2;
      rel = { ...rel, kind: later.retraction ? "retracts" : "replaces" };
    }
    return publish(db, job, c.deps, async (tx) => {
      if ((await tx`select count(*)::int n from l3_stmt where id = any(${tx.array([x, y], "text")})`)[0].n !== 2) return;
      await tx`insert into l3_rel (key, a, b, kind, model, prompt_v) values (${job.key}, ${rel.a}, ${rel.b}, ${rel.kind}, ${model}, ${PAIR_VERSION})
               on conflict (key) do update set a = excluded.a, b = excluded.b, kind = excluded.kind, model = excluded.model, prompt_v = excluded.prompt_v, at = now()`;
    });
  }
  throw new Error(problems.length ? `pair judgment invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

/** The statements in effect at `t` for a reader (purpose, dest), with the relations among readable ones. */
export async function l3View(q: Tx, t: number, purpose: string, dest: string) {
  const rows = await q`select * from l3_stmt order by id`;
  const ok = new Set<string>();
  for (const r of rows) if (!(await unreadable(q, r, purpose, dest))) ok.add(r.id);
  const rels = (await q`select a, b, kind from l3_rel`) as Rel[];
  const stmts = rows.map(asStmt);
  const byId = new Map(rows.map((r: any) => [r.id, r]));
  return { view: viewAt(stmts, rels, t, (s: Stmt) => ok.has(s.id)).map((s) => byId.get(s.id)), next: nextBoundary(stmts.filter((s: Stmt) => ok.has(s.id)), t) };
}
