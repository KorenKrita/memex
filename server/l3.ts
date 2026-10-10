// L3 explicit statements (DESIGN §4.5): what the user said, stored as immutable statements with relations
// between them; "in effect" is computed at a time `as_of`, never stored as a state.
//
// - A statement is eligible at t when it had been said (stated_at ≤ t) and its effective time had come
//   ((effective_from ?? stated_at) ≤ t). Its effective time is its own: a retroactive "last week already B"
//   is not in effect before it was said.
// - Relations: replaces(a, b), retracts(a, b) (a newer statement withdraws b), contradicts, unrelated. The
//   model judges the kind; the direction comes from code (orient): a replacement by effective times (equal →
//   contradicts), a withdrawal from the retraction to what it withdraws, said before it took effect or after.
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

/** A judged relation in its fixed form. A withdrawal keeps who withdraws whom: the statement marked as a
 *  retraction (or, with neither or both marked, the one said later) retracts the other, provided it was said
 *  after it — it may withdraw a decision not yet in effect, so its direction is not taken from effective
 *  times. A replacement points from the later-effective statement to the earlier one. No order → contradicts;
 *  symmetric kinds keep the ids in sorted order. */
export function orient(x: Stmt, y: Stmt, kind: RelKind): Rel {
  if (kind === "replaces" || kind === "retracts") {
    if (!!x.retraction !== !!y.retraction || kind === "retracts") {
      const [r, t] = !!x.retraction !== !!y.retraction ? (x.retraction ? [x, y] : [y, x]) : x.stated_at > y.stated_at ? [x, y] : [y, x];
      if (r.stated_at > t.stated_at) return { a: r.id, b: t.id, kind: "retracts" };
    } else {
      const ex = effective(x), ey = effective(y);
      if (ex !== ey) return ex > ey ? { a: x.id, b: y.id, kind } : { a: y.id, b: x.id, kind };
    }
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
import { checkSpan, env, guardedPost, parseJson, publish, sameDeps, skip, snapshot, unchanged } from "./derive.ts";
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
-- What a statement was made from: the whole extraction input (its span at that input, every source's version,
-- every read scope), not only the record it cites.
alter table l3_stmt add column if not exists deps jsonb not null default '{}';
alter table l3_stmt add column if not exists input_fp text not null default '';
alter table l3_stmt add column if not exists scopes text[] not null default '{}';
alter table l3_seen add column if not exists span text not null default '';
alter table l3_seen add column if not exists input_fp text not null default '';
-- Statements and claims stored before generation data existed cannot be shown to hold: they go (their text is
-- never stamped with today's provenance), and their spans are read again by the backfill below.
delete from l3_rel r using l3_stmt s where s.input_fp = '' and (r.a = s.id or r.b = s.id);
delete from l3_stmt where input_fp = '';
delete from l3_seen where span = '';
-- Spans closed before L3 existed (or whose extraction was lost): queue them, once (idempotent on every start).
insert into job (kind, key)
  select 'l3', s.id from span s
  where exists (select 1 from jsonb_array_elements(s.records) x join unnest(s.keys, s.canons) k(key, canon) on k.key = x->>'key'
                where x->>'kind' = 'user' and not exists (select 1 from l3_seen l where l.src || ':' || l.rec = k.canon))
    and exists (select 1 from view v where v.span_ids @> array[s.id])
on conflict (kind, key) where state in ('queued', 'running') do nothing;
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
    const seen = (await q`select 1 from l3_seen l join span sp on sp.id = l.span and sp.input_fp = l.input_fp
                          where l.src = ${src} and l.rec = ${rec} and l.rec_hash = ${row.hash}`).length > 0;
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
      problems.push("a statement cites something that is not a citable user record of <input>"); // never echo model output: it may carry input text
      continue;
    }
    if (typeof x.text !== "string" || !x.text.trim() || bytes(x.text) > STMT_MAX) problems.push(`text not a non-empty string of at most ${STMT_MAX} bytes`);
    if (x.scope !== "global" && x.scope !== "repo") problems.push("scope not global or repo");
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
  let about: Record<string, number> | null = null;
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const r = await checkSpan(q, job.key, "llm");
      if (!r.ok) return r;
      const cands = await candidates(q, (r.span as any).harness, r.span.records);
      return { ...r, cands: new Map(cands.map((x) => [x.id, x])) };
    });
    if (c.ok && !sameDeps(about, c.deps)) problems = [];
    if (c.ok) about = c.deps;
    if (!c.ok) return skip(db, job, c.reason, c.scopes);
    // Nothing new to read: the statements already made from this very input only have their conservative vector
    // renewed (an unrelated revision elsewhere moved a source's version), no model call, their text unchanged.
    if (![...c.cands.values()].some((x) => x.citable)) return publish(db, job, c.deps, (tx) => renew(tx, job.key, c.span.input_fp, c.deps, c.scopes));
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
      // Claim each record's extraction; one claimed meanwhile (a fork copy, another span) whose input still holds
      // keeps the first set. A claim whose input moved (its span re-cut, its context revised) is taken over: its
      // statements are unreadable for good, and go with their relations (their text is never rewritten).
      const mine = new Set<string>();
      for (const x of c.cands.values()) {
        if (!x.citable) continue;
        const won = await tx`insert into l3_seen (src, rec, rec_hash, span, input_fp) values (${x.src}, ${x.rec}, ${x.hash}, ${job.key}, ${c.span.input_fp})
                             on conflict (src, rec, rec_hash) do update set span = excluded.span, input_fp = excluded.input_fp
                             where not exists (select 1 from span sp where sp.id = l3_seen.span and sp.input_fp = l3_seen.input_fp) returning 1`;
        if (!won.length) continue;
        mine.add(x.id);
        await dropStale(tx, x.src, x.rec);
      }
      const stmts = chk.stmts.filter((x) => mine.has(x.cand.id));
      await renew(tx, job.key, c.span.input_fp, c.deps, c.scopes);
      await planAndInsert(tx, stmts, job.key, c.span.input_fp, c.deps, c.scopes, model);
    });
  }
  throw new Error(problems.length ? `extraction invalid after ${MAX_TRIES} tries` : "inputs kept changing");
}

/** Insert new statements and queue every pair they make in their scopes, under those scopes' locks. */
async function planAndInsert(tx: Tx, stmts: NewStmt[], span: string, inputFp: string, deps: Record<string, number>, scopes0: string[], model: string) {
  const scopes = [...new Set(stmts.map((x) => x.scope))].sort();
  for (const s of scopes) await tx`select pg_advisory_xact_lock(hashtextextended(${"l3:" + s}, 0))`;
  for (const x of stmts)
    await tx`insert into l3_stmt (id, src, rec, rec_hash, text, scope, auth_scope, stated_at, effective_from, retraction, span, model, prompt_v, deps, input_fp, scopes)
             values (${x.id}, ${x.cand.src}, ${x.cand.rec}, ${x.cand.hash}, ${x.text}, ${x.scope}, ${x.cand.auth}, ${new Date(x.cand.stated_at)},
                     ${x.effective_from === null ? null : new Date(x.effective_from)}, ${x.retraction}, ${span}, ${model}, ${EXTRACT_VERSION},
                     ${deps}, ${inputFp}, ${tx.array([...new Set([...scopes0, x.cand.auth])].sort(), "text")})
             on conflict do nothing`;
  for (const s of scopes) {
    const all = (await tx`select id from l3_stmt where scope = ${s}`).map((r: any) => r.id as string);
    await l3Hooks.afterPlanRead();
    const fresh = new Set(stmts.filter((x) => x.scope === s).map((x) => x.id));
    for (const a of fresh) for (const b of all) if (b !== a && !(fresh.has(b) && b < a)) await enqueue(tx, "l3pair", pairKey(a, b));
  }
}

/** Statements made from exactly this input (span and fingerprint) get the vector it was just checked under. */
const renew = (tx: Tx, span: string, inputFp: string, deps: Record<string, number>, scopes: string[]) =>
  tx`update l3_stmt set deps = ${deps}, scopes = ${tx.array([...scopes].sort(), "text")} where span = ${span} and input_fp = ${inputFp}`;

/** A claim's statements once its input moved: unreadable for good, removed with their relations. */
async function dropStale(tx: Tx, src: string, rec: string) {
  const gone = (await tx`delete from l3_stmt s where s.src = ${src} and s.rec = ${rec}
                         and not exists (select 1 from span sp where sp.id = s.span and sp.input_fp = s.input_fp) returning id`).map((r: any) => r.id as string);
  if (gone.length) await tx`delete from l3_rel where a = any(${tx.array(gone, "text")}) or b = any(${tx.array(gone, "text")})`;
}

/** Why statement `s` cannot be read for (purpose, dest) now, or null. Readable only while what it was made
 *  from holds as a whole (DESIGN §5.5): its span at the same input (a revised record or <context> changes the
 *  input), that span's projection current, the cited record unchanged, nothing of its sources tombed, every
 *  source's version as made (a forget or revision anywhere in the input moves one), every read scope granted. */
async function unreadable(q: Tx, s: any, purpose: string, dest: string): Promise<string | null> {
  if ((await q`select 1 from tomb where key = any(${q.array(["source:" + s.src, `canon:${s.src}:${s.rec}`], "text")}) limit 1`).length) return "forgotten";
  if (!(await q`select 1 from record where source = ${s.src} and rec = ${s.rec} and state = 'ok' and hash = ${s.rec_hash}`).length) return "record-changed";
  const [sp] = await q`select sp.source, p.deps from span sp left join projected p on p.source = sp.source and p.status = 'ok'
                       where sp.id = ${s.span} and sp.input_fp = ${s.input_fp}`;
  if (!sp) return "input-changed"; // for good: that input is no more
  // Possibly for a while only: a projection not caught up, or a version moved elsewhere (renewed if the input holds).
  if (!sp.deps || !(await unchanged(q, sp.deps)) || !(await unchanged(q, sDeps(s.deps)))) return "stale";
  const scopes: string[] = s.scopes?.length ? s.scopes : [s.auth_scope];
  if ((await q`select count(*)::int n from grant_rule where scope = any(${q.array(scopes, "text")}) and purpose = ${purpose} and dest = ${dest}`)[0].n !== scopes.length)
    return `not-granted:${dest}`;
  return null;
}
/** The source versions of a generation vector (its d: keys are the model destination's, not the reader's). */
const sDeps = (d: Record<string, number>) => Object.fromEntries(Object.entries(d ?? {}).filter(([k]) => k.startsWith("s:")));

const asStmt = (r: any): Stmt => ({ id: r.id, scope: r.scope, stated_at: +new Date(r.stated_at), effective_from: r.effective_from ? +new Date(r.effective_from) : null, retraction: r.retraction });

/** Job `l3pair` for one pair key. */
export async function runL3Pair(db: Db, job: Job) {
  const url = env("MEMEX_LLM_URL").replace(/\/+$/, "") + "/chat/completions";
  const model = env("MEMEX_LLM_MODEL");
  const key = env("MEMEX_LLM_KEY");
  const [x, y] = job.key.split("|");
  let problems: string[] = [];
  let about: Record<string, number> | null = null;
  for (let i = 0; i < MAX_TRIES; i++) {
    const c = await snapshot(db, async (q) => {
      const rows = await q`select * from l3_stmt where id = any(${q.array([x, y], "text")}) order by id`;
      if (rows.length !== 2) return { ok: false as const, gone: true };
      if ((await q`select 1 from l3_rel where key = ${job.key} and model = ${model} and prompt_v = ${PAIR_VERSION}`).length) return { ok: false as const, gone: true };
      for (const r of rows) {
        const why = await unreadable(q, r, "derive", "llm");
        if (why === "forgotten" || why === "input-changed" || why === "record-changed") return { ok: false as const, gone: true }; // for good
        if (why === "stale") return { ok: false as const, later: true };
        if (why) return { ok: false as const, reason: why, scopes: [...new Set<string>(rows.flatMap((r: any) => (r.scopes?.length ? r.scopes : [r.auth_scope])))] };
      }
      const keys: string[] = [...new Set<string>(rows.flatMap((r: any) => ["s:" + r.src, ...Object.keys(sDeps(r.deps))])), "d:llm"];
      const vs = new Map<string, number>((await q`select key, v from vis_version where key = any(${q.array(keys, "text")})`).map((r: any) => [r.key, Number(r.v)]));
      return { ok: true as const, rows, deps: Object.fromEntries(keys.map((k) => [k, vs.get(k) ?? 1])) };
    });
    if (!c.ok) {
      if ("gone" in c) return db.begin((tx: Tx) => finishJob(tx, job)); // forgotten, or judged already
      if ("later" in c)
        return db.begin(async (tx: Tx) => {
          const fin = await finishJob(tx, job);
          if (fin === "done") await enqueue(tx, job.kind, job.key, {}, 2_000); // after the renewal has had its turn
          return fin;
        });
      return skip(db, job, c.reason!, c.scopes as string[]);
    }
    const show = (r: any, n: number) =>
      `<statement n="${n}" stated_at="${local(+new Date(r.stated_at))}"${r.effective_from ? ` effective_from="${local(+new Date(r.effective_from))}"` : ""}${r.retraction ? ' retraction="yes"' : ""}>\n${esc(r.text)}\n</statement>`;
    if (!sameDeps(about, c.deps)) problems = [];
    about = c.deps;
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
    const kind: RelKind = s.same_object ? s.relation : "unrelated";
    const rel = orient(p, q2, kind);
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

// ---------- user card (DESIGN §4.5, §6.1) ----------

export const USER_MAX = 2048; // UTF-8 bytes of the block [待标定 ≤2KB]

/** The user card at `t` for a snapshot reader: global statements in effect, newest first, whole lines within
 *  USER_MAX, each naming its evidence (`record:<canonical id>` opens it). Compiled by code, no model call.
 *  Its vector holds s:<source> of every readable global statement — those shown and those that could hide one
 *  (a suppressor withdrawn would make the card stale), so a forget or revision drops the block as a whole. */
export async function userCard(q: Tx, t: number, purpose: string, dest: string): Promise<{ text: string; deps: Record<string, number>; scopes: string[]; next: number | null } | null> {
  const rows = await q`select * from l3_stmt where scope = 'global' order by id`;
  const readable: any[] = [];
  for (const r of rows) if (!(await unreadable(q, r, purpose, dest))) readable.push(r);
  if (!readable.length) return null;
  const rels = (await q`select a, b, kind from l3_rel`) as Rel[];
  const shown = viewAt(readable.map(asStmt), rels, t);
  if (!shown.length) return null;
  const byId = new Map(readable.map((r) => [r.id, r]));
  const lines = shown
    .map((s) => byId.get(s.id))
    .sort((a, z) => +new Date(z.stated_at) - +new Date(a.stated_at) || (a.id < z.id ? -1 : 1))
    .map((r) => `- ${r.text.replace(/\s+/g, " ")} [record:${r.src}:${r.rec}]`);
  let text = "## About the user (what they said, newest first)";
  for (const l of lines) if (bytes(text + "\n" + l) <= USER_MAX) text += "\n" + l;
  const keys = [...new Set<string>(readable.flatMap((r) => ["s:" + r.src, ...Object.keys(sDeps(r.deps))]))];
  const vs = new Map<string, number>((await q`select key, v from vis_version where key = any(${q.array(keys, "text")})`).map((r: any) => [r.key, Number(r.v)]));
  const scopes = [...new Set<string>(readable.flatMap((r) => (r.scopes?.length ? r.scopes : [r.auth_scope])))].sort();
  return { text, deps: Object.fromEntries(keys.map((k) => [k, vs.get(k) ?? 1])), scopes, next: nextBoundary(readable.map(asStmt), t) };
}
