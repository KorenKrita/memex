// Deterministic interleaving model for two protocols (abstract; NOT Postgres or perf):
//  A) publish barrier: OLD (v5.8: owner-only source vector, version sampled after check, stale snapshot)
//     vs NEW (full vector sampled in same snapshot as check; locked compare on fresh read; write-side map).
//  B) sync: OLD (per-collector CAS ticket, shared L0 overwritten by any accepted upload)
//     vs NEW (per-(source,collector) projection + merge with conflict isolation; per-collector fair lease queue).
let ok = 0, fail = 0;
const check = (n: string, c: boolean) => { c ? ok++ : fail++; console.log(`${c ? "PASS" : "FAIL"} ${n}`); };
const expectOld = (n: string, oldBad: boolean) => check(`[old fails as expected] ${n}`, oldBad);

// ---------------- A. publish barrier ----------------
type DB = { ver: Map<string, number>; tomb: Set<string>; dest: Map<string, { ver: number; allowed: boolean }>; canon: Map<string, string> };
const mkdb = (): DB => ({ ver: new Map([["P", 1], ["F", 1], ["Q", 1]]), tomb: new Set(), dest: new Map([["E", { ver: 1, allowed: true }]]), canon: new Map([["F:r1", "P:r1"]]) });
const snap = (db: DB): DB => ({ ver: new Map(db.ver), tomb: new Set(db.tomb), dest: new Map([...db.dest].map(([k, v]) => [k, { ...v }])), canon: new Map(db.canon) });
// Write side (NEW mapping): every invalidating change bumps something an old vector observes.
const W = {
  forgetSource(db: DB, s: string) { db.tomb.add(s); db.ver.set(s, db.ver.get(s)! + 1); },
  hideView(db: DB, s: string) { db.tomb.add("view:" + s); db.ver.set(s, db.ver.get(s)! + 1); },
  remap(db: DB, occ: string, newCanon: string) { db.canon.set(occ, newCanon); const owner = occ.split(":")[0]; db.ver.set(owner, db.ver.get(owner)! + 1); },
  revokeDest(db: DB, e: string) { const d = db.dest.get(e)!; d.allowed = false; d.ver++; },
};
// Artifact: forest root R of fork F: native F records + inherited P records (canonical P), viewed via F, sent to E.
const R = { canonSources: ["P", "F"], viewSessions: ["F"], occ: ["F:r1"], dest: "E" };
function exactCheck(db: DB) {
  return R.canonSources.every(s => !db.tomb.has(s)) && R.viewSessions.every(s => !db.tomb.has("view:" + s)) &&
    R.occ.every(o => db.canon.get(o) === "P:r1") && db.dest.get(R.dest)!.allowed;
}
type Proto = "old" | "new";
// Returns whether a publish happened while the exact predicate is false on the CURRENT db (= a bad publish).
function publish(proto: Proto, db: DB, change: (db: DB) => void, at: "during" | "afterCheck" | "beforeLock") {
  let view = snap(db);
  if (at === "during") change(db); // change commits mid-check; old proto reads mixed state, new proto reads one snapshot
  const okCheck = proto === "new" ? exactCheck(view) : exactCheck(at === "during" ? db : view) || exactCheck(view);
  if (at === "afterCheck") change(db);
  // sample versions
  const vec = proto === "new"
    ? { src: [...R.canonSources, ...R.viewSessions].map(s => [s, view.ver.get(s)!] as const), dest: view.dest.get(R.dest)!.ver }
    : { src: [["F", db.ver.get("F")!] as const], dest: db.dest.get(R.dest)!.ver }; // old: owner-only, sampled late
  if (at === "beforeLock") change(db);
  if (!okCheck) return { published: false, bad: false };
  // locked compare: new = fresh read of db; old = stale snapshot `view` for dest, fresh for src
  const read = proto === "new" ? db : view;
  const same = vec.src.every(([s, v]) => (proto === "new" ? db : db).ver.get(s) === v) && read.dest.get(R.dest)!.ver === vec.dest;
  if (!same) return { published: false, bad: false };
  return { published: true, bad: !exactCheck(db) };
}
const changes: [string, (db: DB) => void][] = [
  ["forget parent P", d => W.forgetSource(d, "P")],
  ["hide target view F", d => W.hideView(d, "F")],
  ["canonical remap F:r1", d => W.remap(d, "F:r1", "Q:r9")],
  ["revoke destination E", d => W.revokeDest(d, "E")],
];
for (const [name, ch] of changes) for (const at of ["during", "afterCheck", "beforeLock"] as const) {
  const n = publish("new", mkdb(), ch, at); check(`A new: ${name} @${at} → no stale publish`, !n.bad);
}
{ // old protocol must exhibit at least one bad publish (owner-only vector misses parent forget; late sampling misses dest revoke)
  const bad = changes.flatMap(([, ch]) => (["during", "afterCheck", "beforeLock"] as const).map(at => publish("old", mkdb(), ch, at).bad));
  expectOld("A old: some interleaving publishes stale content", bad.some(Boolean));
}
// unrelated change does not block publish (progress)
{ const db = mkdb(); const r = publish("new", db, d => d.ver.set("Q", 9), "afterCheck"); check("A new: unrelated source change still publishes", r.published && !r.bad); }
// locked-section budget: O(S) compare with a deadline; exceeding deadline → no publish
{
  const S = 64, deadline = 32; let compared = 0, published = false;
  for (let i = 0; i < S; i++) { if (compared >= deadline) break; compared++; }
  published = compared === S;
  check("A new: locked compare over deadline aborts without publishing", !published);
}

// ---------------- B. sync ----------------
// B1 cross-collector: same stable record id, different content, order swapped.
type Up = { collector: string; rec: string; content: string };
function oldShared(ups: Up[]) { const l0 = new Map<string, string>(); for (const u of ups) l0.set(u.rec, u.content); return l0.get("r")!; }
function newMerge(ups: Up[]) {
  const proj = new Map<string, Map<string, string>>(); // collector -> rec -> content (latest per collector)
  for (const u of ups) { if (!proj.has(u.collector)) proj.set(u.collector, new Map()); proj.get(u.collector)!.set(u.rec, u.content); }
  const vals = new Set([...proj.values()].map(m => m.get("r")).filter(Boolean));
  return vals.size === 1 ? [...vals][0]! : "CONFLICT";
}
{
  const a: Up[] = [{ collector: "N", rec: "r", content: "B'" }, { collector: "M", rec: "r", content: "B" }];
  const b: Up[] = [...a].reverse();
  expectOld("B1 old: last writer wins, result depends on order", oldShared(a) !== oldShared(b));
  check("B1 new: same conflict state regardless of order", newMerge(a) === "CONFLICT" && newMerge(b) === "CONFLICT");
  check("B1 new: agreeing collectors merge cleanly", newMerge([{ collector: "N", rec: "r", content: "X" }, { collector: "M", rec: "r", content: "X" }]) === "X");
}
// B2 same collector, stale read arriving late.
{
  // file timeline: v1 "B" then v2 "B'"; process P reads v1 (holds data), Q reads v2 and commits; then P's request arrives
  // NEW: lease acquired BEFORE reading; P's lease expired/superseded → rejected.
  let leaseSeq = 0, current = 0; const acquire = () => (current = ++leaseSeq);
  const pLease = acquire(); const pData = "B";
  const qLease = acquire(); const qData = "B'"; let l0 = "";
  const commit = (lease: number, data: string) => { if (lease !== current) return false; l0 = data; current = 0; return true; };
  commit(qLease, qData); const pOk = commit(pLease, pData);
  check("B2 new: stale read with superseded lease rejected", !pOk && l0 === "B'");
}
// B3 progress: continuous tail appends vs queued prefix revision.
function runSync(proto: Proto, rounds: number) {
  let base = 0, slowDone = false;
  if (proto === "old") { // CAS: both read base; fast path commits first every round
    for (let i = 0; i < rounds && !slowDone; i++) { const slowBase = base; base++; /* fast commits */ if (slowBase === base) slowDone = true; }
  } else { // fair FIFO lease queue per (source, collector): slow enqueued once, fast enqueues every round
    const q: string[] = ["slow"]; for (let i = 0; i < rounds && !slowDone; i++) { q.push("fast"); const job = q.shift()!; if (job === "slow") slowDone = true; }
  }
  return slowDone;
}
expectOld("B3 old: prefix revision starved by 100 tail appends", !runSync("old", 100));
check("B3 new: queued prefix revision commits despite continuous appends", runSync("new", 100));
// B4 recovery: lease expiry, lost receipt (idempotent by commit id).
{
  const done = new Map<string, string>(); let current = 0, seq = 0; const acquire = () => (current = ++seq);
  const commit = (id: string, lease: number, data: string) => { if (done.has(id)) return done.get(id)!; if (lease !== current) return "expired"; done.set(id, data); current = 0; return data; };
  const l1 = acquire(); /* plugin crashes; lease expires */ current = 0;
  check("B4 new: expired lease cannot commit", commit("c1", l1, "x") === "expired");
  const l2 = acquire(); commit("c2", l2, "y");
  check("B4 new: lost receipt retry returns same result", commit("c2", l2, "y") === "y");
}

console.log(`\n${ok} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
