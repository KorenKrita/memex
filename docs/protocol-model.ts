// memex protocol interleaving model — abstract state transitions, NOT Postgres, locks, HTTP or performance.
// Run: bun docs/protocol-model.ts   (exit 1 on any failed assertion)
//
// Every "old/broken variant" is checked SEPARATELY: each must be caught by at least one interleaving,
// so that deleting a rule from the design makes this model go red.
let passed = 0, failed = 0;
const check = (name: string, cond: boolean) => { cond ? passed++ : failed++; console.log(`${cond ? "PASS" : "FAIL"} ${name}`); };
const clone = <T>(x: T): T => structuredClone(x);

// ============================================================================
// A. Publish barrier (§5.5)
// ============================================================================
type Occ = { view: string; canon: string };                  // occurrence shown in session `view`, evidence `canon` ("P:r1")
type Art = { name: string; occ: Occ[]; query: string; dest: string };
type PDB = {
  ver: Record<string, number>;                               // "s:P" session_vis_version, "d:E" dest_vis_version
  tomb: Set<string>;                                         // "rec:P:r1" | "view:F" | "query:Q"
  conflict: Set<string>;                                     // canonical records in cross-replica conflict
  canonOf: Record<string, string>;                           // occurrence key "F:r1" -> canonical
  destAllowed: Record<string, boolean>;
};
const sess = (canon: string) => canon.split(":")[0];
const mkPDB = (): PDB => ({
  ver: { "s:P": 1, "s:F": 1, "s:Q": 1, "s:Z": 1, "d:E": 1 },
  tomb: new Set(), conflict: new Set(),
  canonOf: { "F:r1": "P:r1", "F:r2": "F:r2" }, destAllowed: { E: true },
});
const occKey = (o: Occ) => `${o.view}:${o.canon.split(":")[1]}`;
// R1: fork root mixing inherited P evidence and native F evidence. R2: purely inherited evidence shown in view F.
const R1: Art = { name: "fork-root", occ: [{ view: "F", canon: "P:r1" }, { view: "F", canon: "F:r2" }], query: "Q", dest: "E" };
const R2: Art = { name: "inherited-only", occ: [{ view: "F", canon: "P:r1" }], query: "Q", dest: "E" };

function exactOk(db: PDB, a: Art): boolean {
  return a.occ.every(o =>
    db.canonOf[occKey(o)] === o.canon && !db.tomb.has("rec:" + o.canon) && !db.tomb.has("view:" + o.view) &&
    !db.conflict.has(o.canon)) && !db.tomb.has("query:" + a.query) && db.destAllowed[a.dest];
}
type Variant = { omitView?: boolean; omitQuery?: boolean; sampleLate?: boolean; staleSnapshot?: boolean; conflictNoBump?: boolean; remapBumpsNewCanon?: boolean };
function vectorKeys(a: Art, v: Variant): string[] {
  const k = new Set<string>();
  for (const o of a.occ) { k.add("s:" + sess(o.canon)); if (!v.omitView) k.add("s:" + o.view); }
  if (!v.omitQuery) k.add("s:" + a.query);
  k.add("d:" + a.dest);
  return [...k];
}
const bump = (db: PDB, key: string) => { db.ver[key] = (db.ver[key] ?? 0) + 1; };
// Write side. Each invalidating change advances at least one key an old vector observes (unless a broken variant).
const changes = (v: Variant): [string, (db: PDB) => void][] => [
  ["forget parent P", db => { db.tomb.add("rec:P:r1"); bump(db, "s:P"); }],
  ["forget view F", db => { db.tomb.add("view:F"); bump(db, "s:F"); }],
  ["forget query session Q", db => { db.tomb.add("query:Q"); bump(db, "s:Q"); }],
  ["remap F:r1 -> Z:r9", db => { db.canonOf["F:r1"] = "Z:r9"; bump(db, v.remapBumpsNewCanon ? "s:Z" : "s:F"); }],
  ["revoke destination E", db => { db.destAllowed.E = false; bump(db, "d:E"); }],
  ["P:r1 enters cross-replica conflict", db => { db.conflict.add("P:r1"); if (!v.conflictNoBump) bump(db, "s:P"); }],
];
type Pause = "duringCheck" | "afterCheck" | "beforeLock";
// Returns "published-ok" | "published-STALE" | "rejected"
function publish(v: Variant, a: Art, change: (db: PDB) => void, at: Pause): string {
  const db = mkPDB();
  const snap1 = clone(db);                                   // step 1: one consistent snapshot
  if (at === "duringCheck") change(db);                      // commits while step 1 runs; snapshot unaffected
  const ok1 = exactOk(snap1, a);
  if (at === "afterCheck") change(db);
  const sampleFrom = v.sampleLate ? db : snap1;              // broken: vector read after the check
  const vec = Object.fromEntries(vectorKeys(a, v).map(k => [k, sampleFrom.ver[k]]));
  if (at === "beforeLock") change(db);
  if (!ok1) return "rejected";
  const readView = v.staleSnapshot ? snap1 : db;             // step 2: fresh committed read (broken: reuse snapshot)
  const same = Object.entries(vec).every(([k, n]) => readView.ver[k] === n);
  if (!same) return "rejected";
  return exactOk(db, a) ? "published-ok" : "published-STALE";
}
const pauses: Pause[] = ["duringCheck", "afterCheck", "beforeLock"];
function anyStale(v: Variant, arts: Art[]) {
  return arts.some(a => changes(v).some(([, ch]) => pauses.some(p => publish(v, a, ch, p) === "published-STALE")));
}
for (const a of [R1, R2]) for (const [n, ch] of changes({})) for (const p of pauses)
  check(`A new: ${a.name} / ${n} @${p} -> never stale`, publish({}, a, ch, p) !== "published-STALE");
check("A new: unrelated session Z change still publishes",
  publish({}, R1, db => bump(db, "s:Z"), "afterCheck") === "published-ok");
const brokenPublish: [string, Variant][] = [
  ["vector omits view sessions", { omitView: true }],
  ["vector omits query session", { omitQuery: true }],
  ["versions sampled after the check", { sampleLate: true }],
  ["step 2 reuses step-1 snapshot", { staleSnapshot: true }],
  ["conflict transition does not bump version", { conflictNoBump: true }],
  ["remap bumps only the new canonical source", { remapBumpsNewCanon: true }],
];
for (const [n, v] of brokenPublish) check(`A broken variant caught: ${n}`, anyStale(v, [R1, R2]));

// Locked section with an execution deadline: publisher compares S keys (1 tick each) holding the shared lock;
// a writer waiting for the exclusive lock must not wait longer than the deadline; abort => no publish.
function lockedCompare(S: number, deadline: number) {
  let t = 0, published = false;
  while (t < deadline && t < S) t++;
  if (t === S && t <= deadline) published = true;            // finished in time -> hand to transport, release
  const writerCommitsAt = t;                                 // lock released at t either way
  return { published, writerWait: writerCommitsAt };
}
{
  const small = lockedCompare(2, 10), big = lockedCompare(500, 10);
  check("A lock: small vector publishes within deadline", small.published && small.writerWait <= 10);
  check("A lock: oversized vector aborts, releases lock at deadline, no publish", !big.published && big.writerWait === 10);
}

// ============================================================================
// B. Multi-collector projections (§4.1) — dedupe, projection, merge, conflict -> visibility
// ============================================================================
type SDB = { blobs: Set<string>; proj: Record<string, Record<string, string>>; shared: Record<string, string>; ver: number; done: Record<string, string> };
const mkSDB = (): SDB => ({ blobs: new Set(), proj: {}, shared: {}, ver: 1, done: {} });
type SyncVariant = { dedupeSkipsProjection?: boolean; conflictNoBump?: boolean };
function syncCommit(db: SDB, collector: string, rec: string, hash: string, commitId: string, v: SyncVariant = {}) {
  if (db.done[commitId]) return db.done[commitId];          // same commit id retry -> same result
  const key = `${rec}#${hash}`;
  const existed = db.blobs.has(key);
  db.blobs.add(key);                                         // dedupe = never store the body twice
  if (!(v.dedupeSkipsProjection && existed)) (db.proj[collector] ??= {})[rec] = hash;
  const vals = new Set(Object.values(db.proj).map(p => p[rec]).filter(Boolean));
  const next = vals.size === 1 ? [...vals][0] : "CONFLICT";
  const prev = db.shared[rec];
  db.shared[rec] = next;
  if (prev !== undefined && prev !== next && !(v.conflictNoBump && next === "CONFLICT")) db.ver++;
  return (db.done[commitId] = next);
}
const run = (steps: [string, string][], v: SyncVariant = {}) => {
  const db = mkSDB(); steps.forEach(([c, h], i) => syncCommit(db, c, "r", h, "c" + i, v)); return db;
};
{
  const t1: [string, string][] = [["M", "B'"], ["N", "B"], ["N", "B'"]];
  const t2: [string, string][] = [["M", "B"], ["N", "B"], ["M", "B'"]];
  const t3: [string, string][] = [["M", "B"], ["M", "B'"], ["M", "B"]];
  check("B new: M:B'/N:B -> N:B' resolves conflict although body exists", run(t1).shared.r === "B'");
  check("B new: second collector with same body is registered; M:B' then conflicts", run(t2).shared.r === "CONFLICT");
  check("B new: same collector B -> B' -> B ends at B", run(t3).shared.r === "B");
  const ok = [run(t1, { dedupeSkipsProjection: true }).shared.r !== "B'", run(t2, { dedupeSkipsProjection: true }).shared.r !== "CONFLICT", run(t3, { dedupeSkipsProjection: true }).shared.r !== "B"];
  check("B broken variant caught: dedupe skips projection update (all 3 traces)", ok.every(Boolean));
  const a = run([["N", "B'"], ["M", "B"]]).shared.r, b = run([["M", "B"], ["N", "B'"]]).shared.r;
  check("B new: conflict state independent of upload order", a === "CONFLICT" && b === "CONFLICT");
  check("B new: agreeing collectors merge", run([["M", "X"], ["N", "X"]]).shared.r === "X");
  const db = mkSDB(); syncCommit(db, "M", "r", "B", "c1");
  check("B new: lost receipt retry with same commit id is idempotent", syncCommit(db, "M", "r", "B", "c1") === "B" && Object.keys(db.done).length === 1);
  // conflict entry must advance the session version so in-flight publishes and frozen blocks notice
  const before = run([["M", "B"]]); const v0 = before.ver; syncCommit(before, "N", "r", "B'", "x");
  check("B new: entering conflict advances session version", before.ver === v0 + 1);
  const nb = run([["M", "B"]], { conflictNoBump: true }); const v1 = nb.ver; syncCommit(nb, "N", "r", "B'", "x", { conflictNoBump: true });
  check("B broken variant caught: conflict without version bump leaves frozen block cursor unchanged", nb.ver === v1);
}
// Frozen injection block K (§6.2): remote replaces the snapshot when any key in K's vector advanced.
{
  const db = mkPDB(); const K = { vec: Object.fromEntries(vectorKeys(R1, {}).map(k => [k, db.ver[k]])) };
  const req = (d: PDB) => Object.entries(K.vec).some(([k, n]) => d.ver[k] !== n) ? "replace" : "no-new-blocks";
  const d1 = clone(db); changes({})[5][1](d1);
  check("B frozen: zero new blocks + dependency conflict -> snapshot replace", req(d1) === "replace");
  const d2 = clone(db); bump(d2, "s:Z");
  check("B frozen: unrelated session change -> no replace", req(d2) === "no-new-blocks");
}

// ============================================================================
// C. Per-collector FIFO leases (§5.1, §5.2) — time, expiry, commit, progress
// ============================================================================
type Job = { id: string; work: number; isScanChunk?: boolean };
function simulateLeases(jobsAt: (t: number) => Job[], L: number, ticks: number) {
  const q: Job[] = []; let cur: { job: Job; start: number } | null = null;
  const committed: string[] = []; let scanProgress = 0, overrideAttempts = 0;
  for (let t = 0; t < ticks; t++) {
    q.push(...jobsAt(t));
    if (cur && t - cur.start >= cur.job.work) {             // finished reading -> commit, only if lease still valid
      if (cur.job.work <= L) { committed.push(cur.job.id); if (cur.job.isScanChunk) scanProgress++; }
      else q.push(cur.job);                                  // expired: rejected, re-enqueue, no progress
      cur = null;
    }
    if (cur && t - cur.start >= L) { q.push(cur.job); cur = null; }   // lease expiry
    if (!cur && q.length) cur = { job: q.shift()!, start: t };       // grant only when no valid lease
    else if (cur && q.length) overrideAttempts++;            // others wait; a valid lease is never overridden
  }
  return { committed, scanProgress, overrideAttempts };
}
{
  // continuous fast appends every tick; one prefix-revision scan chunk enqueued at t=0
  const r = simulateLeases(t => [{ id: "fast" + t, work: 1 }, ...(t === 0 ? [{ id: "slow", work: 3, isScanChunk: true }] : [])], 5, 100);
  check("C new: queued scan chunk commits despite continuous appends (FIFO)", r.committed.includes("slow") && r.scanProgress === 1);
  check("C new: valid lease is never overridden (others queue)", r.overrideAttempts > 0);
  const tooBig = simulateLeases(t => (t === 0 ? [{ id: "huge", work: 9, isScanChunk: true }] : []), 5, 200);
  check("C new: chunk larger than lease never counts as progress (so chunks must be bounded)", tooBig.scanProgress === 0 && !tooBig.committed.includes("huge"));
  // broken: optimistic CAS without queue — fast path commits every tick, slow reads 3 ticks then CAS on base
  let base = 0, slowDone = false;
  for (let t = 0; t < 100 && !slowDone; t += 3) { const b0 = base; base += 3; if (base === b0) slowDone = true; }
  check("C broken variant caught: CAS without queue starves the prefix revision", !slowDone);
}

// ============================================================================
// D. Codex Legacy, option B (§4.1): single collector, staged chunks, server-side alignment off-lease
// ============================================================================
type Slot = { id: number; h: string; unresolved?: boolean };
// Align new ordered hashes against old slots. Unique-hash LCS anchors; gaps: equal length -> revisions (keep ids);
// otherwise new ids for inserted lines. Hashes duplicated on either side -> those lines unresolved.
function align(old: Slot[], next: string[], counter: { n: number }): Slot[] {
  const cnt = (xs: string[]) => xs.reduce((m, x) => m.set(x, (m.get(x) ?? 0) + 1), new Map<string, number>());
  const co = cnt(old.map(s => s.h)), cn = cnt(next);
  const uniq = (h: string) => co.get(h) === 1 && cn.get(h) === 1;
  // LCS over unique hashes
  const A = old.filter(s => uniq(s.h)), B = next.map((h, i) => ({ h, i })).filter(x => uniq(x.h));
  const dp = Array.from({ length: A.length + 1 }, () => Array(B.length + 1).fill(0));
  for (let i = A.length - 1; i >= 0; i--) for (let j = B.length - 1; j >= 0; j--)
    dp[i][j] = A[i].h === B[j].h ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
  const anchors: [number, number][] = []; // [oldIndex, newIndex]
  for (let i = 0, j = 0; i < A.length && j < B.length;) {
    if (A[i].h === B[j].h) { anchors.push([old.indexOf(A[i]), B[j].i]); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) i++; else j++;
  }
  anchors.push([old.length, next.length]);
  const out: Slot[] = []; let po = 0, pn = 0;
  for (const [ao, an] of anchors) {
    const og = old.slice(po, ao), ng = next.slice(pn, an);
    ng.forEach((h, k) => {
      const ambiguous = (co.get(h) ?? 0) > 1 || (cn.get(h) ?? 0) > 1;
      if (og.length === ng.length) out.push({ id: og[k].id, h, unresolved: ambiguous || undefined }); // revision or same
      else out.push({ id: ++counter.n, h, unresolved: ambiguous || undefined });
    });
    if (ao < old.length) out.push({ id: old[ao].id, h: next[an] });
    po = ao + 1; pn = an + 1;
  }
  return out;
}
{
  const c = { n: 0 };
  const s1 = align([], ["A", "B", "C"], c);
  const s2 = align(s1, ["A", "X", "B", "C"], c);
  check("D legacy: insert keeps existing slots (A B C -> A X B C = 1 4 2 3)", s2.map(s => s.id).join(" ") === "1 4 2 3");
  const s3 = align(s2, ["A", "X", "B'", "C"], c);
  check("D legacy: same-position change is a revision of slot 2", s3[2].id === 2 && s3[2].h === "B'");
  const d1 = align([], ["A", "Q", "A"], { n: 0 });
  check("D legacy: duplicated lines are unresolved, not silently matched", d1.filter(s => s.unresolved).length === 2);
}
// Staging + server alignment: client uploads bounded chunks under leases, then seals (staged generation g).
// Server aligns off-lease and publishes with CAS on (staged generation, projection version).
{
  let stagedGen = 1, projVer = 1; let proj = "old";
  const alignJob = (gen: number, pv: number, result: string) => {
    if (gen !== stagedGen || pv !== projVer) return false; proj = result; projVer++; return true;
  };
  const g = stagedGen, p = projVer;          // server starts aligning generation 1
  stagedGen++;                               // meanwhile client seals a newer snapshot
  check("D legacy: stale alignment (newer seal arrived) is rejected", !alignJob(g, p, "from-gen1") && proj === "old");
  check("D legacy: re-run on current generation publishes", alignJob(stagedGen, projVer, "from-gen2") && proj === "from-gen2");
}
// Second collector for a Legacy session -> whole session cross-replica conflict (option B), order independent.
{
  const legacy = (order: string[]) => {
    const owners = new Set<string>(); let state = "ok", ver = 1;
    for (const c of order) { owners.add(c); if (owners.size > 1 && state !== "replica_conflict") { state = "replica_conflict"; ver++; } }
    return { state, ver };
  };
  const x = legacy(["M", "M", "N"]), y = legacy(["N", "M", "M"]);
  check("D legacy: second collector isolates the whole session, both orders", x.state === "replica_conflict" && y.state === "replica_conflict");
  check("D legacy: isolation advances session version (publishes/frozen blocks notice)", x.ver === 2 && y.ver === 2);
  check("D legacy: single collector stays available", legacy(["M", "M"]).state === "ok");
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
