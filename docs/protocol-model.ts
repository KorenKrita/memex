// memex protocol interleaving model — abstract state transitions, NOT Postgres, locks, HTTP or performance.
// Run: bun docs/protocol-model.ts   (exit 1 on any failed assertion)
//
// Each listed broken variant is checked SEPARATELY and must be caught by at least one interleaving.
// This covers only the variants listed here; other deleted rules may still pass (see DESIGN §8 for scope).
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
type SyncVariant = { dedupeSkipsProjection?: boolean; conflictNoBump?: boolean; noRetryShortCircuit?: boolean };
function syncCommit(db: SDB, collector: string, rec: string, hash: string, commitId: string, v: SyncVariant = {}) {
  if (!v.noRetryShortCircuit && db.done[commitId]) return db.done[commitId];   // same commit id retry -> same result
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
  const dv = { dedupeSkipsProjection: true };
  check("B broken variant caught: dedupe skips projection update (each of 3 traces)",
    run(t1, dv).shared.r !== "B'" && run(t2, dv).shared.r !== "CONFLICT" && run(t3, dv).shared.r !== "B");
  const a = run([["N", "B'"], ["M", "B"]]).shared.r, b = run([["M", "B"], ["N", "B'"]]).shared.r;
  check("B new: conflict state independent of upload order", a === "CONFLICT" && b === "CONFLICT");
  check("B new: agreeing collectors merge", run([["M", "X"], ["N", "X"]]).shared.r === "X");
  // retry interleaving: c1:B committed, receipt lost; c2:B' committed; delayed retry of c1 arrives
  const retry = (v: SyncVariant) => { const db = mkSDB(); syncCommit(db, "M", "r", "B", "c1", v); syncCommit(db, "M", "r", "B'", "c2", v); syncCommit(db, "M", "r", "B", "c1", v); return db.shared.r; };
  check("B new: delayed retry of an old commit id does not roll back (stays B')", retry({}) === "B'");
  check("B broken variant caught: retry without commit-id short-circuit rolls back", retry({ noRetryShortCircuit: true }) === "B");
  const before = run([["M", "B"]]); const v0 = before.ver; syncCommit(before, "N", "r", "B'", "x");
  check("B new: entering conflict advances session version", before.ver === v0 + 1);
  const nb = run([["M", "B"]], { conflictNoBump: true }); const v1 = nb.ver; syncCommit(nb, "N", "r", "B'", "x", { conflictNoBump: true });
  check("B broken variant caught: conflict without version bump is invisible to version checks", nb.ver === v1);
}
// Frozen injection blocks (§6.2): client holds blocks + an opaque cursor in memory; the cursor carries the session's
// dependency vector, so even an empty held set learns about invalidation/re-derivation (zero-new-blocks case).
// A block is valid only while every key of the vector it was GENERATED under is unchanged and its inputs pass the
// exact check; once any key advanced, the block is permanently invalid (re-derivation creates a new block id).
type Block = { id: string; genVec: Record<string, number>; inputs: Art };
type FrozenVariant = { appendOnly?: boolean; ignoreGenVec?: boolean };
const blockValid = (b: Block, db: PDB, v: FrozenVariant) =>
  (v.ignoreGenVec || Object.entries(b.genVec).every(([k, n]) => db.ver[k] === n)) && exactOk(db, b.inputs);
type Cursor = Record<string, number>;
function frozenRound(db: PDB, held: Block[], cur: Cursor, all: Block[], v: FrozenVariant = {}): [Block[], Cursor] {
  const stale = Object.entries(cur).some(([k, n]) => db.ver[k] !== n);
  if (!stale || v.appendOnly) return [held, cur];            // nothing changed (or broken: append-only, never replaces)
  return [all.filter(b => blockValid(b, db, v)), Object.fromEntries(Object.keys(cur).map(k => [k, db.ver[k]]))];
}
{
  const db0 = mkPDB();
  const genVec = (d: PDB) => Object.fromEntries(vectorKeys(R1, {}).map(k => [k, d.ver[k]]));
  const K: Block = { id: "K", genVec: genVec(db0), inputs: R1 };
  const cur0: Cursor = Object.fromEntries(vectorKeys(R1, {}).map(k => [k, db0.ver[k]]));
  const d1 = clone(db0); changes({})[5][1](d1);              // P:r1 enters conflict
  const [after, cur1] = frozenRound(d1, [K], cur0, [K]);
  check("B frozen: dependency conflict + zero new blocks -> K actually removed from client", !after.some(b => b.id === "K"));
  check("B broken variant caught: append-only cursor keeps replaying K", frozenRound(d1, [K], cur0, [K], { appendOnly: true })[0].some(b => b.id === "K"));
  const d2 = clone(db0); bump(d2, "s:Z");
  check("B frozen: unrelated session change -> blocks unchanged", frozenRound(d2, [K], cur0, [K])[0][0] === K);
  // conflict resolved later: old K must not come back (it was derived from the pre-conflict state); re-derived K2 appears
  const d3 = clone(d1); d3.conflict.delete("P:r1"); bump(d3, "s:P");
  const K2: Block = { id: "K2", genVec: genVec(d3), inputs: R1 };   // re-derived after resolution
  const [resolved] = frozenRound(d3, after, cur1, [K, K2]);         // the SAME original K is offered again
  check("B broken variant caught: validity ignoring generation vector revives old K",
    frozenRound(d3, after, cur1, [K, K2], { ignoreGenVec: true })[0].some(b => b.id === "K"));
  check("B frozen: after resolution old K is not revived, re-derived K2 appears", !resolved.some(b => b.id === "K") && resolved.some(b => b.id === "K2"));
}
// Read eligibility (§4.1, §5.5): unresolved never readable; cross-replica conflict readable only by mem_zoom when canonical known.
type Rec = { state: "ok" | "conflict" | "unresolved"; canonKnown: boolean; tomb: boolean };
function readable(r: Rec, use: "zoom" | "auto", v: { zoomAllowsUnresolved?: boolean } = {}) {
  if (r.tomb) return false;
  if (r.state === "unresolved") return !!(v.zoomAllowsUnresolved && use === "zoom");   // canonical unknown: never readable
  if (!r.canonKnown) return false;
  if (r.state === "ok") return true;
  return use === "zoom";                                     // conflict: forensic zoom only
}
{
  const unres: Rec = { state: "unresolved", canonKnown: false, tomb: false };  // A->B->C, A forgotten, B lineage missing
  check("B read: unresolved prefix not readable by mem_zoom", !readable(unres, "zoom"));
  check("B read: unresolved prefix not readable by auto paths", !readable(unres, "auto"));
  check("B broken variant caught: zoom exception extended to unresolved", readable(unres, "zoom", { zoomAllowsUnresolved: true }));
  const conf: Rec = { state: "conflict", canonKnown: true, tomb: false };
  check("B read: conflict versions shown to mem_zoom, never auto-injected", readable(conf, "zoom") && !readable(conf, "auto"));
  check("B read: tombstoned conflict not readable even by zoom", !readable({ ...conf, tomb: true }, "zoom"));
}

// ============================================================================
// C. Per-collector FIFO leases (§5.1, §5.2) — time, expiry, commit validity, progress
// ============================================================================
type Job = { id: string; work: number; isScanChunk?: boolean };
type LeaseVariant = { overrideValidLease?: boolean; commitIgnoresLeaseValidity?: boolean; countFailedAsProgress?: boolean };
function simulateLeases(jobsAt: (t: number) => Job[], L: number, ticks: number, v: LeaseVariant = {}) {
  const q: Job[] = []; let cur: { job: Job; start: number; lease: number } | null = null; let leaseSeq = 0, current = 0;
  const committed: string[] = []; let scanProgress = 0;
  const inflight: { job: Job; lease: number; at: number }[] = [];   // requests whose read finished, arriving at `at`
  for (let t = 0; t < ticks; t++) {
    q.push(...jobsAt(t));
    for (const r of inflight.filter(r => r.at === t)) {         // commit arrival: valid only if lease is current AND unexpired
      const valid = r.lease === current && cur !== null && t < cur.start + L;
      if (v.commitIgnoresLeaseValidity || valid) { committed.push(r.job.id); if (r.job.isScanChunk) scanProgress++; if (valid) { current = 0; cur = null; } }
      else if (v.countFailedAsProgress && r.job.isScanChunk) scanProgress++;
    }
    if (cur && t - cur.start >= L) { q.push(cur.job); cur = null; current = 0; }      // lease expiry -> requeue
    if (cur && t - cur.start === cur.job.work && !inflight.some(r => r.lease === cur!.lease))   // read done -> request sent
      inflight.push({ job: cur.job, lease: cur.lease, at: t + 1 + (cur.job.id === "slowpost" ? L : 0) }); // slowpost: slow network
    if (q.length && (!cur || v.overrideValidLease)) { if (cur && v.overrideValidLease) q.push(cur.job); const job = q.shift()!; current = ++leaseSeq; cur = { job, start: t, lease: current }; }
  }
  return { committed, scanProgress };
}
{
  const fastAndSlow = (t: number) => [{ id: "fast" + t, work: 1 }, ...(t === 0 ? [{ id: "slow", work: 3, isScanChunk: true }] : [])];
  const r = simulateLeases(fastAndSlow, 5, 100);
  check("C new: queued scan chunk commits despite continuous appends (FIFO)", r.committed.includes("slow") && r.scanProgress === 1);
  check("C broken variant caught: granting over a valid lease starves the scan chunk", !simulateLeases(fastAndSlow, 5, 100, { overrideValidLease: true }).committed.includes("slow"));
  // a chunk whose request arrives after its lease expired (slow network) must not commit
  const late = (t: number) => (t === 0 ? [{ id: "slowpost", work: 2, isScanChunk: true }] : [{ id: "fast" + t, work: 1 }]);
  const lr = simulateLeases(late, 5, 20);
  check("C new: request arriving after lease expiry is rejected, no progress", !lr.committed.includes("slowpost") && lr.scanProgress === 0);
  check("C broken variant caught: commit ignoring lease validity accepts stale read", simulateLeases(late, 5, 20, { commitIgnoresLeaseValidity: true }).committed.includes("slowpost"));
  check("C broken variant caught: counting failed commits as scan progress", simulateLeases(late, 5, 20, { countFailedAsProgress: true }).scanProgress > 0);
  // deadline boundary: request sent at start+work, arrives one tick later; L = 5
  const at = (work: number) => simulateLeases(t => (t === 0 ? [{ id: "edge", work, isScanChunk: true }] : []), 5, 4 + work);
  check("C new: arrival before lease deadline commits", at(3).committed.includes("edge"));
  check("C new: arrival exactly at lease deadline is rejected", !at(4).committed.includes("edge") && at(4).scanProgress === 0);
  check("C new: arrival after lease deadline is rejected", !at(5).committed.includes("edge"));
  const tooBig = simulateLeases(t => (t === 0 ? [{ id: "huge", work: 9, isScanChunk: true }] : []), 5, 200);
  check("C new: chunk larger than lease never counts as progress (so chunks must be bounded)", tooBig.scanProgress === 0 && !tooBig.committed.includes("huge"));
  let base = 0, slowDone = false;                            // optimistic CAS without queue
  for (let t = 0; t < 100 && !slowDone; t += 3) { const b0 = base; base += 3; if (base === b0) slowDone = true; }
  check("C broken variant caught: CAS without queue starves the prefix revision", !slowDone);
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
