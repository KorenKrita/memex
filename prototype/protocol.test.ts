// Integration tests against a real Postgres (see README). Each test drives the public protocol functions from
// several independent connections; interleavings are forced with explicit pause hooks and by observing
// pg_locks / the clock, never with blind sleeps.
import { beforeAll, beforeEach, afterAll, expect, test, describe } from "bun:test";
import { SQL } from "bun";
import * as P from "./protocol";

const URL = process.env.MEMEX_PG_URL ?? "postgres://postgres:memex@127.0.0.1:55432/memex";
const db = new SQL(URL, { max: 12 });
type Conn = Awaited<ReturnType<typeof db.reserve>>;
const conns: Conn[] = [];
const conn = async () => { const c = await db.reserve(); conns.push(c); return c; };

beforeAll(async () => { await P.installSchema(db); });
beforeEach(async () => {
  for (const c of conns.splice(0)) c.release();
  // isolation: a failed test may leave a backend sleeping or holding a lock; kill it so failures don't cascade
  await db`select pg_terminate_backend(pid) from pg_stat_activity
           where datname = current_database() and pid <> pg_backend_pid() and state <> 'idle'`;
  await P.resetData(db);
});
afterAll(async () => { for (const c of conns.splice(0)) c.release(); await db.close(); });

/** Resolves once some other backend is waiting on an advisory lock (the writer is really blocked). */
async function waitForAdvisoryWaiter() {
  for (let i = 0; i < 500; i++) {
    const [r] = await db`select count(*)::int n from pg_locks where locktype = 'advisory' and not granted`;
    if (r.n > 0) return;
    await Bun.sleep(10);
  }
  throw new Error("no advisory lock waiter appeared");
}
/** Resolves once the server clock has passed the lease's expiry. */
async function waitUntilExpired(leaseId: number) {
  for (let i = 0; i < 500; i++) {
    const [r] = await db`select clock_timestamp() >= expires_at as gone from lease where id = ${leaseId}`;
    if (r.gone) return;
    await Bun.sleep(10);
  }
  throw new Error("lease never expired");
}

// Fixture: parent session P, fork F inheriting P:r1 and owning native F:r2; query session Q; destination E.
async function seedFork() {
  await P.seed(db, {
    occurrences: [
      { occ: "F:r1", view: "F", canon: "P:r1" },
      { occ: "F:r2", view: "F", canon: "F:r2" },
    ],
    dests: ["E"],
  });
}
const forkRoot = { id: "R1", occs: ["F:r1", "F:r2"], query: "Q", dest: "E" };   // mixes parent + native evidence
const inheritedOnly = { id: "R2", occs: ["F:r1"], query: "Q", dest: "E" };      // only P evidence, shown in view F

// ---------------------------------------------------------------------------------------------------------------
describe("publish barrier (§5.5)", () => {
  const writers: [string, () => Promise<void>][] = [
    ["forget parent record P:r1", () => P.forgetRecord(db, "P:r1")],
    ["forget view F", () => P.hideView(db, "F")],
    ["forget query session Q", () => P.forgetQuerySession(db, "Q")],
    ["revoke destination E", () => P.revokeDest(db, "E")],
    ["remap F:r1 -> Z:r9", () => P.remap(db, "F:r1", "Z:r9")],
  ];
  for (const art of [forkRoot, inheritedOnly])
    for (const [name, write] of writers)
      for (const at of ["duringCheck", "afterCheck", "beforeLock"] as const)
        test(`${art.id}: ${name} committed ${at} -> not published`, async () => {
          await seedFork(); await P.createArtifact(db, art);
          const c = await conn();
          const r = await P.publish(c, art.id, { [at]: write });
          expect(r).toBe("rejected");
          expect(await P.publicationCount(db, art.id)).toBe(0);
        });

  test("unrelated session change does not block publishing", async () => {
    await seedFork(); await P.seed(db, { occurrences: [{ occ: "Z:r1", view: "Z", canon: "Z:r1" }], dests: [] });
    await P.createArtifact(db, forkRoot);
    const c = await conn();
    expect(await P.publish(c, "R1", { afterCheck: () => P.forgetRecord(db, "Z:r1") })).toBe("published");
  });

  test("publisher holding the shared lock makes the writer wait; writer then invalidates the published copy", async () => {
    await seedFork(); await P.createArtifact(db, forkRoot);
    const c = await conn();
    let writer: Promise<void> | undefined;
    const r = await P.publish(c, "R1", {
      inLock: async () => { writer = P.forgetRecord(db, "P:r1"); await waitForAdvisoryWaiter(); },
    });
    expect(r).toBe("published");
    await writer;
    expect(await P.isArtifactValid(db, "R1")).toBe(false);
  });

  test("broken variant caught: sampling versions after the check (separate snapshot) publishes stale content", async () => {
    await seedFork(); await P.createArtifact(db, forkRoot);
    const c = await conn();
    expect(await P.publish(c, "R1", { duringCheck: () => P.revokeDest(db, "E") }, { sampleLate: true })).toBe("published");
  });

  test("broken variant caught: step 2 reusing the step-1 REPEATABLE READ snapshot publishes stale content", async () => {
    await seedFork(); await P.createArtifact(db, forkRoot);
    const c = await conn();
    const r = await P.publish(c, "R1", { afterCheck: () => P.revokeDest(db, "E") }, { reuseSnapshot: true });
    expect(r).toBe("published");          // the bug the correct protocol prevents
  });

  test("broken variant caught: vector without view sessions misses forget(view F) for inherited-only content", async () => {
    await seedFork(); await P.createArtifact(db, inheritedOnly);
    const c = await conn();
    expect(await P.publish(c, "R2", { afterCheck: () => P.hideView(db, "F") }, { omitViewSessions: true })).toBe("published");
  });

  test("locked section over its deadline is cancelled server-side: no publication, waiting writer proceeds", async () => {
    await seedFork(); await P.createArtifact(db, forkRoot);
    const c = await conn();
    let writer: Promise<void> | undefined;
    const r = await P.publish(c, "R1", {
      inLock: async (tx) => {
        writer = P.forgetRecord(db, "P:r1");
        await waitForAdvisoryWaiter();
        await tx`select pg_sleep(5)`;   // simulates an oversized vector compare inside the lock
      },
    }, { lockDeadlineMs: 300 });
    expect(r).toBe("timeout");
    await writer;                       // would hang for 5s if the lock were still held
    expect(await P.publicationCount(db, "R1")).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------------------------------
describe("multi-collector projections (§4.1)", () => {
  const commit = async (collector: string, hash: string, commitId: string) => {
    const lease = await P.acquireLease(db, "P", collector, 10_000);
    return P.syncCommit(db, { leaseId: lease, commitId, records: [{ rec: "P:r1", hash }] });
  };

  test("M:B'/N:B then N:B' resolves the conflict although body B' already stored", async () => {
    await commit("M", "B'", "c1"); await commit("N", "B", "c2");
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "conflict" });
    await commit("N", "B'", "c3");
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "ok", hash: "B'" });
  });
  test("second collector with an already-stored body is registered, so a later change conflicts", async () => {
    await commit("M", "B", "c1"); await commit("N", "B", "c2"); await commit("M", "B'", "c3");
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "conflict" });
  });
  test("same collector B -> B' -> B ends at B", async () => {
    await commit("M", "B", "c1"); await commit("M", "B'", "c2"); await commit("M", "B", "c3");
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "ok", hash: "B" });
  });
  test("two collectors committing different content concurrently end in conflict", async () => {
    const lm = await P.acquireLease(db, "P", "M", 10_000), ln = await P.acquireLease(db, "P", "N", 10_000);
    await Promise.all([
      P.syncCommit(db, { leaseId: lm, commitId: "m", records: [{ rec: "P:r1", hash: "B" }] }),
      P.syncCommit(db, { leaseId: ln, commitId: "n", records: [{ rec: "P:r1", hash: "B'" }] }),
    ]);
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "conflict" });
  });
  test("broken variant caught: merging without the global write barrier lets both commits miss each other", async () => {
    const lm = await P.acquireLease(db, "P", "M", 10_000), ln = await P.acquireLease(db, "P", "N", 10_000);
    let arrived = 0, release!: () => void; const gate = new Promise<void>(r => (release = r));
    const barrier = async () => { if (++arrived === 2) release(); await gate; };
    await Promise.all([
      P.syncCommit(db, { leaseId: lm, commitId: "m", records: [{ rec: "P:r1", hash: "B" }] }, { noMergeLock: true, beforeMerge: barrier }),
      P.syncCommit(db, { leaseId: ln, commitId: "n", records: [{ rec: "P:r1", hash: "B'" }] }, { noMergeLock: true, beforeMerge: barrier }),
    ]);
    expect((await P.sharedState(db, "P:r1")).state).toBe("ok");   // conflict lost: the bug
  });
});

// ---------------------------------------------------------------------------------------------------------------
describe("sync leases and idempotency (§5.1, §5.2)", () => {
  test("leases for one (source, collector) are granted strictly FIFO, never over a valid lease", async () => {
    const a = await P.requestLease(db, "P", "M", 10_000);
    const b = await P.requestLease(db, "P", "M", 10_000);
    const c = await P.requestLease(db, "P", "M", 10_000);
    expect(await P.tryGrant(db, a)).toBe(true);
    expect(await P.tryGrant(db, c)).toBe(false);   // not its turn
    expect(await P.tryGrant(db, b)).toBe(false);   // a still valid
    await P.syncCommit(db, { leaseId: a, commitId: "a", records: [{ rec: "P:r1", hash: "X" }] });
    expect(await P.tryGrant(db, c)).toBe(false);   // b is ahead
    expect(await P.tryGrant(db, b)).toBe(true);
  });
  test("a stale read arriving after its lease expired and a newer commit landed is rejected", async () => {
    const old = await P.acquireLease(db, "P", "M", 200);       // reads B, then is slow
    await waitUntilExpired(old);
    const fresh = await P.acquireLease(db, "P", "M", 10_000);
    expect(await P.syncCommit(db, { leaseId: fresh, commitId: "new", records: [{ rec: "P:r1", hash: "B'" }] })).toEqual({ ok: true });
    expect(await P.syncCommit(db, { leaseId: old, commitId: "old", records: [{ rec: "P:r1", hash: "B" }], scanChunks: 1 })).toEqual({ ok: false, reason: "lease" });
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "ok", hash: "B'" });
    expect(await P.scanProgress(db, "P", "M")).toBe(0);
  });
  test("commit before the deadline succeeds and advances scan progress", async () => {
    const l = await P.acquireLease(db, "P", "M", 10_000);
    expect(await P.syncCommit(db, { leaseId: l, commitId: "c", records: [{ rec: "P:r1", hash: "B" }], scanChunks: 2 })).toEqual({ ok: true });
    expect(await P.scanProgress(db, "P", "M")).toBe(2);
  });
  test("commit after the deadline is rejected even with no competitor, progress unchanged", async () => {
    const l = await P.acquireLease(db, "P", "M", 150);
    await waitUntilExpired(l);
    expect(await P.syncCommit(db, { leaseId: l, commitId: "c", records: [{ rec: "P:r1", hash: "B" }], scanChunks: 2 })).toEqual({ ok: false, reason: "lease" });
    expect(await P.scanProgress(db, "P", "M")).toBe(0);
  });
  test("delayed retry of an old commit id returns the original result and does not roll back", async () => {
    const l1 = await P.acquireLease(db, "P", "M", 10_000);
    await P.syncCommit(db, { leaseId: l1, commitId: "c1", records: [{ rec: "P:r1", hash: "B" }] });
    const l2 = await P.acquireLease(db, "P", "M", 10_000);
    await P.syncCommit(db, { leaseId: l2, commitId: "c2", records: [{ rec: "P:r1", hash: "B'" }] });
    expect(await P.syncCommit(db, { leaseId: l1, commitId: "c1", records: [{ rec: "P:r1", hash: "B" }] })).toEqual({ ok: true });
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "ok", hash: "B'" });
  });
  test("a commit that fails mid-transaction leaves no partial state and keeps the lease usable", async () => {
    const l = await P.acquireLease(db, "P", "M", 10_000);
    await expect(P.syncCommit(db, { leaseId: l, commitId: "bad", records: [{ rec: "P:r1", hash: "B" }, { rec: "P:r2", hash: null as any }], scanChunks: 1 })).rejects.toThrow();
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "absent" });
    expect(await P.scanProgress(db, "P", "M")).toBe(0);
    expect(await P.syncCommit(db, { leaseId: l, commitId: "bad", records: [{ rec: "P:r1", hash: "B" }], scanChunks: 1 })).toEqual({ ok: true });
  });
});

// ---------------------------------------------------------------------------------------------------------------
describe("conflict lifecycle and derived artifacts (§4.1, §6.2)", () => {
  test("artifact K becomes invalid when its input enters conflict and is not revived when the conflict resolves", async () => {
    const commit = async (col: string, hash: string, id: string) =>
      P.syncCommit(db, { leaseId: await P.acquireLease(db, "P", col, 10_000), commitId: id, records: [{ rec: "P:r1", hash }] });
    await commit("M", "B", "m1");
    await P.seed(db, { occurrences: [{ occ: "P:r1", view: "P", canon: "P:r1" }], dests: ["E"] });
    await P.createArtifact(db, { id: "K", occs: ["P:r1"], query: "Q", dest: "E" });
    expect(await P.isArtifactValid(db, "K")).toBe(true);
    await commit("N", "B'", "n1");                                   // enters conflict
    expect(await P.isArtifactValid(db, "K")).toBe(false);
    const c = await conn();
    expect(await P.publish(c, "K")).toBe("rejected");
    await commit("N", "B", "n2");                                    // conflict resolved
    expect(await P.sharedState(db, "P:r1")).toEqual({ state: "ok", hash: "B" });
    expect(await P.isArtifactValid(db, "K")).toBe(false);            // same K stays dead
    await P.createArtifact(db, { id: "K2", occs: ["P:r1"], query: "Q", dest: "E" });
    expect(await P.isArtifactValid(db, "K2")).toBe(true);
  });
});
