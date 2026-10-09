// Job queue semantics (§5.5): a request that arrives while a run holds the job is never absorbed by that run.
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { claimJob, connect, enqueue, failJob, finishJob, migrate, type Db } from "./db.ts";

let db: Db;
beforeAll(async () => {
  db = connect();
  await migrate(db);
});
afterAll(async () => {
  await db.close();
});
beforeEach(async () => {
  await db`truncate job restart identity`;
});

const state = async () => (await db`select state, req, attempts from job order by id`).map((r: any) => `${r.state}:${r.req}`);

test("enqueue twice before a claim: one job; finish marks it done", async () => {
  await enqueue(db, "project", "s1");
  await enqueue(db, "project", "s1");
  const j = (await claimJob(db, ["project"]))!;
  expect(await claimJob(db, ["project"])).toBeUndefined();
  expect(await db.begin((tx) => finishJob(tx, j))).toBe(true);
  expect(await state()).toEqual(["done:2"]);
});

test("a request during a run sends the job back to queued", async () => {
  await enqueue(db, "project", "s1");
  const j = (await claimJob(db, ["project"]))!;
  await enqueue(db, "project", "s1"); // new input after the run read its input
  expect(await db.begin((tx) => finishJob(tx, j))).toBe(true);
  expect(await state()).toEqual(["queued:2"]);
  const j2 = (await claimJob(db, ["project"]))!;
  expect(await db.begin((tx) => finishJob(tx, j2))).toBe(true);
  expect(await state()).toEqual(["done:2"]);
});

test("a run whose lock lapsed and was re-claimed cannot finish or fail the job", async () => {
  process.env.MEMEX_JOB_LOCK_MS = "1";
  await enqueue(db, "project", "s1");
  const j = (await claimJob(db, ["project"]))!;
  await db`update job set locked_until = clock_timestamp() - interval '1 second'`;
  const j2 = (await claimJob(db, ["project"]))!;
  expect(j2.id).toBe(j.id);
  expect(await db.begin((tx) => finishJob(tx, j))).toBe(false);
  await failJob(db, j, new Error("stale"));
  expect((await db`select state, error from job`)[0]).toEqual({ state: "running", error: null });
  expect(await db.begin((tx) => finishJob(tx, j2))).toBe(true);
});

test("failure backs off; a done job is not reused by a new request", async () => {
  await enqueue(db, "project", "s1");
  const j = (await claimJob(db, ["project"]))!;
  await failJob(db, j, new Error("x"));
  expect(await claimJob(db, ["project"])).toBeUndefined(); // not_before in the future
  await db`update job set not_before = now()`;
  const j2 = (await claimJob(db, ["project"]))!;
  await db.begin((tx) => finishJob(tx, j2));
  await enqueue(db, "project", "s1");
  expect(await state()).toEqual(["done:1", "queued:1"]);
});
