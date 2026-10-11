// The worker loop outlives database errors (Postgres restarting): it waits, tries again, and goes on.
import { expect, test } from "bun:test";
import { loop } from "./worker.ts";

test("a database error in the loop is retried, not the end of the loop", async () => {
  let calls = 0;
  // claimJob: db`…${db(kinds)}…` — a tagged call (strings array first) is the query; a plain call a fragment.
  const db: any = (first: unknown) => {
    if (!(Array.isArray(first) && "raw" in first)) return first; // db(kinds): a fragment, not a query
    calls++;
    return calls <= 2 ? Promise.reject(new Error("the database system is shutting down")) : Promise.resolve([]); // then no job due
  };
  const stop = new AbortController();
  const run = loop(db, stop.signal, 5);
  await Bun.sleep(150);
  stop.abort();
  await run; // resolves: did not throw
  expect(calls).toBeGreaterThan(2);
});

test("stopping the loop during a long backoff ends it at once", async () => {
  const db: any = (first: unknown) =>
    Array.isArray(first) && "raw" in first ? Promise.reject(new Error("connection refused")) : first;
  const stop = new AbortController();
  const run = loop(db, stop.signal, 2_000); // the first backoff is 2 s, then 4 s…
  await Bun.sleep(50);
  const at = Date.now();
  stop.abort();
  await run;
  expect(Date.now() - at).toBeLessThan(200);
});
