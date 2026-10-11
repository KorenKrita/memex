// Shutdown is bounded as a whole: a query still running when the stop budget ends is cut, not waited for.
import { expect, test } from "bun:test";
import { start } from "./main.ts";

test("close() returns within MEMEX_STOP_MS even with a database query outstanding", async () => {
  process.env.MEMEX_STOP_MS = "200";
  const s = await start({ port: 0 });
  const q = s.db`select pg_sleep(3)`.catch(() => "cut");
  await Bun.sleep(100);
  const at = Date.now();
  await s.close();
  expect(Date.now() - at).toBeLessThan(1_000);
  expect(await q).toBe("cut");
  delete process.env.MEMEX_STOP_MS;
});
