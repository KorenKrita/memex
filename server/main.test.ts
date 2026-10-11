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

test("/healthz answers 503 within its deadline when the database stops answering", async () => {
  const { serve } = await import("./http.ts");
  const db: any = () => new Promise(() => {}); // an established connection that never answers
  const api = await serve(db, { port: 0 });
  const at = Date.now();
  const r = await fetch(`http://127.0.0.1:${api.port}/healthz`, { signal: AbortSignal.timeout(6_000) });
  expect(r.status).toBe(503);
  expect(Date.now() - at).toBeLessThan(4_000);
  await api.stop();
});

// A TCP proxy to the test database that can go silent: sockets stay open, nothing passes either way (a
// network black hole), or close everything and refuse (Postgres down).
const PG = new URL(process.env.MEMEX_PG_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex");
function pgProxy() {
  let mode: "pass" | "silent" | "refuse" = "pass";
  const open = new Set<any>();
  const server = Bun.listen<{ peer?: any; queue: Uint8Array[] }>({
    hostname: "127.0.0.1", port: 0,
    socket: {
      async open(c) {
        c.data = { queue: [] };
        if (mode === "refuse") return void c.end();
        open.add(c);
        c.data.peer = await Bun.connect({
          hostname: PG.hostname, port: Number(PG.port),
          socket: {
            data: (_, d) => void (mode === "pass" && c.write(d)),
            close: () => void c.end(),
          },
        });
        for (const d of c.data.queue) c.data.peer.write(d);
        open.add(c.data.peer);
      },
      data(c, d) {
        if (mode !== "pass") return;
        c.data.peer ? c.data.peer.write(d) : c.data.queue.push(new Uint8Array(d));
      },
      close: (c) => void c.data.peer?.end(),
    },
  });
  const url = new URL(PG);
  url.host = `127.0.0.1:${server.port}`;
  return {
    url: url.toString(),
    set(m: typeof mode) {
      mode = m;
      if (m === "refuse") for (const s of open) s.end();
    },
    stop: () => server.stop(true),
  };
}
const runMain = (url: string) =>
  Bun.spawn(["bun", import.meta.dir + "/main.ts"], {
    env: { ...process.env, MEMEX_PG_URL: url, MEMEX_PORT: "0", MEMEX_WATCHDOG_MS: "300" },
    stdout: "pipe", stderr: "pipe",
  });

test("database silent on open connections: the process exits for a restart; Postgres down: it stays", async () => {
  const proxy = pgProxy();
  const p = runMain(proxy.url);
  try {
    await Bun.sleep(1_500); // migrated, serving
    expect(p.exitCode).toBe(null);
    proxy.set("refuse"); // Postgres down: connections fail at once, an answer; the process waits it out
    await Bun.sleep(12_000); // long enough for 3 missed checks, were refusals counted as silence
    expect(p.exitCode).toBe(null);
    proxy.set("pass");
    await Bun.sleep(1_000);
    proxy.set("silent"); // connections open, nothing answers: 3 missed checks of 3 s each
    const at = Date.now();
    expect(await p.exited).toBe(1);
    expect(Date.now() - at).toBeLessThan(15_000);
    expect(await new Response(p.stderr).text()).toContain("has not answered for 3 checks");
  } finally {
    p.kill();
    proxy.stop();
  }
}, 45_000);
