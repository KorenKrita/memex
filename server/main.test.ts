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
  expect(Date.now() - at).toBeLessThan(600); // budget 200 ms, plus the close of the API and pools
  expect(await q).toBe("cut");
  delete process.env.MEMEX_STOP_MS;
});

test("/healthz answers 503 in time when the database stops answering, with one query in flight however often asked", async () => {
  const { serve } = await import("./http.ts");
  let issued = 0;
  const db: any = () => (issued++, new Promise(() => {})); // an established connection that never answers
  const api = await serve(db, { port: 0 });
  for (let i = 0; i < 3; i++) {
    const at = Date.now();
    const r = await fetch(`http://127.0.0.1:${api.port}/healthz`, { signal: AbortSignal.timeout(6_000) });
    expect(r.status).toBe(503);
    expect(Date.now() - at).toBeLessThan(2_500);
  }
  expect(issued).toBe(1);
  await api.stop();
});

// A TCP proxy to the test database that can go silent (sockets stay open, nothing passes either way: a
// network black hole, a frozen host) or refuse (every socket closed, new ones closed at once: Postgres down).
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
      },
      data(c, d) {
        if (mode !== "pass") return;
        c.data.peer ? c.data.peer.write(d) : c.data.queue.push(new Uint8Array(d));
      },
      close: (c) => (open.delete(c), void c.data.peer?.end()),
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
test("a busy application pool is not a silent database: /healthz answers from its own connection", async () => {
  const p = Bun.spawn(["bun", import.meta.dir + "/main.ts"], { env: { ...process.env, MEMEX_PORT: "0" }, stdout: "pipe", stderr: "pipe" });
  try {
    await Bun.sleep(1_500);
    const port = Number((await (async () => {
      // the listening port is printed on stdout
      const reader = p.stdout.getReader();
      const { value } = await reader.read();
      reader.releaseLock();
      return new TextDecoder().decode(value).match(/:(\d+)/)![1];
    })()));
    // Hold all 8 application connections: with api_key locked from outside, every authenticated request waits
    // on its key lookup, each on a pool connection. The database itself answers all along.
    const { connect } = await import("./db.ts");
    const admin = connect();
    const tx = admin.begin(async (t: any) => {
      await t`lock table api_key in access exclusive mode`;
      await Bun.sleep(4_000);
    });
    await Bun.sleep(200);
    const blocked = Array.from({ length: 12 }, () =>
      fetch(`http://127.0.0.1:${port}/v1/grep`, { method: "POST", headers: { authorization: "Bearer x" }, body: "{}" }).catch(() => null));
    await Bun.sleep(1_000);
    const h = await fetch(`http://127.0.0.1:${port}/healthz`).then((r) => r.status);
    expect(h).toBe(200); // the probe's own connection
    await tx;
    expect(p.exitCode).toBe(null);
    await Promise.all(blocked);
    await admin.close();
  } finally {
    p.kill();
  }
}, 45_000);

const spawnMain = (env: Record<string, string>) => {
  const p = Bun.spawn(["bun", import.meta.dir + "/main.ts"], { env: { ...process.env, MEMEX_PORT: "0", ...env }, stdout: "pipe", stderr: "pipe" });
  const err: string[] = [];
  void (async () => { for await (const c of p.stderr) err.push(new TextDecoder().decode(c)); })();
  const port = (async () => {
    const reader = p.stdout.getReader();
    const n = Number(new TextDecoder().decode((await reader.read()).value).match(/:(\d+)/)![1]);
    reader.releaseLock();
    return n;
  })();
  return { p, err, port };
};

test("database silent while requests keep coming: API requests answer 503 without queries, the process exits within its limit", async () => {
  // Under Bun 1.4.2, queries submitted to a silent connection keep it from idling out: nothing ever settles
  // as long as traffic continues (measured: 41 queries over 40 s, none settled).
  const proxy = pgProxy();
  const { p, err, port } = spawnMain({ MEMEX_PG_URL: proxy.url, MEMEX_DB_STUCK_MS: "10000" });
  try {
    const base = `http://127.0.0.1:${await port}`;
    expect((await fetch(base + "/healthz")).status).toBe(200);
    proxy.set("silent");
    const at = Date.now();
    const statuses: number[] = [];
    let gated = 0;
    while (p.exitCode === null && Date.now() - at < 20_000) {
      void fetch(base + "/v1/grep", { method: "POST", headers: { authorization: "Bearer x" }, body: "{}", signal: AbortSignal.timeout(1_000) })
        .then((r) => (statuses.push(r.status), r.status === 503 && gated++), () => {});
      await Bun.sleep(500);
    }
    expect(await p.exited).toBe(1);
    const took = Date.now() - at;
    expect(took).toBeGreaterThan(9_000); // not before the limit
    expect(took).toBeLessThan(15_000);
    expect(gated).toBeGreaterThan(5); // once health missed its deadline, requests were turned away at once
    expect(err.join("")).toContain("has not answered");
  } finally {
    p.kill();
    proxy.stop();
  }
}, 40_000);

test("Postgres refusing connections past the limit while requests keep coming: the process stays", async () => {
  const proxy = pgProxy();
  const { p, port } = spawnMain({ MEMEX_PG_URL: proxy.url, MEMEX_DB_STUCK_MS: "7000" }); // above the 5 s connect timeout
  try {
    const base = `http://127.0.0.1:${await port}`;
    expect((await fetch(base + "/healthz")).status).toBe(200);
    proxy.set("refuse");
    for (const at = Date.now(); Date.now() - at < 20_000; await Bun.sleep(500))
      void fetch(base + "/v1/grep", { method: "POST", headers: { authorization: "Bearer x" }, body: "{}", signal: AbortSignal.timeout(1_000) }).catch(() => {});
    expect(p.exitCode).toBe(null); // a refusal is an answer, not silence
    proxy.set("pass");
    let ok = false;
    for (let i = 0; i < 30 && !ok; i++) ok = (await fetch(base + "/healthz").then((r) => r.status)) === 200 || (await Bun.sleep(500), false);
    expect(ok).toBe(true);
  } finally {
    p.kill();
    proxy.stop();
  }
}, 60_000);

test("SIGTERM with a health query in flight on a silent database: the process exits within MEMEX_STOP_MS", async () => {
  const proxy = pgProxy();
  const { p, port } = spawnMain({ MEMEX_PG_URL: proxy.url, MEMEX_STOP_MS: "300" });
  try {
    const base = `http://127.0.0.1:${await port}`;
    expect((await fetch(base + "/healthz")).status).toBe(200);
    proxy.set("silent");
    expect((await fetch(base + "/healthz")).status).toBe(503); // its query is in flight, never answered
    const at = Date.now();
    p.kill("SIGTERM");
    await p.exited;
    expect(Date.now() - at).toBeLessThan(1_500);
  } finally {
    p.kill();
    proxy.stop();
  }
}, 30_000);

