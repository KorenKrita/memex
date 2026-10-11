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

// A TCP proxy to the test database that can go silent: sockets stay open, nothing passes either way (a
// network black hole, a frozen host).
const PG = new URL(process.env.MEMEX_PG_URL ?? "postgres://postgres:memex@127.0.0.1:55433/memex");
function pgProxy() {
  let mode: "pass" | "silent" = "pass";
  const server = Bun.listen<{ peer?: any; queue: Uint8Array[] }>({
    hostname: "127.0.0.1", port: 0,
    socket: {
      async open(c) {
        c.data = { queue: [] };
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
      close: (c) => void c.data.peer?.end(),
    },
  });
  const url = new URL(PG);
  url.host = `127.0.0.1:${server.port}`;
  return {
    url: url.toString(),
    set: (m: typeof mode) => void (mode = m),
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

test("database silent on open connections, default settings: queries fail after the idle timeout, the same process recovers", async () => {
  const { syncFile } = await import("../client/sync.ts");
  const { transport } = await import("../client/api.ts");
  const { sourceIdOf } = await import("../client/records.ts");
  const { addApiKey } = await import("./http.ts");
  const { allowGrant } = await import("./derive.ts");
  const { connect } = await import("./db.ts");
  const { mkdtempSync, writeFileSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const admin = connect();
  const key = await addApiKey(admin, "silent-" + Date.now());
  await allowGrant(admin, "personal", "sync", "memex");
  await allowGrant(admin, "personal", "tool", "memex");
  const proxy = pgProxy();
  const p = Bun.spawn(["bun", import.meta.dir + "/main.ts"], {
    env: { ...process.env, MEMEX_PG_URL: proxy.url, MEMEX_PORT: "0", MEMEX_IDLE_CLOSE_MS: "0" },
    stdout: "pipe", stderr: "pipe",
  });
  try {
    const err: string[] = [];
    void (async () => { for await (const c of p.stderr) err.push(new TextDecoder().decode(c)); })();
    const reader = p.stdout.getReader();
    const port = Number(new TextDecoder().decode((await reader.read()).value).match(/:(\d+)/)![1]);
    const base = `http://127.0.0.1:${port}`;
    expect((await fetch(base + "/healthz")).status).toBe(200);
    proxy.set("silent");
    // Several health checks, some at once: each answers 503 in time, one probe query in flight.
    const at = Date.now();
    const hs = await Promise.all([1, 2, 3].map(() => fetch(base + "/healthz").then((r) => r.status)));
    expect(hs).toEqual([503, 503, 503]);
    expect(Date.now() - at).toBeLessThan(2_500);
    await Bun.sleep(33_000); // past Bun's 30 s idle timeout: the silent connections are given up
    proxy.set("pass");
    // The same process: health back, and a new session synced and projected (its own record found by grep).
    let ok = false;
    for (let i = 0; i < 60 && !ok; i++) ok = (await fetch(base + "/healthz").then((r) => r.status)) === 200 || (await Bun.sleep(500), false);
    expect(ok).toBe(true);
    expect(p.exitCode).toBe(null);
    const dir = mkdtempSync(tmpdir() + "/memex-silent-");
    const id = "silent-" + Date.now();
    const file = `${dir}/${id}.jsonl`;
    writeFileSync(file, [JSON.stringify({ type: "session", version: 3, id, timestamp: "2026-10-11T00:00:00Z", cwd: "/tmp" }),
      JSON.stringify({ type: "message", id: "u1", parentId: null, timestamp: "2026-10-11T00:00:01Z", message: { role: "user", content: [{ type: "text", text: `after-silence-${id}` }] } })].join("\n") + "\n");
    const t = transport(base, key);
    const policy = { scopes: [{ name: "personal", paths: [dir], allow: { sync: ["memex"], tool: ["memex"] } }] };
    // A sync that began connecting while the database was still silent fails once (5 s connect timeout); a
    // plugin syncs again on its next trigger. Recovery: one succeeds within 20 s.
    let res: any;
    for (const s = Date.now(); Date.now() - s < 20_000; await Bun.sleep(1_000))
      if ((res = await syncFile(t, policy as any, file, "pi", { collector: "silent:pi" })).status === "synced") break;
    if (res.status !== "synced") console.log("sync:", JSON.stringify(res), "stderr:", err.join("").slice(-1500));
    expect(res.status).toBe("synced");
    let found = false;
    for (let i = 0; i < 60 && !found; i++) {
      const r: any = await t("POST", "/v1/grep", { source: sourceIdOf("pi", id), scope: "personal", purpose: "tool", pattern: `after-silence-${id}`, dests: ["memex"] });
      found = r.hits?.some((h: any) => h.in === sourceIdOf("pi", id)) || (await Bun.sleep(500), false);
    }
    expect(found).toBe(true);
  } finally {
    p.kill();
    proxy.stop();
    await admin.close();
  }
}, 120_000);
