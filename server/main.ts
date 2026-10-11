// Server entry: every module that registers a migration is imported before migrate() runs, then the HTTP
// API and one worker loop start. Environment: MEMEX_PG_URL, MEMEX_PORT, MEMEX_HOST, model endpoints.
import { connect, migrate } from "./db.ts";
import "./project.ts";
import "./derive.ts";
import "./search.ts";
import "./forget.ts";
import "./forest.ts";
import "./snapshot.ts";
import { prober, serve } from "./http.ts";
import { loop } from "./worker.ts";

export async function start(opts: { port?: number; hostname?: string } = {}) {
  const db = connect();
  await migrate(db);
  // The health probe has a connection of its own: a busy application pool is not a silent database.
  const probeDb = connect(undefined, 1);
  const health = prober(probeDb);
  const api = await serve(db, { port: opts.port ?? Number(process.env.MEMEX_PORT ?? 8787), hostname: opts.hostname ?? process.env.MEMEX_HOST, health });
  const stop = new AbortController();
  const worker = loop(db, stop.signal);
  // A database silent on open connections (a frozen host, a network black hole): under Bun 1.4.2 a query on such
  // a connection settles only once the connection has seen no traffic for idleTimeout, which further queries
  // keep postponing. So past the probe's deadline API requests are refused before any query (http.ts), and
  // when the one probe query has gone MEMEX_DB_STUCK_MS (10 s; above the 5 s connect timeout, after which a
  // refused connection does answer) without any answer, the process exits 1 for its supervisor to restart.
  const stuckLimit = Number(process.env.MEMEX_DB_STUCK_MS ?? 10_000);
  const watch = setInterval(() => {
    void health.check(); // keeps one probe going while the database is in doubt
    if (health.stuckMs() >= stuckLimit) {
      console.error(`memex: the database has not answered for ${Math.round(health.stuckMs() / 1000)} s; exiting for a restart`);
      process.exit(1);
    }
  }, 1_000);
  return {
    db, port: api.port,
    close: async () => {
      stop.abort();
      clearInterval(watch);
      // One budget for the whole stop. A job in flight (a model call can take MEMEX_MODEL_TIMEOUT_MS) or a query
      // waiting on a lock may run into it; past it, open requests and connections are cut. Nothing is published
      // without its commit (a cut transaction rolls back), and an interrupted job's lock lapses and it runs again.
      const until = Date.now() + Number(process.env.MEMEX_STOP_MS ?? 5_000);
      await Promise.race([worker, Bun.sleep(until - Date.now())]);
      await api.stop(); // closes open connections too
      // Bun 1.4.2: close({ timeout: 0 }) waits for queries in flight; any positive timeout cuts them (measured).
      const left = () => Math.max(1, until - Date.now()) / 1000;
      await Promise.all([db.close({ timeout: left() }), probeDb.close({ timeout: left() })]);
    },
  };
}

if (import.meta.main) {
  const s = await start();
  console.log(`memex listening on ${process.env.MEMEX_HOST ?? "127.0.0.1"}:${s.port}`);
  for (const sig of ["SIGINT", "SIGTERM"] as const)
    process.on(sig, () => {
      // Whatever close() runs into, the process is gone shortly after its budget.
      setTimeout(() => process.exit(0), Number(process.env.MEMEX_STOP_MS ?? 5_000) + 1_000).unref();
      void s.close().then(() => process.exit(0));
    });
}
