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
  // A database that goes silent on open connections needs nothing more here: Bun 1.4.2 gives up a connection
  // that stays without traffic for idleTimeout (30 s, db.ts) even with a query on it, so that query fails and
  // the worker's backoff and later requests reconnect (server/main.test.ts, "default settings").
  return {
    db, port: api.port,
    close: async () => {
      stop.abort();
      // One budget for the whole stop. A job in flight (a model call can take MEMEX_MODEL_TIMEOUT_MS) or a query
      // waiting on a lock may run into it; past it, open requests and connections are cut. Nothing is published
      // without its commit (a cut transaction rolls back), and an interrupted job's lock lapses and it runs again.
      const until = Date.now() + Number(process.env.MEMEX_STOP_MS ?? 5_000);
      await Promise.race([worker, Bun.sleep(until - Date.now())]);
      await api.stop(); // closes open connections too
      await db.close({ timeout: Math.max(0, until - Date.now()) / 1000 });
      await probeDb.close({ timeout: 0 });
    },
  };
}

if (import.meta.main) {
  const s = await start();
  console.log(`memex listening on ${process.env.MEMEX_HOST ?? "127.0.0.1"}:${s.port}`);
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => void s.close().then(() => process.exit(0)));
}
