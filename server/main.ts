// Server entry: every module that registers a migration is imported before migrate() runs, then the HTTP
// API and one worker loop start. Environment: MEMEX_PG_URL, MEMEX_PORT, MEMEX_HOST, model endpoints.
import { connect, migrate } from "./db.ts";
import "./project.ts";
import "./derive.ts";
import "./search.ts";
import "./forget.ts";
import "./forest.ts";
import "./snapshot.ts";
import { serve } from "./http.ts";
import { loop } from "./worker.ts";

export async function start(opts: { port?: number; hostname?: string } = {}) {
  const db = connect();
  await migrate(db);
  const api = await serve(db, { port: opts.port ?? Number(process.env.MEMEX_PORT ?? 8787), hostname: opts.hostname ?? process.env.MEMEX_HOST });
  const stop = new AbortController();
  const worker = loop(db, stop.signal);
  // Watchdog: a database that stops answering on open connections (a network black hole, a frozen host)
  // leaves queries waiting forever, which no error path sees. Three checks in a row without an answer
  // within 3 s: exit 1, and the restart policy brings a fresh process. An error (Postgres down, connection
  // refused) is an answer: the worker's backoff handles it.
  const every = Number(process.env.MEMEX_WATCHDOG_MS ?? 10_000);
  const watchdog = (async () => {
    // One check at a time: a silent database never piles up more than one waiting query here.
    for (let missed = 0; !stop.signal.aborted; ) {
      const answered = await Promise.race([db`select 1`.then(() => true, () => true), Bun.sleep(3_000).then(() => false)]);
      missed = answered ? 0 : missed + 1;
      if (missed >= 3 && !stop.signal.aborted) {
        console.error("memex: the database has not answered for 3 checks; exiting for a restart");
        process.exit(1);
      }
      await Bun.sleep(every);
    }
  })();
  return {
    db, port: api.port,
    close: async () => {
      stop.abort();
      void watchdog; // ends at its next check (stop is aborted)
      // One budget for the whole stop. A job in flight (a model call can take MEMEX_MODEL_TIMEOUT_MS) or a query
      // waiting on a lock may run into it; past it, open requests and connections are cut. Nothing is published
      // without its commit (a cut transaction rolls back), and an interrupted job's lock lapses and it runs again.
      const until = Date.now() + Number(process.env.MEMEX_STOP_MS ?? 5_000);
      await Promise.race([worker, Bun.sleep(until - Date.now())]);
      await api.stop(); // closes open connections too
      await db.close({ timeout: Math.max(0, until - Date.now()) / 1000 });
    },
  };
}

if (import.meta.main) {
  const s = await start();
  console.log(`memex listening on ${process.env.MEMEX_HOST ?? "127.0.0.1"}:${s.port}`);
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => void s.close().then(() => process.exit(0)));
}
