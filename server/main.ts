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
  return {
    db, port: api.port,
    close: async () => {
      stop.abort();
      // A job in flight (a model call can take MEMEX_MODEL_TIMEOUT_MS) gets a few seconds to finish; past that the
      // process goes anyway: nothing is published without its commit, and the job's lock lapses and it runs again.
      await Promise.race([worker, Bun.sleep(Number(process.env.MEMEX_STOP_MS ?? 5_000))]);
      await api.stop();
      await db.close();
    },
  };
}

if (import.meta.main) {
  const s = await start();
  console.log(`memex listening on ${process.env.MEMEX_HOST ?? "127.0.0.1"}:${s.port}`);
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => void s.close().then(() => process.exit(0)));
}
