// HTTP API (Bun.serve). Every route needs `Authorization: Bearer <api key>`; keys are stored as sha256.
// Bodies are JSON, at most MAX_BODY bytes. Errors: {error} with a status; no request text is logged.
// A route may return `Guarded`: a response derived from published state (search, zoom, grep). It is written
// only after its version vector is compared under the shared barrier, and the barrier is held until the
// body is handed to the transport (DESIGN §5.5 "对外响应"); a stale result is recomputed.
//
// The hand-over point. Bun's `res.end` callback (node:http) and `flush()` resolve once Bun has the bytes,
// before the socket takes them (measured: 64 MB "sent" in 55 ms to a client not reading). A pull-mode
// body stream with highWaterMark 0 is asked for its next chunk only after the previous one went to the
// socket (measured: pulls stop while the client does not read, resume when it does). So the body goes out
// in CHUNK pieces and is handed over when the stream is asked for more after the last piece; at most one
// CHUNK can still be in Bun's buffer then. Past the deadline the stream errors and the connection closes.
import type { Db, Tx } from "./db.ts";
import { blockRecords, BARRIER, commit, HttpError, manifest, markItem, nextItems, openRound, releaseLease, requestLease, scanStatus,
         tryGrant } from "./sync.ts";
import { unchanged } from "./derive.ts";
const CHUNK = 16 * 1024;

export const MAX_BODY = 10 * 1024 * 1024;
export const DELIVER_DEADLINE_MS = 5_000; // lock wait + comparison + handing the body to the transport
const GUARD_TRIES = 3;
/** A response valid while `deps` hold; `again` recomputes it from a newer snapshot. */
export class Guarded {
  /** `until` (epoch ms): an absolute deadline the answer must be handed over by (a recall's); past it, `late`
   *  is answered instead. `again` recomputes after a withdrawal and may answer a plain body. */
  constructor(readonly deps: Record<string, number>, readonly body: unknown, readonly again: () => Promise<Guarded | object>,
              readonly until?: number, readonly late?: object) {}
}
/** Test seam: runs after the comparison, before the body is written (where a writer must wait). */
export const httpHooks = { beforeGuard: async () => {}, beforeDeliver: async () => {}, beforePiece: async () => {} };
export type Route = (db: Db, body: any, params: string[], url: URL) => Promise<unknown>;
const routes: [string, RegExp, Route][] = [];
export function route(method: string, pattern: RegExp, fn: Route) {
  routes.push([method, pattern, fn]);
}

const str = (v: unknown, name: string, max = 256) => {
  if (typeof v !== "string" || !v || v.length > max) throw new HttpError(400, `${name} must be a non-empty string`);
  return v;
};
const leaseId = (p: string[]) => Number(p[0]);

route("POST", /^\/v1\/sync\/lease$/, async (db, b) => ({
  lease: await requestLease(
    db,
    {
      source: str(b.source, "source", 64),
      harness: b.harness === "pi" || b.harness === "codex" ? b.harness : (() => { throw new HttpError(400, "harness"); })(),
      nativeId: str(b.nativeId, "nativeId"),
      scope: str(b.scope, "scope", 64),
      parent: b.parent ?? null,
      meta: typeof b.meta === "object" && b.meta ? b.meta : {},
    },
    str(b.collector, "collector", 128),
  ),
}));
route("POST", /^\/v1\/sync\/lease\/(\d+)\/poll$/, async (db, _b, p) => tryGrant(db, leaseId(p)));
route("POST", /^\/v1\/sync\/lease\/(\d+)\/release$/, async (db, _b, p) => (await releaseLease(db, leaseId(p)), { ok: true }));
route("GET", /^\/v1\/sync\/lease\/(\d+)\/manifest$/, async (db, _b, p) => manifest(db, leaseId(p)));
route("POST", /^\/v1\/sync\/lease\/(\d+)\/blocks$/, async (db, b, p) =>
  blockRecords(db, leaseId(p), Array.isArray(b.blocks) ? b.blocks.filter((x: unknown) => Number.isInteger(x) && (x as number) >= 0) : []));
route("POST", /^\/v1\/sync\/lease\/(\d+)\/commit$/, async (db, b, p) =>
  commit(db, {
    leaseId: leaseId(p),
    commitId: str(b.commitId, "commitId", 64),
    length: b.length,
    changes: Array.isArray(b.changes) ? b.changes : [],
    scan: b.scan && typeof b.scan === "object" ? b.scan : undefined,
    leaf: b.leaf === undefined ? undefined : b.leaf === null ? null : String(b.leaf),
  }));

// Batched forms of the lease steps (DESIGN §5.4: an import does not take one round trip per file). Each entry
// runs exactly as its single route, in its own transaction; one entry's error does not stop the others.
const MAX_BATCH = 64;
const each = async (list: unknown, fn: (x: any) => Promise<unknown>) => {
  if (!Array.isArray(list) || list.length > MAX_BATCH) throw new HttpError(400, `a list of at most ${MAX_BATCH}`);
  const out: unknown[] = [];
  for (const x of list)
    try {
      out.push(await fn(x));
    } catch (e) {
      if (!(e instanceof HttpError)) throw e;
      out.push({ error: `${e.status} ${e.message}` });
    }
  return { results: out };
};
const leaseRoute = routes.find(([m, re]) => m === "POST" && re.source === "^\\/v1\\/sync\\/lease$")![2];
const commitRoute = routes.find(([m, re]) => m === "POST" && re.source === "^\\/v1\\/sync\\/lease\\/(\\d+)\\/commit$")![2];
route("POST", /^\/v1\/sync\/batch\/lease$/, async (db, b) => each(b.requests, (x) => leaseRoute(db, x ?? {}, [], null as any)));
route("POST", /^\/v1\/sync\/batch\/poll$/, async (db, b) => each(b.leases, (id) => tryGrant(db, Number(id))));
route("POST", /^\/v1\/sync\/batch\/release$/, async (db, b) => each(b.leases, async (id) => (await releaseLease(db, Number(id)), { ok: true })));
route("POST", /^\/v1\/sync\/batch\/blocks$/, async (db, b) =>
  each(b.requests, (x) => blockRecords(db, Number(x?.lease), Array.isArray(x?.blocks) ? x.blocks.filter((y: unknown) => Number.isInteger(y) && (y as number) >= 0) : [])));
route("POST", /^\/v1\/sync\/batch\/commit$/, async (db, b) => each(b.commits, (x) => commitRoute(db, x?.body ?? {}, [String(x?.lease)], null as any)));

route("POST", /^\/v1\/scan\/open$/, async (db, b) => openRound(db, b));
route("POST", /^\/v1\/scan\/next$/, async (db, b) => nextItems(db, b));
route("POST", /^\/v1\/scan\/skip$/, async (db, b) => markItem(db, b, "skip"));
route("POST", /^\/v1\/scan\/fail$/, async (db, b) => markItem(db, b, "fail"));
route("GET", /^\/v1\/scan\/status$/, async (db, _b, _p, url) => scanStatus(db, str(url.searchParams.get("collector"), "collector", 128)));

route("POST", /^\/v1\/search$/, async (db, b) => (await import("./search.ts")).search(db, b));
route("POST", /^\/v1\/zoom$/, async (db, b) => (await import("./search.ts")).zoom(db, b));
route("POST", /^\/v1\/grep$/, async (db, b) => (await import("./search.ts")).grep(db, b));
route("POST", /^\/v1\/snapshot$/, async (db, b) => (await import("./snapshot.ts")).startSnapshot(db, b));
route("POST", /^\/v1\/recall$/, async (db, b) => (await import("./recall.ts")).recall(db, b));
route("POST", /^\/v1\/recall\/blocks$/, async (db, b) => (await import("./recall.ts")).blocks(db, b));
route("POST", /^\/v1\/forget$/, async (db, b) =>
  (await import("./forget.ts")).forgetSource(db, str(b.source, "source", 64)));

const sha = (s: string) => new Bun.CryptoHasher("sha256").update(s).digest("hex");
export async function addApiKey(db: Db, name: string): Promise<string> {
  const key = "mx_" + Buffer.from(crypto.getRandomValues(new Uint8Array(24))).toString("base64url");
  await db`insert into api_key (hash, name) values (${sha(key)}, ${name})`;
  return key;
}

const json = (status: number, body: unknown) => Response.json(body, { status });

/** A guarded response: compare under the shared barrier, stream the body out, release once handed over.
 *  One deadline covers the lock wait, the comparison and the hand-over. */
async function deliver(db: Db, g: Guarded): Promise<Response> {
  const lateOr = (e: unknown) => {
    if (g.until !== undefined && Date.now() >= g.until && g.late) return json(200, g.late);
    throw e;
  };
  for (let i = 0; i < GUARD_TRIES; i++) {
    await httpHooks.beforeGuard();
    if (g.until !== undefined && Date.now() >= g.until) return lateOr(new HttpError(503, "busy; retry"));
    let respond!: (r: Response) => void;
    const response = new Promise<Response>((ok) => (respond = ok));
    const held = db.begin(async (tx: Tx) => {
      const until = Math.min(Date.now() + DELIVER_DEADLINE_MS, g.until ?? Infinity);
      if (until <= Date.now()) throw new Error("deadline passed");
      let timer: ReturnType<typeof setTimeout> | undefined;
      const left = () => Math.max(1, until - Date.now()) + "ms";
      await tx`select set_config('lock_timeout', ${left()}, true), set_config('statement_timeout', ${left()}, true)`;
      await tx`select pg_advisory_xact_lock_shared(${BARRIER})`;
      await tx`select set_config('statement_timeout', ${left()}, true)`;
      if (!(await unchanged(tx, g.deps))) return false;
      await httpHooks.beforeDeliver();
      if (Date.now() >= until) throw new Error("deadline passed"); // nothing handed over past the deadline
      const data = Buffer.from(JSON.stringify(g.body));
      await new Promise<void>((done, fail) => {
        let at = 0;
        let ctl!: ReadableStreamDefaultController<Uint8Array>;
        const stop = (e: Error) => (clearTimeout(timer), fail(e));
        timer = setTimeout(() => {
          try { ctl.error(new Error("deliver deadline")); } catch {}
          stop(new Error("deliver deadline"));
        }, Math.max(1, until - Date.now()));
        const body = new ReadableStream<Uint8Array>({
          start: (c) => void (ctl = c),
          pull: async (c) => {
            await httpHooks.beforePiece();
            // The deadline is checked on every piece, not left to a timer (a stalled event loop runs pulls first).
            if (Date.now() >= until) {
              try { c.error(new Error("deliver deadline")); } catch {}
              return stop(new Error("deliver deadline"));
            }
            if (at >= data.length) {
              clearTimeout(timer);
              c.close();
              done(); // every piece went to the socket (the last one may still be in Bun's buffer)
              return;
            }
            c.enqueue(data.subarray(at, (at += CHUNK)));
          },
          cancel: () => stop(new Error("client gone")),
        }, { highWaterMark: 0 });
        respond(new Response(body, { headers: { "content-type": "application/json", "content-length": String(data.length) } }));
      });
      return true;
    });
    // The wait for a connection is bounded by the deadline too.
    let tm: ReturnType<typeof setTimeout> | undefined;
    const late = g.until === undefined ? null : new Promise<never>((_, bad) => (tm = setTimeout(() => bad(new Error("deadline passed")), Math.max(1, g.until! - Date.now()))));
    held.catch(() => {});
    let out: Response | "held" | "stale";
    try {
      out = await Promise.race([held.then((ok) => (ok ? ("held" as const) : ("stale" as const))), response, ...(late ? [late] : [])]);
    } catch (e) {
      if (g.until !== undefined && Date.now() >= g.until && g.late) return json(200, g.late);
      throw e instanceof HttpError ? e : new HttpError(503, "busy; retry");
    } finally {
      clearTimeout(tm);
    }
    if (out instanceof Response) {
      held.catch(() => {}); // the hand-over failed after the head went out: the stream already errored
      return out;
    }
    if (out === "stale") {
      const next = await g.again();
      if (!(next instanceof Guarded)) return json(200, next);
      g = next;
      continue;
    }
  }
  throw new HttpError(503, "state kept changing; retry");
}

export function serve(db: Db, opts: { port: number; hostname?: string }) {
  const server = Bun.serve({
    port: opts.port,
    hostname: opts.hostname ?? "127.0.0.1",
    maxRequestBodySize: MAX_BODY,
    async fetch(req) {
      const url = new URL(req.url);
      try {
        if (url.pathname === "/healthz") // the database too: a server that cannot reach it is not healthy
          return await db`select 1`.then(() => json(200, { ok: true }), () => json(503, { ok: false, error: "database unavailable" }));
        const m = (req.headers.get("authorization") ?? "").match(/^Bearer (\S+)$/);
        const [k] = m ? await db`select 1 from api_key where hash = ${sha(m[1])} and not revoked` : [];
        if (!k) return json(401, { error: "unauthorized" });
        const found = routes.find(([method, re]) => method === req.method && re.test(url.pathname));
        if (!found) return json(404, { error: "not found" });
        const text = req.method === "GET" ? "" : await req.text();
        let body: any = {};
        if (text) try { body = JSON.parse(text); } catch { throw new HttpError(400, "body must be JSON"); }
        const out = await found[2](db, body ?? {}, url.pathname.match(found[1])!.slice(1), url);
        return out instanceof Guarded ? await deliver(db, out) : json(200, out);
      } catch (e) {
        if (e instanceof HttpError) return json(e.status, { error: e.message });
        console.error(`${req.method} ${url.pathname}: ${(e as Error).name}: ${(e as Error).message?.slice(0, 200)}`);
        return json(500, { error: "internal" });
      }
    },
  });
  return Promise.resolve({ port: server.port as number, stop: async () => void (await server.stop(true)) });
}

/** Client transport (client/api.ts), re-exported for the server tests. */
export { transport } from "../client/api.ts";
