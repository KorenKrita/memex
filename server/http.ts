// HTTP API (node:http). Every route needs `Authorization: Bearer <api key>`; keys are stored as sha256.
// Bodies are JSON, at most MAX_BODY bytes. Errors: {error} with a status; no request text is logged.
// A route may return `Guarded`: a response derived from published state (search, zoom, grep). It is written
// only after its version vector is compared under the shared barrier, and the barrier is held until the
// body is handed to the transport (DESIGN §5.5 "对外响应"); a stale result is recomputed.
import http from "node:http";
import type { Db, Tx } from "./db.ts";
import { blockRecords, BARRIER, commit, HttpError, manifest, releaseLease, requestLease, tryGrant } from "./sync.ts";
import { unchanged } from "./derive.ts";

export const MAX_BODY = 10 * 1024 * 1024;
export const DELIVER_DEADLINE_MS = 5_000; // lock wait + comparison + handing the body to the transport
const GUARD_TRIES = 3;
/** A response valid while `deps` hold; `again` recomputes it from a newer snapshot. */
export class Guarded {
  constructor(readonly deps: Record<string, number>, readonly body: unknown, readonly again: () => Promise<Guarded>) {}
}
/** Test seam: runs after the comparison, before the body is written (where a writer must wait). */
export const httpHooks = { beforeGuard: async () => {}, beforeDeliver: async () => {} };
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
    scanChunks: Number.isInteger(b.scanChunks) ? b.scanChunks : undefined,
    leaf: b.leaf === undefined ? undefined : b.leaf === null ? null : String(b.leaf),
  }));

route("POST", /^\/v1\/search$/, async (db, b) => (await import("./search.ts")).search(db, b));

const sha = (s: string) => new Bun.CryptoHasher("sha256").update(s).digest("hex");
export async function addApiKey(db: Db, name: string): Promise<string> {
  const key = "mx_" + Buffer.from(crypto.getRandomValues(new Uint8Array(24))).toString("base64url");
  await db`insert into api_key (hash, name) values (${sha(key)}, ${name})`;
  return key;
}

const send = (res: http.ServerResponse, status: number, body: unknown) => {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { "content-type": "application/json", "content-length": data.length });
  return data;
};

/** Write a guarded response: compare under the shared barrier, hand the body over, then release. */
async function deliver(db: Db, res: http.ServerResponse, g: Guarded) {
  for (let i = 0; i < GUARD_TRIES; i++) {
    await httpHooks.beforeGuard();
    const done = await db.begin(async (tx: Tx) => {
      await tx`select set_config('lock_timeout', ${DELIVER_DEADLINE_MS + "ms"}, true), set_config('statement_timeout', ${DELIVER_DEADLINE_MS + "ms"}, true)`;
      await tx`select pg_advisory_xact_lock_shared(${BARRIER})`;
      if (!(await unchanged(tx, g.deps))) return false;
      await httpHooks.beforeDeliver();
      const data = send(res, 200, g.body);
      // Handed over once the transport has taken the body; past the deadline the response is cancelled.
      await new Promise<void>((ok, bad) => {
        const t = setTimeout(() => (res.destroy(), bad(new Error("deliver deadline"))), DELIVER_DEADLINE_MS);
        res.end(data, () => (clearTimeout(t), ok()));
      });
      return true;
    });
    if (done) return;
    g = await g.again();
  }
  throw new HttpError(503, "state kept changing; retry");
}

export function serve(db: Db, opts: { port: number; hostname?: string }): Promise<{ port: number; stop: () => Promise<void> }> {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://x");
    try {
      if (url.pathname === "/healthz") return void res.end(send(res, 200, { ok: true }));
      const m = (req.headers.authorization ?? "").match(/^Bearer (\S+)$/);
      const [k] = m ? await db`select 1 from api_key where hash = ${sha(m[1])} and not revoked` : [];
      if (!k) return void res.end(send(res, 401, { error: "unauthorized" }));
      const found = routes.find(([method, re]) => method === req.method && re.test(url.pathname));
      if (!found) return void res.end(send(res, 404, { error: "not found" }));
      const parts: Buffer[] = [];
      let size = 0;
      for await (const c of req) {
        size += c.length;
        if (size > MAX_BODY) throw new HttpError(413, "body too large");
        parts.push(c);
      }
      const text = Buffer.concat(parts).toString();
      let body: any = {};
      if (text) try { body = JSON.parse(text); } catch { throw new HttpError(400, "body must be JSON"); }
      const out = await found[2](db, body ?? {}, url.pathname.match(found[1])!.slice(1), url);
      if (out instanceof Guarded) await deliver(db, res, out);
      else res.end(send(res, 200, out));
    } catch (e) {
      if (res.headersSent) return void res.destroy();
      if (e instanceof HttpError) return void res.end(send(res, e.status, { error: e.message }));
      console.error(`${req.method} ${url.pathname}: ${(e as Error).name}: ${(e as Error).message?.slice(0, 200)}`);
      res.end(send(res, 500, { error: "internal" }));
    }
  });
  server.requestTimeout = 60_000;
  return new Promise((ok) =>
    server.listen(opts.port, opts.hostname ?? "127.0.0.1", () =>
      ok({ port: (server.address() as any).port, stop: () => new Promise<void>((done) => (server.closeAllConnections(), server.close(() => done()))) })));
}

/** Client transport for tests and plugins: JSON over fetch with the API key. */
export function transport(base: string, key: string, f: typeof fetch = fetch) {
  return async (method: string, path: string, body?: unknown) => {
    const r = await f(base + path, {
      method,
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: method === "GET" ? undefined : JSON.stringify(body ?? {}),
    });
    const j: any = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${j.error ?? ""}`.trim());
    return j;
  };
}
