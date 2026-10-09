// HTTP API (Bun.serve). Every route needs `Authorization: Bearer <api key>`; keys are stored as sha256.
// Bodies are JSON, at most MAX_BODY bytes. Errors: {error} with a status; no request text is logged.
import type { Db } from "./db.ts";
import { blockRecords, commit, HttpError, manifest, releaseLease, requestLease, tryGrant } from "./sync.ts";

export const MAX_BODY = 10 * 1024 * 1024;
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

const sha = (s: string) => new Bun.CryptoHasher("sha256").update(s).digest("hex");
export async function addApiKey(db: Db, name: string): Promise<string> {
  const key = "mx_" + Buffer.from(crypto.getRandomValues(new Uint8Array(24))).toString("base64url");
  await db`insert into api_key (hash, name) values (${sha(key)}, ${name})`;
  return key;
}

export function serve(db: Db, opts: { port: number; hostname?: string }) {
  return Bun.serve({
    port: opts.port,
    hostname: opts.hostname ?? "127.0.0.1",
    maxRequestBodySize: MAX_BODY,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/healthz") return Response.json({ ok: true });
      const auth = req.headers.get("authorization") ?? "";
      const m = auth.match(/^Bearer (\S+)$/);
      if (!m) return Response.json({ error: "unauthorized" }, { status: 401 });
      const [k] = await db`select 1 from api_key where hash = ${sha(m[1])} and not revoked`;
      if (!k) return Response.json({ error: "unauthorized" }, { status: 401 });
      for (const [method, re, fn] of routes) {
        if (method !== req.method) continue;
        const mm = url.pathname.match(re);
        if (!mm) continue;
        try {
          const text = req.method === "GET" ? "" : await req.text();
          let body: any = {};
          if (text) try { body = JSON.parse(text); } catch { throw new HttpError(400, "body must be JSON"); }
          return Response.json(await fn(db, body ?? {}, mm.slice(1), url));
        } catch (e) {
          if (e instanceof HttpError) return Response.json({ error: e.message }, { status: e.status });
          console.error(`${req.method} ${url.pathname}: ${(e as Error).name}: ${(e as Error).message?.slice(0, 200)}`);
          return Response.json({ error: "internal" }, { status: 500 });
        }
      }
      return Response.json({ error: "not found" }, { status: 404 });
    },
  });
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
