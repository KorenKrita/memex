// Client side of the HTTP API, shared by the CLI and both plugins. No state: one function per request.
// Every request has a timeout (REQUEST_MS, or less when the caller's deadline is nearer), so a hung server
// cannot hold a hook past its budget.
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
export type Transport = (method: string, path: string, body?: unknown, o?: { deadline?: number }) => Promise<any>;
export const REQUEST_MS = Number(process.env.MEMEX_REQUEST_MS ?? 15_000);

/** JSON over fetch with the API key. Throws `<status> <error>` on an HTTP error, `timeout` past the deadline. */
export function transport(base: string, key: string, f: typeof fetch = fetch): Transport {
  return async (method, path, body, o) => {
    const ms = Math.max(1, Math.min(REQUEST_MS, o?.deadline !== undefined ? o.deadline - Date.now() : REQUEST_MS));
    let r: Response;
    try {
      r = await f(base + path, {
        method,
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: method === "GET" ? undefined : JSON.stringify(body ?? {}),
        signal: AbortSignal.timeout(ms),
      });
    } catch (e) {
      if ((e as Error)?.name === "TimeoutError" || (e as Error)?.name === "AbortError") throw new Error("timeout");
      throw e;
    }
    const j: any = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`${r.status} ${j.error ?? ""}`.trim());
    return j;
  };
}

export const clientPath = (env = process.env) => env.MEMEX_CLIENT ?? join(homedir(), ".config", "memex", "client.json");
/** Server settings: MEMEX_URL and MEMEX_KEY, else the client file (~/.config/memex/client.json, or MEMEX_CLIENT):
 *  { "url": "...", "key": "..." } — hooks run with a host daemon's environment, not the shell's. Undefined when
 *  neither gives both. */
export function fromEnv(env = process.env): Transport | undefined {
  let url = env.MEMEX_URL, key = env.MEMEX_KEY;
  if (!url || !key) {
    try {
      const c = JSON.parse(readFileSync(clientPath(env), "utf8"));
      url = typeof c?.url === "string" ? c.url : undefined;
      key = typeof c?.key === "string" ? c.key : undefined;
    } catch {
      return undefined;
    }
  }
  return url && key ? transport(url.replace(/\/+$/, ""), key) : undefined;
}
