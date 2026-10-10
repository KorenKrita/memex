// Client side of the HTTP API, shared by the CLI and both plugins. No state: one function per request.
// Every request has a timeout (REQUEST_MS, or less when the caller's deadline is nearer), so a hung server
// cannot hold a hook past its budget.
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

/** Server settings from the environment: MEMEX_URL, MEMEX_KEY. Undefined when either is missing. */
export function fromEnv(env = process.env): Transport | undefined {
  return env.MEMEX_URL && env.MEMEX_KEY ? transport(env.MEMEX_URL.replace(/\/+$/, ""), env.MEMEX_KEY) : undefined;
}
