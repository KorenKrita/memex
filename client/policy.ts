// Outbound authorization (DESIGN §7). Every request that carries session text — sync, start-of-session
// snapshot, per-turn recall, mem_* tool queries — is checked here BEFORE any network call, by
// (scope of the current session, purpose, destination). The default policy allows nothing.
//
// Policy file (~/.config/memex/policy.json, or MEMEX_POLICY):
//   { "scopes": [ { "name": "personal", "paths": ["/Users/me/.pi/agent/sessions/--Users-me-Coding--"],
//                   "allow": { "sync": ["memex"], "snapshot": ["memex"], "recall": ["memex", "embedding", "rerank"],
//                              "tool": ["memex", "embedding", "rerank"], "import": ["memex"] } } ] }
// "import": the scope may be imported by `memex import` (history, §5.4); live sync needs only "sync".
// A session belongs to the first scope one of whose `paths` is a prefix of the session file's real path.
// "memex" = the memex server itself; "embedding" / "rerank" = the external models the server would send the
// query text to (the server checks these again on its own grant table; the client only narrows).
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { join, sep } from "node:path";

export type Purpose = "sync" | "import" | "snapshot" | "recall" | "tool";
export type Dest = "memex" | "llm" | "embedding" | "rerank";
export type Scope = { name: string; paths: string[]; allow: Partial<Record<Purpose, Dest[]>> };
export type Policy = { scopes: Scope[] };

export const NONE = "unauthorized";
export const policyPath = () => process.env.MEMEX_POLICY ?? join(homedir(), ".config", "memex", "policy.json");

export function loadPolicy(path = policyPath()): Policy {
  if (!existsSync(path)) return { scopes: [] };
  const p = JSON.parse(readFileSync(path, "utf8"));
  if (!Array.isArray(p?.scopes)) throw new Error(`${path}: "scopes" must be a list`);
  for (const s of p.scopes) {
    if (typeof s.name !== "string" || !/^[a-z0-9][a-z0-9_-]{0,40}$/.test(s.name) || s.name === NONE) throw new Error(`${path}: bad scope name`);
    if (!Array.isArray(s.paths) || !s.paths.every((x: unknown) => typeof x === "string" && x.startsWith("/"))) throw new Error(`${path}: scope ${s.name}: paths must be absolute`);
  }
  return p as Policy;
}

const real = (p: string) => {
  try {
    return realpathSync(p);
  } catch {
    return p;
  }
};

/** Scope of a session file, or NONE. Prefix match on whole path components of the real path. */
export function scopeOf(policy: Policy, file: string): string {
  const f = real(file);
  for (const s of policy.scopes)
    for (const p of s.paths) {
      const r = real(p).replace(/\/+$/, "");
      if (f === r || f.startsWith(r + sep)) return s.name;
    }
  return NONE;
}

export function allowed(policy: Policy, scope: string, purpose: Purpose, dest: Dest): boolean {
  if (scope === NONE) return false;
  const s = policy.scopes.find((x) => x.name === scope);
  return !!s?.allow?.[purpose]?.includes(dest);
}

/** Destinations the server may use for this (scope, purpose), sent with query requests (§7). */
export const allowedDests = (policy: Policy, scope: string, purpose: Purpose): Dest[] =>
  scope === NONE ? [] : (policy.scopes.find((x) => x.name === scope)?.allow?.[purpose] ?? []).filter((d) => d !== "memex");
