// Codex session lookup shared by the hook and the MCP server.
import { existsSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import { scopeOf, type Policy } from "../../client/policy.ts";
import { readHead, sourceIdOf } from "../../client/records.ts";

/** The repository a directory belongs to: the name of the closest ancestor holding `.git`. */
export function repoOf(cwd: string): string | undefined {
  for (let d = cwd; ; d = dirname(d)) {
    if (existsSync(join(d, ".git"))) return basename(d);
    if (dirname(d) === d) return undefined;
  }
}

/** The rollouts of thread `id` under CODEX_HOME/sessions (YYYY/MM/DD/rollout-<ts>-<id>.jsonl, and the
 *  `rollout-<ts>-<id>_<rollout id>.jsonl` a revert writes), each verified by its session_meta id. */
export function rolloutsOf(id: string, env = process.env): string[] {
  if (!/^[0-9a-zA-Z-]{8,64}$/.test(id)) return [];
  const root = join(env.CODEX_HOME ?? join(homedir(), ".codex"), "sessions");
  const name = new RegExp(`^rollout-.*-${id.replace(/-/g, "\\-")}(_[0-9a-zA-Z-]{8,64})?\\.jsonl$`);
  const found: string[] = [];
  const walk = (d: string, depth: number) => {
    let names: string[];
    try {
      names = readdirSync(d);
    } catch {
      return;
    }
    for (const n of names) {
      const p = join(d, n);
      if (depth < 3) walk(p, depth + 1);
      else if (name.test(n)) found.push(p);
    }
  };
  walk(root, 0);
  return found.filter((p) => {
    const h = readHead(p, "codex");
    return h.ok && h.nativeId === id;
  });
}

/** The session of thread `id` for the tools: its source and the policy scope of its rollouts — all of them in
 *  one scope (a thread is one source, whatever file holds it), else undefined (ambiguous: nothing sent). */
export function sessionOfThread(id: string, policy: Policy, env = process.env): { source: string; scope: string } | undefined {
  const files = rolloutsOf(id, env);
  if (!files.length) return undefined;
  const scopes = new Set(files.map((f) => scopeOf(policy, f)));
  if (scopes.size !== 1) return undefined;
  return { source: sourceIdOf("codex", id), scope: [...scopes][0] };
}
