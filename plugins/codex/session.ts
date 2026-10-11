// Codex session lookup shared by the hook and the MCP server.
import { existsSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import { readHead } from "../../client/records.ts";

/** The repository a directory belongs to: the name of the closest ancestor holding `.git`. */
export function repoOf(cwd: string): string | undefined {
  for (let d = cwd; ; d = dirname(d)) {
    if (existsSync(join(d, ".git"))) return basename(d);
    if (dirname(d) === d) return undefined;
  }
}

/** The rollout of thread `id` under CODEX_HOME/sessions (YYYY/MM/DD/rollout-…-<id>.jsonl), verified by its
 *  session_meta id. Undefined when none or more than one file claims it. */
export function rolloutOf(id: string, env = process.env): string | undefined {
  if (!/^[0-9a-zA-Z-]{8,64}$/.test(id)) return undefined;
  const root = join(env.CODEX_HOME ?? join(homedir(), ".codex"), "sessions");
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
      else if (n.startsWith("rollout-") && n.endsWith(`-${id}.jsonl`)) found.push(p);
    }
  };
  walk(root, 0);
  const ok = found.filter((p) => {
    const h = readHead(p, "codex");
    return h.ok && h.nativeId === id;
  });
  return ok.length === 1 ? ok[0] : undefined;
}
