// memex command line (DESIGN §5.4): history import and scan status.
//   bun client/cli.ts import [--dry-run] [--since <days>]   import the scopes your policy allows to import
//   bun client/cli.ts status                                  this machine's reconciliation / import rounds
// Server: MEMEX_URL, MEMEX_KEY. Policy: ~/.config/memex/policy.json (MEMEX_POLICY). A scope is imported only
// if its `allow.import` lists "memex" (on top of `allow.sync`); `--dry-run` only counts locally (no network,
// no model) with the phase 0 statistics. The import is the reconciliation link's third entry (§5.2): an
// import round per collector (machine + harness), newest files first, resumable (progress is on the server).
import { statSync } from "node:fs";
import { hostname } from "node:os";
import { collect, dryRun } from "../dryrun/dryrun.ts";
import { fromEnv } from "./api.ts";
import { allowed, loadPolicy, scopeOf, type Policy } from "./policy.ts";
import { readHead, type Harness } from "./records.ts";
import { scanOnce, type ScanFile, type Transport } from "./sync.ts";

const PI_SUBAGENT = /(^|\/)subagent-artifacts\/[^/]*_transcript\.jsonl$/;
/** Collector of this machine for a harness: what the plugins report as well (§5.2: machine + host). */
export const collectorOf = (machine: string, harness: Harness) => `${machine}:${harness}`;

/** Harness of a log from its header line (whole line parsed: key order does not matter), or undefined. A
 *  Codex Legacy or child-thread file is still Codex: its round item is closed with that reason. */
function harnessOf(path: string): Harness | undefined {
  for (const h of ["pi", "codex"] as const) {
    const r = readHead(path, h);
    if (r.ok || r.reason === "legacy" || r.reason === "child-thread") return h;
  }
  return undefined;
}

/** Files to import: logs under the scopes allowed to import (and sync), modified since `since`, newest first.
 *  Discovery is the phase 0 one (dryrun::collect): regular files only, symlinks never followed, real paths. */
export function importPlan(policy: Policy, o: { since?: number } = {}) {
  const out: Record<Harness, (ScanFile & { mtime: number })[]> = { pi: [], codex: [] };
  const roots = policy.scopes.filter((s) => s.allow?.import?.includes("memex")).flatMap((s) => s.paths);
  const { files } = collect(roots);
  for (const p of files) {
    if (!p.endsWith(".jsonl") || PI_SUBAGENT.test(p)) continue;
    const scope = scopeOf(policy, p);
    if (!allowed(policy, scope, "import", "memex") || !allowed(policy, scope, "sync", "memex")) continue;
    let mtime: number;
    try {
      mtime = statSync(p).mtimeMs;
    } catch {
      continue;
    }
    if (o.since !== undefined && mtime < o.since) continue;
    const h = harnessOf(p);
    if (h) out[h].push({ path: p, harness: h, mtime });
  }
  for (const h of ["pi", "codex"] as const) out[h].sort((a, b) => b.mtime - a.mtime);
  return out;
}

/** `--dry-run`: the phase 0 statistics over the plan's files, read locally (no network, no model). */
export function importDryRun(plan: ReturnType<typeof importPlan>) {
  const paths = [...plan.pi, ...plan.codex].map((f) => f.path);
  const { perFile: _, ...stats } = paths.length ? dryRun(paths) : ({ perFile: [] } as any);
  return { files: paths.length, stats };
}

type Summary = { finished: boolean; total: number; done: number; skipped: number; pending: number };
/** Run the import rounds until each finishes, or makes no progress for `maxIdle` calls in a row (a file that
 *  keeps failing): what is left is reported, and the next run continues the same round. */
export async function importFiles(t: Transport, policy: Policy, plan: ReturnType<typeof importPlan>,
                                  o: { machine: string; log?: (s: string) => void; maxRecs?: number; maxIdle?: number }) {
  const log = o.log ?? (() => {});
  const res: Partial<Record<Harness, Summary>> = {};
  for (const h of ["pi", "codex"] as const) {
    if (!plan[h].length) continue;
    let idle = 0;
    for (;;) {
      const r: any = await scanOnce(t, policy, plan[h], { collector: collectorOf(o.machine, h), kind: "import", maxRecs: o.maxRecs });
      if (r.round === null) break;
      res[h] = { finished: r.finished, total: r.total, done: r.done, skipped: r.skipped, pending: r.pending };
      log(`${h}: ${r.done + r.skipped}/${r.total} (${r.skipped} skipped)`);
      if (r.finished) break;
      // Progress = accepted segments (a large file advances many segments before it is done).
      idle = r.advanced ? 0 : idle + 1;
      if (idle >= (o.maxIdle ?? 20)) break;
    }
  }
  return res;
}

if (import.meta.main) {
  const [cmd, ...args] = process.argv.slice(2);
  const t: Transport = fromEnv() ?? (async () => { throw new Error("MEMEX_URL and MEMEX_KEY"); });
  const machine = process.env.MEMEX_MACHINE ?? hostname();
  if (cmd === "import") {
    const days = args.includes("--since") ? Number(args[args.indexOf("--since") + 1]) : undefined;
    if (days !== undefined && !(days > 0)) throw new Error("--since <days>");
    const policy = loadPolicy();
    const plan = importPlan(policy, { since: days ? Date.now() - days * 86_400_000 : undefined });
    if (args.includes("--dry-run")) console.log(JSON.stringify(importDryRun(plan), null, 2));
    else {
      if (!(process.env.MEMEX_URL && process.env.MEMEX_KEY)) throw new Error("MEMEX_URL and MEMEX_KEY");
      console.log(JSON.stringify(await importFiles(t, policy, plan, { machine, log: (s) => console.error(s) }), null, 2));
    }
  } else if (cmd === "status") {
    const out: Record<string, unknown> = {};
    for (const h of ["pi", "codex"] as const) out[h] = await t("GET", `/v1/scan/status?collector=${encodeURIComponent(collectorOf(machine, h))}`);
    console.log(JSON.stringify(out, null, 2));
  } else {
    console.error("usage: bun client/cli.ts import [--dry-run] [--since <days>] | status");
    process.exit(2);
  }
}
