// Indexable files of a frozen corpus, as decided by the reference dry-run (dryrun/dryrun.ts): prints one JSON
// object {indexed: [corpus-relative paths], skipped: {reason: count}}. Pi `<session>/<id>/run-N/` runs are
// reported separately (`runs`): the dry-run indexes them, the bank does not (see eval/README.md).
//
//   bun eval/qbank/indexable.ts <corpus-dir>
import { realpathSync } from "node:fs";
import { relative } from "node:path";
import { dryRun } from "../../dryrun/dryrun.ts";

const root = realpathSync(process.argv[2]);
const r = dryRun([root]);
const indexed: string[] = [];
const runs: string[] = [];
const skipped: Record<string, number> = {};
for (const f of r.perFile) {
  const rel = relative(root, realpathSync(f.path));
  if (f.status !== "indexed") skipped[f.reason!] = (skipped[f.reason!] ?? 0) + 1;
  else if (/^pi\/.*\/run-\d+\//.test(rel)) runs.push(rel);
  else indexed.push(rel);
}
console.log(JSON.stringify({ indexed: indexed.sort(), runs: runs.sort(), skipped }));
