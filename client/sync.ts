// One sync segment (DESIGN §5.1): lease → read the local file → manifest diff → one commit. No local state:
// progress lives on the server, a lost receipt is retried with the same commit id.
//
// Outbound authorization (§7) comes first: a source outside the allowed scopes makes no request at all.
import { blockHashes, CHUNK, piHeaderId, readSession, sourceIdOf, type Harness, type ReadResult } from "./records.ts";
import type { Policy } from "./policy.ts";
import { allowed, scopeOf } from "./policy.ts";

export type Transport = (method: string, path: string, body?: unknown) => Promise<any>;
export type SyncOutcome =
  | { status: "synced"; sent: number; length: number; refused: number; more: boolean } // more: segment limit hit, call again
  | { status: "unchanged"; length: number }
  | { status: "skipped"; reason: string }
  | { status: "failed"; reason: string };

const MAX_CHANGES = 2_000;
const MAX_BYTES = 6 * 1024 * 1024;

export async function syncFile(t: Transport, policy: Policy, path: string, harness: Harness, opts: { leaf?: string | null; collector: string; meta?: object; scanChunks?: boolean }): Promise<SyncOutcome> {
  const scope = scopeOf(policy, path);
  if (!allowed(policy, scope, "sync", "memex")) return { status: "skipped", reason: "not-authorized" };
  const pre = readSession(path, harness);
  if (!pre.ok) return { status: "skipped", reason: pre.reason };
  // Fork parent (§4.1). Pi names a parent file: only one in the same scope is opened. Codex names a thread id.
  // The server links a parent only if it is registered in the same scope; otherwise the copied prefix stays
  // unresolved rather than tied to a session this scope may not read.
  let parentNative = pre.parentNative;
  if (pre.parentPath) parentNative = scopeOf(policy, pre.parentPath) === scope ? piHeaderId(pre.parentPath) : undefined;
  const parent = parentNative ? sourceIdOf(harness, parentNative) : null;
  const decl = { source: pre.sourceId, harness, nativeId: pre.nativeId, scope, parent, meta: opts.meta ?? {} };
  let lease: number;
  try {
    lease = (await t("POST", "/v1/sync/lease", { ...decl, collector: opts.collector })).lease;
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) }; // e.g. "410 source forgotten"
  }
  // Every way out before a commit lets the lease go (an accepted commit ends it on the server).
  let committed = false;
  try {
    let granted = false;
    for (let i = 0; i < 600 && !granted; i++) {
      granted = (await t("POST", `/v1/sync/lease/${lease}/poll`)).granted;
      if (!granted) await Bun.sleep(Math.min(50 * (i + 1), 500));
    }
    if (!granted) return { status: "failed", reason: "lease-timeout" };
    // Read only after the lease is held (§5.1): the read is the snapshot this commit stands for.
    const cur: ReadResult = readSession(path, harness);
    if (!cur.ok) return { status: "skipped", reason: cur.reason };
    if (cur.sourceId !== pre.sourceId) return { status: "failed", reason: "file-changed-identity" };
    const m = await t("GET", `/v1/sync/lease/${lease}/manifest`);
    const local = cur.records;
    const mine = blockHashes(local);
    // Blocks to compare record by record: every block whose hash differs (slow path covers prefix rewrites).
    const differ: number[] = [];
    for (let b = 0; b < Math.max(mine.length, m.blocks.length); b++) if (mine[b] !== m.blocks[b] && b < mine.length) differ.push(b);
    const remote = new Map<number, { id: string; hash: string }>();
    for (let i = 0; i < differ.length; i += 64) {
      const got = await t("POST", `/v1/sync/lease/${lease}/blocks`, { blocks: differ.slice(i, i + 64) });
      for (const [b, rows] of Object.entries(got as Record<string, { id: string; hash: string }[]>))
        rows.forEach((r, j) => remote.set(Number(b) * CHUNK + j, r));
    }
    const changes: { pos: number; id: string; hash: string; raw: string }[] = [];
    let bytes = 0;
    let hitLimit = false;
    scan: for (const b of differ)
      for (let pos = b * CHUNK; pos < Math.min(local.length, (b + 1) * CHUNK); pos++) {
        const r = remote.get(pos);
        if (r && r.id === local[pos].id && r.hash === local[pos].hash) continue;
        if (changes.length >= MAX_CHANGES || bytes + local[pos].raw.length > MAX_BYTES) {
          hitLimit = true;
          break scan;
        }
        changes.push({ pos, ...local[pos] });
        bytes += local[pos].raw.length;
      }
    // A bounded segment: if not every change fits, commit the copy as the prefix up to the last change sent
    // (the server drops its rows past that); the next segment continues from there.
    if (hitLimit && !changes.length) return { status: "failed", reason: "record-too-large" };
    const length = hitLimit ? changes[changes.length - 1].pos + 1 : local.length;
    // Unchanged: the same records, and no leaf to report or the one the server already has for this collector.
    if (!changes.length && m.length === local.length && (opts.leaf === undefined || opts.leaf === m.leaf))
      return { status: "unchanged", length: local.length };
    const commitId = crypto.randomUUID();
    const body = { commitId, length, changes, leaf: opts.leaf, scanChunks: opts.scanChunks ? mine.length : undefined };
    let r: any;
    for (let i = 0; i < 3; i++) {
      try {
        r = await t("POST", `/v1/sync/lease/${lease}/commit`, body);
        committed = true;
        break;
      } catch (e) {
        if (i === 2) throw e; // same commit id: the server returns the stored result if it had committed
      }
    }
    if (!r.ok) return { status: "failed", reason: r.reason };
    return { status: "synced", sent: changes.length, length: r.length, refused: r.refused, more: hitLimit };
  } catch (e) {
    return { status: "failed", reason: String((e as Error).message ?? e) };
  } finally {
    if (!committed) await t("POST", `/v1/sync/lease/${lease}/release`).catch(() => {});
  }
}
