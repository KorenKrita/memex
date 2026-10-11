// memex for Codex: one command hook for every lifecycle event (DESIGN §5.1–5.3, §6.1, §6.2). The hook payload
// (JSON) arrives on stdin; what Codex should add goes to stdout as the event's JSON shape. Never fails the turn:
// any error is swallowed, and an event that needs JSON (Stop) always gets `{}`.
//   SessionStart        sync, then the start-of-session snapshot as additionalContext; reconciliation scan
//   UserPromptSubmit    sync, then recall for this prompt as additionalContext (Codex writes it into the rollout
//                       and replays it itself)
//   Stop, PreCompact, SessionEnd   sync
// Identity is the transcript itself: its session_meta id must be the payload's session_id, and the file must
// lie in a policy scope; otherwise nothing leaves this machine.
import { createHash } from "node:crypto";
import { appendFileSync } from "node:fs";
import { hostname } from "node:os";
import { fromEnv, type Transport } from "../../client/api.ts";
import { allowed, allowedDests, loadPolicy, scopeOf, type Policy } from "../../client/policy.ts";
import { readHead, readSession, type Rec } from "../../client/records.ts";
import { scanOnce, syncFile } from "../../client/sync.ts";
import { codexTextOf, collect } from "../../dryrun/dryrun.ts";
import { repoOf } from "./session.ts";

const SYNC_MS = Number(process.env.MEMEX_CODEX_SYNC_MS ?? 8_000);
const SNAPSHOT_MS = 3_000;
const RECALL_MS = Number(process.env.MEMEX_CODEX_RECALL_MS ?? 3_000);
const SCAN_MS = 10_000;
const collector = () => `${process.env.MEMEX_MACHINE ?? hostname()}:codex`;
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

type Payload = { hook_event_name?: string; session_id?: string; transcript_path?: string | null; cwd?: string; turn_id?: string; prompt?: string; source?: string };

/** The session this payload is about, when its transcript proves it and the policy allows `sync`. */
function sessionOf(p: Payload, policy: Policy) {
  const path = p.transcript_path;
  if (!path || !p.session_id) return undefined;
  const h = readHead(path, "codex");
  if (!h.ok || h.nativeId !== p.session_id) return undefined;
  const scope = scopeOf(policy, path);
  return { path, scope, source: h.sourceId };
}

const out = (event: string, context?: string) =>
  JSON.stringify(context ? { hookSpecificOutput: { hookEventName: event, additionalContext: context } } : {});

async function sync(t: Transport, policy: Policy, s: { path: string }, cwd: string | undefined, deadlineMs = SYNC_MS) {
  const repo = cwd ? repoOf(cwd) : undefined;
  for (let i = 0; i < 20; i++) {
    const r = await syncFile(t, policy, s.path, "codex", { collector: collector(), meta: repo ? { repo } : {}, deadlineMs });
    if (r.status !== "synced" || !r.more) return r;
  }
}

/** The rollout's records up to now, newest last: the previous exchange (user, then its reply) as references. */
function previous(recs: Rec[]) {
  let u: Rec | undefined, a: Rec | undefined;
  for (let i = recs.length - 1; i >= 1 && !u; i--) {
    const line = JSON.parse(recs[i].raw);
    if (!a && codexTextOf(line, "assistant")) a = recs[i];
    else if (codexTextOf(line, "user")) u = recs[i];
  }
  if (!u) return {};
  const ul = JSON.parse(u.raw);
  const ref = (r: Rec) => ({ rec: r.id, hash: r.hash });
  // The reply must follow the user message (the last exchange); an older reply alone is not referenced.
  const reply = a && Number(a.id) > Number(u.id) ? a : undefined;
  return { prev_user: codexTextOf(ul, "user"), prev_user_ref: ref(u),
           ...(reply ? { prev_assistant: codexTextOf(JSON.parse(reply.raw), "assistant"), prev_assistant_ref: ref(reply) } : {}) };
}

export async function handle(p: Payload, env = process.env): Promise<string> {
  const event = p.hook_event_name ?? "";
  const t = fromEnv(env);
  let policy: Policy;
  try {
    policy = loadPolicy();
  } catch {
    return out(event);
  }
  const s = t && sessionOf(p, policy);
  if (!t || !s || !allowed(policy, s.scope, "sync", "memex")) return out(event);
  const repo = p.cwd ? repoOf(p.cwd) : undefined;

  if (event === "SessionStart") {
    await sync(t, policy, s, p.cwd).catch(() => {});
    if (env.MEMEX_SCAN !== "0") {
      const roots = policy.scopes.filter((x) => x.allow?.sync?.includes("memex")).flatMap((x) => x.paths);
      const files = collect(roots).files.filter((f) => f.endsWith(".jsonl") && readHead(f, "codex").ok).map((path) => ({ path, harness: "codex" as const }));
      await scanOnce(t, policy, files, { collector: collector(), deadlineMs: SCAN_MS }).catch(() => {});
    }
    if (!allowed(policy, s.scope, "snapshot", "memex")) return out(event);
    const r: any = await t("POST", "/v1/snapshot", { source: s.source, scope: s.scope, ...(repo ? { repo } : {}) },
                           { deadline: Date.now() + SNAPSHOT_MS }).catch(() => null);
    return out(event, typeof r?.text === "string" && r.text ? `<memex>\n${r.text}\n</memex>` : undefined);
  }

  if (event === "UserPromptSubmit") {
    const deadline = Date.now() + RECALL_MS;
    // Sync first: the previous exchange must be on the server to be referenced (§6.2). The prompt itself is
    // not in the rollout yet (Codex records it after this hook), so it is bound by the turn id.
    await sync(t, policy, s, p.cwd, Math.max(500, Math.min(1_500, deadline - Date.now()))).catch(() => {});
    if (!allowed(policy, s.scope, "recall", "memex") || !p.turn_id || typeof p.prompt !== "string") return out(event);
    const read = readSession(s.path, "codex");
    if (!read.ok) return out(event);
    const body = {
      // The turn and this prompt's hash: the server finds the prompt's record by them once it is uploaded.
      source: s.source, scope: s.scope, user_rec: `turn:${p.turn_id}:${sha(p.prompt.trim()).slice(0, 16)}`, input: p.prompt, ...previous(read.records),
      dests: allowedDests(policy, s.scope, "recall"), budget_ms: Math.max(1, deadline - Date.now() - 200),
    };
    const r: any = await t("POST", "/v1/recall", body, { deadline }).catch((e) => ({ state: "failed", reason: String(e?.message ?? e) }));
    if (env.MEMEX_DEBUG_LOG) appendFileSync(env.MEMEX_DEBUG_LOG, JSON.stringify({ event, turn: p.turn_id, state: r?.state, reason: r?.reason, prev: (body as any).prev_user_ref ?? null }) + "\n");
    return out(event, r?.state === "shown" && typeof r.text === "string" && r.text ? r.text : undefined);
  }

  if (event === "Stop" || event === "PreCompact" || event === "SessionEnd") await sync(t, policy, s, p.cwd).catch(() => {});
  return out(event);
}

if (import.meta.main) {
  const chunks: Buffer[] = [];
  for await (const c of process.stdin) chunks.push(c as Buffer);
  let p: Payload = {};
  try {
    p = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {}
  const text = await handle(p).catch(() => out(p.hook_event_name ?? ""));
  process.stdout.write(text + "\n");
  process.exit(0);
}
