// memex for Pi (DESIGN §5.1–5.3, §6.1, §6.2, §6.4): sync, start-of-session snapshot, per-turn recall, mem_* tools.
// No local state beyond this process's memory: progress, frozen recall blocks and snapshots live on the server.
// Nothing leaves this machine unless MEMEX_URL, MEMEX_KEY and a policy scope allowing (scope, purpose, dest) are
// all present; the server checks its own grants again.
//
//   pi -e ~/Coding/memex/plugins/pi/index.ts      (or list it under "extensions" in settings)
//   MEMEX_URL, MEMEX_KEY: server; MEMEX_POLICY: policy file (default ~/.config/memex/policy.json);
//   MEMEX_MACHINE: collector name (default hostname).
import { existsSync, statSync } from "node:fs";
import { hostname } from "node:os";
import { basename, dirname, join } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { clientPath, fromEnv, type Transport } from "../../client/api.ts";
import { allowed, allowedDests, loadPolicy, NONE, policyPath, scopeOf, type Policy, type Purpose } from "../../client/policy.ts";
import { readHead, readSession, sourceIdOf } from "../../client/records.ts";
import { scanAll, syncFile, type SyncOutcome } from "../../client/sync.ts";
import { callTool, TOOLS } from "../../client/tools.ts";
import { collect, piTextOf } from "../../dryrun/dryrun.ts";

const SYNC_MS = 30_000; // one live sync call
const SHUTDOWN_MS = 5_000; // what exit waits for the last sync
const SNAPSHOT_MS = 3_000;
const RECALL_MS = 3_000; // the server's own budget is 2.5 s; a later answer is dropped
const BLOCKS_MS = 1_500;
const SCAN_MS = 60_000; // one reconciliation run (background)
const PI_SUBAGENT = /(^|\/)subagent-artifacts\/[^/]*_transcript\.jsonl$/;

type Session = {
  file: string | undefined; // the session file, fixed for this state (a switch makes a new state)
  source: string;
  scope: string;
  meta: { repo?: string };
  chain: Promise<unknown>; // syncs, one at a time
  lastSync?: { at: string; outcome: SyncOutcome | { status: "error"; reason: string } };
  inputs: { at: number; text: string; human: boolean }[]; // input events not yet tied to a user entry (last 50)
  seen: Set<string>; // user entries already tied to an input, or there before it
  based: boolean; // the baseline (entries there before the first input) taken
  recalls: Map<string, Promise<void>>; // user entry id → its one recall request
  late: Set<string>; // user entries whose recall missed its budget: never shown
  blocks: Map<string, string>; // user entry id → frozen recall block
  cursor?: string;
  lastRecall?: string;
  scan?: string;
  scanning?: boolean; // a reconciliation run in flight
  scanAt?: number; // when the last one started
};

/** The repository a directory belongs to: the name of the closest ancestor holding `.git`. */
function repoOf(cwd: string): string | undefined {
  for (let d = cwd; ; d = dirname(d)) {
    if (existsSync(join(d, ".git"))) return basename(d);
    if (dirname(d) === d) return undefined;
  }
}

const err = (e: unknown) => String((e as Error)?.message ?? e);
/** The user entry `id` ends the branch's conversation: nothing after it but non-message entries (custom
 *  context an extension added at agent start, labels…) — no reply, no later user message. */
const tail = (branch: any[], id: string) => {
  const at = branch.findIndex((b) => b.id === id);
  return at >= 0 && branch.slice(at + 1).every((b) => b.type !== "message");
};
const within = <T>(p: Promise<T>, ms: number) =>
  Promise.race([p, new Promise<"late">((r) => setTimeout(() => r("late"), ms).unref?.())]);

export default function memex(pi: ExtensionAPI) {
  let t: Transport | undefined;
  let policy: Policy = { scopes: [] };
  let configError: string | undefined;
  try {
    t = fromEnv();
    policy = loadPolicy();
  } catch (e) {
    configError = `policy ${policyPath()}: ${err(e)}`;
    t = undefined;
  }
  const collector = `${process.env.MEMEX_MACHINE ?? hostname()}:pi`;
  let s: Session | undefined;

  const can = (x: Session, purpose: Purpose) => !!t && allowed(policy, x.scope, purpose, "memex");

  function stateOf(ctx: ExtensionContext): Session {
    const file = ctx.sessionManager.getSessionFile();
    if (s && s.file === file) return s;
    s = {
      file, source: sourceIdOf("pi", ctx.sessionManager.getSessionId()), scope: file ? scopeOf(policy, file) : NONE,
      meta: { ...(repoOf(ctx.cwd) ? { repo: repoOf(ctx.cwd) } : {}) },
      chain: Promise.resolve(), inputs: [], seen: new Set(), based: false, recalls: new Map(), late: new Set(), blocks: new Map(),
    };
    return s;
  }

  /** Queue a sync of the session file at the current leaf (§5.1); `more` (segment limit) continues it. */
  function sync(ctx: ExtensionContext, deadlineMs = SYNC_MS) {
    const x = stateOf(ctx);
    if (!x.file || !can(x, "sync")) return x.chain;
    const leaf = ctx.sessionManager.getLeafId();
    x.chain = x.chain.then(async () => {
      try {
        let out: SyncOutcome;
        for (let i = 0; ; i++) {
          out = await syncFile(t!, policy, x.file!, "pi", { collector, leaf, meta: x.meta, deadlineMs });
          if (out.status !== "synced" || !out.more || i >= 20) break;
        }
        x.lastSync = { at: new Date().toISOString(), outcome: out! };
        if (process.env.MEMEX_DEBUG) console.error("[memex] sync", JSON.stringify(x.lastSync));
      } catch (e) {
        x.lastSync = { at: new Date().toISOString(), outcome: { status: "error", reason: err(e) } };
        if (process.env.MEMEX_DEBUG) console.error("[memex] sync", JSON.stringify(x.lastSync));
      }
    });
    return x.chain;
  }

  /** Reconciliation (§5.2) over this machine's authorized Pi logs, in the background: calls until the round is
   *  finished (the server keeps the progress), at session start and again after turns while the session lives
   *  (at most every MEMEX_SCAN_EVERY_MS, default 10 min; the server opens a new round only once its own interval has passed). */
  function scan(x: Session, now = false) {
    const delay = Number(process.env.MEMEX_SCAN_DELAY_MS ?? 2_000); // after session start; < 0: off altogether
    if (!t || delay < 0 || x.scanning) return;
    const roots = policy.scopes.filter((p) => p.allow?.sync?.includes("memex")).flatMap((p) => p.paths);
    if (!roots.length) return;
    x.scanning = true;
    x.scanAt = Date.now();
    setTimeout(() => {
      let run: Promise<unknown>;
      try {
        const files = collect(roots).files
          .filter((p) => p.endsWith(".jsonl") && !PI_SUBAGENT.test(p) && readHead(p, "pi").ok)
          .map((path) => ({ path, harness: "pi" as const }));
        run = scanAll(t!, policy, files, { collector, deadlineMs: SCAN_MS })
          .then((r: any) => (x.scan = `${new Date().toISOString()} ${JSON.stringify(r).slice(0, 300)}`));
      } catch (e) {
        run = Promise.reject(e);
      }
      run.catch((e) => (x.scan = `${new Date().toISOString()} failed: ${err(e)}`)).finally(() => (x.scanning = false));
    }, now ? 0 : delay).unref?.();
  }

  pi.on("session_start", async (_e, ctx) => {
    s = undefined;
    const x = stateOf(ctx);
    scan(x);
  });
  pi.on("turn_end", async (_e, ctx) => {
    void sync(ctx);
    const x = stateOf(ctx);
    if (Date.now() - (x.scanAt ?? 0) >= Number(process.env.MEMEX_SCAN_EVERY_MS ?? 600_000)) scan(x, true);
  });
  pi.on("session_compact", async (_e, ctx) => void sync(ctx));
  pi.on("session_tree", async (_e, ctx) => void sync(ctx));
  pi.on("session_shutdown", async (_e, ctx) => {
    await within(sync(ctx, SHUTDOWN_MS), SHUTDOWN_MS);
  });

  // §6.1: the snapshot is built once per session and stored on the server; each run asks for it again, so the
  // server re-checks it (a forget or revoke since drops what it covered) — the same text otherwise (stable prefix).
  pi.on("before_agent_start", async (e, ctx) => {
    const x = stateOf(ctx);
    if (!can(x, "snapshot")) return;
    const text = await t!("POST", "/v1/snapshot", { source: x.source, scope: x.scope, ...(x.meta.repo ? { repo: x.meta.repo } : {}) },
                          { deadline: Date.now() + SNAPSHOT_MS })
      .then((r: any) => (typeof r?.text === "string" ? r.text : ""))
      .catch(() => "");
    if (text && stateOf(ctx) === x) e.systemPromptOptions.sections.memex = text;
  });

  // An input becomes at most one new user entry, made after it (Pi stamps the message after the input handlers;
  // another extension may handle the input and make none). Entries that exist when the session is first seen
  // here (history of a resumed session, other branches) are never an input's entry: the baseline is taken
  // once, on the first input. Each input is kept with its time, its text and whether it may be recalled: typed
  // by the user (not an extension's) and not a /command (Pi expands /templates and /skill: after this event).
  pi.on("input", async (e, ctx) => {
    const x = stateOf(ctx);
    if (!x.based) {
      x.based = true;
      for (const b of ctx.sessionManager.getEntries() as any[]) if (b.type === "message" && b.message?.role === "user") x.seen.add(b.id);
    }
    const text = String(e.text ?? "");
    x.inputs = [...x.inputs, { at: Date.now(), text, human: e.source !== "extension" && !text.trimStart().startsWith("/") }].slice(-50);
  });

  // §6.2: recall for the latest user message, once per user entry; frozen blocks replayed on their messages.
  pi.on("context", async (e, ctx) => {
    const x = stateOf(ctx);
    if (!x.file || !can(x, "recall")) return;
    const branch = ctx.sessionManager.getBranch();
    const ts = (m: any) => (m?.role === "user" ? m.timestamp : undefined);
    // A context user message ↔ its entry by (role, timestamp), unique on both sides; otherwise none.
    const entryOf = (m: any) => {
      const at = ts(m);
      if (at === undefined || e.messages.filter((y: any) => ts(y) === at).length !== 1) return undefined;
      const hit = branch.filter((b: any) => b.type === "message" && ts(b.message) === at);
      return hit.length === 1 ? (hit[0] as any) : undefined;
    };
    // Each new user entry on the branch is tied to the pending input whose text it carries (Pi writes the
    // input's text as is, image hints after "\n\n"; an input handled with no entry is never matched, and
    // inputs are delivered in any order). It is the user's only when exactly one pending input made before it
    // matches and that one may be recalled. Fail closed: no match (another extension transformed it) or two
    // matches (the same text queued twice, whoever sent it) recall none.
    let human = false;
    for (const b of branch as any[])
      if (b.type === "message" && b.message?.role === "user" && !x.seen.has(b.id)) {
        x.seen.add(b.id);
        const at = Number(b.message.timestamp);
        const c = b.message.content;
        const first = typeof c === "string" ? c : Array.isArray(c) && c[0]?.type === "text" ? String(c[0].text) : undefined;
        const mine = first === undefined ? [] : x.inputs.filter((i) => i.at <= at && (first === i.text || first.startsWith(i.text + "\n\n")));
        if (mine.length) x.inputs = x.inputs.filter((i) => i !== mine[0]);
        human = mine.length === 1 && mine[0].human;
      }
    const leaf = ctx.sessionManager.getLeafId();
    const latest = [...e.messages].reverse().find((m: any) => m.role === "user");
    const cur = latest && entryOf(latest);
    // One budget from here for this input's recall, its blocks included: past it, this input's block is never
    // injected — not now, not by a later replay in this process (another process may still replay a decision the
    // server saved before its own deadline: the server's budget_ms bound is the protocol's, see DESIGN §6.2).
    let deadline: number | undefined;
    if (cur && human && tail(branch, cur.id) && !x.recalls.has(cur.id)) {
      const until = (deadline = Date.now() + Number(process.env.MEMEX_PI_RECALL_MS ?? RECALL_MS));
      x.recalls.set(cur.id, recallFor(ctx, x, branch, cur, until).then(() => {
        if (Date.now() >= until) x.late.add(cur.id);
      }));
      if ((await within(x.recalls.get(cur.id)!, Math.max(0, until - Date.now()))) === "late") x.late.add(cur.id);
    }
    // Blocks as the server has them now; unreachable → every past block is left out (§6.2 degradation).
    let ok = false;
    try {
      const r: any = await t!("POST", "/v1/recall/blocks", { source: x.source, scope: x.scope, ...(x.cursor ? { cursor: x.cursor } : {}) },
                              { deadline: Math.min(Date.now() + BLOCKS_MS, deadline ?? Infinity) });
      if (r.replace) x.blocks = new Map();
      for (const b of r.blocks ?? []) x.blocks.set(b.user_rec, b.text);
      x.cursor = r.cursor;
      ok = true;
    } catch {
      x.cursor = undefined;
      x.blocks = new Map();
    }
    // Past this input's budget (the blocks answer came after it): its block counts as late, never shown.
    if (deadline !== undefined && Date.now() >= deadline) x.late.add(cur.id);
    // Still the state this request was made for: same session, same leaf.
    if (!ok || !x.blocks.size || stateOf(ctx) !== x || ctx.sessionManager.getLeafId() !== leaf) return;
    let changed = false;
    const messages = e.messages.map((m: any) => {
      const entry = m.role === "user" ? entryOf(m) : undefined;
      const block = entry && !x.late.has(entry.id) ? x.blocks.get(entry.id) : undefined;
      if (!block) return m;
      changed = true;
      const content = typeof m.content === "string" ? [{ type: "text", text: m.content }] : m.content;
      return { ...m, content: [...content, { type: "text", text: block }] };
    });
    return changed ? { messages } : undefined;
  });

  /** The one recall request of a user entry: its text, the same branch's previous exchange (with references to
   *  the lines as uploaded), the leaf the query reads from. A refusal or timeout injects nothing. */
  async function recallFor(ctx: ExtensionContext, x: Session, branch: any[], cur: any, deadline: number) {
    const input = piTextOf(cur, "user");
    if (!input || input.startsWith("/")) return;
    await within(x.chain, Math.max(0, Math.min(1_000, deadline - Date.now()))); // the previous turn's sync, so its records can be referenced
    const at = branch.findIndex((b) => b.id === cur.id);
    const prior = at < 0 ? [] : branch.slice(0, at);
    const u = prior.findLastIndex((b) => b.type === "message" && piTextOf(b, "user"));
    const a = prior.findLastIndex((b) => b.type === "message" && piTextOf(b, "assistant"));
    const read = readSession(x.file!, "pi");
    const hashOf = new Map(read.ok ? read.records.map((r) => [r.id, r.hash]) : []);
    const ref = (b: any) => (b && hashOf.has(b.id) ? { rec: b.id, hash: hashOf.get(b.id)! } : null);
    const pu = u >= 0 ? prior[u] : undefined, pa = a > u && u >= 0 ? prior[a] : undefined;
    const pur = ref(pu), par = pur ? ref(pa) : null;
    const body = {
      source: x.source, scope: x.scope, user_rec: cur.id, input,
      ...(pur ? { prev_user: piTextOf(pu, "user"), prev_user_ref: pur } : {}),
      ...(par ? { prev_assistant: piTextOf(pa, "assistant"), prev_assistant_ref: par } : {}),
      dests: allowedDests(policy, x.scope, "recall"),
      // The branch the input continues: its parent. Valid whether or not the server has the input yet (a local
      // file holding it says nothing about the server's copy).
      ...(cur.parentId ? { leaf: cur.parentId } : {}),
      // What is left of the budget, less the way back: the server saves nothing after it.
      budget_ms: Math.max(1, deadline - Date.now() - 200),
      inContext: ctx.sessionManager.buildContextEntries().map((b: any) => b.id).filter((id: string) => id !== cur.id).slice(-10_000),
    };
    try {
      const r: any = await t!("POST", "/v1/recall", body, { deadline });
      x.lastRecall = `${new Date().toISOString()} ${r.state}${r.reason ? ` (${r.reason})` : ""}`;
    } catch (e) {
      x.lastRecall = `${new Date().toISOString()} failed: ${err(e)}`;
    }
  }

  // §6.4: the agent's tools. Identity and scope come from this session, never from the model.
  for (const d of TOOLS)
    pi.registerTool({
      name: d.name, label: d.name.replace("_", " "), description: d.description, parameters: d.inputSchema as any,
      async execute(_id, p: any, _sig, _up, ctx) {
        const x = stateOf(ctx);
        const text = await callTool(t, policy, x.file ? { source: x.source, scope: x.scope, leaf: ctx.sessionManager.getLeafId() } : undefined, d.name, p);
        return { content: [{ type: "text" as const, text }], details: undefined };
      },
    });

  pi.registerCommand("memex", {
    description: "memex status: configuration, authorization, last sync / recall / scan",
    handler: async (_args, ctx) => {
      const x = stateOf(ctx);
      const purposes: Purpose[] = ["sync", "snapshot", "recall", "tool"];
      const lines = [
        `server: ${t ? (process.env.MEMEX_URL && process.env.MEMEX_KEY ? process.env.MEMEX_URL : `from ${clientPath()}`) : configError ?? `not configured (MEMEX_URL / MEMEX_KEY or ${clientPath()})`}`,
        `policy: ${policyPath()} — scope ${x.scope}${x.file ? "" : " (no session file)"}`,
        `allowed: ${purposes.filter((p) => allowed(policy, x.scope, p, "memex")).join(", ") || "nothing (no memex data leaves this session)"}`,
        `last sync: ${x.lastSync ? `${x.lastSync.at} ${JSON.stringify(x.lastSync.outcome)}` : "none"}`,
        `last recall: ${x.lastRecall ?? "none"}; frozen blocks: ${x.blocks.size}`,
        `scan: ${x.scan ?? "none"}`,
        "limits: tree moves and compaction are synced at session_tree / session_compact; past recall blocks are omitted when the server is unreachable.",
      ];
      if (x.file && existsSync(x.file)) lines.push(`session file: ${x.file} (${statSync(x.file).size} bytes)`);
      ctx.ui.notify(lines.join("\n"), "info");
    },
  });
}
