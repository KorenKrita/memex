// Pi probe (stage 4): can each user message in a `context` event be tied to its session entry? Logs, per
// context call, the user messages' positions and, for each, the projection entry (buildSessionProjection) whose
// messages it equals by content (role + timestamp + JSON), and how many entries match. Never logs text.
//
//   MEMEX_PROBE_LOG=/tmp/memex-eval/pi-ctx.jsonl pi -ne -nc -ns -e eval/probes/pi-context-map-probe.ts \
//     --session-dir /tmp/memex-eval/pi-sessions -p "..."
import { appendFileSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const LOG = process.env.MEMEX_PROBE_LOG ?? "/tmp/memex-eval/pi-ctx.jsonl";
const key = (m: any) => JSON.stringify([m?.role, m?.timestamp, m?.content]);

export default function (pi: ExtensionAPI) {
  pi.on("context", async (e, ctx) => {
    const proj = ctx.sessionManager.buildSessionProjection();
    const byKey = new Map<string, string[]>();
    for (const pe of proj.entries)
      for (const m of pe.messages) {
        const k = key(m);
        byKey.set(k, [...(byKey.get(k) ?? []), (pe.sourceEntry as any).id]);
      }
    const users = e.messages.map((m: any, i: number) => ({ m, i })).filter((x) => x.m.role === "user");
    appendFileSync(LOG, JSON.stringify({
      t: Date.now(), leaf: ctx.sessionManager.getLeafId(), n: e.messages.length, projMessages: proj.messages.length,
      sameLength: proj.messages.length === e.messages.length,
      users: users.map(({ m, i }) => ({ i, ts: m.timestamp ?? null, matches: byKey.get(key(m)) ?? [], sameAtIndex: key(proj.messages[i]) === key(m) })),
    }) + "\n");
  });
  pi.on("before_agent_start", async (e, ctx) => {
    appendFileSync(LOG, JSON.stringify({ t: Date.now(), event: "before_agent_start", leaf: ctx.sessionManager.getLeafId(), sections: Object.keys(e.systemPromptOptions.sections) }) + "\n");
  });
  pi.on("turn_end", async (e, ctx) => {
    appendFileSync(LOG, JSON.stringify({ t: Date.now(), event: "turn_end", leaf: ctx.sessionManager.getLeafId(), messageEntryId: e.messageEntryId }) + "\n");
  });
}
