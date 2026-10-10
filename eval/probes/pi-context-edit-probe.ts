// Companion to pi-context-map-probe.ts (load it first, before the map probe): (1) once, after the first agent
// run, sends a user message from this extension (input source "extension"); (2) in `context`, appends a marker
// to the text of the latest user message — another extension editing messages before memex sees them.
// Logs the input sources it sees. Never logs text.
import { appendFileSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const LOG = process.env.MEMEX_PROBE_LOG ?? "/tmp/memex-eval/pi-ctx.jsonl";

export default function (pi: ExtensionAPI) {
  let sent = false;
  pi.on("input", async (e) => {
    appendFileSync(LOG, JSON.stringify({ t: Date.now(), event: "input", source: e.source }) + "\n");
  });
  pi.on("agent_end", async () => {
    if (sent) return;
    sent = true;
    pi.sendUserMessage("Reply with the word ok.", { deliverAs: "followUp" });
  });
  if (process.env.MEMEX_PROBE_EDIT === "1")
    pi.on("context", async (e) => {
      const last = [...e.messages].reverse().find((m: any) => m.role === "user") as any;
      if (last && Array.isArray(last.content)) last.content = [...last.content, { type: "text", text: " [edited]" }];
      else if (last && typeof last.content === "string") last.content += " [edited]";
      return { messages: e.messages };
    });
}
