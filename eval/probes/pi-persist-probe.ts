// Pi probe (DESIGN §11 item 2): at each lifecycle boundary, is the in-memory branch already in the session
// file? Records only entry ids and types, never text. Load explicitly; it writes one JSONL log.
//
//   MEMEX_PROBE_LOG=/tmp/memex-eval/pi-probe.jsonl pi -ne -nc -ns -e eval/probes/pi-persist-probe.ts \
//     --session-dir /tmp/memex-eval/pi-sessions -p "..."
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const LOG = process.env.MEMEX_PROBE_LOG ?? "/tmp/memex-eval/pi-probe.jsonl";

export default function (pi: ExtensionAPI) {
  const snap = (event: string, ctx: any, extra: Record<string, unknown> = {}) => {
    const sm = ctx.sessionManager;
    const file: string | undefined = sm.getSessionFile?.();
    const branch: any[] = sm.getBranch?.() ?? [];
    const mem = branch.map((e) => e.id);
    let disk: string[] = [];
    let diskLast: string | undefined;
    if (file && existsSync(file)) {
      const rows = readFileSync(file, "utf8").split("\n").filter(Boolean);
      const parsed = rows.flatMap((l) => {
        try {
          return [JSON.parse(l)];
        } catch {
          return [{ torn: true }];
        }
      });
      disk = parsed.filter((e) => e.type !== "session").map((e) => e.id);
      const last = parsed.at(-1);
      diskLast = last ? `${last.type}:${last.message?.role ?? last.customType ?? ""}` : undefined;
    }
    const onDisk = new Set(disk);
    const missing = branch.filter((e) => !onDisk.has(e.id)).map((e) => `${e.type}:${e.message?.role ?? e.customType ?? ""}`);
    appendFileSync(
      LOG,
      JSON.stringify({ t: Date.now(), event, leaf: sm.getLeafId?.(), memEntries: mem.length, diskEntries: disk.length, diskLast, missingOnDisk: missing, fileExists: !!file && existsSync(file), ...extra }) + "\n",
    );
  };
  pi.on("session_start", async (_e, ctx) => snap("session_start", ctx));
  pi.on("agent_start", async (_e, ctx) => snap("agent_start", ctx));
  pi.on("message_end", async (e: any, ctx) => snap("message_end", ctx, { role: e.message?.role }));
  pi.on("turn_end", async (_e, ctx) => snap("turn_end", ctx));
  pi.on("agent_end", async (_e, ctx) => snap("agent_end", ctx));
  pi.on("agent_settled", async (_e, ctx) => snap("agent_settled", ctx));
  pi.on("session_shutdown", async (_e, ctx) => snap("session_shutdown", ctx));
}
