// Drive `codex app-server` over stdio JSON-RPC (DESIGN §11 item 1): list hooks with their trust hashes, or
// run one synthetic turn on a fresh thread and wait for it to finish. Uses whatever CODEX_HOME is set.
//
//   CODEX_HOME=/tmp/memex-eval/codex-home bun eval/probes/codex-appserver-probe.ts hooks
//   CODEX_HOME=/tmp/memex-eval/codex-home bun eval/probes/codex-appserver-probe.ts turn "<prompt>" [model]
const [mode, prompt, model] = process.argv.slice(2);
const proc = Bun.spawn(["codex", "app-server"], { stdin: "pipe", stdout: "pipe", stderr: "inherit", cwd: process.cwd() });
let nextId = 1;
const waiting = new Map<number, (v: any) => void>();
const notes: any[] = [];
let onNote: ((n: any) => void) | undefined;
const send = (method: string, params: unknown) =>
  new Promise<any>((resolve) => {
    const id = nextId++;
    waiting.set(id, resolve);
    proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
  });
(async () => {
  let buf = "";
  for await (const chunk of proc.stdout) {
    buf += new TextDecoder().decode(chunk);
    let i: number;
    while ((i = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, i);
      buf = buf.slice(i + 1);
      if (!line.trim()) continue;
      const msg = JSON.parse(line);
      if (msg.id !== undefined && waiting.has(msg.id)) {
        waiting.get(msg.id)!(msg);
        waiting.delete(msg.id);
      } else if (msg.id !== undefined && msg.method) {
        // server request (approval etc.): decline
        proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: { decision: "decline" } }) + "\n");
      } else {
        notes.push(msg);
        onNote?.(msg);
      }
    }
  }
})();
const timeout = setTimeout(() => {
  console.log(JSON.stringify({ error: "timeout", lastNotes: notes.slice(-5).map((n) => n.method) }));
  proc.kill();
  process.exit(2);
}, 180_000);
const init = await send("initialize", { clientInfo: { name: "memex-probe", version: "0" } });
if (init.error) throw new Error(JSON.stringify(init.error));
proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", method: "initialized" }) + "\n");
let exitCode = 0;
if (mode === "hooks") {
  const r = await send("hooks/list", { cwds: [process.cwd()] });
  console.log(JSON.stringify(r.result ?? r.error));
  if (r.error) exitCode = 1;
} else {
  let threadId = "";
  // only this thread's completion counts
  const done = new Promise<any>((resolve) => (onNote = (n) => n.method === "turn/completed" && n.params?.threadId === threadId && resolve(n)));
  const t = await send("thread/start", { cwd: process.cwd(), ...(model ? { model } : {}), approvalPolicy: "never", sandbox: "read-only" });
  if (t.error) throw new Error(JSON.stringify(t.error));
  threadId = t.result.thread.id;
  const turn = await send("turn/start", { threadId, input: [{ type: "text", text: prompt ?? "Reply with the single word: ok" }] });
  if (turn.error) throw new Error(JSON.stringify(turn.error));
  const c = await done;
  const turnStatus = c.params?.turn?.status;
  console.log(JSON.stringify({ threadId, path: t.result.thread.path, turnStatus, methods: [...new Set(notes.map((n) => n.method))] }));
  if (turnStatus !== "completed") exitCode = 1;
}
clearTimeout(timeout);
// Close stdin and wait for the server to exit on its own, so its SessionEnd hooks finish before the next run.
proc.stdin.end();
const exited = await Promise.race([proc.exited, Bun.sleep(30_000).then(() => "timeout" as const)]);
if (exited === "timeout") {
  console.log(JSON.stringify({ error: "app-server did not exit within 30s after stdin closed" }));
  proc.kill();
  await proc.exited;
  exitCode = 1;
}
process.exit(exitCode);
