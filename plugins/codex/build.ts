// Build the Codex plugin into a self-contained directory (Codex copies a plugin into its cache, so nothing may
// point back into this repo), plus a local marketplace naming it:
//   bun plugins/codex/build.ts [<out dir>]        default: dist/codex-marketplace
//   codex plugin marketplace add <out dir> && codex plugin add memex@memex-local
// Then, in ~/.codex/config.toml, let the tools run without a prompt (they only read memex):
//   [plugins."memex@memex-local".mcp_servers.memex]
//   default_tools_approval_mode = "approve"
// and trust the hooks with /hooks in the Codex CLI (Codex skips untrusted plugin hooks).
import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

export async function build(out: string) {
  const root = join(out, "plugins", "memex");
  mkdirSync(join(root, ".codex-plugin"), { recursive: true });
  mkdirSync(join(root, "hooks"), { recursive: true });
  for (const name of ["hook", "mcp"]) {
    const r = await Bun.build({ entrypoints: [join(import.meta.dir, `${name}.ts`)], target: "bun", format: "esm" });
    if (!r.success) throw new Error(`${name}: ${r.logs.join("\n")}`);
    writeFileSync(join(root, `${name}.js`), await r.outputs[0].text());
  }
  const bun = process.execPath; // hooks run with the host daemon's environment: no PATH lookup
  writeFileSync(join(root, ".codex-plugin", "plugin.json"), JSON.stringify({
    name: "memex", version: "0.1.0", description: "memex: sync, start-of-session context, per-turn recall and mem_* tools over your own memex server.",
  }, null, 2));
  // The MCP server starts with a clean environment: pass on the ones memex reads (unset ones stay unset; the
  // defaults are ~/.config/memex/{client,policy}.json and ~/.codex).
  writeFileSync(join(root, ".mcp.json"), JSON.stringify({ mcpServers: { memex: { command: bun, args: ["./mcp.js"], cwd: ".",
    env_vars: ["HOME", "CODEX_HOME", "MEMEX_URL", "MEMEX_KEY", "MEMEX_CLIENT", "MEMEX_POLICY"] } } }, null, 2));
  const hook = (timeout: number) => [{ hooks: [{ type: "command", command: `"${bun}" "$PLUGIN_ROOT/hook.js"`, timeout }] }];
  writeFileSync(join(root, "hooks", "hooks.json"), JSON.stringify({ hooks: {
    SessionStart: hook(30), UserPromptSubmit: hook(10), Stop: hook(30), PreCompact: hook(30), SessionEnd: hook(10),
  } }, null, 2));
  mkdirSync(join(out, ".agents", "plugins"), { recursive: true });
  writeFileSync(join(out, ".agents", "plugins", "marketplace.json"), JSON.stringify({
    name: "memex-local",
    plugins: [{ name: "memex", source: { source: "local", path: "./plugins/memex" }, policy: { installation: "AVAILABLE", authentication: "ON_INSTALL" }, category: "Productivity" }],
  }, null, 2));
  return root;
}

if (import.meta.main) console.log(await build(resolve(process.argv[2] ?? join(import.meta.dir, "..", "..", "dist", "codex-marketplace"))));
