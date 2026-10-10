// Server administration, run on the server host against its database (MEMEX_PG_URL); there is no HTTP route
// for any of this. Keys are shown once, when made; only their sha256 is stored.
//   bun server/admin.ts key add <name>          a new API key for a client (printed once)
//   bun server/admin.ts key list                names, creation time, revoked
//   bun server/admin.ts key revoke <name>
//   bun server/admin.ts grant allow <scope> <purpose> <dest>     purpose: sync snapshot recall tool derive
//   bun server/admin.ts grant revoke <scope> <purpose> <dest>    dest: memex llm embedding rerank
//   bun server/admin.ts grant list
// Grants are the server's own authorization (DESIGN §7); the client policy can only narrow them. A revoke
// runs under the visibility barrier and withdraws what was derived or served under the grant.
import { connect, migrate, type Db } from "./db.ts";
import "./main.ts";
import { addApiKey } from "./http.ts";
import { allowGrant, revokeGrant } from "./derive.ts";

const PURPOSES = new Set(["sync", "snapshot", "recall", "tool", "derive"]);
const DESTS = new Set(["memex", "llm", "embedding", "rerank"]);
const USAGE = "usage: key add <name> | key list | key revoke <name> | grant allow|revoke <scope> <purpose> <dest> | grant list";

function grantArgs(a: string[]) {
  const [scope, purpose, dest] = a;
  if (!scope || !/^[a-z0-9][a-z0-9_-]{0,40}$/.test(scope)) throw new Error("scope: lower-case letters, digits, - and _");
  if (!PURPOSES.has(purpose)) throw new Error(`purpose: one of ${[...PURPOSES].join(" ")}`);
  if (!DESTS.has(dest)) throw new Error(`destination: one of ${[...DESTS].join(" ")}`);
  return [scope, purpose, dest] as const;
}

/** Run one admin command; returns what to print. */
export async function admin(db: Db, argv: string[]): Promise<string> {
  const [what, cmd, ...a] = argv;
  if (what === "key" && cmd === "add" && a[0]) {
    if ((await db`select 1 from api_key where name = ${a[0]}`).length) throw new Error(`a key named ${a[0]} exists`);
    return `${a[0]}: ${await addApiKey(db, a[0])}\n(shown once; give it to the client as MEMEX_KEY)`;
  }
  if (what === "key" && cmd === "list")
    return (await db`select name, created_at, revoked from api_key order by created_at`)
      .map((r: any) => `${r.name}\t${new Date(r.created_at).toISOString()}${r.revoked ? "\trevoked" : ""}`).join("\n");
  if (what === "key" && cmd === "revoke" && a[0])
    return `revoked ${(await db`update api_key set revoked = true where name = ${a[0]} and not revoked returning 1`).length}`;
  if (what === "grant" && cmd === "allow") return (await allowGrant(db, ...grantArgs(a)), "ok");
  if (what === "grant" && cmd === "revoke") return (await revokeGrant(db, ...grantArgs(a)), "ok");
  if (what === "grant" && cmd === "list")
    return (await db`select scope, purpose, dest from grant_rule order by scope, purpose, dest`).map((r: any) => `${r.scope}\t${r.purpose}\t${r.dest}`).join("\n");
  throw new Error(USAGE);
}

if (import.meta.main) {
  const db = connect(undefined, 2);
  try {
    await migrate(db);
    console.log(await admin(db, process.argv.slice(2)));
  } catch (e) {
    console.error((e as Error).message);
    process.exitCode = 1;
  } finally {
    await db.close();
  }
}
