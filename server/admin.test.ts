// Admin CLI against a real Postgres (MEMEX_PG_URL, a throwaway database: tests truncate it).
import { afterAll, beforeAll, beforeEach, expect, test } from "bun:test";
import { connect, migrate, type Db } from "./db.ts";
import { admin } from "./admin.ts";
import { serve, transport } from "./http.ts";

let db: Db, server: Awaited<ReturnType<typeof serve>>, base: string;
beforeAll(async () => {
  db = connect();
  await migrate(db);
  server = await serve(db, { port: 0 });
  base = `http://127.0.0.1:${server.port}`;
});
afterAll(async () => {
  await server.stop();
  await db.close();
});
beforeEach(async () => {
  await db.unsafe(`truncate api_key, grant_rule restart identity cascade`);
});

test("key add / list / revoke: the key works until revoked; the list never shows a key", async () => {
  const out = await admin(db, ["key", "add", "laptop"]);
  const k = out.match(/mx_[A-Za-z0-9_-]+/)![0];
  expect(await transport(base, k)("GET", "/v1/scan/status?collector=x")).toBeDefined();
  const list = await admin(db, ["key", "list"]);
  expect(list).toContain("laptop");
  expect(list).not.toContain(k);
  expect(await admin(db, ["key", "revoke", "laptop"])).toContain("1");
  await expect(transport(base, k)("GET", "/v1/scan/status?collector=x")).rejects.toThrow("401");
  await expect(admin(db, ["key", "add", "laptop"])).rejects.toThrow(); // names stay unique, revoked or not
});

test("grant allow / list / revoke; unknown purpose or destination is refused", async () => {
  await admin(db, ["grant", "allow", "personal", "sync", "memex"]);
  await admin(db, ["grant", "allow", "personal", "derive", "llm"]);
  expect(await admin(db, ["grant", "list"])).toBe("personal\tderive\tllm\npersonal\tsync\tmemex");
  await admin(db, ["grant", "revoke", "personal", "derive", "llm"]);
  expect(await admin(db, ["grant", "list"])).toBe("personal\tsync\tmemex");
  await expect(admin(db, ["grant", "allow", "personal", "sync", "elsewhere"])).rejects.toThrow("destination");
  await expect(admin(db, ["grant", "allow", "personal", "lunch", "memex"])).rejects.toThrow("purpose");
  await expect(admin(db, ["grant", "allow", "Bad Scope", "sync", "memex"])).rejects.toThrow("scope");
});

test("usage on an unknown command", async () => {
  await expect(admin(db, ["nope"])).rejects.toThrow("usage");
});
