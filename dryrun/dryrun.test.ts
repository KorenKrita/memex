// Synthetic fixtures only: every log here is written by the test into a temp dir.
import { describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { analyzeFile, codexForkBoundary, codexIsChild, dryRun, SPAN_MAX, SPAN_MIN, units, type Rec } from "./dryrun";

const dir = mkdtempSync(join(tmpdir(), "memex-dryrun-"));

const jsonl = (rows: unknown[]) => rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
function file(name: string, rows: unknown[] | string): string {
  const p = join(dir, name);
  mkdirSync(join(p, ".."), { recursive: true });
  writeFileSync(p, typeof rows === "string" ? rows : jsonl(rows));
  return p;
}

// ---------- Pi fixtures ----------
const piHeader = (extra: object = {}) => ({ type: "session", version: 3, id: "s1", timestamp: "2026-10-01T00:00:00Z", cwd: "/w", ...extra });
let n = 0;
const piMsg = (id: string, parentId: string | null, message: object) => ({ type: "message", id, parentId, timestamp: `2026-10-01T00:00:${String(n++ % 60).padStart(2, "0")}Z`, message });
const user = (text: unknown) => ({ role: "user", content: text, timestamp: 1 });
const asst = (content: unknown[]) => ({ role: "assistant", content, api: "x", provider: "x", model: "x", timestamp: 1 });
const result = (toolCallId: string, text: string, isError = false) => ({ role: "toolResult", toolCallId, toolName: "bash", content: [{ type: "text", text }], isError, timestamp: 1 });

describe("Pi", () => {
  test("thinking, memex snapshot, memex custom message and other memory systems stay out of spans", () => {
    const p = file("pi/inject.jsonl", [
      piHeader(),
      piMsg("a", null, { role: "system", sections: { memex: "SNAPSHOT", base: "BASE" }, timestamp: 1 }),
      { type: "custom_message", id: "b", parentId: "a", customType: "memex-recall", content: "RECALLED", display: false, timestamp: "t" },
      piMsg("c", "b", user("<openviking-context>OVBODY</openviking-context>\nreal question")),
      piMsg("d", "c", asst([{ type: "thinking", thinking: "SECRET-THOUGHT" }, { type: "text", text: "answer" }])),
    ]);
    const r = analyzeFile(p);
    expect(r.status).toBe("indexed");
    expect(r.evidence).toBe(2); // the user message and the reply
    expect(r.excluded.thinking).toBe(Buffer.byteLength("SECRET-THOUGHT"));
    expect(r.excluded["memex-injected"]).toBe(Buffer.byteLength("RECALLED"));
    expect(r.excluded["other-memory-injected"]).toBe(Buffer.byteLength("<openviking-context>OVBODY</openviking-context>"));
    expect(r.excluded["system-prompt"]).toBeGreaterThan(0);
    // digest = "user: real question\n" + "assistant: answer\n"
    expect(r.digestBytes).toBe(Buffer.byteLength("user: real question\nassistant: answer\n"));
  });

  test("off-path entries are counted, not merged into the inferred-tail view", () => {
    const p = file("pi/branch.jsonl", [
      piHeader(),
      piMsg("u1", null, user("first")),
      piMsg("a1", "u1", asst([{ type: "text", text: "old branch reply" }])),
      piMsg("b1", "u1", asst([{ type: "text", text: "new branch reply" }])), // last entry = resume leaf
    ]);
    const r = analyzeFile(p);
    expect(r.leafBasis).toBe("inferred-tail");
    expect(r.offPath).toBe(1);
    expect(r.evidence).toBe(2);
    expect(r.lexicalBytes).toBe(Buffer.byteLength("first") + Buffer.byteLength("new branch reply"));
  });

  test("a torn last line is counted and skipped", () => {
    const good = jsonl([piHeader(), piMsg("u1", null, user("hi"))]);
    const r = analyzeFile(file("pi/torn.jsonl", good + '{"type":"message","id":"u2","par'));
    expect(r.tornLines).toBe(1);
    expect(r.records).toBe(1);
  });

  test("subagent transcripts are skipped without reading them", () => {
    const p = file("pi/subagent-artifacts/x_transcript.jsonl", "not json at all");
    const r = analyzeFile(p);
    expect(r).toMatchObject({ status: "skipped", reason: "child-thread", tornLines: 0 });
  });

  test("fork prefix is inherited only when id and content match a parent given in the same run", () => {
    const parentRows = [piHeader(), piMsg("u1", null, user("shared")), piMsg("a1", "u1", asst([{ type: "text", text: "shared reply" }]))];
    const parent = file("pi/fork/parent.jsonl", parentRows);
    const edited = { ...parentRows[2], message: asst([{ type: "text", text: "same id, different content" }]) } as any;
    const child = file("pi/fork/child.jsonl", [piHeader({ id: "s2", parentSession: parent }), parentRows[1], edited, piMsg("u2", "a1", user("new in child"))]);
    const solo = dryRun([child]).perFile[0];
    expect(solo.forkStatus).toBe("parent-not-given");
    expect(solo.inherited).toBe(0);
    const both = dryRun([join(dir, "pi/fork")]).perFile.find((f) => f.path === child)!;
    expect(both.forkStatus).toBe("resolved");
    expect(both.inherited).toBe(1); // u1 only; the edited a1 is new evidence
    expect(both.evidence).toBe(2);
  });

  test("unsupported header version is skipped", () => {
    expect(analyzeFile(file("pi/v2.jsonl", [{ type: "session", version: 2, id: "x" }])).reason).toBe("unsupported-version");
  });
});

// ---------- Codex fixtures ----------
const meta = (extra: object = {}) => ({ timestamp: "t", ordinal: 0, type: "session_meta", payload: { id: "11111111-1111-1111-1111-111111111111", history_mode: "paginated", source: "cli", ...extra } });
const ri = (ordinal: number, payload: object) => ({ timestamp: "t", ordinal, type: "response_item", payload });
const cuser = (text: string, kinds?: string[]) => ri(0, { type: "message", role: "user", content: [{ type: "input_text", text }], ...(kinds ? { internal_chat_message_metadata_passthrough: { content_item_kinds: kinds } } : {}) });
const withOrd = (rows: any[]) => rows.map((r, i) => ({ ...r, ordinal: i }));

describe("Codex", () => {
  test("Legacy and missing ordinals are skipped, not supported", () => {
    const legacy = file("codex/legacy.jsonl", [{ ...meta({ history_mode: "legacy" }) }, { timestamp: "t", type: "response_item", payload: {} }]);
    expect(analyzeFile(legacy).reason).toBe("legacy");
    const holes = file("codex/holes.jsonl", [meta(), { timestamp: "t", type: "response_item", payload: {} }]);
    expect(analyzeFile(holes).reason).toBe("no-ordinal");
    const absent = file("codex/absent-mode.jsonl", [{ timestamp: "t", type: "session_meta", payload: { id: "x" } }]);
    expect(analyzeFile(absent).reason).toBe("legacy"); // serde default is Legacy
  });

  test("child-thread classification follows thread_source, then source", () => {
    expect(codexIsChild({ thread_source: "subagent" })).toBe(true);
    expect(codexIsChild({ thread_source: "guardian_review" })).toBe(true);
    expect(codexIsChild({ thread_source: "user", source: { subagent: "review" } })).toBe(false);
    expect(codexIsChild({ source: { subagent: { other: "guardian" } } })).toBe(true);
    expect(codexIsChild({ source: { internal: "guardian" } })).toBe(true);
    expect(codexIsChild({ source: "cli" })).toBe(false);
  });

  test("only response items count; event_msg, reasoning, harness context and memex hooks are excluded", () => {
    const p = file(
      "codex/main.jsonl",
      withOrd([
        meta(),
        cuser("# AGENTS.md instructions for /w\n<INSTRUCTIONS>rules</INSTRUCTIONS>"),
        cuser("<environment_context>cwd</environment_context>"),
        cuser("annotated context", ["hooks.additional_context"]),
        ri(0, { type: "message", role: "developer", content: [{ type: "input_text", text: "<memex-snapshot>old cards</memex-snapshot>" }], internal_chat_message_metadata_passthrough: { content_item_kinds: ["hooks.additional_context"] } }),
        cuser("fix the bug", ["user.text"]),
        { timestamp: "t", ordinal: 0, type: "event_msg", payload: { type: "user_message", message: "fix the bug" } },
        ri(0, { type: "reasoning", summary: [{ type: "summary_text", text: "THINK" }], encrypted_content: "zzz" }),
        ri(0, { type: "function_call", name: "shell", arguments: '{"cmd":"ls"}', call_id: "c1" }),
        ri(0, { type: "function_call_output", call_id: "c1", output: "a.txt" }),
        ri(0, { type: "message", role: "assistant", content: [{ type: "output_text", text: "done" }] }),
      ]),
    );
    const r = analyzeFile(p);
    expect(r.status).toBe("indexed");
    expect(r.evidence).toBe(4); // user, call, output, reply
    expect(r.exchanges).toBe(1);
    expect(r.excluded.thinking).toBe(5);
    expect(r.excluded["memex-injected"]).toBe(Buffer.byteLength("<memex-snapshot>old cards</memex-snapshot>"));
    expect(r.excluded["harness-context"]).toBeGreaterThan(0);
  });

  test("fork boundary reuses Codex's rule; unknown boundary quarantines the file", () => {
    const parent = "22222222-2222-2222-2222-222222222222";
    const id = "11111111-1111-1111-1111-111111111111";
    expect(codexForkBoundary({ forked_from_id: parent, forked_from_ordinal_exclusive: 3, history_base: { thread_id: parent, end_ordinal_exclusive: 9 } }, "x")).toBe(3);
    expect(codexForkBoundary({ forked_from_id: parent, history_base: { thread_id: parent, end_ordinal_exclusive: 9 } }, "x")).toBe(9);
    const reverted = "33333333-3333-3333-3333-333333333333";
    expect(codexForkBoundary({ id, forked_from_id: parent, history_base: { thread_id: reverted, end_ordinal_exclusive: 9 } }, `rollout-2026-10-01T00-00-00-${id}_${reverted}.jsonl`)).toBeUndefined();
    expect(codexForkBoundary({ id, forked_from_id: parent, history_base: { thread_id: reverted, end_ordinal_exclusive: 9 } }, `rollout-2026-10-01T00-00-00-${id}.jsonl`)).toBe(9);

    const rows = withOrd([meta({ forked_from_id: parent, forked_from_ordinal_exclusive: 3 }), cuser("old", ["user.text"]), ri(0, { type: "message", role: "assistant", content: [{ type: "output_text", text: "old reply" }] }), cuser("new", ["user.text"])]);
    const r = analyzeFile(file("codex/fork.jsonl", rows));
    expect(r).toMatchObject({ forkStatus: "resolved", inherited: 3, evidence: 1 });
    const unknown = analyzeFile(file("codex/fork-unknown.jsonl", withOrd([meta({ forked_from_id: parent }), cuser("x", ["user.text"])])));
    expect(unknown).toMatchObject({ forkStatus: "unresolved", unresolved: 2, evidence: 0, spans: 0 });
  });

  test("a child thread file is skipped before any record is read", () => {
    const p = file("codex/child.jsonl", withOrd([meta({ thread_source: "subagent" }), cuser("child work", ["user.text"])]));
    expect(analyzeFile(p)).toMatchObject({ status: "skipped", reason: "child-thread", records: 0 });
  });
});

// ---------- splitting and sizes ----------
describe("spans and sizes", () => {
  test("UTF-8 bytes, not characters", () => {
    const r = analyzeFile(file("pi/utf8.jsonl", [piHeader(), piMsg("u1", null, user("记忆系统"))]));
    expect(r.lexicalBytes).toBe(12);
    expect(r.digestBytes).toBe(Buffer.byteLength("user: 记忆系统\n"));
  });

  test("a long exchange splits at tool boundaries, never between a call and its result", () => {
    const rows: unknown[] = [piHeader(), piMsg("u1", null, user("go"))];
    let parent = "u1";
    for (let i = 0; i < 12; i++) {
      rows.push(piMsg(`c${i}`, parent, asst([{ type: "toolCall", id: `t${i}`, name: "bash", arguments: { i } }])));
      rows.push(piMsg(`r${i}`, `c${i}`, result(`t${i}`, "x".repeat(1500))));
      parent = `r${i}`;
    }
    const r = analyzeFile(file("pi/long.jsonl", rows));
    expect(r.exchanges).toBe(1);
    expect(r.spans).toBeGreaterThan(1);
    expect(r.oversizedSpans).toBe(0);
    // every span but the last holds at least SPAN_MIN bytes of digest
    expect(r.digestBytes).toBeGreaterThan(SPAN_MIN * (r.spans - 1));
  });

  test("a tool call and its result always land in the same span", () => {
    const rec = (kind: Rec["kind"]): Rec => ({ id: kind, kind, text: kind });
    const u = units([rec("user"), rec("tool_call"), rec("tool_call"), rec("tool_result"), rec("tool_result"), rec("assistant"), rec("tool_result")]);
    expect(u.map((x) => x.map((r) => r.kind).join("+"))).toEqual(["user", "tool_call+tool_call+tool_result+tool_result", "assistant", "tool_result"]);
    // user+text ≈ 4.1KB, call ≈ 7KB, result ≈ 1.5KB: cutting between call and result would give
    // [user,text,call][result] = 2 spans with the result alone; kept whole it is [user,text][call,result]
    const rows: unknown[] = [piHeader(), piMsg("u1", null, user("go")), piMsg("a0", "u1", asst([{ type: "text", text: "p".repeat(SPAN_MIN) }]))];
    rows.push(piMsg("c", "a0", asst([{ type: "toolCall", id: "t", name: "bash", arguments: { s: "a".repeat(7000) } }])));
    rows.push(piMsg("r", "c", result("t", "b".repeat(1500))));
    const r = analyzeFile(file("pi/pair.jsonl", rows));
    expect(r.spans).toBe(2);
  });

  test("a single unit larger than SPAN_MAX stays whole and is reported", () => {
    const big = "y".repeat(SPAN_MAX * 2);
    const r = analyzeFile(file("pi/big.jsonl", [piHeader(), piMsg("u1", null, user("q")), piMsg("a1", "u1", asst([{ type: "text", text: big }]))]));
    expect(r.spans).toBe(1);
    expect(r.oversizedSpans).toBe(1);
  });

  test("tool output middle is cut from digest but fully counted for lexical chunks", () => {
    const out = "head\n" + "m".repeat(5000) + "\nERROR: middle failure\n" + "m".repeat(5000) + "\ntail";
    const r = analyzeFile(
      file("pi/tool.jsonl", [piHeader(), piMsg("u1", null, user("q")), piMsg("c", "u1", asst([{ type: "toolCall", id: "t", name: "bash", arguments: {} }])), piMsg("r", "c", result("t", out))]),
    );
    expect(r.lexicalBytes).toBeGreaterThan(10000);
    expect(r.digestBytes).toBeLessThan(4000);
    expect(r.lexicalChunks).toBe(1 + 1 + Math.ceil(Buffer.byteLength(out) / 2048)); // "q", call, output
  });

  test("forest internal nodes = spans - popcount(spans)", () => {
    const rows: unknown[] = [piHeader()];
    let parent: string | null = null;
    for (let i = 0; i < 13; i++) {
      rows.push(piMsg(`u${i}`, parent, user(`q${i}`)));
      parent = `u${i}`;
    }
    const r = analyzeFile(file("pi/forest.jsonl", rows));
    expect(r.spans).toBe(13);
    expect(r.forestInternalNodes).toBe(13 - 3);
  });
});

test("dry run never writes into its inputs", () => {
  const p = file("pi/ro.jsonl", [piHeader(), piMsg("u1", null, user("hi"))]);
  const before = readFileSync(p);
  const report = dryRun([join(dir, "pi"), join(dir, "codex")]);
  const subagents = report.skipped["child-thread"] ?? [];
  expect(readFileSync(p).equals(before)).toBe(true);
  expect(report.skipped.legacy?.length).toBeGreaterThan(0);
  expect(subagents.some((p) => p.includes("subagent-artifacts"))).toBe(true);
  expect(subagents.some((p) => p.endsWith("codex/child.jsonl"))).toBe(true);
});
