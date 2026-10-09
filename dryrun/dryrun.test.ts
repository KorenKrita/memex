// Synthetic fixtures only: every log here is written by the test into a temp dir.
import { describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { analyzeFile, codexForkBoundary, codexIsChild, collect, dryRun, type Rec, SPAN_MAX, spansOf, units } from "./dryrun";

const dir = mkdtempSync(join(tmpdir(), "memex-dryrun-"));
const jsonl = (rows: unknown[]) => rows.map((r) => (typeof r === "string" ? r : JSON.stringify(r))).join("\n") + "\n";
function file(name: string, rows: unknown[] | string): string {
  const p = join(dir, name);
  mkdirSync(join(p, ".."), { recursive: true });
  writeFileSync(p, typeof rows === "string" ? rows : jsonl(rows));
  return p;
}
const B = (s: string) => Buffer.byteLength(s);

// ---------- Pi fixtures ----------
const piHeader = (extra: object = {}) => ({ type: "session", version: 3, id: "s1", timestamp: "2026-10-01T00:00:00Z", cwd: "/w", ...extra });
const piMsg = (id: string, parentId: string | null, message: object) => ({ type: "message", id, parentId, timestamp: "2026-10-01T00:00:00Z", message });
const user = (text: unknown) => ({ role: "user", content: text, timestamp: 1 });
const asst = (content: unknown[]) => ({ role: "assistant", content, api: "x", provider: "x", model: "x", timestamp: 1 });
const say = (text: string) => asst([{ type: "text", text }]);
const call = (id: string, name = "bash", args: object = {}) => asst([{ type: "toolCall", id, name, arguments: args }]);
const result = (toolCallId: string, text: string, toolName = "bash") => ({ role: "toolResult", toolCallId, toolName, content: [{ type: "text", text }], isError: false, timestamp: 1 });

describe("Pi", () => {
  test("memex snapshot section, memex / other-memory custom messages and memory blocks never become text", () => {
    const p = file("pi/inject.jsonl", [
      piHeader(),
      piMsg("a", null, { role: "system", sections: { memex: "SNAPSHOT", base: "BASE RULES" }, timestamp: 1 }),
      { type: "custom_message", id: "b", parentId: "a", customType: "memex-recall", content: "RECALLED", display: false, timestamp: "t" },
      { type: "custom_message", id: "c", parentId: "b", customType: "memini-recall", content: "MEMINI", display: false, timestamp: "t" },
      piMsg("d", "c", user("<openviking-context>OVBODY</openviking-context>\nreal question")),
      piMsg("e", "d", asst([{ type: "thinking", thinking: "THOUGHT" }, { type: "text", text: "answer" }])),
    ]);
    const r = analyzeFile(p);
    expect(r.excluded["memex-injected"]).toEqual({ records: 2, bytes: B("SNAPSHOT") + B("RECALLED") });
    expect(r.excluded["other-memory-injected"]).toEqual({ records: 2, bytes: B("MEMINI") + B("<openviking-context>OVBODY</openviking-context>") });
    // the rest of the system prompt and the thinking stay in L0 / lexical, not in the digest
    expect(r.digestExcluded.context).toEqual({ records: 1, bytes: B("BASE RULES") });
    expect(r.digestExcluded.thinking).toEqual({ records: 1, bytes: B("THOUGHT") });
    expect(r.digestBytes).toBe(B("user: real question\nassistant: answer\n"));
    expect(r.lexicalBytes).toBe(B("BASE RULES") + B("real question") + B("answer") + B("THOUGHT"));
    expect(r.evidence).toBe(2);
    expect(r.exchanges).toBe(1);
  });

  test("memory tool results are excluded, ordinary tool output keeps its text minus memory blocks", () => {
    const p = file("pi/memtool.jsonl", [
      piHeader(),
      piMsg("u", null, user("q")),
      piMsg("c1", "u", call("t1", "mem_search")),
      piMsg("r1", "c1", result("t1", "RECALLED_EVIDENCE", "mem_search")),
      piMsg("c2", "r1", call("t2", "openviking_find")),
      piMsg("r2", "c2", result("t2", "OV_HIT", "openviking_find")),
      piMsg("c3", "r2", call("t3")),
      piMsg("r3", "c3", result("t3", "ok <openviking-context>X</openviking-context>")),
    ]);
    const r = analyzeFile(p);
    expect(r.excluded["memex-tool-result"]).toEqual({ records: 1, bytes: B("RECALLED_EVIDENCE") });
    expect(r.excluded["other-memory-tool-result"]).toEqual({ records: 1, bytes: B("OV_HIT") });
    expect(r.excluded["other-memory-injected"]?.records).toBe(1);
    expect(r.lexicalBytes).not.toBeGreaterThan(B("q") + 200 + B("ok"));
  });

  test("every branch is counted once; shared prefixes are not duplicated", () => {
    // u1 → a1 (old reply) and u1 → b1 → u2 → b2 (new branch, the inferred tail)
    const p = file("pi/branch.jsonl", [
      piHeader(),
      piMsg("u1", null, user("first")),
      piMsg("a1", "u1", say("old reply")),
      piMsg("b1", "u1", say("new reply")),
      piMsg("u2", "b1", user("second")),
      piMsg("b2", "u2", say("done")),
    ]);
    const r = analyzeFile(p);
    expect(r.offTail).toBe(1);
    expect(r.branchPoints).toBe(1);
    expect(r.evidence).toBe(5);
    expect(r.lexicalBytes).toBe(B("first") + B("old reply") + B("new reply") + B("second") + B("done"));
    // u1 alone, [a1], [b1], [u2 b2]: the shared user message is in one span only
    expect(r.spans).toBe(4);
    expect(r.exchanges).toBe(2);
  });

  test("a fork inside an exchange does not merge the two branches into one span", () => {
    const p = file("pi/inspan.jsonl", [
      piHeader(),
      piMsg("u1", null, user("go")),
      piMsg("c", "u1", call("t")),
      piMsg("r", "c", result("t", "out")),
      piMsg("a2", "r", say("branch A")),
      piMsg("b2", "r", say("branch B")),
    ]);
    const r = analyzeFile(p);
    expect(r.spans).toBe(3); // [u1 c r], [a2], [b2]
    expect(r.digestBytes).toBe(B("user: go\n") + B('tool_call: bash {}\n') + B("tool_result: out\n") + B("assistant: branch A\n") + B("assistant: branch B\n"));
  });

  test("torn last line vs malformed lines", () => {
    const good = jsonl([piHeader(), piMsg("u1", null, user("hi")), "null", "[1]", "{bad"]);
    const r = analyzeFile(file("pi/torn.jsonl", good + '{"type":"message","id":"u2","par'));
    expect(r.tornLines).toBe(1);
    expect(r.malformedLines).toBe(3);
    expect(r.records).toBe(1);
    expect(r.status).toBe("indexed");
  });

  test("subagent transcripts are not read, not even by the fork pre-pass", () => {
    const sub = file("pi/sub/subagent-artifacts/x_transcript.jsonl", jsonl([piHeader(), piMsg("u", null, user("child"))]));
    const child = file("pi/sub/child.jsonl", [piHeader({ parentSession: sub }), piMsg("u", null, user("child"))]);
    const spy = spyOn(fs, "readFileSync");
    const report = dryRun([join(dir, "pi/sub")]);
    const read = spy.mock.calls.map((c) => String(c[0]));
    spy.mockRestore();
    expect(read).not.toContain(sub);
    expect(report.skipped["child-thread"]).toEqual([sub]);
    expect(report.perFile.find((f) => f.path === child)!.forkStatus).toBe("parent-not-given");
  });

  test("fork inheritance: same id and content (key order irrelevant); parent not given → quarantined", () => {
    const parentRows = [piHeader(), piMsg("u1", null, user("shared")), piMsg("a1", "u1", say("shared reply"))];
    const parent = file("pi/fork/parent.jsonl", parentRows);
    const reordered = Object.fromEntries(Object.entries(parentRows[1] as object).reverse());
    const edited = { ...(parentRows[2] as object), message: say("same id, different content") };
    const childRows = [piHeader({ id: "s2", parentSession: parent }), reordered, edited, piMsg("u2", "a1", user("new in child"))];
    const child = file("pi/fork/child.jsonl", childRows);
    const solo = dryRun([child]).perFile[0];
    expect(solo).toMatchObject({ forkStatus: "parent-not-given", unresolved: 3, evidence: 0, spans: 0 });
    const both = dryRun([join(dir, "pi/fork")]).perFile.find((f) => f.path === child)!;
    expect(both).toMatchObject({ forkStatus: "resolved", inherited: 1, evidence: 2 });
  });

  test("unsupported header version is skipped", () => {
    expect(analyzeFile(file("pi/v2.jsonl", [{ type: "session", version: 2, id: "x" }])).reason).toBe("unsupported-version");
  });
});

// ---------- Codex fixtures ----------
const meta = (extra: object = {}) => ({ timestamp: "t", ordinal: 0, type: "session_meta", payload: { id: "11111111-1111-1111-1111-111111111111", history_mode: "paginated", source: "cli", ...extra } });
const ri = (payload: object) => ({ timestamp: "t", ordinal: 0, type: "response_item", payload });
const cmsg = (role: string, parts: string[], kinds?: string[]) =>
  ri({ type: "message", role, content: parts.map((text) => ({ type: role === "assistant" ? "output_text" : "input_text", text })), ...(kinds ? { internal_chat_message_metadata_passthrough: { content_item_kinds: kinds } } : {}) });
const withOrd = (rows: any[]) => rows.map((r, i) => ({ ...r, ordinal: i }));

describe("Codex", () => {
  test("Legacy and missing ordinals are skipped, not supported", () => {
    expect(analyzeFile(file("codex/legacy.jsonl", [meta({ history_mode: "legacy" })])).reason).toBe("legacy");
    expect(analyzeFile(file("codex/holes.jsonl", [meta(), { timestamp: "t", type: "response_item", payload: {} }])).reason).toBe("no-ordinal");
    expect(analyzeFile(file("codex/absent-mode.jsonl", [{ timestamp: "t", type: "session_meta", payload: { id: "x" } }])).reason).toBe("legacy"); // serde default
  });

  test("child-thread classification follows thread_source, then source", () => {
    expect(codexIsChild({ thread_source: "subagent" })).toBe(true);
    expect(codexIsChild({ thread_source: "guardian_review" })).toBe(true);
    expect(codexIsChild({ thread_source: "user", source: { subagent: "review" } })).toBe(false);
    expect(codexIsChild({ source: { subagent: { other: "guardian" } } })).toBe(true);
    expect(codexIsChild({ source: { internal: "guardian" } })).toBe(true);
    expect(codexIsChild({ source: "cli" })).toBe(false);
    const p = file("codex/child.jsonl", withOrd([meta({ thread_source: "subagent" }), cmsg("user", ["child work"], ["user.text"])]));
    expect(analyzeFile(p)).toMatchObject({ status: "skipped", reason: "child-thread", records: 0 });
  });

  test("fragments are classified one by one: user provenance kept, harness context kept out of the digest", () => {
    const p = file(
      "codex/frag.jsonl",
      withOrd([
        meta(),
        cmsg("user", ["# AGENTS.md instructions for /w\n<INSTRUCTIONS>rules</INSTRUCTIONS>"]), // unannotated, marker
        cmsg("user", ["<environment_context>cwd</environment_context>", "real ask"], ["environment.context", "user.text"]),
        cmsg("user", ['<codex_internal_context source="user_goal">ship it</codex_internal_context>'], ["user.goal"]),
        cmsg("developer", ["<memex-snapshot>old cards</memex-snapshot>"], ["hooks.additional_context"]),
        cmsg("developer", ["<permissions instructions>sandbox</permissions instructions>"]),
        { timestamp: "t", ordinal: 0, type: "event_msg", payload: { type: "user_message", message: "real ask" } },
        ri({ type: "reasoning", summary: [{ type: "summary_text", text: "THINK" }], encrypted_content: "zzz" }),
        cmsg("assistant", ["done"]),
      ]),
    );
    const r = analyzeFile(p);
    expect(r.exchanges).toBe(2); // "real ask" and the user goal
    expect(r.digestBytes).toBe(B("user: real ask\n") + B('user: <codex_internal_context source="user_goal">ship it</codex_internal_context>\n') + B("assistant: done\n"));
    expect(r.excluded["memex-injected"]).toEqual({ records: 1, bytes: B("<memex-snapshot>old cards</memex-snapshot>") });
    expect(r.digestExcluded.context.records).toBe(3); // AGENTS.md, environment_context fragment, permissions
    expect(r.digestExcluded.thinking).toEqual({ records: 1, bytes: 5 });
    expect(r.nonContent.event_msg).toBe(1);
  });

  test("memory tool outputs are matched to their call by call_id", () => {
    const p = file(
      "codex/memtool.jsonl",
      withOrd([
        meta(),
        cmsg("user", ["q"], ["user.text"]),
        ri({ type: "function_call", name: "mcp__memex__mem_zoom", arguments: "{}", call_id: "c1" }),
        ri({ type: "function_call_output", call_id: "c1", output: "ZOOMED" }),
        ri({ type: "function_call", name: "shell", arguments: "{}", call_id: "c2" }),
        ri({ type: "function_call_output", call_id: "c2", output: "ls output" }),
      ]),
    );
    const r = analyzeFile(p);
    expect(r.excluded["memex-tool-result"]).toEqual({ records: 1, bytes: B("ZOOMED") });
    expect(r.lexicalBytes).toBe(B("q") + B("mcp__memex__mem_zoom {}") + B("[memex tool result]") + B("shell {}") + B("ls output"));
  });

  test("fork boundary reuses Codex's rule; unknown boundary quarantines the file", () => {
    const parent = "22222222-2222-2222-2222-222222222222";
    const id = "11111111-1111-1111-1111-111111111111";
    const reverted = "33333333-3333-3333-3333-333333333333";
    expect(codexForkBoundary({ forked_from_id: parent, forked_from_ordinal_exclusive: 3, history_base: { thread_id: parent, end_ordinal_exclusive: 9 } }, "x")).toBe(3);
    expect(codexForkBoundary({ forked_from_id: parent, history_base: { thread_id: parent, end_ordinal_exclusive: 9 } }, "x")).toBe(9);
    expect(codexForkBoundary({ id, forked_from_id: parent, history_base: { thread_id: reverted, end_ordinal_exclusive: 9 } }, `rollout-2026-10-01T00-00-00-${id}_${reverted}.jsonl`)).toBeUndefined();
    expect(codexForkBoundary({ id, forked_from_id: parent, history_base: { thread_id: reverted, end_ordinal_exclusive: 9 } }, `rollout-2026-10-01T00-00-00-${id}.jsonl`)).toBe(9);
    const rows = withOrd([meta({ forked_from_id: parent, forked_from_ordinal_exclusive: 3 }), cmsg("user", ["old"], ["user.text"]), cmsg("assistant", ["old reply"]), cmsg("user", ["new"], ["user.text"])]);
    expect(analyzeFile(file("codex/fork.jsonl", rows))).toMatchObject({ forkStatus: "resolved", inherited: 3, evidence: 1 });
    const unknown = analyzeFile(file("codex/fork-unknown.jsonl", withOrd([meta({ forked_from_id: parent }), cmsg("user", ["x"], ["user.text"])])));
    expect(unknown).toMatchObject({ forkStatus: "unresolved", unresolved: 2, evidence: 0, spans: 0 });
  });
});

// ---------- units, spans and sizes ----------
const rec = (kind: Rec["kind"], callId?: string, size = 1): Rec => ({ id: `${kind}${callId ?? ""}`, kind, callId, text: "x".repeat(size) });
const shape = (us: Rec[][]) => us.map((u) => u.map((r) => r.id).join("+"));

describe("units and spans", () => {
  test("a call and its result stay together across commentary, parallel calls and out-of-order results", () => {
    expect(shape(units([rec("user"), rec("tool_call", "A"), rec("assistant"), rec("tool_result", "A"), rec("assistant")]))).toEqual([
      "user",
      "tool_callA+assistant+tool_resultA",
      "assistant",
    ]);
    expect(shape(units([rec("tool_call", "A"), rec("tool_call", "B"), rec("tool_result", "B"), rec("tool_result", "A"), rec("user")]))).toEqual([
      "tool_callA+tool_callB+tool_resultB+tool_resultA",
      "user",
    ]);
    // a result that never arrives keeps the unit open to the end of the segment
    expect(shape(units([rec("tool_call", "A"), rec("assistant"), rec("assistant")]))).toEqual(["tool_callA+assistant+assistant"]);
  });

  test("SPAN_MAX wins over SPAN_MIN: short user + 10KB reply become two spans", () => {
    const s = spansOf([rec("user", undefined, 3000), rec("assistant", undefined, 10000)]);
    expect(s.map((x) => x.oversized)).toEqual([false, false]);
    expect(s.length).toBe(2);
  });

  test("only a single indivisible unit may exceed SPAN_MAX", () => {
    const s = spansOf([rec("user"), rec("tool_call", "A", 9000), rec("assistant", undefined, 9000), rec("tool_result", "A", 10)]);
    expect(s.length).toBe(2);
    expect(s[1].oversized).toBe(true);
    expect(s[1].recs.map((r) => r.kind)).toEqual(["tool_call", "assistant", "tool_result"]);
  });

  test("UTF-8 bytes, not characters", () => {
    const r = analyzeFile(file("pi/utf8.jsonl", [piHeader(), piMsg("u1", null, user("记忆系统"))]));
    expect(r.lexicalBytes).toBe(12);
    expect(r.digestBytes).toBe(B("user: 记忆系统\n"));
  });

  test("tool output middle is cut from the digest but fully in lexical chunks", () => {
    const out = "head\n" + "m".repeat(5000) + "\nERROR: middle failure\n" + "m".repeat(5000) + "\ntail";
    const r = analyzeFile(file("pi/tool.jsonl", [piHeader(), piMsg("u1", null, user("q")), piMsg("c", "u1", call("t")), piMsg("r", "c", result("t", out))]));
    expect(r.lexicalBytes).toBe(B("q") + B("bash {}") + B(out));
    expect(r.digestBytes).toBeLessThan(3000);
    expect(r.lexicalChunks).toBe(1 + 1 + Math.ceil(B(out) / 2048));
  });

  test("forest internal nodes = spans - popcount(spans)", () => {
    const rows: unknown[] = [piHeader()];
    for (let i = 0; i < 13; i++) rows.push(piMsg(`u${i}`, i ? `u${i - 1}` : null, user(`q${i}`)));
    const r = analyzeFile(file("pi/forest.jsonl", rows));
    expect(r.spans).toBe(13);
    expect(r.forestInternalNodes).toBe(10);
  });
});

describe("input boundary", () => {
  test("symlinks are reported, never followed; overlapping inputs are deduplicated", () => {
    const outside = mkdtempSync(join(tmpdir(), "memex-outside-"));
    writeFileSync(join(outside, "secret.jsonl"), jsonl([piHeader(), piMsg("u", null, user("outside"))]));
    const root = join(dir, "bound");
    mkdirSync(join(root, "sub"), { recursive: true });
    const inside = join(root, "sub", "in.jsonl");
    writeFileSync(inside, jsonl([piHeader(), piMsg("u", null, user("inside"))]));
    symlinkSync(outside, join(root, "linked"));
    symlinkSync(root, join(root, "sub", "loop"));
    const { files, symlinks } = collect([root, join(root, "sub"), inside, inside]);
    expect(files).toEqual([inside]);
    expect(symlinks).toEqual([join(root, "linked"), join(root, "sub", "loop")]);
    expect(dryRun([root, inside]).total.evidence).toBe(1);
  });

  test("an unreadable file is reported and does not stop the batch", () => {
    const root = join(dir, "batch");
    file("batch/ok.jsonl", [piHeader(), piMsg("u", null, user("fine"))]);
    file("batch/weird.jsonl", [piHeader(), { type: "message", id: "x", parentId: null, message: { role: "assistant", content: "not-an-array" } }]);
    file("batch/odd.jsonl", ["null", piHeader()]); // a stray null line is malformed, the rest still parses
    file("batch/ledger.jsonl", [{ kind: "ledger" }]);
    chmodSync(file("batch/locked.jsonl", [piHeader()]), 0);
    const report = dryRun([root]);
    expect(report.indexed.pi).toBe(3);
    expect(report.skipped.unreadable).toEqual([join(root, "locked.jsonl")]);
    expect(report.perFile.find((f) => f.path.endsWith("odd.jsonl"))!.malformedLines).toBe(1);
    expect(report.skipped["unknown-format"]).toEqual([join(root, "ledger.jsonl")]);
  });

  test("dry run never writes into its inputs", () => {
    const p = file("ro/ro.jsonl", [piHeader(), piMsg("u1", null, user("hi"))]);
    const before = readFileSync(p);
    dryRun([join(dir, "ro")]);
    expect(readFileSync(p).equals(before)).toBe(true);
  });
});

test("span size limits", () => {
  expect(SPAN_MAX).toBe(12288);
});
