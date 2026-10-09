// Synthetic fixtures only: every log here is written by the test into a temp dir.
import { describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, renameSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { analyzeFile, codexForkBoundary, codexIsChild, collect, dryRun } from "./dryrun";
const SPAN_MAX = 12288; // DESIGN §4.3 upper target; mirrored here, not imported
const D = (kind: string, text: string) => B(`${kind}: ${text}`) + 1; // one record's digest bytes

const dir = realpathSync(mkdtempSync(join(tmpdir(), "memex-dryrun-")));
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
    // views in creation order: S = [u1 a1]; the second view forks inside S → P = [u1], then [b1], [u2 b2]
    expect(r).toMatchObject({ views: 2, spans: 4, prefixSpans: 1, exchanges: 2 });
    expect(r.summaryInputBytes).toBe(2 * D("user", "first") + D("assistant", "old reply") + D("assistant", "new reply") + D("user", "second") + D("assistant", "done"));
    expect(r.digestBytes).toBe(r.summaryInputBytes - D("user", "first")); // unique records only
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
    // DESIGN §4.3 example: S = [u1 c r a2] closed, fork at r → P = [u1 c r] + [b2]; S is kept for its view
    expect(r).toMatchObject({ views: 2, spans: 3, prefixSpans: 1 });
    const prefix = D("user", "go") + D("tool_call", "bash {}") + D("tool_result", "out");
    expect(r.digestBytes).toBe(prefix + D("assistant", "branch A") + D("assistant", "branch B"));
    expect(r.summaryInputBytes).toBe(prefix + D("assistant", "branch A") + prefix + D("assistant", "branch B"));
  });

  test("forest nodes are counted per branch with shared prefixes once", () => {
    // 3 shared user messages, then 2 branches of 2 more each: depths 1-3 shared, 4-5 on each branch
    const rows: unknown[] = [piHeader()];
    for (let i = 0; i < 3; i++) rows.push(piMsg(`s${i}`, i ? `s${i - 1}` : null, user(`shared ${i}`)));
    for (const b of ["a", "b"]) for (let i = 0; i < 2; i++) rows.push(piMsg(`${b}${i}`, i ? `${b}${i - 1}` : "s2", user(`${b} ${i}`)));
    const r = analyzeFile(file("pi/forest-branch.jsonl", rows));
    expect(r).toMatchObject({ spans: 7, prefixSpans: 0 }); // forks on span boundaries share spans directly
    // view a: (s0 s1) (s2 a0) ((s0 s1)(s2 a0)); view b reuses (s0 s1), adds (s2 b0) and its parent
    expect(r.forestInternalNodes).toBe(5);
  });

  test("a branch point between a call and its result keeps the call in both branches' spans", () => {
    const p = file("pi/split-call.jsonl", [
      piHeader(),
      piMsg("u", null, user("go")),
      piMsg("c", "u", call("t")),
      piMsg("x", "c", say("note A")),
      piMsg("y", "c", say("note B")),
      piMsg("rx", "x", result("t", "out A")),
      piMsg("ry", "y", result("t", "out B")),
    ]);
    const r = analyzeFile(p);
    // S = [u c x rx]; fork after c is inside the unit [c x rx] → P cut back to the whole unit [u], then [c y ry]
    expect(r).toMatchObject({ spans: 3, prefixSpans: 1, unpairedCalls: 0, evidence: 6 });
    expect(r.digestBytes).toBe(B("user: go\n") + B("tool_call: bash {}\n") + B("assistant: note A\n") + B("assistant: note B\n") + B("tool_result: out A\n") + B("tool_result: out B\n"));
  });

  test("views are partitioned in creation order: the first branch keeps its span, a later one gets P", () => {
    const p = file("pi/order.jsonl", [
      piHeader(),
      piMsg("u1", null, user("u".repeat(6000))),
      piMsg("a1", "u1", say("a".repeat(6000))), // first: S = [u1 a1] fits under SPAN_MAX
      piMsg("b1", "u1", say("b".repeat(7000))), // later: forks inside S → P = [u1], then [b1]
    ]);
    // reversed order would give [u1], [b1] first, then reuse [u1] whole: no prefix span
    expect(analyzeFile(p)).toMatchObject({ views: 2, spans: 3, prefixSpans: 1 });
  });

  test("an open call at a branch point stays whole in each branch's span", () => {
    const big = { s: "a".repeat(11000) }; // call ~11KB + result digest ~2KB (head/tail cut) > SPAN_MAX
    const p = file("pi/carry.jsonl", [
      piHeader(),
      piMsg("u", null, user("u".repeat(6000))),
      piMsg("c", "u", call("t", "bash", big)),
      piMsg("x", "c", say("A")),
      piMsg("y", "c", say("B")),
      piMsg("rx", "x", result("t", "r".repeat(7000))),
      piMsg("ry", "y", result("t", "r".repeat(7000))),
    ]);
    const r = analyzeFile(p);
    // [u] alone (shared, a span boundary); each branch [c x rx] is one indivisible unit over SPAN_MAX
    expect(r).toMatchObject({ spans: 3, oversizedSpans: 2, prefixSpans: 0 });
  });

  test("a child holding only labels or state does not make a branch", () => {
    const p = file("pi/label.jsonl", [
      piHeader(),
      piMsg("u", null, user("q")),
      { type: "label", id: "l", parentId: "u", timestamp: "t", targetId: "u", label: "x" },
      piMsg("a", "u", say("answer")),
    ]);
    expect(analyzeFile(p)).toMatchObject({ branchPoints: 0, spans: 1 });
  });

  test("system content and non-memex sections are both kept", () => {
    const p = file("pi/system.jsonl", [
      piHeader(),
      piMsg("a", null, { role: "system", content: "BASE", sections: { memex: "SNAP", tools: "TOOLS" }, timestamp: 1 }),
      piMsg("b", "a", { role: "system", content: [{ type: "text", text: "ARRAY BASE" }], sections: { memex: "SNAP2" }, timestamp: 1 }),
      piMsg("u", "b", user("q")),
    ]);
    const r = analyzeFile(p);
    expect(r.digestExcluded.context).toEqual({ records: 2, bytes: B("BASE\nTOOLS") + B("ARRAY BASE") });
    expect(r.excluded["memex-injected"]).toEqual({ records: 2, bytes: B("SNAP") + B("SNAP2") });
  });

  test("memory blocks are stripped from thinking; exclusions count records, not blocks", () => {
    const p = file("pi/think-mem.jsonl", [
      piHeader(),
      piMsg("u", null, user("<openviking-context>A</openviking-context> q <openviking-context>B</openviking-context>")),
      piMsg("a", "u", asst([{ type: "thinking", thinking: "plan <memex-recall>R</memex-recall> <openviking-context>C</openviking-context>more" }, { type: "text", text: "ok <openviking-context>D</openviking-context>" }])),
    ]);
    const r = analyzeFile(p);
    // two records (u and a), although a holds blocks in two places
    expect(r.excluded["other-memory-injected"]).toEqual({ records: 2, bytes: 4 * B("<openviking-context>A</openviking-context>") });
    expect(r.excluded["memex-injected"]).toEqual({ records: 1, bytes: B("<memex-recall>R</memex-recall>") });
    expect(r.digestExcluded.thinking).toEqual({ records: 1, bytes: B("plan  more") });
    expect(r.digestBytes).toBe(B("user: q\n") + B("assistant: ok\n"));
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

  test("short or long content_item_kinds arrays classify by position", () => {
    const p = file(
      "codex/kinds.jsonl",
      withOrd([meta(), cmsg("user", ["plain context", "typed"], ["environment.context"]), cmsg("user", ["more"], ["user.text", "extra"])]),
    );
    const r = analyzeFile(p);
    expect(r.digestBytes).toBe(B("user: typed\n") + B("user: more\n"));
    expect(r.digestExcluded.context.records).toBe(1);
  });

  test("an inherited memory call still marks its output after the fork boundary", () => {
    const rows = withOrd([
      meta({ forked_from_id: "22222222-2222-2222-2222-222222222222", forked_from_ordinal_exclusive: 2 }),
      ri({ type: "function_call", name: "mem_search", arguments: "{}", call_id: "c1" }),
      ri({ type: "function_call_output", call_id: "c1", output: "RECALLED" }),
    ]);
    const r = analyzeFile(file("codex/fork-mem.jsonl", rows));
    expect(r.excluded["memex-tool-result"]).toEqual({ records: 1, bytes: B("RECALLED") });
    expect(r.evidence).toBe(0);
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
    expect(r.lexicalBytes).toBe(B("q") + B("mcp__memex__mem_zoom {}") + B("shell {}") + B("ls output"));
    expect(r.evidence).toBe(4); // user, both calls, the shell output; the memory result is not evidence
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
const pi = (name: string, msgs: object[]) => file(`pi/${name}.jsonl`, [piHeader(), ...msgs.map((m, i) => piMsg(`e${i}`, i ? `e${i - 1}` : null, m))]);
const big = (n: number) => ({ s: "x".repeat(n) });

describe("units and spans", () => {
  test("a call and its result stay together across commentary, parallel calls and out-of-order results", () => {
    // [callA say rA] is ~14KB: kept whole as one oversized span instead of being split at the commentary
    const a = analyzeFile(pi("unit-commentary", [user("q"), call("A", "bash", big(6000)), say("y".repeat(6000)), result("A", "z".repeat(2000))]));
    expect(a).toMatchObject({ spans: 2, oversizedSpans: 1, unpairedCalls: 0 });
    const b = analyzeFile(pi("unit-parallel", [user("q"), call("A", "bash", big(5000)), call("B", "bash", big(5000)), result("B", "z".repeat(2000)), result("A", "z".repeat(2000))]));
    expect(b).toMatchObject({ spans: 2, oversizedSpans: 1, unpairedCalls: 0 });
  });

  test("a call with no result ends at the next user message and is reported", () => {
    const r = analyzeFile(pi("unpaired", [user("U1"), call("A"), user("U2"), say("reply")]));
    // [U1 callA], [U2 reply]: U2's exchange is not swallowed by the open call
    expect(r).toMatchObject({ spans: 2, unpairedCalls: 1, exchanges: 2, oversizedSpans: 0 });
    const big2 = analyzeFile(pi("unpaired-big", [user("U1"), call("A", "bash", big(7000)), user("u".repeat(7000)), say("reply")]));
    expect(big2).toMatchObject({ spans: 2, unpairedCalls: 1, oversizedSpans: 0 });
  });

  test("SPAN_MAX wins over SPAN_MIN: short user + 10KB reply become two spans", () => {
    expect(analyzeFile(pi("max-min", [user("u".repeat(3000)), say("a".repeat(10000))]))).toMatchObject({ spans: 2, oversizedSpans: 0 });
  });

  test("only a single indivisible unit may exceed SPAN_MAX", () => {
    const r = analyzeFile(pi("oversized", [user("q"), call("A", "bash", big(9000)), say("a".repeat(9000)), result("A", "ok")]));
    expect(r).toMatchObject({ spans: 2, oversizedSpans: 1 });
  });

  test("a memory result never opens a span or pushes one over SPAN_MAX", () => {
    const mem = result("m", "RECALLED", "mem_search"); // no call: an orphan memory result
    expect(analyzeFile(pi("mem-only", [mem]))).toMatchObject({ spans: 0, evidence: 0 });
    const full = "u".repeat(SPAN_MAX - D("user", ""));
    expect(D("user", full)).toBe(SPAN_MAX);
    expect(analyzeFile(pi("mem-at-max", [user(full), mem]))).toMatchObject({ spans: 1, oversizedSpans: 0, summaryInputBytes: SPAN_MAX });
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
    expect(r).toMatchObject({ views: 1, spans: 13, forestInternalNodes: 10 });
  });
});

describe("input boundary", () => {
  test("symlinks are reported, never followed; overlapping inputs are deduplicated", () => {
    const outside = realpathSync(mkdtempSync(join(tmpdir(), "memex-outside-")));
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

  test("missing inputs and unlistable directories are reported; the rest still runs", () => {
    const root = join(dir, "disc");
    file("disc/ok/a.jsonl", [piHeader(), piMsg("u", null, user("fine"))]);
    mkdirSync(join(root, "locked"), { recursive: true });
    chmodSync(join(root, "locked"), 0);
    const report = dryRun([join(root, "missing"), root]);
    expect(report.discoveryErrors.map((e) => e.path)).toEqual([join(root, "missing"), join(root, "locked")]);
    expect(report.indexed.pi).toBe(1);
  });

  test("a symlink given straight to analyzeFile is not followed", () => {
    const outside = realpathSync(mkdtempSync(join(tmpdir(), "memex-outside-")));
    writeFileSync(join(outside, "s.jsonl"), jsonl([piHeader(), piMsg("u", null, user("outside"))]));
    const link = join(dir, "direct-link.jsonl");
    symlinkSync(join(outside, "s.jsonl"), link);
    expect(analyzeFile(link)).toMatchObject({ status: "skipped", reason: "unreadable" });
  });

  test("a memory result adds no digest bytes to its span", () => {
    const rows = withOrd([meta(), cmsg("user", ["q"], ["user.text"]), ri({ type: "function_call", name: "mem_zoom", arguments: "{}", call_id: "c" }), ri({ type: "function_call_output", call_id: "c", output: "Z" })]);
    const s = analyzeFile(file("codex/memsize.jsonl", rows));
    expect(s.digestBytes).toBe(B("user: q\n") + B("tool_call: mem_zoom {}\n"));
  });

  test("a file swapped for a symlink after discovery is not read", () => {
    const outside = realpathSync(mkdtempSync(join(tmpdir(), "memex-outside-")));
    writeFileSync(join(outside, "s.jsonl"), jsonl([piHeader(), piMsg("u", null, user("outside"))]));
    const p = file("swap/a.jsonl", [piHeader(), piMsg("u", null, user("inside"))]);
    const { files, identity } = collect([join(dir, "swap")]);
    const opts = { identity: identity.get(files[0]) };
    expect(analyzeFile(files[0], opts).status).toBe("indexed");
    const kept = join(dir, "kept");
    mkdirSync(kept, { recursive: true });
    renameSync(p, join(kept, "a-1.jsonl")); // moved aside, not deleted
    symlinkSync(join(outside, "s.jsonl"), p);
    expect(analyzeFile(files[0], opts)).toMatchObject({ status: "skipped", reason: "unreadable" });
    renameSync(p, join(kept, "a-link"));
    writeFileSync(p, jsonl([piHeader(), piMsg("u", null, user("replaced"))]));
    expect(analyzeFile(files[0], opts)).toMatchObject({ status: "skipped", reason: "unreadable" }); // new inode
    expect(analyzeFile(files[0], { identity: "0:0" })).toMatchObject({ status: "skipped", reason: "unreadable" });
  });

  test("dry run never writes into its inputs", () => {
    const p = file("ro/ro.jsonl", [piHeader(), piMsg("u1", null, user("hi"))]);
    const before = readFileSync(p);
    dryRun([join(dir, "ro")]);
    expect(readFileSync(p).equals(before)).toBe(true);
  });
});
