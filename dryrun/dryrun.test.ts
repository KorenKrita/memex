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
let headerN = 0; // every header gets its own session id unless a test names one (same id = copies of one session)
const piHeader = (extra: object = {}) => ({ type: "session", version: 3, id: `s${++headerN}`, timestamp: "2026-10-01T00:00:00Z", cwd: "/w", ...extra });
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
    // production opens by path and reads the descriptor, so observe openSync paths
    const spy = spyOn(fs, "openSync");
    const report = dryRun([join(dir, "pi/sub")]);
    const opened = spy.mock.calls.map((c) => String(c[0]));
    spy.mockRestore();
    expect(opened).toContain(child); // the spy does see production opens
    expect(opened).not.toContain(sub);
    expect(report.skipped["child-thread"]).toEqual([sub]);
    expect(report.perFile.find((f) => f.path === child)!.forkStatus).toBe("parent-not-given");
  });

  test("fork inheritance: same id and content (key order irrelevant); parent not given → quarantined", () => {
    const parentRows = [piHeader(), piMsg("u1", null, user("shared")), piMsg("a1", "u1", say("shared reply"))];
    const parent = file("pi/fork/parent.jsonl", parentRows);
    const reordered = Object.fromEntries(Object.entries(parentRows[1] as object).reverse());
    const edited = { ...(parentRows[2] as object), message: say("same id, different content") };
    const childRows = [piHeader({ id: "child", parentSession: parent }), reordered, edited, piMsg("u2", "a1", user("new in child"))];
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

  test("tool output digest keeps head, tail, error lines and a locator; lexical keeps everything", () => {
    const H = "H".repeat(1024);
    const T = "T".repeat(1024);
    const out = `${H}\n${"m".repeat(3000)}\nERROR: middle failure\n${"m".repeat(3000)}\n${T}`;
    const r = analyzeFile(file("pi/tool.jsonl", [piHeader(), piMsg("u1", null, user("q")), piMsg("c", "u1", call("t")), piMsg("r", "c", result("t", out))]));
    expect(r.lexicalBytes).toBe(B("q") + B("bash {}") + B(out));
    const digest = `tool_result: ${H}\n‹tool-output record=r bytes=${B(out)}›\nERROR: middle failure\n${T}`;
    expect(r.digestBytes).toBe(D("user", "q") + D("tool_call", "bash {}") + B(digest) + 1);
    expect(r.lexicalChunks).toBe(1 + 1 + Math.ceil(B(out) / 2048));
  });

  test("forest internal nodes = spans - popcount(spans)", () => {
    const rows: unknown[] = [piHeader()];
    for (let i = 0; i < 13; i++) rows.push(piMsg(`u${i}`, i ? `u${i - 1}` : null, user(`q${i}`)));
    const r = analyzeFile(file("pi/forest.jsonl", rows));
    expect(r).toMatchObject({ views: 1, spans: 13, forestInternalNodes: 10 });
  });
});

// Runs code against this module in a child process with a deadline, so a hang fails the test
// instead of the suite. `signal` is set when the deadline killed it.
const isolated = (code: string, timeout = 5000) => {
  const mod = JSON.stringify(join(import.meta.dir, "dryrun.ts"));
  const p = Bun.spawnSync(["bun", "-e", `import { analyzeFile, dryRun } from ${mod};\n${code}`], { timeout, stdout: "pipe", stderr: "pipe" });
  return { exit: p.exitCode, signal: p.signalCode ?? null, out: p.stdout.toString(), err: p.stderr.toString() };
};
const keptAside = () => {
  const k = join(dir, "kept");
  mkdirSync(k, { recursive: true });
  return k;
};

describe("third review", () => {
  test("a reused span must end on a unit boundary of the new view, or a later result is lost", () => {
    const p = file("pi/reuse-boundary.jsonl", [
      piHeader(),
      piMsg("u", null, user("q")),
      piMsg("c", "u", call("t")),
      piMsg("x", "c", say("waiting")),
      piMsg("old", "x", user("next")), // first view: the call is unpaired here
      piMsg("r", "x", result("t", "RESULT")), // second view: [c x r] is one unit
    ]);
    // [u c x], [old]; then P = [u] and [c x r]
    expect(analyzeFile(p)).toMatchObject({ spans: 4, prefixSpans: 1, unpairedCalls: 1 });
    expect(analyzeFile(p).summaryInputBytes).toBe(
      D("user", "q") + D("tool_call", "bash {}") + D("assistant", "waiting") + D("user", "next") + D("user", "q") + D("tool_call", "bash {}") + D("assistant", "waiting") + D("tool_result", "RESULT"),
    );
  });

  test("duplicate ids or a parent cycle make the file invalid; the batch goes on", () => {
    const dup = file("cycle/dup.jsonl", [piHeader(), piMsg("a", null, user("first")), piMsg("b", "a", user("second")), piMsg("a", "b", user("duplicate"))]);
    const loop = file("cycle/loop.jsonl", [piHeader(), piMsg("a", "b", user("x")), piMsg("b", "a", user("y"))]);
    file("cycle/self.jsonl", [piHeader(), piMsg("a", "a", user("self"))]); // Pi's getTree: a root
    file("cycle/ok.jsonl", [piHeader(), piMsg("u", null, user("fine"))]);
    const r = isolated(`const r = dryRun([${JSON.stringify(join(dir, "cycle"))}]); console.log(JSON.stringify({ skipped: r.skipped, evidence: r.total.evidence }));`);
    expect(r.signal).toBeNull(); // not killed by the deadline
    expect(JSON.parse(r.out)).toEqual({ skipped: { "invalid-tree": [dup, loop] }, evidence: 2 });
  });

  test("a file swapped for a FIFO after discovery is refused without blocking", () => {
    const p = file("fifo/a.jsonl", [piHeader(), piMsg("u", null, user("inside"))]);
    const { identity } = collect([join(dir, "fifo")]);
    renameSync(p, join(keptAside(), "fifo-a.jsonl"));
    Bun.spawnSync(["mkfifo", p]);
    const r = isolated(`const r = analyzeFile(${JSON.stringify(p)}, { identity: ${JSON.stringify(identity.get(p))} }); console.log(JSON.stringify([r.status, r.reason]));`);
    expect(r.signal).toBeNull();
    expect(JSON.parse(r.out)).toEqual(["skipped", "unreadable"]);
  });

  test("views are taken in creation order, also for nested forks", () => {
    const p = file("pi/nested-order.jsonl", [
      piHeader(),
      piMsg("u", null, user("u".repeat(3000))),
      piMsg("a", "u", say("a".repeat(3000))),
      piMsg("x", "a", say("x".repeat(3000))), // view 1: u-a-x
      piMsg("b", "u", say("b".repeat(1000))), // view 2: u-b
      piMsg("y", "a", say("y".repeat(1000))), // view 3: u-a-y
    ]);
    // [u a x]; view 2 forks from it at u: P = [u], [b]; view 3 forks from view 1 (not view 2) at a:
    // P = [u a], [y]. (The third review expected 4 spans by letting view 2's short P decide view 3's cut;
    // DESIGN §4.3 cuts P out of the span the branch actually forked from.)
    const r = analyzeFile(p);
    expect(r).toMatchObject({ views: 3, spans: 5, prefixSpans: 2 });
    const U = D("user", "u".repeat(3000));
    const A = D("assistant", "a".repeat(3000));
    expect(r.summaryInputBytes).toBe(U + A + D("assistant", "x".repeat(3000)) + U + D("assistant", "b".repeat(1000)) + U + A + D("assistant", "y".repeat(1000)));
  });

  test("the source is the view sharing the longest prefix, not the first one sharing any", () => {
    const p = file("pi/deep-source.jsonl", [
      piHeader(),
      piMsg("u", null, user("u".repeat(3000))),
      piMsg("b", "u", say("b".repeat(1000))), // view 1: [u b]
      piMsg("a", "u", say("a".repeat(3000))),
      piMsg("x", "a", say("x".repeat(3000))), // view 2 forks from 1 at u: P = [u], [a x]
      piMsg("y", "a", say("y".repeat(1000))), // view 3 forks from 2 at a: reuse [u], P = [a], [y]
    ]);
    expect(analyzeFile(p)).toMatchObject({ views: 3, spans: 5, prefixSpans: 2 });
  });

  test("a branch that keeps growing after a later fork is still the earlier view", () => {
    const p = file("pi/late-growth.jsonl", [
      piHeader(),
      piMsg("u", null, user("u".repeat(3000))),
      piMsg("a", "u", say("a".repeat(3000))),
      piMsg("b", "u", say("b".repeat(1000))), // second view forks inside [u a ...]
      piMsg("a2", "a", say("c".repeat(1000))), // first view grows on
    ]);
    // [u a a2]; P = [u], [b]
    expect(analyzeFile(p)).toMatchObject({ views: 2, spans: 3, prefixSpans: 1 });
  });

  test("a resolved Pi fork keeps the inherited prefix for navigation, not as new evidence", () => {
    const parent = file("pi/xfork/parent.jsonl", [piHeader(), piMsg("u", null, user("question")), piMsg("a", "u", say("old reply"))]);
    const child = file("pi/xfork/child.jsonl", [piHeader({ id: "child", parentSession: parent }), piMsg("u", null, user("question")), piMsg("b", "u", say("new reply"))]);
    const r = dryRun([join(dir, "pi/xfork")]);
    // parent S = [u a]; child P = [u], [b]
    expect(r.total).toMatchObject({ spans: 3, prefixSpans: 1 });
    expect(r.total.digestBytes).toBe(D("user", "question") + D("assistant", "old reply") + D("assistant", "new reply"));
    expect(r.total.summaryInputBytes).toBe(r.total.digestBytes + D("user", "question"));
    expect(r.perFile.find((f) => f.path === child)).toMatchObject({ forkStatus: "resolved", inherited: 1, evidence: 1 });
  });

  test("a fork whose parent file is invalid is not resolved against it", () => {
    const parent = file("pi/badparent/parent.jsonl", [piHeader(), piMsg("u", null, user("q")), piMsg("b", "u", user("x")), piMsg("u", "b", user("dup"))]);
    const child = file("pi/badparent/child.jsonl", [piHeader({ id: "child", parentSession: parent }), piMsg("u", null, user("q")), piMsg("b", "u", say("new"))]);
    const r = isolated(`const r = dryRun([${JSON.stringify(join(dir, "pi/badparent"))}]); const c = r.perFile.find((f) => f.path === ${JSON.stringify(child)}); console.log(JSON.stringify([r.skipped, c.forkStatus, c.evidence]));`);
    expect(r.signal).toBeNull();
    expect(JSON.parse(r.out)).toEqual([{ "invalid-tree": [parent] }, "unresolved", 0]);
  });

  test("short sessions (< 4 leaves) build no internal nodes", () => {
    const linear = (n: number) => {
      const rows: unknown[] = [piHeader()];
      for (let i = 0; i < n; i++) rows.push(piMsg(`u${i}`, i ? `u${i - 1}` : null, user(`q${i}`)));
      return analyzeFile(file(`pi/short-${n}.jsonl`, rows));
    };
    expect([2, 3, 4].map((n) => [linear(n).spans, linear(n).forestInternalNodes])).toEqual([
      [2, 0],
      [3, 0],
      [4, 3],
    ]);
  });

  test("a completed memory call is paired; an unanswered call at the end of the file is not", () => {
    const mem = analyzeFile(pi("mem-paired", [user("q"), call("m", "mem_search"), result("m", "R", "mem_search"), user("next")]));
    expect(mem).toMatchObject({ unpairedCalls: 0, spans: 2 });
    expect(analyzeFile(pi("eof-unpaired", [user("q"), call("A")]))).toMatchObject({ unpairedCalls: 1, spans: 1 });
  });

  test("a child holding only extension context does not make a branch", () => {
    const p = file("pi/ctx-branch.jsonl", [
      piHeader(),
      piMsg("u", null, user("q")),
      { type: "custom_message", id: "x", parentId: "u", customType: "some-extension", content: "ctx", display: false, timestamp: "t" },
      piMsg("a", "u", say("answer")),
    ]);
    expect(analyzeFile(p)).toMatchObject({ branchPoints: 0, views: 1, spans: 1 });
  });
});

describe("fourth review", () => {
  test("a fork chain that loops back is unresolved all the way, nothing reaches a span", () => {
    const [A, B] = ["A", "B"].map((n) => join(dir, "pi/forkloop", `${n}.jsonl`));
    file("pi/forkloop/A.jsonl", [piHeader({ id: "sA", parentSession: B }), piMsg("u", null, user("u")), piMsg("a", "u", say("a"))]);
    file("pi/forkloop/B.jsonl", [piHeader({ id: "sB", parentSession: A }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    const r = dryRun([join(dir, "pi/forkloop")]);
    for (const f of r.perFile) expect(f).toMatchObject({ forkStatus: "unresolved", evidence: 0, spans: 0, summaryInputBytes: 0 });
  });

  test("a fork of an unresolved fork is unresolved too", () => {
    const missing = join(dir, "pi/chain/not-given.jsonl");
    const B = file("pi/chain/B.jsonl", [piHeader({ id: "sB", parentSession: missing }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    const C = file("pi/chain/C.jsonl", [piHeader({ id: "sC", parentSession: B }), piMsg("u", null, user("u")), piMsg("b", "u", say("b")), piMsg("c", "b", say("c"))]);
    const r = dryRun([B, C]);
    expect(r.perFile.find((f) => f.path === B)).toMatchObject({ forkStatus: "parent-not-given", spans: 0 });
    expect(r.perFile.find((f) => f.path === C)).toMatchObject({ forkStatus: "unresolved", evidence: 0, spans: 0, summaryInputBytes: 0 });
  });

  test("an entry whose parent is missing is an orphan root (as in Pi's getTree), and is reported", () => {
    const r = analyzeFile(file("pi/orphan.jsonl", [piHeader(), piMsg("a", "missing", user("a")), piMsg("b", "a", say("b"))]));
    expect(r).toMatchObject({ status: "indexed", orphanRoots: 1, views: 1, spans: 1, evidence: 2 });
    expect(analyzeFile(file("pi/self-root.jsonl", [piHeader(), piMsg("a", "a", user("a")), piMsg("b", "a", say("b"))]))).toMatchObject({ orphanRoots: 0, views: 1, spans: 1 });
  });

  test("nodes a short parent never built are built (and counted) by its longer fork", () => {
    const P = file("pi/shortpar/P.jsonl", [piHeader(), piMsg("u0", null, user("q0")), piMsg("u1", "u0", user("q1"))]);
    const rows: unknown[] = [piHeader({ id: "c2", parentSession: P }), piMsg("u0", null, user("q0")), piMsg("u1", "u0", user("q1"))];
    for (let i = 2; i < 6; i++) rows.push(piMsg(`u${i}`, `u${i - 1}`, user(`q${i}`)));
    const C = file("pi/shortpar/C.jsonl", rows);
    const r = dryRun([join(dir, "pi/shortpar")]);
    expect(r.perFile.find((f) => f.path === P)).toMatchObject({ spans: 2, forestInternalNodes: 0 });
    expect(r.perFile.find((f) => f.path === C)).toMatchObject({ spans: 4, forestInternalNodes: 4 }); // 6 leaves: 6 - popcount(6)
  });

  test("sibling forks share one prefix span", () => {
    const P = file("pi/sib/P.jsonl", [piHeader(), piMsg("u", null, user("u")), piMsg("a", "u", say("a"))]);
    file("pi/sib/C1.jsonl", [piHeader({ id: "c1", parentSession: P }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    file("pi/sib/C2.jsonl", [piHeader({ id: "c2", parentSession: P }), piMsg("u", null, user("u")), piMsg("c", "u", say("c"))]);
    const r = dryRun([join(dir, "pi/sib")]).total;
    // S = [u a], P = [u], [b], [c]
    expect(r).toMatchObject({ spans: 4, prefixSpans: 1 });
    expect(r.digestBytes).toBe(D("user", "u") + 3 * D("assistant", "a"));
    expect(r.summaryInputBytes).toBe(r.digestBytes + D("user", "u"));
  });

  // Fifth review: the partition a fork gets depends only on the view it forked from; identical spans are
  // shared afterwards; records new in different sessions are different records.
  test("sibling forks cut at different places: totals do not depend on file names", () => {
    const run = (d: string, shortName: string, longName: string) => {
      const P = file(`${d}/P.jsonl`, [piHeader(), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("x", "a", say("x"))]);
      file(`${d}/${shortName}.jsonl`, [piHeader({ id: "s-short", parentSession: P }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
      file(`${d}/${longName}.jsonl`, [piHeader({ id: "s-long", parentSession: P }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("y", "a", say("y"))]);
      return dryRun([join(dir, d)]).total;
    };
    // S = [u a x], P_short = [u], [b], P_long = [u a], [y]
    const want = { spans: 5, prefixSpans: 2, summaryInputBytes: 3 * D("user", "u") + 2 * D("assistant", "a") + 3 * D("assistant", "x") };
    expect(run("pi/order-ab", "A", "B")).toMatchObject(want);
    expect(run("pi/order-ba", "B", "A")).toMatchObject(want);
  });

  test("records new in two sibling forks are distinct even when byte-identical", () => {
    const P = file("pi/collide/P.jsonl", [piHeader(), piMsg("u", null, user("u"))]);
    const z = piMsg("z", "u", user("same"));
    file("pi/collide/A.jsonl", [piHeader({ id: "sA", parentSession: P }), piMsg("u", null, user("u")), z]);
    file("pi/collide/B.jsonl", [piHeader({ id: "sB", parentSession: P }), piMsg("u", null, user("u")), z]);
    const r = dryRun([join(dir, "pi/collide")]).total;
    expect(r).toMatchObject({ evidence: 3, spans: 3, digestBytes: 30, summaryInputBytes: 30 });
  });

  test("a record inherited through a middle ancestor stays one record", () => {
    const P = file("pi/grand/P.jsonl", [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("a", "u", say("a"))]);
    const C = file("pi/grand/C.jsonl", [piHeader({ id: "sC", parentSession: P }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("c", "a", say("c"))]);
    file("pi/grand/G.jsonl", [piHeader({ id: "sG", parentSession: C }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("c", "a", say("c")), piMsg("g", "c", user("g"))]);
    const r = dryRun([join(dir, "pi/grand")]);
    // P: S = [u a]. C forks where S ends (a span boundary): S shared, [c] new. G: S and [c] shared, [g] new.
    expect(r.total).toMatchObject({ evidence: 4, spans: 3, prefixSpans: 0 });
    expect(r.perFile.map((f) => [f.path.split("/").pop(), f.inherited])).toEqual([
      ["C.jsonl", 2],
      ["G.jsonl", 3],
      ["P.jsonl", 0],
    ]);
  });

  test("a grandchild forking inside a span its grandparent made cuts P from it, reused by an uncle", () => {
    const P = file("pi/grand2/P.jsonl", [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("x", "a", say("x"))]);
    const C = file("pi/grand2/C.jsonl", [piHeader({ id: "sC", parentSession: P }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("x", "a", say("x")), piMsg("q", "x", user("q"))]);
    file("pi/grand2/G.jsonl", [piHeader({ id: "sG", parentSession: C }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("y", "a", say("y"))]);
    file("pi/grand2/U.jsonl", [piHeader({ id: "sU", parentSession: P }), piMsg("u", null, user("u")), piMsg("a", "u", say("a")), piMsg("z", "a", say("z"))]);
    // S = [u a x]; C: S, [q]; G and U both fork at a inside S → one P = [u a], then [y] and [z]
    expect(dryRun([join(dir, "pi/grand2")]).total).toMatchObject({ spans: 5, prefixSpans: 1, evidence: 6 });
  });

  test("a long linear session is traversed in linear time", () => {
    const timed = (n: number) => {
      const rows: string[] = [JSON.stringify(piHeader())];
      for (let i = 0; i < n; i++) rows.push(JSON.stringify(piMsg(`e${i}`, i ? `e${i - 1}` : null, user(`q${i}`))));
      const p = file(`pi/long-${n}.jsonl`, rows.join("\n") + "\n");
      let best = Infinity;
      for (let k = 0; k < 2; k++) {
        const t = performance.now();
        expect(analyzeFile(p).spans).toBe(n);
        best = Math.min(best, performance.now() - t);
      }
      return best;
    };
    // 4x the entries: linear work grows ~4x, quadratic path copying ~16x
    expect(timed(40000) / timed(10000)).toBeLessThan(8);
  }, 60000);

  // Sixth review: a fork's views take their partition from the parent it names, not from an older
  // ancestor holding the same records.
  test("a grandchild continues its parent's partition, even where the parent cut back to a whole unit", () => {
    const P = file("pi/lineage/P.jsonl", [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r0", "c", result("t", "A"))]);
    const C = file("pi/lineage/C.jsonl", [piHeader({ id: "sC", parentSession: P }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r1", "c", result("t", "B"))]);
    const G = file("pi/lineage/G.jsonl", [
      piHeader({ id: "sG", parentSession: C }),
      piMsg("u", null, user("u")),
      piMsg("c", "u", call("t")),
      piMsg("g", "c", user("g")),
      piMsg("h", "g", user("h")),
    ]);
    const r = dryRun([join(dir, "pi/lineage")]);
    // P: [u c r0]. C forks inside it after c (inside the unit) → P cut back to [u], then [c r1].
    // G continues C: reuse [u], cut [c] out of C's [c r1], then [g], [h]: 4 leaves → 3 nodes.
    expect(r.perFile.find((f) => f.path === G)).toMatchObject({ spans: 3, summaryInputBytes: D("tool_call", "bash {}") + D("user", "g") + D("user", "h"), forestInternalNodes: 3 });
    expect(r.total.summaryInputBytes).toBe(
      D("user", "u") + D("tool_call", "bash {}") + D("tool_result", "A") + // P
        D("user", "u") + D("tool_call", "bash {}") + D("tool_result", "B") + // C
        D("tool_call", "bash {}") + D("user", "g") + D("user", "h"), // G
    );
    expect(r.perFile.find((f) => f.path === C)).toMatchObject({ prefixSpans: 1 });
  });

  test("a fork's second view through the same cut reuses its first view's spans", () => {
    // P: [u c r0]. C has two views: [u c r1] (forks inside P's unit: [u], [c r1]) and [u c r2], which
    // shares [u c] with P's view and with C's first view. Here both sources cut it
    // alike: [u] reused, then [c r2] (see the next test for a tie where they differ).
    const P = file("pi/tie/P.jsonl", [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r0", "c", result("t", "A"))]);
    const C = file("pi/tie/C.jsonl", [
      piHeader({ id: "sC", parentSession: P }),
      piMsg("u", null, user("u")),
      piMsg("c", "u", call("t")),
      piMsg("r1", "c", result("t", "B")),
      piMsg("r2", "c", result("t", "C")),
    ]);
    const c = dryRun([join(dir, "pi/tie")]).perFile.find((f) => f.path === C)!;
    expect(c).toMatchObject({ views: 2, spans: 3, prefixSpans: 1 });
  });

  // Seventh review: on an equal shared prefix the two sources can cut it differently (unit boundaries
  // depend on the continuation), so this file's earlier view must win.
  test("on an equal shared prefix this file's earlier view is the source, not the parent's", () => {
    const P = file("pi/tie2/P.jsonl", [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r0", "c", result("t", "A"))]);
    const C = file("pi/tie2/C.jsonl", [
      piHeader({ id: "sC", parentSession: P }),
      piMsg("u", null, user("u")),
      piMsg("c", "u", call("t")),
      piMsg("r1", "c", result("t", "B")),
      piMsg("g", "c", user("g")),
      piMsg("h", "g", user("h")),
    ]);
    const r = dryRun([join(dir, "pi/tie2")]);
    // view 1: [u] (P cut from P's [u c r0]), [c r1]; view 2 follows view 1: [u] reused, [c] cut from [c r1],
    // [g], [h]. The parent as source would give [u c] [g] [h] and no nodes for view 2's 3 leaves.
    const tc = D("tool_call", "bash {}");
    expect(r.perFile.find((f) => f.path === C)).toMatchObject({
      summaryInputBytes: D("user", "u") + tc + D("tool_result", "B") + tc + D("user", "g") + D("user", "h"),
      forestInternalNodes: 4,
    });
  });

  test("a span counted as a prefix span once, whichever file meets it first and in which role", () => {
    const run = (d: string, gName: string, sName: string) => {
      const P = file(`${d}/P.jsonl`, [piHeader({ id: "sP" }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r0", "c", result("t", "A"))]);
      const C = file(`${d}/C.jsonl`, [piHeader({ id: "sC", parentSession: P }), piMsg("u", null, user("u")), piMsg("c", "u", call("t")), piMsg("r1", "c", result("t", "B"))]);
      file(`${d}/${gName}.jsonl`, [
        piHeader({ id: "sG", parentSession: C }),
        piMsg("u", null, user("u")),
        piMsg("c", "u", call("t")),
        piMsg("g", "c", user("g")),
        piMsg("h", "g", user("h")),
      ]);
      // S: u′ (same id, other content: native to S) then P's c, inherited; too big for one span → [u′] [c]
      file(`${d}/${sName}.jsonl`, [piHeader({ id: "sS", parentSession: P }), piMsg("u", null, user("x".repeat(12280))), piMsg("c", "u", call("t"))]);
      return dryRun([join(dir, d)]).total; // every total field, not only the prefix count
    };
    const a = run("pi/roles-ab", "A", "B");
    expect(run("pi/roles-ba", "B", "A")).toEqual(a);
    expect(a.prefixSpans).toBe(2); // P's [u] for C, C's [c] for G
  });

  // Sixth review: copies of one native session (same header id) are one session.
  test("identical copies of a session are counted once; the others are reported as copies", () => {
    const rows = [piHeader({ id: "same" }), piMsg("u", null, user("u"))];
    const a = file("pi/copies/a.jsonl", rows);
    const b = file("pi/copies/b.jsonl", rows);
    const r = dryRun([join(dir, "pi/copies")]);
    expect(r.files).toBe(2);
    expect(r.total).toMatchObject({ evidence: 1, spans: 1, digestBytes: D("user", "u"), summaryInputBytes: D("user", "u") });
    expect(r.perFile.find((f) => f.path === b)).toMatchObject({ status: "skipped", reason: "duplicate-copy", copyOf: a });
  });

  test("identical copies of a fork are counted once", () => {
    const P = file("pi/forkcopies/P.jsonl", [piHeader({ id: "P" }), piMsg("u", null, user("u"))]);
    const rows = [piHeader({ id: "copy", parentSession: P }), piMsg("u", null, user("u")), piMsg("z", "u", user("same"))];
    file("pi/forkcopies/A.jsonl", rows);
    file("pi/forkcopies/B.jsonl", rows);
    const r = dryRun([join(dir, "pi/forkcopies")]).total;
    expect(r).toMatchObject({ evidence: 2, digestBytes: D("user", "u") + D("user", "same"), summaryInputBytes: D("user", "u") + D("user", "same") });
  });

  test("a fork naming either copy of its parent resolves the same way", () => {
    const rows = [piHeader({ id: "par" }), piMsg("u", null, user("u")), piMsg("a", "u", say("a"))];
    const p1 = file("pi/copyparent/p1.jsonl", rows);
    const p2 = file("pi/copyparent/p2.jsonl", rows);
    const kid = (name: string, parentSession: string) =>
      file(`pi/copyparent/${name}.jsonl`, [piHeader({ id: name, parentSession }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    const k1 = kid("k1", p1);
    const k2 = kid("k2", p2);
    const r = dryRun([join(dir, "pi/copyparent")]);
    for (const k of [k1, k2]) expect(r.perFile.find((f) => f.path === k)).toMatchObject({ forkStatus: "resolved", inherited: 1, evidence: 1 });
    // S = [u a], P = [u], [b] from k1, [b] from k2 (two native records)
    expect(r.total).toMatchObject({ spans: 4, prefixSpans: 1 });
  });

  test("copies that differ are all quarantined, and so are forks of them, whatever the file names", () => {
    const run = (d: string, first: string, second: string) => {
      const x = file(`${d}/${first}.jsonl`, [piHeader({ id: "dup" }), piMsg("u", null, user("u"))]);
      file(`${d}/${second}.jsonl`, [piHeader({ id: "dup" }), piMsg("u", null, user("u")), piMsg("a", "u", say("longer"))]);
      const k = file(`${d}/kid.jsonl`, [piHeader({ id: "kid", parentSession: x }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
      const r = dryRun([join(dir, d)]);
      return { conflicting: r.skipped["conflicting-copies"]?.length, kid: r.perFile.find((f) => f.path === k)!.forkStatus, evidence: r.total.evidence };
    };
    const want = { conflicting: 2, kid: "unresolved" as const, evidence: 0 };
    expect(run("pi/conflict-ab", "a", "b")).toEqual(want);
    expect(run("pi/conflict-ba", "b", "a")).toEqual(want);
  });

  test("byte-identical content under different session ids is two sessions", () => {
    file("pi/twoids/a.jsonl", [piHeader({ id: "one" }), piMsg("u", null, user("u"))]);
    file("pi/twoids/b.jsonl", [piHeader({ id: "two" }), piMsg("u", null, user("u"))]);
    expect(dryRun([join(dir, "pi/twoids")]).total).toMatchObject({ evidence: 2, spans: 2 });
  });

  test("a Pi file without a session id is reported and is never a usable parent", () => {
    const p = file("pi/noid/p.jsonl", [{ type: "session", version: 3, timestamp: "t", cwd: "/w" }, piMsg("u", null, user("u"))]);
    const k = file("pi/noid/k.jsonl", [piHeader({ id: "k", parentSession: p }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    const r = dryRun([join(dir, "pi/noid")]);
    expect(r.perFile.find((f) => f.path === p)).toMatchObject({ status: "skipped", reason: "no-session-id" });
    expect(r.perFile.find((f) => f.path === k)).toMatchObject({ forkStatus: "unresolved", evidence: 0 });
  });

  test("copies are found whatever lines come before the header (blank, malformed, non-object)", () => {
    const body = (more: unknown[] = []) => jsonl([piHeader({ id: "lead" }), piMsg("u", null, user("u")), ...more]);
    // byte-identical copies, each led by a line the analysis skips: counted once
    for (const lead of ["null\n", "\n", "{oops\n"]) {
      const d = `pi/lead${JSON.stringify(lead).length}`;
      file(`${d}/a.jsonl`, lead + body());
      const b = file(`${d}/b.jsonl`, lead + body());
      const r = dryRun([join(dir, d)]);
      expect(r.perFile.find((f) => f.path === b)).toMatchObject({ reason: "duplicate-copy" });
      expect(r.total.evidence).toBe(1);
    }
    // differing copies, one led by a blank line: both quarantined, and a fork of either stays unresolved
    const x = file("pi/leadconf/a.jsonl", "\n" + body());
    file("pi/leadconf/b.jsonl", body([piMsg("a", "u", say("more"))]));
    const k = file("pi/leadconf/k.jsonl", [piHeader({ id: "k", parentSession: x }), piMsg("u", null, user("u")), piMsg("b", "u", say("b"))]);
    const r = dryRun([join(dir, "pi/leadconf")]);
    expect(r.skipped["conflicting-copies"]?.length).toBe(2);
    expect(r.perFile.find((f) => f.path === k)).toMatchObject({ forkStatus: "unresolved" });
  });

  test("a file rewritten between the copy pre-pass and analysis is refused", () => {
    const p = file("pi/rewrite/a.jsonl", [piHeader({ id: "rw" }), piMsg("u", null, user("u"))]);
    const spy = spyOn(fs, "readFileSync");
    let n = 0;
    // first read (copy pre-pass) sees one content, the second (analysis) another: a rewrite in between
    spy.mockImplementation((() => {
      n++;
      return n === 1 ? jsonl([piHeader({ id: "rw" }), piMsg("u", null, user("u"))]) : jsonl([piHeader({ id: "rw" }), piMsg("u", null, user("changed"))]);
    }) as any);
    const r = dryRun([p]);
    spy.mockRestore();
    expect(r.perFile[0]).toMatchObject({ status: "skipped", reason: "unreadable" });
  });

  test("a branch-heavy session finishes within its deadline (views are materialized: sum of view lengths)", () => {
    // a chain of 4000 user entries with one extra leaf on each: 4000 views, ~8M records copied in total
    const rows: string[] = [JSON.stringify(piHeader())];
    for (let i = 0; i < 4000; i++) {
      rows.push(JSON.stringify(piMsg(`c${i}`, i ? `c${i - 1}` : null, user(`q${i}`))));
      rows.push(JSON.stringify(piMsg(`l${i}`, `c${i}`, say(`l${i}`))));
    }
    const p = file("pi/branchy.jsonl", rows.join("\n") + "\n");
    const r = isolated(`const r = analyzeFile(${JSON.stringify(p)}); console.log(JSON.stringify([r.views, r.status]));`, 30000);
    expect(r.signal).toBeNull();
    expect(JSON.parse(r.out)).toEqual([4000, "indexed"]);
  }, 40000);
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

  test("a file that becomes a Pi session after the copy pre-pass is refused, not counted outside its group", () => {
    const rows = [piHeader({ id: "same" }), piMsg("u", null, user("u"))];
    const a = file("pi/race/a.jsonl", rows);
    file("pi/race/b.jsonl", rows);
    const spy = spyOn(fs, "readFileSync");
    let n = 0;
    // a's pre-pass read sees an empty file (header not yet written), every later read the full session
    spy.mockImplementation((() => (++n === 1 ? "" : jsonl(rows))) as any);
    const r = dryRun([join(dir, "pi/race")]);
    spy.mockRestore();
    expect(r.perFile.find((f) => f.path === a)).toMatchObject({ status: "skipped", reason: "unreadable" });
    expect(r.total).toMatchObject({ evidence: 1, spans: 1, summaryInputBytes: D("user", "u") });
  });

  test("a file the copy pre-pass cannot read is reported unreadable and not read again", () => {
    const rows = [piHeader({ id: "same" }), piMsg("u", null, user("u"))];
    const a = file("pi/race2/a.jsonl", rows);
    file("pi/race2/b.jsonl", rows);
    const spy = spyOn(fs, "readFileSync");
    let n = 0;
    // a's pre-pass read fails, every later read would succeed
    spy.mockImplementation((() => {
      if (++n === 1) throw new Error("EIO");
      return jsonl(rows);
    }) as any);
    const r = dryRun([join(dir, "pi/race2")]);
    spy.mockRestore();
    expect(r.perFile.find((f) => f.path === a)).toMatchObject({ status: "skipped", reason: "unreadable", error: "EIO" });
    expect(r.total).toMatchObject({ evidence: 1, spans: 1 });
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
