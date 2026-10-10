// L3 explicit statements: the as_of view (DESIGN §4.5) — pure evaluation, no model.
import { expect, test } from "bun:test";
import { viewAt, orient, nextBoundary, type Stmt, type Rel } from "./l3.ts";

const D = (s: string) => new Date(s).getTime();
const st = (id: string, stated: string, eff: string | null = null, scope = "global"): Stmt => ({ id, scope, stated_at: D(stated), effective_from: eff ? D(eff) : null });
const ids = (xs: Stmt[]) => xs.map((x) => x.id).sort();

test("a statement is in effect only once said and once effective", () => {
  const a = st("A", "2026-01-01T00:00:00Z");
  const b = st("B", "2026-01-05T00:00:00Z", "2026-01-12T00:00:00Z"); // "from next Monday, B"
  const rels: Rel[] = [{ a: "B", b: "A", kind: "replaces" }];
  expect(ids(viewAt([a, b], rels, D("2026-01-03T00:00:00Z")))).toEqual(["A"]);
  expect(ids(viewAt([a, b], rels, D("2026-01-08T00:00:00Z")))).toEqual(["A"]); // said, not yet effective: A holds
  expect(ids(viewAt([a, b], rels, D("2026-01-12T00:00:00Z")))).toEqual(["B"]);
});

test("a retroactive statement ('last week already B') is not in effect before it was said", () => {
  const a = st("A", "2026-01-01T00:00:00Z");
  const b = st("B", "2026-01-10T00:00:00Z", "2026-01-03T00:00:00Z");
  const rels: Rel[] = [{ a: "B", b: "A", kind: "replaces" }];
  expect(ids(viewAt([a, b], rels, D("2026-01-05T00:00:00Z")))).toEqual(["A"]);
  expect(ids(viewAt([a, b], rels, D("2026-01-10T00:00:00Z")))).toEqual(["B"]);
});

test("contradictions and pairs not judged yet show both", () => {
  const a = st("A", "2026-01-01T00:00:00Z"), b = st("B", "2026-01-02T00:00:00Z");
  expect(ids(viewAt([a, b], [{ a: "A", b: "B", kind: "contradicts" }], D("2026-02-01T00:00:00Z")))).toEqual(["A", "B"]);
  expect(ids(viewAt([a, b], [], D("2026-02-01T00:00:00Z")))).toEqual(["A", "B"])
});

test("a chain A → B → C does not revive A", () => {
  const a = st("A", "2026-01-01T00:00:00Z"), b = st("B", "2026-01-02T00:00:00Z"), c = st("C", "2026-01-03T00:00:00Z");
  const rels: Rel[] = [{ a: "B", b: "A", kind: "replaces" }, { a: "C", b: "B", kind: "replaces" }, { a: "C", b: "A", kind: "unrelated" }];
  expect(ids(viewAt([a, b, c], rels, D("2026-02-01T00:00:00Z")))).toEqual(["C"]);
});

test("a retraction suppresses its target, and a retracted replacement no longer suppresses", () => {
  const a = st("A", "2026-01-01T00:00:00Z"), b = st("B", "2026-01-02T00:00:00Z"), r = { ...st("R", "2026-01-03T00:00:00Z"), retraction: true };
  // B replaced A; then R retracts B ("forget what I said about B") — A is back, and R itself is not shown.
  const rels: Rel[] = [{ a: "B", b: "A", kind: "replaces" }, { a: "R", b: "B", kind: "retracts" }, { a: "R", b: "A", kind: "unrelated" }];
  expect(ids(viewAt([a, b, r], rels, D("2026-01-02T12:00:00Z")))).toEqual(["B"]);
  expect(ids(viewAt([a, b, r], rels, D("2026-01-04T00:00:00Z")))).toEqual(["A"]);
});

test("a retraction is itself not a statement of fact shown in the view", () => {
  const a = st("A", "2026-01-01T00:00:00Z"), r = { ...st("R", "2026-01-03T00:00:00Z"), retraction: true };
  expect(ids(viewAt([a, r], [{ a: "R", b: "A", kind: "retracts" }], D("2026-01-04T00:00:00Z")))).toEqual([]);
});

test("unreadable statements neither show nor suppress", () => {
  const a = st("A", "2026-01-01T00:00:00Z"), b = st("B", "2026-01-02T00:00:00Z");
  const rels: Rel[] = [{ a: "B", b: "A", kind: "replaces" }];
  expect(ids(viewAt([a, b], rels, D("2026-02-01T00:00:00Z"), (s) => s.id !== "B"))).toEqual(["A"]);
});

test("statements of different scopes never interact", () => {
  const a = st("A", "2026-01-01T00:00:00Z", null, "repo:x"), b = st("B", "2026-01-02T00:00:00Z", null, "repo:y");
  expect(ids(viewAt([a, b], [{ a: "B", b: "A", kind: "replaces" }], D("2026-02-01T00:00:00Z")))).toEqual(["A", "B"]);
});

test("direction comes from effective times, not from the model or insertion order; equal times → contradicts", () => {
  const a = st("A", "2026-01-05T00:00:00Z", "2026-01-20T00:00:00Z"); // said later, effective later
  const b = st("B", "2026-01-10T00:00:00Z"); // said in between, effective when said
  expect(orient(a, b, "replaces")).toEqual({ a: "A", b: "B", kind: "replaces" });
  expect(orient(b, a, "replaces")).toEqual({ a: "A", b: "B", kind: "replaces" }); // same pair, either order
  const c = st("C", "2026-01-10T00:00:00Z");
  expect(orient(b, c, "replaces").kind).toBe("contradicts");
  expect(orient(b, c, "unrelated")).toEqual({ a: "B", b: "C", kind: "unrelated" });
});

test("any insertion order gives the same view", () => {
  const S = [st("A", "2026-01-01T00:00:00Z"), st("B", "2026-01-02T00:00:00Z", "2026-01-09T00:00:00Z"), st("C", "2026-01-03T00:00:00Z"), st("D", "2026-01-04T00:00:00Z")];
  const R: Rel[] = [orient(S[1], S[0], "replaces"), orient(S[2], S[1], "contradicts"), orient(S[3], S[0], "retracts")];
  const ref = ids(viewAt(S, R, D("2026-01-10T00:00:00Z")));
  const perm = (xs: any[]): any[][] => (xs.length <= 1 ? [xs] : xs.flatMap((x, i) => perm([...xs.slice(0, i), ...xs.slice(i + 1)]).map((p) => [x, ...p])));
  for (const s of perm(S)) for (const r of perm(R)) expect(ids(viewAt(s, r, D("2026-01-10T00:00:00Z")))).toEqual(ref);
});

test("the next boundary is the earliest time a statement becomes eligible after now", () => {
  const S = [st("A", "2026-01-01T00:00:00Z"), st("B", "2026-01-05T00:00:00Z", "2026-01-12T15:00:00Z"), st("C", "2026-01-20T00:00:00Z")];
  expect(nextBoundary(S, D("2026-01-06T00:00:00Z"))).toBe(D("2026-01-12T15:00:00Z"));
  expect(nextBoundary(S.slice(0, 1), D("2026-01-06T00:00:00Z"))).toBeNull();
});

test("a retraction withdraws a decision not yet in effect: it never takes effect", () => {
  const a = st("A", "2026-01-01T00:00:00Z");
  const b = st("B", "2026-01-05T00:00:00Z", "2026-01-12T00:00:00Z"); // "from the 12th, B"
  const r = { ...st("R", "2026-01-06T00:00:00Z"), retraction: true }; // "cancel that"
  const rels = [orient(b, a, "replaces"), orient(r, b, "retracts"), orient(r, a, "unrelated")];
  expect(rels[1]).toEqual({ a: "R", b: "B", kind: "retracts" });
  expect(orient(b, r, "retracts")).toEqual({ a: "R", b: "B", kind: "retracts" }); // either argument order
  expect(ids(viewAt([a, b, r], rels, D("2026-01-05T12:00:00Z")))).toEqual(["A"]);
  expect(ids(viewAt([a, b, r], rels, D("2026-01-13T00:00:00Z")))).toEqual(["A"]);
});

test("a retraction said before what it would retract withdraws nothing", () => {
  const r = { ...st("R", "2026-01-01T00:00:00Z"), retraction: true }, b = st("B", "2026-01-02T00:00:00Z");
  expect(orient(r, b, "retracts").kind).toBe("contradicts");
});
