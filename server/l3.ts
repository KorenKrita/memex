// L3 explicit statements (DESIGN §4.5): what the user said, stored as immutable statements with relations
// between them; "in effect" is computed at a time `as_of`, never stored as a state.
//
// - A statement is eligible at t when it had been said (stated_at ≤ t) and its effective time had come
//   ((effective_from ?? stated_at) ≤ t). Its effective time is its own: a retroactive "last week already B"
//   is not in effect before it was said.
// - Relations: replaces(a, b), retracts(a, b) (a newer statement withdraws b), contradicts, unrelated. The
//   model judges the kind; the direction comes from effective times here (orient), equal times → contradicts.
// - A statement suppresses another (replaces / retracts) only while it has power: eligible, readable, and not
//   itself retracted by a statement with power. Being replaced does not take power away, so A → B → C does not
//   bring A back; a retraction of B does (B no longer stands, so what B replaced stands again).
// - Relations only count within one scope (global, or one repository); unreadable statements neither show nor
//   suppress; pairs not judged yet suppress nothing (both show).

export type Stmt = { id: string; scope: string; stated_at: number; effective_from: number | null; retraction?: boolean };
export type RelKind = "replaces" | "retracts" | "contradicts" | "unrelated";
export type Rel = { a: string; b: string; kind: RelKind };

const effective = (s: Stmt) => s.effective_from ?? s.stated_at;
/** The first time `s` is eligible. */
const eligibleFrom = (s: Stmt) => Math.max(s.stated_at, effective(s));

/** A judged relation in its fixed form: replaces/retracts point from the later-effective statement to the
 *  earlier one (no order between them → contradicts); symmetric kinds keep the ids in sorted order. */
export function orient(x: Stmt, y: Stmt, kind: RelKind): Rel {
  if (kind === "replaces" || kind === "retracts") {
    const ex = effective(x), ey = effective(y);
    if (ex !== ey) return ex > ey ? { a: x.id, b: y.id, kind } : { a: y.id, b: x.id, kind };
    kind = "contradicts";
  }
  return x.id < y.id ? { a: x.id, b: y.id, kind } : { a: y.id, b: x.id, kind };
}

/** The statements in effect at `t`. */
export function viewAt(stmts: Stmt[], rels: Rel[], t: number, readable: (s: Stmt) => boolean = () => true): Stmt[] {
  const byId = new Map(stmts.map((s) => [s.id, s]));
  const against = new Map<string, Rel[]>(); // b → relations that would suppress b
  for (const r of rels) {
    if (r.kind !== "replaces" && r.kind !== "retracts") continue;
    const a = byId.get(r.a), b = byId.get(r.b);
    if (!a || !b || a.scope !== b.scope) continue;
    against.set(r.b, [...(against.get(r.b) ?? []), r]);
  }
  const memo = new Map<string, boolean>();
  // A cycle of retractions (only possible with hand-made relations) gives its members no power.
  const power = (id: string, seen = new Set<string>()): boolean => {
    if (memo.has(id)) return memo.get(id)!;
    const s = byId.get(id)!;
    if (seen.has(id)) return false;
    seen.add(id);
    const p = eligibleFrom(s) <= t && readable(s) && !(against.get(id) ?? []).some((r) => r.kind === "retracts" && power(r.a, seen));
    seen.delete(id);
    memo.set(id, p);
    return p;
  };
  return stmts.filter((s) => !s.retraction && power(s.id) && !(against.get(s.id) ?? []).some((r) => power(r.a)));
}

/** The next time after `now` the view can change by time alone (a statement becoming eligible), or null. */
export function nextBoundary(stmts: Stmt[], now: number): number | null {
  let next: number | null = null;
  for (const s of stmts) {
    const e = eligibleFrom(s);
    if (e > now && (next === null || e < next)) next = e;
  }
  return next;
}
