"""Regenerate the synthetic fixture (no real session text). uv run --no-project python fixture/make.py"""
import hashlib, json, os

H = os.path.dirname(os.path.abspath(__file__))
C = f"{H}/corpus"
w = lambda p, rows: (os.makedirs(os.path.dirname(f"{C}/{p}"), exist_ok=True), open(f"{C}/{p}", "w").write("".join(json.dumps(r) + "\n" for r in rows)))
M = lambda i, par, t, role, text: {"type": "message", "id": i, "parentId": par, "timestamp": t, "message": {"role": role, "content": [{"type": "text", "text": text}]}}
w("pi/p/s1.jsonl", [{"type": "session", "version": 3, "id": "s1", "timestamp": "2026-01-01T00:00:00Z", "cwd": "/x"},
    M("a1", None, "2026-01-01T00:00:01Z", "user", "old value is RED"),
    M("a2", "a1", "2026-01-01T00:00:05Z", "assistant", "branch-1 says RED"),
    M("b2", "a1", "2026-01-01T00:00:06Z", "assistant", "branch-2 says BLUE"),
    M("a3", "a2", "2026-01-01T00:00:10Z", "user", "tie AT CUTOFF"),
    M("a4", "a3", "2026-01-01T00:00:09Z", "assistant", "clock went back AFTER")])
w("pi/p/s1/r1/run-0/session.jsonl", [{"type": "session", "version": 3, "id": "r", "timestamp": "2026-01-01T00:00:00Z", "cwd": "/x"},
    M("r1", None, "2026-01-01T00:00:02Z", "user", "subagent run says GREEN")])
w("pi/p/subagent-artifacts/x_transcript.jsonl", [{"type": "session", "version": 3, "id": "t", "timestamp": "2026-01-01T00:00:00Z", "cwd": "/x"},
    M("t1", None, "2026-01-01T00:00:02Z", "user", "transcript says ORANGE")])
X = lambda o, t, typ, p: {"timestamp": t, "ordinal": o, "type": typ, "payload": p}
msg = lambda role, text: {"type": "message", "role": role, "content": [{"type": "input_text" if role == "user" else "output_text", "text": text}]}
meta = lambda **k: {"id": "00000000-0000-0000-0000-000000000000", "history_mode": "paginated", **k}
w("codex/2026/01/01/rollout-main.jsonl", [X(0, "2026-01-01T00:00:00Z", "session_meta", meta(source="cli", thread_source="user")),
    X(1, "2026-01-01T00:00:03Z", "response_item", msg("user", "codex SAME1")),
    X(2, "2026-01-01T00:00:03Z", "response_item", msg("assistant", "codex SAME2")),
    X(3, "2026-01-01T00:00:10Z", "response_item", msg("assistant", "codex LATE"))])
w("codex/2026/01/01/rollout-sub.jsonl", [X(0, "2026-01-01T00:00:00Z", "session_meta", meta(source={"subagent": {"thread_spawn": {"parent_thread_id": "x"}}})),
    X(1, "2026-01-01T00:00:02Z", "response_item", msg("assistant", "sub YELLOW"))])
w("codex/2026/01/01/rollout-guardian.jsonl", [X(0, "2026-01-01T00:00:00Z", "session_meta", meta(source="cli", thread_source="guardian_review")),
    X(1, "2026-01-01T00:00:02Z", "response_item", msg("assistant", "guardian PINK"))])
w("codex/2026/01/01/rollout-legacy.jsonl", [X(0, "2026-01-01T00:00:00Z", "session_meta", {"id": "1", "source": "cli"}),
    X(1, "2026-01-01T00:00:02Z", "response_item", msg("assistant", "legacy GREY"))])
open(f"{C}/codex/2026/01/01/rollout-old.jsonl.zst", "wb").write(b"\x28\xb5\x2f\xfd not a real frame\n")
files = {}
for dp, _, fs in os.walk(C):
    for fn in fs:
        if fn.endswith((".jsonl", ".jsonl.zst")):
            p = os.path.join(dp, fn)
            files[os.path.relpath(p, C)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
json.dump({"files": dict(sorted(files.items()))}, open(f"{C}/manifest.json", "w"), indent=0)

Q = lambda i, ev, split="test", cl="k", fg=None, typ="t", cut="2026-01-01T00:00:10Z": dict(id=i, task_cluster=cl, split=split, type=typ,
    needs_history=True, origin="synthetic", question="Q?", current_context="ctx", cutoff=cut, answer_points=["SECRET-ANSWER"],
    forbidden=["SECRET-FORBIDDEN"], evidence=ev, check_status="checked", notes="SECRET-NOTE", **({"fact_group": fg} if fg else {}))
E = lambda f, e, q, u="answer": {"file": f, "entry": e, "quote": q, "use": u}
S1, MAIN = "p/s1.jsonl", "codex/2026/01/01/rollout-main.jsonl"
cases = {
    "ok": [Q("ok1", [E(S1, "a1", "RED", "old-claim"), E(S1, "b2", "BLUE", "correction"), E(MAIN, "2", "SAME2")], typ="correction-next-exchange")],
    "tie": [Q("t", [E(S1, "a3", "tie")])],
    "clockback": [Q("c", [E(S1, "a4", "clock")])],
    "codexlate": [Q("l", [E(MAIN, "3", "LATE")])],
    "subagent": [Q("s", [E("p/s1/r1/run-0/session.jsonl", "r1", "GREEN")])],
    "transcript": [Q("tr", [E("p/subagent-artifacts/x_transcript.jsonl", "t1", "ORANGE")])],
    "codexsub": [Q("s2", [E("codex/2026/01/01/rollout-sub.jsonl", "1", "YELLOW")])],
    "guardian": [Q("g", [E("codex/2026/01/01/rollout-guardian.jsonl", "1", "PINK")])],
    "legacy": [Q("lg", [E("codex/2026/01/01/rollout-legacy.jsonl", "1", "GREY")])],
    "split": [Q("x1", [E(S1, "a1", "RED")]), Q("x2", [E(S1, "a2", "RED")], split="tune")],
    "factgroup": [Q("f1", [E(S1, "a1", "RED")], cl="k1", fg=["g"]), Q("f2", [E(S1, "a2", "RED")], cl="k2", split="tune", fg=["g"])],
    "sameanswer": [Q("y1", [E(S1, "a2", "RED")], cl="k1"), Q("y2", [E(S1, "a2", "RED")], cl="k2", split="tune")],
    "quote": [Q("qq", [E(S1, "a2", "PURPLE")])],
    "corrsame": [Q("cs", [E(S1, "a2", "RED", "old-claim"), E(S1, "a2", "RED", "correction")], typ="correction-next-exchange")],
    "corrother": [Q("co", [E(S1, "a1", "RED", "old-claim"), E(S1, "b2", "BLUE", "correction")], typ="correction-other-session")],
}
for k, v in cases.items():
    open(f"{H}/{k}.jsonl", "w").write("".join(json.dumps(q) + "\n" for q in v))
