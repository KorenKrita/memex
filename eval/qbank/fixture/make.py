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
# Two copies of one Pi session (same header id): identical before the cutoff, different after it. Before the
# cutoff they are one session (one copy indexed); only the full files conflict.
cp_head = [{"type": "session", "version": 3, "id": "s2", "timestamp": "2026-01-01T00:00:00Z", "cwd": "/x"},
    M("c1", None, "2026-01-01T00:00:01Z", "user", "copy says TEAL")]
w("pi/q/s2.jsonl", cp_head + [M("c2", "c1", "2026-01-01T00:00:20Z", "assistant", "later in copy A")])
w("pi/r/s2.jsonl", cp_head + [M("c2", "c1", "2026-01-01T00:00:20Z", "assistant", "later in copy B")])
# Correction distances on one linear Pi path (spans per the reference indexer: [d1 d2] [d3, oversized] [d4]
# [d5 d6] [d7 d8]), a Codex user message that starts with "<", and a torn multibyte write after the cutoff.
d = lambda i, par, t, role, text: M(i, par, f"2026-01-01T00:00:0{t}Z", role, text)
w("pi/d/dist.jsonl", [{"type": "session", "version": 3, "id": "d", "timestamp": "2026-01-01T00:00:00Z", "cwd": "/x"},
    d("d1", None, 1, "user", "start OLD0"), d("d2", "d1", 2, "assistant", "OLD1 claim"),
    d("d3", "d2", 3, "assistant", "x" * 12288 + " FIXADJ"), d("d4", "d3", 4, "assistant", "FIXFAR"),
    M("e5", "d4", "2026-01-01T00:00:04.5Z", "user", "sibling user turn"), M("e6", "e5", "2026-01-01T00:00:04.6Z", "assistant", "SIBOLD"),
    d("d5", "d4", 5, "user", "one user turn"), d("d6", "d5", 6, "assistant", "FIXNEXT"),
    d("d7", "d6", 7, "user", "second user turn"), d("d8", "d7", 8, "assistant", "FIXLATER"),
    M("d9", "d8", "2026-01-01T00:00:20Z", "assistant", "after the cutoff")])
open(f"{C}/pi/d/dist.jsonl", "ab").write(b'{"type": "message", "id": "d10", "parentId": "d9", "timestamp": "2026-01-01T00:00:20Z", "partial": "\xe2\x82')
w("codex/2026/01/01/rollout-dist.jsonl", [X(0, "2026-01-01T00:00:00Z", "session_meta", meta(source="cli", thread_source="user")),
    X(1, "2026-01-01T00:00:02Z", "response_item", msg("assistant", "codex OLD2")),
    X(2, "2026-01-01T00:00:03Z", "response_item", msg("user", "<svg> should be <canvas>; fix it")),
    X(3, "2026-01-01T00:00:04Z", "response_item", msg("assistant", "codex FIX2")),
    X(4, "2026-01-01T00:00:05Z", "response_item", {**msg("user", "plain hook context"),
        "internal_chat_message_metadata_passthrough": {"content_item_kinds": ["hooks.additional_context"]}}),
    X(5, "2026-01-01T00:00:06Z", "response_item", msg("assistant", "codex FIX3"))])
files = {}
for dp, _, fs in os.walk(C):
    for fn in fs:
        if fn.endswith((".jsonl", ".jsonl.zst")):
            p = os.path.join(dp, fn)
            files[os.path.relpath(p, C)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
json.dump({"files": dict(sorted(files.items()))}, open(f"{C}/manifest.json", "w"), indent=0)

Q = lambda i, ev, split="test", cl="k", fg=None, typ="t", cut="2026-01-01T00:00:10Z", hist=True: dict(id=i, task_cluster=cl, split=split, type=typ,
    needs_history=hist, origin="synthetic", question="Q?", current_context="ctx", cutoff=cut, answer_points=["SECRET-ANSWER"],
    forbidden=["SECRET-FORBIDDEN"], evidence=ev, check_status="checked", notes="SECRET-NOTE", **({"fact_group": fg} if fg else {}))
E = lambda f, e, q, u="answer": {"file": f, "entry": e, "quote": q, "use": u}
S1, MAIN = "p/s1.jsonl", "codex/2026/01/01/rollout-main.jsonl"
DI, CD = "d/dist.jsonl", "codex/2026/01/01/rollout-dist.jsonl"
cases = {
    "ok": [Q("ok1", [E(S1, "a1", "RED", "old-claim"), E(S1, "b2", "BLUE", "correction"), E(MAIN, "2", "SAME2")], typ="correction-self-adjacent-span")],
    "dist": [Q("ds", [E(DI, "d1", "OLD0", "old-claim"), E(DI, "d2", "OLD1", "correction")], typ="correction-same-span"),
             Q("da", [E(DI, "d2", "OLD1", "old-claim"), E(DI, "d3", "FIXADJ", "correction")], typ="correction-self-adjacent-span"),
             Q("dn", [E(DI, "d2", "OLD1", "old-claim"), E(DI, "d6", "FIXNEXT", "correction")], typ="correction-next-exchange"),
             Q("dl", [E(DI, "d2", "OLD1", "old-claim"), E(DI, "d8", "FIXLATER", "correction")], typ="correction-later-turn"),
             Q("dc", [E(CD, "1", "OLD2", "old-claim"), E(CD, "3", "FIX2", "correction")], typ="correction-next-exchange"),
             Q("dk", [E(CD, "3", "FIX2", "old-claim"), E(CD, "5", "FIX3", "correction")], typ="correction-same-span"),
             Q("do", [E(DI, "d2", "OLD1", "old-claim"), E(CD, "3", "FIX2", "correction")], typ="state-update-other-session"),
             Q("de", [], hist=False, cut="2026-01-01T00:00:00Z")],
    "distsib": [Q("dz", [E(DI, "e6", "SIBOLD", "old-claim"), E(DI, "d8", "FIXLATER", "correction")], typ="correction-later-turn")],
    "distfar": [Q("df", [E(DI, "d2", "OLD1", "old-claim"), E(DI, "d4", "FIXFAR", "correction")], typ="correction-self-adjacent-span"),
                Q("dg", [E(DI, "d2", "OLD1", "old-claim"), E(DI, "d4", "FIXFAR", "correction")], typ="correction-nonadjacent-span")],
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
    "samesession": [Q("z1", [E(S1, "a2", "RED")], cl="k1"), Q("z2", [E(S1, "b2", "BLUE", "source-only")], cl="k2", split="tune")],
    "futurecopy": [Q("fc", [E("q/s2.jsonl", "c1", "TEAL")])],
    "quote": [Q("qq", [E(S1, "a2", "PURPLE")])],
    "corrsame": [Q("cs", [E(S1, "a2", "RED", "old-claim"), E(S1, "a2", "RED", "correction")], typ="correction-next-exchange")],
    "corrother": [Q("co", [E(S1, "a1", "RED", "old-claim"), E(S1, "b2", "BLUE", "correction")], typ="correction-other-session")],
}
for k, v in cases.items():
    open(f"{H}/{k}.jsonl", "w").write("".join(json.dumps(q) + "\n" for q in v))
