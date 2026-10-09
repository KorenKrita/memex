#!/bin/bash
# Codex hook probe (DESIGN §11 item 1). One command hook for every event: appends one JSON line with the event,
# session id, transcript path, and the rollout's line count / last lines' types and ordinal at that moment.
# Never logs message text. The hook payload (JSON) arrives on stdin and is passed on via stdin, not argv.
export MEMEX_PROBE_LOG=${MEMEX_PROBE_LOG:-/tmp/memex-eval/codex-probe.jsonl}
exec python3 -c '
import json, os, sys, time
p = json.load(sys.stdin)
tp = p.get("transcript_path")
info = {"t": time.time(), "event": p.get("hook_event_name"), "session": p.get("session_id"), "turn": p.get("turn_id"),
        "transcript": tp, "exists": bool(tp and os.path.exists(tp)), "keys": sorted(p.keys())}
if info["exists"]:
    lines = [l for l in open(tp, encoding="utf-8", errors="replace").read().split("\n") if l.strip()]
    last = {}
    try:
        last = json.loads(lines[-1]) if lines else {}
    except Exception:
        last = {"torn": True}
    kinds = []
    for l in lines[-6:]:
        try:
            d = json.loads(l); pl = d.get("payload") or {}
            kinds.append(":".join([str(d.get("type")), str(pl.get("type") or ""), str(pl.get("role") or "")]))
        except Exception:
            kinds.append("torn")
    info.update(lines=len(lines), lastOrdinal=last.get("ordinal"), tail=kinds)
open(os.environ["MEMEX_PROBE_LOG"], "a").write(json.dumps(info) + "\n")
'
