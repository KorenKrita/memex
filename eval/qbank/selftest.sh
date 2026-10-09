#!/bin/bash
# Self-test for build.py on the synthetic fixture (regenerate with fixture/make.py): the accepted case must
# publish exactly the public schema and the expected visible map; every other case must be rejected for its
# stated reason.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d)/qb
export MEMEX_CORPUS="$here/fixture/corpus"
uv run --no-project python "$here/build.py" "$out" "$here/fixture/ok.jsonl" >/dev/null
uv run --no-project python - "$out" <<'PY'
import json, os, stat, sys
out = sys.argv[1]
qs = [json.loads(l) for l in open(f"{out}/public/questions.jsonl")]
assert [sorted(q) for q in qs] == [["current_context", "cutoff", "id", "question", "visible"]], qs
vis = json.load(open(f"{out}/public/{qs[0]['visible']}"))
assert sorted(vis) == ["corpus", "corpus_manifest_sha256", "cutoff", "files"], sorted(vis)
got = {k: v[1] for k, v in vis["files"].items()}
# s1: header + a1 a2 b2 (a3 is at the cutoff); main: meta + ordinals 1, 2 (ordinal 3 is at the cutoff);
# s2: the two copies are identical before the cutoff, so the dry-run indexes one (q/) as a duplicate pair.
# Not visible: Pi run-N and subagent transcript, Codex subagent/guardian/legacy, compressed files.
# dist: header + d1..d4 e5 e6 d5..d8 (d9 is after the cutoff, then a torn write); rollout-dist: meta +
# ordinals 1..5 and the empty string after the last newline (whole file).
assert got == {"pi/p/s1.jsonl": 4, "codex/2026/01/01/rollout-main.jsonl": 3, "pi/q/s2.jsonl": 2,
               "pi/d/dist.jsonl": 11, "codex/2026/01/01/rollout-dist.jsonl": 7}, got
view = f"{out}/public/views/{qs[0]['visible'][8:-5]}"
seen = {os.path.relpath(os.path.join(d, f), view) for d, _, fs in os.walk(view) for f in fs}
assert "pi/p/s1/r1/run-0/session.jsonl" not in seen and "pi/p/s1.jsonl" in seen, seen
text = "".join(open(os.path.join(view, f), errors="replace").read() for f in seen)
for s in ("AT CUTOFF", "AFTER", "codex LATE", "later in copy"):
    assert s not in text, f"post-cutoff {s!r} in the view"
pub = "".join(open(os.path.join(d, f)).read() for d, _, fs in os.walk(f"{out}/public") if not d.startswith(f"{out}/public/views") for f in fs)
for s in ("SECRET-ANSWER", "SECRET-FORBIDDEN", "SECRET-NOTE", '"evidence"', '"quote"', '"entry"', '"answer_points"', '"a1"', '"b2"', "SAME2"):
    assert s not in pub, f"{s} in public/"
assert "SECRET" not in text
for d, ds, fs in os.walk(out):
    assert stat.S_IMODE(os.stat(d).st_mode) == 0o700, d
    for f in fs:
        assert stat.S_IMODE(os.stat(os.path.join(d, f)).st_mode) == 0o600, f
PY
bun "$here/span-distance.ts" "$out" >/dev/null || { echo "FAIL: ok distance"; exit 1; }
# Correction distances on the indexer's records: same span (Pi; Codex across a hook-context user message,
# which is not a user turn), adjacent self-correction, next exchange (Pi with a sibling-branch user turn in
# file order between; Codex user message starting with "<"), later turn, other session; plus an
# empty-history question whose cutoff precedes every record (empty visible map). The Pi file ends in a torn
# multibyte write after the cutoff. An old claim on a sibling branch is an error; a nonadjacent
# self-correction fails under any label.
uv run --no-project python "$here/build.py" "$out-dist" "$here/fixture/dist.jsonl" >/dev/null || { echo "FAIL: dist rejected"; exit 1; }
d=$(bun "$here/span-distance.ts" "$out-dist") || { echo "FAIL: dist distances: $d"; exit 1; }
for r in "ds"$'\t'".*same-span"$'\t'"ok" "da"$'\t'".*self-adjacent-span"$'\t'"ok" "dn"$'\t'".*"$'\t'"1"$'\t'"next-exchange"$'\t'"ok" \
         "dl"$'\t'".*later-turn"$'\t'"ok" "dc"$'\t'".*"$'\t'"1"$'\t'"next-exchange"$'\t'"ok" "dk"$'\t'".*same-span"$'\t'"ok" \
         "do"$'\t'".*other-session"$'\t'"ok"; do
  grep -q "^$r$" <<<"$d" || { echo "FAIL: dist distances ($r): $d"; exit 1; }
done
uv run --no-project python - "$out-dist" <<'PY'
import json, sys
v = json.load(open(f"{sys.argv[1]}/public/visible/2026-01-01T000000Z.json"))
assert v["files"] == {}, v["files"]
PY
uv run --no-project python "$here/build.py" "$out-distsib" "$here/fixture/distsib.jsonl" >/dev/null || { echo "FAIL: distsib rejected"; exit 1; }
if d=$(bun "$here/span-distance.ts" "$out-distsib"); then echo "FAIL: old claim on a sibling branch passed"; exit 1; fi
grep -q "not on the correction's path" <<<"$d" || { echo "FAIL: distsib: $d"; exit 1; }
uv run --no-project python "$here/build.py" "$out-distfar" "$here/fixture/distfar.jsonl" >/dev/null || { echo "FAIL: distfar rejected"; exit 1; }
if d=$(bun "$here/span-distance.ts" "$out-distfar"); then echo "FAIL: nonadjacent spans passed as self-adjacent"; exit 1; fi
[ "$(grep -c "nonadjacent-span"$'\t'"MISMATCH" <<<"$d")" = 2 ] || { echo "FAIL: distfar (both labels must fail): $d"; exit 1; }
# Accepted: a session copied twice whose copies only diverge after the cutoff is indexable at the cutoff.
uv run --no-project python "$here/build.py" "$out-futurecopy" "$here/fixture/futurecopy.jsonl" >/dev/null \
  || { echo "FAIL: futurecopy rejected (later data decided indexability)"; exit 1; }
n=0
while read -r c want; do
  if msg=$(uv run --no-project python "$here/build.py" "$out-$c" "$here/fixture/$c.jsonl" 2>&1); then
    echo "FAIL: $c was accepted"; exit 1
  fi
  grep -q -- "$want" <<<"$msg" || { echo "FAIL: $c rejected for another reason: $msg"; exit 1; }
  if grep -q "PURPLE" <<<"$msg"; then echo "FAIL: $c error prints session text"; exit 1; fi
  n=$((n + 1))
done <<'CASES'
tie outside the visible prefix
clockback outside the visible prefix
codexlate outside the visible prefix
subagent not indexable at the cutoff: pi/p/s1/r1/run-0
transcript not indexable at the cutoff: pi/p/subagent-artifacts
codexsub not indexable at the cutoff: codex/2026/01/01/rollout-sub
guardian not indexable at the cutoff: codex/2026/01/01/rollout-guardian
legacy not indexable at the cutoff: codex/2026/01/01/rollout-legacy
split task_cluster 'k' spans splits
factgroup fact_group 'g' spans splits
sameanswer session file 's1.jsonl' spans splits
samesession session file 's1.jsonl' spans splits
quote quote not in a2 of pi/p/s1.jsonl
corrsame correction is not after the old claim
corrother no correction in another file
CASES
echo "ok: accepted builds (schema, visible map, cut views, future copies, 7 distances, empty history); sibling-branch and nonadjacent distances fail; $n rejected for the expected reason without session text"
