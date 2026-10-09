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
# s1: header + a1 a2 b2 (a3 is at the cutoff); main: meta + ordinals 1, 2 (ordinal 3 is at the cutoff).
# Not visible: Pi run-N and subagent transcript, Codex subagent/guardian/legacy, compressed files.
assert got == {"pi/p/s1.jsonl": 4, "codex/2026/01/01/rollout-main.jsonl": 3}, got
pub = "".join(open(os.path.join(d, f)).read() for d, _, fs in os.walk(f"{out}/public") for f in fs)
for s in ("SECRET-ANSWER", "SECRET-FORBIDDEN", "SECRET-NOTE", '"evidence"', '"quote"', '"entry"', '"answer_points"', '"a1"', '"b2"', "SAME2"):
    assert s not in pub, f"{s} in public/"
for d, ds, fs in os.walk(out):
    assert stat.S_IMODE(os.stat(d).st_mode) == 0o700, d
    for f in fs:
        assert stat.S_IMODE(os.stat(os.path.join(d, f)).st_mode) == 0o600, f
PY
n=0
while read -r c want; do
  if msg=$(uv run --no-project python "$here/build.py" "$out-$c" "$here/fixture/$c.jsonl" 2>&1); then
    echo "FAIL: $c was accepted"; exit 1
  fi
  grep -q -- "$want" <<<"$msg" || { echo "FAIL: $c rejected for another reason: $msg"; exit 1; }
  n=$((n + 1))
done <<'CASES'
tie outside the visible prefix
clockback outside the visible prefix
codexlate outside the visible prefix
subagent non-indexed file pi/p/s1/r1/run-0
transcript non-indexed file pi/p/subagent-artifacts
codexsub non-indexed file codex/2026/01/01/rollout-sub
guardian non-indexed file codex/2026/01/01/rollout-guardian
legacy non-indexed file codex/2026/01/01/rollout-legacy
split task_cluster 'k' spans splits
factgroup fact_group 'g' spans splits
sameanswer answer record 'pi/p/s1.jsonl#a2' spans splits
quote quote not in
corrsame correction is not after the old claim
corrother no correction in another file
CASES
echo "ok: 1 accepted with the exact public schema and visible map, $n rejected for the expected reason"
