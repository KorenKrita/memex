#!/bin/bash
# Self-test for build.py on the synthetic fixture: one valid case, eight that must be rejected for a stated reason.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d)/qb
export MEMEX_CORPUS="$here/fixture/corpus"
uv run --no-project python "$here/build.py" "$out" "$here/fixture/ok.jsonl" >/dev/null
grep -rq SECRET-ANSWER "$out/public" && { echo "FAIL: answer text in public/"; exit 1; }
grep -q '"pi/p/s1.jsonl": \[' "$out"/public/visible/*.json && ! grep -q 'run-0\|rollout-sub' "$out"/public/visible/*.json \
  || { echo "FAIL: visible set"; exit 1; }
while read -r c want; do
  if msg=$(uv run --no-project python "$here/build.py" "$out-$c" "$here/fixture/$c.jsonl" 2>&1); then
    echo "FAIL: $c was accepted"; exit 1
  fi
  grep -q -- "$want" <<<"$msg" || { echo "FAIL: $c rejected for another reason: $msg"; exit 1; }
done <<'CASES'
tie outside the visible prefix
clockback outside the visible prefix
codexlate outside the visible prefix
subagent non-indexed
codexsub non-indexed
split task_cluster 'k' spans splits
factgroup fact_group 'g' spans splits
quote quote not in
CASES
echo "ok: 1 accepted, 8 rejected for the expected reason, public/ has no answers"
