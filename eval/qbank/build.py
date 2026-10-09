"""Build the question bank from drafts (run locally; the bank itself never enters the repo).

  uv run --no-project python build.py OUTDIR drafts1.jsonl [drafts2.jsonl ...]
  env MEMEX_CORPUS=<frozen corpus dir> (default /tmp/memex-eval/corpus-20261009, made by freeze.py)

Retrievable scope ("visible set") of a question with cutoff C:
  * indexable files are those the reference dry-run indexes (indexable.ts runs dryrun/dryrun.ts over the
    corpus: Codex legacy, child (subagent / guardian) and compressed files and Pi subagent transcripts are
    skipped), minus Pi `<session>/<id>/run-N/` subagent runs;
  * per file, the visible prefix = all lines before the first line whose timestamp >= C (file order decides
    ties and clocks that go backwards; a Pi prefix contains every branch written before C); a file whose
    first timestamped line is already >= C is not visible.
Evidence for a history question must lie inside that prefix (checked by line number, not only by time).

Outputs (directories 0700, files 0600):
  OUTDIR/public/questions.jsonl         id, question, current_context, cutoff, visible
  OUTDIR/public/visible/<cutoff>.json   {corpus, corpus_manifest_sha256, cutoff, files: {path: [sha256, lines]}}
  OUTDIR/grader/grading.jsonl           everything else, evidence enriched with sha256/line/timestamp/role
  OUTDIR/grader/evidence/<sha256>.jsonl copies of evidence files (grader only)
Checks: unique ids; files match the frozen manifest; quotes are substrings of the cited record; history
evidence inside the visible prefix of an indexable file; no task_cluster, no fact_group and no answer
record (file, entry) in both splits; correction types carry old-claim and correction evidence in the
stated file relation (same file and later line, or another file). The span distance of a correction is
checked separately with span-distance.ts (see eval/README.md).
Pi evidence paths are relative to CORPUS/pi (entry = record id); Codex paths start with codex/ or
codex-archived/ (entry = str(ordinal) of a response_item: message, tool call or tool output).
"""
import hashlib, json, os, subprocess, sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.environ.get("MEMEX_CORPUS", "/tmp/memex-eval/corpus-20261009")
MAN_BYTES = open(CORPUS + "/manifest.json", "rb").read()
MANIFEST = json.loads(MAN_BYTES)["files"]
outdir, drafts_paths = sys.argv[1], sys.argv[2:]
if os.path.exists(outdir):
    sys.exit(f"{outdir} exists; build into a new directory")
ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))


def fail(msg):
    raise SystemExit(msg)


ref = json.loads(subprocess.run(["bun", f"{HERE}/indexable.ts", CORPUS], check=True, capture_output=True, text=True).stdout)
INDEXABLE = set(ref["indexed"])
for k in INDEXABLE:
    if k not in MANIFEST:
        fail(f"dry-run indexes {k}, which is not in the frozen manifest")


def text_of(m):
    c = m.get("content")
    if isinstance(c, str):
        return c
    out = []
    for x in c or []:
        if isinstance(x, dict):
            if x.get("type") == "text":
                out.append(x.get("text", ""))
            elif x.get("type") == "toolCall":
                out.append(f"[toolCall {x.get('name')}] " + json.dumps(x.get("arguments"), ensure_ascii=False))
    return "\n".join(out)


def parse(key, data):
    """-> (line timestamps, {entry: (line, timestamp, role, text)})"""
    codex = key.startswith(("codex/", "codex-archived/"))
    stamps, es = [], {}
    for n, line in enumerate(data.decode("utf-8").split("\n")):
        try:
            d = json.loads(line)
        except Exception:
            stamps.append(None)
            continue
        stamps.append(d.get("timestamp") if isinstance(d, dict) else None)
        if not isinstance(d, dict):
            continue
        if codex:
            p = d.get("payload") or {}
            if d.get("type") != "response_item":
                continue
            if p.get("type") == "message":
                role, text = p.get("role"), "".join(c.get("text", "") for c in p.get("content", []) if isinstance(c, dict))
            elif p.get("type") in ("function_call_output", "custom_tool_call_output"):
                o = p.get("output")
                role, text = "toolResult", o if isinstance(o, str) else json.dumps(o, ensure_ascii=False)
            elif p.get("type") in ("function_call", "custom_tool_call"):
                role, text = "toolCall", str(p.get("arguments") or p.get("input") or "")
            else:
                continue
            es[str(d["ordinal"])] = (n, d["timestamp"], role, text)
        elif d.get("type") == "message":
            m = d.get("message", {})
            es[d["id"]] = (n, d["timestamp"], m.get("role"), text_of(m))
    return stamps, es


def prefix(stamps, cutoff):
    c = ts(cutoff)
    for n, s in enumerate(stamps):
        if s and ts(s) >= c:
            return n
    return len(stamps)


cache = {}


def load(key):
    if key not in cache:
        if key not in MANIFEST:
            fail(f"{key} is not in the frozen corpus manifest")
        data = open(f"{CORPUS}/{key}", "rb").read()
        sha = hashlib.sha256(data).hexdigest()
        if MANIFEST[key] != sha:
            fail(f"{key} differs from the frozen corpus manifest")
        cache[key] = (sha, data) + (parse(key, data) if key in INDEXABLE else ([], {}))
    return cache[key]


drafts = [json.loads(l) for p in drafts_paths for l in open(p) if l.strip()]
ids = [q["id"] for q in drafts]
if len(ids) != len(set(ids)):
    fail("duplicate ids")
evkey = lambda f: f if f.startswith(("codex/", "codex-archived/")) else "pi/" + f
seen = {}
for q in drafts:
    groups = [("task_cluster", q["task_cluster"])] + [("fact_group", g) for g in q.get("fact_group", [])]
    groups += [("answer record", f"{evkey(e['file'])}#{e['entry']}") for e in q["evidence"] if e["use"] == "answer"]
    for kind, g in groups:
        if seen.setdefault((kind, g), q["split"]) != q["split"]:
            fail(f"{q['id']}: {kind} {g!r} spans splits")
for q in drafts:
    for ev in q["evidence"]:
        key = evkey(ev["file"])
        sha, _, stamps, es = load(key)
        if key not in INDEXABLE:
            if q["needs_history"]:
                fail(f"{q['id']}: evidence {ev['entry']} is in a non-indexed file {key}")
            stamps, es = parse(key, cache[key][1])
        if ev["entry"] not in es:
            fail(f"{q['id']}: no entry {ev['entry']} in {key}")
        n, t, role, text = es[ev["entry"]]
        if ev["quote"] not in text:
            fail(f"{q['id']}: quote not in {ev['entry']}: {ev['quote']!r}")
        ev.update(sha256=sha, line=n, timestamp=t, role=role)
        if q["needs_history"] and n >= prefix(stamps, q["cutoff"]):
            fail(f"{q['id']}: evidence {ev['entry']} (line {n}, {t}) is outside the visible prefix")
    if q["needs_history"] and not q["evidence"]:
        fail(f"{q['id']}: history question without evidence")
    if q["type"].startswith("correction") or q["type"] == "state-update-other-session":
        old = [e for e in q["evidence"] if e["use"] == "old-claim"]
        cor = [e for e in q["evidence"] if e["use"] == "correction"]
        if not old or not cor:
            fail(f"{q['id']}: {q['type']} needs old-claim and correction evidence")
        o, other = old[0], q["type"].endswith("other-session")
        c = [e for e in cor if (e["file"] != o["file"]) == other]
        if not c:
            fail(f"{q['id']}: no correction in {'another' if other else 'the same'} file as the old claim")
        if not other and min(e["line"] for e in c) <= o["line"]:
            fail(f"{q['id']}: correction is not after the old claim")

old_umask = os.umask(0o077)
for d in ("public/visible", "grader/evidence"):
    os.makedirs(f"{outdir}/{d}")
man_sha = hashlib.sha256(MAN_BYTES).hexdigest()
cutoffs = sorted({q["cutoff"] for q in drafts})
vis_name = {c: c.replace(":", "") + ".json" for c in cutoffs}
for c in cutoffs:
    files = {}
    for key in sorted(INDEXABLE):
        sha, _, stamps, _ = load(key)
        k = prefix(stamps, c)
        if k:
            files[key] = [sha, k]
    with open(f"{outdir}/public/visible/{vis_name[c]}", "w") as f:
        json.dump({"corpus": CORPUS, "corpus_manifest_sha256": man_sha, "cutoff": c, "files": files}, f, indent=0)
public = ["id", "question", "current_context", "cutoff"]
with open(f"{outdir}/public/questions.jsonl", "w") as f:
    for q in drafts:
        f.write(json.dumps({**{k: q[k] for k in public}, "visible": "visible/" + vis_name[q["cutoff"]]}, ensure_ascii=False) + "\n")
with open(f"{outdir}/grader/grading.jsonl", "w") as f:
    for q in drafts:
        f.write(json.dumps({k: v for k, v in q.items() if k not in ("question", "current_context")}, ensure_ascii=False) + "\n")
for q in drafts:
    for ev in q["evidence"]:
        sha, data = cache[evkey(ev["file"])][:2]
        p = f"{outdir}/grader/evidence/{sha}.jsonl"
        if not os.path.exists(p):
            open(p, "wb").write(data)
os.umask(old_umask)
for dp, ds, fs in os.walk(outdir):
    os.chmod(dp, 0o700)
    for fn in fs:
        os.chmod(f"{dp}/{fn}", 0o600)
h = lambda p: hashlib.sha256(open(f"{outdir}/{p}", "rb").read()).hexdigest()
print(json.dumps({"questions": len(drafts), "cutoffs": len(cutoffs), "questions.jsonl": h("public/questions.jsonl"), "grading.jsonl": h("grader/grading.jsonl")}))
