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
import hashlib, json, os, re, subprocess, sys
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


def indexable(root):
    """corpus-relative files the reference dry-run indexes under root, minus Pi run-N subagent runs"""
    out = subprocess.run(["bun", f"{HERE}/indexable.ts", root], check=True, capture_output=True, text=True).stdout
    return set(json.loads(out)["indexed"])


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
        if key.endswith(".zst"):
            cache[key] = (sha, data, [], {})  # compressed: the dry-run skips it, so it is never visible
        else:
            cache[key] = (sha, data) + parse(key, data)
    return cache[key]


drafts = [json.loads(l) for p in drafts_paths for l in open(p) if l.strip()]
ids = [q["id"] for q in drafts]
if len(ids) != len(set(ids)):
    fail("duplicate ids")
evkey = lambda f: f if f.startswith(("codex/", "codex-archived/")) else "pi/" + f
session_of = lambda f: f.split("/")[-1]  # native session file (Pi and Codex file names carry the session id)
seen = {}
for q in drafts:
    groups = [("task_cluster", q["task_cluster"])] + [("fact_group", g) for g in q.get("fact_group", [])]
    groups += [("session file", session_of(e["file"])) for e in q["evidence"]]
    for kind, g in groups:
        if seen.setdefault((kind, g), q["split"]) != q["split"]:
            fail(f"{q['id']}: {kind} {g!r} spans splits")

# Per cutoff, the corpus as it stood then (public/views/<cutoff>/, what the system under test indexes): every
# file cut to the lines before its first record at or after the cutoff (whole files are APFS clones, only
# files that span the cutoff are rewritten), Pi run-N subagent runs left out. The reference dry-run then
# decides indexability on that view, so later data cannot decide what was indexable earlier (e.g. copies
# that only diverge after the cutoff).
old_umask = os.umask(0o077)
for d in ("public/visible", "public/views", "grader/evidence"):
    os.makedirs(f"{outdir}/{d}")
cutoffs = sorted({q["cutoff"] for q in drafts})
vis_name = {c: c.replace(":", "") + ".json" for c in cutoffs}
RUN = re.compile(r"^pi/.*/run-\d+/")
VIS = {}  # cutoff -> {key: (sha256 of the whole frozen file, visible lines)}
for c in cutoffs:
    root = f"{outdir}/public/views/{vis_name[c][:-5]}"
    cut = {}
    whole = {}  # target dir -> whole files to clone there (one cp per directory)
    for key in MANIFEST:
        sha, data, stamps, _ = load(key)
        k = prefix(stamps, c) if stamps else 0
        if not k or RUN.match(key):
            continue
        os.makedirs(os.path.dirname(f"{root}/{key}"), exist_ok=True)
        if k == len(stamps):
            whole.setdefault(os.path.dirname(f"{root}/{key}"), []).append(f"{CORPUS}/{key}")
        else:
            open(f"{root}/{key}", "wb").write(b"\n".join(data.split(b"\n")[:k]) + b"\n")
        cut[key] = (sha, k)
    for d, srcs in whole.items():
        subprocess.run(["cp", "-c", *srcs, d + "/"], check=True)
    VIS[c] = {key: cut[key] for key in indexable(root)}
for q in drafts:
    for ev in q["evidence"]:
        key = evkey(ev["file"])
        sha, _, stamps, es = load(key)
        if q["needs_history"] and key not in VIS[q["cutoff"]]:
            fail(f"{q['id']}: evidence {ev['entry']} is in a file not indexable at the cutoff: {key}")
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

man_sha = hashlib.sha256(MAN_BYTES).hexdigest()
for c in cutoffs:
    files = {key: list(VIS[c][key]) for key in sorted(VIS[c])}
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
