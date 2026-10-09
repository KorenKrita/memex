"""Freeze the whole session corpus once (run locally; the corpus never enters the repo).

  uv run --no-project python freeze.py OUTDIR [--exclude-file LIST]

APFS-clones ~/.pi/agent/sessions and ~/.codex/{sessions,archived_sessions} into OUTDIR/{pi,codex,codex-archived}
file by file, then hashes every copied file into OUTDIR/manifest.json. LIST holds corpus-relative paths
(`pi/...`, `codex/...`, `codex-archived/...`) of sessions that contain the question bank itself; they are
skipped before copying (nothing is ever deleted) and a LIST entry that matches no source file is an error.
Directories are 0700 and files 0600. This is the raw archive; what a question may retrieve is defined by
build.py (public/visible/<cutoff>.json).
"""
import hashlib, json, os, subprocess, sys

out = sys.argv[1]
excl = []
if "--exclude-file" in sys.argv:
    excl = [l.strip().removeprefix("./") for l in open(sys.argv[sys.argv.index("--exclude-file") + 1]) if l.strip()]
src = {"pi": "~/.pi/agent/sessions", "codex": "~/.codex/sessions", "codex-archived": "~/.codex/archived_sessions"}
bad = [e for e in excl if e.split("/", 1)[0] not in src]
if bad:
    sys.exit(f"exclusions must start with one of {sorted(src)}/: {bad}")
if os.path.exists(out):
    sys.exit(f"{out} exists; freeze into a new directory")
plan, matched = [], set()
for name, p in src.items():
    base = os.path.expanduser(p)
    for dp, _, fs in os.walk(base):
        for fn in fs:
            rel = f"{name}/{os.path.relpath(os.path.join(dp, fn), base)}"
            if rel in excl:
                matched.add(rel)  # excluded before copying: nothing is ever deleted
            else:
                plan.append((os.path.join(dp, fn), rel))
unmatched = sorted(set(excl) - matched)
if unmatched:
    sys.exit(f"exclusions match no source file: {unmatched}")
os.umask(0o077)
os.makedirs(out)
files = {}
for path, rel in plan:
    dst = f"{out}/{rel}"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    subprocess.run(["cp", "-c", path, dst], check=True)  # APFS clone; no -p: mode comes from the umask
    os.chmod(dst, 0o600)
    if rel.endswith((".jsonl", ".jsonl.zst")):
        files[rel] = hashlib.sha256(open(dst, "rb").read()).hexdigest()
for dp, _, _ in os.walk(out):
    os.chmod(dp, 0o700)
json.dump({"sources": src, "excluded": excl, "files": dict(sorted(files.items()))}, open(f"{out}/manifest.json", "w"), indent=0)
os.chmod(f"{out}/manifest.json", 0o600)
print(len(files), "files;", hashlib.sha256(open(f"{out}/manifest.json", "rb").read()).hexdigest())
