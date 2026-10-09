"""Freeze the whole session corpus once (local only, not in the repo).

  python3 freeze.py OUTDIR [--exclude-file LIST]

APFS-clones ~/.pi/agent/sessions and ~/.codex/{sessions,archived_sessions} into OUTDIR/{pi,codex,codex-archived}
file by file, skipping excluded files (sessions that contain the question bank itself) before copying, then hashes
every copied file into OUTDIR/manifest.json. This is the raw archive; what a question may retrieve is defined by
build.py (visible.jsonl per question).
"""
import hashlib, json, os, subprocess, sys

out = sys.argv[1]
excl = []
if "--exclude-file" in sys.argv:
    excl = [l.strip().lstrip("./") for l in open(sys.argv[sys.argv.index("--exclude-file") + 1]) if l.strip()]
if os.path.exists(out):
    sys.exit(f"{out} exists; freeze into a new directory")
os.makedirs(out, mode=0o700)
src = {"pi": "~/.pi/agent/sessions", "codex": "~/.codex/sessions", "codex-archived": "~/.codex/archived_sessions"}
skip = {f"pi/{rel}" for rel in excl}
for name, p in src.items():
    base = os.path.expanduser(p)
    for dp, _, fs in os.walk(base):
        for fn in fs:
            rel = f"{name}/{os.path.relpath(os.path.join(dp, fn), base)}"
            if rel in skip:
                continue  # excluded before copying: nothing is ever deleted
            os.makedirs(os.path.dirname(f"{out}/{rel}"), mode=0o700, exist_ok=True)
            subprocess.run(["cp", "-c", "-p", os.path.join(dp, fn), f"{out}/{rel}"], check=True)
files = {}
for name in src:
    for dp, _, fs in os.walk(f"{out}/{name}"):
        for fn in fs:
            if fn.endswith((".jsonl", ".jsonl.zst")):
                p = f"{dp}/{fn}"
                files[os.path.relpath(p, out)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
json.dump({"sources": src, "excluded": excl, "files": dict(sorted(files.items()))}, open(f"{out}/manifest.json", "w"), indent=0)
os.chmod(f"{out}/manifest.json", 0o600)
print(len(files), "files;", hashlib.sha256(open(f"{out}/manifest.json", "rb").read()).hexdigest())
