"""Codex interactive TUI hook probe (DESIGN §11 item 1). Runs `codex` (no subcommand) in a pseudo-terminal
with the given CODEX_HOME (hooks configured and trusted there, see codex-hook-probe.sh), types one prompt,
waits for the hook log to show Stop, then sends Ctrl-C twice to quit and waits for exit. Synthetic prompt.

In 0.162.0-alpha.2 the TUI attaches to a shared app-server daemon under CODEX_HOME and the daemon runs the
hooks with *its* environment: MEMEX_PROBE_LOG must match the daemon's (stop it between runs with
`CODEX_HOME=... codex app-server daemon stop`), and SessionEnd arrives when the daemon drops the session,
not when the TUI exits.

  CODEX_HOME=/tmp/memex-eval/codex-home MEMEX_PROBE_LOG=/tmp/memex-eval/codex-tui.jsonl \
    python3 eval/probes/codex-tui-probe.py "Reply with the single word: ok" gpt-6-sol
"""
import json, os, pty, select, sys, time

prompt, model = sys.argv[1], sys.argv[2]
log = os.environ["MEMEX_PROBE_LOG"]
start = os.path.getsize(log) if os.path.exists(log) else 0


def events():
    if not os.path.exists(log):
        return []
    with open(log) as f:
        f.seek(start)
        return [json.loads(l).get("event") for l in f if l.strip()]


pid, fd = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm-256color"
    os.execvp("codex", ["codex", "-m", model, "-s", "read-only", "-a", "never", "--no-alt-screen"])

screen = bytearray()


def pump(seconds):
    end = time.time() + seconds
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.2)
        if r:
            try:
                data = os.read(fd, 65536)
            except OSError:
                return False
            if not data:
                return False
            screen.extend(data)
            # answer cursor-position queries so the TUI does not wait on them
            if b"\x1b[6n" in data:
                os.write(fd, b"\x1b[1;1R")
    return True


def wait_for(pred, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        if not pump(0.5):
            return False
    return False


ok_start = wait_for(lambda: "SessionStart" in events(), 60)
pump(2)  # let the composer settle
for ch in prompt:
    os.write(fd, ch.encode())
    time.sleep(0.01)
pump(0.5)
os.write(fd, b"\r")
ok_stop = wait_for(lambda: "Stop" in events(), 180)
pump(2)
closed_before_quit = not pump(0)
for _ in range(2):
    try:
        os.write(fd, b"\x03")
    except OSError:
        closed_before_quit = True
        break
    pump(1.5)
deadline = time.time() + 30
status = None
while time.time() < deadline:
    done, st = os.waitpid(pid, os.WNOHANG)
    if done:
        status = st
        break
    pump(0.5)
if status is None:
    os.kill(pid, 9)
    _, status = os.waitpid(pid, 0)
    exited = "killed after 30s"
else:
    exited = f"exit {os.waitstatus_to_exitcode(status)}"
time.sleep(2)  # hooks run in child processes; give a late SessionEnd time to land
tail = screen[-1500:].decode(errors="replace")
print(json.dumps({"sessionStartSeen": ok_start, "stopSeen": ok_stop, "ptyClosedBeforeQuit": closed_before_quit, "process": exited, "events": events(), "screenTail": tail}))
sys.exit(0 if ok_start and ok_stop and exited.startswith("exit") else 1)
