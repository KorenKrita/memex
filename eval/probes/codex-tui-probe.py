"""Codex interactive TUI hook probe (DESIGN §11 item 1). Synthetic prompt only.

Stops any app-server daemon under CODEX_HOME first, so the daemon the TUI starts inherits this
MEMEX_PROBE_LOG (in 0.162.0-alpha.2 the TUI attaches to a shared daemon and the daemon runs the hooks with
its own environment). Runs `codex` in a pseudo-terminal, waits until the screen is quiet (the TUI is
ready), types the prompt, waits for this session's Stop hook, quits with Ctrl-C, checks the real exit code,
then watches up to --end-wait seconds for this session's SessionEnd; if none, stops the daemon and watches
10 s more. Prints one JSON line with event times relative to the prompt submit.

  CODEX_HOME=/tmp/memex-eval/codex-home MEMEX_PROBE_LOG=/tmp/memex-eval/codex-tui.jsonl \
    python3 eval/probes/codex-tui-probe.py "Reply with the single word: ok" gpt-6-sol [--end-wait 300]
"""
import json, os, pty, select, subprocess, sys, time

args = sys.argv[1:]
end_wait = 300
if "--end-wait" in args:
    i = args.index("--end-wait")
    end_wait = int(args[i + 1])
    del args[i : i + 2]
prompt, model = args
log = os.environ["MEMEX_PROBE_LOG"]
home = os.environ.get("CODEX_HOME", "")
if not home or os.path.realpath(home) == os.path.realpath(os.path.expanduser("~/.codex")):
    sys.exit("set CODEX_HOME to an isolated probe home (not ~/.codex): this probe stops that home's daemon")
daemon = ["codex", "app-server", "daemon"]
subprocess.run(daemon + ["stop"], capture_output=True)
start = os.path.getsize(log) if os.path.exists(log) else 0


def hooks():
    if not os.path.exists(log):
        return []
    with open(log) as f:
        f.seek(start)
        return [json.loads(l) for l in f if l.strip()]


pid, fd = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm-256color"
    os.execvp("codex", ["codex", "-m", model, "-s", "read-only", "-a", "never", "--no-alt-screen"])

screen = bytearray()
last_out = time.time()
closed = False


def pump(seconds):
    global last_out, closed
    end = time.time() + seconds
    while not closed and time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.1)
        if not r:
            continue
        try:
            data = os.read(fd, 65536)
        except OSError:
            data = b""
        if not data:
            closed = True
            break
        screen.extend(data)
        last_out = time.time()
        if b"\x1b[6n" in data:  # answer cursor-position queries
            os.write(fd, b"\x1b[1;1R")


def write(b):
    try:
        os.write(fd, b)
        return True
    except OSError:
        return False


t_launch = time.time()
while time.time() - t_launch < 60 and not (time.time() - t_launch > 5 and time.time() - last_out > 2):
    pump(0.3)  # ready = at least 5 s after launch and 2 s without output
ready_s = round(time.time() - t_launch, 1)
for ch in prompt:
    write(ch.encode())
    time.sleep(0.01)
pump(0.5)
t_submit = time.time()
write(b"\r")
session = None
while time.time() - t_submit < 180 and not closed:
    hs = hooks()
    starts = [h for h in hs if h["event"] == "SessionStart"]
    session = session or (starts[0]["session"] if starts else None)
    if session and any(h["event"] == "Stop" and h["session"] == session for h in hs):
        break
    pump(0.5)
pump(2)
for _ in range(2):
    if not write(b"\x03"):
        break
    pump(1.5)
status = None
t_quit = time.time()
while time.time() - t_quit < 30:
    done, st = os.waitpid(pid, os.WNOHANG)
    if done:
        status = os.waitstatus_to_exitcode(st)
        break
    pump(0.3)
if status is None:
    os.kill(pid, 9)
    os.waitpid(pid, 0)
t_exit = time.time()


def ended():
    return any(h["event"] == "SessionEnd" and h["session"] == session for h in hooks())


while session and time.time() - t_exit < end_wait and not ended():
    time.sleep(1)
end_before_daemon_stop = ended()
daemon_stopped_at = None
if session and not end_before_daemon_stop:
    daemon_stopped_at = time.time()
    subprocess.run(daemon + ["stop"], capture_output=True)
    for _ in range(10):
        if ended():
            break
        time.sleep(1)
rel = lambda t: None if t is None else round(t - t_submit, 1)
mine = [h for h in hooks() if h["session"] == session]
others = sorted({h["session"] for h in hooks()} - {session})
result = {
    "session": session,
    "readyAfterLaunchS": ready_s,
    "exitCode": status if status is not None else "killed",
    "exitAt": rel(t_exit),
    "events": [{"event": h["event"], "at": rel(h["t"]), "lines": h.get("lines"), "tail": h.get("tail", [])[-2:]} for h in mine],
    "sessionEndBeforeDaemonStop": end_before_daemon_stop,
    "daemonStoppedAt": rel(daemon_stopped_at),
    "otherSessionsInLog": others,
}
print(json.dumps(result, ensure_ascii=False))
names = [e["event"] for e in result["events"]]
sys.exit(0 if status == 0 and "Stop" in names and not others else 1)
