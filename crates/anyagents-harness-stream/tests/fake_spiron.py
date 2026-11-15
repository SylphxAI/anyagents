"""A fake `spiron --headless --json` peer for the AnyAgents steering tests.

The fixture directory is the `--state-dir` argument when there is one (as
AnyAgents launches Spiron), else argv[1]. FAKE_SPIRON_CAPABLE=1 announces
`steer_consumed` and `idempotent_input` in `ready` and follows them, as
Spiron's headless frontend does:
- a prompt opens a turn at once; the turn is announced with `accepted` only
  after its prompt is journaled under `input:<request_id>`, so steers written
  behind it can be acknowledged before that `accepted`;
- a prompt already in the journal is announced `accepted {mode: duplicate}`
  and closed by a `result {duplicate: true}` that lists every steer admitted
  to it in `undelivered_steer_ids`; nothing runs;
- a turn that cannot journal its prompt (the session is still leased) did
  not start: its prompt and every steer admitted to it are refused
  `turn_not_started`, and no `result` follows;
- a step journals every acknowledged steer once, in order, and answers each
  with `steer_consumed`.
Without FAKE_SPIRON_CAPABLE the peer speaks the protocol as it was before
those capabilities: no `steer_consumed`, no deduplication.

The test drives the peer with files in the fixture directory; each command
file is removed once acted on:
  step     a step boundary of the announced turn
  seal     the turn takes no more steers (they are refused as turn_closed)
  finish   the announced turn ends; steers not yet journaled are listed
           undelivered
  crash    the process exits at once
  journal-crash
           the process journals every acknowledged steer, as a step does,
           and exits before it can say so
Two state files are left alone while they exist:
  hold     no new turn is announced (its prompt is not yet journaled)
  lease    a new turn cannot journal its prompt: it is refused

wire.jsonl gets every stdin line, once handled, as {"launch": n, "msg": line};
journal.jsonl is the durable session journal; steps.jsonl lists what each
step journaled.
"""
import json
import os
import pathlib
import sys
import threading
import time

args = sys.argv[1:]
root = pathlib.Path(args[args.index("--state-dir") + 1] if "--state-dir" in args else args[0])
capable = os.environ.get("FAKE_SPIRON_CAPABLE") == "1"
launches = root / "launches"
launch = int(launches.read_text()) + 1 if launches.exists() else 1
launches.write_text(str(launch))
journal = root / "journal.jsonl"
out_lock = threading.Lock()
turn_lock = threading.Lock()
# The open turn: its request id and prompt, whether `accepted` announced it,
# whether it takes more steers, and the steers acknowledged and not journaled.
active = None
prompt = ""
announced = False
sealed = False
pending = []


def emit(frame, request_id=None):
    frame["session_id"] = "fake-session"
    frame["request_id"] = request_id
    with out_lock:
        print(json.dumps(frame), flush=True)


def append(path, value):
    with path.open("a") as out:
        out.write(json.dumps(value) + "\n")


def journaled(key):
    if not journal.exists():
        return False
    return any(json.loads(line)["key"] == key for line in journal.read_text().splitlines())


def journal_input(request_id, text, turn):
    """Journal one input; False when idempotent input finds it already there."""
    key = "input:" + request_id
    if capable and journaled(key):
        return False
    append(journal, {"key": key, "text": text, "turn": turn, "launch": launch})
    return True


def take(name):
    path = root / name
    if path.exists():
        path.unlink()
        return True
    return False


def close_turn():
    global active, prompt, announced, sealed, pending
    active, prompt, announced, sealed, pending = None, "", False, False, []


def settle():
    """Journal the open turn's prompt and announce it, or refuse the turn."""
    global announced
    turn = active
    if capable and (root / "lease").exists():
        emit({"type": "rejected", "code": "turn_not_started",
              "error": "session already has an active turn"}, turn)
        for request_id, _ in pending:
            emit({"type": "rejected", "code": "turn_not_started"}, request_id)
        close_turn()
        return
    if not journal_input(turn, prompt, turn):
        emit({"type": "accepted", "mode": "duplicate"}, turn)
        emit({
            "type": "result", "status": "completed", "is_error": False, "error": None,
            "duplicate": True, "usage": {"input_tokens": 0, "output_tokens": 0},
            "undelivered_steers": [text for _, text in pending],
            "undelivered_steer_ids": [request_id for request_id, _ in pending],
        }, turn)
        close_turn()
        return
    announced = True
    emit({"type": "accepted", "mode": "turn"}, turn)


def drive():
    global sealed, pending
    while True:
        if take("crash"):
            os._exit(1)
        with turn_lock:
            if (root / "journal-crash").exists():
                for request_id, text in pending:
                    journal_input(request_id, text, active)
                take("journal-crash")
                os._exit(1)
            if active is not None and not announced and not (root / "hold").exists():
                settle()
            if (root / "step").exists() and (active is None or announced):
                steered = []
                for request_id, text in pending:
                    fresh = journal_input(request_id, text, active)
                    steered.append(request_id)
                    if capable:
                        emit({"type": "steer_consumed", "steer_request_id": request_id,
                              "turn_id": active, "duplicate": not fresh}, active)
                pending = []
                append(root / "steps.jsonl", {"launch": launch, "turn": active, "consumed": steered})
                take("step")
            if take("seal"):
                sealed = True
            if (root / "finish").exists() and active is not None and announced:
                turn = active
                frame = {
                    "type": "result", "status": "completed", "is_error": False, "error": None,
                    "usage": {"input_tokens": 3, "output_tokens": 1},
                    "undelivered_steers": [text for _, text in pending],
                }
                if capable:
                    frame["undelivered_steer_ids"] = [request_id for request_id, _ in pending]
                close_turn()
                emit(frame, turn)
                take("finish")
        time.sleep(0.005)


ready = {"type": "ready", "protocol": "spiron.headless/v1"}
if capable:
    ready["capabilities"] = ["steer_consumed", "idempotent_input"]
emit(ready)
threading.Thread(target=drive, daemon=True).start()
for line in sys.stdin:
    msg = json.loads(line)
    kind = msg.get("type")
    request_id = msg.get("request_id")
    text = msg.get("text", "")
    with turn_lock:
        # On the wire only once handled: a test that sees it can act on it.
        append(root / "wire.jsonl", {"launch": launch, "msg": msg})
        if kind in ("prompt", "user", "steer") and active is not None:
            if sealed:
                emit({"type": "rejected", "code": "turn_closed"}, request_id)
            else:
                pending.append((request_id, text))
                emit({"type": "steer_ack", "accepted": True, "turn_id": active}, request_id)
        elif kind == "steer":
            emit({"type": "rejected", "code": "no_active_turn"}, request_id)
        elif kind in ("prompt", "user"):
            active, prompt, announced, sealed, pending = request_id, text, False, False, []
        else:
            emit({"type": "rejected", "code": "unknown_command"}, request_id)
os._exit(0)
