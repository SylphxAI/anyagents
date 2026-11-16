"""`spiron --headless --json` (`spiron.headless/v2` over Unix socket):

- Every frame carries `session_id`, `request_id`, and monotonic `seq`.
- A `prompt`/`steer` with empty text is `rejected` `empty_prompt`; a
  line naming another session `wrong_session`; an unknown `type`
  `unknown_command`. No offer makes Spiron take such a line.
- A `steer` with no turn running is `rejected` `no_active_turn`; one that
  reaches a turn that sealed `turn_closed`. The next turn takes it.
- A turn that cannot take its session's lease (a process that died still
  holds it, for up to 180 s) or journal its input did not start: its prompt
  is `rejected` `turn_not_started` with the `error`, and no `result`
  follows.
- `result` carries `status`, `is_error`, `error`, `usage`,
  `undelivered_steers` (texts) and `undelivered_steer_ids`.

Besides the engine's commands, this vendor reads these files:
  lease   while it exists, a new turn cannot start (`turn_not_started`)
  reject  the next input is refused with the code the file holds (a code
          Spiron answers for a line no offer can make it take)
"""
import json
import os
import socket
import threading
import time

from engine import Input, Protocol, image_file

LEASED = "This Session already has an active turn."
CAPABILITIES = ["steer_consumed", "idempotent_input"]


def images_of(msg):
    """Images an input line would carry (`images: [{"path", "mime_type"}]`)."""
    return [image_file(item["path"], item["mime_type"]) for item in msg.get("images") or []]


class Spiron(Protocol):
    """`spiron --headless --json` speaking `spiron.headless/v2` over Unix socket."""

    idempotent = True

    def __init__(self, engine, args):
        self.conn = None
        self.conn_lock = threading.RLock()
        self.seq = 0
        self.frames = []
        self.sock_path = None
        self.listener = None
        super().__init__(engine, args)

    def frame(self, frame, request_id):
        with self.conn_lock:
            self.seq += 1
            frame["session_id"] = self.session
            frame["request_id"] = request_id
            frame["seq"] = self.seq
            self.frames.append(dict(frame))
        self.emit(frame)

    def write_out(self):
        while True:
            not_before, line = self.outbox.get()
            wait = not_before - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            with self.conn_lock:
                if self.conn is not None:
                    try:
                        self.conn.sendall((line + "\n").encode())
                    except OSError:
                        self.conn = None
            self.outbox.task_done()

    def start(self):
        if "--session" in self.args:
            self.session = self.args[self.args.index("--session") + 1]
        self.sock_path = self.args[self.args.index("--listen") + 1]
        try:
            os.unlink(self.sock_path)
        except FileNotFoundError:
            pass
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(self.sock_path)
        os.chmod(self.sock_path, 0o660)
        self.listener.listen()

    def serve(self):
        while True:
            conn, _ = self.listener.accept()
            reader = conn.makefile("r")
            first = reader.readline()
            if not first:
                reader.close()
                conn.close()
                continue
            attach = json.loads(first)
            assert attach["type"] == "attach" and attach["protocol"] == "spiron.headless/v2", attach
            with self.conn_lock:
                if self.conn is not None:
                    try:
                        self.conn.close()
                    except OSError:
                        pass
                self.conn = conn
                after = attach.get("after_seq")
                if after is not None:
                    for frame in self.frames:
                        if frame["seq"] > after:
                            conn.sendall((json.dumps(frame) + "\n").encode())
                self.frame(
                    {
                        "type": "ready",
                        "protocol": "spiron.headless/v2",
                        "capabilities": CAPABILITIES,
                        "replay": {"after_seq": after, "complete": True},
                    },
                    attach["request_id"],
                )
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                with self.engine.lock:
                    self.on_line(msg)
            reader.close()
            conn.close()

    def refused(self, msg):
        """The code Spiron refuses `msg` with before any turn sees it."""
        reject = self.engine.root / "reject"
        if reject.exists():
            code = reject.read_text().strip()
            reject.unlink()
            return code
        if msg.get("session_id") not in (None, self.session):
            return "wrong_session"
        if msg.get("type") not in ("prompt", "steer", "cancel", "stop"):
            return "unknown_command"
        if msg.get("type") not in ("cancel", "stop") and not (msg.get("text") or "").strip():
            return "empty_prompt"
        return None

    def on_line(self, msg):
        kind = msg.get("type")
        rid = msg.get("request_id")
        if kind == "stop":
            self.frame({"type": "stopped"}, rid)
            self.flush()
            try:
                os.unlink(self.sock_path)
            except FileNotFoundError:
                pass
            os._exit(0)
        if kind == "cancel":
            return
        inp = Input(rid, msg.get("text", ""), kind, media=images_of(msg))
        self.engine.received(inp)
        engine = self.engine
        code = self.refused(msg)
        if code:
            self.later(lambda: self.frame({"type": "rejected", "code": code}, rid))
            return
        if engine.turn is None:
            if kind == "steer":
                self.later(lambda: self.frame({"type": "rejected", "code": "no_active_turn"}, rid))
                return
            if (engine.root / "lease").exists():
                self.later(lambda: self.frame(
                    {"type": "rejected", "code": "turn_not_started", "error": LEASED}, rid))
                return
            # Spiron names a turn after its prompt's request id.
            if engine.open_turn(inp, rid):
                turn = engine.turn
                self.later(lambda: self.frame({"type": "accepted", "mode": "turn"}, turn))
            else:
                engine.end_turn()
                self.later(lambda: (
                    self.frame({"type": "accepted", "mode": "duplicate"}, rid),
                    self.frame({"type": "result", "status": "completed", "is_error": False,
                                "error": None, "duplicate": True, "usage": None,
                                "undelivered_steers": [], "undelivered_steer_ids": []}, rid)))
            return
        if self.take_refusal():
            self.later(lambda: self.frame({"type": "rejected", "code": "turn_closed"}, rid))
            return
        engine.queue(inp)
        turn = engine.turn
        self.later(lambda: self.frame(
            {"type": "steer_ack", "accepted": True, "turn_id": turn}, rid))

    def consumed(self, taken):
        for inp, fresh in taken:
            self.frame({"type": "steer_consumed", "steer_request_id": inp.id,
                        "turn_id": self.engine.turn, "duplicate": not fresh}, self.engine.turn)

    def finish(self):
        turn = self.engine.turn
        leftover = self.engine.end_turn()
        self.frame({"type": "result", "status": "completed", "is_error": False, "error": None,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "undelivered_steers": [inp.text for inp in leftover],
                    "undelivered_steer_ids": [inp.id for inp in leftover]}, turn)


PROTOCOLS = {"spiron": Spiron}
