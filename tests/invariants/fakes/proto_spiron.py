"""`spiron --headless --json` (`spiron.headless/v1`), with and without
`steer_consumed` and `idempotent_input`, as `spiron-cli`'s `headless.rs`
speaks it on the deployed branch (`fix/headless-steer-consumption`):

- Every frame carries `session_id` and `request_id` (the input's own id, or
  the turn's for the turn's frames).
- A `prompt`/`user`/`steer` with empty text is `rejected` `empty_prompt`; a
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
from engine import Input, Protocol

LEASED = "This Session already has an active turn."


class Spiron(Protocol):
    """`spiron --headless --json` with `steer_consumed` and `idempotent_input`."""

    idempotent = True

    def frame(self, frame, request_id):
        frame["session_id"] = self.session
        frame["request_id"] = request_id
        self.emit(frame)

    def start(self):
        if "--session" in self.args:
            self.session = self.args[self.args.index("--session") + 1]
        self.frame({"type": "ready", "protocol": "spiron.headless/v1",
                    "capabilities": ["steer_consumed", "idempotent_input"]}, None)

    def refused(self, msg):
        """The code Spiron refuses `msg` with before any turn sees it."""
        reject = self.engine.root / "reject"
        if reject.exists():
            code = reject.read_text().strip()
            reject.unlink()
            return code
        if msg.get("session_id") not in (None, self.session):
            return "wrong_session"
        if msg.get("type") not in ("prompt", "user", "steer", "cancel", "interrupt"):
            return "unknown_command"
        if msg.get("type") != "cancel" and not (msg.get("text") or "").strip():
            return "empty_prompt"
        return None

    def on_line(self, msg):
        kind = msg.get("type")
        rid = msg.get("request_id")
        if kind in ("cancel", "interrupt"):
            return
        inp = Input(rid, msg.get("text", ""), kind)
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


class SpironV0(Spiron):
    """A Spiron from before `steer_consumed`: `ready` names no capability,
    `accepted` opens a turn, a mid-turn input is only acknowledged, and the
    result lists the acknowledged steers it never applied, by text."""

    idempotent = False

    def start(self):
        if "--session" in self.args:
            self.session = self.args[self.args.index("--session") + 1]
        self.frame({"type": "ready", "protocol": "spiron.headless/v1"}, None)

    def on_line(self, msg):
        kind = msg.get("type")
        rid = msg.get("request_id")
        if kind == "cancel":
            return
        inp = Input(rid, msg.get("text", ""), kind)
        self.engine.received(inp)
        engine = self.engine
        if engine.turn is None:
            engine.open_turn(inp, rid)
            turn = engine.turn
            self.later(lambda: self.frame({"type": "accepted", "mode": "turn"}, turn))
            return
        engine.queue(inp)
        turn = engine.turn
        self.later(lambda: self.frame(
            {"type": "steer_ack", "accepted": True, "turn_id": turn}, rid))

    def consumed(self, taken):
        pass

    def finish(self):
        turn = self.engine.turn
        leftover = self.engine.end_turn()
        self.frame({"type": "result", "status": "completed", "is_error": False, "error": None,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "undelivered_steers": [inp.text for inp in leftover]}, turn)


PROTOCOLS = {"spiron": Spiron, "spiron-v0": SpironV0}
