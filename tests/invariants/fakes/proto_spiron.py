"""`spiron --headless --json` (`spiron.headless/v1`), with and without
`steer_consumed` and `idempotent_input`."""
from engine import Input, Protocol


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

    def on_line(self, msg):
        kind = msg.get("type")
        rid = msg.get("request_id")
        if kind == "cancel":
            return
        inp = Input(rid, msg.get("text", ""), kind)
        self.engine.received(inp)
        engine = self.engine
        if engine.turn is None:
            if kind == "steer":
                self.later(lambda: self.frame({"type": "rejected", "code": "no_active_turn"}, rid))
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
                                "duplicate": True, "usage": {},
                                "undelivered_steer_ids": []}, rid)))
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
        self.frame({"type": "result", "status": "completed", "is_error": False,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
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
        self.frame({"type": "result", "status": "completed", "is_error": False,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "undelivered_steers": [inp.text for inp in leftover]}, turn)


PROTOCOLS = {"spiron": Spiron, "spiron-v0": SpironV0}
