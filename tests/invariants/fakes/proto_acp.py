"""An `acp/v1` runner on stdio (JSON lines), one session, as the `acp/v1`
wire specification defines it:

- Every control frame needs a non-empty `session_id` and `request_id` and a
  `lease_epoch >= 1`, else `protocol_error` `invalid_envelope`.
- `attach` at a `lease_epoch` below the session's active one is
  `rejected` `stale_lease_epoch`; any other frame before `attach`, or above
  the active epoch, `not_attached`. Neither carries a `seq`.
- Every other frame the runner writes is journaled first with the next
  `seq`. `attach` replays the journal after `after_seq`, then `ready`. The
  active epoch, the `seq` and the journal outlive the process.
- `deliver` while idle skips items the session already consumed (durable
  `input:<id>`): all consumed is `accepted` `duplicate` and a duplicate
  `result`; else the first new item opens the turn (`accepted` `turn`) and
  the rest are queued (`steer_ack`). While a turn runs, new items are queued
  (`steer_ack`) and an item already consumed is answered `steer_consumed`
  with `duplicate` at once. The next step consumes every queued item in
  order, one `steer_consumed` each.
- `result` hands back the items the turn never consumed in
  `undelivered_ids`; `interrupt` ends the turn the same way.
- The suite's `refuse` answers the next item that reaches a running turn
  `rejected` `turn_closed`.

`acp-turn-end` is the same runner whose `ready` names no `next_step_steer`.
"""
import json

from engine import Input, Protocol

VERSION = "1.0.0"
CONTEXT = {"window_tokens": 200000, "window_source": "fallback",
           "reported_window_tokens": 200000, "output_reserve_tokens": 8192,
           "request_limit_bytes": 16777216, "compaction_threshold_bytes": None,
           "policy": "window"}
USAGE = {"input_tokens": 1, "output_tokens": 1, "cached_tokens": 0, "cache_write_tokens": 0}


class Acp(Protocol):
    idempotent = True
    capabilities = ["next_step_steer"]

    def start(self):
        path = self.engine.root / "acp.json"
        self.durable = json.loads(path.read_text()) if path.exists() else {"lease": 0, "seq": 0}
        self.attached = False
        self.session = self.durable.get("session")

    def keep(self):
        (self.engine.root / "acp.json").write_text(json.dumps(self.durable))

    def journaled(self, frame, request_id=None):
        self.durable["seq"] += 1
        frame.update(seq=self.durable["seq"], session_id=self.session,
                     lease_epoch=self.durable["lease"], request_id=request_id)
        self.engine.append("frames.jsonl", frame)
        self.keep()
        self.emit(frame)

    def reject(self, msg, code):
        self.emit({"type": "rejected", "seq": None, "session_id": msg["session_id"],
                   "lease_epoch": msg["lease_epoch"], "request_id": msg["request_id"],
                   "code": code, "active_lease_epoch": max(self.durable["lease"], 1),
                   "error": None})

    def on_line(self, msg):
        if not (isinstance(msg.get("session_id"), str) and msg["session_id"]
                and isinstance(msg.get("request_id"), str) and msg["request_id"]
                and isinstance(msg.get("lease_epoch"), int) and msg["lease_epoch"] >= 1):
            self.emit({"type": "protocol_error", "code": "invalid_envelope",
                       "session_id": msg.get("session_id") or None, "error": None})
            return
        kind, lease = msg.get("type"), msg["lease_epoch"]
        if kind == "attach":
            if lease < self.durable["lease"]:
                return self.reject(msg, "stale_lease_epoch")
            self.attach(msg)
            return
        if lease < self.durable["lease"]:
            return self.reject(msg, "stale_lease_epoch")
        if not self.attached or lease > self.durable["lease"] or msg["session_id"] != self.session:
            return self.reject(msg, "not_attached")
        rid = msg["request_id"]
        if kind == "deliver":
            self.deliver(msg, rid)
        elif kind == "interrupt":
            if self.engine.turn is None:
                return self.reject(msg, "no_active_turn")
            turn = self.engine.turn
            self.later(lambda: (
                self.journaled({"type": "interrupt_ack", "turn_id": turn,
                                "status": "cancelling"}, rid),
                self.end(rid, "cancelled")))
        elif kind == "stop":
            if self.engine.turn is not None:
                self.end(rid, "cancelled")
            self.journaled({"type": "stopped"}, rid)
        elif kind == "host_tool_result":
            self.reject(msg, "unknown_call")
        else:
            self.reject(msg, "unknown_command")

    def attach(self, msg):
        self.durable["lease"] = msg["lease_epoch"]
        self.durable["session"] = self.session = msg["session_id"]
        self.attached = True
        self.keep()
        after = msg.get("after_seq")
        frames = self.engine.root / "frames.jsonl"
        if after is not None and frames.exists():
            for line in frames.read_text().splitlines():
                frame = json.loads(line)
                if frame["seq"] > after and frame["session_id"] == self.session:
                    self.emit(frame)
        self.journaled({"type": "ready", "protocol": "acp/v1", "protocols": ["acp/v1"],
                        "harness": msg.get("harness") or "runner", "version": VERSION,
                        "commit": None, "model": msg.get("model") or "fake-model",
                        "context": CONTEXT, "host_tools": [],
                        "capabilities": self.capabilities, "pending_host_calls": [],
                        "replay": {"after_seq": after, "complete": True}}, msg["request_id"])

    def deliver(self, msg, rid):
        engine = self.engine
        items, seen = [], set()
        for item in msg.get("items") or []:
            if item["id"] not in seen:
                seen.add(item["id"])
                inp = Input(item["id"], item["text"], "deliver")
                engine.received(inp)
                items.append(inp)
        if not items:
            return self.reject(msg, "empty_deliver")
        mode = msg.get("mode", "auto")
        consumed = engine.journal_ids(kept_only=True)
        fresh = [inp for inp in items if inp.id not in consumed]
        if engine.turn is None:
            if mode == "steer":
                return self.reject(msg, "no_active_turn")
            if not fresh:
                turn = "duplicate-%s" % rid
                self.later(lambda: (
                    self.journaled({"type": "accepted", "turn_id": turn, "mode": "duplicate"}, rid),
                    self.journaled({"type": "result", "turn_id": turn, "status": "completed",
                                    "is_error": False, "error": None, "duplicate": True,
                                    "usage": None, "cost_micros": None, "context": None,
                                    "undelivered_ids": []}, rid)))
                return
            engine.open_turn(fresh[0])
            turn, rest = engine.turn, fresh[1:]
            for inp in rest:
                engine.queue(inp)
            self.later(lambda: (
                self.journaled({"type": "accepted", "turn_id": turn, "mode": "turn"}, rid),
                rest and self.journaled({"type": "steer_ack", "turn_id": turn,
                                         "item_ids": [inp.id for inp in rest]}, rid)))
            return
        if mode == "turn":
            return self.reject(msg, "turn_open")
        if self.take_refusal():
            return self.reject(msg, "turn_closed")
        turn = engine.turn
        pending = {inp.id for inp in engine.pending}
        queued = []
        for inp in items:
            if inp.id in consumed:
                self.later(lambda inp=inp: self.journaled(
                    {"type": "steer_consumed", "steer_id": inp.id, "turn_id": turn,
                     "duplicate": True}, rid))
            elif inp.id not in pending:
                engine.queue(inp)
                queued.append(inp.id)
        if queued:
            self.later(lambda: self.journaled(
                {"type": "steer_ack", "turn_id": turn, "item_ids": queued}, rid))

    def consumed(self, taken):
        for inp, fresh in taken:
            self.journaled({"type": "steer_consumed", "steer_id": inp.id,
                            "turn_id": self.engine.turn, "duplicate": not fresh})

    def end(self, rid, status):
        turn = self.engine.turn
        leftover = self.engine.end_turn()
        self.journaled({"type": "result", "turn_id": turn, "status": status,
                        "is_error": False, "error": None, "duplicate": False,
                        "usage": USAGE, "cost_micros": None, "context": None,
                        "undelivered_ids": [inp.id for inp in leftover]}, rid)

    def finish(self):
        self.journaled({"type": "text_delta", "text": "done"})
        self.end(None, "completed")


class AcpTurnEnd(Acp):
    capabilities = []


PROTOCOLS = {"acp": Acp, "acp-turn-end": AcpTurnEnd}
