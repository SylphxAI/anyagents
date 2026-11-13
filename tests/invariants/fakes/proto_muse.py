"""`muse serve` (Muse 1.4.18): JSON-RPC 2.0, one object per line.

`turn/start {commandId, sessionId, input, ifBusy: "steer"}` opens a turn when
none runs (the turn is named after its `commandId`) and otherwise writes the
input durably into the running turn's inbox before it answers. The answer is
an admission ack only: `{commandId, status, disposition, turnId,
startedNewTurn, queuedAtUs}`. What proves the model saw an input is the
`userMessage` item (`item/completed`, no `item/started`) the turn emits when
the input enters the conversation: at the turn's start for its own prompt, at
the next step boundary for steered input, every waiting steer together and
oldest first. `turn/started` is emitted for a turn's own submit, never for a
steer. `turn/completed` closes a turn with its usage.

A `commandId` is an idempotency key, durable within the session: replaying it
with the same payload answers the same ack and applies nothing; reusing it
with a different payload is refused (-32030). A refusal (-32030
`commandRejected`, which the `refuse` command produces for the next steer) is
recorded against its command the same way, and the input is not applied.

A steer still waiting when its turn ends is not dropped: it starts a
successor turn whose `turnId` is that steer's `commandId`, and every other
waiting steer drains into it. The inbox is durable: a process that dies with
steers waiting leaves them to the next process on the session, which runs
them in such a successor turn once the session is resumed.
"""
import hashlib
import json
import time
import uuid

from engine import Input, Protocol, marker_id

COMMANDS = "muse-commands.json"
INBOX = "muse-inbox.json"
SESSION = "muse-session"
VERSION = "1.4.18-R8587.1"

COMMAND_REJECTED = -32030
SESSION_NOT_FOUND = -32020
METHOD_NOT_FOUND = -32601


def now_us():
    return int(time.time() * 1_000_000)


class Muse(Protocol):
    """`muse serve` over the shared engine."""

    # Muse deduplicates by `commandId`, which this front end does itself; the
    # engine records every input it is handed, so a second application of one
    # mailbox message under another command shows in the journal.
    idempotent = False

    def __init__(self, engine, args):
        super().__init__(engine, args)
        self.cursor = 0
        self.queued_turns = []
        # Steers a dead process left waiting, run once the session resumes.
        self.leftover = self.load(INBOX, {"turn": None, "items": []})
        self.save_inbox()

    # The fixture directory is the session's durable state.

    def load(self, name, default):
        path = self.engine.root / name
        return json.loads(path.read_text()) if path.exists() else default

    def store(self, name, value):
        (self.engine.root / name).write_text(json.dumps(value))

    def save_inbox(self):
        """The running turn's waiting input, as it would be after a crash."""
        items = [{"id": inp.id, "text": inp.text, "commandId": inp.meta["commandId"]}
                 for inp in self.engine.pending]
        items += self.leftover["items"]
        self.store(INBOX, {"turn": self.engine.turn or self.leftover["turn"], "items": items})

    def record(self, command, fingerprint, answer):
        commands = self.load(COMMANDS, {})
        commands[command] = dict(answer, fingerprint=fingerprint)
        self.store(COMMANDS, commands)

    # The wire.

    def reply(self, rid, result):
        self.emit({"jsonrpc": "2.0", "id": rid, "result": result})

    def error(self, rid, error):
        self.emit({"jsonrpc": "2.0", "id": rid, "error": error})

    def note(self, method, params):
        self.cursor += 1
        params = dict(params, sessionId=self.session, viewCursor=str(self.cursor))
        self.emit({"jsonrpc": "2.0", "method": method, "params": params})

    def user_message(self, inp, turn, steered):
        item = {"itemId": str(uuid.uuid4()), "kind": "userMessage", "turnId": turn,
                "commandId": inp.meta["commandId"], "revision": 1, "status": "completed",
                "recordedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "text": inp.text}
        if steered:
            item["steered"] = True
        self.note("item/completed", {"item": item})

    def session_result(self, turn=None):
        return {"session": {"sessionId": self.session, "activeTurnId": turn,
                            "workspaceRoot": str(self.engine.root)}}

    def on_line(self, msg):
        method, rid = msg.get("method"), msg.get("id")
        params = msg.get("params") or {}
        if method == "initialize":
            self.reply(rid, {"serverInfo": VERSION, "userAgent": "muse/%s (fake)" % VERSION,
                             "schema": {"version": 1, "fingerprint": "fake"}})
        elif method == "initialized":
            pass
        elif method == "session/start":
            self.session = str(uuid.uuid4())
            (self.engine.root / SESSION).write_text(self.session)
            self.store(COMMANDS, {})
            self.leftover = {"turn": None, "items": []}
            self.save_inbox()
            self.reply(rid, self.session_result())
        elif method == "session/resume":
            stored = self.engine.root / SESSION
            wanted = params.get("sessionId")
            if not stored.exists() or stored.read_text() != wanted:
                self.error(rid, {"code": SESSION_NOT_FOUND, "message": "session not found",
                                 "data": {"kind": "sessionNotFound", "retryable": False}})
                return
            self.session = wanted
            self.reply(rid, self.session_result(self.engine.turn))
            self.resume_leftover()
        elif method == "session/setApprovalMode":
            self.reply(rid, {"commandId": params.get("commandId"), "status": "accepted"})
        elif method == "turn/start":
            self.turn_start(rid, params)
        elif method == "turn/cancel":
            self.reply(rid, {"commandId": params.get("commandId"), "status": "accepted"})
            if self.engine.turn is not None:
                self.finish("cancelled")
        elif rid is not None and method:
            self.error(rid, {"code": METHOD_NOT_FOUND, "message": "method not found"})

    def turn_start(self, rid, params):
        engine = self.engine
        command = params.get("commandId")
        text = "".join(part.get("text", "") for part in params.get("input") or []
                       if part.get("type") == "text")
        inp = Input(marker_id(text) or command, text, "turn/start", {"commandId": command})
        engine.received(inp)
        payload = {key: value for key, value in params.items() if key != "commandId"}
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        known = self.load(COMMANDS, {}).get(command)
        if known is not None:
            if known["fingerprint"] != fingerprint:
                error = {"code": COMMAND_REJECTED, "message": "commandId reused with another payload",
                         "data": {"kind": "commandRejected", "retryable": False,
                                  "commandId": command, "reason": "fingerprintMismatch"}}
                self.later(lambda: self.error(rid, error))
            elif "error" in known:
                self.later(lambda: self.error(rid, known["error"]))
            else:
                self.later(lambda: self.reply(rid, known["ack"]))
            return
        if engine.turn is not None and params.get("ifBusy") == "steer":
            if self.take_refusal():
                error = {"code": COMMAND_REJECTED, "message": "the running turn refused the steer",
                         "data": {"kind": "commandRejected", "retryable": True,
                                  "commandId": command, "reason": "turnClosing"}}
                self.record(command, fingerprint, {"error": error})
                self.later(lambda: self.error(rid, error))
                return
            ack = self.ack(command, "steered", engine.turn, False)
            self.record(command, fingerprint, {"ack": ack})
            engine.queue(inp)
            self.save_inbox()
            self.later(lambda: self.reply(rid, ack))
            return
        if engine.turn is not None:
            # `ifBusy` absent or `queue`: a turn of its own, after this one.
            ack = self.ack(command, "queued", command, False)
            self.record(command, fingerprint, {"ack": ack})
            self.queued_turns.append(inp)
            self.later(lambda: self.reply(rid, ack))
            return
        ack = self.ack(command, "started", command, True)
        self.record(command, fingerprint, {"ack": ack})
        engine.open_turn(inp, command)

        def started():
            self.reply(rid, ack)
            self.note("turn/started", {"turnId": command, "commandId": command})
            self.user_message(inp, command, False)
        self.later(started)

    def ack(self, command, disposition, turn, started):
        return {"commandId": command, "status": "accepted", "disposition": disposition,
                "turnId": turn, "startedNewTurn": started, "queuedAtUs": now_us()}

    def consumed(self, taken):
        for inp, _ in taken:
            self.user_message(inp, self.engine.turn, True)
        self.save_inbox()

    def successor(self, waiting):
        """Run steers a turn ended on in a turn of their own, named after the
        oldest one's command."""
        first, rest = waiting[0], waiting[1:]
        turn = first.meta["commandId"]
        self.engine.open_turn(first, turn)
        self.note("turn/started", {"turnId": turn, "commandId": turn})
        self.user_message(first, turn, True)
        for inp in rest:
            self.engine.queue(inp)
        self.consumed(self.engine.step())

    def resume_leftover(self):
        items = self.leftover["items"]
        self.leftover = {"turn": None, "items": []}
        if items and self.engine.turn is None:
            self.successor([Input(row["id"], row["text"], "turn/start",
                                  {"commandId": row["commandId"]}) for row in items])
        self.save_inbox()

    def finish(self, terminal="completed"):
        turn = self.engine.turn
        waiting = self.engine.end_turn()
        self.note("turn/completed", {
            "turnId": turn, "terminal": terminal, "durationMs": 1,
            "usage": {"inputTokens": 1, "outputTokens": 1, "cachedInputTokens": 0}})
        if waiting:
            self.successor(waiting)
        elif self.queued_turns:
            inp = self.queued_turns.pop(0)
            turn = inp.meta["commandId"]
            self.engine.open_turn(inp, turn)
            self.note("turn/started", {"turnId": turn, "commandId": turn})
            self.user_message(inp, turn, False)
        self.save_inbox()


PROTOCOLS = {"muse": Muse}
