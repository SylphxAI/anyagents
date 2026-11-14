"""`muse serve` (Muse 1.4.20): JSON-RPC 2.0, one object per line.

`turn/start {commandId, sessionId, input, ifBusy: "steer"}` (`input` text
parts, then `image` parts `{mediaType, base64Data, width, height}`) opens a turn when
none runs (the turn is named after its `commandId`) and otherwise writes the
input durably into the running turn's inbox before it answers. The answer is
an admission ack only: `{commandId, status, disposition, turnId,
startedNewTurn}`, plus `queuedAtUs` on a fresh `queued` or `steered`
admission, and only to a connection that opted into `experimentalApi`: never
on `started`, and never on the replay of a command an earlier process
admitted. What proves the model saw an input is the `userMessage` item
(`item/completed`, no `item/started`) the turn emits when the input enters
the conversation: at the turn's start for its own prompt, at the next step
boundary for steered input, every waiting steer together and oldest first.
`turn/started` is emitted for a turn's own submit, never for a steer.
`turn/completed` closes a turn with its usage.

A `commandId` is an idempotency key, durable within the session: replaying it
with the same payload answers the recorded decision and applies nothing;
reusing it with a different payload is refused -32030 `commandRejected`,
reason `command_id_conflict`. Any other -32030 is a decision recorded against
the id, which the same id answers again; its `data.retryable` says whether a
retry under another id can succeed. The `refuse` command makes the next steer
meet one: `runtime_busy`, a turn command refused at its deadline, retryable.
The input is not applied.

The session's record is what the model saw (the engine's journal): a resume
without `excludeItems: true` serves it as `inline` history, one `userMessage`
item per input, with the `commandId` that wrote it.

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

from engine import Input, Protocol, image_b64, marker_id

# The image types Muse stores as input.
IMAGE_TYPES = ("image/png", "image/jpeg", "image/webp")

COMMANDS = "muse-commands.json"
INBOX = "muse-inbox.json"
SESSION = "muse-session"
VERSION = "1.4.20-R8619.1"

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
    # Muse records an input before it announces it: its view events, the
    # `userMessage` item among them, fold from durable records.
    stores_after_proof = False

    def __init__(self, engine, args):
        super().__init__(engine, args)
        self.cursor = 0
        self.queued_turns = []
        self.experimental = False
        # Commands an earlier process admitted: their replay drops `queuedAtUs`.
        self.earlier = set(self.load(COMMANDS, {}))
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

    def record(self, command, ident, fingerprint, answer):
        commands = self.load(COMMANDS, {})
        commands[command] = dict(answer, fingerprint=fingerprint, id=ident)
        self.store(COMMANDS, commands)

    def history(self):
        """The session's record: one `userMessage` item per input its model
        saw, with the command that wrote it."""
        command_of = {row["id"]: command for command, row in self.load(COMMANDS, {}).items()
                      if "ack" in row}
        path = self.engine.root / "journal.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        items = []
        for row in rows:
            if row["kind"] == "lost":
                continue
            item = {"itemId": "item-%d" % len(items), "kind": "userMessage", "turnId": row["turn"],
                    "revision": 1, "status": "completed", "text": row["text"]}
            if row["id"] in command_of:
                item["commandId"] = command_of[row["id"]]
            if row["kind"] == "steer":
                item["steered"] = True
            items.append(item)
        return items

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
            self.experimental = (params.get("capabilities") or {}).get("experimentalApi") is True
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
            result = self.session_result(self.engine.turn)
            result["viewCursor"] = str(self.cursor)
            if params.get("excludeItems") is True:
                result["history"] = {"mode": "none", "items": None, "snapshot": None,
                                     "noneReason": "excludeItems"}
            else:
                result["history"] = {"mode": "inline", "items": self.history(), "snapshot": None}
            self.reply(rid, result)
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
        parts = params.get("input") or []
        text = "".join(part.get("text", "") for part in parts if part.get("type") == "text")
        # `{"type": "image", "mediaType", "base64Data"}` parts follow the text.
        images = [image_b64(part["base64Data"], part["mediaType"])
                  for part in parts if part.get("type") == "image"]
        assert all(image["mime"] in IMAGE_TYPES for image in images), images
        inp = Input(marker_id(text) or command, text, "turn/start", {"commandId": command},
                    images)
        engine.received(inp)
        payload = {key: value for key, value in params.items() if key != "commandId"}
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        known = self.load(COMMANDS, {}).get(command)
        if known is not None:
            if known["fingerprint"] != fingerprint:
                error = {"code": COMMAND_REJECTED,
                         "message": "command id replayed with a different payload",
                         "data": {"kind": "commandRejected", "retryable": False,
                                  "commandId": command, "reason": "command_id_conflict"}}
                self.later(lambda: self.error(rid, error))
            elif "error" in known:
                self.later(lambda: self.error(rid, known["error"]))
            else:
                ack = dict(known["ack"])
                if command in self.earlier:
                    ack.pop("queuedAtUs", None)
                self.later(lambda: self.reply(rid, ack))
            return
        if engine.turn is not None and params.get("ifBusy") == "steer":
            if self.take_refusal():
                # Refused at the turn command's deadline: recorded against
                # the id, and a retry under another id can succeed.
                error = {"code": COMMAND_REJECTED, "message": "the runtime is busy",
                         "data": {"kind": "commandRejected", "commandId": command,
                                  "reason": "runtime_busy", "retryable": True}}
                self.record(command, inp.id, fingerprint, {"error": error})
                self.later(lambda: self.error(rid, error))
                return
            ack = self.ack(command, "steered", engine.turn, False)
            self.record(command, inp.id, fingerprint, {"ack": ack})
            engine.queue(inp)
            self.save_inbox()
            self.later(lambda: self.reply(rid, ack))
            return
        if engine.turn is not None:
            # `ifBusy` absent or `queue`: a turn of its own, after this one.
            ack = self.ack(command, "queued", command, False)
            self.record(command, inp.id, fingerprint, {"ack": ack})
            self.queued_turns.append(inp)
            self.later(lambda: self.reply(rid, ack))
            return
        ack = self.ack(command, "started", command, True)
        self.record(command, inp.id, fingerprint, {"ack": ack})
        engine.open_turn(inp, command)

        def started():
            self.reply(rid, ack)
            self.note("turn/started", {"turnId": command, "commandId": command})
            self.user_message(inp, command, False)
        self.later(started)

    def ack(self, command, disposition, turn, started):
        ack = {"commandId": command, "status": "accepted", "disposition": disposition,
               "turnId": turn, "startedNewTurn": started}
        if disposition != "started" and self.experimental:
            ack["queuedAtUs"] = now_us()
        return ack

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
        # The inbox drains as the session records what its model took: an
        # input the record holds is no longer waiting, even when the process
        # died before it rewrote the inbox.
        seen = self.engine.journal_ids(kept_only=True)
        items = [row for row in self.leftover["items"] if row["id"] not in seen]
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
