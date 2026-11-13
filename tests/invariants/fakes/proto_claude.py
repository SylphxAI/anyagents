"""Claude Code 2.1.323 `claude -p --input-format stream-json --output-format
stream-json --verbose --replay-user-messages [--resume <session>]`.

A `user` line carries a `uuid`. While a turn runs, a line is queued; the turn's
next step folds every queued line, in order, into the conversation as a
`queued_command` attachment, emitting `command_lifecycle` `started` and the
replay echo (`isReplay`) for each. A line queued when the turn ends starts the
next turn, merged with the lines queued behind it. Claude Code drops a line
whose uuid it already holds as a user row of the session's transcript (or saw
in this process), answering it with an echo; a line folded in mid-turn is
stored only as an attachment, which its dedupe does not consult. There is no
refusal of a queued line. The transcript is
`$CLAUDE_CONFIG_DIR/projects/<project>/<session>.jsonl`.
"""
import json
import os
import pathlib

from engine import Input, Protocol, marker_id

VERSION = "2.1.323"


def text_of(content):
    if isinstance(content, str):
        return content
    return "".join(block.get("text", "") for block in content or []
                   if block.get("type") == "text")


class Claude(Protocol):
    idempotent = False

    def start(self):
        if "--resume" in self.args:
            self.session = self.args[self.args.index("--resume") + 1]
        config = pathlib.Path(os.environ.get("CLAUDE_CONFIG_DIR", str(self.engine.root / "claude")))
        self.transcript = config / "projects" / "fixture-project" / (self.session + ".jsonl")
        self.transcript.parent.mkdir(parents=True, exist_ok=True)
        self.seen_uuids = set()
        self.turn_uuids = []

    def record(self, row):
        row["sessionId"] = self.session
        with self.transcript.open("a") as out:
            out.write(json.dumps(row) + "\n")

    def user_row_uuids(self):
        if not self.transcript.exists():
            return set()
        return {json.loads(line).get("uuid") for line in self.transcript.read_text().splitlines()
                if json.loads(line).get("type") == "user"}

    def echo(self, inp):
        self.emit({"type": "user", "message": {"role": "user", "content": inp.meta["content"]},
                   "session_id": self.session, "parent_tool_use_id": None,
                   "uuid": inp.meta["uuid"], "isReplay": True})

    def started(self, inp):
        self.emit({"type": "command_lifecycle", "command_uuid": inp.meta["uuid"],
                   "state": "started", "uuid": inp.meta["uuid"], "session_id": self.session})

    def begin_turn(self, inputs):
        """A turn whose prompt is `inputs`, merged, as a queued batch starts one."""
        engine = self.engine
        engine.open_turn(inputs[0])
        for inp in inputs[1:]:
            engine.journal(inp, "prompt")
        for inp in inputs:
            self.record({"type": "user", "uuid": inp.meta["uuid"],
                         "message": {"role": "user", "content": inp.meta["content"]}})
        self.turn_uuids = [inp.meta["uuid"] for inp in inputs]

        def frames():
            self.emit({"type": "system", "subtype": "init", "session_id": self.session,
                       "claude_code_version": VERSION})
            for inp in inputs:
                self.started(inp)
            self.emit({"type": "assistant", "session_id": self.session,
                       "message": {"role": "assistant",
                                   "content": [{"type": "text", "text": "working"}]}})
            for inp in inputs:
                self.echo(inp)
        self.later(frames)

    def on_line(self, msg):
        kind = msg.get("type")
        if kind == "control_request":
            self.emit({"type": "control_response",
                       "response": {"subtype": "success", "request_id": msg.get("request_id")}})
            return
        if kind != "user":
            return
        content = (msg.get("message") or {}).get("content")
        text = text_of(content)
        uuid = msg.get("uuid")
        inp = Input(marker_id(text), text, kind, {"uuid": uuid, "content": content})
        self.engine.received(inp)
        if uuid and (uuid in self.seen_uuids or uuid in self.user_row_uuids()):
            self.later(lambda: self.echo(inp))
            return
        if uuid:
            self.seen_uuids.add(uuid)
        if self.engine.turn is None:
            self.begin_turn([inp])
            return
        self.engine.queue(inp)
        self.later(lambda: self.emit({"type": "command_lifecycle", "command_uuid": uuid,
                                      "state": "queued", "uuid": uuid,
                                      "session_id": self.session}))

    def consumed(self, taken):
        for inp, _ in taken:
            self.record({"type": "attachment",
                         "attachment": {"type": "queued_command", "prompt": inp.meta["content"],
                                        "source_uuid": inp.meta["uuid"],
                                        "commandMode": "prompt"}})
            self.turn_uuids.append(inp.meta["uuid"])

        def frames():
            for inp, _ in taken:
                self.started(inp)
                self.echo(inp)
        self.later(frames)

    def finish(self):
        engine = self.engine
        uuids, self.turn_uuids = self.turn_uuids, []
        leftover = engine.end_turn()
        self.emit({"type": "result", "subtype": "success", "is_error": False,
                   "session_id": self.session, "result": "done",
                   "usage": {"input_tokens": 1, "output_tokens": 1},
                   "user_message_uuids": uuids})
        if leftover:
            self.begin_turn(leftover)


PROTOCOLS = {"claude": Claude}
