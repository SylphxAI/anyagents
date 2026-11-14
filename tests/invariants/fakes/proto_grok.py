"""Grok CLI 1.0.67 `grok agent --no-leader --always-approve stdio`: ACP,
JSON-RPC 2.0 over stdio, one object per line, with `[ui] follow_up_behavior =
"steer"` in its config.

What this models comes from the strings of the 1.0.67 binary, not from a
live session, and is marked where it is inferred:

- A `session/prompt` that arrives while a turn runs is appended to the
  session's `pending_inputs` queue ("server appended prompt to
  pending_inputs"), and the front of the queue is promoted into the running
  turn at its next safe gap ("promoting front of pending_inputs to the
  running turn"), every pending input in order (inferred from the plural
  strings).
- `x.ai/queue/changed` reports the queue whenever it changes: the running
  prompt (`runningPromptId`, `runningText`, `runningKind`,
  `runningCombinedTexts`) and the pending entries (`QueueEntryWire`: `id`,
  `owner`, `lastEditor`, `combinedTexts`, ...), under ids the server assigns.
  The text of an entry is the prompt as sent (inferred).
- A promoted input reaches the model as a `user_message_chunk` whose text
  reads "The user sent a message while you were working:\\n<user_query>...".
- A turn ends with `x.ai/session/prompt_complete`, then the `session/prompt`
  answer `{stopReason}`. A prompt merged into a running turn is answered when
  that turn ends (inferred; the binary does not establish when).
- Input still pending when a turn ends is not dropped: it becomes the next
  turn ("Converted stranded interjection into a queued prompt turn").
- Grok has no refusal that a later offer clears. With `follow_up_behavior
  = "steer"` every prompt for the loaded session is appended to
  `pending_inputs`; a prompt a `UserPromptSubmit` hook blocks "is held at
  the front of the queue" until the user sends or removes it. The texts once
  read as refusals are not protocol errors: "The session is not ready yet.
  Send again in a moment." is the TUI's (`crates/codegen/xai-grok-pager`),
  and "prompt blocked: ..." / "prompt refused: session folder removed" sit
  among the prompt handler's tracing events. So the suite's `refuse` command
  changes nothing here: the input is queued, as Grok queues it.
- An error answer comes from the ACP layer (`agent-client-protocol` 0.10.4
  in the binary): `{code, message, data?}`, message "Invalid params",
  "Internal error", ... with the detail in `data` (inferred from the crate's
  constructors). Two state files make the next `session/prompt` fail before
  it is queued, once each:
    invalid-params   -32602 "Invalid params": a request the server could
                     not read; the handler never ran it
    internal-error   -32603 "Internal error": the handler failed; whether
                     it applied the prompt is unknown to the client
- Nothing is deduplicated by an id the client sends.
"""
from engine import Input, Protocol, marker_id

VERSION = "1.0.67"
INTERJECTION = "The user sent a message while you were working:\n<user_query>\n%s\n</user_query>"
# The next prompt fails before it is queued, once, while the file exists.
ERRORS = {
    "invalid-params": (-32602, "Invalid params", "missing field `prompt`"),
    "internal-error": (-32603, "Internal error", "session store unavailable"),
}


def text_of(prompt):
    return "".join(block.get("text", "") for block in prompt or []
                   if block.get("type") == "text")


class Grok(Protocol):
    idempotent = False

    def start(self):
        self.prompts = 0
        # The request each queued or running input came in on, by input.
        self.requests = {}
        self.running = None
        self.merged = []

    def reply(self, rid, result):
        self.emit({"jsonrpc": "2.0", "id": rid, "result": result})

    def error(self, rid, code, message, data=None):
        error = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        self.emit({"jsonrpc": "2.0", "id": rid, "error": error})

    def failure(self):
        """The error the next prompt fails with, if a state file asks."""
        for name, error in ERRORS.items():
            path = self.engine.root / name
            if path.exists():
                path.unlink()
                return error
        return None

    def note(self, method, params):
        self.emit({"jsonrpc": "2.0", "method": method, "params": params})

    def entry(self, inp):
        return {"id": inp.meta["queue_id"], "text": inp.text, "kind": "user", "owner": "client",
                "lastEditor": "client", "combinedTexts": [inp.text], "createdAtMs": 0,
                "hold": False}

    def queue_changed(self):
        running = self.running
        self.note("x.ai/queue/changed", {
            "sessionId": self.session,
            "entries": [self.entry(inp) for inp in self.engine.pending],
            "runningPromptId": running.meta["queue_id"] if running else None,
            "runningText": running.text if running else None,
            "runningKind": "user" if running else None,
            "runningCombinedTexts": [inp.text for inp in [running] + self.merged] if running else [],
        })

    def begin(self, inp):
        """`inp` runs as a prompt turn of its own."""
        self.engine.open_turn(inp)
        self.running = inp
        self.merged = []

    def on_line(self, msg):
        method, rid = msg.get("method"), msg.get("id")
        params = msg.get("params") or {}
        engine = self.engine
        if method == "initialize":
            self.reply(rid, {"protocolVersion": 1,
                             "agentCapabilities": {"loadSession": True},
                             "authMethods": [],
                             "agentInfo": {"name": "grok-shell", "version": VERSION}})
        elif method == "session/new":
            self.session = "grok-session-%d" % engine.launch
            self.reply(rid, {"sessionId": self.session})
        elif method == "session/load":
            self.session = params.get("sessionId") or self.session
            self.reply(rid, {})
        elif method == "session/prompt":
            text = text_of(params.get("prompt"))
            self.prompts += 1
            inp = Input(marker_id(text), text, method,
                        {"rid": rid, "queue_id": "q-%d-%d" % (engine.launch, self.prompts)})
            engine.received(inp)
            failure = self.failure()
            if failure is not None:
                self.later(lambda: self.error(rid, *failure))
                return
            if engine.turn is None:
                self.begin(inp)
                self.later(self.queue_changed)
                return
            engine.queue(inp)
            self.later(self.queue_changed)
        elif method == "session/cancel":
            pass
        elif rid is not None and method:
            self.reply(rid, {})

    def consumed(self, taken):
        for inp, _ in taken:
            self.merged.append(inp)
            self.note("session/update", {"sessionId": self.session, "update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": INTERJECTION % inp.text}}})
        if taken:
            self.queue_changed()

    def finish(self):
        answered = [self.running] + self.merged
        stranded = self.engine.end_turn()
        self.running, self.merged = None, []
        self.note("x.ai/session/prompt_complete", {"sessionId": self.session,
                                                    "stopReason": "end_turn"})
        for inp in answered:
            self.reply(inp.meta["rid"], {"stopReason": "end_turn"})
        if stranded:
            # Converted stranded interjection into a queued prompt turn.
            self.begin(stranded[0])
            for inp in stranded[1:]:
                self.engine.queue(inp)
        self.queue_changed()


PROTOCOLS = {"grok": Grok}
