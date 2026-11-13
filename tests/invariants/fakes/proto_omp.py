"""oh-my-pi 18.12.6 `omp --mode rpc`: one JSON object per line each way.

Read from the package's own source (`src/modes/rpc/rpc-mode.ts`,
`rpc-prompt-results.ts`, `rpc-session-events.ts`,
`src/session/agent-session.ts`, `pi-agent-core/src/agent-loop.ts`):

- Without `--mode rpc` a launch whose stdin is not a terminal runs as `-p`
  with no prompt and exits with a usage error.
- The first line is `ready` {protocolVersion: 1, supportedProtocolVersions}.
  Commands are read in order. Each carries an optional string `id` and is
  answered `{id, type: "response", command, success: true[, data]}`, or
  `success: false, error` when it throws (then nothing was queued).
- A `prompt` is answered once admitted: an idle session starts a run with
  it; a busy one (`streamingBehavior: "steer"`) pushes it onto the steering
  queue (`queue_update`).
- At each step boundary (after a tool batch, before the next model call) a
  run dequeues steering, one message in the default `one-at-a-time` mode,
  every queued one after `set_steering_mode` `all`, and emits
  `message_start`/`message_end` {message: {role: "user", content,
  steering: true}, messageId: "msg-N"} for each; the opening prompt is
  emitted the same way, without `steering`, when its run starts. The session
  file records each user message at its `message_end`.
- A run polls steering again at its stop boundary, so it never ends with
  steering queued. When it yields: `agent_end`, one `prompt_result` {id,
  agentInvoked, status, sessionSettled} per prompt it took (the steered ones,
  then the one that opened it), then `session_settled`.
- Nothing is deduplicated by `id`.
- The session file (`get_state` names it `sessionFile`): a `session` header,
  then `{type: "message", id, parentId, timestamp, message}` per message.
"""
import datetime
import json
import os
import sys

from engine import Input, Protocol, marker_id

READY = {"type": "ready", "protocolVersion": 1, "supportedProtocolVersions": [1, 2],
         "maxFrameBytes": 1024 * 1024, "maxReassembledFrameBytes": 64 * 1024 * 1024}
BUSY = ("Agent is already processing. Use steer() or followUp() to queue messages, "
        "or wait for completion.")


def text_of(content):
    if isinstance(content, str):
        return content
    return "".join(block.get("text", "") for block in content or []
                   if block.get("type") == "text")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class Omp(Protocol):
    idempotent = False

    def start(self):
        args = self.args
        if "--mode" not in args or args[args.index("--mode") + 1] != "rpc":
            sys.stderr.write("Error: no prompt given and stdin is not a terminal\n")
            sys.stderr.flush()
            os._exit(2)
        self.mode = "one-at-a-time"
        self.messages = 0
        self.tickets = []
        # Steering past the one a one-at-a-time boundary takes.
        self.held = []
        if "--session" in args:
            self.session = args[args.index("--session") + 1]
        else:
            self.session = "omp-session-%d" % self.engine.launch
        sessions = self.engine.root / "omp-sessions"
        sessions.mkdir(exist_ok=True)
        self.file = sessions / ("2026-11-13T00-00-00-000Z_%s.jsonl" % self.session)
        self.parent = None
        if self.file.exists():
            lines = self.file.read_text().splitlines()
            self.parent = json.loads(lines[-1]).get("id") if lines else None
        else:
            self.persist({"type": "session", "version": 3, "id": self.session,
                          "timestamp": now(), "cwd": os.getcwd()})
        self.emit(READY)

    def persist(self, entry):
        with self.file.open("a") as out:
            out.write(json.dumps(entry) + "\n")

    def respond(self, rid, command, data=None, error=None):
        frame = {"id": rid, "type": "response", "command": command, "success": error is None}
        if error is not None:
            frame["error"] = error
        elif data is not None:
            frame["data"] = data
        self.emit(frame)

    def user_message(self, inp, steering):
        """The run took `inp`: its frames, and its line in the session file."""
        message = {"role": "user", "content": [{"type": "text", "text": inp.text}],
                   "attribution": "user", "timestamp": 0}
        if steering:
            message["steering"] = True
        entry = "%08x" % (len(self.file.read_text().splitlines()) + 1)
        self.persist({"type": "message", "id": entry, "parentId": self.parent,
                      "timestamp": now(), "message": message})
        self.parent = entry
        self.messages += 1
        frame_id = "msg-%d" % self.messages
        return [{"type": "message_start", "message": message, "messageId": frame_id},
                {"type": "message_end", "message": message, "messageId": frame_id}]

    def queue_update(self):
        steering = [inp.text for inp in self.engine.pending + self.held]
        self.emit({"type": "queue_update", "steering": steering, "followUp": [],
                   "liveSteered": 0})

    def state(self):
        engine = self.engine
        return {"sessionId": self.session, "sessionFile": str(self.file),
                "steeringMode": self.mode, "followUpMode": "one-at-a-time",
                "interruptMode": "immediate", "isStreaming": engine.turn is not None,
                "isCompacting": False, "messageCount": self.messages,
                "queuedMessageCount": len(engine.pending) + len(self.held),
                "hasPendingAsyncWork": False, "isSettled": engine.turn is None,
                "autoCompactionEnabled": True, "fastModeEnabled": False,
                "fastModeActive": False, "slowModeSupported": False, "slowModeEnabled": False,
                "tokensPerSecond": None, "todoPhases": [], "goal": None,
                "queuedMessages": {"steering": [inp.text for inp in engine.pending + self.held],
                                   "followUp": [], "liveSteered": 0}}

    def on_line(self, msg):
        kind, rid = msg.get("type"), msg.get("id")
        engine = self.engine
        if kind == "set_steering_mode":
            self.mode = msg.get("mode")
            if self.mode == "all":
                for inp in self.held:
                    engine.queue(inp)
                self.held = []
            self.respond(rid, kind)
        elif kind == "get_state":
            self.respond(rid, kind, data=self.state())
        elif kind == "abort":
            # The run stops; its steering queue stays for the next one.
            self.respond(rid, kind)
        elif kind == "prompt":
            text = msg.get("message", "")
            inp = Input(marker_id(text), text, kind, {"rid": rid})
            engine.received(inp)
            if engine.turn is None:
                engine.open_turn(inp)
                self.tickets = [rid]
                frames = self.user_message(inp, steering=False)
                self.later(lambda: (self.respond(rid, kind), self.emit({"type": "agent_start"}),
                                    [self.emit(frame) for frame in frames]))
                return
            if msg.get("streamingBehavior") not in ("steer", "followUp"):
                self.later(lambda: self.respond(rid, kind, error=BUSY))
                return
            if self.take_refusal():
                self.later(lambda: self.respond(rid, kind, error=BUSY))
                return
            if self.mode == "all" or not engine.pending:
                engine.queue(inp)
            else:
                self.held.append(inp)
            self.tickets.append(rid)
            self.later(lambda: (self.respond(rid, kind), self.queue_update()))
        else:
            self.respond(rid, kind or "unknown", error="Unknown command: %s" % kind)

    def consumed(self, taken):
        for inp, _ in taken:
            for frame in self.user_message(inp, steering=True):
                self.emit(frame)
        if self.held:
            self.engine.queue(self.held.pop(0))
        if taken:
            self.queue_update()

    def finish(self):
        engine = self.engine
        # The stop boundary takes what is still queued before the run yields.
        while engine.pending:
            self.consumed(engine.step())
        tickets, self.tickets = self.tickets, []
        engine.end_turn()
        assistant = {"role": "assistant", "content": [{"type": "text", "text": "done"}],
                     "stopReason": "stop",
                     "usage": {"input": 3, "output": 2, "cacheRead": 1, "cacheWrite": 0,
                               "totalTokens": 6,
                               "cost": {"input": 0.000003, "output": 0.000002, "cacheRead": 0,
                                        "cacheWrite": 0, "total": 0.000005}}}
        self.messages += 1
        frame_id = "msg-%d" % self.messages
        self.emit({"type": "message_start", "message": assistant, "messageId": frame_id})
        self.emit({"type": "message_end", "message": assistant, "messageId": frame_id})
        self.emit({"type": "agent_end", "messages": [assistant]})
        for rid in tickets[1:] + tickets[:1]:
            self.emit({"type": "prompt_result", "id": rid, "agentInvoked": True,
                       "status": "completed", "sessionSettled": True})
        self.emit({"type": "session_settled"})


PROTOCOLS = {"omp": Omp}
