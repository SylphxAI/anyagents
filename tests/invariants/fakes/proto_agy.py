"""Antigravity `agy --input-format stream-json --output-format stream-json`
and its language server 1.5.8, as AnyAgents drives them.

What the real pair does, from static analysis of the installed binary and
live journals (marked "inferred" where neither shows it):

- The CLI reads one `{"type": "user", ...}` line per turn from stdin; a line
  that arrives while a turn runs waits for that turn to end and starts the
  next one (inferred). It prints `init` (no version), one `step_update` per
  step (`step_type` `user_input` for each input the cascade took) and
  `result` when the turn ends.
- The language server runs in the same process. `cli.log` (a link to this
  launch's `log/cli-*.log` under `$AGY_CONFIG_DIR/.gemini/antigravity-cli`)
  names its pid, its version and its random HTTP port. It serves Connect
  RPC, `POST /exa.language_server_pb.LanguageServerService/<Method>`, to a
  client that sends the `x-codeium-csrf-token` the CLI was started with
  (`--csrf_token`):
    GetAllCascadeTrajectories  every cascade and its run status
    GetCascadeTrajectoryGeneratorMetadata  the planner config a new
                invocation needs
    SendUserCascadeMessage  with `MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION`
                writes the message to the cascade's durable mailbox,
                `brain/<cid>/.system_generated/messages/<uuid>.json` (its
                text verbatim inside a base64 `CortexStep`), and answers
                `{}`. The running turn takes every waiting message, in send
                order, as `USER_INPUT` steps at its next step boundary, marks
                each in `messages/read.json` and records it in
                `logs/transcript_full.jsonl`. On an idle cascade it starts an
                invocation, which needs a planner config. A refusal ("plan
                model not specified; ...", "cascade not found: ...",
                "cannot send message: there is an ongoing battle") is a
                Connect error and queues nothing. Nothing is deduplicated.
    CancelCascadeInvocation  stops the running turn.
- A turn that ends with messages still waiting takes them at one more step
  first (inferred: the cascade reads its mailbox at every step boundary).
- A new process on the same conversation does not take what the last one
  left waiting (inferred).

Besides the engine's commands, this vendor acts once on each of these files:
  drop-request  the next SendUserCascadeMessage is read and dropped: the
                connection closes with no answer, and nothing is queued
  drop-answer   the next SendUserCascadeMessage is queued, then the
                connection closes with no answer
  fail-probe    the next GetAllCascadeTrajectories answers `unavailable`
and records every SendUserCascadeMessage as it arrives, before anything else
happens to it, in `rpc.jsonl` (`{"id", "launch"}`).
"""
import base64
import datetime
import json
import os
import pathlib
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from engine import Input, Protocol, marker_id

VERSION = "1.5.8"
SERVICE = "/exa.language_server_pb.LanguageServerService/"
PLANNER = {"planModel": "MODEL_PLACEHOLDER_M26",
           "requestedModel": {"model": "MODEL_PLACEHOLDER_M26"}}
# A request the server reads and leaves without an answer.
DROP = object()
STATUS = {"unknown": 500, "unavailable": 503, "not_found": 404, "permission_denied": 403,
          "invalid_argument": 400, "unimplemented": 501}


def varint(value):
    out = bytearray()
    while True:
        low, value = value & 0x7F, value >> 7
        out.append(low | (0x80 if value else 0))
        if not value:
            return bytes(out)


def cortex_step(text):
    """A `USER_INPUT` `CortexStep` (type 14) whose user input holds `text`."""
    data = text.encode()
    user_input = b"\x12" + varint(len(data)) + data
    return b"\x08\x0e\x9a\x01" + varint(len(user_input)) + user_input


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def connect_error(message, code="unknown"):
    """A Connect error: what a handler that returned a Go error answers."""
    return STATUS[code], {"code": code, "message": message}


def write_atomically(path, text):
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


class Rpc(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):
        pass

    def do_POST(self):
        agy = self.server.agy
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if not self.path.startswith(SERVICE):
            return self.answer(*connect_error("no such service", "not_found"))
        if self.headers.get("x-codeium-csrf-token") != agy.csrf:
            return self.answer(*connect_error("invalid CSRF token", "permission_denied"))
        try:
            request = json.loads(body or b"{}")
        except ValueError:
            return self.answer(*connect_error("malformed request", "invalid_argument"))
        reply = agy.call(self.path[len(SERVICE):], request)
        if reply is DROP:
            return
        status, payload, delay = reply
        if delay > 0:
            time.sleep(delay)
        self.answer(status, payload)

    def answer(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Agy(Protocol):
    idempotent = False

    def flag(self, name):
        return self.args[self.args.index(name) + 1] if name in self.args else None

    def start(self):
        self.csrf = self.flag("--csrf_token")
        self.session = self.flag("--conversation") or str(uuid.uuid4())
        # A conversation exists once a turn ran in it.
        self.exists = "--conversation" in self.args
        config = pathlib.Path(os.environ.get("AGY_CONFIG_DIR") or self.engine.root / "agy")
        self.home = config / ".gemini" / "antigravity-cli"
        self.generated = self.home / "brain" / self.session / ".system_generated"
        (self.generated / "messages").mkdir(parents=True, exist_ok=True)
        (self.generated / "logs").mkdir(parents=True, exist_ok=True)
        transcript = self.transcript()
        self.steps = len(transcript.read_text().splitlines()) if transcript.exists() else 0
        self.backlog = []
        self.announced = False
        server = ThreadingHTTPServer(("127.0.0.1", 0), Rpc)
        server.daemon_threads = True
        server.agy = self
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.write_log(server.server_address[1])

    def write_log(self, port):
        logs = self.home / "log"
        logs.mkdir(parents=True, exist_ok=True)
        path = logs / ("cli-%s-%d.log" % (time.strftime("%Y%m%d_%H%M%S"), self.engine.launch))
        stamp = time.strftime("I%m%d %H:%M:%S.000000")
        lines = [
            "Starting language server process with pid %d" % os.getpid(),
            "Language server version: %s" % VERSION,
            "Language server will attempt to listen on host localhost",
            "Language server listening on random port at %d for HTTPS (gRPC)" % (port + 1),
            "Language server listening on random port at %d for HTTP" % port,
        ]
        path.write_text("".join("%s %7d server.go:1] %s\n" % (stamp, 68, line) for line in lines))
        link = self.home / ("cli.log.%d" % os.getpid())
        link.symlink_to(path.relative_to(self.home))
        os.replace(link, self.home / "cli.log")

    def transcript(self):
        return self.generated / "logs" / "transcript_full.jsonl"

    def once(self, name):
        path = self.engine.root / name
        if path.exists():
            path.unlink()
            return True
        return False

    def delay(self):
        slow = self.engine.root / "slow"
        return int(slow.read_text() or "0") / 1000 if slow.exists() else 0

    # The conversation's durable records.

    def record(self, row):
        row.update({"step_index": self.steps, "created_at": now(), "status": "DONE"})
        with self.transcript().open("a") as out:
            out.write(json.dumps(row) + "\n")
        self.emit({"event": "step_update", "step_update": {
            "conversation_id": self.session, "step_index": self.steps, "state": "DONE",
            "step_type": row["type"].lower()}})
        self.steps += 1

    def post(self, inp):
        """Queue `inp` in the cascade's mailbox."""
        mail = str(uuid.uuid4())
        write_atomically(self.generated / "messages" / (mail + ".json"), json.dumps({
            "id": mail, "recipient": self.session, "sender": "user",
            "priority": "MESSAGE_PRIORITY_HIGH", "timestamp": now(),
            "stepPayload": base64.b64encode(cortex_step(inp.text)).decode(),
            "deliveryStrategy": "MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION"}))
        inp.meta["mail"] = mail

    def took(self, inp):
        """`inp` became a `USER_INPUT` step of the conversation."""
        mail = inp.meta.get("mail")
        if mail:
            path = self.generated / "messages" / "read.json"
            read = json.loads(path.read_text()) if path.exists() else {}
            read[mail] = True
            write_atomically(path, json.dumps(read))
        self.record({"type": "USER_INPUT", "source": "USER_EXPLICIT",
                     "content": "<USER_REQUEST>\n%s\n</USER_REQUEST>" % inp.text})

    def thought(self):
        """The model call that follows the step's inputs."""
        self.record({"type": "PLANNER_RESPONSE", "source": "MODEL"})

    # The CLI.

    def begin(self, inp):
        """A turn (an invocation of the cascade) that opens with `inp`."""
        self.exists = True
        self.engine.open_turn(inp)
        if not self.announced:
            self.announced = True
            self.emit({"event": "init", "session_id": self.session})
        self.took(inp)
        self.thought()

    def on_line(self, msg):
        if (msg.get("type") or msg.get("event")) != "user":
            return
        text = msg.get("text") or (msg.get("message") or {}).get("content") or ""
        inp = Input(marker_id(text), text, "user")
        self.engine.received(inp)
        if self.engine.turn is not None:
            self.backlog.append(inp)
            return
        self.later(lambda: self.begin(inp))

    def consumed(self, taken):
        for inp, _ in taken:
            self.took(inp)
        if taken:
            self.thought()

    def end(self, status):
        self.engine.end_turn()
        self.emit({"event": "result", "result": {
            "conversation_id": self.session, "status": status,
            "usage": {"input_tokens": 1, "output_tokens": 1}}})
        if self.backlog:
            self.begin(self.backlog.pop(0))

    def finish(self):
        if self.engine.pending:
            self.consumed(self.engine.step())
        self.end("SUCCESS")

    # The language server.

    def call(self, method, request):
        with self.engine.lock:
            if method == "GetAllCascadeTrajectories":
                if self.once("fail-probe"):
                    return connect_error("language server unavailable", "unavailable") + (0,)
                summaries = {}
                if self.exists:
                    running = self.engine.turn is not None
                    summaries[self.session] = {
                        "status": "CASCADE_RUN_STATUS_RUNNING" if running
                        else "CASCADE_RUN_STATUS_IDLE",
                        "stepCount": self.steps}
                return 200, {"trajectorySummaries": summaries}, 0
            if method == "GetCascadeTrajectoryGeneratorMetadata":
                if not self.exists or request.get("cascadeId") != self.session:
                    return connect_error("cascade not found: %s" % request.get("cascadeId")) + (0,)
                return 200, {"generatorMetadata": [{"plannerConfig": PLANNER}]}, 0
            if method == "SendUserCascadeMessage":
                return self.send_message(request)
            if method == "CancelCascadeInvocation":
                if self.engine.turn is not None and request.get("cascadeId") == self.session:
                    self.end("CANCELLED")
                return 200, {}, 0
            return connect_error("%s is not implemented" % method, "unimplemented") + (0,)

    def refuse(self, message):
        return connect_error(message) + (self.delay(),)

    def send_message(self, request):
        engine = self.engine
        text = "".join(item.get("text", "") for item in request.get("items") or [])
        inp = Input(marker_id(text), text, "SendUserCascadeMessage")
        engine.append("rpc.jsonl", {"id": inp.id, "launch": engine.launch})
        if self.once("drop-request"):
            return DROP
        engine.received(inp)
        cascade = request.get("cascadeId")
        if not self.exists or cascade != self.session:
            return self.refuse("cascade not found: %s" % cascade)
        if engine.turn is None:
            if not (request.get("cascadeConfig") or {}).get("plannerConfig"):
                return self.refuse("plan model not specified; you must specify a valid model")
        elif self.take_refusal():
            return self.refuse("cannot send message: there is an ongoing battle")
        self.post(inp)
        if engine.turn is None:
            self.begin(inp)
        else:
            engine.queue(inp)
        if self.once("drop-answer"):
            return DROP
        return 200, {}, self.delay()


PROTOCOLS = {"agy": Agy}
