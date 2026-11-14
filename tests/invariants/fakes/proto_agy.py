"""Antigravity `agy --input-format stream-json --output-format stream-json`
and its language server 1.5.8, as AnyAgents drives them.

What the real pair does, from static analysis of the installed binary and
live records (marked "inferred" where neither shows it):

- The CLI reads one `{"type": "user", ...}` line per turn from stdin; a line
  that arrives while a turn runs waits for that turn to end and starts the
  next one (inferred). It prints `init` (no version), one `step_update` per
  step (`step_type` `user_input` for each input the cascade took) and
  `result` when the turn ends, `status` `ERROR` with an `error` when its run
  failed.
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
                (its `items` the text, its `media` `{mimeType,
                inlineData}` the images the model is given with it)
                writes the message to the cascade's durable mailbox
                (`messages.(*FileStore).Save`):
                `brain/<cid>/.system_generated/messages/<uuid>.json`, its
                text verbatim inside a base64 `CortexStep`, and an empty
                `messages/undelivered/<uuid>` marker; then answers `{}`.
                That answer says the message is queued, never that the
                cascade took it: a run that fails never takes what waits
                (live: two messages a cascade's failing run left in
                `undelivered/`, unread, in no transcript). The cascade takes
                every waiting message, in send order, as `USER_INPUT` steps
                at a step boundary of its run (inferred), and at the end of a
                run that finds mail waiting it runs on (`MessageContinueCheck:
                undelivered messages found, continuing execution`). Each one
                it takes loses its undelivered marker (`MarkDelivered`), is
                marked in `messages/read.json` (`MarkRead`) and gets a
                `USER_INPUT` row in `logs/transcript_full.jsonl`. On an idle
                cascade it starts an invocation, which needs a planner config
                and takes every waiting message. A refusal ("plan model not
                specified; ...", "cascade not found: ...", "cannot send
                message: there is an ongoing battle") is a Connect error and
                queues nothing. Nothing is deduplicated.
    CancelCascadeInvocation  stops the running turn.
- A process started on a conversation whose mailbox still holds mail takes
  it at once, in one invocation of the resumed cascade (inferred from the
  changelog: 1.1.28 fixed a bug that "left queued when-idle messages
  undelivered until restart", 1.2.4 made "conversations with active turns
  ... auto-resume after a restart").
- A turn opened by a stdin line while mail waits takes the line first and the
  mail at its next step boundary (inferred; AnyAgents never opens one then).
- Records are written as the vendor writes them: compact transcript rows
  (`{"step_index":..,"source":..,"type":..,"status":..,"created_at":..}`),
  mailbox files with `", "` between fields, `read.json` compact.

Besides the engine's commands, this vendor acts once on each of these files:
  drop-request  the next SendUserCascadeMessage is read and dropped: the
                connection closes with no answer, and nothing is queued
  drop-answer   the next SendUserCascadeMessage is queued, then the
                connection closes with no answer
  fail-probe    the next GetAllCascadeTrajectories answers `unavailable`
  fail          the running turn's run fails: an `ERROR_MESSAGE` step, then
                `result` `ERROR`; the mail waiting for it stays in the
                mailbox, undelivered
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

from engine import Input, Protocol, image_b64, marker_id

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


def read_varint(data, at):
    value, shift = 0, 0
    while True:
        byte = data[at]
        at += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, at


def step_text(step):
    """The user input text of a `cortex_step`."""
    at = step.index(b"\x9a\x01") + 2
    _, at = read_varint(step, at)
    assert step[at] == 0x12
    length, at = read_varint(step, at + 1)
    return step[at:at + length].decode()


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def connect_error(message, code="unknown"):
    """A Connect error: what a handler that returned a Go error answers."""
    return STATUS[code], {"code": code, "message": message}


def compact(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


ERROR = "API error (attempt 3): request failed: stream idle timeout (5m0s): context deadline exceeded"


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
        (self.generated / "messages" / "undelivered").mkdir(exist_ok=True)
        transcript = self.transcript()
        self.steps = len(transcript.read_text().splitlines()) if transcript.exists() else 0
        self.backlog = []
        # Mail the cascade's mailbox holds that no running turn will take.
        self.waiting = []
        self.announced = False
        server = ThreadingHTTPServer(("127.0.0.1", 0), Rpc)
        server.daemon_threads = True
        server.agy = self
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.write_log(server.server_address[1])
        threading.Thread(target=self.watch, daemon=True).start()
        with self.engine.lock:
            held = self.held_mail()
            if held:
                self.later(lambda: self.invoke(held))

    def held_mail(self):
        """What the mailbox still holds, in send order."""
        messages = self.generated / "messages"
        held = []
        for marker in (messages / "undelivered").iterdir():
            mail = json.loads((messages / (marker.name + ".json")).read_text())
            text = step_text(base64.b64decode(mail["stepPayload"]))
            held.append((mail["timestamp"], Input(marker_id(text), text, "mailbox",
                                                  {"mail": marker.name})))
        return [inp for _, inp in sorted(held, key=lambda pair: pair[0])]

    def watch(self):
        """Act on `fail`."""
        path = self.engine.root / "fail"
        while True:
            with self.engine.lock:
                if path.exists():
                    if self.engine.turn is not None:
                        self.fail()
                    path.unlink()
            time.sleep(0.005)

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

    def record(self, source, kind, **fields):
        row = {"step_index": self.steps, "source": source, "type": kind, "status": "DONE",
               "created_at": now()}
        row.update(fields)
        with self.transcript().open("a") as out:
            out.write(compact(row) + "\n")
        self.emit({"event": "step_update", "step_update": {
            "conversation_id": self.session, "step_index": self.steps, "state": "DONE",
            "step_type": kind.lower()}})
        self.steps += 1

    def post(self, inp):
        """Queue `inp` in the cascade's mailbox."""
        mail = str(uuid.uuid4())
        messages = self.generated / "messages"
        write_atomically(messages / (mail + ".json"), json.dumps({
            "id": mail, "recipient": self.session, "sender": "user",
            "priority": "MESSAGE_PRIORITY_HIGH", "timestamp": now(),
            "stepPayload": base64.b64encode(cortex_step(inp.text)).decode(),
            "deliveryStrategy": "MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION"},
            separators=(", ", ":")))
        (messages / "undelivered" / mail).write_text("")
        inp.meta["mail"] = mail

    def took(self, inp):
        """`inp` became a `USER_INPUT` step of the conversation."""
        mail = inp.meta.get("mail")
        if mail:
            messages = self.generated / "messages"
            (messages / "undelivered" / mail).unlink(missing_ok=True)
            path = messages / "read.json"
            read = json.loads(path.read_text()) if path.exists() else {}
            read[mail] = True
            write_atomically(path, compact(read))
        self.record("USER_EXPLICIT", "USER_INPUT", content=(
            "<USER_REQUEST>\n%s\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\n"
            "The current local time is: %s.\n</ADDITIONAL_METADATA>" % (inp.text, now())))

    def thought(self):
        """The model call that follows the step's inputs."""
        self.record("MODEL", "PLANNER_RESPONSE")

    # The CLI.

    def announce(self):
        if not self.announced:
            self.announced = True
            self.emit({"event": "init", "session_id": self.session})

    def begin(self, inp):
        """A turn (an invocation of the cascade) that opens with `inp`; mail
        the mailbox holds waits for its next step boundary."""
        self.exists = True
        self.engine.open_turn(inp)
        self.announce()
        self.took(inp)
        self.thought()
        for waiting in self.waiting:
            self.engine.queue(waiting)
        self.waiting = []

    def invoke(self, mail):
        """An invocation of the idle cascade for the mail its mailbox holds:
        its first step takes all of it, in send order."""
        self.exists = True
        self.waiting = []
        self.engine.open_turn(mail[0])
        self.announce()
        self.took(mail[0])
        for inp in mail[1:]:
            self.engine.queue(inp)
        self.consumed(self.engine.step())
        if len(mail) == 1:
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

    def end(self, status, error=None):
        # What the turn never took stays in the mailbox.
        self.waiting.extend(self.engine.end_turn())
        result = {"conversation_id": self.session, "status": status,
                  "usage": {"input_tokens": 1, "output_tokens": 1}}
        if error:
            result["error"] = error
        self.emit({"event": "result", "result": result})
        if self.backlog:
            self.begin(self.backlog.pop(0))

    def fail(self):
        """The run fails: what waits for it is not taken."""
        self.record("SYSTEM", "ERROR_MESSAGE", error=ERROR)
        self.end("ERROR", ERROR)

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
        # The input's own media, which the model is given beside its text.
        media = [image_b64(item["inlineData"], item["mimeType"])
                 for item in request.get("media") or [] if item.get("inlineData")]
        inp = Input(marker_id(text), text, "SendUserCascadeMessage", media=media)
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
            self.invoke(self.waiting + [inp])
        else:
            engine.queue(inp)
        if self.once("drop-answer"):
            return DROP
        return 200, {}, self.delay()


PROTOCOLS = {"agy": Agy}
