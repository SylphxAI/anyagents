"""`opencode serve` (OpenCode 2.1.4) and its one-shot `opencode api` client.

The service speaks HTTP, not lines: `serve --hostname H --port P` prints
`server listening on http://H:P` and `server password <password>`, then
answers requests until it is stopped. `api --server URL METHOD PATH [-d
BODY]` makes one request against it and prints the answer's body; an answer
that is not 2xx also prints `HTTP <status> <text>` on stderr and exits 1. An
error's body is `{"_tag": <error>, "message": ...}`.

`POST /api/session/{id}/prompt` (`{id?, text, files?, delivery}`) admits one input
to the session's durable inbox and answers with that inbox record. Given an
`id` (it must start with `msg_`, or the payload does not decode: 400) the
session already holds, as an inbox record or as a message it consumed, it
answers with that record, created when it was first admitted, and admits
nothing (`SessionInbox.reconcile`); an id another session holds is a 409
`ConflictError` (`Session.PromptConflictError`), for good. A steer joins the running
turn at its next step, every pending steer together, in admission order, and
a turn with steers pending at its end runs another step for them. An idle
session opens a turn with the input. Consumption is announced only on the
event stream (`session.inbox.delivered`), which this fake does not serve:
`GET /api/session/{id}/message/{messageID}` finds a consumed input under its
own id. `POST /api/experimental/session/{id}/wait` answers 204 once the
session is idle.

A `files` entry `{"uri": "file://<path>"}` is read by the service and given
to the model beside the text; one it cannot read or decode refuses the whole
prompt with a 400 `AttachmentError` and admits nothing.

The refusal the test asks for is the service's readiness gate: while it is
(re)starting, every route but `GET /api/info` answers 503 `{"code":
"service_starting"}` with `retry-after: 1`, after the password check and
before any route, so nothing is admitted. OpenCode declares no other
transient refusal for a prompt: its 400 and 409 are permanent. Not modelled:
the inbox records a process left pending when it died (the real inbox keeps
them; here they go with it), and the event stream.
"""
import base64
import http.client
import http.server
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse

from engine import Input, Protocol, image_file, marker_id


def sniff(path):
    """The image type of a file, from its bytes, as OpenCode reads it."""
    with open(path, "rb") as source:
        head = source.read(12)
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    return None

VERSION = "2.1.4"


def now_ms():
    return int(time.time() * 1000)


def vendor_args():
    """The arguments the adapter gave the vendor program."""
    args = sys.argv[1:]
    return [arg for i, arg in enumerate(args)
            if arg not in ("--fixture", "--protocol")
            and (i == 0 or args[i - 1] not in ("--fixture", "--protocol"))]


def authorization(password):
    token = base64.b64encode(("opencode:" + password).encode()).decode()
    return "Basic " + token


def api(args):
    """`opencode api --server URL METHOD PATH [-d BODY]`: one request."""
    server = args[args.index("--server") + 1]
    body = args[args.index("-d") + 1] if "-d" in args else None
    rest = [arg for i, arg in enumerate(args)
            if arg not in ("--server", "-d") and (i == 0 or args[i - 1] not in ("--server", "-d"))]
    method, path = rest[0], rest[1]
    url = urllib.parse.urlsplit(server)
    headers = {"authorization": authorization(os.environ.get("OPENCODE_PASSWORD", ""))}
    if body is not None:
        headers["content-type"] = "application/json"
    try:
        connection = http.client.HTTPConnection(url.hostname, url.port)
        connection.request(method, path, body=None if body is None else body.encode(),
                           headers=headers)
        answer = connection.getresponse()
        text = answer.read().decode()
    except OSError as err:
        sys.stderr.write("error: Unable to connect. %s\n" % err)
        return 1
    if text:
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    sys.stdout.flush()
    if not 200 <= answer.status < 300:
        sys.stderr.write("HTTP %d %s\n" % (answer.status, answer.reason))
        return 1
    return 0


# `opencode api` is a client of a running service, not a vendor process: it
# runs here, on import, before `vendor.py` starts an engine (and counts a
# launch) for it.
if vendor_args()[:1] == ["api"]:
    sys.stdout.flush()
    os._exit(api(vendor_args()[1:]))


class Failure(Exception):
    def __init__(self, status, tag, message, **fields):
        super().__init__(message)
        self.status = status
        self.body = dict({"_tag": tag, "message": message}, **fields)
        self.headers = {}


class NotReady(Failure):
    """The readiness gate's answer, which carries no `_tag` and no message."""

    def __init__(self):
        super().__init__(503, None, "service_starting")
        self.body = {"code": "service_starting"}
        self.headers = {"retry-after": "1"}


def session_not_found(session):
    return Failure(404, "SessionNotFoundError", "Session not found: %s" % session,
                   sessionID=session)


class OpenCode(Protocol):
    """`opencode serve`: one session store, its inbox, and the HTTP API."""

    idempotent = True

    def __init__(self, engine, args):
        super().__init__(engine, args)
        self.password = secrets.token_hex(16)
        self.idle = threading.Condition(engine.lock)
        # Pending steers, by their id: the inbox records not yet consumed.
        self.inbox = {}
        self.httpd = None

    # The session store and the messages the sessions consumed are durable.

    def store(self, name):
        return self.engine.root / ("opencode-" + name)

    def sessions(self):
        path = self.store("sessions.json")
        return json.loads(path.read_text()) if path.exists() else []

    def create_session(self):
        session = "ses_" + secrets.token_hex(12)
        self.store("sessions.json").write_text(json.dumps(self.sessions() + [session]))
        return session

    def messages(self):
        path = self.store("messages.jsonl")
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def message(self, ident):
        return next((row for row in self.messages() if row["id"] == ident), None)

    def consume(self, inp):
        """The session took `inp` into its conversation, under its own id."""
        self.inbox.pop(inp.meta["vid"], None)
        with self.store("messages.jsonl").open("a") as out:
            out.write(json.dumps({
                "id": inp.meta["vid"], "sessionID": inp.meta["session"], "type": "user",
                "text": inp.text, "time": {"created": inp.meta["created"]},
            }) + "\n")

    def start(self):
        args = self.args
        host = args[args.index("--hostname") + 1] if "--hostname" in args else "127.0.0.1"
        port = int(args[args.index("--port") + 1])
        protocol = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def answer(self, status, body=None, headers=None):
                data = b"" if body is None else json.dumps(body).encode()
                self.send_response(status)
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                if body is not None:
                    self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_one(self, method):
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length) if length else b""
                if self.headers.get("authorization") != authorization(protocol.password):
                    self.answer(401, {"_tag": "UnauthorizedError", "message": "Unauthorized"})
                    return
                headers = None
                try:
                    status, body = protocol.route(method, self.path, raw)
                except Failure as failure:
                    status, body, headers = failure.status, failure.body, failure.headers
                self.answer(status, body, headers)

            def do_GET(self):
                self.handle_one("GET")

            def do_POST(self):
                self.handle_one("POST")

        self.httpd = http.server.ThreadingHTTPServer((host, port), Handler)
        self.httpd.daemon_threads = True
        sys.stdout.write("server listening on http://%s:%d\n" % (host, port))
        sys.stdout.write("server password %s\n" % self.password)
        sys.stdout.flush()

    def serve(self):
        self.httpd.serve_forever()

    def route(self, method, path, raw):
        path = urllib.parse.urlsplit(path).path
        parts = path.strip("/").split("/")
        if (method, path) == ("GET", "/api/info"):
            return 200, {"version": VERSION, "pid": os.getpid(),
                         "urls": ["http://%s:%d" % self.httpd.server_address],
                         "paths": {"tmp": "/tmp"}, "capabilities": {}}
        if (method, path) == ("GET", "/api/mcp"):
            return 200, {"data": [{"name": "anyagents", "status": {"status": "connected"}}]}
        if (method, path) == ("GET", "/api/session"):
            return 200, {"data": [{"id": session} for session in self.sessions()],
                         "cursor": {}}
        if (method, path) == ("POST", "/api/session"):
            with self.engine.lock:
                return 200, {"data": {"id": self.create_session()}}
        if (method, path) == ("GET", "/api/session/active"):
            with self.engine.lock:
                running = self.engine.turn is not None
                return 200, {"data": {self.session: {"type": "running"}} if running else {}}
        if parts[:2] == ["api", "session"] and len(parts) >= 3:
            session = parts[2]
            if session not in self.sessions():
                raise session_not_found(session)
            rest = parts[3:]
            if method == "GET" and rest == []:
                return 200, {"data": {"id": session, "title": "conformance",
                                      "tokens": {"input": 0, "output": 0}, "cost": 0}}
            if method == "POST" and rest == ["prompt"]:
                return self.prompt(session, json.loads(raw or b"{}"))
            if method == "GET" and rest == ["message"]:
                rows = [row for row in self.messages() if row["sessionID"] == session]
                return 200, {"data": list(reversed(rows))}
            if method == "GET" and len(rest) == 2 and rest[0] == "message":
                row = self.message(rest[1])
                if row is None or row["sessionID"] != session:
                    raise Failure(404, "MessageNotFoundError", "Message not found: %s" % rest[1],
                                  sessionID=session, messageID=rest[1])
                return 200, {"data": row}
            if method == "POST" and rest == ["interrupt"]:
                with self.engine.lock:
                    if self.engine.turn is not None:
                        self.finish(consume=False)
                return 204, None
        if parts[:3] == ["api", "experimental", "session"] and parts[4:] == ["wait"]:
            session = parts[3]
            if session not in self.sessions():
                raise session_not_found(session)
            with self.idle:
                while self.engine.turn is not None:
                    self.idle.wait()
            return 204, None
        return 404, None

    def prompt(self, session, body):
        ident = body.get("id")
        if ident is not None and not (isinstance(ident, str) and ident.startswith("msg_")):
            raise Failure(400, "HttpApiDecodeError", 'Expected a string starting with "msg_"')
        text = body.get("text") or ""
        delivery = body.get("delivery") or "steer"
        engine = self.engine
        with engine.lock:
            images = []
            for attached in body.get("files") or []:
                uri = attached.get("uri") or ""
                path = urllib.parse.unquote(uri[len("file://"):]) if uri.startswith("file://") \
                    else None
                mime = sniff(path) if path and os.path.isfile(path) else None
                if mime is None:
                    # An attachment it cannot read or decode: the whole
                    # prompt is refused, and nothing is admitted.
                    raise Failure(400, "AttachmentError", "Cannot read attachment %s" % uri)
                images.append(image_file(path, mime))
            vid = ident or "msg_" + secrets.token_hex(13)
            inp = Input(marker_id(text) or vid, text, "prompt" if engine.turn is None else "steer",
                        {"vid": vid, "session": session, "created": now_ms()}, images)
            engine.received(inp)
            if engine.turn is not None and self.take_refusal():
                # The gate answers before the route reads the inbox.
                raise NotReady()
            record = self.reconcile(vid, session)
            if record is None:
                self.session = session
                record = self.record(inp, delivery)
                if engine.turn is None:
                    if engine.open_turn(inp):
                        self.consume(inp)
                else:
                    self.inbox[vid] = record
                    engine.queue(inp)
        # The input is handled; only the answer waits out the `slow` delay.
        slow = engine.root / "slow"
        delay = int(slow.read_text() or "0") / 1000 if slow.exists() else 0
        if delay > 0:
            time.sleep(delay)
        return 200, {"data": record}

    def reconcile(self, vid, session):
        """The record this session already holds under `vid`, if any."""
        held = self.inbox.get(vid)
        if held is None:
            row = self.message(vid)
            if row is None:
                return None
            held = {"id": vid, "sessionID": row["sessionID"], "time": row["time"],
                    "type": "user", "payload": {"text": row["text"]}, "delivery": "steer"}
        if held["sessionID"] != session:
            raise Failure(409, "ConflictError",
                          "Prompt message ID conflicts with an existing durable record: %s" % vid,
                          resource=vid)
        return held

    @staticmethod
    def record(inp, delivery):
        return {"id": inp.meta["vid"], "sessionID": inp.meta["session"],
                "time": {"created": inp.meta["created"]}, "type": "user",
                "payload": {"text": inp.text}, "delivery": delivery}

    def consumed(self, taken):
        for inp, fresh in taken:
            if fresh:
                self.consume(inp)
            else:
                self.inbox.pop(inp.meta["vid"], None)

    def finish(self, consume=True):
        engine = self.engine
        # Steers pending when a step ends keep the turn going for one more.
        if consume and engine.pending:
            self.consumed(engine.step())
        engine.end_turn()
        self.inbox.clear()
        self.idle.notify_all()


PROTOCOLS = {"opencode": OpenCode}
