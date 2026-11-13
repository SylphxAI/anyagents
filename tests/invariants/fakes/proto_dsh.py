"""`dsh --profile web --no-open --port 0` (DeepSeek Harness 0.1.5-rc.2).

dsh serves a web gateway and prints its address with a one-time launch token,
`dsh web: http://127.0.0.1:<port>/?token=<token>`. `GET /?token=<token>`
answers 303 with a `dsh-auth-<hash of the authority>` cookie; every API call
then needs that cookie, a loopback `Host`, an `Origin` (if any) on the same
host, and `content-type: application/json`, or it is answered 401, 403 or
415 with a plain body. A call is `POST /api/<method>` with the envelope
`{type: "client-request", rpcId, method, payload: {args: {<parameter>: ...}}}`
(the parameter is `request` for `session/create`, `session/prompt` and
`session/cancel`, `_request` for `session/list`), always answered 200 with
`{type: "server-response", rpcId, result: {ok: true, value} | {ok: false,
error: {code, message, details}}}`.

`session/prompt` (`{sessionId, requestId, content, mode}`) puts the message
in the session's durable `next-step` inbox with `mode: "steer"` (`next-turn`
with `"queue"`) and answers `{accepted: true}`. A `requestId` the session
holds already, pending or consumed (a `user/message` event whose source
carries it), is answered `{accepted: true}` and admitted nothing. A running
turn takes its whole `next-step` list at its next step, in order, and a
turn whose `next-step` list is not empty when a step ends runs another; an
idle agent starts a turn. `session/list` reports each session's `running`.
`dsh -V` prints the version.

The refusal the test asks for is `session/model-unavailable`, raised before
the inbox. Not modelled: the inbox a process left pending when it died (the
real one is durable), and the gateway's event stream.
"""
import base64
import hashlib
import http.server
import json
import os
import secrets
import sys
import time
import urllib.parse

from engine import Input, Protocol, marker_id

VERSION = "0.1.5-rc.2"
COOKIE_PREFIX = "dsh-auth-"
# The one parameter each endpoint takes, by its wire name.
PARAMETERS = {
    "session/create": "request",
    "session/prompt": "request",
    "session/cancel": "request",
    "session/list": "_request",
}


def vendor_args():
    """The arguments the adapter gave the vendor program."""
    args = sys.argv[1:]
    return [arg for i, arg in enumerate(args)
            if arg not in ("--fixture", "--protocol")
            and (i == 0 or args[i - 1] not in ("--fixture", "--protocol"))]


# `dsh -V` prints the version and starts nothing: it runs here, on import,
# before `vendor.py` starts an engine (and counts a launch) for it.
if vendor_args() in (["-V"], ["--version"]):
    sys.stdout.write(VERSION + "\n")
    sys.stdout.flush()
    os._exit(0)


def b64url(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def cookie_name(authority):
    return COOKIE_PREFIX + b64url(hashlib.sha256(authority.encode()).digest())


class RemoteError(Exception):
    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.details = details


class Dsh(Protocol):
    """`dsh` web: the gateway, its sessions, and their durable inboxes."""

    idempotent = True

    def __init__(self, engine, args):
        super().__init__(engine, args)
        self.token = secrets.token_urlsafe(24)
        self.secret = secrets.token_urlsafe(24)
        self.httpd = None

    # Sessions and the `user/message` events they recorded are durable.

    def store(self, name):
        return self.engine.root / ("dsh-" + name)

    def sessions(self):
        path = self.store("sessions.json")
        return json.loads(path.read_text()) if path.exists() else []

    def events(self):
        path = self.store("events.jsonl")
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def record(self, inp):
        """The session took `inp`: a `user/message` event carrying its rpcId."""
        with self.store("events.jsonl").open("a") as out:
            out.write(json.dumps({"type": "user/message", "session": inp.meta["session"],
                                  "data": {"source": {"kind": "user",
                                                      "rpcId": inp.meta["requestId"]}}}) + "\n")

    def has_prompt_request(self, session, request_id):
        if any(inp.meta.get("requestId") == request_id for inp in self.engine.pending):
            return True
        return any(event["session"] == session
                   and event["data"]["source"].get("rpcId") == request_id
                   for event in self.events())

    def start(self):
        protocol = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def plain(self, status, text, headers=()):
                data = text.encode()
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def authority(self):
                return self.headers.get("host")

            def authenticated(self):
                authority = self.authority()
                if authority is None:
                    return False
                wanted = cookie_name(authority)
                for segment in (self.headers.get("cookie") or "").split(";"):
                    name, _, value = segment.strip().partition("=")
                    if name == wanted:
                        return value == protocol.cookie_value(authority)
                return False

            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                tokens = urllib.parse.parse_qs(url.query).get("token", [])
                if url.path == "/" and tokens == [protocol.token] and self.authority():
                    authority = self.authority()
                    cookie = "%s=%s; Max-Age=2592000; Path=/; HttpOnly; SameSite=Strict" % (
                        cookie_name(authority), protocol.cookie_value(authority))
                    self.plain(303, "", [("location", "/"), ("set-cookie", cookie)])
                    return
                self.plain(401, "dsh web authentication required; "
                                "reopen the URL printed by dsh web.\n")

            def do_POST(self):
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length) if length else b""
                host = (self.authority() or "").rsplit(":", 1)[0]
                origin = self.headers.get("origin")
                if host not in ("127.0.0.1", "localhost") or (
                        origin is not None
                        and urllib.parse.urlsplit(origin).netloc != self.authority()):
                    self.plain(403, "forbidden")
                    return
                if not self.authenticated():
                    self.plain(401, "unauthorized")
                    return
                path = urllib.parse.urlsplit(self.path).path
                if not path.startswith("/api/"):
                    self.plain(404, "not found")
                    return
                content_type = (self.headers.get("content-type") or "").split(";")[0].strip()
                if content_type.lower() != "application/json":
                    self.plain(415, "content type must be application/json")
                    return
                try:
                    body = json.loads(raw)
                except ValueError:
                    self.plain(400, "body is not JSON")
                    return
                status, answer = 200, protocol.envelope(path[len("/api/"):], body)
                data = json.dumps(answer).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        port = self.httpd.server_address[1]
        sys.stdout.write("dsh web: http://127.0.0.1:%d/?token=%s\n" % (port, self.token))
        sys.stdout.flush()

    def serve(self):
        self.httpd.serve_forever()

    def cookie_value(self, authority):
        payload = b64url(json.dumps({"version": 1, "authority": authority}).encode())
        signature = b64url(hashlib.sha256((self.secret + payload).encode()).digest())
        return "v1.%s.%s" % (payload, signature)

    def envelope(self, endpoint, body):
        rpc_id = body.get("rpcId") if isinstance(body, dict) else None
        if not (isinstance(body, dict) and body.get("type") == "client-request"
                and isinstance(rpc_id, str) and isinstance(body.get("method"), str)
                and "payload" in body):
            return self.response(rpc_id if isinstance(rpc_id, str) else "invalid-request",
                                 error=RemoteError("gateway/bad-request",
                                                   "invalid client-request message", issues=[]))
        if body["method"] != endpoint:
            return self.response(rpc_id, error=RemoteError(
                "gateway/bad-request",
                "method %s does not match endpoint %s" % (json.dumps(body["method"]),
                                                          json.dumps(endpoint)), issues=[]))
        try:
            value = self.dispatch(endpoint, rpc_id, body["payload"])
        except RemoteError as err:
            return self.response(rpc_id, error=err)
        return self.response(rpc_id, value=value)

    @staticmethod
    def response(rpc_id, value=None, error=None):
        if error is not None:
            result = {"ok": False, "error": {"code": error.code, "message": str(error),
                                             "details": error.details}}
        else:
            result = {"ok": True} if value is None else {"ok": True, "value": value}
        return {"type": "server-response", "rpcId": rpc_id, "result": result}

    def dispatch(self, endpoint, rpc_id, payload):
        wire = PARAMETERS.get(endpoint)
        if wire is None:
            raise RemoteError("gateway/invocation-unavailable",
                              "no active Remote method exports this endpoint")
        args = payload.get("args") if isinstance(payload, dict) else None
        if not isinstance(args, dict) or set(args) != {wire} or not isinstance(args[wire], dict):
            raise RemoteError("gateway/arguments-invalid",
                              "args fields do not match the descriptor: expected %s"
                              % json.dumps(wire))
        request = args[wire]
        engine = self.engine
        if endpoint == "session/create":
            with engine.lock:
                session = request.get("sessionId") or "session-" + secrets.token_hex(16)
                if session not in self.sessions():
                    self.store("sessions.json").write_text(json.dumps(self.sessions() + [session]))
                self.session = session
            return {"sessionId": session}
        if endpoint == "session/list":
            with engine.lock:
                return {"items": [{"sessionId": session, "updatedAt": 0, "blank": False,
                                   "running": engine.turn is not None and session == self.session}
                                  for session in self.sessions()],
                        "hasMore": False}
        if endpoint == "session/cancel":
            with engine.lock:
                self.resolve(request.get("sessionId"))
                if engine.turn is not None:
                    engine.end_turn()
            return None
        return self.prompt(request)

    def resolve(self, session):
        if session not in self.sessions():
            raise RemoteError("session/not-found", 'session "%s" not found' % session,
                              sessionId=session)

    def prompt(self, request):
        content = request.get("content") or []
        text = "".join(part.get("text", "") for part in content if part.get("type") == "text")
        if not text.strip():
            raise RemoteError("gateway/bad-request",
                              "prompt content must include non-whitespace text or an attachment")
        session, request_id = request.get("sessionId"), request.get("requestId")
        engine = self.engine
        with engine.lock:
            self.resolve(session)
            inp = Input(marker_id(text) or request_id, text,
                        "prompt" if engine.turn is None else "steer",
                        {"session": session, "requestId": request_id})
            engine.received(inp)
            if not self.has_prompt_request(session, request_id):
                if engine.turn is not None and self.take_refusal():
                    raise RemoteError("session/model-unavailable",
                                      'no adapter serves provider "deepseek-official"; '
                                      "select a model for this session",
                                      provider="deepseek-official", model="deepseek-chat")
                self.session = session
                if engine.turn is None:
                    if engine.open_turn(inp):
                        self.record(inp)
                else:
                    engine.queue(inp)
        # The message is handled; only the answer waits out the `slow` delay.
        slow = engine.root / "slow"
        delay = int(slow.read_text() or "0") / 1000 if slow.exists() else 0
        if delay > 0:
            time.sleep(delay)
        return {"accepted": True}

    def consumed(self, taken):
        for inp, fresh in taken:
            if fresh:
                self.record(inp)

    def finish(self):
        engine = self.engine
        # A step that ends with `next-step` input pending runs another.
        if engine.pending:
            self.consumed(engine.step())
        engine.end_turn()


PROTOCOLS = {"dsh": Dsh}
