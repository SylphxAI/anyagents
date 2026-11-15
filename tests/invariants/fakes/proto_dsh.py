"""`dsh --profile web --no-open --port 0` (DeepSeek Harness 0.1.5-rc.2).

Read from the package's own source (`@deepseek-ai/dsh` 0.1.5-rc.2; workspace
packages `dsh-client-connection`, `dsh-api-gateway`,
`dsh-api-session-controller`, `dsh-agent-loop`, `dsh-session`,
`dsh-session-persistence-jsonl`, and `dsh-acp` 0.1.5-rc.3; earlier gateway
notes read 0.2.2-rc.1) and verified against a live loopback gateway:

dsh serves a web gateway and prints its address with its process launch
token, `dsh web: http://127.0.0.1:<port>/?token=<token>`. `GET /?token=<token>`
answers 303 with a `dsh-auth-<hash of the authority>` cookie; every API call
and WebSocket upgrade then needs that cookie, a loopback `Host`, and an
`Origin` (if any) on the same authority, or it is answered 401 or 403 with a
plain body (`POST /api/*` without `content-type: application/json` is 415).
A unary call is `POST /api/<method>` with the envelope `{type:
"client-request", rpcId, method, payload: {args: {<parameter>: ...}}}` (the
parameter is `request` for `session/create`, `session/prompt`,
`session/cancel`, `session/updateQueue`, and `session/page`, `_request` for
`session/list`), answered 200 with `{type: "server-response", rpcId, result:
{ok: true, value} | {ok: false, error: {code, message, details}}}`. Stream
methods reject `POST` (`gateway/signature-invalid`) and run over WebSocket
`GET /api/remote.mux`: `{type: "open", streamId, endpoint, payload}` or
`{type: "cancel", streamId}` in, `{type: "item" | "end" | "error", streamId,
...}` out. The gateway names no version.

`session/create` (`{sessionId, cwd}`) calls `ensureSession` ->
`createOrAdopt`: returns the live agent if already attached, resumes the
persisted session with that `sessionId` if on disk (appending `turn/end` with
`reason: {kind: "interrupted"}` when an earlier process left a turn open, in
`idle` phase without waking for pending inbox items), or creates a fresh
session under `sessionId`, answering `{sessionId}`. (0.2.2-rc.1 minted a fresh
id when `session/create` named a non-blank session while leaving the named
session intact.)

`session/follow` (`{address: {kind: "session", sessionId}, maxMessages?}`)
yields one `snapshot` item `{type: "snapshot", header, cursor, records,
hasMore, projections: {asOfSeq: cursor, values: {inbox: {"next-turn": [...],
"next-step": [...]}}}}` (`cursor` is `-1` on an empty log) and then gap-free
`{type: "event", event}` items starting at `cursor + 1`. `session/page`
(`{address, throughSeq, beforeSeq?, maxMessages?}`) slices events in
`[cut, min(throughSeq + 1, beforeSeq))` with `hasMore = cut > 0`.

`session/prompt` (`{sessionId, requestId, content, mode}`) resolves the
session (`session/not-found` if absent), checks `hasPromptRequest` against the
live `next-turn`/`next-step` inbox and `user/message` events in the session
log (answering `{accepted: true}` without waking or splicing when found), and
otherwise splices the message into `next-step` (`mode: "steer"`) or
`next-turn` (`mode: "queue"`) via `agent/inbox/spliced` and answers
`{accepted: true}`. An idle agent starts a turn (`turn/start`), claims the
inbox (`agent/inbox/spliced`), and appends `user/message` carrying `source:
{kind: "user", rpcId: requestId}` before building the model request; a busy
turn claims the whole `next-step` list at its next step in order and appends
each `user/message` before the model call, running another step if `next-step`
is non-empty at step end. `session/cancel` aborts the active turn with
`keepInbox: true` (`turn/end` `reason: {kind: "aborted"}`), leaving unconsumed
steers in `next-step`; `session/updateQueue` (`{sessionId, itemId, action:
{kind: "remove"}}`) removes a pending item via `agent/inbox/spliced` (or
raises `session/queue-item-not-found`).

`SessionImpl.append` synchronously notifies `session/follow` before
`dsh-session-persistence-jsonl` flushes its 200 ms live write batch
(`stores_after_proof = True`); `SIGTERM` clears the pending inbox and drains
live writes, whereas here every appended event is written synchronously so
`step-N` and `crash` `os._exit(1)` seams also exercise cold-resumed inbox
cleanup. The refusal `refuse` triggers is `session/not-found` before inbox
admission.
"""
import base64
import hashlib
import http.server
import json
import os
import secrets
import struct
import sys
import threading
import time
import urllib.parse

from engine import Input, Protocol, image_b64, marker_id

COOKIE_PREFIX = "dsh-auth-"
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
SURFACE_MESSAGE_TYPES = frozenset({
    "user/message",
    "assistant/message",
    "assistant/tool-call",
    "tool/result",
})
# The one parameter each unary endpoint takes, by its wire name.
PARAMETERS = {
    "session/create": "request",
    "session/prompt": "request",
    "session/cancel": "request",
    "session/updateQueue": "request",
    "session/page": "request",
    "session/list": "_request",
}


def b64url(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def cookie_name(authority):
    return COOKIE_PREFIX + b64url(hashlib.sha256(authority.encode()).digest())


def images_of(content):
    media = []
    for part in content or []:
        if not isinstance(part, dict) or part.get("type") != "image":
            continue
        if isinstance(part.get("data"), str):
            media.append(image_b64(part["data"], part.get("mediaType", "image/png")))
        elif isinstance(part.get("attachment"), dict):
            att = part["attachment"]
            raw = json.dumps(att, sort_keys=True).encode()
            media.append(image_b64(base64.b64encode(raw).decode(),
                                   att.get("mediaType", "image/png")))
    return media


def ws_frame(opcode, payload):
    head = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        head.append(n)
    elif n <= 0xFFFF:
        head.append(126)
        head.extend(struct.pack("!H", n))
    else:
        head.append(127)
        head.extend(struct.pack("!Q", n))
    return bytes(head) + payload


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def ws_recv_message(sock):
    """Read one client WebSocket message, answering pings through caller."""
    fragments = bytearray()
    while True:
        hdr = recv_exact(sock, 2)
        if hdr is None:
            return None
        fin = (hdr[0] & 0x80) != 0
        opcode = hdr[0] & 0x0F
        masked = (hdr[1] & 0x80) != 0
        length = hdr[1] & 0x7F
        if length == 126:
            ext = recv_exact(sock, 2)
            if ext is None:
                return None
            length = struct.unpack("!H", ext)[0]
        elif length == 127:
            ext = recv_exact(sock, 8)
            if ext is None:
                return None
            length = struct.unpack("!Q", ext)[0]
        mask = recv_exact(sock, 4) if masked else None
        if masked and mask is None:
            return None
        payload = recv_exact(sock, length) if length else b""
        if payload is None:
            return None
        if mask is not None:
            payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        if opcode == 0x8:
            return ("close", payload)
        if opcode == 0x9:
            return ("ping", payload)
        if opcode == 0xA:
            continue
        if opcode in (0x0, 0x1, 0x2):
            fragments.extend(payload)
            if fin:
                return ("text", bytes(fragments).decode("utf-8"))


class MuxPeer:
    def __init__(self, sock):
        self.sock = sock
        self.lock = threading.Lock()
        self.closed = False

    def send(self, opcode, payload):
        with self.lock:
            if self.closed:
                return False
            try:
                self.sock.sendall(ws_frame(opcode, payload))
                return True
            except OSError:
                self.closed = True
                return False

    def send_json(self, obj):
        return self.send(0x1, json.dumps(obj).encode("utf-8"))


class RemoteError(Exception):
    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.details = details


class Dsh(Protocol):
    """`dsh` web: the gateway, its sessions, their follow streams and inboxes."""

    idempotent = False
    stores_after_proof = True

    def __init__(self, engine, args):
        super().__init__(engine, args)
        self.token = secrets.token_urlsafe(24)
        self.secret = secrets.token_urlsafe(24)
        self.httpd = None
        self.attached = set()
        # (session_id, stream_id, MuxPeer)
        self.followers = []
        self.known_sessions = self.load_sessions()
        self.logs = {session: self.load_log(session) for session in self.known_sessions}

    def store(self, name):
        return self.engine.root / ("dsh-" + name)

    def log_path(self, session):
        digest = hashlib.sha256(session.encode("utf-8")).hexdigest()[:24]
        return self.store("session-%s.jsonl" % digest)

    def load_sessions(self):
        path = self.store("sessions.json")
        return json.loads(path.read_text()) if path.exists() else []

    def save_sessions(self):
        self.store("sessions.json").write_text(json.dumps(self.known_sessions))

    def load_log(self, session):
        path = self.log_path(session)
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def append_event(self, session, event_type, data, surface=False):
        events = self.logs.setdefault(session, [])
        event = {
            "type": event_type,
            "seq": len(events),
            "time": int(time.time() * 1000),
            "data": data,
        }
        if surface:
            event["surfaceOp"] = "append"
        events.append(event)
        with self.log_path(session).open("a") as out:
            out.write(json.dumps(event) + "\n")
        dead = []
        for sub in list(self.followers):
            sub_session, stream_id, peer = sub
            if sub_session == session:
                ok = peer.send_json({
                    "type": "item",
                    "streamId": stream_id,
                    "value": {"type": "event", "event": event},
                })
                if not ok:
                    dead.append(sub)
        for sub in dead:
            if sub in self.followers:
                self.followers.remove(sub)
        return event

    def inbox_of(self, session):
        inbox = {"next-turn": [], "next-step": []}
        for event in self.logs.get(session, ()):
            if event["type"] == "agent/inbox/spliced":
                data = event["data"]
                target = data["target"]
                start = data["start"]
                removed = data.get("removedCount", 0)
                inbox[target][start:start + removed] = list(data["inserted"])
        return inbox

    def open_turn_num(self, session):
        open_turn = None
        for event in self.logs.get(session, ()):
            if event["type"] == "turn/start":
                open_turn = event["data"].get("turn", 1)
            elif event["type"] == "turn/end":
                open_turn = None
        return open_turn

    def next_turn_num(self, session):
        last = 0
        for event in self.logs.get(session, ()):
            if event["type"] in ("turn/start", "turn/end"):
                last = max(last, int(event["data"].get("turn", 0)))
        return last + 1

    def has_prompt_request(self, session, request_id):
        inbox = self.inbox_of(session)
        for queue in (inbox["next-turn"], inbox["next-step"]):
            for msg in queue:
                src = msg.get("source") or {}
                if src.get("kind") == "user" and src.get("rpcId") == request_id:
                    return True
        for event in self.logs.get(session, ()):
            if event["type"] == "user/message":
                src = (event["data"].get("source") or {})
                if src.get("kind") == "user" and src.get("rpcId") == request_id:
                    return True
        return False

    def paginate(self, events, before_seq, max_messages, through_seq):
        if through_seq < 0:
            return [], False
        end = min(through_seq + 1, through_seq + 1 if before_seq is None else before_seq)
        end = max(0, min(end, len(events)))
        count = 0
        cut = 0
        for idx in range(end - 1, -1, -1):
            ev = events[idx]
            if ev["type"] not in SURFACE_MESSAGE_TYPES or ev.get("surfaceOp") != "append":
                continue
            count += 1
            if max_messages is not None and count >= max_messages:
                cut = ev["seq"]
                break
        return events[cut:end], cut > 0

    def start(self):
        protocol = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def plain(self, status, text, headers=()):
                data = text.encode()
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.send_header("content-length", str(len(data)))
                self.send_header("connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.close_connection = True

            def authority(self):
                return self.headers.get("host")

            def allowed_origin(self):
                host = (self.authority() or "").rsplit(":", 1)[0]
                origin = self.headers.get("origin")
                return host in ("127.0.0.1", "localhost") and (
                    origin is None
                    or urllib.parse.urlsplit(origin).netloc == self.authority()
                )

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
                if url.path == "/api/remote.mux":
                    if not self.allowed_origin():
                        self.plain(403, "forbidden")
                        return
                    if not self.authenticated():
                        self.plain(401, "unauthorized")
                        return
                    key = None
                    for hdr_name, hdr_val in self.headers.items():
                        if hdr_name.lower() == "-".join(("sec", "websocket", "key")):
                            key = hdr_val.strip()
                            break
                    if (self.headers.get("upgrade") or "").lower() != "websocket" or not key:
                        self.plain(400, "websocket upgrade required")
                        return
                    accept = base64.b64encode(
                        hashlib.sha1((key + WS_GUID).encode("ascii")).digest()
                    ).decode("ascii")
                    self.send_response(101, "Switching Protocols")
                    self.send_header("Upgrade", "websocket")
                    self.send_header("Connection", "Upgrade")
                    self.send_header("-".join(("Sec", "WebSocket", "Accept")), accept)
                    self.end_headers()
                    self.wfile.flush()
                    self.close_connection = True
                    protocol.serve_mux(self.connection)
                    return
                self.plain(401, "dsh web authentication required; "
                                "reopen the URL printed by dsh web.\n")

            def do_POST(self):
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length) if length else b""
                if not self.allowed_origin():
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
                self.send_header("connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.close_connection = True

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        port = self.httpd.server_address[1]
        sys.stdout.write("dsh web: http://127.0.0.1:%d/?token=%s\n" % (port, self.token))
        sys.stdout.flush()

    def serve(self):
        self.httpd.serve_forever()

    def serve_mux(self, sock):
        peer = MuxPeer(sock)
        try:
            while True:
                item = ws_recv_message(sock)
                if item is None:
                    break
                kind, payload = item
                if kind == "close":
                    peer.send(0x8, b"")
                    break
                if kind == "ping":
                    peer.send(0xA, payload)
                    continue
                try:
                    frame = json.loads(payload)
                except ValueError:
                    break
                ftype = frame.get("type")
                stream_id = frame.get("streamId")
                if not isinstance(stream_id, str):
                    continue
                if ftype == "cancel":
                    with self.engine.lock:
                        self.followers = [s for s in self.followers
                                          if not (s[1] == stream_id and s[2] is peer)]
                    peer.send_json({"type": "end", "streamId": stream_id})
                    continue
                if ftype == "open":
                    self.open_stream(peer, stream_id, frame.get("endpoint"), frame.get("payload"))
        except OSError:
            pass
        finally:
            peer.closed = True
            with self.engine.lock:
                self.followers = [s for s in self.followers if s[2] is not peer]

    def open_stream(self, peer, stream_id, endpoint, payload):
        if endpoint != "session/follow":
            peer.send_json({
                "type": "error",
                "streamId": stream_id,
                "error": {"code": "gateway/invocation-unavailable",
                          "message": "unknown stream endpoint", "details": {}},
            })
            return
        args = payload.get("args") if isinstance(payload, dict) else None
        req = args.get("request") if isinstance(args, dict) else None
        addr = req.get("address") if isinstance(req, dict) else None
        session = addr.get("sessionId") if isinstance(addr, dict) and addr.get("kind") == "session" else None
        if not isinstance(session, str) or not session:
            peer.send_json({
                "type": "error",
                "streamId": stream_id,
                "error": {"code": "gateway/arguments-invalid",
                          "message": "missing session address", "details": {}},
            })
            return
        max_messages = req.get("maxMessages")
        with self.engine.lock:
            if session not in self.known_sessions:
                peer.send_json({
                    "type": "error",
                    "streamId": stream_id,
                    "error": {"code": "session/not-found",
                              "message": 'session "%s" not found' % session,
                              "details": {"sessionId": session}},
                })
                return
            events = self.logs.get(session, [])
            cursor = len(events) - 1
            sliced, has_more = self.paginate(events, None, max_messages, cursor)
            snapshot = {
                "type": "snapshot",
                "header": {"id": session, "cwd": os.getcwd()},
                "cursor": cursor,
                "records": [{"type": "event", "event": ev} for ev in sliced],
                "hasMore": has_more,
                "projections": {
                    "asOfSeq": cursor,
                    "values": {"inbox": self.inbox_of(session)},
                },
            }
            if peer.send_json({"type": "item", "streamId": stream_id, "value": snapshot}):
                self.followers.append((session, stream_id, peer))

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
        if endpoint == "session/follow":
            raise RemoteError("gateway/signature-invalid",
                              "stream endpoint requires remote.mux")
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
                session = request.get("sessionId") or ("session-" + secrets.token_hex(16))
                if session not in self.known_sessions:
                    self.known_sessions.append(session)
                    self.save_sessions()
                    self.logs[session] = []
                if session not in self.attached:
                    turn_num = self.open_turn_num(session)
                    if turn_num is not None:
                        self.append_event(session, "turn/end",
                                          {"turn": turn_num, "reason": {"kind": "interrupted"}})
                    self.attached.add(session)
            return {"sessionId": session}
        if endpoint == "session/list":
            with engine.lock:
                return {"items": [{"sessionId": session, "updatedAt": 0,
                                   "blank": len(self.logs.get(session, ())) == 0,
                                   "running": engine.turn is not None and session == self.session}
                                  for session in self.known_sessions],
                        "hasMore": False}
        if endpoint == "session/page":
            with engine.lock:
                addr = request.get("address") or {}
                session = addr.get("sessionId") if addr.get("kind") == "session" else None
                if session not in self.known_sessions:
                    raise RemoteError("session/not-found", 'session "%s" not found' % session,
                                      sessionId=session)
                events = self.logs.get(session, [])
                through_seq = request.get("throughSeq")
                if not isinstance(through_seq, int) or through_seq >= len(events) or through_seq < -1:
                    raise RemoteError("gateway/arguments-invalid", "invalid throughSeq")
                before_seq = request.get("beforeSeq")
                max_messages = request.get("maxMessages")
                sliced, has_more = self.paginate(events, before_seq, max_messages, through_seq)
                return {
                    "records": [{"type": "event", "event": ev} for ev in sliced],
                    "hasMore": has_more,
                }
        if endpoint == "session/updateQueue":
            with engine.lock:
                session = request.get("sessionId")
                item_id = request.get("itemId")
                action = request.get("action") or {}
                if session not in self.attached:
                    raise RemoteError("session/queue-item-not-found",
                                      "queued item is no longer pending", itemId=item_id)
                inbox = self.inbox_of(session)
                located = None
                for target in ("next-turn", "next-step"):
                    for idx, msg in enumerate(inbox[target]):
                        if msg.get("id") == item_id:
                            located = (target, idx)
                            break
                    if located is not None:
                        break
                if located is None:
                    raise RemoteError("session/queue-item-not-found",
                                      "queued item is no longer pending", itemId=item_id)
                target, idx = located
                if action.get("kind") != "remove":
                    raise RemoteError("gateway/bad-request", "unsupported queue action")
                engine.pending = [p for p in engine.pending if p.meta.get("itemId") != item_id]
                self.append_event(session, "agent/inbox/spliced", {
                    "target": target,
                    "start": idx,
                    "removedCount": 1,
                    "inserted": [],
                    "outcome": "canceled",
                })
                return {"accepted": True}
        if endpoint == "session/cancel":
            with engine.lock:
                session = request.get("sessionId")
                self.resolve(session)
                if engine.turn is not None:
                    turn_num = self.open_turn_num(session)
                    engine.end_turn()
                    if turn_num is not None:
                        self.append_event(session, "turn/end",
                                          {"turn": turn_num, "reason": {"kind": "aborted"}})
            return {"accepted": True}
        return self.prompt(request)

    def resolve(self, session):
        if session not in self.attached:
            raise RemoteError("session/not-found", 'session "%s" not found' % session,
                              sessionId=session)

    def prompt(self, request):
        content = request.get("content") or []
        text = "".join(part.get("text", "") for part in content if part.get("type") == "text")
        media = images_of(content)
        if not text.strip() and not media:
            raise RemoteError("gateway/bad-request",
                              "prompt content must include non-whitespace text or an attachment")
        session, request_id = request.get("sessionId"), request.get("requestId")
        target = "next-turn" if request.get("mode") == "queue" else "next-step"
        engine = self.engine
        with engine.lock:
            self.resolve(session)
            inp = Input(marker_id(text) or request_id, text,
                        "prompt" if engine.turn is None else "steer",
                        {"session": session, "requestId": request_id}, media=media)
            engine.received(inp)
            if not self.has_prompt_request(session, request_id):
                if engine.turn is not None and self.take_refusal():
                    self.attached.discard(session)
                    raise RemoteError("session/not-found",
                                      'session "%s" was disposed during prompt admission'
                                      % session, sessionId=session)
                msg = {
                    "id": "item-" + secrets.token_hex(8),
                    "role": "user",
                    "content": content,
                    "source": {"kind": "user", "rpcId": request_id},
                }
                inp.meta["itemId"] = msg["id"]
                inp.meta["message"] = msg
                cur = self.inbox_of(session)[target]
                self.append_event(session, "agent/inbox/spliced", {
                    "target": target,
                    "start": len(cur),
                    "removedCount": 0,
                    "inserted": [msg],
                })
                self.session = session
                if engine.turn is None:
                    turn_num = self.next_turn_num(session)
                    self.append_event(session, "turn/start", {"turn": turn_num})
                    self.append_event(session, "agent/inbox/spliced", {
                        "target": target,
                        "start": 0,
                        "removedCount": 1,
                        "inserted": [],
                    })
                    if engine.open_turn(inp):
                        self.append_event(session, "user/message", msg, surface=True)
                else:
                    engine.queue(inp)
        slow = engine.root / "slow"
        delay = int(slow.read_text() or "0") / 1000 if slow.exists() else 0
        if delay > 0:
            time.sleep(delay)
        return {"accepted": True}

    def consumed(self, taken):
        if not taken:
            return
        session = taken[0][0].meta["session"]
        inbox = self.inbox_of(session)["next-step"]
        ids = {inp.meta["itemId"] for inp, _ in taken}
        for item_id in [m["id"] for m in inbox if m.get("id") in ids]:
            cur = self.inbox_of(session)["next-step"]
            idx = next(i for i, m in enumerate(cur) if m.get("id") == item_id)
            self.append_event(session, "agent/inbox/spliced", {
                "target": "next-step",
                "start": idx,
                "removedCount": 1,
                "inserted": [],
            })
        for inp, fresh in taken:
            if fresh:
                self.append_event(session, "user/message", inp.meta["message"], surface=True)

    def finish(self):
        engine = self.engine
        if engine.pending:
            self.consumed(engine.step())
        turn_num = self.open_turn_num(self.session)
        engine.end_turn()
        if turn_num is not None:
            self.append_event(self.session, "turn/end",
                              {"turn": turn_num, "reason": {"kind": "completed"}})


PROTOCOLS = {"dsh": Dsh}
