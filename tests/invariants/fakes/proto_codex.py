"""`codex app-server` 0.169: JSON-RPC 2.0, one object per line."""
import json
import uuid

from engine import Input, Protocol, image_url, marker_id

# JSON-RPC error codes the app-server answers with.
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602

# The longest input text Codex takes, in characters.
MAX_INPUT_CHARS = 1 << 20


class Codex(Protocol):
    """`codex app-server` 0.169.

    `turn/start` opens a turn whose prompt enters the conversation at once;
    while a turn runs it steers into it instead. `turn/steer` (which names
    the running turn as `expectedTurnId`) puts input in the running turn's
    pending list and answers `{turnId}`: queued, not yet seen. At its next
    sampling step the turn takes every pending input, in order, and records
    each as a `userMessage` item (`item/started`, `item/completed`) carrying
    our `clientUserMessageId` as `clientId`. A turn samples again while input
    is pending, so it never completes on unconsumed input; an interrupt drops
    it. The server keeps no record of our ids: the same input sent twice is
    applied twice. `thread/items/list` pages through the thread's history.

    `thread/start` mints a thread id; `thread/resume` of an id with no
    recorded rollout answers `-32600 no rollout found for thread id <id>`.
    An input over 1,048,576 characters is refused with `-32602`, and
    nothing is recorded."""

    def reply(self, rid, result):
        self.emit({"jsonrpc": "2.0", "id": rid, "result": result})

    def error(self, rid, code, message, data=None):
        error = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        self.emit({"jsonrpc": "2.0", "id": rid, "error": error})

    def note(self, method, params):
        self.emit({"jsonrpc": "2.0", "method": method, "params": params})

    @staticmethod
    def item(ident, text):
        return {"type": "userMessage", "id": "item-" + ident, "clientId": ident,
                "content": [{"type": "text", "text": text, "text_elements": []}]}

    def recorded(self, inp):
        """The turn's step took `inp` into the conversation."""
        item = self.item(inp.id, inp.text)
        base = {"threadId": self.session, "turnId": self.engine.turn}
        self.note("item/started", dict(base, item=item, startedAtMs=0))
        self.note("item/completed", dict(base, item=item, completedAtMs=0))

    def turn(self, ident, status, error=None):
        return {"id": ident, "rootTurnId": ident, "items": [], "itemsView": "notLoaded",
                "status": status, "error": error}

    def history(self, params):
        """`thread/items/list`: the thread's journaled inputs, as `userMessage`
        items, filtered to one turn when asked, a page at a time."""
        path = self.engine.root / "journal.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        if params.get("turnId"):
            rows = [row for row in rows if row["turn"] == params["turnId"]]
        entries = [{"turnId": row["turn"], "startedAtMs": None, "completedAtMs": None,
                    "item": self.item(row["id"], row["text"])} for row in rows]
        if params.get("sortDirection") == "desc":
            entries.reverse()
        start = int(params.get("cursor") or 0)
        limit = int(params.get("limit") or 100)
        page = entries[start:start + limit]
        more = start + limit < len(entries)
        return {"data": page, "nextCursor": str(start + limit) if more else None,
                "backwardsCursor": str(start) if page else None}

    def on_line(self, msg):
        method, rid = msg.get("method"), msg.get("id")
        params = msg.get("params") or {}
        engine = self.engine
        if method == "initialize":
            self.reply(rid, {"userAgent": "codex_cli_rs/0.169.0 (Linux; fake)",
                             "codexHome": str(engine.root), "platformFamily": "unix",
                             "platformOs": "linux"})
        elif method == "initialized":
            pass
        elif method == "thread/start":
            self.session = str(uuid.uuid4())
            with (engine.root / "codex-threads").open("a") as out:
                out.write(self.session + "\n")
            self.reply(rid, {"thread": {"id": self.session}})
        elif method == "thread/resume":
            thread = params.get("threadId")
            threads = engine.root / "codex-threads"
            known = threads.read_text().split() if threads.exists() else []
            if thread not in known:
                self.error(rid, INVALID_REQUEST, "no rollout found for thread id %s" % thread)
                return
            self.session = thread
            self.reply(rid, {"thread": {"id": self.session}})
        elif method == "thread/items/list":
            self.reply(rid, self.history(params))
        elif method in ("turn/start", "turn/steer"):
            self.input(method, rid, params)
        elif method == "turn/interrupt":
            self.reply(rid, {})
            if engine.turn is not None:
                turn = engine.turn
                engine.end_turn()  # pending input is dropped
                self.note("turn/completed", {"threadId": self.session,
                                             "turn": self.turn(turn, "interrupted")})
        elif rid is not None:
            self.error(rid, METHOD_NOT_FOUND, "method not found: %s" % method)

    def input(self, method, rid, params):
        engine = self.engine
        parts = params.get("input") or []
        text = "".join(part.get("text", "") for part in parts)
        # `{"type": "image", "url": "data:..."}` reaches the model as an image.
        images = [image_url(part["url"]) for part in parts if part.get("type") == "image"]
        inp = Input(params.get("clientUserMessageId") or marker_id(text), text, method,
                    media=images)
        engine.received(inp)
        if len(text) > MAX_INPUT_CHARS:
            message = "Input exceeds the maximum length of %d characters." % MAX_INPUT_CHARS
            self.later(lambda: self.error(rid, INVALID_PARAMS, message))
            return
        if method == "turn/steer":
            if engine.turn is None:
                self.later(lambda: self.error(rid, INVALID_REQUEST, "no active turn to steer"))
                return
            if params.get("expectedTurnId") != engine.turn:
                message = "expected active turn id `%s` but found `%s`" % (
                    params.get("expectedTurnId"), engine.turn)
                self.later(lambda: self.error(rid, INVALID_REQUEST, message))
                return
        if engine.turn is not None:
            if self.take_refusal():
                self.later(lambda: self.error(
                    rid, INVALID_REQUEST, "cannot steer a compact turn",
                    {"codexErrorInfo": {"activeTurnNotSteerable": {"turnKind": "compact"}}}))
                return
            engine.queue(inp)
            turn = engine.turn
            if method == "turn/steer":
                self.later(lambda: self.reply(rid, {"turnId": turn}))
            else:
                # `turn/start` while a turn runs steers into it.
                self.later(lambda: self.reply(rid, {"turn": self.turn(turn, "inProgress")}))
            return
        engine.open_turn(inp)
        turn = engine.turn

        def started():
            self.reply(rid, {"turn": self.turn(turn, "inProgress")})
            self.note("turn/started", {"threadId": self.session,
                                       "turn": self.turn(turn, "inProgress")})
            if engine.turn == turn:
                self.recorded(inp)
        self.later(started)

    def consumed(self, taken):
        for inp, _ in taken:
            self.recorded(inp)

    def finish(self):
        engine = self.engine
        # A turn with pending input samples again before it completes.
        while engine.pending:
            self.consumed(engine.step())
        turn = engine.turn
        engine.end_turn()
        self.note("thread/tokenUsage/updated", {
            "threadId": self.session, "turnId": turn,
            "tokenUsage": {"last": {"inputTokens": 1, "outputTokens": 1},
                           "total": {"inputTokens": 1, "outputTokens": 1}}})
        self.note("turn/completed", {"threadId": self.session,
                                     "turn": self.turn(turn, "completed")})


PROTOCOLS = {"codex": Codex}
