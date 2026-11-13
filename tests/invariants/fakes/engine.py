"""The scripted vendor engine shared by every protocol front end.

See `vendor.py` for how a test drives it.
"""

import json
import queue
import os
import sys
import threading
import time

MARKER = "anyagents-message:"


def marker_id(text):
    """The mailbox id on the first line of an input's text."""
    first = (text or "").split("\n", 1)[0]
    return first[len(MARKER):] if first.startswith(MARKER) else None


class Input:
    def __init__(self, ident, text, kind, meta=None):
        self.id = ident
        self.text = text
        self.kind = kind
        self.meta = meta or {}


class Engine:
    """One vendor session: a durable journal, the running turn, pending input."""

    def __init__(self, root, idempotent):
        self.root = root
        self.idempotent = idempotent
        self.lock = threading.RLock()
        counter = root / "launches"
        self.launch = int(counter.read_text()) + 1 if counter.exists() else 1
        counter.write_text(str(self.launch))
        self.turn = None
        self.turns = 0
        self.step_no = 0
        self.pending = []
        self.save()

    def append(self, name, value):
        with (self.root / name).open("a") as out:
            out.write(json.dumps(value) + "\n")

    def journal_ids(self, kept_only=False):
        path = self.root / "journal.jsonl"
        if not path.exists():
            return set()
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        return {row["id"] for row in rows if not (kept_only and row["kind"] == "lost")}

    def received(self, inp):
        self.append("wire.jsonl", {"id": inp.id, "launch": self.launch, "kind": inp.kind})

    def save(self):
        """What a test reads about the running turn."""
        (self.root / "state.json").write_text(
            json.dumps({"turn": self.turn, "pending": len(self.pending)}))

    def journal(self, inp, kind):
        """Record that the model saw `inp`. A vendor that deduplicates by our
        id answers False, and records nothing, for an input already there.
        A recovery note is recorded as a note: it carries no input of its own.
        An input `lost` with its process is no input the session holds."""
        if self.idempotent and inp.id in self.journal_ids(kept_only=True):
            return False
        if "[Recovery]" in (inp.text or ""):
            kind = "note"
        self.append("journal.jsonl", {
            "id": inp.id, "kind": kind, "turn": self.turn, "step": self.step_no,
            "launch": self.launch, "text": inp.text,
        })
        return True

    def open_turn(self, inp, turn_id=None):
        """Open a turn with `inp` as its prompt; `turn_id` names it the way the
        vendor does, when the vendor names a turn after its prompt."""
        self.turns += 1
        self.turn = turn_id or "turn-%d-%d" % (self.launch, self.turns)
        self.step_no = 0
        self.pending = []
        fresh = self.journal(inp, "prompt")
        self.save()
        return fresh

    def queue(self, inp):
        self.pending.append(inp)
        self.save()

    def step(self, limit=None, kind="steer"):
        """The running turn's next step: the pending inputs it consumed, each
        with whether it was new to the journal."""
        if self.turn is None:
            return []
        take = self.pending if limit is None else self.pending[:limit]
        self.pending = self.pending[len(take):]
        self.step_no += 1
        taken = [(inp, self.journal(inp, kind)) for inp in take]
        self.save()
        return taken

    def end_turn(self):
        """Close the running turn; the inputs it never consumed."""
        leftover, self.pending, self.turn = self.pending, [], None
        self.save()
        return leftover


class Protocol:
    """One vendor's wire format over the engine."""

    idempotent = False
    # The vendor writes an input's proof before it stores the input (Claude
    # Code appends its transcript up to 100 ms after it announces a line).
    stores_after_proof = False

    def __init__(self, engine, args):
        self.engine = engine
        self.args = args
        self.refusing = False
        self.session = "fake-session"
        # Everything the vendor prints leaves in order: a late answer holds
        # back what follows it, as on a real vendor's one output stream.
        self.outbox = queue.Queue()
        self.local = threading.local()
        threading.Thread(target=self.write_out, daemon=True).start()

    def emit(self, frame):
        not_before = getattr(self.local, "not_before", 0.0)
        self.outbox.put((not_before, json.dumps(frame)))

    def write_out(self):
        while True:
            not_before, line = self.outbox.get()
            wait = not_before - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
            self.outbox.task_done()

    def flush(self):
        """Wait until everything emitted so far is written."""
        self.outbox.join()

    def later(self, action):
        """Run `action` now; what it prints leaves after the `slow` delay."""
        slow = self.engine.root / "slow"
        delay = int(slow.read_text() or "0") / 1000 if slow.exists() else 0
        self.local.not_before = time.monotonic() + max(delay, 0)
        try:
            action()
        finally:
            self.local.not_before = 0.0

    def take_refusal(self):
        refused, self.refusing = self.refusing, False
        return refused

    def start(self):
        pass

    def serve(self):
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            with self.engine.lock:
                self.on_line(msg)

    def on_line(self, msg):
        raise NotImplementedError

    def consumed(self, taken):
        """The running turn consumed `taken`, a list of (input, fresh)."""

    def finish(self):
        """End the running turn the way this vendor does."""
        self.engine.end_turn()


def drive(engine, protocol):
    """Act on the test's command files."""
    root = engine.root
    while True:
        with engine.lock:
            for path in sorted(root.glob("step-*")):
                count = int(path.name.split("-", 1)[1])
                protocol.consumed(engine.step(count))
                protocol.flush()
                path.unlink()
                os._exit(1)
            # The model takes the inputs, then the process dies before it
            # writes a word of proof. A vendor that stores input only after
            # proving it (`stores_after_proof`) never stored them: the next
            # process's session does not hold them, so they are journaled as
            # lost, not as input the session kept.
            for path in sorted(root.glob("unproven-*")):
                count = int(path.name.split("-", 1)[1])
                taken = engine.step(count, kind="lost" if protocol.stores_after_proof else "steer")
                path.unlink()
                os._exit(1)
            if (root / "crash").exists():
                (root / "crash").unlink()
                os._exit(1)
            if (root / "refuse").exists():
                (root / "refuse").unlink()
                protocol.refusing = True
            if (root / "step").exists():
                if engine.turn is not None:
                    protocol.consumed(engine.step())
                (root / "step").unlink()
            if (root / "finish").exists():
                if engine.turn is not None:
                    protocol.finish()
                (root / "finish").unlink()
        time.sleep(0.005)


