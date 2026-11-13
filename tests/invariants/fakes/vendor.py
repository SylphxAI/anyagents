"""A scripted vendor for the AnyAgents delivery conformance suite.

    vendor.py --fixture DIR --protocol NAME [the vendor's own arguments...]

One engine models a vendor session: the durable journal of every input the
model saw, the running turn, and the input waiting for the turn's next step.
A protocol front end speaks one vendor's wire format, the way the AnyAgents
adapter for that vendor expects it, on top of that engine. The engine is the
same for every vendor; only what a vendor says about it differs.

The fixture directory holds the state. A test drives the vendor by writing a
command file into it, which is removed once acted on:
  step      the running turn reaches its next step and consumes every
            pending input, in order
  step-N    the next step consumes the first N pending inputs, says so, and
            the process exits before anything else (a crash after partial
            consumption)
  finish    the running turn ends
  crash     the process exits at once
  refuse    the next input that reaches a running turn is refused, and not
            applied
A state file is read while it exists:
  slow      the vendor answers each input this many milliseconds late

Records, one JSON object per line:
  journal.jsonl  {"id", "kind", "turn", "step", "launch", "text"}: an input the
                 model saw, in the order it saw them ("prompt" opens a turn,
                 "steer" joined a running one at a step)
  wire.jsonl     {"id", "launch", "kind"}: every input as it arrived
  launches       how many processes have started on this fixture
"""
import importlib
import os
import pathlib
import sys
import threading

from engine import Engine, drive

# One module per protocol family, `proto_<family>.py`, each with a
# `PROTOCOLS` map from protocol name to its front end.
FAMILIES = {
    "spiron": "proto_spiron",
    "spiron-v0": "proto_spiron",
    "codex": "proto_codex",
    "claude": "proto_claude",
    "muse": "proto_muse",
    "opencode": "proto_opencode",
    "dsh": "proto_dsh",
    "grok": "proto_grok",
    "omp": "proto_omp",
    "agy": "proto_agy",
}


def main():
    args = sys.argv[1:]
    root = pathlib.Path(args[args.index("--fixture") + 1])
    name = args[args.index("--protocol") + 1]
    rest = [arg for i, arg in enumerate(args)
            if arg not in ("--fixture", "--protocol")
            and (i == 0 or args[i - 1] not in ("--fixture", "--protocol"))]
    kind = importlib.import_module(FAMILIES[name]).PROTOCOLS[name]
    engine = Engine(root, kind.idempotent)
    protocol = kind(engine, rest)
    protocol.start()
    threading.Thread(target=drive, args=(engine, protocol), daemon=True).start()
    protocol.serve()
    # The host closed our input: a vendor process ends with it.
    os._exit(0)


if __name__ == "__main__":
    main()
