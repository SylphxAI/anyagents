"""An async stdio server: EOF cancels work, and shutdown needs client EOF."""

import json
import os
import select
import sys


def emit(message):
    print(json.dumps(message), flush=True)


requests = [json.loads(sys.stdin.readline()) for _ in range(3)]
assert requests[0]["method"] == "initialize"
assert requests[1]["method"] == "notifications/initialized"
assert requests[2]["method"] == "tools/call"
assert requests[2]["params"] == {"name": "browser", "arguments": {"action": "snapshot"}}
emit({"jsonrpc": "2.0", "id": 0, "result": {"capabilities": {"tools": {}}}})

mode = sys.argv[1]
if mode == "initialize_only":
    os.close(sys.stdout.fileno())
else:
    # Match rmcp canceling an async call when its transport receives EOF.
    if select.select([sys.stdin], [], [], 0.05)[0] and not sys.stdin.read(1):
        sys.exit(0)
    sys.stderr.write("diagnostic\n" * 16384)
    sys.stderr.flush()
    print("non-JSON diagnostic", flush=True)
    emit({"jsonrpc": "2.0", "method": "notifications/progress", "params": {}})
    emit({"jsonrpc": "2.0", "id": 2, "result": {"content": [{"text": "unrelated"}]}})
    if mode == "error":
        emit({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "snapshot failed"}})
    else:
        emit({"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": '{"snapshot":"ready"}'}]}})

assert sys.stdin.read() == ""
