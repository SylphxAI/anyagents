#!/usr/bin/env bash
# Deterministic, credential-free verification. Never launches a live vendor model.
set -euo pipefail
cd "$(dirname "$0")/.."

echo '== Native adapter protocols (scripted vendor processes) =='
for adapter in claude codex opencode agy muse omp; do
    echo "== $adapter =="
    cargo test --locked -p "anyagents-harness-$adapter" --test protocol
done

echo '== Shared stream/RPC implementation (including dsh and grok) =='
cargo test --locked -p anyagents-harness-stream

echo '== Per-catalog-harness server round trips (contract doubles) =='
cargo test --locked -p anyagents --test server_harnesses

echo '== Native OpenCode server path (scripted CLI) =='
cargo test --locked -p anyagents --test server_opencode

echo '== Sign-in persistence, MCP/API and runtime identity =='
cargo test --locked -p anyagents --test signin --test mcp --test identity

echo '== Mailbox crash/retry invariants and supervisor =='
cargo test --locked -p anyagents-invariants -p anyagents-supervisor

echo 'PASS: scripted verification only; live vendor auth and OS isolation are not certified.'
