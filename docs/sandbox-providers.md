# Universal Multi-Provider Sandbox Runtime

AnyAgents separates **agent orchestration** from **code execution** through a vendor-neutral async [`SandboxProvider`](../crates/anyagents-tools/src/providers/mod.rs) trait, a unified persistent volume layout, and a repo-owned multi-stage [`Dockerfile.sandbox`](../Dockerfile.sandbox).

## 1. Persistent Volume Layout (`/home/user/agent-data`)

Earlier revisions mounted only `/home/user/agent-state` while leaving `/home/user/workspace` on the ephemeral container/microVM rootfs, causing every lease `:release` or container recycle to wipe git checkouts and uncommitted edits.

All providers now standardize on a **single persistent volume mount at `/home/user/agent-data`**:

```text
/home/user/agent-data/            <- single persistent volume mount
  ├── state/                      <- agent config, transcripts, session IDs, skills, MCP token
  └── workspace/                  <- default working directory (git repos & uncommitted edits)

/home/user/agent-state -> /home/user/agent-data/state      (backwards-compatible symlink)
/home/user/workspace   -> /home/user/agent-data/workspace  (backwards-compatible symlink)
```

Because both `state/` and `workspace/` live inside the single mounted volume:
- Releasing compute (`release()`) preserves all session history, skills, git repositories, build caches, and uncommitted edits.
- Destroying an agent (`destroy()`) releases active compute and deletes the persistent volume.

## 2. Two-Tier Lease Policy & Guest Activity Heartbeat

`LeasePolicy` prevents both cold-boot thrashing on interactive chat and 24-hour orphan compute burn:

- **`idle_ttl_ms` (default: `300_000` ms = 5 min; `0` for batch jobs)**: warm-hold window after a turn completes before releasing compute.
- **`max_lease_ttl_ms` (default: `900_000` ms = 15 min)**: upper bound safety lease TTL renewed only while an agent is active or within its warm-hold window.

Inside the sandbox, `anyagents-guest` reports remote subprocess activity so an idle sweeper never releases compute while a turn or detached build/test command is still running:

- **`anyagents-guest serve`** emits periodic NDJSON heartbeat frames (`--heartbeat-interval-ms`, env `ANYAGENTS_GUEST_HEARTBEAT_INTERVAL_MS`, default `5000` ms) and maintains `<state-dir>/.guest-activity.json`:
  ```json
  {
    "type": "system",
    "subtype": "heartbeat",
    "pid": 42,
    "timestamp_ms": 1763506800000,
    "harness": "claude-code",
    "session_id": "sess-1",
    "turn_state": "idle",
    "active_turns": 0,
    "queued_turns": 0,
    "inflight_children": 1,
    "descendant_processes": 2,
    "busy": true
  }
  ```
- **`anyagents-guest health`** reads live Linux `/proc` descendant counts for the active `serve` process (or reports `0` when idle/booting) and outputs `active_turns`, `inflight_children`, `descendant_processes`, `busy`, `state_dir`, and `workdir`.

## 3. Built-In Sandbox Providers

Configure the active provider via `ANYAGENTS_SANDBOX_PROVIDER` (or `<data_dir>/sandbox-provider.json`) and load it with `ProviderRegistry::from_env_or_config(...)`.

| Provider | `ANYAGENTS_SANDBOX_PROVIDER` | OCI Image | Templates | Persistent Volume Survives `release()` | Bundle Sync | Network Policy |
|---|---|---|---|---|---|---|
| `LocalHostProvider` | `local` (default) | — | — | Yes (local dir) | Direct tar extract | — |
| `SylphxProvider` | `sylphx` | Yes (`@sha256:...`) | Yes | Yes (Sylphx Volume) | Single `.tar.gz` upload + extract | Yes (`Allow`/`Deny`/`Allowlist` + live `:setNetwork`) |
| `E2bProvider` | `e2b` | Custom server | Yes | Yes (`pause`/`resume`) | Single `.tar.gz` via `envd` | `Allow`/`Deny` |
| `ModalProvider` | `modal` | Yes | Yes | Yes (Modal Volume) | Single `.tar.gz` upload + extract | `Allow`/`Deny`/`Allowlist` |
| `DaytonaProvider` | `daytona` | Yes | Yes (snapshots) | Yes (Daytona Volume + `stop`/`start`) | Single `.tar.gz` upload + extract | `Allow`/`Deny`/`Allowlist` |
| `DockerProvider` | `docker` | Yes | — | Yes (named Docker volume) | Streaming `docker exec -i tar -xzf -` | `Allow`/`Deny`/custom network |
| `CloudflareProvider` | `cloudflare` | Yes | — | Yes (Container sleep + DO/R2 state) | `PUT /fs/tar` `.tar.gz` | Passed in contract |
| `GenericHttpOciProvider` | `http` | Yes | Yes | Yes | `POST /bundle` `.tar.gz` | Yes (+ live `/network`) |

### Common Environment Variables

```bash
ANYAGENTS_SANDBOX_PROVIDER=local|sylphx|e2b|modal|daytona|docker|cloudflare|http
ANYAGENTS_SANDBOX_IMAGE=ghcr.io/<org>/anyagents-sandbox@sha256:<64-hex-digest>
ANYAGENTS_SANDBOX_TEMPLATE=agent
ANYAGENTS_SANDBOX_SHAPE=linux-m
ANYAGENTS_SANDBOX_IDLE_TTL_MS=300000
ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS=900000
ANYAGENTS_SANDBOX_VOLUME_SIZE_GIB=20
ANYAGENTS_SANDBOX_MOUNT_PATH=/home/user/agent-data
ANYAGENTS_SANDBOX_WORKDIR=/home/user/agent-data/workspace
```

Provider-specific variables:
- **Sylphx**: `ANYAGENTS_SYLPHX_PARENT` (`orgs/{o}/projects/{p}/envs/{e}`), `ANYAGENTS_SANDBOX_ENDPOINT` (default `https://api.sylphx.com`), `ANYAGENTS_SANDBOX_TOKEN` / `SYLPHX_API_KEY`.
- **E2B**: `E2B_API_KEY`, `E2B_API_URL` (default `https://api.e2b.app`), `E2B_DOMAIN` (default `e2b.app`), `E2B_TEMPLATE`.
- **Modal**: `ANYAGENTS_MODAL_ENDPOINT` (or `MODAL_SANDBOX_URL`), `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`, `ANYAGENTS_MODAL_APP`.
- **Daytona**: `DAYTONA_API_KEY`, `DAYTONA_API_URL` (default `https://app.daytona.io/api`), `DAYTONA_TARGET`.
- **Docker**: `DOCKER_HOST` (or `ANYAGENTS_DOCKER_HOST`), `ANYAGENTS_DOCKER_NETWORK`, `ANYAGENTS_DOCKER_MEMORY`, `ANYAGENTS_DOCKER_CPUS`.
- **Cloudflare**: `ANYAGENTS_CLOUDFLARE_SANDBOX_URL`, `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`.
- **Generic HTTP OCI**: `ANYAGENTS_SANDBOX_HTTP_URL` (or `ANYAGENTS_SANDBOX_PROVIDER_URL`), `ANYAGENTS_SANDBOX_TOKEN`.

## 4. Building & Pinning Custom Images (`Dockerfile.sandbox`)

Build the minimal base image, the developer toolchain image (`rustc`/`cargo` via `rustup` + `bun`), or the full image with selected public CLI harnesses:

```bash
# Minimal base sandbox (anyagents-guest + git, rg, fd, jq, python3, node/npm, gh, tini)
docker build -f Dockerfile.sandbox --target sandbox-base \
  -t ghcr.io/<your-org>/anyagents-sandbox:base .

# Developer toolchain sandbox (extends sandbox-base with rustc/cargo via rustup + bun)
docker build -f Dockerfile.sandbox --target sandbox-dev \
  -t ghcr.io/<your-org>/anyagents-sandbox:dev .

# Full sandbox with selected public CLI harnesses
docker build -f Dockerfile.sandbox --target sandbox-full \
  --build-arg INSTALL_HARNESSES="claude-code codex opencode" \
  -t ghcr.io/<your-org>/anyagents-sandbox:full .
```

The vendor-neutral [`.github/workflows/docker-publish.yml`](../.github/workflows/docker-publish.yml) workflow builds and publishes both `Dockerfile` (Coordinator) and all three `Dockerfile.sandbox` targets (`sandbox-base`, `sandbox-dev`, `sandbox-full`) on version tags, GitHub releases, or manual `workflow_dispatch` (with configurable `registry`), outputting exact immutable `@sha256:...` digests.

Extend `sandbox-base` with your own SDKs, language toolchains, or private CLIs:

```dockerfile
FROM ghcr.io/<your-org>/anyagents-sandbox:base

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential golang-go \
    && rm -rf /var/lib/apt/lists/*
USER 1000:1000
```

Push to your OCI registry and extract the immutable digest:

```bash
docker push ghcr.io/<your-org>/anyagents-sandbox:full
DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/<your-org>/anyagents-sandbox:full)
echo "$DIGEST"
# -> ghcr.io/<your-org>/anyagents-sandbox@sha256:0123456789abcdef...
```

Pass the digest-pinned image reference to **any** OCI-capable provider:

```bash
export ANYAGENTS_SANDBOX_PROVIDER=sylphx   # or docker, modal, daytona, cloudflare, http
export ANYAGENTS_SANDBOX_IMAGE="$DIGEST"
```

## 5. Generic HTTP OCI Wire Contract (`ANYAGENTS_SANDBOX_PROVIDER=http`)

Any custom sandbox controller (Kubernetes, Firecracker, Fly Machines, Nomad, or an internal platform) can integrate with AnyAgents with **zero Rust changes** by implementing these 6 HTTP JSON endpoints (authenticated with `Authorization: Bearer <token>` when configured):

1. **`POST /v1/sandboxes/ensure`**
   - Request:
     ```json
     {
       "agent_id": "agt_123",
       "agent_name": "builder",
       "image": "ghcr.io/org/anyagents-sandbox@sha256:...",
       "template": null,
       "env": { "FOO": "bar" },
       "volume": { "id_or_name": "anyagents-agt-123", "mount_path": "/home/user/agent-data", "size_gib": 20 },
       "shape": "linux-m",
       "network": { "mode": "allow" },
       "lease": { "idle_ttl_ms": 300000, "max_lease_ttl_ms": 900000 },
       "existing_sandbox_id": null,
       "workdir": "/home/user/agent-data/workspace"
     }
     ```
   - Response (`200`/`201`):
     ```json
     {
       "sandbox_id": "sbx_abc",
       "volume_id": "vol_123",
       "mount_path": "/home/user/agent-data",
       "state_dir": "/home/user/agent-data/state",
       "workdir": "/home/user/agent-data/workspace",
       "transport_url": "https://sandbox.example/sbx_abc/envd",
       "transport_token": "optional-token",
       "expires_at": "2026-11-19T00:00:00Z",
       "metadata": {}
     }
     ```

2. **`POST /v1/sandboxes/{id}/exec`**
   - Request: `{ "command": ["/bin/sh", "-lc", "git status"], "cwd": "/home/user/agent-data/workspace", "env": {}, "timeout_ms": 30000, "stdin_base64": null }`
   - Response (`200`): `{ "stdout": "...", "stderr": "", "exit_code": 0 }`

3. **`POST /v1/sandboxes/{id}/bundle?dest=/home/user/agent-data/state`**
   - Request: raw `.tar.gz` bytes (`Content-Type: application/gzip`).
   - Response (`200`/`204`). If the server returns `404`/`405`/`501`, the provider automatically falls back to base64-streaming the archive through `/exec`.

4. **`POST /v1/sandboxes/{id}/network`** *(optional live egress mutation)*
   - Request: `{ "network": { "mode": "allowlist", "allowlist": ["api.github.com"] } }`
   - Response (`200`/`204`).

5. **`POST /v1/sandboxes/{id}/release`**
   - Request: `{ "volume_id": "vol_123" }`
   - Stops/releases compute while **preserving** the persistent volume.

6. **`DELETE /v1/sandboxes/{id}?volume_id=vol_123`**
   - Permanently tears down compute and deletes the persistent volume (idempotent on `404`).

7. **`GET /v1/health`**
   - Response (`200`): `{ "healthy": true, "message": "ok" }`.
