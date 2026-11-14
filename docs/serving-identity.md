# Serving process identity

`GET /v1/webhooks/healthz` on `anyagents.sock` (`serve`) identifies the running
image. No token is required; reaching the socket is the operator's authority.

```sh
curl --unix-socket "$DATA/anyagents.sock" http://local/v1/webhooks/healthz
```

The public TCP listener answers `GET /`, `/healthz` and `/v1/webhooks/healthz`
with readiness only, `{"ok":true}` and `Cache-Control: no-store`. The exact
version and binary digest tell a scanner which advisories apply, so they stay
on the socket.

## Listeners

| Listener | Bound by | Serves | Credentials |
| --- | --- | --- | --- |
| `anyagents.sock` | always | the whole local API | operator token and local capabilities on the API; remote session tokens only on MCP |
| local TCP | `ANYAGENTS_WEBHOOK_LISTEN`; default `0.0.0.0:8788`, or `127.0.0.1:8788` once `public_listen` is set | readiness, Telegram webhooks, MCP (`/v1/webhooks/mcp`, `/v1/mcp`, `/mcp`) | remote session tokens on MCP (a local capability gets 403); a channel's secret token on its Telegram webhook |
| public TCP | `public_listen` under `[server]` in `<data>/anyagents.toml` | `/`, `/healthz`, `/v1/webhooks/healthz`, `/v1/webhooks/telegram/{channel}`, `/v1/webhooks/mcp`; every other path is 404 | remote session tokens on MCP; a channel's secret token on its Telegram webhook |

The listener a request arrives on decides what it may present. No source
address and no `X-Forwarded-*` or `Forwarded` header widens it. The bound
address is written to `<data>/public.addr` (and `anyagents status --json`
`public_addr`). A configuration file that cannot be read fails closed: no
public listener, and the local TCP listener defaults to loopback.

| Field | Meaning |
| --- | --- |
| `ok` | `true`, preserving the existing readiness response. |
| `version` | Cargo package version embedded at build time. |
| `source_sha` | Git HEAD embedded at build time; `unknown` for a source archive without Git metadata. Runtime environment variables do not override it. |
| `binary_sha256` | Lowercase SHA-256 of the running image, computed once in a background worker after bind; `null` until complete. Linux reads `/proc/self/exe`; other supported Unix hosts use `current_exe()`. |
| `binary_sha256_state` | `computing`, `ready`, or `failed`. A read failure leaves the hash `null`, without blocking readiness. |

Version and source SHA are available immediately. Wait for
`binary_sha256_state: "ready"` before comparing the hash; a `failed` state never
substitutes an installed-path hash or a fabricated value.

Responses carry `Cache-Control: no-store`. Replacing the installed executable
path does not change a running process's response. After installation, the
serving binary hash must match the published platform identity. A disk
`--version` check alone does not establish the serving revision.

These fields contain no token, environment, account or agent data. They establish
process identity, not whether every supervised agent is healthy.

## Persisted execution principals

An OS-seat deployment can keep its explicit grants without setting an environment
variable on every restart. `<data>/execution-principals.json` records principal
bindings keyed by immutable agent ID, using the same schema as
`ANYAGENTS_PRINCIPALS`:

```json
{"agt_00000000000000000000000000":{"user":"worker","uid":42001,"gid":42002,"socket_gid":42003}}
```

The daemon user must own this regular file; use mode `0600`. Shared-write files,
links, malformed JSON, invalid identities and incomplete grants are rejected
without modifying the registry. The CLI loads it before launching services or
vendors only when `ANYAGENTS_PRINCIPALS` is absent. An explicit environment map
always wins, including an empty or invalid value: it never falls back to another
principal.

On the first daemon startup without a registry, the server can import existing
operator-owned OS accounts using `ANYAGENTS_SEAT_USER_PREFIX` (default `seat-`)
and the configured socket group. It prefers an account named for the immutable
ID, accepts existing display-name accounts only for that initial import, and
persists the resulting ID bindings before starting workers. Subsequent startups
and renames never derive grants from mutable names or credential-file ownership.
Keep this registry with the deployment's durable state.

At creation, an exact per-ID OS account is preferred; otherwise a child inherits
its parent's bound principal. Newly created roots without an exact ID account
need an explicit operator grant before managed-seat execution. Explicit
environment maps disable this provisioning path and remain wholly operator-owned.
The registry is mode `0600`, and launch boundary checks deny it to agent seats.
