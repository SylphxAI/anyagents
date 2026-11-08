# Serving process identity

`GET /v1/webhooks/healthz` identifies the process answering the request on both
`anyagents.sock` (`serve`) and `admin.sock` (`serve-admin`). No token is required.
The primary socket and optional public webhook listener expose only the image
identity at `/v1/webhooks/healthz` (plus `/` and `/healthz` on TCP). PID and process
start time are available only on the admin Unix socket.

```sh
curl --unix-socket "$DATA/anyagents.sock" http://local/v1/webhooks/healthz
curl --unix-socket "$DATA/admin.sock" http://local/v1/webhooks/healthz
```

| Field | Meaning |
| --- | --- |
| `ok` | `true`, preserving the existing readiness response. |
| `version` | Cargo package version embedded at build time. |
| `source_sha` | Git HEAD embedded at build time; `unknown` for a source archive without Git metadata. Runtime environment variables do not override it. |
| `binary_sha256` | Lowercase SHA-256 of the running image, computed once in a background worker after bind; `null` until complete. Linux reads `/proc/self/exe`; other supported Unix hosts use `current_exe()`. |
| `binary_sha256_state` | `computing`, `ready`, or `failed`. A read failure leaves the hash `null`, without blocking readiness. |
| `process_started_at_ms` | Admin socket only: UTC Unix milliseconds captured at entry to `main`, before command parsing. |
| `pid` | Admin socket only: PID of the responding process. |

Version and source SHA are available immediately. Wait for
`binary_sha256_state: "ready"` before comparing the hash; a `failed` state never
substitutes an installed-path hash or a fabricated value.

Responses carry `Cache-Control: no-store`. Replacing the installed executable
path does not change a running process's response. Read each socket separately
after installation: both binary hashes must match the published platform identity,
and the admin PID/start time must describe its replacement process. A disk
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
