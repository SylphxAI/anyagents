# Serving process identity

`GET /v1/webhooks/healthz` identifies the process answering the request on both
`anyagents.sock` (`serve`) and `admin.sock` (`serve-admin`). No token is required.
The optional public webhook listener exposes the same response at `/`, `/healthz`
and `/v1/webhooks/healthz`; it identifies `serve`, not the admin sidecar.

```sh
curl --unix-socket "$DATA/anyagents.sock" http://local/v1/webhooks/healthz
curl --unix-socket "$DATA/admin.sock" http://local/v1/webhooks/healthz
```

| Field | Meaning |
| --- | --- |
| `ok` | `true`, preserving the existing readiness response. |
| `version` | Cargo package version embedded at build time. |
| `source_sha` | Git HEAD embedded at build time; `unknown` for a source archive without Git metadata. Runtime environment variables do not override it. |
| `binary_sha256` | Lowercase SHA-256 of the executable image, computed once before the listener starts. Linux reads `/proc/self/exe`; other supported Unix hosts use `current_exe()`. A read failure prevents startup. |
| `process_started_at_ms` | UTC Unix milliseconds captured at entry to `main`, before command parsing. |
| `pid` | PID of the responding process. |

Responses carry `Cache-Control: no-store`. Replacing the installed executable
path does not change a running process's response. Read each socket separately
after installation: both binary hashes must match the published platform identity,
and both PIDs/start times must describe the replacement processes. A disk
`--version` check alone does not establish the serving revision.

These fields contain no token, environment, account or agent data. They establish
process identity, not whether every supervised agent is healthy.
