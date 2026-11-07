# GitHub watches

GitHub watches share a process-wide observation per normalized repository and
PR number or check ref. Each schedule retains its own baseline, last observation
and delivery state. A PR observation reads the PR, the head's Check Runs and
legacy commit statuses; each list is completely paginated. Duplicate watches do
not multiply those endpoint reads. This is O(distinct targets × pages), not a
promise of one HTTP request for a composite PR observation.

Unchanged observations back off from 60 to 120, 240 and 300 seconds. These are
AnyAgents intervals, not GitHub-mandated intervals. Changed aggregate state or
head SHA or a newly registered watch resets the interval to 60 seconds. PR watches remain active after
checks complete, because the merge/close transition is still outstanding.
Merged/closed PR watches and completed check watches settle after delivery.

## Dedicated credentials

Set `ANYAGENTS_GITHUB_TOKEN_DIR` in the `serve-admin` environment to an absolute
directory containing `<owner>.token` files, for example `SylphxAI.token` and
`Cubeage.token`. Each contains that owner's raw GitHub App installation token
(an optional trailing newline is accepted). Owner matching is case-insensitive;
ambiguous filenames are refused. Each file must be a regular, non-symlink file,
mode **0600**, owned by the daemon's effective user. Files are opened with
nonblocking/no-follow flags and validated through the opened descriptor before
reading, so a FIFO without a writer is refused rather than stopping all probes.
The daemon re-reads the
selected file **before every actual HTTP request**, so it can be replaced
atomically without restarting the daemon. The issuer should refresh it before
expiry (the desk minter plans 45–60 minutes). The daemon never writes these files.

The dedicated path uses an in-process HTTP client, not `gh`; the token is never
an argument, cache entry, error string or log field. A configured but missing,
unreadable, insecurely owned/mode, empty or malformed file, or HTTP 401, reports `probe_error:auth` and
pauses that owner's probes for 60 seconds before trying the file again. Other
owners keep working. It never
falls back to the user's login. HTTP redirects are not followed. A token's
expiry is enforced by GitHub; the raw file does not carry a trusted expiry time.

Without this setting, the existing authorized `gh` adapter remains available.
`ANYAGENTS_GITHUB_API_BASE` overrides the HTTP base for isolated fixtures.

The installation token must cover every watched repository and allow:

| Endpoint | Read permission |
| --- | --- |
| `GET /repos/{owner}/{repo}/pulls/{number}` | `pull_requests` |
| `GET /repos/{owner}/{repo}/commits/{ref}/check-runs` | `checks` |
| `GET /repos/{owner}/{repo}/commits/{ref}/status` | `statuses` |

`metadata` is the standard installation permission. There are no comments,
reviews, GraphQL, writes or Actions endpoints in this poller. The legacy status
endpoint cannot be dropped without losing pending/review-failure observations.
The endpoint permission contracts were read on 2026-11-07 at 06:03 UTC in the
[PR](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request),
[Check Run](https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference)
and [commit status](https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference)
REST documentation.

Secret issuance and the serving environment belong to the deployment, not this
library. No additional service is required.

## Quota and diagnostics

`ANYAGENTS_GITHUB_RATE_LIMIT_FLOOR` is a nonnegative count, default **1500**.
Each response's `X-RateLimit-Remaining` and `X-RateLimit-Reset` are checked,
including successful 200/304 responses. Below the floor, no subsequent page or
target is requested until the UTC reset. The crossing response can still finish
an observation. Other probes report `probe_error:rate_limited`, not `transport`.
Missing/past reset metadata uses a 60-second cooldown. Secondary limits retain
`Retry-After` and bounded exponential backoff. The latest applicable deadline
always wins: a shorter secondary retry cannot shorten an existing reserve hold.
New watches cannot bypass holds.

The reset is an admission barrier, not a common retry time. After either a
reserve or rate-limit pause, retained targets resume across **reset + 60–300
seconds**. The window is stratified across targets, with target order and the
position within each slot hashed from the canonical key and reset. Existing
adaptive intervals and ordinary due times survive the pause; a later ordinary
due time still wins. Targets first seen during the hold receive a separate
recovery gate, and a newly registered watcher cannot clear that gate. There are
no requests at reset or during its first second. A 96-target regression checks
200/304 reserve crossings and 403 exhaustion: distinct request times, exactly
24 targets per quarter of the window, and no early requests. The scheduler's
tick granularity can group nearby deadlines into the same tick.

Each actual response emits `github_watch_probe kind=pull|check_runs|commit_status
status=<HTTP code>` to daemon stderr. Count these records to distinguish wire
200/304 responses by endpoint type; `gh-calls.log` counts child invocations, not
billed requests. Dedicated mode produces no `gh` child at all.

## Specification and live control

Read on **2026-11-07 at 05:58 UTC**, against API version `2022-11-28`:

- [Conditional requests and best practices](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api?apiVersion=2022-11-28#use-conditional-requests):
  reuse ETag with `If-None-Match`, or Last-Modified with `If-Modified-Since`.
  Correctly authenticated 304 responses do not consume primary quota.
- [REST rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28):
  response headers are authoritative; pause until reset on primary exhaustion,
  honor Retry-After and wait at least one minute on secondary limits.

A status-only control at **05:58:40–05:58:47 UTC** used PR #123 in this repository
and its head's first Check Runs page through separate installed `gh` children.
For **each endpoint**, cold then two conditional requests produced:

| Request headers | Status sequence |
| --- | --- |
| `Cache-Control: no-cache`, reused ETag | 200, 304, 304 |
| No Cache-Control header, reused ETag | 200, 304, 304 |

No bodies, tokens or response header values were printed. This proves unchanged
responses can return 304 through the installed adapter, including with no-cache;
it does **not** establish the production daemon's response distribution. Removing
no-cache is cleanup, not an established cause of billed 200s. The pre-change
source already retained ETags in process and coalesced targets. The decisive
user-budget protection is the dedicated installation credential, with reserve
admission and terminal settlement reducing work further. The desk's `gh-state`
cache was considered, but its PR inventory, freshness and truncated rollups do
not cover arbitrary watched SHA refs or complete Check Run pagination; directly
reading REST preserves the existing observation contract without coupling the
library to a desk-specific cache or another GraphQL polling system.
