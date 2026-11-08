# GitHub watch REST replay fixtures

Read on 2026-11-07 at 02:52 UTC from existing, recorded REST responses;
no new GitHub API request was made to prepare these fixtures.

- `completed-check-runs.json`: a recorded completed-check response
- `pending-check-runs.json`: a recorded pending-check response

Only Check Run IDs, names, statuses and conclusions are retained. The 3,000-check
pagination test scales these recorded shapes with distinct synthetic IDs and puts
a pending check on the last page; it is a capacity regression, not a claim that
the original response contained 3,000 checks. Pull, commit-status, HTTP-header
and rate-limit examples are synthetic fixtures of the documented REST contract.

Contract read 2026-11-07 at 02:49 UTC:
<https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api>.
Authenticated ETag/304 requests do not consume the primary limit; a `Retry-After`
response pauses all GitHub watch requests, with an exponential fallback for
secondary-limit responses without that header.
