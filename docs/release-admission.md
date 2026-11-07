# Exact-source release admission

`.github/workflows/release.yml` publishes only a committed version cut whose exact
source passed the `ci` workflow on a push to `main`. Publication still uses
`SylphxAI/mcp-kit@ef675524e619fe3dde5b88213e8598046f0aaff6`, unchanged.

## Admission contract

- Automatic admission starts after `ci` completes successfully on `main`; there
  is no independent push-to-release race. Pull-request, merge-group, failed and
  manual CI runs cannot authorize publication.
- The Actions API must identify workflow **388622800**, path
  `.github/workflows/ci.yml`, name **ci**, event **push**, branch **main**, status
  **completed**, conclusion **success**, and the exact source SHA in this
  repository, not a fork. The triggering webhook must agree with that proof.
- At admission, the successful run's `head_sha`, the release run's `github.sha`,
  the checkout and the live `main` head must all match. The shared publisher's
  initial checkout uses `github.sha`; it has no source-ref input. An admitted
  run stays pinned to that SHA even if `main` subsequently advances during builds.
- The source commit subject is `release: vX.Y.Z` (an optional GitHub ` (#N)`
  suffix is accepted). Relative to its first parent, the committed version must
  increase. Cargo workspace version, all five npm manifests and launcher native
  dependency versions must agree. An ordinary commit with unchanged versions
  records a no-op, even when it follows an unpublished cut.
- Existing npm `gitHead`, GitHub tag/target and identity sidecars must identify
  this same green source. A partial publication from another source is never
  completed. HTTP failures, access denials and rate limits fail closed; only an
  explicit HTTP 404 proves absence. No read is automatically retried.
- Each hosted job and the reusable-workflow call requires repository visibility
  exactly `public`. Standard public hosted runners preserve the US$0 boundary.

## Superseded cuts and manual completion

If `main` advances before a cut is admitted, the run records **no-op: superseded
green source**. Re-running it cannot publish that historical SHA through this
pinned publisher. The rolling cut policy must create a fresh version on current
source; it does not reuse the superseded or partially published version. A later
ordinary commit, including documentation, still represents cumulative unpublished
source and can cause that replacement cut. For example, superseded `0.1.2` is
replaced by a new `0.1.3` cut, not a second source under `0.1.2`.

For a failed or interrupted publication, first inspect its admission and publisher
logs and the npm/GitHub objects. Do not blindly rerun it. If `main` still equals
the original cut and every existing channel has the same source, use the release
workflow's **Run workflow** action on `main` with `ci_run_id` set to its successful
push/main CI run ID. The same proof is mandatory for this manual reconciliation;
a manual CI run cannot substitute. If `main` moved, use a fresh cut instead.
Publishers are serialized; GitHub may replace an older pending workflow run with
a newer pending run, but never cancels an active publisher through concurrency.

## Receipts and checks

The run summary records the admitted SHA and CI run, and distinguishes:

- **no-op**: ordinary commit, superseded source, or already completely published;
- **admitted**: exact green cut, publication still required (not a delivery receipt);
- **published**, with version and SHA: all five npm packages and all eight GitHub
  assets read back at the admitted source; identity sidecar digests are checked;
- **unverified**: admission, publisher or read-back failed; publication can be
  partial after a publisher failure, so existing immutable artifacts are preserved.

A green reusable workflow alone is not a publication receipt. The final receipt
job checks npm distribution metadata, `gitHead`, GitHub tag/source and assets.
Native build integrity and package smoke tests remain in the pinned publisher.
No binary installation or daemon restart is performed by this workflow.

Run the policy cases locally with Python 3.11 or newer:

```sh
python3 -m unittest discover -s tests -p 'test_release_admission.py' -v
```

The Linux CI lane runs these cases before the workspace tests. They exercise
positive cuts and reconciliation, every CI proof dimension, public/main refusal,
ordinary and superseded no-ops, manifest disagreement, cross-source partial
publication, read failures and publication receipt completeness without writes to
any registry.
