# Exact-source release reconciliation

`.github/workflows/release.yml` reconciles incomplete version cuts from main's
history. Each version publishes from the commit that introduced it, after that
commit's own successful push/main CI. Later merges require no release hold.
The reusable publisher and its helpers share the immutable mcp-kit pin declared
in that workflow; its `source-sha` input selects the admitted commit.

## Discovery and admission

Every completed `ci` event on main and every manual release invocation starts the
same reconciliation. The event is a wake-up, not publication authority. Its SHA,
result and run ID cannot substitute for a candidate's own CI proof.

1. Check out the workflow's immutable main SHA with complete history. Confirm
   through GitHub's compare API that this snapshot remains an ancestor of live
   main, binding the comparison to both full SHAs. Newer cuts outside that snapshot
   are discovered by a successor run.
2. Walk first-parent launcher version history in order. An initial repository or
   launcher version is not a bump. For a merge commit, the introducing source is
   the main first-parent transition, not a feature-branch commit.
3. Read publication state at each introducing source. A complete npm/GitHub
   publication with matching source and identity digests is a no-op. A tag alone
   does not establish completion. A source or digest mismatch is refused.
   Historical complete releases may predate the current cut-title policy; that
   does not authorize creating or completing an old nonconforming release.
4. For an incomplete cut, require subject `release: vX.Y.Z` with an optional
   GitHub ` (#N)` suffix, an increasing version relative to its first parent,
   matching Cargo workspace and five npm versions, and matching launcher native
   dependency versions. Reintroducing the same version is refused.
5. Find that exact commit's own CI proof through the Actions API. The proof must
   identify workflow **388622800**, path `.github/workflows/ci.yml`, name **ci**,
   event **push**, branch **main**, status **completed**, conclusion **success**,
   and the candidate's full SHA in this repository, not a fork. Pull-request,
   merge-group, manual, missing, pending and failed CI cannot authorize it.
   An incomplete API inventory is refused rather than treated as proof of absence.
6. Emit a publisher matrix containing only eligible version/source/CI tuples.
   Each invocation supplies its tuple's `source-sha` to mcp-kit. The publisher
   validates that checkout and all existing canonical publication identity agree
   with the explicit source before building or completing channels.

Independent green cuts can publish while another source is refused. Every
refusal appears in the summary and fails the final receipt; a red historical cut
cannot silently authorize its own publication or permanently block a later green
version from completing.

## Evicted events and interrupted publication

Publishers remain serialized by the existing workflow concurrency group. GitHub
may replace an older pending run with a newer one; the surviving run discovers
all incomplete cuts in its snapshot. Two quick version bumps therefore remain
two source-bound publications even if the first event never executes. Matrix
publication is limited to one version at a time, with independent failures kept
visible.

After a failed or interrupted publication, inspect its admission, publisher logs
and immutable npm/GitHub objects before any manual action. Do not blindly rerun
it. A manual **Run workflow** invocation on main performs the same state
reconciliation and needs no event-specific CI input. It can complete a partial
publication only at its original introducing commit, with that source's own
successful push/main CI and consistent existing identity. Main may have advanced.
No existing version, tag or asset is overwritten.

HTTP failures, access denials and rate limits fail closed; only an explicit HTTP
404 proves absence. No read is automatically retried. Each hosted job and the
reusable-workflow call requires repository visibility exactly `public`, preserving
the US$0 standard-hosted-runner boundary.

## Receipts and checks

The run summary identifies its main-history snapshot and lists:

- **Already complete**: publication was read back with matching source/digests;
- **Publish**: an incomplete version and its exact introducing SHA and own CI run;
- **REFUSED**: a source and the proof, manifest, provenance or read failure.

The final receipt distinguishes **no-op**, **published** and **unverified**. A green
reusable workflow alone is not delivery: every admitted version must have all
five npm packages and all eight GitHub assets read back at its source. Identity
sidecar content hashes must match GitHub's published asset digests, and sidecars
must bind binary SHA256, version, platform and source. Publisher failure can leave
partial publication; those immutable artifacts are preserved for reconciliation.
No binary installation or daemon restart is performed.

Run the policy cases with Python 3.11 or newer:

```sh
python3 -m unittest discover -s tests -p 'test_release_admission.py' -v
```

The Linux CI lane runs these before the workspace tests. Cases use real Git
histories and simulated publication boundaries: two quick cuts with the first
event evicted, their separate emitted source tuples, an idempotent second pass,
later green main versus a red cut, independent eligible/refused cuts, first-parent
merges, historical complete releases, every CI proof dimension, public/main
refusal, digest/provenance mismatches and publication-receipt completeness. They
make no registry writes.
