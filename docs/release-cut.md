# Automatic version cuts

A main push reconciles one rolling `release/next` pull request when shipped
inputs have changed since the latest stable GitHub release. A documentation-only
change with no unpublished shipped inputs opens no cut. Pending pushes may
coalesce: the reconciler always reads current main and the complete unreleased
range, not only the push that woke it.

Shipped inputs are `crates/**` (including embedded resources and build scripts),
`packages/**`, `Cargo.toml`, `Cargo.lock`, the Rust toolchain declaration and
`.cargo/**`. Tests, documentation and workflow-only changes do not independently
trigger a cut. This deliberately conservative map may release a crate-local test
edit; it must not omit a runtime input.

The default increment is patch. A merged pull request labelled `minor` or `major`
raises the increment; `major` wins if both appear in the unreleased population.
The cut commits the Cargo workspace version, resolver-generated lockfile, all
five npm package versions and the launcher's optional dependency versions. It
contains no product-source changes of its own.

The cut is mechanical, without a `[review]` title. Its title and squash subject
are `release: vX.Y.Z`. The workflow verifies its exact head and requests normal
auto-merge with `--match-head-commit`; it does not bypass `ci-ok` or the merge
queue. Updating a rolling cut retains branch history with a normal push, never a
force push. The reconciler refuses to replace a release branch that contains
non-version changes.

The writer is an existing repository-scoped GitHub App grant obtained through
OIDC (`release-cut-anyagents`). It requests only `contents:write` and
`pull_requests:write`; no new long-lived secret or paid service is required.
A missing grant fails closed. Standard hosted runners are admitted only when
the repository payload explicitly says `visibility: public`.

Publication is separate. It requires successful main CI at the exact merged cut
SHA and uses the existing pinned mcp-kit publisher. If a later main commit
supersedes a cut before CI completes, unchanged mcp-kit cannot select the retired
SHA: publication is a reported no-op, and the next reconciliation creates a fresh
version from current main. A docs-only follow-up can therefore replace an
unpublished cut, but cannot create a release in an otherwise caught-up tree.
Published or partially published versions are never reassigned to different
source. A replacement increments beyond the version already committed on main.

To reconcile missed events, dispatch `release-cut.yml` on `main`. Read the run
summary for either `cut vX.Y.Z: PR #N, head SHA` or `no-op` and its reason. Do not
manually reset `release/next`, overwrite published packages or bypass the queue.
The workflow refuses any npm error except an explicit structured `E404` as proof
that the planned version is unused.
