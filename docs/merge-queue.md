# Merge queue

Changes to `main` merge through GitHub's merge queue. The repository ruleset
`anyagents-merge-queue` requires a queue entry, and the organization ruleset
`org-required-ci-gate` requires GitHub Actions' `ci-ok` check. There is no bypass
actor in either ruleset.

The `ci` workflow handles both pull requests and `merge_group` events. The
queue tests the synthetic merge with the latest base; a passing pull-request
check alone does not authorize a merge. Documentation-only changes may skip
compilation, but still require `ci-ok` on the merge group.

The queue uses squash merges, builds at most five entries concurrently and
merges one entry at a time. Its check timeout is 60 minutes. Keep these controls
in the repository's GitHub ruleset rather than an independent merge script.

GitHub-hosted standard runners are used only for this public repository. A
private repository must not schedule those jobs.
