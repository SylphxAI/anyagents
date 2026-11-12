#!/usr/bin/env python3
"""Exact-commit deploy builds for .github/workflows/deploy-build.yml.

A deployment pins one main commit and the SHA-256 of the binary built from it.
This script holds the two decisions the workflow makes about that commit:

  admit <sha>       the commit is reachable from live main and its own push/main
                    `ci` run concluded success; anything else, or an unreadable
                    answer, refuses.
  identity <sha> <binary> <healthz.json> <out.json>
                    the built binary's serving identity names that commit and
                    the binary's own digest; writes the identity record.

Stdlib only; reads GITHUB_REPOSITORY and GH_TOKEN for the API.
"""
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request

FULL_SHA = re.compile(r"[0-9a-f]{40}")
CI_WORKFLOW_PATH = ".github/workflows/ci.yml"
TAG_PREFIX = "deploy-"
ASSET = "anyagents-linux-x64-gnu"


class Refused(Exception):
    """A precondition did not hold or could not be read."""


def tag_for(sha):
    return TAG_PREFIX + sha


def get_api(path, *, repository=None, token=None, opener=urllib.request.urlopen):
    repository = repository or os.environ["GITHUB_REPOSITORY"]
    token = token if token is not None else os.environ.get("GH_TOKEN", "")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/{path}",
        headers={"Accept": "application/vnd.github+json", "User-Agent": "anyagents-deploy-build",
                 **({"Authorization": f"Bearer {token}"} if token else {})})
    try:
        with opener(request, timeout=30) as response:
            return json.load(response)
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise Refused(f"cannot read {path}: {error}") from error


def admit(sha, get=get_api):
    """Return None when `sha` may be built; raise Refused otherwise."""
    if not FULL_SHA.fullmatch(sha or ""):
        raise Refused("commit must be a full lowercase 40-hex SHA")
    compare = get(f"compare/{sha}...main")
    # `behind` means main does not contain the commit; `identical` and `ahead`
    # mean it does. Any other answer is not proof of reachability.
    if compare.get("status") not in ("ahead", "identical"):
        raise Refused(f"{sha} is not reachable from main (compare status {compare.get('status')!r})")
    if compare.get("base_commit", {}).get("sha") != sha:
        raise Refused("compare answer is not bound to the requested commit")
    runs = get(f"actions/runs?head_sha={sha}&event=push&branch=main&per_page=100")
    if "workflow_runs" not in runs or not isinstance(runs.get("total_count"), int):
        raise Refused("CI run inventory is unreadable")
    if runs["total_count"] > len(runs["workflow_runs"]):
        raise Refused("CI run inventory is incomplete")
    proof = [run for run in runs["workflow_runs"]
             if run.get("head_sha") == sha and run.get("path") == CI_WORKFLOW_PATH
             and run.get("name") == "ci" and run.get("event") == "push"
             and run.get("head_branch") == "main" and run.get("status") == "completed"
             and run.get("conclusion") == "success"
             and run.get("head_repository", {}).get("full_name") == run.get("repository", {}).get("full_name")]
    if not proof:
        raise Refused(f"{sha} has no successful push/main ci run")
    return None


def identity(sha, binary_path, healthz, version):
    """The identity record for a built binary, or Refused."""
    digest = hashlib.sha256(open(binary_path, "rb").read()).hexdigest()
    if healthz.get("ok") is not True:
        raise Refused("healthz did not report ok")
    if healthz.get("source_sha") != sha:
        raise Refused(f"binary serves source {healthz.get('source_sha')!r}, not {sha}")
    if healthz.get("binary_sha256_state") != "ready" or healthz.get("binary_sha256") != digest:
        raise Refused("serving binary digest is not the built file's digest")
    if healthz.get("version") != version:
        raise Refused(f"binary reports version {healthz.get('version')!r}, workspace is {version!r}")
    return {"asset": ASSET, "commit": sha, "version": version, "sha256": digest, "tag": tag_for(sha)}


def main(argv):
    try:
        if len(argv) == 2 and argv[0] == "admit":
            admit(argv[1])
            print(f"admitted {argv[1]}")
            return 0
        if len(argv) == 6 and argv[0] == "identity":
            sha, binary, healthz_path, version, out = argv[1:]
            record = identity(sha, binary, json.load(open(healthz_path)), version)
            with open(out, "w") as handle:
                json.dump(record, handle, indent=2, sort_keys=True)
                handle.write("\n")
            print(json.dumps(record, sort_keys=True))
            return 0
    except Refused as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
