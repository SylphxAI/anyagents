#!/usr/bin/env python3
"""Exact-source admission and read-only publication receipts for release.yml."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

REPOSITORY = "SylphxAI/anyagents"
CI = {"id": 388622800, "path": ".github/workflows/ci.yml", "name": "ci"}
PLATFORMS = ("darwin-arm64", "darwin-x64", "linux-x64-gnu", "linux-arm64-gnu")
PACKAGE = "@sylphx/anyagents"
SHA = re.compile(r"[a-f0-9]{40}")
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


class OriginBoundRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        old, new = urlsplit(req.full_url), urlsplit(newurl)
        if (old.scheme, old.netloc) != (new.scheme, new.netloc):
            redirected.remove_header("Authorization")
        return redirected


urlopen = build_opener(OriginBoundRedirect()).open


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def request(url, missing=False, binary=False):
    headers = {"User-Agent": "anyagents-release", "Accept": "application/json"}
    if url.startswith("https://api.github.com/"):
        headers["Accept"] = "application/vnd.github+json"
        headers["Authorization"] = f"Bearer {os.environ['GH_TOKEN']}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
        if binary:
            headers["Accept"] = "application/octet-stream"
    try:
        with urlopen(Request(url, headers=headers), timeout=30) as response:
            data = response.read()
    except HTTPError as error:
        code = error.code
        error.close()
        if missing and code == 404:
            return None
        # No retries: rate limits and transport failures are not proof of absence.
        raise RuntimeError(f"release read failed: HTTP {code} at {url}") from None
    return data if binary else json.loads(data)


def api(path, **kwargs):
    return request(f"https://api.github.com/repos/{REPOSITORY}/{path}", **kwargs)


def prove_ci(run, workflow, run_id, sha):
    require(all(workflow.get(key) == value for key, value in CI.items()),
            "ci workflow identity mismatch")
    expected = {
        "id": int(run_id), "workflow_id": CI["id"], "path": CI["path"],
        "name": CI["name"], "event": "push", "head_branch": "main",
        "status": "completed", "conclusion": "success", "head_sha": sha,
    }
    require(SHA.fullmatch(sha) is not None, "invalid source SHA")
    for key, value in expected.items():
        require(run.get(key) == value, f"ci proof mismatch: {key}")
    for key in ("repository", "head_repository"):
        require(run.get(key, {}).get("full_name") == REPOSITORY,
                f"ci proof mismatch: {key}")


def cut_version(read, subject, source="HEAD"):
    """Read both current and first-parent manifests from Git, not the worktree."""
    current = json.loads(read(source, "packages/anyagents/package.json"))
    version = current["version"]
    require(VERSION.fullmatch(version) is not None, "invalid stable release version")
    parent_ref = f"{source}^1"
    parent = json.loads(read(parent_ref, "packages/anyagents/package.json"))
    if parent["version"] == version:
        return None
    require(re.fullmatch(rf"release: v{re.escape(version)}(?: \(#[0-9]+\))?", subject)
            is not None, "version change is not a release-cut commit")
    require(VERSION.fullmatch(parent["version"]) is not None, "invalid parent version")
    require(tuple(map(int, version.split("."))) > tuple(map(int, parent["version"].split("."))),
            "release version must increase")
    for ref, wanted in ((source, version), (parent_ref, parent["version"])):
        cargo = tomllib.loads(read(ref, "Cargo.toml"))
        allowed_cargo = (wanted,) if ref == source else (wanted, version)
        require(cargo["workspace"]["package"]["version"] in allowed_cargo,
                "Cargo/npm version mismatch")
        launcher = json.loads(read(ref, "packages/anyagents/package.json"))
        require(launcher["name"] == PACKAGE, "launcher name mismatch")
        require(launcher.get("optionalDependencies") == {
            f"{PACKAGE}-{platform}": wanted for platform in PLATFORMS
        }, "launcher native dependencies mismatch")
        for platform in PLATFORMS:
            native = json.loads(read(ref, f"packages/npm/{platform}/package.json"))
            require(native["name"] == f"{PACKAGE}-{platform}" and native["version"] == wanted,
                    "native package identity mismatch")
    return version


def publication(version, sha, get_api=api, get=request):
    """Only exact-source existing channels may be completed by the shared publisher."""
    packages = []
    for name in (PACKAGE, *(f"{PACKAGE}-{platform}" for platform in PLATFORMS)):
        value = get(f"https://registry.npmjs.org/{quote(name, safe='')}/{version}", missing=True)
        if value is not None:
            require(value.get("name") == name and value.get("version") == version,
                    "npm package identity mismatch")
            require(value.get("gitHead") == sha, "npm source differs from green source")
            require(value.get("dist", {}).get("integrity") and value.get("dist", {}).get("tarball"),
                    "npm distribution metadata missing")
        packages.append(value is not None)
    # A tag without a release still controls gh release create's effective source.
    tag = get_api(f"git/ref/tags/v{version}", missing=True)
    if tag is not None:
        obj = tag["object"]
        # The publisher creates lightweight tags; permit one annotated tag too.
        if obj["type"] == "tag":
            obj = get_api(f"git/tags/{obj['sha']}")["object"]
        require(obj.get("type") == "commit" and obj.get("sha") == sha,
                "GitHub tag source differs from green source")
    release = get_api(f"releases/tags/v{version}", missing=True)
    assets_done = False
    if release is not None:
        require(tag is not None, "GitHub release tag is absent")
        require(release.get("tag_name") == f"v{version}" and not release.get("draft"),
                "GitHub release identity mismatch")
        # mcp-kit trusts a full target_commitish before resolving the tag.
        target = release.get("target_commitish", "")
        require(not SHA.fullmatch(target) or target == sha,
                "GitHub release target differs from green source")
        assets = {asset["name"]: asset for asset in release["assets"]}
        required = []
        for platform in PLATFORMS:
            archive = f"anyagents-{platform}.tar.gz"
            sidecar = f"anyagents-{platform}.identity.json"
            required.extend((archive, sidecar))
            for name in (archive, sidecar):
                if name in assets:
                    require(re.fullmatch(r"sha256:[a-f0-9]{64}", assets[name].get("digest", "")),
                            "GitHub asset digest missing")
            if sidecar in assets:
                asset = assets[sidecar]
                require(asset["url"].startswith(
                    f"https://api.github.com/repos/{REPOSITORY}/releases/assets/"),
                    "unexpected identity asset URL")
                data = get(asset["url"], binary=True)
                require(f"sha256:{hashlib.sha256(data).hexdigest()}" == asset["digest"],
                        "GitHub identity digest mismatch")
                identity = json.loads(data)
                require(identity.get("name") == "anyagents" and identity.get("version") == version
                        and identity.get("platform") == platform
                        and identity.get("source") == {"repository": REPOSITORY, "commit": sha}
                        and re.fullmatch(r"[a-f0-9]{64}", identity.get("sha256", "")),
                        "GitHub identity source mismatch")
        assets_done = all(name in assets for name in required)
    return {"npm": all(packages), "github": assets_done}


def source_ci(sha, get_api, workflow):
    """Find a source's own proof; the triggering run cannot authorize other cuts."""
    runs = get_api(f"actions/workflows/{CI['id']}/runs?event=push&branch=main&head_sha={sha}&per_page=100")
    values = runs["workflow_runs"]
    require(runs["total_count"] == len(values), "incomplete source CI inventory")
    for run in sorted(values, key=lambda value: value["id"]):
        if run.get("status") == "completed" and run.get("conclusion") == "success":
            prove_ci(run, workflow, run["id"], sha)
            return str(run["id"])
    states = ", ".join(f"{run['id']}:{run.get('conclusion') or run.get('status')}" for run in values)
    raise ValueError(f"no successful exact-source push/main ci ({states or 'no runs'})")


def admit(event, env, get_api=api, inspect=publication):
    require(env["GITHUB_REPOSITORY"] == REPOSITORY, "unexpected repository")
    require(event.get("repository", {}).get("visibility") == "public", "public repository required")
    require(env["GITHUB_REF"] == "refs/heads/main", "main ref required")
    require(env["GITHUB_EVENT_NAME"] in ("workflow_run", "workflow_dispatch"), "unsupported release event")
    require(git("rev-parse", "--is-shallow-repository") == "false", "complete main history required")
    snapshot = git("rev-parse", "HEAD")
    main = get_api("git/ref/heads/main")["object"]["sha"]
    require(SHA.fullmatch(main) is not None, "invalid main SHA")
    # A newer main may arrive after checkout. Reconcile this immutable ancestor
    # snapshot; a successor run will discover any newer cuts without a merge hold.
    ancestry = get_api(f"compare/{snapshot}...{main}")
    require(ancestry.get("status") in ("ahead", "identical")
            and ancestry.get("base_commit", {}).get("sha") == snapshot
            and ancestry.get("merge_base_commit", {}).get("sha") == snapshot,
            "checkout is not an ancestor of main")
    result = {"publish": "false", "outcome": "no-op", "main": snapshot,
              "releases": [], "complete": [], "refused": []}
    read = lambda ref, path: git("show", f"{ref}:{path}")
    workflow = None
    seen = set()
    sources = git("rev-list", "--first-parent", "--reverse", snapshot, "--",
                  "packages/anyagents/package.json").splitlines()
    for sha in sources:
        # The initial repository/package version is not a version bump. This
        # also handles the launcher being added after the repository's root.
        if (len(git("rev-list", "--parents", "-n", "1", sha).split()) < 2
                or not git("ls-tree", "--name-only", f"{sha}^1", "--", "packages/anyagents/package.json")):
            continue
        candidate = {"source": sha}
        try:
            version = json.loads(read(sha, "packages/anyagents/package.json"))["version"]
            if json.loads(read(f"{sha}^1", "packages/anyagents/package.json"))["version"] == version:
                continue
            candidate["version"] = version
            require(VERSION.fullmatch(version) is not None, "invalid stable release version")
            require(version not in seen, "version introduced more than once on main")
            seen.add(version)
            # Completed versions need no new publication authority. Still verify
            # their immutable source and digests instead of trusting tag presence.
            done = inspect(version, sha)
            if all(done.values()):
                result["complete"].append(candidate)
                continue
            # Historical complete releases may predate the cut-title policy.
            # New or partial publication still requires every current cut rule.
            cut_version(read, git("show", "-s", "--format=%s", sha), source=sha)
            if workflow is None:
                workflow = get_api(f"actions/workflows/{CI['id']}")
            candidate["ci_run_id"] = source_ci(sha, get_api, workflow)
            result["releases"].append(candidate)
        except Exception as error:
            # A bad source never enters the publisher matrix. Independent green
            # sources may finish; the final receipt makes every refusal loud.
            result["refused"].append({**candidate, "reason": str(error)})
    if result["releases"]:
        result.update(publish="true", outcome="admitted", reason="incomplete cuts have their own exact green push/main CI")
    elif result["refused"]:
        result.update(outcome="unverified", reason="release sources refused; no publication authorized")
    else:
        result["reason"] = "all committed release cuts are already published, or no cut exists"
    return result


def receipt(env, inspect=publication):
    result = json.loads(env.get("ADMISSION", "{}") or "{}")
    require(env["ADMISSION_RESULT"] == "success", "admission failed; publication not authorized")
    releases = result.get("releases", [])
    if releases:
        require(env["RELEASE_RESULT"] == "success",
                "publisher failed or was cancelled; publication may be partial, reconcile the same sources only")
        for candidate in releases:
            done = inspect(candidate["version"], candidate["source"])
            require(all(done.values()), f"publisher returned success without complete npm/GitHub publication: {candidate['version']}")
    refused = result.get("refused", [])
    require(not refused, "release sources refused: " + "; ".join(
        f"{item.get('version', 'unknown version')} at {item['source']}: {item['reason']}" for item in refused))
    if not releases:
        return result
    return {**result, "outcome": "published", "reason": "npm packages and GitHub releases verified at each admitted source"}


def report(result, env):
    text = f"## Release {result['outcome']}\n\n{result['reason']}\n\n"
    if result.get("main"):
        text += f"Main-history snapshot: `{result['main']}`\n\n"
    for key, label in (("releases", "Publish"), ("complete", "Already complete"), ("refused", "REFUSED")):
        for item in result.get(key, []):
            text += f"- {label}: {item.get('version', 'unknown version')} at `{item['source']}`"
            if item.get("ci_run_id"):
                text += (f" — [push/main ci {item['ci_run_id']}](https://github.com/{REPOSITORY}/actions/runs/{item['ci_run_id']})")
            if item.get("reason"):
                text += f" — {item['reason']}"
            text += "\n"
    print(text)
    if env.get("GITHUB_STEP_SUMMARY"):
        with open(env["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(text)
    if env.get("GITHUB_OUTPUT"):
        with open(env["GITHUB_OUTPUT"], "a") as stream:
            stream.write(f"publish={result.get('publish', 'false')}\n")
            stream.write(f"matrix={json.dumps({'include': result.get('releases', [])})}\n")
            stream.write(f"admission={json.dumps(result)}\n")


def main():
    env = os.environ
    try:
        if sys.argv[1:] == ["admit"]:
            result = admit(json.loads(Path(env["GITHUB_EVENT_PATH"]).read_text()), env)
        else:
            require(sys.argv[1:] == ["receipt"], "expected admit or receipt")
            result = receipt(env)
        report(result, env)
    except Exception as error:
        report({"outcome": "unverified", "reason": str(error)}, env)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
