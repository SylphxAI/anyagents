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


def cut_version(read, subject):
    """Read both current and first-parent manifests from Git, not the worktree."""
    current = json.loads(read("HEAD", "packages/anyagents/package.json"))
    version = current["version"]
    require(VERSION.fullmatch(version) is not None, "invalid stable release version")
    parent = json.loads(read("HEAD^1", "packages/anyagents/package.json"))
    if parent["version"] == version:
        return None
    require(re.fullmatch(rf"release: v{re.escape(version)}(?: \(#[0-9]+\))?", subject)
            is not None, "version change is not a release-cut commit")
    require(VERSION.fullmatch(parent["version"]) is not None, "invalid parent version")
    require(tuple(map(int, version.split("."))) > tuple(map(int, parent["version"].split("."))),
            "release version must increase")
    for ref, wanted in (("HEAD", version), ("HEAD^1", parent["version"])):
        cargo = tomllib.loads(read(ref, "Cargo.toml"))
        require(cargo["workspace"]["package"]["version"] == wanted,
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


def admit(event, env, get_api=api, inspect=publication):
    require(env["GITHUB_REPOSITORY"] == REPOSITORY, "unexpected repository")
    require(event.get("repository", {}).get("visibility") == "public", "public repository required")
    require(env["GITHUB_REF"] == "refs/heads/main", "main ref required")
    sha = env["GITHUB_SHA"]
    if env["GITHUB_EVENT_NAME"] == "workflow_run":
        trigger = event["workflow_run"]
        run_id = trigger["id"]
    else:
        require(env["GITHUB_EVENT_NAME"] == "workflow_dispatch", "unsupported release event")
        run_id = event.get("inputs", {}).get("ci_run_id", "")
    require(re.fullmatch(r"[1-9][0-9]*", str(run_id)), "positive ci_run_id required")
    run = get_api(f"actions/runs/{run_id}")
    workflow = get_api(f"actions/workflows/{CI['id']}")
    # Validate the triggering run before deciding whether its source is superseded.
    prove_ci(run, workflow, run_id, run.get("head_sha", ""))
    if env["GITHUB_EVENT_NAME"] == "workflow_run":
        prove_ci(trigger, workflow, run_id, run["head_sha"])
    if sha != run["head_sha"] or get_api("git/ref/heads/main")["object"]["sha"] != sha:
        return {"publish": "false", "outcome": "no-op", "reason": "superseded green source; a fresh version cut is required",
                "source": run["head_sha"], "ci_run_id": str(run_id)}
    require(git("rev-parse", "HEAD") == sha, "checkout differs from green source")
    version = cut_version(lambda ref, path: git("show", f"{ref}:{path}"), git("show", "-s", "--format=%s", "HEAD"))
    result = {"publish": "false", "outcome": "no-op", "source": sha, "ci_run_id": str(run_id)}
    if version is None:
        return {**result, "reason": "ordinary commit; no committed version cut"}
    done = inspect(version, sha)
    if all(done.values()):
        return {**result, "version": version, "reason": "already published from this exact green source"}
    return {**result, "publish": "true", "outcome": "admitted", "version": version,
            "reason": "exact green version cut; publication required"}


def receipt(env, inspect=publication):
    result = json.loads(env.get("ADMISSION", "{}") or "{}")
    require(env["ADMISSION_RESULT"] == "success", "admission failed; publication not authorized")
    if result.get("publish") != "true":
        return result
    require(env["RELEASE_RESULT"] == "success",
            "publisher failed or was cancelled; publication may be partial, reconcile the same source only")
    done = inspect(result["version"], result["source"])
    require(all(done.values()), "publisher returned success without complete npm/GitHub publication")
    return {**result, "outcome": "published", "reason": "npm packages and GitHub release verified at the admitted source"}


def report(result, env):
    text = f"## Release {result['outcome']}\n\n"
    if result.get("version"):
        text += f"Version: **{result['version']}**\n\n"
    text += f"{result['reason']}\n\n"
    if result.get("source"):
        text += f"Source: `{result['source']}`\n\n"
    if result.get("ci_run_id"):
        text += (f"CI proof: [{result['ci_run_id']}](https://github.com/{REPOSITORY}/actions/runs/{result['ci_run_id']}) "
                 f"— `{CI['path']}` (ID {CI['id']}, name ci), push/main, completed/success.\n")
    print(text)
    if env.get("GITHUB_STEP_SUMMARY"):
        with open(env["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(text)
    if env.get("GITHUB_OUTPUT"):
        with open(env["GITHUB_OUTPUT"], "a") as stream:
            for key in ("publish", "version"):
                stream.write(f"{key}={result.get(key, '')}\n")
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
