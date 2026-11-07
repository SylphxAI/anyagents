"""Behavioral release-admission cases; no network or publication writes."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

SPEC = importlib.util.spec_from_file_location(
    "release_admission", Path(__file__).resolve().parents[1] / "scripts/release_admission.py"
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
SOURCE = "a" * 40
OTHER = "b" * 40


def proof(sha=SOURCE):
    return {
        "id": 123, "workflow_id": release.CI["id"], "path": release.CI["path"], "name": "ci",
        "event": "push", "head_branch": "main", "head_sha": sha,
        "conclusion": "success", "status": "completed",
        "repository": {"full_name": release.REPOSITORY},
        "head_repository": {"full_name": release.REPOSITORY},
    }


def manifests(version):
    native = {f"{release.PACKAGE}-{key}": version for key in release.PLATFORMS}
    files = {
        "Cargo.toml": f'[workspace.package]\nversion = "{version}"\n',
        "packages/anyagents/package.json": json.dumps({
            "name": release.PACKAGE, "version": version, "optionalDependencies": native,
        }),
    }
    for key in release.PLATFORMS:
        files[f"packages/npm/{key}/package.json"] = json.dumps({
            "name": f"{release.PACKAGE}-{key}", "version": version,
        })
    return files


class ProofTests(unittest.TestCase):
    def test_exact_green_push_passes(self):
        release.prove_ci(proof(), release.CI, 123, SOURCE)

    def test_each_proof_dimension_fails_closed(self):
        for key, value in {
            "id": 124, "workflow_id": 7, "path": ".github/workflows/other.yml", "name": "other",
            "event": "pull_request", "head_branch": "other", "head_sha": OTHER,
            "conclusion": "failure", "status": "in_progress", "repository": {},
            "head_repository": {"full_name": "other/fork"},
        }.items():
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                release.prove_ci({**proof(), key: value}, release.CI, 123, SOURCE)
        for key in release.CI:
            with self.subTest(workflow=key), self.assertRaisesRegex(ValueError, "workflow identity"):
                release.prove_ci(proof(), {**release.CI, key: "wrong"}, 123, SOURCE)


class CutTests(unittest.TestCase):
    def setUp(self):
        self.files = {"HEAD": manifests("0.1.2"), "HEAD^1": manifests("0.1.1")}

    def cut(self, subject="release: v0.1.2"):
        return release.cut_version(lambda ref, path: self.files[ref][path], subject)

    def test_cut_and_squash_suffix(self):
        self.assertEqual(self.cut(), "0.1.2")
        self.assertEqual(self.cut("release: v0.1.2 (#90)"), "0.1.2")

    def test_ordinary_commit_is_noop(self):
        self.files["HEAD"] = manifests("0.1.1")
        self.assertIsNone(self.cut("docs: improve setup"))

    def test_marker_downgrade_and_mismatched_manifests_refused(self):
        with self.assertRaisesRegex(ValueError, "release-cut"):
            self.cut("feat: update version")
        self.files["HEAD"] = manifests("0.1.0")
        with self.assertRaisesRegex(ValueError, "must increase"):
            self.cut("release: v0.1.0")
        for path in manifests("0.1.2"):
            with self.subTest(path=path):
                self.files["HEAD"] = manifests("0.1.2")
                if path == "packages/anyagents/package.json":
                    value = json.loads(self.files["HEAD"][path])
                    value["optionalDependencies"] = {}
                    self.files["HEAD"][path] = json.dumps(value)
                else:
                    self.files["HEAD"][path] = manifests("0.1.1")[path]
                with self.assertRaises(ValueError):
                    self.cut()


class AdmissionTests(unittest.TestCase):
    """Use real first-parent Git objects, including a squash-style release commit."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.old_cwd = Path.cwd()
        os.chdir(self.temporary.name)
        subprocess.run(["git", "init", "-q"], check=True)
        release.git("config", "user.name", "Test")
        release.git("config", "user.email", "test@example.com")
        self.commit("0.1.1", "initial")
        self.commit("0.1.2", "release: v0.1.2 (#90)")
        self.sha = release.git("rev-parse", "HEAD")
        self.run = proof(self.sha)
        self.env = {
            "GITHUB_REPOSITORY": release.REPOSITORY, "GITHUB_REF": "refs/heads/main",
            "GITHUB_SHA": self.sha, "GITHUB_EVENT_NAME": "workflow_run",
        }
        self.event = {"repository": {"visibility": "public"}, "workflow_run": self.run}

    def tearDown(self):
        os.chdir(self.old_cwd)
        self.temporary.cleanup()

    def commit(self, version, subject):
        for path, data in manifests(version).items():
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(data)
        release.git("add", ".")
        release.git("commit", "--allow-empty", "-qm", subject)

    def api(self, path):
        return {
            "actions/runs/123": self.run,
            f"actions/workflows/{release.CI['id']}": release.CI,
            "git/ref/heads/main": {"object": {"sha": self.sha}},
        }[path]

    def admit(self, complete=False):
        return release.admit(self.event, self.env, self.api,
                             lambda version, sha: {"npm": complete, "github": complete})

    def test_new_cut_admits_and_complete_release_is_explicit_noop(self):
        self.assertEqual(self.admit()["publish"], "true")
        self.assertEqual(self.admit()["source"], self.sha)
        self.assertEqual(self.admit(True)["reason"], "already published from this exact green source")

    def test_manual_reconcile_requires_same_exact_proof(self):
        self.env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        self.event["inputs"] = {"ci_run_id": "123"}
        self.assertEqual(self.admit()["publish"], "true")
        self.run["event"] = "workflow_dispatch"
        with self.assertRaisesRegex(ValueError, "event"):
            self.admit()
        self.event["inputs"]["ci_run_id"] = ""
        with self.assertRaisesRegex(ValueError, "ci_run_id"):
            self.admit()

    def test_superseded_event_sha_or_live_main_is_noop(self):
        self.env["GITHUB_SHA"] = OTHER
        self.assertIn("superseded", self.admit()["reason"])
        self.env["GITHUB_SHA"] = self.sha
        original_api = self.api
        result = release.admit(self.event, self.env,
                               lambda path: {"object": {"sha": OTHER}} if path == "git/ref/heads/main"
                               else original_api(path), lambda *args: self.fail("must not probe publication"))
        self.assertEqual(result["publish"], "false")

    def test_public_main_required_including_missing_visibility(self):
        for visibility in (None, "private", "internal"):
            self.event["repository"] = {"visibility": visibility}
            with self.subTest(visibility=visibility), self.assertRaisesRegex(ValueError, "public"):
                self.admit()
        self.event["repository"]["visibility"] = "public"
        self.env["GITHUB_REF"] = "refs/heads/feature"
        with self.assertRaisesRegex(ValueError, "main ref"):
            self.admit()

    def test_ordinary_followup_cannot_publish_superseded_cut(self):
        self.commit("0.1.2", "docs: setup")
        self.sha = release.git("rev-parse", "HEAD")
        self.run["head_sha"] = self.sha
        self.env["GITHUB_SHA"] = self.sha
        self.assertEqual(self.admit()["reason"], "ordinary commit; no committed version cut")


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tag = {"object": {"type": "commit", "sha": SOURCE}}
        self.release = {"tag_name": "v0.1.2", "draft": False, "target_commitish": SOURCE, "assets": []}
        self.identities = {}
        for key in release.PLATFORMS:
            data = json.dumps({"name": "anyagents", "version": "0.1.2", "platform": key,
                               "sha256": "c" * 64,
                               "source": {"repository": release.REPOSITORY, "commit": SOURCE}}).encode()
            url = f"https://api.github.com/repos/{release.REPOSITORY}/releases/assets/{key}"
            self.identities[url] = data
            for suffix, digest in (("tar.gz", "d" * 64),
                                   ("identity.json", hashlib.sha256(data).hexdigest())):
                self.release["assets"].append({"name": f"anyagents-{key}.{suffix}",
                                               "digest": f"sha256:{digest}", "url": url})
        self.npm_source = SOURCE
        self.npm_present = True

    def api(self, path, **kwargs):
        return self.tag if path.startswith("git/ref/") else self.release

    def get(self, url, **kwargs):
        if kwargs.get("binary"):
            return self.identities[url]
        if not self.npm_present:
            return None
        name = url.split("/")[-2].replace("%40", "@").replace("%2F", "/")
        return {"name": name, "version": "0.1.2", "gitHead": self.npm_source,
                "dist": {"integrity": "sha512-value", "tarball": "https://registry.npmjs.org/file"}}

    def inspect(self):
        return release.publication("0.1.2", SOURCE, self.api, self.get)

    def test_complete_partial_and_absent_channels(self):
        self.assertEqual(self.inspect(), {"npm": True, "github": True})
        self.release["assets"].pop()
        self.assertEqual(self.inspect(), {"npm": True, "github": False})
        self.npm_present, self.tag, self.release = False, None, None
        self.assertEqual(self.inspect(), {"npm": False, "github": False})

    def test_other_npm_tag_target_and_sidecar_sources_refused(self):
        self.npm_source = OTHER
        with self.assertRaisesRegex(ValueError, "npm source"):
            self.inspect()
        self.npm_source = SOURCE
        self.tag["object"]["sha"] = OTHER
        with self.assertRaisesRegex(ValueError, "tag source"):
            self.inspect()
        self.tag["object"]["sha"] = SOURCE
        self.release["target_commitish"] = OTHER
        with self.assertRaisesRegex(ValueError, "release target"):
            self.inspect()
        self.release["target_commitish"] = SOURCE
        asset = self.release["assets"][1]
        data = json.loads(self.identities[asset["url"]])
        data["source"]["commit"] = OTHER
        encoded = json.dumps(data).encode()
        self.identities[asset["url"]] = encoded
        asset["digest"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
        with self.assertRaisesRegex(ValueError, "identity source"):
            self.inspect()

    def test_missing_digest_and_corrupt_sidecar_refused(self):
        self.release["assets"][0]["digest"] = ""
        with self.assertRaisesRegex(ValueError, "digest missing"):
            self.inspect()
        self.release["assets"].pop(0)
        self.identities[self.release["assets"][0]["url"]] = b"corrupt"
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            self.inspect()

    def test_redirect_keeps_credentials_on_same_origin_only(self):
        request = release.Request("https://api.github.com/repos/example",
                                  headers={"Authorization": "Bearer synthetic-token"})
        handler = release.OriginBoundRedirect()
        same = handler.redirect_request(request, None, 302, "Found", {},
                                        "https://api.github.com/repos/moved")
        self.assertEqual(same.get_header("Authorization"), "Bearer synthetic-token")
        for target in ("https://release-assets.githubusercontent.com/asset", "http://api.github.com/asset"):
            with self.subTest(target=target):
                redirected = handler.redirect_request(request, None, 302, "Found", {}, target)
                self.assertIsNone(redirected.get_header("Authorization"))

    def test_only_404_is_absence_without_retry(self):
        for code in (403, 429, 500, 404):
            error = HTTPError("https://registry.npmjs.org/test", code, "failed", {}, None)
            with self.subTest(code=code), patch.object(release, "urlopen", side_effect=error) as fetch:
                if code == 404:
                    self.assertIsNone(release.request(error.url, missing=True))
                else:
                    with self.assertRaisesRegex(RuntimeError, f"HTTP {code}"):
                        release.request(error.url, missing=True)
                self.assertEqual(fetch.call_count, 1)


class ReceiptTests(unittest.TestCase):
    def test_success_requires_readback_not_just_green_reusable_workflow(self):
        env = {"ADMISSION_RESULT": "success", "RELEASE_RESULT": "success",
               "ADMISSION": json.dumps({"publish": "true", "version": "0.1.2", "source": SOURCE})}
        with self.assertRaisesRegex(ValueError, "without complete"):
            release.receipt(env, lambda *args: {"npm": True, "github": False})
        self.assertEqual(release.receipt(env, lambda *args: {"npm": True, "github": True})["outcome"], "published")
        env["RELEASE_RESULT"] = "failure"
        with self.assertRaisesRegex(ValueError, "may be partial"):
            release.receipt(env)
        env["ADMISSION_RESULT"] = "failure"
        with self.assertRaisesRegex(ValueError, "not authorized"):
            release.receipt(env)

    def test_noop_never_claims_publication_or_reads_channels(self):
        result = {"publish": "false", "outcome": "no-op", "reason": "ordinary commit"}
        env = {"ADMISSION_RESULT": "success", "RELEASE_RESULT": "skipped", "ADMISSION": json.dumps(result)}
        self.assertEqual(release.receipt(env, lambda *args: self.fail("no publication needed")), result)


if __name__ == "__main__":
    unittest.main()
