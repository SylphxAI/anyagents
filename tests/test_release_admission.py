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

    def test_parent_cargo_already_at_cut_version_accepted_other_mismatch_refused(self):
        self.files["HEAD^1"]["Cargo.toml"] = '[workspace.package]\nversion = "0.1.2"\n'
        self.assertEqual(self.cut(), "0.1.2")
        self.files["HEAD^1"]["Cargo.toml"] = '[workspace.package]\nversion = "0.1.9"\n'
        with self.assertRaisesRegex(ValueError, "Cargo/npm version mismatch"):
            self.cut()

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
    """Reconcile real first-parent Git history; registry writes are simulated."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.old_cwd = Path.cwd()
        os.chdir(self.temporary.name)
        subprocess.run(["git", "init", "-q"], check=True)
        release.git("config", "user.name", "Test")
        release.git("config", "user.email", "test@example.com")
        self.commit("0.1.1", "initial")
        self.sha = self.commit("0.1.2", "release: v0.1.2 (#90)")
        self.live_main = self.sha
        self.runs = {self.sha: [proof(self.sha)]}
        self.published = set()
        self.reads = []
        self.env = {
            "GITHUB_REPOSITORY": release.REPOSITORY, "GITHUB_REF": "refs/heads/main",
            "GITHUB_SHA": self.sha, "GITHUB_EVENT_NAME": "workflow_run",
        }
        self.event = {"repository": {"visibility": "public"}, "workflow_run": self.runs[self.sha][0]}

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
        return release.git("rev-parse", "HEAD")

    def advance(self, version, subject):
        self.live_main = self.commit(version, subject)
        self.env["GITHUB_SHA"] = self.live_main
        self.runs[self.live_main] = [{**proof(self.live_main), "id": 124}]
        self.event["workflow_run"] = self.runs[self.live_main][0]
        return self.live_main

    def api(self, path):
        self.reads.append(path)
        if path == f"actions/workflows/{release.CI['id']}":
            return release.CI
        if path == "git/ref/heads/main":
            return {"object": {"sha": self.live_main}}
        if path.startswith("compare/"):
            base, head = path.removeprefix("compare/").split("...")
            try:
                merge_base = release.git("merge-base", base, head)
            except subprocess.CalledProcessError:
                merge_base = OTHER
            return {"status": "identical" if base == head else "ahead" if merge_base == base else "diverged",
                    "base_commit": {"sha": base}, "merge_base_commit": {"sha": merge_base}}
        if path.startswith(f"actions/workflows/{release.CI['id']}/runs?"):
            sha = path.split("head_sha=")[1].split("&")[0]
            values = self.runs.get(sha, [])
            return {"total_count": len(values), "workflow_runs": values}
        self.fail(f"unexpected API request: {path}")

    def inspect(self, version, sha):
        done = (version, sha) in self.published
        return {"npm": done, "github": done}

    def admit(self):
        return release.admit(self.event, self.env, self.api, self.inspect)

    def test_new_cut_admits_and_complete_release_is_explicit_noop(self):
        result = self.admit()
        self.assertEqual(result["publish"], "true")
        self.assertEqual(result["releases"], [{"version": "0.1.2", "source": self.sha, "ci_run_id": "123"}])
        self.published.add(("0.1.2", self.sha))
        self.reads.clear()
        self.assertEqual(self.admit()["publish"], "false")
        self.assertFalse(any("/runs?" in path for path in self.reads))

    def test_later_main_and_trigger_sha_never_replace_the_release_source(self):
        later = self.advance("0.1.2", "docs: setup")
        result = self.admit()
        self.assertEqual(result["main"], later)
        self.assertEqual([item["source"] for item in result["releases"]], [self.sha])
        self.assertEqual(result["releases"][0]["ci_run_id"], "123")

    def test_evicted_first_event_two_quick_bumps_and_idempotent_rerun(self):
        # No first-event handler runs: its pending workflow was evicted. Only
        # the second event survives, and state reconciliation catches both cuts.
        second = self.advance("0.1.3", "release: v0.1.3 (#123)")
        result = self.admit()
        with tempfile.TemporaryDirectory() as output_dir:
            output = Path(output_dir) / "outputs"
            release.report(result, {"GITHUB_OUTPUT": str(output)})
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            matrix = json.loads(values["matrix"])["include"]
        published = []
        for item in matrix:
            published.append((item["version"], item["source"]))
            self.published.add((item["version"], item["source"]))
        self.assertEqual(published, [("0.1.2", self.sha), ("0.1.3", second)])
        self.assertEqual([item["ci_run_id"] for item in matrix], ["123", "124"])
        self.assertEqual(release.receipt({"ADMISSION_RESULT": "success", "RELEASE_RESULT": "success",
                                         "ADMISSION": values["admission"]}, self.inspect)["outcome"], "published")
        again = self.admit()
        self.assertEqual(again["releases"], [])
        self.assertEqual(again["publish"], "false")
        self.assertEqual(again["refused"], [])

    def test_red_cut_refuses_even_with_a_green_later_main(self):
        self.runs[self.sha][0]["conclusion"] = "failure"
        self.advance("0.1.2", "fix: later main is green")
        result = self.admit()
        self.assertEqual(result["releases"], [])
        self.assertEqual(result["refused"][0]["source"], self.sha)
        self.assertIn("123:failure", result["refused"][0]["reason"])
        with self.assertRaisesRegex(ValueError, "release sources refused.*exact-source"):
            release.receipt({"ADMISSION_RESULT": "success", "RELEASE_RESULT": "skipped",
                             "ADMISSION": json.dumps(result)}, self.inspect)

    def test_red_cut_does_not_authorize_or_prevent_an_independent_green_cut(self):
        self.runs[self.sha][0]["conclusion"] = "failure"
        second = self.advance("0.1.3", "release: v0.1.3")
        result = self.admit()
        self.assertEqual([item["source"] for item in result["releases"]], [second])
        self.assertEqual([item["source"] for item in result["refused"]], [self.sha])
        self.published.add(("0.1.3", second))
        with self.assertRaisesRegex(ValueError, "release sources refused.*123:failure"):
            release.receipt({"ADMISSION_RESULT": "success", "RELEASE_RESULT": "success",
                             "ADMISSION": json.dumps(result)}, self.inspect)

    def test_manual_and_failed_trigger_only_reconcile_own_source_proofs(self):
        self.env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        self.event["inputs"] = {"ci_run_id": "unrelated-input-is-not-authority"}
        self.assertEqual(self.admit()["publish"], "true")
        self.env["GITHUB_EVENT_NAME"] = "workflow_run"
        self.event["workflow_run"] = {**proof(OTHER), "conclusion": "failure"}
        self.assertEqual(self.admit()["releases"][0]["source"], self.sha)
        self.runs[self.sha][0]["event"] = "workflow_dispatch"
        self.assertIn("event", self.admit()["refused"][0]["reason"])

    def test_missing_pending_and_incomplete_source_proofs_refuse(self):
        for runs in ([], [{**proof(self.sha), "status": "in_progress", "conclusion": None}]):
            with self.subTest(runs=runs):
                self.runs[self.sha] = runs
                result = self.admit()
                self.assertEqual(result["releases"], [])
                self.assertIn("exact-source", result["refused"][0]["reason"])
        original_api = self.api
        def incomplete(path):
            value = original_api(path)
            return {**value, "total_count": 101} if "/runs?" in path else value
        result = release.admit(self.event, self.env, incomplete, self.inspect)
        self.assertIn("incomplete source CI inventory", result["refused"][0]["reason"])

    def test_snapshot_can_lag_live_main_but_cannot_leave_its_ancestry(self):
        later = self.advance("0.1.2", "docs: later main")
        release.git("checkout", "--detach", self.sha)
        self.assertEqual(self.admit()["releases"][0]["source"], self.sha)
        self.assertIn(f"compare/{self.sha}...{later}", self.reads)
        release.git("checkout", "--orphan", "unrelated")
        self.commit("0.1.2", "initial unrelated")
        with self.assertRaisesRegex(ValueError, "not an ancestor"):
            self.admit()

    def test_first_parent_merge_is_the_version_introducing_source(self):
        original = self.sha
        release.git("checkout", "-b", "feature")
        feature = self.commit("0.1.3", "release: v0.1.3")
        release.git("checkout", "--detach", original)
        release.git("merge", "--no-ff", "-m", "release: v0.1.3 (#91)", feature)
        merged = release.git("rev-parse", "HEAD")
        self.live_main = merged
        self.runs[merged] = [{**proof(merged), "id": 125}]
        result = self.admit()
        self.assertEqual([item["source"] for item in result["releases"]], [original, merged])
        self.assertNotIn(feature, [item["source"] for item in result["releases"]])

    def test_existing_provenance_or_digest_mismatch_is_a_loud_refusal(self):
        def mismatch(*args):
            raise ValueError("GitHub identity digest mismatch")
        result = release.admit(self.event, self.env, self.api, mismatch)
        self.assertEqual(result["releases"], [])
        self.assertIn("digest mismatch", result["refused"][0]["reason"])
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            release.receipt({"ADMISSION_RESULT": "success", "RELEASE_RESULT": "skipped",
                             "ADMISSION": json.dumps(result)}, self.inspect)

    def test_complete_legacy_cut_is_noop_but_cannot_authorize_new_publication(self):
        release.git("commit", "--amend", "-qm", "chore: release AnyAgents 0.1.2")
        legacy = release.git("rev-parse", "HEAD")
        self.live_main = legacy
        self.published.add(("0.1.2", legacy))
        self.assertEqual(self.admit()["complete"], [{"version": "0.1.2", "source": legacy}])
        self.published.clear()
        self.assertIn("release-cut commit", self.admit()["refused"][0]["reason"])

    def test_initial_launcher_added_after_root_is_not_an_unreleased_bump(self):
        # Real history introduced the launcher after the repository's first
        # commit. Neither that initial version nor a root is a version cut.
        release.git("checkout", "--orphan", "launcher-later")
        release.git("rm", "-rf", ".")
        release.git("commit", "--allow-empty", "-qm", "repository root")
        self.live_main = self.commit("0.1.1", "add launcher")
        result = self.admit()
        self.assertEqual(result["releases"], [])
        self.assertEqual(result["refused"], [])

    def test_ancestry_response_must_name_the_snapshot_as_base_and_merge_base(self):
        original_api = self.api
        for key in ("base_commit", "merge_base_commit"):
            def wrong(path):
                value = original_api(path)
                return {**value, key: {"sha": OTHER}} if path.startswith("compare/") else value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "not an ancestor"):
                release.admit(self.event, self.env, wrong, self.inspect)

    def test_reintroduced_version_cannot_borrow_its_original_publication(self):
        original = self.sha
        second = self.advance("0.1.3", "release: v0.1.3")
        self.published.update((("0.1.2", original), ("0.1.3", second)))
        repeated = self.advance("0.1.2", "release: v0.1.2")
        result = self.admit()
        self.assertEqual(result["releases"], [])
        self.assertEqual(result["refused"], [{"version": "0.1.2", "source": repeated,
                                             "reason": "version introduced more than once on main"}])

    def test_shallow_history_refuses_instead_of_missing_an_older_cut(self):
        with tempfile.TemporaryDirectory() as clone:
            subprocess.run(["git", "clone", "-q", "--depth", "1", Path.cwd().as_uri(), clone], check=True)
            os.chdir(clone)
            try:
                with self.assertRaisesRegex(ValueError, "complete main history"):
                    self.admit()
            finally:
                os.chdir(self.temporary.name)

    def test_public_main_required_including_missing_visibility(self):
        for visibility in (None, "private", "internal"):
            self.event["repository"] = {"visibility": visibility}
            with self.subTest(visibility=visibility), self.assertRaisesRegex(ValueError, "public"):
                self.admit()
        self.event["repository"]["visibility"] = "public"
        self.env["GITHUB_REF"] = "refs/heads/feature"
        with self.assertRaisesRegex(ValueError, "main ref"):
            self.admit()


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

    def test_request_uses_each_providers_media_type_and_credential(self):
        cases = (
            ("https://registry.npmjs.org/package/0.1.2", False, "application/json", None),
            ("https://api.github.com/repos/example", False, "application/vnd.github+json", "Bearer synthetic-token"),
            ("https://api.github.com/repos/example/releases/assets/1", True, "application/octet-stream", "Bearer synthetic-token"),
        )
        for url, binary, accept, credential in cases:
            with self.subTest(url=url), patch.dict(os.environ, {"GH_TOKEN": "synthetic-token"}), \
                    patch.object(release, "urlopen") as fetch:
                fetch.return_value.__enter__.return_value.read.return_value = b'{}'
                release.request(url, binary=binary)
                sent = fetch.call_args.args[0]
                self.assertEqual(sent.get_header("Accept"), accept)
                self.assertEqual(sent.get_header("Authorization"), credential)

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
               "ADMISSION": json.dumps({"publish": "true", "releases": [{"version": "0.1.2", "source": SOURCE}]})}
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
