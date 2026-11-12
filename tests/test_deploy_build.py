"""Admission and identity decisions for exact-commit deploy builds."""
import hashlib
import importlib.util
import os
import pathlib
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "deploy_build", pathlib.Path(__file__).resolve().parents[1] / "scripts" / "deploy_build.py")
deploy_build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy_build)

SHA = "a" * 40
REPO = {"full_name": "example/engine"}


def ci_run(**overrides):
    run = {"head_sha": SHA, "path": ".github/workflows/ci.yml", "name": "ci", "event": "push",
           "head_branch": "main", "status": "completed", "conclusion": "success",
           "head_repository": REPO, "repository": REPO}
    run.update(overrides)
    return run


def fake_api(compare=None, runs=None, fail=None):
    compare = compare if compare is not None else {"status": "ahead", "base_commit": {"sha": SHA}}
    runs = runs if runs is not None else {"total_count": 1, "workflow_runs": [ci_run()]}

    def get(path):
        if fail and path.startswith(fail):
            raise deploy_build.Refused(f"cannot read {path}")
        return compare if path.startswith("compare/") else runs
    return get


class AdmitTest(unittest.TestCase):
    def test_admits_main_commit_with_successful_push_ci(self):
        self.assertIsNone(deploy_build.admit(SHA, get=fake_api()))
        self.assertIsNone(deploy_build.admit(
            SHA, get=fake_api(compare={"status": "identical", "base_commit": {"sha": SHA}})))

    def test_refuses_short_or_uppercase_sha(self):
        for bad in ("a" * 39, "A" * 40, "main", "", None):
            with self.assertRaises(deploy_build.Refused):
                deploy_build.admit(bad, get=fake_api())

    def test_refuses_commit_not_on_main(self):
        for status in ("behind", "diverged", None):
            with self.assertRaises(deploy_build.Refused):
                deploy_build.admit(SHA, get=fake_api(compare={"status": status, "base_commit": {"sha": SHA}}))

    def test_refuses_compare_bound_to_another_commit(self):
        with self.assertRaises(deploy_build.Refused):
            deploy_build.admit(SHA, get=fake_api(compare={"status": "ahead", "base_commit": {"sha": "b" * 40}}))

    def test_refuses_without_own_successful_push_main_ci(self):
        for run in (ci_run(conclusion="failure"), ci_run(status="in_progress", conclusion=None),
                    ci_run(event="pull_request"), ci_run(event="merge_group"), ci_run(head_branch="feature"),
                    ci_run(head_sha="b" * 40), ci_run(name="release"), ci_run(path=".github/workflows/x.yml"),
                    ci_run(head_repository={"full_name": "fork/engine"})):
            with self.subTest(run=run), self.assertRaises(deploy_build.Refused):
                deploy_build.admit(SHA, get=fake_api(runs={"total_count": 1, "workflow_runs": [run]}))

    def test_unreadable_or_incomplete_inventory_refuses(self):
        with self.assertRaises(deploy_build.Refused):
            deploy_build.admit(SHA, get=fake_api(fail="actions/"))
        with self.assertRaises(deploy_build.Refused):
            deploy_build.admit(SHA, get=fake_api(fail="compare/"))
        with self.assertRaises(deploy_build.Refused):
            deploy_build.admit(SHA, get=fake_api(runs={"workflow_runs": [ci_run()]}))
        with self.assertRaises(deploy_build.Refused):
            deploy_build.admit(SHA, get=fake_api(runs={"total_count": 101, "workflow_runs": [ci_run()]}))


class IdentityTest(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp()
        os.write(handle, b"binary bytes")
        os.close(handle)
        self.digest = hashlib.sha256(b"binary bytes").hexdigest()
        self.addCleanup(os.unlink, self.path)

    def healthz(self, **overrides):
        value = {"ok": True, "version": "1.2.3", "source_sha": SHA,
                 "binary_sha256": self.digest, "binary_sha256_state": "ready"}
        value.update(overrides)
        return value

    def test_records_commit_version_and_digest(self):
        record = deploy_build.identity(SHA, self.path, self.healthz(), "1.2.3")
        self.assertEqual(record, {"asset": "anyagents-linux-x64-gnu", "commit": SHA, "version": "1.2.3",
                                  "sha256": self.digest, "tag": "deploy-" + SHA})

    def test_refuses_mismatched_serving_identity(self):
        for bad in (self.healthz(source_sha="b" * 40), self.healthz(source_sha="unknown"),
                    self.healthz(binary_sha256="0" * 64), self.healthz(binary_sha256_state="computing"),
                    self.healthz(binary_sha256_state="failed", binary_sha256=None),
                    self.healthz(version="1.2.4"), self.healthz(ok=False)):
            with self.subTest(healthz=bad), self.assertRaises(deploy_build.Refused):
                deploy_build.identity(SHA, self.path, bad, "1.2.3")


if __name__ == "__main__":
    unittest.main()
