#!/usr/bin/env python3
"""Unit tests for tools/ci_scope.py."""

from __future__ import annotations

import io
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import ci_scope


class RealWorkspaceScopeTests(unittest.TestCase):
    def test_docs_agents_md_prebuilt_and_tools_only_skip_rust_and_macos(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            [
                "AGENTS.md",
                "README.md",
                "docs/architecture.md",
                "Dockerfile.prebuilt",
                "sylphx.toml",
                "tools/module_size_scan.py",
                "tools/module_size_baseline.json",
                "scripts/deploy_build.py",
                "tests/test_deploy_build.py",
            ],
            event="pull_request",
        )
        self.assertFalse(res.code)
        self.assertEqual(res.rust_scope, "none")
        self.assertEqual(res.cargo_pkg_args, "")
        self.assertFalse(res.run_macos)
        self.assertFalse(res.run_host_acceptance)
        self.assertFalse(res.run_npm)

    def test_npm_only_skips_rust_and_macos_but_runs_npm(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["packages/anyagents/bin/anyagents.js", "package.json"],
            event="pull_request",
        )
        self.assertFalse(res.code)
        self.assertEqual(res.rust_scope, "none")
        self.assertFalse(res.run_macos)
        self.assertTrue(res.run_npm)

    def test_standalone_mcp_crates_in_real_workspace_scope_to_single_crate(self) -> None:
        for pkg in (
            "anyagents-mcp-browser",
            "anyagents-mcp-web",
            "anyagents-mcp-computer",
        ):
            with self.subTest(pkg=pkg):
                res = ci_scope.compute_scope(
                    REPO_ROOT,
                    [f"crates/{pkg}/src/server.rs"],
                    event="pull_request",
                )
                self.assertTrue(res.code)
                self.assertEqual(res.rust_scope, f"crates:{pkg}")
                self.assertEqual(res.cargo_pkg_args, f"-p {pkg}")
                self.assertEqual(res.cargo_doc_args, f"-p {pkg}")
                self.assertFalse(res.run_macos)
                self.assertFalse(res.run_host_acceptance)
                self.assertFalse(res.needs_fake_opencode)

    def test_guest_only_scopes_to_guest_and_coordinator_skips_macos_and_doc_on_bin(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-guest/src/turn.rs"],
            event="pull_request",
        )
        self.assertTrue(res.code)
        self.assertEqual(res.rust_scope, "crates:anyagents,anyagents-guest")
        self.assertEqual(res.cargo_pkg_args, "-p anyagents -p anyagents-guest")
        # `anyagents` is bin-only so cargo_doc_args must exclude it.
        self.assertEqual(res.cargo_doc_args, "-p anyagents-guest")
        self.assertFalse(res.run_macos)
        self.assertFalse(res.run_host_acceptance)
        self.assertFalse(res.needs_fake_opencode)

    def test_leaf_harness_codex_scopes_to_reverse_dependents_and_skips_macos(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-harness-codex/src/lib.rs"],
            event="merge_group",
        )
        self.assertTrue(res.code)
        self.assertEqual(
            res.rust_scope,
            "crates:anyagents,anyagents-guest,anyagents-harness-codex,anyagents-invariants",
        )
        self.assertEqual(
            res.cargo_doc_args,
            "-p anyagents-guest -p anyagents-harness-codex -p anyagents-invariants",
        )
        self.assertFalse(res.run_macos)
        self.assertFalse(res.run_host_acceptance)
        self.assertFalse(res.needs_fake_opencode)

    def test_leaf_harness_agy_dev_dep_does_not_trigger_macos_or_host_acceptance(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-harness-agy/src/lib.rs"],
            event="pull_request",
        )
        self.assertTrue(res.code)
        self.assertEqual(
            res.rust_scope,
            "crates:anyagents,anyagents-agent,anyagents-guest,anyagents-harness-agy,anyagents-invariants",
        )
        self.assertFalse(res.run_macos)
        self.assertFalse(res.run_host_acceptance)

    def test_opencode_harness_enables_macos_and_fake_opencode_helper(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-harness-opencode/src/lib.rs"],
            event="pull_request",
        )
        self.assertTrue(res.code)
        self.assertEqual(
            res.rust_scope,
            "crates:anyagents,anyagents-guest,anyagents-harness-opencode,anyagents-invariants",
        )
        self.assertTrue(res.run_macos)
        self.assertTrue(res.needs_fake_opencode)
        self.assertFalse(res.run_host_acceptance)

    def test_installer_runs_scoped_crates_on_linux_and_macos(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-installer/src/platform.rs"],
            event="pull_request",
        )
        self.assertTrue(res.code)
        self.assertEqual(res.rust_scope, "crates:anyagents,anyagents-installer")
        self.assertTrue(res.run_macos)
        self.assertFalse(res.run_host_acceptance)

    def test_core_coordinator_manifest_and_ci_yml_trigger_full_workspace(self) -> None:
        for trigger in (
            "Cargo.toml",
            "Cargo.lock",
            "crates/anyagents-core/src/store.rs",
            "crates/anyagents/src/server.rs",
            "crates/anyagents-harness/src/lib.rs",
            ".github/workflows/ci.yml",
            "tests/invariants/src/lib.rs",
            "Dockerfile",
        ):
            with self.subTest(trigger=trigger):
                res = ci_scope.compute_scope(REPO_ROOT, [trigger], event="pull_request")
                self.assertTrue(res.code)
                self.assertEqual(res.rust_scope, "workspace")
                self.assertEqual(res.cargo_pkg_args, "--workspace")
                self.assertEqual(res.cargo_doc_args, "--workspace")
                self.assertTrue(res.run_macos)
                self.assertTrue(res.run_host_acceptance)
                self.assertTrue(res.needs_fake_opencode)

    def test_push_workflow_dispatch_and_diff_failed_run_full_workspace(self) -> None:
        for event in ("push", "workflow_dispatch"):
            res = ci_scope.compute_scope(REPO_ROOT, ["README.md"], event=event)
            self.assertEqual(res.rust_scope, "workspace")
            self.assertTrue(res.run_macos)
            self.assertTrue(res.run_host_acceptance)

        failed = ci_scope.compute_scope(REPO_ROOT, [], event="pull_request", diff_failed=True)
        self.assertEqual(failed.rust_scope, "workspace")
        self.assertTrue(failed.run_macos)


class SyntheticWorkspaceScopeTests(unittest.TestCase):
    def test_standalone_mcp_crate_scopes_to_single_crate_and_handles_bin_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text(
                '[workspace]\nmembers = ["crates/anyagents-mcp-browser", "crates/anyagents"]\n',
                encoding="utf-8",
            )
            mcp = root / "crates" / "anyagents-mcp-browser"
            (mcp / "src").mkdir(parents=True)
            (mcp / "Cargo.toml").write_text(
                '[package]\nname = "anyagents-mcp-browser"\nversion = "0.1.0"\n',
                encoding="utf-8",
            )
            (mcp / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")

            coord = root / "crates" / "anyagents"
            (coord / "src").mkdir(parents=True)
            (coord / "Cargo.toml").write_text(
                '[package]\nname = "anyagents"\nversion = "0.1.0"\n',
                encoding="utf-8",
            )
            (coord / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")

            res = ci_scope.compute_scope(
                root,
                ["crates/anyagents-mcp-browser/src/main.rs"],
                event="pull_request",
            )
            self.assertTrue(res.code)
            self.assertEqual(res.rust_scope, "crates:anyagents-mcp-browser")
            self.assertEqual(res.cargo_pkg_args, "-p anyagents-mcp-browser")
            # Bin-only standalone crate has empty cargo_doc_args.
            self.assertEqual(res.cargo_doc_args, "")
            self.assertFalse(res.run_macos)
            self.assertFalse(res.run_host_acceptance)

    def test_cli_writes_github_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths_file = Path(tmp) / "scope.txt"
            gh_out = Path(tmp) / "github_output.txt"
            paths_file.write_text("AGENTS.md\ntools/module_size_scan.py\n", encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = ci_scope.main(
                    [
                        "--root",
                        str(REPO_ROOT),
                        "--event",
                        "pull_request",
                        "--paths-file",
                        str(paths_file),
                        "--github-output",
                        str(gh_out),
                    ]
                )
            self.assertEqual(rc, 0)
            written = dict(
                line.split("=", 1)
                for line in gh_out.read_text(encoding="utf-8").splitlines()
                if "=" in line
            )
            self.assertEqual(written["code"], "false")
            self.assertEqual(written["rust_scope"], "none")
            self.assertEqual(written["run_macos"], "false")


if __name__ == "__main__":
    unittest.main()
