#!/usr/bin/env python3
"""Unit tests for tools/ci_scope.py."""

from __future__ import annotations

import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

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
                    event="merge_group",
                )
                self.assertTrue(res.code)
                self.assertEqual(res.rust_scope, f"crates:{pkg}")
                self.assertEqual(res.cargo_pkg_args, f"-p {pkg}")
                self.assertEqual(res.cargo_doc_args, f"-p {pkg}")
                self.assertEqual(res.run_macos, pkg != "anyagents-mcp-web")
                self.assertFalse(res.run_host_acceptance)
                self.assertFalse(res.needs_fake_opencode)

    def test_guest_only_scopes_to_guest_and_coordinator_skips_macos_and_doc_on_bin(self) -> None:
        res = ci_scope.compute_scope(
            REPO_ROOT,
            ["crates/anyagents-guest/src/turn.rs"],
            event="merge_group",
        )
        self.assertTrue(res.code)
        self.assertEqual(res.rust_scope, "crates:anyagents,anyagents-guest")
        self.assertEqual(res.cargo_pkg_args, "-p anyagents -p anyagents-guest")
        # `anyagents` is bin-only so cargo_doc_args must exclude it.
        self.assertEqual(res.cargo_doc_args, "-p anyagents-guest")
        self.assertFalse(res.run_macos)
        self.assertFalse(res.run_host_acceptance)
        self.assertFalse(res.needs_fake_opencode)

    def test_leaf_harness_codex_scopes_to_reverse_dependents_on_both_platforms(self) -> None:
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
        self.assertTrue(res.run_macos)
        self.assertFalse(res.run_host_acceptance)
        self.assertFalse(res.needs_fake_opencode)

    def test_leaf_harness_agy_dev_dep_does_not_trigger_host_acceptance(self) -> None:
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
        self.assertTrue(res.run_macos)
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
            "crates/anyagents/src/cli.rs",
            "crates/anyagents/src/runtime_cli.rs",
            "crates/anyagents/src/sandbox/setup.rs",
            "crates/anyagents/src/local.rs",
            "crates/anyagents/src/vendor/profile.rs",
            "crates/anyagents/src/server/native_signin/identity.rs",
            "crates/anyagents/src/server/file_cache.rs",
            "crates/anyagents/src/server/console/unrecognized.rs",
            "rust-toolchain.toml",
            "tools/ci_scope.py",
            ".cargo/config.toml",
            " .github/workflows/ci.yml",
            "unrecognized\nREADME.md",
            "crates/anyagents-harness/src/lib.rs",
            ".github/workflows/ci.yml",
            "tests/invariants/src/lib.rs",
            "Dockerfile",
        ):
            with self.subTest(trigger=trigger):
                res = ci_scope.compute_scope(REPO_ROOT, [trigger], event="merge_group")
                self.assertTrue(res.code)
                self.assertEqual(res.rust_scope, "workspace")
                self.assertEqual(res.cargo_pkg_args, "--workspace")
                self.assertEqual(res.cargo_doc_args, "--workspace")
                self.assertTrue(res.run_macos)
                self.assertTrue(res.run_host_acceptance)
                self.assertTrue(res.needs_fake_opencode)

    def test_console_and_docs_select_only_console(self) -> None:
        for event in ("pull_request", "merge_group"):
            for paths in (
                ["apps/console/src/main.tsx"],
                ["apps/console/package-lock.json"],
                ["crates/anyagents/src/server/console/index.html"],
                ["apps/console/templates/signin.html", "docs/releases/audit.json", "sylphx.toml"],
            ):
                with self.subTest(event=event, paths=paths):
                    res = ci_scope.compute_scope(REPO_ROOT, paths, event=event)
                    self.assertTrue(res.run_console)
                    self.assertFalse(res.run_npm)
                    self.assertFalse(res.run_macos)
                    self.assertFalse(res.run_host_acceptance)
                    self.assertEqual(res.rust_scope, "none")
                    self.assertEqual(json.loads(res.linux_matrix), {"include": []})

    def test_server_route_keeps_coordinator_but_not_macos(self) -> None:
        for event in ("pull_request", "merge_group"):
            res = ci_scope.compute_scope(REPO_ROOT, [
                "crates/anyagents/src/server/messaging.rs", "docs/architecture-audit.jsonl",
                "apps/console/src/main.tsx",
            ], event=event)
            self.assertEqual(res.rust_scope, "crates:anyagents")
            self.assertEqual(res.run_host_acceptance, event == "merge_group")
            self.assertTrue(res.run_console)
            self.assertFalse(res.run_macos)
            self.assertEqual(res.cargo_doc_args, "")
            expected = ["lint", "coordinator"]
            if event == "merge_group":
                expected.extend(("identity", "host"))
            self.assertEqual(json.loads(res.linux_matrix), {"include": [
                {"lane": lane, "packages": "-p anyagents", "docs": ""}
                for lane in expected
            ]})

    def test_storage_only_keeps_full_linux_including_host(self) -> None:
        paths = ["crates/anyagents-core/migrations_pg/011_example.sql",
                 "crates/anyagents-core/src/db/sqlite_to_pg.rs"]
        res = ci_scope.compute_scope(REPO_ROOT, paths, event="merge_group")
        self.assertEqual(res.rust_scope, "workspace")
        self.assertTrue(res.run_host_acceptance)
        self.assertFalse(res.run_macos)
        self.assertTrue(ci_scope.compute_scope(REPO_ROOT, [*paths, "Cargo.lock"]).run_macos)

    def test_workspace_pr_runs_only_rust_lint_and_unit_lanes(self) -> None:
        for paths, diff_failed in ((["Cargo.lock"], False), ([], True)):
            with self.subTest(paths=paths, diff_failed=diff_failed):
                res = ci_scope.compute_scope(REPO_ROOT, paths, diff_failed=diff_failed)
                self.assertEqual(res.rust_scope, "workspace")
                self.assertTrue(res.run_macos)
                self.assertFalse(res.run_host_acceptance)
                self.assertEqual(res.cargo_doc_args, "")
                self.assertEqual(json.loads(res.linux_matrix), {"include": [
                    {"lane": "lint", "packages": "--workspace", "docs": ""},
                    {"lane": "crates", "packages": "--workspace --exclude anyagents", "docs": ""},
                    {"lane": "coordinator", "packages": "-p anyagents", "docs": ""},
                ]})

    def test_explicit_host_crates_include_macos(self) -> None:
        for name in ("keeper", "agent", "toolchain", "harness-spiron"):
            with self.subTest(name=name):
                path = f"crates/anyagents-{name}/src/lib.rs"
                self.assertTrue(ci_scope.compute_scope(REPO_ROOT, [path]).run_macos)

    def test_push_schedule_dispatch_and_diff_failed_run_full_workspace(self) -> None:
        for event in ("push", "schedule", "workflow_dispatch", "release"):
            res = ci_scope.compute_scope(REPO_ROOT, ["README.md"], event=event)
            self.assertEqual(res.rust_scope, "workspace")
            self.assertTrue(res.run_macos)
            self.assertTrue(res.run_host_acceptance)
            self.assertTrue(res.run_console)
            lanes = {entry["lane"]: entry for entry in json.loads(res.linux_matrix)["include"]}
            self.assertEqual(set(lanes), {"lint", "crates", "coordinator", "identity", "host"})
            self.assertEqual(lanes["crates"]["packages"], "--workspace --exclude anyagents")
            self.assertEqual(lanes["crates"]["docs"], "--workspace")

        failed = ci_scope.compute_scope(REPO_ROOT, [], event="merge_group", diff_failed=True)
        self.assertEqual(failed.rust_scope, "workspace")
        self.assertTrue(failed.run_macos)
        self.assertTrue(failed.run_host_acceptance)


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
            self.assertTrue(res.run_macos)
            self.assertFalse(res.run_host_acceptance)


class GitRangeTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(tmp)
        self.git("init", "-q")
        self.git("config", "user.name", "Fixture")
        self.git("config", "user.email", "fixture@example.org")
        self.git("config", "commit.gpgsign", "false")

    def git(self, *args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def commit(self, path: str, content: str) -> str:
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content, encoding="utf-8")
        self.git("add", "--", path)
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_merge_group_includes_earlier_entry_and_uses_event_base(self) -> None:
        base = self.commit("README.md", "base")
        earlier = self.commit("Cargo.lock", "first queued entry changes the toolchain")
        head = self.commit("docs/new.md", "second queued entry edits docs")
        paths = ci_scope.changed_git_paths(self.root, base, head)
        self.assertEqual(paths, ["Cargo.lock", "docs/new.md"])
        self.assertTrue(ci_scope.compute_scope(self.root, paths, event="merge_group").run_macos)
        docs_only = ci_scope.changed_git_paths(self.root, earlier, head)
        self.assertFalse(ci_scope.compute_scope(self.root, docs_only, event="merge_group").run_macos)

    def test_diff_preserves_path_bytes_and_deletions(self) -> None:
        base = self.commit("Cargo.lock", "base")
        path = " docs/with\nnewline.txt"
        self.git("rm", "-q", "Cargo.lock")
        head = self.commit(path, "still not docs")
        paths = ci_scope.changed_git_paths(self.root, base, head)
        self.assertEqual(paths, [path, "Cargo.lock"])
        self.assertTrue(ci_scope.compute_scope(self.root, paths[:1]).run_macos)
        self.assertTrue(ci_scope.compute_scope(self.root, paths[1:]).run_macos)
        with self.assertRaisesRegex(ValueError, "not the checked-out commit"):
            ci_scope.changed_git_paths(self.root, base, base)

    def test_cli_writes_github_output_and_falls_back_when_range_is_unavailable(self) -> None:
        base = self.commit("README.md", "base")
        head = self.commit("docs/release.json", "{}")
        for candidate, enabled in ((head, "false"), (base, "true"), ("", "true")):
            with self.subTest(candidate=candidate):
                out = self.root / "github_output.txt"
                out.unlink(missing_ok=True)
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    rc = ci_scope.main([
                        "--root", str(self.root), "--event", "merge_group",
                        "--base", base, "--head", candidate, "--github-output", str(out),
                    ])
                self.assertEqual(rc, 0)
                written = dict(line.split("=", 1) for line in out.read_text().splitlines())
                self.assertEqual(written["code"], enabled)
                self.assertEqual(written["run_macos"], enabled)

    def test_purity_refuses_forbidden_text_in_docs_without_rust_build(self) -> None:
        policy = "crates/anyagents/tests/open_source_purity.rs"
        self.commit(policy, (REPO_ROOT / policy).read_text())
        self.commit("docs/a.md", "safe public docs")
        ci_scope.check_purity(self.root)
        self.commit("docs/a.md", "/home/" + "kyle/fixture")
        with self.assertRaisesRegex(ValueError, "docs/a.md:1: hardcoded user home"):
            ci_scope.check_purity(self.root)

    def test_vendor_boundary_blocks_production_but_not_fixtures_or_forwarding(self) -> None:
        policy = "crates/anyagents/tests/open_source_purity.rs"
        self.commit(policy, (REPO_ROOT / policy).read_text())
        signature = '"https://chatgpt.com/backend-api/wham/usage"'
        fixture = f'const URL: &str = {signature};\n'
        for path in (
            "crates/anyagents/tests/oauth_fixture.rs",
            "crates/anyagents/src/server/native_signin/fixture.rs",
            "crates/anyagents-harness-spiron/src/fixture.rs",
            "crates/anyagents/src/serverless_cloud.rs",
            "apps/console/src/accounts/ConnectDialog.tsx",
        ):
            self.commit(path, fixture)
        production = "crates/anyagents/src/forward.rs"
        forwarding = 'fn forward(req: Request) { sink(req.header("user-agent")); }\n'
        with_test = forwarding + "#[cfg(test)]\nmod fixture {\n" + fixture + "}\n"
        self.commit(production, with_test)
        ci_scope.check_purity(self.root)
        self.commit(production, with_test + fixture)
        with self.assertRaisesRegex(ValueError, "forward.rs:6: private Codex API"):
            ci_scope.check_purity(self.root)

    def test_console_assets_reject_missing_templates_and_substitution_markers(self) -> None:
        assets = self.root / ci_scope.CONSOLE_DIR
        assets.mkdir(parents=True)
        ci_scope.check_console_assets(REPO_ROOT)
        with self.assertRaisesRegex(ValueError, "expected exactly"):
            ci_scope.check_console_assets(self.root)
        for asset in ci_scope.CONSOLE_ASSETS:
            (assets / asset).write_text("nonempty asset")
        with self.assertRaisesRegex(ValueError, "missing Rust substitution marker"):
            ci_scope.check_console_assets(self.root)


class ConsoleLockfileTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        assets = self.root / ci_scope.CONSOLE_DIR
        assets.mkdir(parents=True)
        for asset in ci_scope.CONSOLE_ASSETS:
            (assets / asset).write_bytes((REPO_ROOT / ci_scope.CONSOLE_DIR / asset).read_bytes())
        self.lockfile = self.root / "apps/console/package-lock.json"
        self.lockfile.parent.mkdir(parents=True)

    def write_lock(self, resolved: object) -> None:
        # Include a valid entry before the test value so checking only the first
        # resolution cannot make a mixed lockfile pass.
        self.lockfile.write_text(json.dumps({"lockfileVersion": 3, "packages": {
            "": {"name": "fixture"},
            "node_modules/one": {"resolved": "https://registry.npmjs.org/one/-/one-1.0.0.tgz"},
            "node_modules/one/node_modules/two": {"resolved": resolved},
        }}), encoding="utf-8")

    def check_assets(self) -> tuple[int, str]:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = ci_scope.main(["--root", str(self.root), "--check-console-assets"])
        return rc, stderr.getvalue()

    def test_public_npm_resolution(self) -> None:
        self.write_lock("https://registry.npmjs.org/@scope/two/-/two-1.0.0.tgz")
        self.assertEqual(self.check_assets(), (0, ""))

    def test_off_registry_and_invalid_resolutions_fail_closed(self) -> None:
        for value in (
            "https://registry-cache.example/two/-/two-1.0.0.tgz",
            "http://registry.npmjs.org/two.tgz",
            "https://registry.npmjs.org.example/two.tgz",
            "https://registry.npmjs.org@registry-cache.example/two.tgz",
            "https://registry.npmjs.org/",
            "https://registry.npmjs.org/two\n.tgz",
            "file:two.tgz", "", None, ["https://registry.npmjs.org/two.tgz"],
        ):
            with self.subTest(value=value):
                self.write_lock(value)
                rc, stderr = self.check_assets()
                self.assertEqual(rc, 1)
                self.assertIn("resolved URLs must use https://registry.npmjs.org/", stderr)
                self.assertNotIn("registry-cache.example", stderr)

    def test_missing_malformed_and_ambiguous_lockfiles_fail_closed(self) -> None:
        rc, stderr = self.check_assets()
        self.assertEqual(rc, 1)
        self.assertIn("expected regular lockfile", stderr)
        for contents, message in (
            ("{", "Expecting property name"),
            ('{"packages":null}', "expected npm lockfile"),
            ('{"packages":{}, "packages":{}}', "duplicate JSON key"),
        ):
            with self.subTest(contents=contents):
                self.lockfile.write_text(contents, encoding="utf-8")
                rc, stderr = self.check_assets()
                self.assertEqual(rc, 1)
                self.assertIn(message, stderr)


if __name__ == "__main__":
    unittest.main()
