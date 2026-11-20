#!/usr/bin/env python3
"""Unit tests for tools/module_size_scan.py."""

from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import module_size_scan as scanner


def _write_lines(path: Path, count: int, line: str = "fn x() {}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(line for _ in range(count)) + "\n", encoding="utf-8")


class RustProductionLineCounterTests(unittest.TestCase):
    def test_strips_inline_cfg_test_and_preserves_trailing_prod_code(self) -> None:
        src = "\n".join(
            [
                "pub fn alpha() -> usize { 1 }",
                "#[cfg(test)]",
                "mod early_tests {",
                '    const RAW: &str = r#" } not closing "#;',
                "    const CH: char = '}';",
                "    /* nested /* } */ comment } */",
                "    #[test]",
                "    fn t() { assert_eq!(1, 1); }",
                "}",
                "pub fn beta() -> usize { 2 }",
                '#[cfg(all(test, not(target_os = "linux")))]',
                "mod non_linux_tests {",
                "    #[test]",
                "    fn u() {}",
                "}",
            ]
        )
        # Only `pub fn alpha` and `pub fn beta` are production lines.
        self.assertEqual(scanner.count_rust_production_lines(src), 2)

    def test_does_not_strip_cfg_any_test_production_modules(self) -> None:
        src = "\n".join(
            [
                "pub fn prod() {}",
                '#[cfg(any(test, not(target_os = "linux")))]',
                "mod fallback {",
                "    pub fn stub() {}",
                "}",
            ]
        )
        self.assertEqual(scanner.count_rust_production_lines(src), 5)

    def test_is_cfg_test_attribute_variants(self) -> None:
        self.assertTrue(scanner.is_cfg_test_attribute("#[cfg(test)]"))
        self.assertTrue(scanner.is_cfg_test_attribute("#[cfg(all(test, unix))]"))
        self.assertTrue(
            scanner.is_cfg_test_attribute('#[cfg(all(unix, test, not(target_os = "linux")))]')
        )
        self.assertFalse(scanner.is_cfg_test_attribute("#[cfg(not(test))]"))
        self.assertFalse(
            scanner.is_cfg_test_attribute('#[cfg(any(test, not(target_os = "linux")))]')
        )
        self.assertFalse(scanner.is_cfg_test_attribute('#[cfg(target_os = "linux")]'))


class ModuleSizeRatchetTests(unittest.TestCase):
    def _make_repo(self) -> tuple[tempfile.TemporaryDirectory[str], Path, Path]:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        baseline = root / "tools" / "module_size_baseline.json"
        return tmp, root, baseline

    def test_pass_under_floor_and_at_baseline_and_excludes_test_files(self) -> None:
        tmp, root, baseline = self._make_repo()
        self.addCleanup(tmp.cleanup)

        _write_lines(root / "crates/demo/src/small.rs", 1500)
        _write_lines(root / "crates/demo/src/legacy.rs", 1800)
        _write_lines(root / "crates/demo/assets/ui.js", 1200, line="const x = 1;")
        # Excluded paths over 1500 lines must never fail.
        _write_lines(root / "crates/demo/tests/integration.rs", 2500)
        _write_lines(root / "crates/demo/src/foo_tests.rs", 2500)
        _write_lines(root / "crates/demo/src/tests.rs", 2500)
        _write_lines(root / "crates/demo/benches/perf.rs", 2500)
        _write_lines(root / "crates/demo/examples/demo.rs", 2500)
        _write_lines(root / "crates/demo/fixtures/big.js", 2500, line="const f = 1;")
        _write_lines(root / "crates/demo/assets/app.test.js", 2500, line="const t = 1;")
        _write_lines(root / "crates/demo/src/test_helper.py", 2500, line="x = 1")

        # File with 1400 prod lines + 500 inline test lines passes without baseline.
        mixed = "\n".join(["fn p() {}"] * 1400 + ["#[cfg(test)]", "mod tests {"] + ["    fn t() {}"] * 500 + ["}"])
        (root / "crates/demo/src/mixed.rs").write_text(mixed + "\n", encoding="utf-8")

        scanner.write_baseline(baseline, {"crates/demo/src/legacy.rs": 1800})
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = scanner.main(["--root", str(root), "--baseline", str(baseline)])
        self.assertEqual(rc, 0, buf.getvalue())
        self.assertIn("PASS:", buf.getvalue())

    def test_growth_violation_on_baselined_file_fails(self) -> None:
        tmp, root, baseline = self._make_repo()
        self.addCleanup(tmp.cleanup)

        _write_lines(root / "crates/demo/src/legacy.rs", 1805)
        scanner.write_baseline(baseline, {"crates/demo/src/legacy.rs": 1800})

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = scanner.main(["--root", str(root), "--baseline", str(baseline)])
        self.assertEqual(rc, 1)
        out = buf.getvalue()
        self.assertIn("GREW past baseline", out)
        self.assertIn("crates/demo/src/legacy.rs", out)
        self.assertIn("+5", out)

    def test_new_file_over_1500_fails_for_rust_and_js(self) -> None:
        tmp, root, baseline = self._make_repo()
        self.addCleanup(tmp.cleanup)

        _write_lines(root / "crates/demo/src/new_monolith.rs", 1501)
        _write_lines(root / "crates/demo/assets/new_bundle.js", 1502, line="let a = 1;")
        scanner.write_baseline(baseline, {})

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = scanner.main(["--root", str(root), "--baseline", str(baseline)])
        self.assertEqual(rc, 1)
        out = buf.getvalue()
        self.assertIn("NEW oversized file: crates/demo/src/new_monolith.rs", out)
        self.assertIn("NEW oversized file: crates/demo/assets/new_bundle.js", out)

    def test_shrink_pass_and_update_baseline_locks_in_improvement(self) -> None:
        tmp, root, baseline = self._make_repo()
        self.addCleanup(tmp.cleanup)

        _write_lines(root / "crates/demo/src/shrunk_still_over.rs", 1600)
        _write_lines(root / "crates/demo/src/shrunk_under.rs", 1450)
        scanner.write_baseline(
            baseline,
            {
                "crates/demo/src/shrunk_still_over.rs": 1900,
                "crates/demo/src/shrunk_under.rs": 1700,
                "crates/demo/src/deleted.rs": 1600,
            },
        )

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = scanner.main(["--root", str(root), "--baseline", str(baseline)])
        self.assertEqual(rc, 0, buf.getvalue())
        out = buf.getvalue()
        self.assertIn("SHRUNK: crates/demo/src/shrunk_still_over.rs: 1900 -> 1600", out)
        self.assertIn("CLEARED: crates/demo/src/shrunk_under.rs: 1700 -> 1450", out)
        self.assertIn("CLEARED: crates/demo/src/deleted.rs: 1600 -> removed", out)

        # --update-baseline shrinks and clears entries.
        upd = io.StringIO()
        with redirect_stdout(upd):
            rc_upd = scanner.main(
                ["--root", str(root), "--baseline", str(baseline), "--update-baseline"]
            )
        self.assertEqual(rc_upd, 0)
        saved = json.loads(baseline.read_text(encoding="utf-8"))
        self.assertEqual(saved["files"], {"crates/demo/src/shrunk_still_over.rs": 1600})

    def test_missing_baseline_exits_nonzero(self) -> None:
        tmp, root, baseline = self._make_repo()
        self.addCleanup(tmp.cleanup)

        err = io.StringIO()
        with redirect_stderr(err):
            rc = scanner.main(["--root", str(root), "--baseline", str(baseline)])
        self.assertEqual(rc, 2)
        self.assertIn("baseline file not found", err.getvalue())

    def test_live_repository_passes_against_checked_in_baseline(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = scanner.main([])
        self.assertEqual(rc, 0, buf.getvalue())


if __name__ == "__main__":
    unittest.main()
