#!/usr/bin/env python3
"""Production module size scanner and baseline ratchet for SylphxAI/anyagents.

Counting rule (enforced identically locally and in CI):
- Scans shipped production source files under `crates/` with extensions:
  `.rs`, `.ts`, `.tsx`, `.js`, `.jsx`, `.mjs`, `.cjs`, `.py`.
- Excludes any path containing a directory in:
  `tests`, `benches`, `examples`, `fixtures`, `target`, `node_modules`, `__pycache__`.
- Excludes dedicated test files:
  `tests.rs`, `test.rs`, `*_tests.rs`, `*_test.rs`, `test_*.py`, `*_test.py`,
  `*.test.*`, `*.spec.*`.
- For non-Rust files, counts all lines (`len(text.splitlines())`).
- For Rust (`.rs`) files, counts production lines: total lines minus top-level
  inline `#[cfg(test)]` / `#[cfg(all(..., test, ...))]` `mod <name> { ... }`
  blocks, matched lexically (ignoring comments, strings, raw strings, and char
  literals). Production code appearing after an inline test module is still
  counted.

Ratchet policy:
- `FLOOR = 1500` production lines.
- Any scanned file not listed in `tools/module_size_baseline.json` with
  `lines > FLOOR` fails immediately.
- Any legacy file listed in `tools/module_size_baseline.json` with
  `lines > baseline[path]` fails immediately; legacy files may only shrink.
- Run `--update-baseline` to regenerate `tools/module_size_baseline.json` after
  shrinking or removing legacy oversized files.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys

FLOOR = 1500
SCAN_ROOTS = ("crates",)
SOURCE_EXTENSIONS = frozenset(
    {".rs", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py"}
)
EXCLUDED_DIRS = frozenset(
    {
        "tests",
        "benches",
        "examples",
        "fixtures",
        "target",
        "node_modules",
        "__pycache__",
    }
)
COUNTING_RULE = (
    "shipped production source lines under crates/ (excluding test/bench/example/"
    "fixture/target dirs, dedicated test files, and top-level Rust #[cfg(test)] "
    "inline mod blocks)"
)
DEFAULT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASELINE = Path(__file__).resolve().parent / "module_size_baseline.json"

# Lexical token scanner for Rust brace matching. Order matters: comments and
# literals are matched before bare `{` / `}` characters.
_RUST_TOKEN_RE = re.compile(
    r"""
      (?P<line_comment>//[^\n]*)
    | (?P<block_open>/\*)
    | (?P<raw_str>(?<![A-Za-z0-9_])(?:[bc])?r(?P<hashes>\#*)")
    | (?P<str>(?<![A-Za-z0-9_])(?:[bc])?"(?:\\.|[^"\\])*")
    | (?P<char>(?<![A-Za-z0-9_])b?'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]+\}|.)|[^'\\\n])')
    | (?P<lbrace>\{)
    | (?P<rbrace>\})
    """,
    re.VERBOSE | re.DOTALL,
)

_MOD_DECL_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+[A-Za-z_][A-Za-z0-9_]*\s*(\{)?\s*(?://.*)?$"
)


def is_excluded_path(rel_path: Path) -> bool:
    """Return True when `rel_path` (relative to repo root) is non-production."""
    if any(part in EXCLUDED_DIRS for part in rel_path.parts[:-1]):
        return True
    name = rel_path.name
    stem = rel_path.stem
    if (
        name in ("tests.rs", "test.rs")
        or stem.endswith("_tests")
        or stem.endswith("_test")
        or stem.startswith("test_")
        or ".test." in name
        or ".spec." in name
    ):
        return True
    return False


def _split_top_level_commas(expr: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for idx, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return []
        elif ch == "," and depth == 0:
            parts.append(expr[start:idx].strip())
            start = idx + 1
    if depth != 0:
        return []
    parts.append(expr[start:].strip())
    return parts


def is_cfg_test_attribute(line: str) -> bool:
    """Return True for `#[cfg(test)]` or `#[cfg(all(..., test, ...))]`."""
    stripped = line.strip()
    if not (stripped.startswith("#[cfg(") and stripped.endswith(")]")):
        return False
    inner = stripped[len("#[cfg(") : -len(")]")].strip()
    if inner == "test":
        return True
    if inner.startswith("all(") and inner.endswith(")"):
        preds = _split_top_level_commas(inner[len("all(") : -1])
        return "test" in preds
    return False


def _scan_rust_top_level_braces(text: str) -> tuple[list[int], dict[int, int]] | None:
    """Return `(line_start_depth, open_line_to_close_line)` for top-level `{}` blocks.

    Returns None if braces are unbalanced (caller falls back to full line count).
    """
    raw_lines = text.splitlines(keepends=True)
    n_lines = len(raw_lines)
    if n_lines == 0:
        return [], {}

    # Build character offsets of every newline for fast offset -> 0-based line lookup.
    # Because we advance monotonically through `text`, we track current line index
    # incrementally in O(1) amortized time.
    line_start_depth = [0] * n_lines
    open_braces: dict[int, int] = {}
    depth = 0
    open_stack: list[int] = []

    pos = 0
    length = len(text)
    cur_line = 0
    # Track next newline position so updating `line_start_depth` stays linear.
    next_nl = text.find("\n", 0)

    def advance_lines_to(target: int, cur_depth: int) -> None:
        nonlocal cur_line, next_nl
        while next_nl != -1 and next_nl < target:
            cur_line += 1
            if cur_line < n_lines:
                line_start_depth[cur_line] = cur_depth
            next_nl = text.find("\n", next_nl + 1)

    while pos < length:
        match = _RUST_TOKEN_RE.search(text, pos)
        if match is None:
            advance_lines_to(length, depth)
            break

        kind = match.lastgroup
        start = match.start()
        end = match.end()
        advance_lines_to(start, depth)

        if kind == "block_open":
            # Rust block comments nest (`/* /* */ */`).
            cdepth = 1
            scan = end
            while scan < length and cdepth > 0:
                next_open = text.find("/*", scan)
                next_close = text.find("*/", scan)
                if next_close == -1:
                    advance_lines_to(length, depth)
                    return None
                if next_open != -1 and next_open < next_close:
                    cdepth += 1
                    scan = next_open + 2
                else:
                    cdepth -= 1
                    scan = next_close + 2
            advance_lines_to(scan, depth)
            pos = scan
            continue

        if kind == "raw_str":
            hashes = match.group("hashes")
            closing = '"' + hashes
            close_idx = text.find(closing, end)
            if close_idx == -1:
                advance_lines_to(length, depth)
                return None
            end_pos = close_idx + len(closing)
            advance_lines_to(end_pos, depth)
            pos = end_pos
            continue

        if kind == "lbrace":
            if depth == 0:
                open_stack.append(cur_line)
            depth += 1
            pos = end
            continue

        if kind == "rbrace":
            if depth == 0:
                return None
            depth -= 1
            if depth == 0 and open_stack:
                open_line = open_stack.pop()
                if open_line not in open_braces:
                    open_braces[open_line] = cur_line
            pos = end
            continue

        # line_comment, str, char
        advance_lines_to(end, depth)
        pos = end

    if depth != 0:
        return None
    return line_start_depth, open_braces


def count_rust_production_lines(text: str) -> int:
    """Count production lines in a Rust source file, excluding top-level `#[cfg(test)]` inline `mod` blocks."""
    lines = text.splitlines()
    total = len(lines)
    if total == 0:
        return 0
    # Fast path: files with no cfg-test attributes require no lexical brace scan.
    if "#[cfg(" not in text or "test" not in text:
        return total

    scanned = _scan_rust_top_level_braces(text)
    if scanned is None:
        return total
    line_start_depth, open_braces = scanned

    excluded_lines = 0
    idx = 0
    while idx < total:
        if line_start_depth[idx] == 0 and is_cfg_test_attribute(lines[idx]):
            j = idx + 1
            while j < total and (
                lines[j].strip() == ""
                or lines[j].lstrip().startswith("#[")
                or lines[j].lstrip().startswith("//")
            ):
                j += 1
            if j < total and line_start_depth[j] == 0:
                mod_match = _MOD_DECL_RE.match(lines[j])
                if mod_match is not None:
                    open_line: int | None = None
                    if mod_match.group(1) == "{":
                        open_line = j
                    elif (
                        j + 1 < total
                        and line_start_depth[j + 1] == 0
                        and lines[j + 1].lstrip().startswith("{")
                    ):
                        open_line = j + 1
                    if open_line is not None and open_line in open_braces:
                        close_line = open_braces[open_line]
                        excluded_lines += close_line - idx + 1
                        idx = close_line + 1
                        continue
        idx += 1

    return total - excluded_lines


def count_source_lines(path: Path) -> int:
    """Count production lines for a single source file."""
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix == ".rs":
        return count_rust_production_lines(text)
    return len(text.splitlines())


def scan_repository(
    root: Path = DEFAULT_ROOT,
    scan_roots: tuple[str, ...] = SCAN_ROOTS,
) -> dict[str, int]:
    """Return `{posix_rel_path: prod_line_count}` for all scanned production files."""
    counts: dict[str, int] = {}
    for subdir in scan_roots:
        base = root / subdir
        if not base.is_dir():
            continue
        for file_path in sorted(base.rglob("*")):
            if not file_path.is_file() or file_path.suffix not in SOURCE_EXTENSIONS:
                continue
            rel = file_path.relative_to(root)
            if is_excluded_path(rel):
                continue
            counts[rel.as_posix()] = count_source_lines(file_path)
    return counts


def load_baseline(baseline_path: Path) -> dict[str, int]:
    """Load `{posix_rel_path: max_allowed_lines}` from the baseline JSON file."""
    if not baseline_path.exists():
        return {}
    raw = json.loads(baseline_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "files" in raw and isinstance(raw["files"], dict):
        entries = raw["files"]
    elif isinstance(raw, dict):
        entries = raw
    else:
        raise ValueError(f"invalid baseline format in {baseline_path}")
    result: dict[str, int] = {}
    for rel, limit in entries.items():
        if not isinstance(rel, str) or not isinstance(limit, int) or isinstance(limit, bool):
            raise ValueError(f"invalid baseline entry {rel!r}: {limit!r}")
        result[rel] = limit
    return result


def write_baseline(
    baseline_path: Path,
    counts: dict[str, int],
    *,
    floor: int = FLOOR,
) -> dict[str, int]:
    """Write all files over `floor` sorted by path into `baseline_path`."""
    oversized = {
        rel: lines for rel, lines in sorted(counts.items()) if lines > floor
    }
    payload = {
        "floor": floor,
        "counting_rule": COUNTING_RULE,
        "files": oversized,
    }
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(
        json.dumps(payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return oversized


@dataclass(frozen=True)
class Violation:
    kind: str  # "new_oversized" | "grew_past_baseline"
    path: str
    lines: int
    limit: int

    def format_message(self) -> str:
        delta = self.lines - self.limit
        if self.kind == "new_oversized":
            return (
                f"NEW oversized file: {self.path} has {self.lines} prod lines "
                f"(cap: <= {self.limit}, +{delta}). Split into dedicated feature submodules."
            )
        return (
            f"GREW past baseline: {self.path} has {self.lines} prod lines "
            f"(baseline: {self.limit}, +{delta}). Legacy monoliths may only shrink; "
            f"extract new code into a dedicated submodule."
        )


@dataclass(frozen=True)
class ScanReport:
    scanned_count: int
    oversized_count: int
    floor: int
    violations: list[Violation]
    shrunk: list[tuple[str, int, int]]  # (path, old_baseline, new_lines)
    removed_or_under_floor: list[tuple[str, int, int | None]]  # (path, old_baseline, new_lines_or_None)
    top_files: list[tuple[str, int]]  # sorted descending by lines

    @property
    def ok(self) -> bool:
        return not self.violations


def evaluate(
    counts: dict[str, int],
    baseline: dict[str, int],
    *,
    floor: int = FLOOR,
    top_n: int = 10,
) -> ScanReport:
    """Compare scanned production line counts against the floor and baseline ratchet."""
    violations: list[Violation] = []
    shrunk: list[tuple[str, int, int]] = []
    removed_or_under_floor: list[tuple[str, int, int | None]] = []

    for rel, lines in sorted(counts.items()):
        if lines <= floor:
            continue
        if rel not in baseline:
            violations.append(
                Violation(kind="new_oversized", path=rel, lines=lines, limit=floor)
            )
        else:
            limit = baseline[rel]
            if lines > limit:
                violations.append(
                    Violation(
                        kind="grew_past_baseline",
                        path=rel,
                        lines=lines,
                        limit=limit,
                    )
                )
            elif lines < limit:
                shrunk.append((rel, limit, lines))

    for rel, limit in sorted(baseline.items()):
        current = counts.get(rel)
        if current is None or current <= floor:
            removed_or_under_floor.append((rel, limit, current))

    oversized_items = sorted(
        ((rel, lines) for rel, lines in counts.items() if lines > floor),
        key=lambda item: (-item[1], item[0]),
    )
    return ScanReport(
        scanned_count=len(counts),
        oversized_count=len(oversized_items),
        floor=floor,
        violations=violations,
        shrunk=shrunk,
        removed_or_under_floor=removed_or_under_floor,
        top_files=oversized_items[:top_n],
    )


def format_report(report: ScanReport) -> str:
    lines: list[str] = [
        f"module_size_scan: scanned {report.scanned_count} production files "
        f"(cap <= {report.floor} prod lines, {report.oversized_count} legacy baselined)."
    ]
    if report.top_files:
        lines.append("Top oversized legacy files:")
        for rel, count in report.top_files:
            lines.append(f"  {count:6d}  {rel}")
    if report.shrunk or report.removed_or_under_floor:
        lines.append(
            "Baseline shrink opportunities (run `python3 tools/module_size_scan.py --update-baseline`):"
        )
        for rel, old, new in report.shrunk:
            lines.append(f"  SHRUNK: {rel}: {old} -> {new} ({new - old})")
        for rel, old, new in report.removed_or_under_floor:
            label = "removed" if new is None else f"{new} (<= {report.floor})"
            lines.append(f"  CLEARED: {rel}: {old} -> {label}")
    if report.violations:
        lines.append(f"FAIL: {len(report.violations)} module size violation(s):")
        for violation in report.violations:
            lines.append(f"  - {violation.format_message()}")
    else:
        lines.append("PASS: no new files over cap and no baselined files grew.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enforce the 1,500-line production module size cap and baseline ratchet."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Repository root directory (default: parent of tools/).",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help="Path to baseline JSON (default: <root>/tools/module_size_baseline.json).",
    )
    parser.add_argument(
        "--floor",
        type=int,
        default=FLOOR,
        help=f"Production line cap for new files (default: {FLOOR}).",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Regenerate the baseline JSON from current production file sizes.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON report.",
    )
    args = parser.parse_args(argv)

    root = args.root.resolve()
    baseline_path = (
        args.baseline.resolve()
        if args.baseline is not None
        else root / "tools" / "module_size_baseline.json"
    )
    counts = scan_repository(root)

    if args.update_baseline:
        oversized = write_baseline(baseline_path, counts, floor=args.floor)
        print(
            f"Updated {baseline_path} with {len(oversized)} legacy files > {args.floor} prod lines."
        )
        return 0

    if not baseline_path.exists():
        print(
            f"ERROR: baseline file not found at {baseline_path}. "
            f"Run with --update-baseline to generate it.",
            file=sys.stderr,
        )
        return 2

    baseline = load_baseline(baseline_path)
    report = evaluate(counts, baseline, floor=args.floor)

    if args.json:
        payload = {
            "ok": report.ok,
            "floor": report.floor,
            "scanned_count": report.scanned_count,
            "oversized_count": report.oversized_count,
            "violations": [
                {
                    "kind": v.kind,
                    "path": v.path,
                    "lines": v.lines,
                    "limit": v.limit,
                    "message": v.format_message(),
                }
                for v in report.violations
            ],
            "shrunk": [
                {"path": rel, "baseline": old, "lines": new}
                for rel, old, new in report.shrunk
            ],
            "cleared": [
                {"path": rel, "baseline": old, "lines": new}
                for rel, old, new in report.removed_or_under_floor
            ],
            "top_files": [
                {"path": rel, "lines": count} for rel, count in report.top_files
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(format_report(report))

    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
