#!/usr/bin/env python3
"""Cargo workspace crate-graph path-scoping for .github/workflows/ci.yml.

Reads the root `Cargo.toml` and every workspace member's `Cargo.toml` via the
Python standard library (`tomllib`), computes Cargo-accurate reverse dependents
(transitive normal/build dependencies plus 1-hop dev-dependencies), and maps
changed git paths to minimal, sound CI execution scopes:

- Docs / markdown / `AGENTS.md` / `sylphx.toml` /
  `tools/` / Python policy-only PRs -> `rust_scope=none`, `run_macos=false`.
- `packages/` / `package.json`-only PRs -> `rust_scope=none`, `run_npm=true`,
  `run_macos=false`.
- Leaf-crate-only PRs (e.g. `crates/anyagents-guest/**`,
  `crates/anyagents-mcp-*/**`, `crates/anyagents-gateway/**`, leaf harness
  adapters) -> `rust_scope=crates:<pkg1>,<pkg2>,...` targeting only touched
  crates + Cargo reverse dependents, skipping macOS and privileged Linux host
  acceptance lanes when no macOS/host-platform crate is affected.
- Core / coordinator / workspace-wide changes (`Cargo.toml`, `Cargo.lock`,
  `rustfmt.toml`, `crates/anyagents-core/**`, `crates/anyagents/**`,
  `crates/anyagents-harness/**`, `tests/invariants/**`,
  `.github/workflows/ci.yml`), non-diff events (`push`, `workflow_dispatch`),
  diff errors, or unrecognized paths -> fail-closed `rust_scope=workspace`,
  `run_macos=true`, `run_host_acceptance=true`.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
import tomllib

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
PKG_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Paths validated directly in the fast `changes` job (no Rust build needed).
NON_RUST_PATTERNS = (
    re.compile(r"^docs/.+\.md$"),
    re.compile(r"^[^/]+\.md$"),
    re.compile(r"^LICENSE[^/]*$"),
    re.compile(r"^\.github/ISSUE_TEMPLATE/.+$"),
    re.compile(r"^Dockerfile\.prebuilt$"),
    re.compile(r"^sylphx\.toml$"),
    re.compile(r"^tools/.+$"),
    re.compile(r"^scripts/(release_admission|deploy_build)\.py$"),
    re.compile(r"^tests/test_(release_admission|deploy_build)\.py$"),
)

NPM_PATTERNS = (
    re.compile(r"^packages/.+$"),
    re.compile(r"^package\.json$"),
)

# Paths that always require the full workspace Linux + macOS + host-acceptance gate.
WORKSPACE_TRIGGER_PATTERNS = (
    re.compile(r"^Cargo\.(toml|lock)$"),
    re.compile(r"^rustfmt\.toml$"),
    re.compile(r"^\.cargo/.+$"),
    re.compile(r"^\.github/workflows/ci\.yml$"),
    re.compile(r"^scripts/verify-host-memory-seat\.py$"),
    re.compile(r"^scripts/verify-harnesses\.sh$"),
    re.compile(r"^crates/anyagents-core/.+$"),
    re.compile(r"^crates/anyagents/.+$"),
    re.compile(r"^tests/invariants/.+$"),
)

# Crates owning macOS or host-OS platform-branching behavior (other than the root
# `anyagents` binary, which links all leaf crates and is handled directly via
# WORKSPACE_TRIGGER_PATTERNS when `crates/anyagents/**` is modified).
MACOS_PLATFORM_CRATES = frozenset(
    {
        "anyagents-core",
        "anyagents-harness",
        "anyagents-harness-stream",
        "anyagents-harness-opencode",
        "anyagents-installer",
        "anyagents-keeper",
        "anyagents-supervisor",
    }
)

# Crates participating in Linux privileged host/seat/workstation acceptance.
HOST_ACCEPTANCE_CRATES = frozenset(
    {
        "anyagents",
        "anyagents-core",
        "anyagents-harness",
        "anyagents-agent",
        "anyagents-keeper",
        "anyagents-supervisor",
    }
)

# Root binary package excluded when checking whether a leaf crate transitively
# reaches macOS/host-only crates (changes to `crates/anyagents/**` directly
# trigger full workspace scope above).
ROOT_COORDINATOR_PKG = "anyagents"
CORE_PROMOTE_PKG = "anyagents-core"
FAKE_OPENCODE_PKG = "anyagents-harness-opencode"


@dataclass(frozen=True)
class CrateInfo:
    name: str
    rel_path: str
    has_lib: bool
    has_macos_target_cfg: bool
    normal_deps: frozenset[str]
    dev_deps: frozenset[str]


@dataclass(frozen=True)
class WorkspaceGraph:
    packages: dict[str, CrateInfo]
    path_to_pkg: dict[str, str]
    rev_normal: dict[str, frozenset[str]]
    rev_dev: dict[str, frozenset[str]]


@dataclass(frozen=True)
class ScopeResult:
    code: bool
    rust_scope: str  # "none" | "crates:<pkg1>,<pkg2>" | "workspace"
    cargo_pkg_args: str
    cargo_doc_args: str
    run_macos: bool
    run_host_acceptance: bool
    run_npm: bool
    needs_fake_opencode: bool
    reason: str

    def to_github_outputs(self) -> dict[str, str]:
        return {
            "code": "true" if self.code else "false",
            "rust_scope": self.rust_scope,
            "cargo_pkg_args": self.cargo_pkg_args,
            "cargo_doc_args": self.cargo_doc_args,
            "run_macos": "true" if self.run_macos else "false",
            "run_host_acceptance": "true" if self.run_host_acceptance else "false",
            "run_npm": "true" if self.run_npm else "false",
            "needs_fake_opencode": "true" if self.needs_fake_opencode else "false",
            "reason": self.reason,
        }


def _expand_workspace_members(root: Path, patterns: list[str], excludes: list[str]) -> list[str]:
    excluded: set[str] = set()
    for pat in excludes:
        for match in root.glob(pat):
            if match.is_dir():
                excluded.add(match.relative_to(root).as_posix())

    members: list[str] = []
    seen: set[str] = set()
    for pat in patterns:
        if any(ch in pat for ch in "*?["):
            candidates = sorted(root.glob(pat))
        else:
            candidates = [root / pat]
        for candidate in candidates:
            if not candidate.is_dir():
                continue
            rel = candidate.relative_to(root).as_posix()
            if rel in excluded or rel in seen:
                continue
            if (candidate / "Cargo.toml").is_file():
                seen.add(rel)
                members.append(rel)
    return members


def load_workspace_graph(root: Path = DEFAULT_ROOT) -> WorkspaceGraph:
    """Parse `Cargo.toml` and member manifests into a workspace dependency graph."""
    ws_data = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    ws = ws_data.get("workspace", {})
    member_paths = _expand_workspace_members(
        root,
        list(ws.get("members", [])),
        list(ws.get("exclude", [])),
    )

    manifests: dict[str, tuple[str, dict]] = {}
    path_to_pkg: dict[str, str] = {}
    for rel in member_paths:
        data = tomllib.loads((root / rel / "Cargo.toml").read_text(encoding="utf-8"))
        name = data["package"]["name"]
        if not PKG_NAME_RE.fullmatch(name):
            raise ValueError(f"unexpected crate name {name!r} in {rel}/Cargo.toml")
        manifests[name] = (rel, data)
        path_to_pkg[rel] = name

    all_names = set(manifests)
    packages: dict[str, CrateInfo] = {}
    rev_normal_mut: dict[str, set[str]] = {n: set() for n in all_names}
    rev_dev_mut: dict[str, set[str]] = {n: set() for n in all_names}

    for name, (rel, data) in manifests.items():
        member_dir = root / rel
        normal_deps: set[str] = set()
        dev_deps: set[str] = set()

        def collect(table: dict, bucket: set[str]) -> None:
            for dep_key, spec in table.items():
                dep_name = spec.get("package", dep_key) if isinstance(spec, dict) else dep_key
                if dep_name in all_names and dep_name != name:
                    bucket.add(dep_name)
                elif isinstance(spec, dict) and isinstance(spec.get("path"), str):
                    resolved = (member_dir / spec["path"]).resolve()
                    try:
                        dep_rel = resolved.relative_to(root.resolve()).as_posix()
                    except ValueError:
                        dep_rel = ""
                    if dep_rel in path_to_pkg and path_to_pkg[dep_rel] != name:
                        bucket.add(path_to_pkg[dep_rel])

        collect(data.get("dependencies", {}), normal_deps)
        collect(data.get("build-dependencies", {}), normal_deps)
        collect(data.get("dev-dependencies", {}), dev_deps)
        target_table = data.get("target", {})
        has_macos_target_cfg = False
        for target_cfg, target_data in target_table.items():
            low = target_cfg.lower()
            if "macos" in low or "darwin" in low:
                has_macos_target_cfg = True
            if isinstance(target_data, dict):
                collect(target_data.get("dependencies", {}), normal_deps)
                collect(target_data.get("build-dependencies", {}), normal_deps)
                collect(target_data.get("dev-dependencies", {}), dev_deps)

        has_lib = (member_dir / "src" / "lib.rs").is_file() or "lib" in data
        packages[name] = CrateInfo(
            name=name,
            rel_path=rel,
            has_lib=has_lib,
            has_macos_target_cfg=has_macos_target_cfg,
            normal_deps=frozenset(normal_deps),
            dev_deps=frozenset(dev_deps),
        )
        for dep in normal_deps:
            rev_normal_mut[dep].add(name)
        for dep in dev_deps:
            rev_dev_mut[dep].add(name)

    return WorkspaceGraph(
        packages=packages,
        path_to_pkg=path_to_pkg,
        rev_normal={k: frozenset(v) for k, v in rev_normal_mut.items()},
        rev_dev={k: frozenset(v) for k, v in rev_dev_mut.items()},
    )


def affected_packages(
    graph: WorkspaceGraph,
    touched: set[str],
) -> tuple[list[str], set[str]]:
    """Return `(all_affected_sorted, normal_reachable)` using Cargo dependency semantics.

    Normal/build dependencies propagate transitively; `dev-dependencies` apply
    only 1 hop from any crate in `normal_reachable`.
    """
    normal_reach = set(touched)
    stack = list(touched)
    while stack:
        cur = stack.pop()
        for rev in graph.rev_normal.get(cur, ()):
            if rev not in normal_reach:
                normal_reach.add(rev)
                stack.append(rev)

    affected = set(normal_reach)
    for pkg in normal_reach:
        affected |= graph.rev_dev.get(pkg, frozenset())

    return sorted(affected), normal_reach


def _full_workspace_scope(reason: str) -> ScopeResult:
    return ScopeResult(
        code=True,
        rust_scope="workspace",
        cargo_pkg_args="--workspace",
        cargo_doc_args="--workspace",
        run_macos=True,
        run_host_acceptance=True,
        run_npm=True,
        needs_fake_opencode=True,
        reason=reason,
    )


def _match_member_crate(path: str, sorted_member_paths: list[tuple[str, str]]) -> str | None:
    for rel_prefix, pkg in sorted_member_paths:
        if path == rel_prefix or path.startswith(rel_prefix + "/"):
            return pkg
    return None


def compute_scope(
    root: Path,
    changed_paths: list[str],
    *,
    event: str = "pull_request",
    diff_failed: bool = False,
) -> ScopeResult:
    """Compute CI scope outputs for a change set."""
    if diff_failed:
        return _full_workspace_scope("diff-failed")
    if event not in ("pull_request", "merge_group"):
        return _full_workspace_scope(f"event:{event}")

    cleaned = [p.strip().lstrip("./") for p in changed_paths if p.strip()]
    if not cleaned:
        return ScopeResult(
            code=False,
            rust_scope="none",
            cargo_pkg_args="",
            cargo_doc_args="",
            run_macos=False,
            run_host_acceptance=False,
            run_npm=False,
            needs_fake_opencode=False,
            reason="empty-diff",
        )

    graph = load_workspace_graph(root)
    # Match longest member path first so nested paths resolve unambiguously.
    sorted_members = sorted(
        graph.path_to_pkg.items(),
        key=lambda item: (-len(item[0]), item[0]),
    )

    touched_crates: set[str] = set()
    run_npm = False
    for path in cleaned:
        if any(pat.fullmatch(path) for pat in WORKSPACE_TRIGGER_PATTERNS):
            return _full_workspace_scope(f"workspace-trigger:{path}")
        if any(pat.fullmatch(path) for pat in NON_RUST_PATTERNS):
            continue
        if any(pat.fullmatch(path) for pat in NPM_PATTERNS):
            run_npm = True
            continue
        pkg = _match_member_crate(path, sorted_members)
        if pkg is not None:
            touched_crates.add(pkg)
            continue
        # Fail closed on unrecognized paths outside docs/tools/packages/members.
        return _full_workspace_scope(f"unclassified-path:{path}")

    if not touched_crates:
        reason = "npm-only" if run_npm else "non-rust-only"
        return ScopeResult(
            code=False,
            rust_scope="none",
            cargo_pkg_args="",
            cargo_doc_args="",
            run_macos=False,
            run_host_acceptance=False,
            run_npm=run_npm,
            needs_fake_opencode=False,
            reason=reason,
        )

    affected, normal_reach = affected_packages(graph, touched_crates)

    # Changes whose normal reverse dependents reach `anyagents-core` (e.g.
    # `anyagents-harness`) span almost the entire workspace; run full workspace.
    if CORE_PROMOTE_PKG in normal_reach:
        return _full_workspace_scope(
            f"core-dependent:{','.join(sorted(touched_crates))}"
        )

    non_root_normal = normal_reach - {ROOT_COORDINATOR_PKG}
    run_macos = any(
        pkg in MACOS_PLATFORM_CRATES or graph.packages[pkg].has_macos_target_cfg
        for pkg in non_root_normal
    )
    run_host_acceptance = bool(non_root_normal & HOST_ACCEPTANCE_CRATES)
    needs_fake_opencode = FAKE_OPENCODE_PKG in affected

    cargo_pkg_args = " ".join(f"-p {pkg}" for pkg in affected)
    doc_pkgs = [pkg for pkg in affected if graph.packages[pkg].has_lib]
    cargo_doc_args = " ".join(f"-p {pkg}" for pkg in doc_pkgs)

    return ScopeResult(
        code=True,
        rust_scope="crates:" + ",".join(affected),
        cargo_pkg_args=cargo_pkg_args,
        cargo_doc_args=cargo_doc_args,
        run_macos=run_macos,
        run_host_acceptance=run_host_acceptance,
        run_npm=run_npm,
        needs_fake_opencode=needs_fake_opencode,
        reason=f"scoped-crates:{','.join(sorted(touched_crates))}",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute Cargo crate-graph CI scoping outputs from changed paths."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Repository root directory (default: parent of tools/).",
    )
    parser.add_argument(
        "--event",
        default="pull_request",
        help="GitHub Actions event_name (default: pull_request).",
    )
    parser.add_argument(
        "--paths-file",
        type=Path,
        default=None,
        help="Newline-delimited file of changed repository paths (from git diff --name-only).",
    )
    parser.add_argument(
        "--diff-failed",
        action="store_true",
        help="Indicate git diff could not be computed (fails closed to workspace).",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        default=None,
        help="Append key=value lines to the given GITHUB_OUTPUT path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print outputs as JSON to stdout.",
    )
    args = parser.parse_args(argv)

    paths: list[str] = []
    if args.paths_file is not None and not args.diff_failed:
        paths = args.paths_file.read_text(encoding="utf-8").splitlines()

    result = compute_scope(
        args.root.resolve(),
        paths,
        event=args.event,
        diff_failed=args.diff_failed,
    )
    outputs = result.to_github_outputs()

    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as handle:
            for key, value in outputs.items():
                handle.write(f"{key}={value}\n")

    if args.json:
        print(json.dumps(outputs, indent=2, sort_keys=True))
    else:
        for key, value in outputs.items():
            print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
