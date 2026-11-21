#!/usr/bin/env python3
"""Plan required CI jobs from the tested tree and its event base.

Diff events run policy checks plus the affected lanes. No trustworthy diff or
unrecognized input means workspace-wide scope. PRs run affected unit tests;
merge groups run every affected Linux lane. Only explicit platform-neutral
paths can skip macOS; main, scheduled, and manual runs test both OSes in full.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from vendor_boundary import check_vendor_boundary

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
PKG_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Paths validated directly in the fast `changes` job (no Rust build needed).
NON_RUST_PATTERNS = (
    re.compile(r"^docs/.+$"),
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

# Paths that affect CI itself or other workspace-wide inputs.
WORKSPACE_TRIGGER_PATTERNS = (
    re.compile(r"^Cargo\.(toml|lock)$"),
    re.compile(r"^rustfmt\.toml$"),
    re.compile(r"^rust-toolchain(?:\.toml)?$"),
    re.compile(r"^tools/(?:test_)?ci_scope\.py$"),
    re.compile(r"^\.cargo/.+$"),
    re.compile(r"^\.github/workflows/ci\.yml$"),
    re.compile(r"^scripts/verify-host-memory-seat\.py$"),
    re.compile(r"^scripts/verify-harnesses\.sh$"),
    re.compile(r"^tests/invariants/.+$"),
)

CONSOLE_DIR = "crates/anyagents/src/server/console"
CONSOLE_LOCKFILE = "apps/console/package-lock.json"
NPM_RESOLVED_URL = re.compile(r"https://registry\.npmjs\.org/[^\s\\\x00-\x1f\x7f]+")
CONSOLE_ASSETS = frozenset({
    "index.html", "signin.html", "auth_recovery.html", "console.css", "console.js",
})
COORDINATOR_DIR = "crates/anyagents"
SERVER_DIR = f"{COORDINATOR_DIR}/src/server"
# Explicit HTTP/persistence modules; keep platform boundaries, unknown modules
# and the composition root (server.rs) out of the Linux-only path.
SERVER_ONLY_MODULES = frozenset({
    "admin_auth", "admin_ui", "agents_roster", "channels_groups", "console_html",
    "hybrid", "in_flight", "knowledge", "memory_backlog", "messaging",
    "messaging_conversation", "messaging_delivery", "messaging_memory", "oidc_bearer",
    "ops", "profile_models", "profiles", "reap_queue", "roles", "s3_media_profiles",
    "schedules", "secrets", "secrets_tests", "serverless_envelope", "serverless_failure",
    "serverless_maintenance", "serverless_memory", "serverless_steer", "serverless_watches",
    "timer_fire", "types", "versions", "webhooks_mcp",
})
CORE_DB_ONLY = re.compile(
    r"crates/anyagents-core/(?:migrations(?:_pg)?/[^/]+\.sql|"
    r"src/db/(?:pg_migrations|sqlite_to_pg(?:_[a-z]+)?)\.rs|"
    r"tests/(?:postgres_store|stateless_coordinator_pg)\.rs)"
)

# Crates with host-platform contracts (all anyagents-harness* crates too).
MACOS_PLATFORM_CRATES = frozenset(
    {
        "anyagents-core",
        "anyagents-agent",
        "anyagents-toolchain",
        "anyagents-mcp-browser",
        "anyagents-mcp-computer",
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

# Linking the coordinator as a reverse dependent does not turn every leaf
# input into a host change. Direct coordinator paths are classified separately.
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
    run_console: bool
    needs_fake_opencode: bool
    reason: str
    linux_matrix: str

    def to_github_outputs(self) -> dict[str, str]:
        return {
            "code": "true" if self.code else "false",
            "rust_scope": self.rust_scope,
            "cargo_pkg_args": self.cargo_pkg_args,
            "cargo_doc_args": self.cargo_doc_args,
            "run_macos": "true" if self.run_macos else "false",
            "run_host_acceptance": "true" if self.run_host_acceptance else "false",
            "run_npm": "true" if self.run_npm else "false",
            "run_console": "true" if self.run_console else "false",
            "linux_matrix": self.linux_matrix,
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


def _scope(
    reason: str,
    *,
    pr: bool = False,
    graph: WorkspaceGraph | None = None,
    affected: tuple[str, ...] | list[str] = (),
    workspace: bool = False,
    macos: bool = False,
    host: bool = False,
    npm: bool = False,
    console: bool = False,
) -> ScopeResult:
    def pkg_args(names: list[str]) -> str:
        return " ".join(f"-p {name}" for name in names)

    coordinator = workspace or ROOT_COORDINATOR_PKG in affected
    crates = [pkg for pkg in affected if pkg != ROOT_COORDINATOR_PKG]
    crate_args = "--workspace --exclude anyagents" if workspace else pkg_args(crates)
    doc_args = "--workspace" if workspace else pkg_args(
        [pkg for pkg in crates if graph is not None and graph.packages[pkg].has_lib]
    )
    if pr:
        doc_args = ""
        host = False
    scope = "workspace" if workspace else "crates:" + ",".join(affected) if affected else "none"
    cargo_args = "--workspace" if workspace else pkg_args(list(affected))
    # In queue and trunk CI, the complement of the identity binary stays
    # in coordinator so new integration tests are never omitted.
    lanes = []
    if cargo_args:
        lanes.append({"lane": "lint", "packages": cargo_args, "docs": ""})
    if crate_args:
        lanes.append({"lane": "crates", "packages": crate_args, "docs": doc_args})
    if coordinator:
        lanes.append({"lane": "coordinator", "packages": "-p anyagents", "docs": ""})
        if not pr:
            lanes.append({"lane": "identity", "packages": "-p anyagents", "docs": ""})
    if host:
        lanes.append({"lane": "host", "packages": "-p anyagents", "docs": ""})
    return ScopeResult(
        code=bool(cargo_args),
        rust_scope=scope,
        cargo_pkg_args=cargo_args,
        cargo_doc_args=doc_args,
        run_macos=macos,
        run_host_acceptance=host,
        run_npm=npm,
        run_console=console,
        needs_fake_opencode=workspace or FAKE_OPENCODE_PKG in affected,
        reason=reason,
        linux_matrix=json.dumps({"include": lanes}, separators=(",", ":")),
    )


def _full_workspace_scope(reason: str, *, macos: bool = True, pr: bool = False) -> ScopeResult:
    return _scope(reason, pr=pr, workspace=True, macos=macos, host=True, npm=True, console=True)


def _match_member_crate(path: str, sorted_member_paths: list[tuple[str, str]]) -> str | None:
    for rel_prefix, pkg in sorted_member_paths:
        if path.startswith(rel_prefix + "/"):
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
    pr = event == "pull_request"
    if diff_failed:
        return _full_workspace_scope("diff-failed", pr=pr)
    if event not in ("pull_request", "merge_group"):
        return _full_workspace_scope(f"event:{event}")

    touched_paths: list[str] = []
    run_npm = run_console = False
    for path in changed_paths:
        if any(ord(char) < 32 for char in path):
            return _full_workspace_scope(f"control-character-in-path:{path!r}", pr=pr)
        if any(pat.fullmatch(path) for pat in WORKSPACE_TRIGGER_PATTERNS):
            return _full_workspace_scope(f"workspace-trigger:{path!r}", pr=pr)
        if any(pat.fullmatch(path) for pat in NON_RUST_PATTERNS):
            continue
        if any(pat.fullmatch(path) for pat in NPM_PATTERNS):
            run_npm = True
        elif path.startswith("apps/console/") or (
            path.startswith(CONSOLE_DIR + "/") and path.removeprefix(CONSOLE_DIR + "/") in CONSOLE_ASSETS
        ):
            run_console = True
        else:
            touched_paths.append(path)

    if not touched_paths:
        return _scope("no-rust-inputs", npm=run_npm, console=run_console)
    def server_only(path: str) -> bool:
        if not path.startswith(SERVER_DIR + "/"):
            return False
        rel = Path(path.removeprefix(SERVER_DIR + "/"))
        return bool(rel.parts and rel.suffix == ".rs" and Path(rel.parts[0]).stem in SERVER_ONLY_MODULES)

    if any(CORE_DB_ONLY.fullmatch(path) for path in touched_paths) and all(
        CORE_DB_ONLY.fullmatch(path) or server_only(path) for path in touched_paths
    ):
        # Shared store changes still exercise every Linux dependent. Shipped
        # storage migrations don't change the workstation/platform contract.
        return _full_workspace_scope("storage-only", macos=False, pr=pr)

    graph = load_workspace_graph(root)
    sorted_members = sorted(graph.path_to_pkg.items(), key=lambda item: -len(item[0]))
    touched_crates: set[str] = set()
    server = False
    for path in touched_paths:
        if server_only(path):
            server = True
            touched_crates.add(ROOT_COORDINATOR_PKG)
            continue
        if path.startswith(COORDINATOR_DIR + "/"):
            return _full_workspace_scope(f"coordinator-platform:{path!r}", pr=pr)
        pkg = _match_member_crate(path, sorted_members)
        if pkg is None:
            return _full_workspace_scope(f"unclassified-path:{path!r}", pr=pr)
        # Manifest/build changes can add/remove and rewire dependencies.
        if path in (f"{graph.packages[pkg].rel_path}/Cargo.toml", f"{graph.packages[pkg].rel_path}/build.rs"):
            return _full_workspace_scope(f"crate-build-input:{path!r}", pr=pr)
        touched_crates.add(pkg)

    affected, normal_reach = affected_packages(graph, touched_crates)
    if CORE_PROMOTE_PKG in normal_reach:
        return _full_workspace_scope(f"core-dependent:{','.join(sorted(touched_crates))}", pr=pr)

    non_root_normal = normal_reach - {ROOT_COORDINATOR_PKG}
    return _scope(
        f"scoped-crates:{','.join(sorted(touched_crates))}",
        pr=pr,
        graph=graph,
        affected=affected,
        macos=any(
            pkg in MACOS_PLATFORM_CRATES or pkg.startswith("anyagents-harness")
            or graph.packages[pkg].has_macos_target_cfg
            for pkg in non_root_normal
        ),
        host=server or bool(non_root_normal & HOST_ACCEPTANCE_CRATES),
        npm=run_npm,
        console=run_console,
    )


def changed_git_paths(root: Path, base: str, head: str) -> list[str]:
    """Diff the event base against the checked-out candidate, including deletions."""
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("missing or invalid event SHA")
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root).decode().strip() != head:
        raise ValueError("event head is not the checked-out commit")
    subprocess.run(["git", "merge-base", "--is-ancestor", base, head], cwd=root, check=True)
    raw = subprocess.check_output(
        ["git", "diff", "--name-only", "--no-renames", "-z", base, head, "--"], cwd=root,
    )
    # Git filenames are bytes, including whitespace. Never strip leading dots,
    # split on newlines, or turn a malformed pathname into an excluded one.
    return [p.decode("utf-8") for p in raw.split(b"\0")[:-1]]


def check_purity(root: Path) -> None:
    """Run the existing purity policy without building the Rust test binary."""
    policy = root / COORDINATOR_DIR / "tests/open_source_purity.rs"
    rules = re.search(r"let forbidden: [^=]+ = &\[(.*?)\];", policy.read_text(), re.S)
    if not rules:
        raise ValueError("missing Rust purity policy")
    # Accept only the concat!("part", "part", ...) rule syntax in that test.
    # Fail instead of silently dropping a rule if the Rust format changes.
    literal = r'"[^"\\]*"'
    entry = re.compile(rf"\(\s*({literal}),\s*concat!\(({literal}(?:,\s*{literal})*)\),?\s*\),")
    remaining = rules[1].strip()
    forbidden = []
    while remaining:
        match = entry.match(remaining)
        if not match:
            raise ValueError("unsupported Rust purity rule syntax")
        forbidden.append((match[1][1:-1], "".join(part[1:-1] for part in re.findall(literal, match[2]))))
        remaining = remaining[match.end():].lstrip()
    if not forbidden:
        raise ValueError("empty Rust purity policy")
    raw = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    extensions = {".rs", ".md", ".toml", ".yml", ".yaml", ".sh"}
    console_extensions = {".ts", ".tsx", ".js", ".css", ".html", ".mjs"}
    violations = []
    for path in map(Path, raw.decode().split("\0")[:-1]):
        is_console = path.parts[:2] == ("apps", "console") or path.is_relative_to(CONSOLE_DIR)
        if path.parts[0] == "docs" or path.suffix in extensions or (
            is_console and path.suffix in console_extensions
        ):
            if path.is_absolute() or (root / path).is_symlink():
                raise ValueError(f"expected regular purity input: {path}")
            # Some documented diagrams/assets are binary; only scan UTF-8 text.
            try:
                content = (root / path).read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for number, line in enumerate(content.splitlines(), 1):
                violations.extend(
                    f"{path}:{number}: {reason}" for reason, needle in forbidden if needle in line
                )
    if violations:
        raise ValueError("open-source purity violations:\n" + "\n".join(violations))


    check_vendor_boundary(root)


def check_console_lockfile(root: Path) -> None:
    """Allow only public npm tarball origins, including in nested entries."""
    lockfile = root / CONSOLE_LOCKFILE
    if lockfile.is_symlink() or not lockfile.is_file():
        raise ValueError(f"expected regular lockfile: {CONSOLE_LOCKFILE}")

    def checked_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        values: dict[str, object] = {}
        for key, value in pairs:
            if key in values:
                raise ValueError(f"{CONSOLE_LOCKFILE}: duplicate JSON key")
            if key == "resolved" and (
                not isinstance(value, str) or not NPM_RESOLVED_URL.fullmatch(value)
            ):
                # Do not repeat rejected URLs; they can contain private data.
                raise ValueError(f"{CONSOLE_LOCKFILE}: resolved URLs must use https://registry.npmjs.org/")
            values[key] = value
        return values

    lock = json.loads(lockfile.read_text(encoding="utf-8"), object_pairs_hook=checked_object)
    if not isinstance(lock, dict) or not isinstance(lock.get("packages"), dict):
        raise ValueError(f"{CONSOLE_LOCKFILE}: expected npm lockfile with a packages object")


def check_console_assets(root: Path) -> None:
    """Validate embedded Rust inputs and the console lockfile before npm ci."""
    files = root / CONSOLE_DIR
    if {p.name for p in files.iterdir()} != CONSOLE_ASSETS:
        raise ValueError(f"expected exactly these embedded assets: {sorted(CONSOLE_ASSETS)}")
    markers = {
        "signin.html": {"{{COORDINATOR_URL}}"},
        "auth_recovery.html": {"{{REASON}}", "{{LOGIN_URL}}", "{{HOME_URL}}"},
    }
    for path in sorted(files.iterdir()):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"expected regular embedded asset: {path}")
        contents = path.read_text(encoding="utf-8")
        placeholders = set(re.findall(r"\{\{[A-Z_]+\}\}", contents))
        if not contents.strip() or not markers.get(path.name, set()) <= placeholders:
            raise ValueError(f"empty asset or missing Rust substitution marker: {path}")
    check_console_lockfile(root)


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
    parser.add_argument("--base", default="", help="Event base commit SHA.")
    parser.add_argument("--head", default="", help="Tested event commit SHA.")
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
    checks = parser.add_mutually_exclusive_group()
    checks.add_argument("--check-purity", action="store_true")
    checks.add_argument("--check-console-assets", action="store_true")
    args = parser.parse_args(argv)

    root = args.root.resolve()
    if args.check_purity or args.check_console_assets:
        try:
            (check_purity if args.check_purity else check_console_assets)(root)
        except (ValueError, OSError, subprocess.CalledProcessError) as err:
            print(err, file=sys.stderr)
            return 1
        return 0

    paths: list[str] = []
    diff_failed = False
    if args.event in ("pull_request", "merge_group"):
        try:
            paths = changed_git_paths(root, args.base, args.head)
        except (ValueError, OSError, subprocess.CalledProcessError) as err:
            print(f"Unavailable candidate diff; using workspace-wide scope: {err}", file=sys.stderr)
            diff_failed = True
    result = compute_scope(root, paths, event=args.event, diff_failed=diff_failed)
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
