"""Reject known vendor-impersonation signatures in shipped source (ADR 0009 D3)."""
from pathlib import Path
import re
import subprocess

from module_size_scan import SOURCE_EXTENSIONS, is_excluded_path, rust_test_module_ranges

# Intentionally narrow rollout exceptions; delete them as each owner lands its slice.
EXCLUDED_DIRS = (
    Path("crates/anyagents/src/server/native_signin"),
    Path("crates/anyagents-harness-spiron"),
)
EXCLUDED_FILES = {
    Path("apps/console/src/accounts/ConnectDialog.tsx"),
    Path("crates/anyagents/src/serverless_cloud.rs"),
    Path("Dockerfile.sandbox"),
}

# Prefixes identify the retired client IDs without retaining complete IDs.
RULES = {
    "borrowed OAuth client identity": (
        r"9d1c250a-e61b-44d9|app_EMoamEEZ73|1071006060591-"
        r"|\b(?:AGY_OAUTH_CLIENT_ID|ANYAGENTS_AGY_CLIENT_SECRET)\b"
    ),
    "private Anthropic OAuth API": (
        r"/api/oauth/(?:usage|profile)\b|(?:anthropic\.com|claude\.com)/v1/oauth/token\b"
        r"|\b(?:post_anthropic_oauth_refresh|refresh_anthropic_profile_oauth_if_due)\b"
    ),
    "private Codex API": r"chatgpt\.com/backend-api\b|/backend-api/wham/usage\b|auth\.openai\.com/oauth/token\b",
    "private Antigravity API": (
        r"cloudcode-pa(?:\.sandbox)?\.googleapis\.com\b"
        r"|\bv1internal:(?:retrieveUserQuotaSummary|fetchAvailableModels|loadCodeAssist)\b"
        r"|\bgoogle_antigravity_pkce_sign_in(?:_with)?\b"
    ),
    "vendor client impersonation header": (
        r"[\"'](?:claude-code|codex-cli|antigravity)/[0-9]"
        r"|[\"']user-agent[\"']\s*,\s*[\"'](?:claude-code|codex-cli|antigravity)[\"']"
        r"|oauth-2025-04-20"
    ),
    "vendor binary secret extraction": (
        r"\bextract_agy_oauth_clients\b|\b(?:extract|scrape|scan|discover|find|read)\w*(?:oauth|client)\w*secret\w*\b"
        r"|\b(?:extract|scrape|scan)\w*secret\w*(?:binary|exe)\w*\b"
    ),
    "other private subscription API": (
        r"cursor\.com/api/usage-summary\b|api2\.cursor\.sh/oauth/token\b"
        r"|api\.meta\.ai/muse-code/key\b|cli-chat-proxy\.grok\.com/v1/billing\b"
    ),
}
COMPILED_RULES = {label: re.compile(pattern, re.IGNORECASE) for label, pattern in RULES.items()}


def check_vendor_boundary(root: Path) -> None:
    # Only shipped files, not build output or untracked local developer data.
    raw = subprocess.check_output(["git", "ls-files", "-z", "--", "crates", "apps", "packages"], cwd=root)
    violations = []
    for path in map(Path, raw.decode("utf-8").split("\0")[:-1]):
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"invalid tracked path in vendor boundary check: {path}")
        if (path.suffix not in SOURCE_EXTENSIONS or is_excluded_path(path)
                or path in EXCLUDED_FILES or any(path.is_relative_to(p) for p in EXCLUDED_DIRS)):
            continue
        file = root / path
        if file.is_symlink() or not file.is_file():
            raise ValueError(f"expected regular vendor-boundary input: {path}")
        content = file.read_text(encoding="utf-8")
        if path.suffix == ".rs":
            lines = content.splitlines(keepends=True)
            for start, end in rust_test_module_ranges(content, content.splitlines()):
                # Preserve line numbers and prevent a match across an excluded module.
                lines[start:end] = ["\0\n"] * (end - start)
            content = "".join(lines)
        for label, rule in COMPILED_RULES.items():
            for match in rule.finditer(content):
                number = content.count("\n", 0, match.start()) + 1
                violations.append(f"{path}:{number}: {label}")
    if violations:
        raise ValueError("ADR 0009 vendor-boundary violations:\n" + "\n".join(violations))
