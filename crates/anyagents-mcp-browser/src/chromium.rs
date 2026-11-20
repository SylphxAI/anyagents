//! Chromium / Google Chrome / Playwright Chromium executable discovery.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// Candidate binary names searched on `$PATH`.
const PATH_BINARY_NAMES: &[&str] = &[
    "chromium",
    "chromium-browser",
    "google-chrome-stable",
    "google-chrome",
    "chrome",
];

/// Relative executable paths inside a Playwright browser directory (`chromium-*` or
/// `chromium_headless_shell-*`).
const PLAYWRIGHT_RELATIVE_BINS: &[&str] = &[
    "chrome-linux/chrome",
    "chrome-linux64/chrome",
    "chrome-linux/headless_shell",
    "chrome-headless-shell-linux64/chrome-headless-shell",
    "chrome-mac/Chromium.app/Contents/MacOS/Chromium",
    "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "chrome-mac/headless_shell",
    "chrome-win/chrome.exe",
    "chrome-win64/chrome.exe",
];

/// Discover a usable Chromium executable using:
/// 1. Explicit CLI/env override (`--chromium-bin` / `ANYAGENTS_CHROMIUM_BIN`, parsed by
///    `BrowserServerConfig` and passed as `explicit`).
/// 2. Standard system binary directories (`/usr/local/bin`, `/usr/bin`, `/opt/homebrew/bin`,
///    `/snap/bin`) and well-known system application paths for Linux, macOS, and Windows.
/// 3. Playwright browser installations (`PLAYWRIGHT_BROWSERS_PATH`, `/opt/playwright`,
///    `$HOME/.cache/ms-playwright`, `$HOME/Library/Caches/ms-playwright`).
pub fn discover_chromium(explicit: Option<&Path>) -> Result<PathBuf, String> {
    let bin_dirs = default_system_bin_dirs();
    let system_paths = default_system_paths();
    let playwright_roots = default_playwright_roots();
    discover_chromium_in(explicit, &bin_dirs, &system_paths, &playwright_roots)
}

fn default_system_bin_dirs() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/usr/local/bin"),
        PathBuf::from("/usr/bin"),
        PathBuf::from("/opt/homebrew/bin"),
        PathBuf::from("/snap/bin"),
    ]
}

/// Pure, testable discovery implementation decoupled from global process env.
pub fn discover_chromium_in(
    override_path: Option<&Path>,
    path_dirs: &[PathBuf],
    system_paths: &[PathBuf],
    playwright_roots: &[PathBuf],
) -> Result<PathBuf, String> {
    if let Some(explicit) = override_path {
        if is_executable_file(explicit) {
            return Ok(explicit.to_path_buf());
        }
        return Err(format!(
            "configured Chromium binary `{}` does not exist or is not executable",
            explicit.display()
        ));
    }

    for name in PATH_BINARY_NAMES {
        for dir in path_dirs {
            let candidate = dir.join(name);
            if is_executable_file(&candidate) {
                return Ok(candidate);
            }
            #[cfg(windows)]
            {
                let exe = dir.join(format!("{name}.exe"));
                if is_executable_file(&exe) {
                    return Ok(exe);
                }
            }
        }
    }

    for candidate in system_paths {
        if is_executable_file(candidate) {
            return Ok(candidate.clone());
        }
    }

    for root in playwright_roots {
        if let Some(found) = find_playwright_chromium(root) {
            return Ok(found);
        }
    }

    Err(
        "could not find a Chromium or Google Chrome executable; set `ANYAGENTS_CHROMIUM_BIN` \
         or install `chromium`, `google-chrome`, or Playwright Chromium under `/opt/playwright`"
            .to_string(),
    )
}

/// Whether `binary` looks like a dedicated `chrome-headless-shell` executable,
/// which runs headless natively and does not accept `--headless=new`.
pub fn is_headless_shell_binary(binary: &Path) -> bool {
    binary
        .file_name()
        .and_then(OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .is_some_and(|name| name.contains("headless_shell") || name.contains("headless-shell"))
}

fn default_system_paths() -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from("/usr/bin/chromium"),
        PathBuf::from("/usr/bin/chromium-browser"),
        PathBuf::from("/usr/bin/google-chrome-stable"),
        PathBuf::from("/usr/bin/google-chrome"),
        PathBuf::from("/usr/local/bin/chromium"),
        PathBuf::from("/snap/bin/chromium"),
        PathBuf::from("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        PathBuf::from("/Applications/Chromium.app/Contents/MacOS/Chromium"),
        PathBuf::from("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
    ];
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        paths.push(PathBuf::from(program_files).join("Google/Chrome/Application/chrome.exe"));
    }
    if let Some(local_app_data) = std::env::var_os("LocalAppData") {
        paths.push(PathBuf::from(local_app_data).join("Google/Chrome/Application/chrome.exe"));
    }
    paths
}

fn default_playwright_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(custom) = std::env::var_os("PLAYWRIGHT_BROWSERS_PATH")
        && !custom.is_empty()
    {
        roots.push(PathBuf::from(custom));
    }
    roots.push(PathBuf::from("/opt/playwright"));
    if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
        let home = PathBuf::from(home);
        roots.push(home.join(".cache/ms-playwright"));
        roots.push(home.join("Library/Caches/ms-playwright"));
    }
    roots
}

/// Scan a Playwright browsers directory (`chromium-*` preferred over
/// `chromium_headless_shell-*`, newest numeric revision first).
fn find_playwright_chromium(root: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    let mut full_chromium: Vec<(u64, PathBuf)> = Vec::new();
    let mut headless_shell: Vec<(u64, PathBuf)> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(rev) = name.strip_prefix("chromium-") {
            let num = rev.parse::<u64>().unwrap_or(0);
            full_chromium.push((num, path));
        } else if let Some(rev) = name.strip_prefix("chromium_headless_shell-") {
            let num = rev.parse::<u64>().unwrap_or(0);
            headless_shell.push((num, path));
        }
    }

    full_chromium.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    headless_shell.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));

    for (_, dir) in full_chromium.into_iter().chain(headless_shell) {
        for rel in PLAYWRIGHT_RELATIVE_BINS {
            let candidate = dir.join(rel);
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

pub(crate) fn is_executable_file(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_exec(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).unwrap();
        }
    }

    #[test]
    fn explicit_override_takes_precedence_and_errors_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let custom = dir.path().join("my-chromium");
        create_exec(&custom);

        let found = discover_chromium_in(Some(&custom), &[], &[], &[]).unwrap();
        assert_eq!(found, custom);

        let missing = dir.path().join("nonexistent");
        let err = discover_chromium_in(Some(&missing), &[], &[], &[]).unwrap_err();
        assert!(err.contains("does not exist or is not executable"));
    }

    #[test]
    fn discovers_chromium_from_path_before_playwright() {
        let dir = tempfile::tempdir().unwrap();
        let bin_dir = dir.path().join("bin");
        let path_chrome = bin_dir.join("google-chrome");
        create_exec(&path_chrome);

        let pw_root = dir.path().join("playwright");
        let pw_bin = pw_root.join("chromium-1140/chrome-linux/chrome");
        create_exec(&pw_bin);

        let found = discover_chromium_in(None, &[bin_dir], &[], &[pw_root]).unwrap();
        assert_eq!(found, path_chrome);
    }

    #[test]
    fn discovers_newest_playwright_chromium_over_headless_shell() {
        let dir = tempfile::tempdir().unwrap();
        let pw_root = dir.path().join("opt-playwright");
        let old_chrome = pw_root.join("chromium-1100/chrome-linux/chrome");
        let new_chrome = pw_root.join("chromium-1160/chrome-linux/chrome");
        let shell = pw_root.join("chromium_headless_shell-1200/chrome-linux/headless_shell");
        create_exec(&old_chrome);
        create_exec(&new_chrome);
        create_exec(&shell);

        let found = discover_chromium_in(None, &[], &[], &[pw_root]).unwrap();
        assert_eq!(found, new_chrome);
    }

    #[test]
    fn falls_back_to_playwright_headless_shell_when_full_chromium_absent() {
        let dir = tempfile::tempdir().unwrap();
        let pw_root = dir.path().join("opt-playwright");
        let shell = pw_root.join("chromium_headless_shell-1180/chrome-linux/headless_shell");
        create_exec(&shell);

        let found = discover_chromium_in(None, &[], &[], &[pw_root]).unwrap();
        assert_eq!(found, shell);
        assert!(is_headless_shell_binary(&found));
    }

    #[test]
    fn returns_clear_error_when_no_browser_found() {
        let err = discover_chromium_in(None, &[], &[], &[]).unwrap_err();
        assert!(err.contains("ANYAGENTS_CHROMIUM_BIN"));
        assert!(err.contains("/opt/playwright"));
    }
}
