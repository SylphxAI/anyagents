//! The one place ambient binary lookup survives, during ADR 0005's migration.
//!
//! Every lookup here is an undeclared tool resolved the way the call site used
//! to resolve it (an `ANYAGENTS_*_BIN` override, a probe list, `PATH`). Each use
//! is recorded in the [`crate::ToolchainReport`] and logged once, so it can be
//! declared in `toolchain.toml`. No new rule may be added: this module is
//! deleted in the removal phase (W3), and `tests/purity.rs` holds that.

use std::path::{Path, PathBuf};

/// Where the legacy rules look besides `PATH`.
#[derive(Clone, Debug, Default)]
pub struct LegacyContext {
    /// The operator's home directory, for runtimes installed under it.
    pub host_home: Option<PathBuf>,
}

/// The program the call site would have run, or `None` when nothing matched.
pub(crate) fn resolve(name: &str, ctx: &LegacyContext) -> Option<PathBuf> {
    if let Some(var) = env_override(name)
        && let Some(value) = std::env::var_os(var)
        && !value.is_empty()
    {
        return Some(PathBuf::from(value));
    }
    match name {
        // An unset provider was always an error, never a `PATH` lookup.
        "checkout-provider" => None,
        "chromium" => [
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ]
        .iter()
        .map(PathBuf::from)
        .find(|p| p.is_file() && answers_version(p))
        .or_else(|| on_path("chromium")),
        "cloudflared" => ["/usr/local/bin/cloudflared", "/usr/bin/cloudflared"]
            .iter()
            .map(PathBuf::from)
            .find(|p| p.is_file())
            .or_else(|| on_path(name)),
        "git" => on_path(name).or_else(|| in_dirs(name, &system_dirs())),
        "node" | "bun" => in_dirs(name, &runtime_dirs(ctx)),
        _ => on_path(name),
    }
}

/// The override variable a call site honoured for this tool.
fn env_override(name: &str) -> Option<&'static str> {
    Some(match name {
        "chromium" => "ANYAGENTS_CHROMIUM_BIN",
        "docker" => "ANYAGENTS_DOCKER_BIN",
        "checkout-provider" => "ANYAGENTS_CHECKOUT_PROVIDER_BIN",
        _ => return None,
    })
}

fn answers_version(program: &Path) -> bool {
    std::process::Command::new(program)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn path_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH")
        .map(|raw| std::env::split_paths(&raw).collect())
        .unwrap_or_default()
}

/// The server's own `PATH` followed by the system directories, as helper
/// processes received it before the toolchain constructed one.
pub(crate) fn inherited_path() -> std::ffi::OsString {
    let mut dirs = path_dirs();
    dirs.extend(system_dirs());
    std::env::join_paths(dirs).unwrap_or_else(|_| "/usr/bin:/bin".into())
}

fn system_dirs() -> Vec<PathBuf> {
    ["/usr/bin", "/bin"].iter().map(PathBuf::from).collect()
}

fn runtime_dirs(ctx: &LegacyContext) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    for base in home.iter().chain(ctx.host_home.iter()) {
        dirs.push(base.join(".local/opt/node/bin"));
        dirs.push(base.join(".local/opt/bun/bin"));
    }
    if let Some(home) = &home {
        dirs.push(home.join(".local/bin"));
        dirs.push(home.join(".bun/bin"));
    }
    dirs.extend(path_dirs());
    dirs.extend(
        ["/usr/local/bin", "/usr/bin", "/bin"]
            .iter()
            .map(PathBuf::from),
    );
    dirs
}

fn on_path(name: &str) -> Option<PathBuf> {
    in_dirs(name, &path_dirs())
}

fn in_dirs(name: &str, dirs: &[PathBuf]) -> Option<PathBuf> {
    dirs.iter()
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}
