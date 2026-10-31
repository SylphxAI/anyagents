//! The first time a harness is needed, the server installs it. This is the one
//! place a vendor tool is downloaded: the installer keeps the copy under
//! `<data>/harnesses/<id>/<version>/` and records it with `.installed`.
//!
//! The record also remembers a failure, so a missing `npm` is reported on the
//! first create and not retried on every one.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock, PoisonError};

use anyagents_installer::{
    CommandRunner, InstallError, LineSink, builtin_catalog, catalog_version, ensure_installed,
};

/// What one harness install ended as.
#[derive(Clone, Debug)]
pub struct Install {
    /// The directory this version lives in, for a caller that reports it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub dir: PathBuf,
}

/// The harnesses one server has installed, keyed by harness id.
pub struct Installer {
    root: PathBuf,
    done: Mutex<HashMap<String, Result<Install, String>>>,
}

impl Installer {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            done: Mutex::new(HashMap::new()),
        }
    }

    /// Install `harness` once. Every agent of that harness gets the same
    /// answer, and a failure is remembered rather than retried.
    pub fn ensure(&self, harness: &str) -> Result<Install, String> {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(answer) = done.get(harness) {
            return answer.clone();
        }
        // The lock is held across the install, so two creates of one harness
        // never run two installs at once; the second is answered by the first.
        let answer = self.install(harness);
        done.insert(harness.to_string(), answer.clone());
        answer
    }

    fn install(&self, harness: &str) -> Result<Install, String> {
        let dirs = self.root.join("harnesses").join(harness);
        if let Ok(versions) = std::fs::read_dir(&dirs) {
            let mut installed: Vec<PathBuf> = versions
                .flatten()
                .map(|entry| entry.path())
                .filter(|dir| dir.join(".installed").exists())
                .collect();
            installed.sort_by_key(|p| version_sort_key(p));
            for dir in installed.into_iter().rev() {
                if launcher(harness, &dir).is_file() {
                    return Ok(Install { dir });
                }
            }
        }
        let catalog = builtin_catalog();
        let mut runner = ProcessRunner;
        let version = match catalog_version(&catalog, harness, &mut runner) {
            Ok(version) => version,
            Err(InstallError::UnknownHarness(id)) => return Err(format!("unknown harness: {id}")),
            Err(err) => return Err(err.to_string()),
        };
        ensure_vendored_binary(&self.root, harness);
        let dir = ensure_installed(&catalog, &self.root, harness, &version, &mut runner)
            .map_err(|err| err.to_string())?;
        // A copy of this version may already be in the private prefix from a
        // build without a launcher on `PATH`; either way this version is there.
        Ok(Install { dir })
    }
}

fn version_sort_key(path: &Path) -> (u64, u64, u64, String) {
    let raw = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_start_matches('v');
    let core = raw.split('-').next().unwrap_or(raw);
    let mut parts = core.split('.');
    let major = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor, patch, raw.to_string())
}

/// Detect the currently active version string of `harness`.
pub fn detect_version(root: &Path, harness: &str) -> String {
    if harness == "sylphx" {
        return env!("CARGO_PKG_VERSION").to_string();
    }
    let dirs = root.join("harnesses").join(harness);
    if let Ok(versions) = std::fs::read_dir(&dirs) {
        let mut installed: Vec<PathBuf> = versions
            .flatten()
            .map(|entry| entry.path())
            .filter(|dir| dir.join(".installed").exists())
            .collect();
        installed.sort_by_key(|p| version_sort_key(p));
        for dir in installed.into_iter().rev() {
            if launcher(harness, &dir).is_file()
                && let Some(ver) = dir.file_name().and_then(|s| s.to_str())
                && ver != "unpinned"
                && ver != "seat-bin"
            {
                return ver.to_string();
            }
        }
    }
    let bin = program(root, harness);
    if let Ok(out) = Command::new(&bin)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        let combined = format!(
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        for token in combined.split_whitespace() {
            let cleaned = token
                .trim_matches(|c: char| c == '(' || c == ')' || c == '[' || c == ']')
                .trim_start_matches("omp/")
                .trim_start_matches('v');
            if cleaned.chars().next().is_some_and(|c| c.is_ascii_digit()) && cleaned.contains('.') {
                return cleaned.to_string();
            }
        }
    }
    "-".to_string()
}

/// Check upstream for the latest version of `harness`, install it into
/// `<root>/harnesses/<harness>/<version>/` (or update its host/staged binary),
/// and prune superseded version directories.
pub fn update_harness(root: &Path, harness: &str) -> Result<(String, String, bool), String> {
    let before = detect_version(root, harness);
    let catalog = builtin_catalog();
    let mut runner = ProcessRunner;
    let version = match catalog_version(&catalog, harness, &mut runner) {
        Ok(ver) => ver,
        Err(InstallError::UnknownHarness(id)) => return Err(format!("unknown harness: {id}")),
        Err(err) => return Err(err.to_string()),
    };
    if version != anyagents_installer::UNPINNED {
        let dir = ensure_installed(&catalog, root, harness, &version, &mut runner)
            .map_err(|err| err.to_string())?;
        if let Some(inst) = INSTALLER.get() {
            let mut done = inst.done.lock().unwrap_or_else(PoisonError::into_inner);
            done.insert(harness.to_string(), Ok(Install { dir: dir.clone() }));
        }
        // Remove older version directories under `<root>/harnesses/<harness>/` so
        // `program()` always resolves the latest version and disk stays clean.
        let dirs = root.join("harnesses").join(harness);
        if let Ok(entries) = std::fs::read_dir(&dirs) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p == dir || !p.is_dir() {
                    continue;
                }
                let name = entry.file_name();
                let name_s = name.to_string_lossy();
                if name_s != "seat-bin" && name_s != "unpinned" {
                    let _ = std::fs::remove_dir_all(&p);
                }
            }
        }
    } else {
        ensure_vendored_binary(root, harness);
        let _ = program(root, harness);
    }
    let after = detect_version(root, harness);
    let changed = before != after;
    Ok((before, after, changed))
}

/// Ensures a vendored or script-installed harness binary (`omp`, `dsh`, `grok`)
/// is present on the host when an agent first requests it in real mode.
fn ensure_vendored_binary(root: &Path, harness: &str) {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        return;
    }
    if std::env::var_os(format!(
        "ANYAGENTS_{}_BIN",
        harness.to_uppercase().replace('-', "_")
    ))
    .is_some()
    {
        return;
    }
    let name = match harness {
        "claude-code" => "claude",
        other => other,
    };
    if which(name).is_some() {
        return;
    }
    let Some(home) = crate::sandbox::host_home_dir() else {
        return;
    };
    let local_bin = home.join(".local/bin");
    match harness {
        "omp" => {
            let _ = Command::new("bun")
                .args(["install", "-g", "@oh-my-pi/pi-coding-agent"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let bun_omp = home.join(".bun/bin/omp");
            if bun_omp.is_file() && local_bin.is_dir() {
                let dst = local_bin.join("omp");
                #[cfg(unix)]
                if !dst.exists() {
                    let _ = std::os::unix::fs::symlink(bun_omp, &dst);
                }
            }
        }
        "dsh" => {
            if let Ok(entries) = std::fs::read_dir(home.join(".dsh")) {
                let mut candidates: Vec<PathBuf> = entries
                    .flatten()
                    .map(|e| e.path().join("node_modules/.bin/dsh"))
                    .filter(|p| p.is_file())
                    .collect();
                candidates.sort();
                if let Some(src) = candidates.pop() {
                    let dst = if local_bin.is_dir() {
                        local_bin.join("dsh")
                    } else {
                        root.join("harnesses/dsh/seat-bin/dsh")
                    };
                    if let Some(parent) = dst.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    #[cfg(unix)]
                    if !dst.exists() {
                        let _ = std::os::unix::fs::symlink(&src, &dst);
                    }
                }
            }
        }
        "grok" => {
            if let Ok(entries) = std::fs::read_dir(home.join(".local/share/grok/versions")) {
                let mut candidates: Vec<PathBuf> = entries
                    .flatten()
                    .map(|e| e.path().join("grok"))
                    .filter(|p| p.is_file())
                    .collect();
                candidates.sort();
                if let Some(src) = candidates.pop()
                    && local_bin.is_dir()
                {
                    let dst = local_bin.join("grok");
                    #[cfg(unix)]
                    if !dst.exists() {
                        let _ = std::os::unix::fs::symlink(&src, &dst);
                    }
                    return;
                }
            }
            let _ = Command::new("sh")
                .args(["-c", "curl -fsSL https://cli.grok.com/install.sh | bash"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        _ => {}
    }
}

/// Whether any adapter can run this harness, and the reason when none can.
pub fn adapter(harness: &str) -> Result<(), anyagents_core::Error> {
    match harness {
        "claude-code" | "codex" | "opencode" | "agy" | "muse" | "omp" | "dsh" | "grok"
        | "sylphx" | "sylphx-agent" | "sylphx_agent" => Ok(()),
        other => Err(anyagents_core::Error::Message(format!(
            "unknown harness: {other}"
        ))),
    }
}

static INSTALLER: OnceLock<Installer> = OnceLock::new();

/// Install a harness that has to be there now, from the data directory the
/// server was started with. It happens once per harness: a later call gets the
/// same answer.
pub fn record(root: &Path, harness: &str) -> Result<Install, String> {
    let installer = INSTALLER.get_or_init(|| Installer::new(root));
    installer.ensure(harness)
}

/// What `record` already answered for an agent's harness: `Some(Ok(()))` once it
/// is installed, `Some(Err(reason))` when it could not be, `None` while this
/// server has not needed it. It never installs anything.
pub fn recorded(harness: &str) -> Option<Result<(), String>> {
    let done = INSTALLER
        .get()?
        .done
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    done.get(harness).map(|answer| match answer {
        Ok(_) => Ok(()),
        Err(reason) => Err(format!("needed: {reason}")),
    })
}

/// The program for a harness: the launcher of the copy the server installed,
/// else the vendor program the machine already has, else the binary name the
/// operating system looks up. `ANYAGENTS_<ID>_BIN` wins, so tests never start a
/// real vendor and an unusual install can be pointed at.
pub fn program(root: &Path, harness: &str) -> PathBuf {
    if let Some(named) = std::env::var_os(format!(
        "ANYAGENTS_{}_BIN",
        harness.to_uppercase().replace('-', "_")
    )) {
        return PathBuf::from(named);
    }
    let dirs = root.join("harnesses").join(harness);
    if let Ok(versions) = std::fs::read_dir(&dirs) {
        let mut installed: Vec<PathBuf> = versions
            .flatten()
            .map(|entry| entry.path())
            .filter(|dir| dir.join(".installed").exists())
            .collect();
        installed.sort_by_key(|p| version_sort_key(p));
        for dir in installed.into_iter().rev() {
            let launcher = launcher(harness, &dir);
            if launcher.is_file() {
                return launcher;
            }
        }
    }
    let name = match harness {
        "claude-code" => "claude",
        "agy" => "agy",
        "muse" => "muse",
        other => other,
    };
    match which(name) {
        Some(found) => ensure_seat_accessible(root, harness, name, found),
        None => PathBuf::from(name),
    }
}

/// When OS seats are configured and a `PATH` binary resolves through a private
/// (`0700`) directory (such as `~/.local/bin/codex -> ~/.codex/packages/...`),
/// stage a world-executable copy under `<root>/harnesses/<harness>/seat-bin/`
/// so confined seat users can execute it without relaxing `~/.codex`.
#[cfg(unix)]
fn ensure_seat_accessible(root: &Path, harness: &str, name: &str, candidate: PathBuf) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        return candidate;
    }
    let Ok(canonical) = std::fs::canonicalize(&candidate) else {
        return candidate;
    };
    let blocked = canonical.ancestors().skip(1).any(|dir| {
        std::fs::metadata(dir)
            .map(|meta| (meta.permissions().mode() & 0o001) == 0)
            .unwrap_or(false)
    });
    if !blocked {
        return candidate;
    }
    let stage_dir = root.join("harnesses").join(harness).join("seat-bin");
    if std::fs::create_dir_all(&stage_dir).is_err() {
        return candidate;
    }
    let _ = std::fs::set_permissions(&stage_dir, std::fs::Permissions::from_mode(0o755));
    if let Some(parent) = canonical.parent()
        && let Ok(entries) = std::fs::read_dir(parent)
    {
        for entry in entries.flatten() {
            let src = entry.path();
            let Ok(src_meta) = std::fs::metadata(&src) else {
                continue;
            };
            if !src_meta.is_file() {
                continue;
            }
            let dst = stage_dir.join(entry.file_name());
            let same_size = std::fs::metadata(&dst)
                .map(|m| m.is_file() && m.len() == src_meta.len())
                .unwrap_or(false);
            if !same_size {
                let tmp = stage_dir.join(format!(
                    ".{}.{}.tmp",
                    entry.file_name().to_string_lossy(),
                    std::process::id()
                ));
                if std::fs::copy(&src, &tmp).is_ok() {
                    let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755));
                    let _ = std::fs::rename(&tmp, &dst);
                }
            }
        }
    }
    let staged = stage_dir.join(name);
    if staged.is_file() { staged } else { candidate }
}

#[cfg(not(unix))]
fn ensure_seat_accessible(
    _root: &Path,
    _harness: &str,
    _name: &str,
    candidate: PathBuf,
) -> PathBuf {
    candidate
}

/// The launcher a package installed into `installed`, or the plain binary name.
fn launcher(harness: &str, installed: &Path) -> PathBuf {
    if harness == "codex" {
        // Prefer the native static Rust binary shipped in `@openai/codex-linux-x64`
        // over the `#!/usr/bin/env node` wrapper in `node_modules/.bin/codex`, so
        // isolated agent seats with custom `HOME` do not depend on a per-seat Node
        // wrapper installation.
        let native = installed
            .join("node_modules")
            .join("@openai")
            .join("codex-linux-x64")
            .join("vendor")
            .join("x86_64-unknown-linux-musl")
            .join("bin")
            .join("codex");
        if native.is_file() {
            return native;
        }
    }
    let names: &[&str] = match harness {
        "claude-code" => &["claude"],
        "codex" => &["codex"],
        // The OpenCode package ships its launcher under either name.
        "opencode" => &["opencode", "opencode-ai"],
        "agy" => &["agy"],
        "omp" => &["omp"],
        "dsh" => &["dsh"],
        "grok" => &["grok"],
        other => &[other],
    };
    let bin = installed.join("node_modules").join(".bin");
    names
        .iter()
        .map(|name| bin.join(name))
        .find(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from(names[0]))
}

/// The first `program` on `PATH`.
fn which(program: &str) -> Option<PathBuf> {
    if program.contains('/') {
        return Some(PathBuf::from(program));
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
}

/// Runs a vendor install for real.
struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        std::fs::create_dir_all(dir).map_err(|err| InstallError::Failed(err.to_string()))?;
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
        let output = command
            .output()
            .map_err(|err| InstallError::Failed(format!("{program}: {err}")))?;
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            on_line.line(line);
        }
        if output.status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!(
                "{program} exited {}",
                output.status
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_installer::UNPINNED;

    #[test]
    fn a_vendored_harness_is_recorded_and_started_from_the_machine() {
        let root = tempfile::tempdir().unwrap();
        let installer = Installer::new(root.path());
        let install = installer.ensure("opencode").unwrap();
        assert_eq!(
            std::fs::read_to_string(install.dir.join(".installed")).unwrap(),
            UNPINNED
        );
        // Nothing was installed, so the program is the machine's own copy. The
        // test never starts a real vendor: the override names this binary.
        let double = std::env::current_exe().unwrap();
        // SAFETY: this test process owns the variable, sets it and removes it
        // inside one test, and reads it nowhere else.
        unsafe { std::env::set_var("ANYAGENTS_OPENCODE_BIN", &double) };
        assert_eq!(program(root.path(), "opencode"), double);
        unsafe { std::env::remove_var("ANYAGENTS_OPENCODE_BIN") };
    }

    #[test]
    fn a_harness_that_answered_once_keeps_that_answer() {
        let root = tempfile::tempdir().unwrap();
        let installer = Installer::new(root.path());
        let first = installer.ensure("opencode").unwrap();
        let second = installer.ensure("opencode").unwrap();
        assert_eq!(first.dir, second.dir);
        assert!(installer.done.lock().unwrap().contains_key("opencode"));
    }

    #[test]
    fn npm_is_asked_which_version_to_pin() {
        struct Answers;
        impl CommandRunner for Answers {
            fn run(
                &mut self,
                _: &str,
                _: &[String],
                _: &Path,
                _: &[(&str, &str)],
                on_line: &mut dyn LineSink,
            ) -> Result<(), InstallError> {
                on_line.line("1.2.3");
                Ok(())
            }
        }
        let version = catalog_version(&builtin_catalog(), "codex", &mut Answers).unwrap();
        assert_eq!(version, "1.2.3");
    }

    #[test]
    fn an_installed_copy_is_started_before_the_same_name_on_the_path() {
        let root = tempfile::tempdir().unwrap();
        let version = root.path().join("harnesses/codex/1.2.3");
        let launcher = version.join("node_modules/.bin/codex");
        std::fs::create_dir_all(launcher.parent().unwrap()).unwrap();
        std::fs::write(&launcher, "").unwrap();
        std::fs::write(version.join(".installed"), "1.2.3").unwrap();
        assert_eq!(program(root.path(), "codex"), launcher);
    }

    #[test]
    fn an_unknown_harness_has_no_adapter() {
        assert!(adapter("codex").is_ok());
        assert!(adapter("nope").is_err());
    }
}
