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
        let catalog = builtin_catalog();
        let mut runner = ProcessRunner;
        let version = match catalog_version(&catalog, harness, &mut runner) {
            Ok(version) => version,
            Err(InstallError::UnknownHarness(id)) => return Err(format!("unknown harness: {id}")),
            Err(err) => return Err(err.to_string()),
        };
        let dir = ensure_installed(&catalog, &self.root, harness, &version, &mut runner)
            .map_err(|err| err.to_string())?;
        // A copy of this version may already be in the private prefix from a
        // build without a launcher on `PATH`; either way this version is there.
        Ok(Install { dir })
    }
}

/// Whether any adapter can run this harness, and the reason when none can.
pub fn adapter(harness: &str) -> Result<(), anyagents_core::Error> {
    match harness {
        "claude-code" | "codex" | "opencode" | "agy" | "muse" => Ok(()),
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
        installed.sort();
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
    which(name).unwrap_or_else(|| PathBuf::from(name))
}

/// The launcher a package installed into `installed`, or the plain binary name.
fn launcher(harness: &str, installed: &Path) -> PathBuf {
    let names: &[&str] = match harness {
        "claude-code" => &["claude"],
        "codex" => &["codex"],
        // The OpenCode package ships its launcher under either name.
        "opencode" => &["opencode", "opencode-ai"],
        "agy" => &["agy"],
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
