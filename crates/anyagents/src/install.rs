//! Managed harness installation, resolution, and upgrade orchestration.
//!
//! Harnesses are installed and promoted atomically by
//! `anyagents_installer::Lifecycle` under `<data>/harnesses/<id>/<version>/`
//! with receipts (`receipt.json`, `current.json`). `PATH` fallback and legacy
//! `.installed` markers are retired; unresolved harnesses fail closed rather
//! than silently picking up an unmanaged binary.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use anyagents_core::Store;
use anyagents_harness::normalize_harness_name;
use anyagents_installer::receipt::{ReceiptState, read_current, read_receipt, version_dir};
use anyagents_installer::{Catalog, InstallError, LeaseView, Lifecycle, NoLeases, Tick};

const BUILTIN_HARNESSES: [&str; 4] = [
    "spiron-cloud",
    "claude-cloud",
    "openai-cloud",
    "cursor-cloud",
];

/// What one harness install ended as.
#[derive(Clone, Debug)]
pub struct Install {
    /// The directory this version lives in, for a caller that reports it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub dir: PathBuf,
}

/// Fail-closed [`LeaseView`] used when `<data>/state.db` exists on disk but
/// could not be opened. Returning `Err` makes garbage collection retain all
/// versions rather than assuming zero active leases.
struct FailClosedLeases(String);

impl LeaseView for FailClosedLeases {
    fn leased_versions(
        &self,
        _harness: &str,
    ) -> Result<std::collections::BTreeSet<String>, String> {
        Err(self.0.clone())
    }
}

fn leases_for_root(root: &Path) -> Arc<dyn LeaseView> {
    let db_path = root.join("state.db");
    if !db_path.exists() {
        return Arc::new(NoLeases);
    }
    match Store::open(&db_path) {
        Ok(store) => Arc::new(store) as Arc<dyn LeaseView>,
        Err(err) => Arc::new(FailClosedLeases(format!(
            "open {} for harness leases: {err}",
            db_path.display()
        ))) as Arc<dyn LeaseView>,
    }
}

/// The harnesses one server has installed, keyed by canonical harness id.
pub struct Installer {
    root: PathBuf,
    leases: Arc<dyn LeaseView>,
    done: Mutex<HashMap<String, Result<Install, String>>>,
}

impl Installer {
    pub fn new(root: &Path) -> Self {
        Self::with_leases(root, leases_for_root(root))
    }

    pub fn with_leases(root: &Path, leases: Arc<dyn LeaseView>) -> Self {
        Self {
            root: root.to_path_buf(),
            leases,
            done: Mutex::new(HashMap::new()),
        }
    }

    /// Install `harness` once. Every agent of that harness gets the same
    /// answer, and a failure is remembered rather than retried.
    pub fn ensure(&self, harness: &str) -> Result<Install, String> {
        let harness = normalize_harness_name(harness);
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(answer) = done.get(harness) {
            return answer.clone();
        }
        let outcome = self.ensure_uncached(harness);
        done.insert(harness.to_string(), outcome.clone());
        outcome
    }

    fn ensure_uncached(&self, harness: &str) -> Result<Install, String> {
        let lifecycle = Lifecycle::production(&self.root, Arc::clone(&self.leases))
            .map_err(|err| err.to_string())?;
        match lifecycle.ensure(harness) {
            Ok(installed) => Ok(Install { dir: installed.dir }),
            Err(InstallError::NotManaged(_)) => Ok(Install {
                dir: self.root.join("harnesses").join(harness).join("builtin"),
            }),
            Err(err) => Err(err.to_string()),
        }
    }
}

/// Verify that `harness` names a known catalog entry.
pub fn adapter(harness: &str) -> Result<(), anyagents_core::Error> {
    let harness = normalize_harness_name(harness);
    if Catalog::builtin().get(harness).is_some() {
        Ok(())
    } else {
        Err(anyagents_core::Error::Message(format!(
            "unknown harness: {harness}"
        )))
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
    let harness = normalize_harness_name(harness);
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

/// Deprecated per-harness binary override variable name for `harness`.
fn env_override_var(harness: &str) -> String {
    format!("ANYAGENTS_{}_BIN", harness.to_uppercase().replace('-', "_"))
}

/// Return the verified launcher path for `harness`, if `current.json` points
/// to an `Ok` receipt whose launcher file exists on disk.
fn verified_launcher(root: &Path, harness: &str) -> Option<(String, PathBuf)> {
    let harnesses = root.join("harnesses");
    let current = read_current(&harnesses, harness).ok()??;
    let receipt = read_receipt(&harnesses, harness, &current.version).ok()??;
    if receipt.state != ReceiptState::Ok {
        return None;
    }
    let launcher = version_dir(&harnesses, harness, &current.version).join(&receipt.launcher);
    if launcher.is_file() {
        Some((current.version, launcher))
    } else {
        None
    }
}

/// Whether `harness` has a healthy verified installation on disk (or runs
/// built-in inside this binary).
pub fn is_healthy(root: &Path, harness: &str) -> bool {
    let harness = normalize_harness_name(harness);
    BUILTIN_HARNESSES.contains(&harness) || verified_launcher(root, harness).is_some()
}

/// Version recorded by the current healthy managed receipt, `"builtin"` for
/// built-in cloud harnesses, or `"-"` when nothing is installed.
pub fn recorded_version(root: &Path, harness: &str) -> String {
    let harness = normalize_harness_name(harness);
    if BUILTIN_HARNESSES.contains(&harness) {
        return "builtin".to_string();
    }
    verified_launcher(root, harness)
        .map(|(version, _)| version)
        .unwrap_or_else(|| "-".to_string())
}

/// Version of `harness` as recorded in the managed registry.
pub fn detect_version(root: &Path, harness: &str) -> String {
    recorded_version(root, harness)
}

/// Launcher program for `harness`:
/// 1. Deprecated `ANYAGENTS_<ID>_BIN` environment override (logged once per call;
///    scheduled for removal in W2 Harness-boundary — no new caller may rely on it).
/// 2. Current executable for built-in cloud harnesses.
/// 3. Verified `current.json` + `Ok` receipt launcher under `<root>/harnesses/`.
/// 4. Uninstalled sentinel path under `<root>/harnesses/<id>/current/<id>` so
///    callers never silently fall back to `PATH`.
pub fn program(root: &Path, harness: &str) -> PathBuf {
    let harness = normalize_harness_name(harness);
    let var = env_override_var(harness);
    if let Some(named) = std::env::var_os(&var) {
        eprintln!("anyagents-install: {var} is declared-by-env (deprecated; removed in W2)");
        return PathBuf::from(named);
    }
    if BUILTIN_HARNESSES.contains(&harness) {
        return std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents"));
    }
    if let Some((_, launcher)) = verified_launcher(root, harness) {
        return launcher;
    }
    root.join("harnesses")
        .join(harness)
        .join("current")
        .join(harness)
}

/// Check for and promote an updated release of `harness` using store leases
/// loaded from `<root>/state.db` (failing closed if `state.db` is unreadable).
/// Returns `(before, after, updated)`.
pub fn update_harness(root: &Path, harness: &str) -> Result<(String, String, bool), String> {
    update_harness_with_store(root, harness, leases_for_root(root))
}

/// Check for and promote an updated release of `harness` against an explicit
/// [`LeaseView`] (such as the server's live `Store`).
pub fn update_harness_with_store(
    root: &Path,
    harness: &str,
    leases: Arc<dyn LeaseView>,
) -> Result<(String, String, bool), String> {
    let harness = normalize_harness_name(harness);
    let lifecycle = Lifecycle::production(root, leases).map_err(|err| err.to_string())?;
    match lifecycle.tick(harness) {
        Ok(Tick::Promoted { from, to }) => Ok((from.unwrap_or_else(|| "-".to_string()), to, true)),
        Ok(Tick::Unchanged { version }) => Ok((version.clone(), version, false)),
        Ok(Tick::Refused { version, reason }) => {
            Err(format!("refused upgrade to {version}: {reason}"))
        }
        Ok(Tick::SkippedBad { version }) => Err(format!("skipped bad version {version}")),
        Err(InstallError::NotManaged(_)) => {
            Ok(("builtin".to_string(), "builtin".to_string(), false))
        }
        Err(err) => Err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_installer::receipt::{
        Current, DoctorRecord, Receipt, write_current, write_receipt,
    };
    use sha2::{Digest, Sha256};

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    #[cfg(unix)]
    fn write_local_override(root: &Path, harness: &str, version: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let src = root.join(format!("{harness}-{version}.sh"));
        let script = format!("#!/bin/sh\necho \"{harness} {version}\"\n");
        std::fs::write(&src, script.as_bytes()).unwrap();
        std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).unwrap();
        let digest = sha256_hex(script.as_bytes());
        let toml = format!(
            "[harness.{harness}]\n\
             launcher = \"{harness}\"\n\
             source = {{ kind = \"local\", version = \"{version}\", path = {:?}, sha256 = \"{digest}\" }}\n\
             doctor = {{ version_args = [\"--version\"] }}\n",
            src.display().to_string()
        );
        std::fs::write(root.join("harnesses.toml"), toml).unwrap();
        src
    }

    #[test]
    fn claude_code_program_override_uses_the_canonical_harness_identifier() {
        let _environment = crate::server::tests::env_lock();
        let root = crate::test_tempdir();
        let expected = root.path().join("synthetic-claude-code-probe");
        let decoy = root.path().join("noncanonical-claude-probe");
        unsafe {
            std::env::set_var("ANYAGENTS_CLAUDE_BIN", &decoy);
            std::env::set_var("ANYAGENTS_CLAUDE_CODE_BIN", &expected);
        }
        assert_eq!(program(root.path(), "claude-code"), expected);
        unsafe {
            std::env::remove_var("ANYAGENTS_CLAUDE_BIN");
            std::env::remove_var("ANYAGENTS_CLAUDE_CODE_BIN");
        }
    }

    #[cfg(unix)]
    #[test]
    fn legacy_first_party_install_and_program_selectors_use_spiron() {
        let _environment = crate::server::tests::env_lock();
        let root = crate::test_tempdir();
        write_local_override(root.path(), "spiron", "1.0.0");

        let installer = Installer::new(root.path());
        for legacy in [
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
        ] {
            adapter(legacy).unwrap();
            let installed = installer.ensure(legacy).unwrap();
            assert_eq!(installed.dir, root.path().join("harnesses/spiron/1.0.0"));
            assert!(program(root.path(), legacy).starts_with(&installed.dir));
            assert!(is_healthy(root.path(), legacy));
            assert_eq!(recorded_version(root.path(), legacy), "1.0.0");
        }
        let done = installer.done.lock().unwrap();
        assert_eq!(done.len(), 1);
        assert!(done.contains_key("spiron"));
    }

    #[cfg(unix)]
    #[test]
    fn a_harness_that_answered_once_keeps_that_answer() {
        let root = crate::test_tempdir();
        write_local_override(root.path(), "opencode", "1.0.0");
        let installer = Installer::new(root.path());
        let first = installer.ensure("opencode").unwrap();
        let second = installer.ensure("opencode").unwrap();
        assert_eq!(first.dir, second.dir);
        assert!(installer.done.lock().unwrap().contains_key("opencode"));
    }

    #[test]
    fn unconfigured_harness_fails_closed_without_path_fallback() {
        let _environment = crate::server::tests::env_lock();
        let root = crate::test_tempdir();
        let installer = Installer::new(root.path());
        let err = installer.ensure("agy").unwrap_err();
        assert!(!err.is_empty());
        let resolved = program(root.path(), "agy");
        assert!(resolved.starts_with(root.path().join("harnesses/agy")));
        assert!(!resolved.is_file());
        assert!(!is_healthy(root.path(), "agy"));
        assert_eq!(recorded_version(root.path(), "agy"), "-");
    }

    #[test]
    fn program_resolves_only_healthy_receipted_current_version() {
        let _environment = crate::server::tests::env_lock();
        let root = crate::test_tempdir();
        let harnesses = root.path().join("harnesses");
        let ver_dir = version_dir(&harnesses, "codex", "1.2.3");
        let launcher = ver_dir.join("bin/codex");
        std::fs::create_dir_all(launcher.parent().unwrap()).unwrap();
        std::fs::write(&launcher, "").unwrap();

        // Without current.json + Ok receipt, binary is not resolved.
        assert!(!is_healthy(root.path(), "codex"));
        assert!(!program(root.path(), "codex").is_file());

        write_current(
            &harnesses,
            "codex",
            &Current {
                version: "1.2.3".to_string(),
                previous: None,
            },
        )
        .unwrap();
        write_receipt(
            &harnesses,
            "codex",
            &Receipt {
                version: "1.2.3".to_string(),
                source: "pinned".to_string(),
                digest: "sha256:00".to_string(),
                launcher: "bin/codex".to_string(),
                installed_at: 1,
                doctor: Some(DoctorRecord {
                    ok: false,
                    at: 1,
                    detail: "boom".to_string(),
                }),
                confirmed_at: None,
                state: ReceiptState::Bad,
                reason: "doctor failed".to_string(),
            },
        )
        .unwrap();
        assert!(!is_healthy(root.path(), "codex"));
        assert_eq!(recorded_version(root.path(), "codex"), "-");

        write_receipt(
            &harnesses,
            "codex",
            &Receipt {
                version: "1.2.3".to_string(),
                source: "pinned".to_string(),
                digest: "sha256:00".to_string(),
                launcher: "bin/codex".to_string(),
                installed_at: 1,
                doctor: Some(DoctorRecord {
                    ok: true,
                    at: 1,
                    detail: "ok".to_string(),
                }),
                confirmed_at: Some(2),
                state: ReceiptState::Ok,
                reason: String::new(),
            },
        )
        .unwrap();
        assert!(is_healthy(root.path(), "codex"));
        assert_eq!(recorded_version(root.path(), "codex"), "1.2.3");
        assert_eq!(program(root.path(), "codex"), launcher);
    }

    #[cfg(unix)]
    #[test]
    fn update_harness_promotes_local_override_upgrade() {
        let root = crate::test_tempdir();
        write_local_override(root.path(), "spiron", "1.0.0");
        let first = update_harness(root.path(), "spiron").unwrap();
        assert_eq!(first, ("-".to_string(), "1.0.0".to_string(), true));

        let same = update_harness(root.path(), "spiron").unwrap();
        assert_eq!(same, ("1.0.0".to_string(), "1.0.0".to_string(), false));

        write_local_override(root.path(), "spiron", "1.1.0");
        let second = update_harness(root.path(), "spiron").unwrap();
        assert_eq!(second, ("1.0.0".to_string(), "1.1.0".to_string(), true));
        assert_eq!(recorded_version(root.path(), "spiron"), "1.1.0");
    }

    #[test]
    fn fail_closed_leases_rejects_corrupt_state_db() {
        let root = crate::test_tempdir();
        std::fs::write(root.path().join("state.db"), b"not a sqlite db").unwrap();
        let leases = leases_for_root(root.path());
        assert!(leases.leased_versions("codex").is_err());
    }

    #[test]
    fn an_unknown_harness_has_no_adapter() {
        assert!(adapter("codex").is_ok());
        assert!(adapter("nope").is_err());
    }
}
