//! The managed-binary lifecycle: install, adopt, smoke-check (`doctor`),
//! atomic promotion, first-turn confirmation and rollback, lease-aware garbage
//! collection, and sign-in command construction.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyagents_harness::normalize_harness_name;

use crate::InstallError;
use crate::catalog::{Catalog, CatalogEntry};
use crate::exec::{Exec, ProcessExec};
use crate::fetch::{Fetch, HttpFetch};
use crate::platform::Platform;
use crate::receipt::{
    BadVersion, Current, DoctorRecord, Receipt, ReceiptState, add_bad, entry_dir, read_bad,
    read_current, read_receipt, receipts, version_dir, write_current, write_receipt,
};
use crate::source::{Artifact, Release, Source, check_version, resolve};
use crate::unpack::unpack;
use crate::verify::{Digest, HashingWriter, check, sha256_file, sha256_tree};

const DEFAULT_DOCTOR_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_NPM_TIMEOUT: Duration = Duration::from_secs(300);

static SEQ: AtomicU64 = AtomicU64::new(1);

/// Reads the versions of a harness that running processes currently hold leases
/// on. An `Err` must never be treated as an empty set by garbage collection.
pub trait LeaseView: Send + Sync {
    fn leased_versions(&self, harness: &str) -> Result<BTreeSet<String>, String>;
}

impl LeaseView for anyagents_core::Store {
    fn leased_versions(&self, harness: &str) -> Result<BTreeSet<String>, String> {
        self.leased_versions(harness).map_err(|err| err.to_string())
    }
}

/// A [`LeaseView`] that reports no leases. Only for contexts where no store
/// exists (such as standalone tests without active processes).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLeases;

impl LeaseView for NoLeases {
    fn leased_versions(&self, _: &str) -> Result<BTreeSet<String>, String> {
        Ok(BTreeSet::new())
    }
}

/// A verified, healthy managed binary ready for execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Installed {
    pub id: String,
    pub version: String,
    pub dir: PathBuf,
    pub launcher: PathBuf,
    pub runtime: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

/// A sign-in command built against the current managed launcher.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invocation {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

/// The outcome of an upgrade check for one harness or runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tick {
    Unchanged { version: String },
    Promoted { from: Option<String>, to: String },
    Refused { version: String, reason: String },
    SkippedBad { version: String },
}

/// On-disk state for one managed entry, served to `doctor` and `list`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessStatus {
    pub id: String,
    pub current: Option<Current>,
    pub receipts: Vec<Receipt>,
    pub bad: Vec<BadVersion>,
}

/// Owns the `<data>/harnesses/` directory and runs the install/upgrade state
/// machine for every managed harness and runtime.
pub struct Lifecycle {
    pub harnesses: PathBuf,
    pub catalog: Catalog,
    pub fetch: Arc<dyn Fetch>,
    pub exec: Arc<dyn Exec>,
    pub leases: Arc<dyn LeaseView>,
    pub platform: Platform,
    pub doctor_timeout: Duration,
}

impl Lifecycle {
    pub fn new(
        harnesses: impl Into<PathBuf>,
        catalog: Catalog,
        fetch: Arc<dyn Fetch>,
        exec: Arc<dyn Exec>,
        leases: Arc<dyn LeaseView>,
        platform: Platform,
    ) -> Self {
        Self {
            harnesses: harnesses.into(),
            catalog,
            fetch,
            exec,
            leases,
            platform,
            doctor_timeout: DEFAULT_DOCTOR_TIMEOUT,
        }
    }

    /// Construct a production lifecycle rooted at `data`, loading
    /// `<data>/harnesses.toml` if present and targeting the host platform.
    pub fn production(data: &Path, leases: Arc<dyn LeaseView>) -> Result<Self, InstallError> {
        Ok(Self::new(
            data.join("harnesses"),
            Catalog::load(data)?,
            Arc::new(HttpFetch::new()),
            Arc::new(ProcessExec),
            leases,
            Platform::host()?,
        ))
    }

    pub fn with_doctor_timeout(mut self, timeout: Duration) -> Self {
        self.doctor_timeout = timeout;
        self
    }

    /// Return the currently healthy version of `id`, adopting an existing
    /// unreceipted layout or installing the latest verified release if none
    /// exists yet.
    pub fn ensure(&self, id: &str) -> Result<Installed, InstallError> {
        let entry = self.managed_entry(id)?;
        let runtime = self.ensure_runtime(entry)?;
        let npm_node = self.ensure_npm_node(entry, runtime.as_ref())?;
        let _lock = self.lock_entry(&entry.id)?;

        if let Some(current) = read_current(&self.harnesses, &entry.id)? {
            return self.installed_from_current(entry, &current.version, runtime);
        }

        if let Some(adopted) = self.adopt_locked(entry, runtime.clone())? {
            return Ok(adopted);
        }

        self.install_initial_locked(entry, runtime, npm_node.as_ref())
    }

    /// Check for a newer release of `id`, stage and doctor it, atomically
    /// promote `current`, and garbage-collect unleased older versions.
    pub fn tick(&self, id: &str) -> Result<Tick, InstallError> {
        let entry = self.managed_entry(id)?;
        let runtime = self.ensure_runtime(entry)?;
        let npm_node = self.ensure_npm_node(entry, runtime.as_ref())?;
        let _lock = self.lock_entry(&entry.id)?;

        let release = resolve(
            &entry.id,
            &entry.source,
            &self.platform,
            self.fetch.as_ref(),
        )?;
        let bad = read_bad(&self.harnesses, &entry.id)?;
        if bad.iter().any(|b| b.version == release.version) {
            return Ok(Tick::SkippedBad {
                version: release.version,
            });
        }

        let prev_current = read_current(&self.harnesses, &entry.id)?;
        if let Some(cur) = &prev_current
            && cur.version == release.version
            && let Some(receipt) = read_receipt(&self.harnesses, &entry.id, &cur.version)?
            && receipt.state == ReceiptState::Ok
            && self.launcher_path(entry, &cur.version).is_file()
        {
            let _ = self.gc_locked(&entry.id);
            return Ok(Tick::Unchanged {
                version: release.version,
            });
        }

        match self.stage_and_doctor_locked(entry, &release, runtime.as_ref(), npm_node.as_ref())? {
            DoctorOutcome::Passed => {
                let from = prev_current.map(|c| c.version);
                write_current(
                    &self.harnesses,
                    &entry.id,
                    &Current {
                        version: release.version.clone(),
                        previous: from.clone(),
                    },
                )?;
                // Promotion succeeded; a lease-read error leaves older dirs intact.
                let _ = self.gc_locked(&entry.id);
                Ok(Tick::Promoted {
                    from,
                    to: release.version,
                })
            }
            DoctorOutcome::Failed(reason) => Ok(Tick::Refused {
                version: release.version,
                reason,
            }),
        }
    }

    /// Record the outcome of the first real turn on `version`. A failure on an
    /// unconfirmed version marks it bad and rolls `current` back to `previous`
    /// when healthy. Returns whether state changed.
    pub fn confirm(&self, id: &str, version: &str, ok: bool) -> Result<bool, InstallError> {
        let entry = self.entry(id)?;
        let _lock = self.lock_entry(&entry.id)?;
        let Some(mut receipt) = read_receipt(&self.harnesses, &entry.id, version)? else {
            return Ok(false);
        };
        if ok {
            if receipt.confirmed_at.is_none() && receipt.state == ReceiptState::Ok {
                receipt.confirmed_at = Some(now_ms());
                write_receipt(&self.harnesses, &entry.id, &receipt)?;
                return Ok(true);
            }
            return Ok(false);
        }

        if receipt.confirmed_at.is_some() {
            return Ok(false);
        }

        let reason = "first turn failed".to_string();
        let at = now_ms();
        receipt.state = ReceiptState::Bad;
        receipt.reason = reason.clone();
        write_receipt(&self.harnesses, &entry.id, &receipt)?;
        add_bad(
            &self.harnesses,
            &entry.id,
            BadVersion {
                version: version.to_string(),
                reason,
                at,
            },
        )?;

        if let Some(cur) = read_current(&self.harnesses, &entry.id)?
            && cur.version == version
            && let Some(prev) = cur.previous
            && let Some(prev_receipt) = read_receipt(&self.harnesses, &entry.id, &prev)?
            && prev_receipt.state == ReceiptState::Ok
            && self.launcher_path(entry, &prev).is_file()
        {
            write_current(
                &self.harnesses,
                &entry.id,
                &Current {
                    version: prev,
                    previous: None,
                },
            )?;
        }
        Ok(true)
    }

    /// Remove old version directories of `id` that have receipts and are
    /// neither `current`, `previous`, nor named by any active lease.
    pub fn gc(&self, id: &str) -> Result<usize, InstallError> {
        let entry = self.entry(id)?;
        let _lock = self.lock_entry(&entry.id)?;
        self.gc_locked(&entry.id)
    }

    /// Build the vendor sign-in command against the current managed launcher
    /// (installing on demand), pointed at `staging_dir`.
    pub fn sign_in_invocation(
        &self,
        id: &str,
        staging_dir: &Path,
    ) -> Result<Invocation, InstallError> {
        let entry = self.managed_entry(id)?;
        let installed = self.ensure(&entry.id)?;
        let (_, sign_in_args) = entry
            .sign_in_command
            .split_first()
            .ok_or_else(|| InstallError::Config(format!("no sign-in command for {}", entry.id)))?;
        let (program, args) = if let Some(rt) = installed.runtime {
            let mut full = Vec::with_capacity(sign_in_args.len() + 1);
            full.push(installed.launcher.display().to_string());
            full.extend(sign_in_args.iter().cloned());
            (rt, full)
        } else {
            (installed.launcher, sign_in_args.to_vec())
        };
        let mut env = installed.env;
        if let Some(var) = &entry.config_var {
            env.insert(var.clone(), staging_dir.display().to_string());
        }
        Ok(Invocation { program, args, env })
    }

    /// Read the recorded state of `id` (current selector, receipts, bad list).
    pub fn status(&self, id: &str) -> Result<HarnessStatus, InstallError> {
        let entry = self.entry(id)?;
        Ok(HarnessStatus {
            id: entry.id.clone(),
            current: read_current(&self.harnesses, &entry.id)?,
            receipts: receipts(&self.harnesses, &entry.id)?,
            bad: read_bad(&self.harnesses, &entry.id)?,
        })
    }

    fn entry(&self, id: &str) -> Result<&CatalogEntry, InstallError> {
        self.catalog
            .get(id)
            .ok_or_else(|| InstallError::UnknownHarness(normalize_harness_name(id).to_string()))
    }

    fn managed_entry(&self, id: &str) -> Result<&CatalogEntry, InstallError> {
        let entry = self.entry(id)?;
        if !entry.source.is_managed() {
            return Err(InstallError::NotManaged(entry.id.clone()));
        }
        Ok(entry)
    }

    fn ensure_runtime(&self, entry: &CatalogEntry) -> Result<Option<Installed>, InstallError> {
        let Some(rt_id) = &entry.runtime else {
            return Ok(None);
        };
        if rt_id == &entry.id {
            return Err(InstallError::Config(format!(
                "{} cannot use itself as a runtime",
                entry.id
            )));
        }
        let rt_entry = self.entry(rt_id)?;
        if rt_entry.runtime.is_some() {
            return Err(InstallError::Config(format!(
                "runtime {rt_id} cannot itself declare a runtime"
            )));
        }
        Ok(Some(self.ensure(rt_id)?))
    }

    fn ensure_npm_node(
        &self,
        entry: &CatalogEntry,
        runtime: Option<&Installed>,
    ) -> Result<Option<Installed>, InstallError> {
        if !matches!(entry.source, Source::NpmTree { .. }) {
            return Ok(None);
        }
        if entry.id == "node" {
            return Err(InstallError::Config(
                "node runtime cannot use an npm-tree source".to_string(),
            ));
        }
        if let Some(rt) = runtime
            && rt.id == "node"
        {
            return Ok(Some(rt.clone()));
        }
        Ok(Some(self.ensure("node")?))
    }

    fn lock_entry(&self, id: &str) -> Result<File, InstallError> {
        let dir = entry_dir(&self.harnesses, id);
        std::fs::create_dir_all(&dir).map_err(|e| io_err(&dir, e))?;
        let lock_path = dir.join(".lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| io_err(&lock_path, e))?;
        file.lock().map_err(|e| io_err(&lock_path, e))?;
        Ok(file)
    }

    fn launcher_rel(&self, entry: &CatalogEntry, version: &str) -> String {
        self.platform.expand(&entry.launcher, version)
    }

    fn launcher_path(&self, entry: &CatalogEntry, version: &str) -> PathBuf {
        version_dir(&self.harnesses, &entry.id, version).join(self.launcher_rel(entry, version))
    }

    fn installed_struct(
        &self,
        entry: &CatalogEntry,
        version: String,
        launcher: PathBuf,
        runtime: Option<Installed>,
    ) -> Installed {
        Installed {
            id: entry.id.clone(),
            dir: version_dir(&self.harnesses, &entry.id, &version),
            version,
            launcher,
            runtime: runtime.map(|r| r.launcher),
            env: entry.self_update_off.clone(),
        }
    }

    fn installed_from_current(
        &self,
        entry: &CatalogEntry,
        version: &str,
        runtime: Option<Installed>,
    ) -> Result<Installed, InstallError> {
        let Some(receipt) = read_receipt(&self.harnesses, &entry.id, version)? else {
            return Err(InstallError::NoHealthyVersion(format!(
                "{}: current version {version} has no receipt",
                entry.id
            )));
        };
        if receipt.state != ReceiptState::Ok {
            return Err(InstallError::NoHealthyVersion(format!(
                "{}: current version {version} is bad ({})",
                entry.id, receipt.reason
            )));
        }
        let launcher = version_dir(&self.harnesses, &entry.id, version).join(&receipt.launcher);
        if !launcher.is_file() {
            return Err(InstallError::NoHealthyVersion(format!(
                "{}: launcher {} is missing",
                entry.id,
                launcher.display()
            )));
        }
        Ok(self.installed_struct(entry, version.to_string(), launcher, runtime))
    }

    fn adopt_locked(
        &self,
        entry: &CatalogEntry,
        runtime: Option<Installed>,
    ) -> Result<Option<Installed>, InstallError> {
        let dir = entry_dir(&self.harnesses, &entry.id);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_err(&dir, err)),
        };
        let bad = read_bad(&self.harnesses, &entry.id)?;
        struct AdoptCandidate {
            installed_mtime: Option<SystemTime>,
            dir_mtime: Option<SystemTime>,
            version: String,
            dir: PathBuf,
            launcher_rel: String,
        }
        let mut best: Option<AdoptCandidate> = None;

        for item in entries {
            let item = item.map_err(|e| io_err(&dir, e))?;
            let path = item.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = item.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if name.starts_with('.') || name == "seat-bin" || name == "unpinned" {
                continue;
            }
            if check_version(&entry.id, &name).is_err() || bad.iter().any(|b| b.version == name) {
                continue;
            }
            if let Some(existing) = read_receipt(&self.harnesses, &entry.id, &name)?
                && existing.state != ReceiptState::Ok
            {
                continue;
            }
            let launcher_rel = self.launcher_rel(entry, &name);
            let launcher = path.join(&launcher_rel);
            if !launcher.is_file() {
                continue;
            }
            let installed_mtime = std::fs::metadata(path.join(".installed"))
                .and_then(|m| m.modified())
                .ok();
            let dir_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
            let candidate = AdoptCandidate {
                installed_mtime,
                dir_mtime,
                version: name,
                dir: path,
                launcher_rel,
            };
            let replace = match &best {
                None => true,
                Some(cur) => {
                    (
                        candidate.installed_mtime,
                        candidate.dir_mtime,
                        &candidate.version,
                    ) > (cur.installed_mtime, cur.dir_mtime, &cur.version)
                }
            };
            if replace {
                best = Some(candidate);
            }
        }

        let Some(AdoptCandidate {
            version,
            dir: winner_dir,
            launcher_rel,
            ..
        }) = best
        else {
            return Ok(None);
        };
        if read_receipt(&self.harnesses, &entry.id, &version)?.is_none() {
            let tree = sha256_tree(&winner_dir).map_err(|e| io_err(&winner_dir, e))?;
            write_receipt(
                &self.harnesses,
                &entry.id,
                &Receipt {
                    version: version.clone(),
                    source: "adopted".to_string(),
                    digest: format!("tree-sha256:{tree}"),
                    launcher: launcher_rel.clone(),
                    installed_at: now_ms(),
                    doctor: None,
                    confirmed_at: None,
                    state: ReceiptState::Ok,
                    reason: String::new(),
                },
            )?;
        }
        write_current(
            &self.harnesses,
            &entry.id,
            &Current {
                version: version.clone(),
                previous: None,
            },
        )?;
        let launcher = winner_dir.join(launcher_rel);
        Ok(Some(
            self.installed_struct(entry, version, launcher, runtime),
        ))
    }

    fn install_initial_locked(
        &self,
        entry: &CatalogEntry,
        runtime: Option<Installed>,
        npm_node: Option<&Installed>,
    ) -> Result<Installed, InstallError> {
        let release = resolve(
            &entry.id,
            &entry.source,
            &self.platform,
            self.fetch.as_ref(),
        )?;
        let bad = read_bad(&self.harnesses, &entry.id)?;
        if bad.iter().any(|b| b.version == release.version) {
            return Err(InstallError::NoHealthyVersion(format!(
                "{}: {} previously failed verification",
                entry.id, release.version
            )));
        }
        match self.stage_and_doctor_locked(entry, &release, runtime.as_ref(), npm_node)? {
            DoctorOutcome::Passed => {
                write_current(
                    &self.harnesses,
                    &entry.id,
                    &Current {
                        version: release.version.clone(),
                        previous: None,
                    },
                )?;
                let launcher = self.launcher_path(entry, &release.version);
                Ok(self.installed_struct(entry, release.version, launcher, runtime))
            }
            DoctorOutcome::Failed(reason) => Err(InstallError::Doctor(reason)),
        }
    }

    fn stage_and_doctor_locked(
        &self,
        entry: &CatalogEntry,
        release: &Release,
        runtime: Option<&Installed>,
        npm_node: Option<&Installed>,
    ) -> Result<DoctorOutcome, InstallError> {
        let target = version_dir(&self.harnesses, &entry.id, &release.version);
        let launcher_rel = self.launcher_rel(entry, &release.version);
        let mut existing_receipt = None;

        if target.exists() {
            match read_receipt(&self.harnesses, &entry.id, &release.version)? {
                Some(receipt) => {
                    if receipt.state == ReceiptState::Bad {
                        add_bad(
                            &self.harnesses,
                            &entry.id,
                            BadVersion {
                                version: release.version.clone(),
                                reason: receipt.reason.clone(),
                                at: now_ms(),
                            },
                        )?;
                        return Ok(DoctorOutcome::Failed(receipt.reason));
                    }
                    if !target.join(&receipt.launcher).is_file() {
                        return Err(InstallError::Corrupt(format!(
                            "{}: receipted version {} is missing launcher {}",
                            entry.id, release.version, receipt.launcher
                        )));
                    }
                    existing_receipt = Some(receipt);
                }
                None => {
                    let legacy = entry_dir(&self.harnesses, &entry.id).join(format!(
                        ".legacy-{}-{}",
                        release.version,
                        unique_tag()
                    ));
                    std::fs::rename(&target, &legacy).map_err(|e| io_err(&target, e))?;
                }
            }
        }

        let mut receipt = match existing_receipt {
            Some(r) => r,
            None => self.stage_release_locked(entry, release, &launcher_rel, &target, npm_node)?,
        };

        if receipt.doctor.as_ref().is_some_and(|d| d.ok) {
            return Ok(DoctorOutcome::Passed);
        }

        match self.run_doctor(entry, &target, &release.version, runtime) {
            Ok(()) => {
                receipt.doctor = Some(DoctorRecord {
                    ok: true,
                    at: now_ms(),
                    detail: String::new(),
                });
                receipt.state = ReceiptState::Ok;
                receipt.reason.clear();
                write_receipt(&self.harnesses, &entry.id, &receipt)?;
                Ok(DoctorOutcome::Passed)
            }
            Err(reason) => {
                let at = now_ms();
                receipt.doctor = Some(DoctorRecord {
                    ok: false,
                    at,
                    detail: reason.clone(),
                });
                receipt.state = ReceiptState::Bad;
                receipt.reason = reason.clone();
                write_receipt(&self.harnesses, &entry.id, &receipt)?;
                add_bad(
                    &self.harnesses,
                    &entry.id,
                    BadVersion {
                        version: release.version.clone(),
                        reason: reason.clone(),
                        at,
                    },
                )?;
                Ok(DoctorOutcome::Failed(reason))
            }
        }
    }

    fn stage_release_locked(
        &self,
        entry: &CatalogEntry,
        release: &Release,
        launcher_rel: &str,
        target: &Path,
        npm_node: Option<&Installed>,
    ) -> Result<Receipt, InstallError> {
        let base = entry_dir(&self.harnesses, &entry.id);
        let tag = unique_tag();
        let staging = base.join(format!(".staging-{tag}"));
        std::fs::create_dir_all(&staging).map_err(|e| io_err(&staging, e))?;
        let mut staging_guard = TempPathGuard::new(staging.clone());

        match &release.artifact {
            Artifact::Download {
                url,
                digest,
                format,
                strip,
                into,
            } => {
                let dl_path = base.join(format!(".download-{tag}"));
                let _dl_guard = TempPathGuard::new(dl_path.clone());
                let file = File::create(&dl_path).map_err(|e| io_err(&dl_path, e))?;
                let mut writer = HashingWriter::new(file, digest);
                self.fetch.download(url, &mut writer)?;
                let actual = writer.finish().map_err(|e| io_err(&dl_path, e))?;
                check(url, digest, &actual)?;
                let dest = if into.is_empty() {
                    staging.clone()
                } else {
                    staging.join(into)
                };
                unpack(&dl_path, *format, *strip, &dest, launcher_rel)?;
            }
            Artifact::Local { path, digest } => {
                if !path.is_file() {
                    return Err(InstallError::Io(format!(
                        "{}: local binary {} does not exist",
                        entry.id,
                        path.display()
                    )));
                }
                let actual_hex = sha256_file(path).map_err(|e| io_err(path, e))?;
                check(
                    &path.display().to_string(),
                    digest,
                    &Digest::Sha256(actual_hex),
                )?;
                let dest = staging.join(launcher_rel);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
                }
                std::fs::copy(path, &dest).map_err(|e| io_err(&dest, e))?;
                set_executable(&dest)?;
            }
            Artifact::NpmTree { spec, registry, .. } => {
                let node = npm_node.ok_or_else(|| {
                    InstallError::Config(format!(
                        "{}: npm-tree install requires the managed node runtime",
                        entry.id
                    ))
                })?;
                let npm_cli = node.dir.join("lib/node_modules/npm/bin/npm-cli.js");
                if !npm_cli.is_file() {
                    return Err(InstallError::Io(format!(
                        "managed node is missing {}",
                        npm_cli.display()
                    )));
                }
                let npm_tmp = base.join(format!(".npm-tmp-{tag}"));
                std::fs::create_dir_all(&npm_tmp).map_err(|e| io_err(&npm_tmp, e))?;
                let _npm_guard = TempPathGuard::new(npm_tmp.clone());

                let mut env = vec![
                    ("HOME".to_string(), npm_tmp.display().to_string()),
                    (
                        "npm_config_cache".to_string(),
                        npm_tmp.display().to_string(),
                    ),
                ];
                if let Some(bin_dir) = node.launcher.parent() {
                    env.push(("PATH".to_string(), bin_dir.display().to_string()));
                }
                let args = vec![
                    npm_cli.display().to_string(),
                    "install".to_string(),
                    "--prefix".to_string(),
                    staging.display().to_string(),
                    "--ignore-scripts".to_string(),
                    "--no-audit".to_string(),
                    "--no-fund".to_string(),
                    "--registry".to_string(),
                    registry.clone(),
                    spec.clone(),
                ];
                self.exec
                    .run(&node.launcher, &args, &env, &staging, DEFAULT_NPM_TIMEOUT)
                    .map_err(|reason| {
                        InstallError::Failed(format!("npm install {spec}: {reason}"))
                    })?;
            }
        }

        let staged_launcher = staging.join(launcher_rel);
        if !staged_launcher.is_file() {
            return Err(InstallError::Unpack(format!(
                "{} {}: missing expected launcher {launcher_rel}",
                entry.id, release.version
            )));
        }
        if entry.runtime.is_none() {
            set_executable(&staged_launcher)?;
        }
        set_tree_permissions(&staging)?;

        let receipt = Receipt {
            version: release.version.clone(),
            source: entry.source.label(),
            digest: release.artifact.digest().label(),
            launcher: launcher_rel.to_string(),
            installed_at: now_ms(),
            doctor: None,
            confirmed_at: None,
            state: ReceiptState::Ok,
            reason: String::new(),
        };
        write_receipt_at(&staging.join(crate::receipt::RECEIPT), &receipt)?;
        std::fs::rename(&staging, target).map_err(|e| io_err(target, e))?;
        staging_guard.disarm();
        Ok(receipt)
    }

    fn run_doctor(
        &self,
        entry: &CatalogEntry,
        version_dir: &Path,
        version: &str,
        runtime: Option<&Installed>,
    ) -> Result<(), String> {
        let scratch =
            entry_dir(&self.harnesses, &entry.id).join(format!(".doctor-{}", unique_tag()));
        std::fs::create_dir_all(&scratch).map_err(|e| e.to_string())?;
        let _guard = TempPathGuard::new(scratch.clone());

        let mut env_map = entry.self_update_off.clone();
        env_map.insert("HOME".to_string(), scratch.display().to_string());
        if let Some(var) = &entry.config_var {
            env_map.insert(var.clone(), scratch.display().to_string());
        }
        if let Some(rt) = runtime
            && let Some(bin_dir) = rt.launcher.parent()
        {
            env_map.insert("PATH".to_string(), bin_dir.display().to_string());
        }
        let env: Vec<(String, String)> = env_map.into_iter().collect();

        let launcher = version_dir.join(self.launcher_rel(entry, version));
        let build_cmd = |args: &[String]| -> (PathBuf, Vec<String>) {
            if let Some(rt) = runtime {
                let mut full = Vec::with_capacity(args.len() + 1);
                full.push(launcher.display().to_string());
                full.extend(args.iter().cloned());
                (rt.launcher.clone(), full)
            } else {
                (launcher.clone(), args.to_vec())
            }
        };

        if !entry.doctor.version_args.is_empty() {
            let (prog, args) = build_cmd(&entry.doctor.version_args);
            let output = self
                .exec
                .run(&prog, &args, &env, &scratch, self.doctor_timeout)?;
            if !output.contains(version) {
                return Err(format!(
                    "version output did not contain {version:?}: {}",
                    output.trim()
                ));
            }
        }

        if !entry.doctor.handshake.is_empty() {
            let (prog, args) = build_cmd(&entry.doctor.handshake);
            self.exec
                .run(&prog, &args, &env, &scratch, self.doctor_timeout)?;
        }

        Ok(())
    }

    fn gc_locked(&self, id: &str) -> Result<usize, InstallError> {
        let dir = entry_dir(&self.harnesses, id);
        if !dir.is_dir() {
            return Ok(0);
        }

        sweep_stale_temps(&dir)?;

        let Some(current) = read_current(&self.harnesses, id)? else {
            return Ok(0);
        };
        let leased = self
            .leases
            .leased_versions(id)
            .map_err(InstallError::Leases)?;

        let mut keep = BTreeSet::new();
        keep.insert(current.version);
        if let Some(prev) = current.previous {
            keep.insert(prev);
        }
        keep.extend(leased);

        let mut removed = 0usize;
        let entries = std::fs::read_dir(&dir).map_err(|e| io_err(&dir, e))?;
        for item in entries {
            let item = item.map_err(|e| io_err(&dir, e))?;
            let path = item.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = item.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if name.starts_with('.') || name == "seat-bin" || name == "unpinned" {
                continue;
            }
            let Some(_receipt) = read_receipt(&self.harnesses, id, &name)? else {
                continue;
            };
            if keep.contains(&name) {
                continue;
            }
            let trash = dir.join(format!(".gc-{name}-{}", unique_tag()));
            std::fs::rename(&path, &trash).map_err(|e| io_err(&path, e))?;
            std::fs::remove_dir_all(&trash).map_err(|e| io_err(&trash, e))?;
            removed += 1;
        }
        Ok(removed)
    }
}

enum DoctorOutcome {
    Passed,
    Failed(String),
}

struct TempPathGuard(Option<PathBuf>);

impl TempPathGuard {
    fn new(path: PathBuf) -> Self {
        Self(Some(path))
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        let Some(path) = self.0.take() else {
            return;
        };
        if path.is_dir() {
            let _ = std::fs::remove_dir_all(&path);
        } else if path.exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn sweep_stale_temps(dir: &Path) -> Result<(), InstallError> {
    let entries = std::fs::read_dir(dir).map_err(|e| io_err(dir, e))?;
    for item in entries {
        let item = item.map_err(|e| io_err(dir, e))?;
        let Some(name) = item.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let is_temp = name.starts_with(".staging-")
            || name.starts_with(".doctor-")
            || name.starts_with(".gc-")
            || name.starts_with(".download-")
            || name.starts_with(".npm-tmp-");
        if !is_temp {
            continue;
        }
        let path = item.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path).map_err(|e| io_err(&path, e))?;
        } else {
            std::fs::remove_file(&path).map_err(|e| io_err(&path, e))?;
        }
    }
    Ok(())
}

fn write_receipt_at(path: &Path, receipt: &Receipt) -> Result<(), InstallError> {
    let mut bytes = serde_json::to_vec_pretty(receipt).map_err(|e| io_err(path, e))?;
    bytes.push(b'\n');
    anyagents_harness::atomic_write_nonempty_file(path, &bytes, 0o644).map_err(|e| io_err(path, e))
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), InstallError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .map_err(|e| io_err(path, e))
}

#[cfg(not(unix))]
fn set_executable(_: &Path) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(unix)]
fn set_tree_permissions(root: &Path) -> Result<(), InstallError> {
    use std::os::unix::fs::PermissionsExt as _;
    if root.is_dir() {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| io_err(root, e))?;
        for entry in std::fs::read_dir(root).map_err(|e| io_err(root, e))? {
            let entry = entry.map_err(|e| io_err(root, e))?;
            let path = entry.path();
            if entry.file_type().map_err(|e| io_err(&path, e))?.is_dir() {
                set_tree_permissions(&path)?;
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_tree_permissions(_: &Path) -> Result<(), InstallError> {
    Ok(())
}

fn io_err(path: &Path, err: impl std::fmt::Display) -> InstallError {
    InstallError::Io(format!("{}: {err}", path.display()))
}

fn unique_tag() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{}-{seq}", std::process::id())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
