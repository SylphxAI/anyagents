//! Declared tool binaries (ADR 0005).
//!
//! A tool is resolved only from the caller-owned manifest
//! (`<data>/toolchain.toml`): an absolute path, an exact version and a sha256,
//! verified before use and re-verified when the file changes. A tool that
//! fails verification is a typed [`ToolchainError`] naming it.
//!
//! During migration an undeclared tool still resolves through [`legacy`], the
//! single remaining ambient lookup. Every such use is recorded in the
//! [`ToolchainReport`] so the deployment can declare it.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use serde::Serialize;

mod legacy;
pub mod manifest;
mod verify;

pub use legacy::LegacyContext;
pub use manifest::{Manifest, ToolSpec};
pub use verify::sha256_file;

/// Why a tool cannot be used.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolchainError {
    #[error("{tool}: not declared in the toolchain manifest (needed by {needed_by})")]
    Undeclared { tool: String, needed_by: String },
    #[error("{tool}: missing at {}", path.display())]
    Missing { tool: String, path: PathBuf },
    #[error("{tool}: checksum mismatch at {} (expected {expected}, found {actual})", path.display())]
    ChecksumMismatch {
        tool: String,
        path: PathBuf,
        expected: String,
        actual: String,
    },
    #[error("{tool}: version mismatch at {} (expected {expected}, found {actual:?})", path.display())]
    VersionMismatch {
        tool: String,
        path: PathBuf,
        expected: String,
        actual: String,
    },
    #[error("{tool}: not an executable file at {}", path.display())]
    NotExecutable { tool: String, path: PathBuf },
    #[error("toolchain manifest {} is unreadable: {reason}", path.display())]
    ManifestUnreadable { path: PathBuf, reason: String },
    #[error("{tool}: not found (undeclared, needed by {needed_by})")]
    NotFound { tool: String, needed_by: String },
}

/// How a resolved tool was found.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Provenance {
    /// Declared and verified.
    Declared,
    /// Declared, failed verification, and not enforced yet (report mode).
    DeclaredUnverified { error: ToolchainError },
    /// Not declared; found by the legacy ambient lookup.
    Legacy,
}

/// A tool that may be executed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedTool {
    pub name: String,
    pub path: PathBuf,
    pub provenance: Provenance,
}

impl ResolvedTool {
    /// The program to execute.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A command for this tool. The only way a declared tool is started.
    pub fn command(&self) -> std::process::Command {
        std::process::Command::new(&self.path)
    }
}

/// One tool's state, as `doctor` and the API show it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ToolStatus {
    pub name: String,
    pub path: PathBuf,
    pub version: String,
    pub expose: bool,
    pub enforce: bool,
    pub capability: Option<String>,
    /// `None` until first verified; then the verification outcome.
    pub verified: Option<Result<(), ToolchainError>>,
}

/// One undeclared tool that was resolved by the legacy lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LegacyUse {
    pub tool: String,
    pub resolved: Option<PathBuf>,
    pub needed_by: BTreeSet<String>,
    pub uses: u64,
}

/// Everything the toolchain knows, for `doctor`, the API and logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ToolchainReport {
    /// The manifest path, or `None` when none was loaded.
    pub manifest: Option<PathBuf>,
    /// Set when the manifest exists but cannot be used (fail closed).
    pub manifest_error: Option<ToolchainError>,
    pub tools: Vec<ToolStatus>,
    pub legacy: Vec<LegacyUse>,
    /// `false` while children inherit the server's `PATH` (no manifest).
    pub child_path_constructed: bool,
}

#[derive(Debug)]
enum Source {
    /// No manifest file: every tool is legacy.
    Absent,
    Loaded { path: PathBuf, manifest: Manifest },
    Unreadable(ToolchainError),
}

type Verified = (Option<verify::Fingerprint>, Result<PathBuf, ToolchainError>);

/// The declared toolchain of one server.
#[derive(Debug)]
pub struct Toolchain {
    source: Source,
    /// `<data>/toolchain/bin`, the first entry of every child's `PATH`.
    bin_dir: Option<PathBuf>,
    legacy: LegacyContext,
    verified: Mutex<BTreeMap<String, Verified>>,
    legacy_uses: Mutex<BTreeMap<String, LegacyUse>>,
}

impl Toolchain {
    /// No manifest: every tool resolves through the legacy lookup.
    pub fn empty() -> Self {
        Self::with_source(Source::Absent)
    }

    /// Load `<data>/toolchain.toml`. A missing file is [`Toolchain::empty`];
    /// a file that cannot be read or validated fails closed.
    pub fn load(data_dir: &Path) -> Self {
        Self::load_file(&data_dir.join(manifest::FILE_NAME))
            .with_bin_dir(data_dir.join("toolchain").join("bin"))
    }

    /// Where exposed tools are linked for children.
    pub fn with_bin_dir(mut self, bin_dir: PathBuf) -> Self {
        self.bin_dir = Some(bin_dir);
        self
    }

    /// Load one manifest file, as [`Toolchain::load`] does.
    pub fn load_file(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Self::empty(),
            Err(err) => Self::with_source(Source::Unreadable(ToolchainError::ManifestUnreadable {
                path: path.to_path_buf(),
                reason: err.to_string(),
            })),
            Ok(text) => Self::from_manifest_text(&text, path),
        }
    }

    /// Parse manifest text that came from `origin`.
    pub fn from_manifest_text(text: &str, origin: &Path) -> Self {
        Self::with_source(match Manifest::parse(text, origin) {
            Ok(manifest) => Source::Loaded {
                path: origin.to_path_buf(),
                manifest,
            },
            Err(err) => Source::Unreadable(err),
        })
    }

    fn with_source(source: Source) -> Self {
        Self {
            source,
            bin_dir: None,
            legacy: LegacyContext::default(),
            verified: Mutex::new(BTreeMap::new()),
            legacy_uses: Mutex::new(BTreeMap::new()),
        }
    }

    /// Context the legacy lookup uses while it exists.
    pub fn with_legacy_context(mut self, ctx: LegacyContext) -> Self {
        self.legacy = ctx;
        self
    }

    /// Resolve `name` for `needed_by` (a capability or call site).
    pub fn tool(&self, name: &str, needed_by: &str) -> Result<ResolvedTool, ToolchainError> {
        let manifest = match &self.source {
            Source::Unreadable(err) => return Err(err.clone()),
            Source::Loaded { manifest, .. } => Some(manifest),
            Source::Absent => None,
        };
        if let Some(spec) = manifest.and_then(|m| m.tool.get(name)) {
            return match self.verify_cached(name, spec) {
                Ok(canonical) => Ok(ResolvedTool {
                    name: name.to_string(),
                    path: canonical,
                    provenance: Provenance::Declared,
                }),
                Err(err) if spec.enforce => Err(err),
                Err(err) => Ok(ResolvedTool {
                    name: name.to_string(),
                    path: spec.path.clone(),
                    provenance: Provenance::DeclaredUnverified { error: err },
                }),
            };
        }
        if manifest.is_some_and(|m| m.enforce_undeclared) {
            return Err(ToolchainError::Undeclared {
                tool: name.to_string(),
                needed_by: needed_by.to_string(),
            });
        }
        let found = legacy::resolve(name, &self.legacy);
        self.record_legacy(name, needed_by, found.as_deref());
        match found {
            Some(path) => Ok(ResolvedTool {
                name: name.to_string(),
                path,
                provenance: Provenance::Legacy,
            }),
            None => Err(ToolchainError::NotFound {
                tool: name.to_string(),
                needed_by: needed_by.to_string(),
            }),
        }
    }

    /// The first of `names` that resolves, e.g. a JS runtime (`node`, `bun`).
    pub fn first_of(
        &self,
        names: &[&str],
        needed_by: &str,
    ) -> Result<ResolvedTool, ToolchainError> {
        let mut first_err = None;
        for name in names {
            match self.tool(name, needed_by) {
                Ok(tool) => return Ok(tool),
                Err(err) => {
                    first_err.get_or_insert(err);
                }
            }
        }
        Err(first_err.unwrap_or_else(|| ToolchainError::NotFound {
            tool: String::new(),
            needed_by: needed_by.to_string(),
        }))
    }

    /// Check an agent's whole required set before it starts. Every failure is
    /// returned, each naming its tool.
    pub fn check_required(
        &self,
        required: &[(&str, &str)],
    ) -> Result<Vec<ResolvedTool>, Vec<ToolchainError>> {
        let (ok, errs): (Vec<_>, Vec<_>) = required
            .iter()
            .map(|(name, needed_by)| self.tool(name, needed_by))
            .partition(Result::is_ok);
        if errs.is_empty() {
            Ok(ok.into_iter().map(Result::unwrap).collect())
        } else {
            Err(errs.into_iter().map(Result::unwrap_err).collect())
        }
    }

    fn verify_cached(&self, name: &str, spec: &ToolSpec) -> Result<PathBuf, ToolchainError> {
        let current = verify::fingerprint(&spec.path).map(|(_, f)| f);
        let mut cache = self.verified.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((seen, result)) = cache.get(name)
            && current.is_some()
            && *seen == current
        {
            return result.clone();
        }
        let result = verify::verify(name, spec);
        cache.insert(name.to_string(), (current, result.clone()));
        result
    }

    fn record_legacy(&self, name: &str, needed_by: &str, found: Option<&Path>) {
        let mut uses = self.legacy_uses.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = uses.entry(name.to_string()).or_insert_with(|| {
            eprintln!(
                "anyagents-toolchain: `{name}` is not declared; legacy lookup for {needed_by} resolved {}. Declare it in {}.",
                found.map_or_else(|| "nothing".to_string(), |p| p.display().to_string()),
                manifest::FILE_NAME,
            );
            LegacyUse {
                tool: name.to_string(),
                resolved: None,
                needed_by: BTreeSet::new(),
                uses: 0,
            }
        });
        entry.resolved = found.map(Path::to_path_buf);
        entry.needed_by.insert(needed_by.to_string());
        entry.uses += 1;
    }

    /// Verify every declared tool now (server start) and return the report.
    pub fn verify_all(&self) -> ToolchainReport {
        if let Source::Loaded { manifest, .. } = &self.source {
            for (name, spec) in &manifest.tool {
                let _ = self.verify_cached(name, spec);
            }
        }
        self.report()
    }

    /// The current state, without verifying anything.
    pub fn report(&self) -> ToolchainReport {
        let cache = self.verified.lock().unwrap_or_else(PoisonError::into_inner);
        let (manifest_path, manifest_error, tools) = match &self.source {
            Source::Absent => (None, None, Vec::new()),
            Source::Unreadable(err) => (None, Some(err.clone()), Vec::new()),
            Source::Loaded { path, manifest } => (
                Some(path.clone()),
                None,
                manifest
                    .tool
                    .iter()
                    .map(|(name, spec)| ToolStatus {
                        name: name.clone(),
                        path: spec.path.clone(),
                        version: spec.version.clone(),
                        expose: spec.expose,
                        enforce: spec.enforce,
                        capability: spec.capability.clone(),
                        verified: cache.get(name).map(|(_, r)| r.clone().map(|_| ())),
                    })
                    .collect(),
            ),
        };
        let legacy = self
            .legacy_uses
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        ToolchainReport {
            child_path_constructed: manifest_path.is_some(),
            manifest: manifest_path,
            manifest_error,
            tools,
            legacy,
        }
    }

    /// The `PATH` every child gets: `toolchain/bin` then the declared tail.
    /// `None` while no manifest is loaded.
    pub fn child_path(&self) -> Option<OsString> {
        let Source::Loaded { manifest, .. } = &self.source else {
            return None;
        };
        std::env::join_paths(self.bin_dir.iter().cloned().chain(manifest.child_path_tail.iter().cloned()))
            .ok()
    }

    /// The `PATH` for a helper process AnyAgents runs itself (a Git or shell
    /// admission step): [`Toolchain::child_path`], else, during migration, the
    /// legacy inherited `PATH` followed by the system directories, recorded as
    /// a legacy use of `PATH` by `needed_by`.
    pub fn exec_path(&self, needed_by: &str) -> OsString {
        if let Some(path) = self.child_path() {
            return path;
        }
        let path = legacy::inherited_path();
        self.record_legacy("PATH", needed_by, None);
        path
    }

    /// Point `bin_dir/<name>` at each exposed tool that may be used, and remove
    /// links to tools that are no longer exposed. Only symlinks are removed.
    #[cfg(unix)]
    pub fn sync_exposed(&self) -> std::io::Result<Vec<ToolchainError>> {
        let (Source::Loaded { manifest, .. }, Some(bin_dir)) = (&self.source, &self.bin_dir) else {
            return Ok(Vec::new());
        };
        std::fs::create_dir_all(bin_dir)?;
        let mut refused = Vec::new();
        let mut wanted = BTreeSet::new();
        for name in manifest.tool.iter().filter(|(_, s)| s.expose).map(|(n, _)| n) {
            let target = match self.tool(name, "child PATH") {
                Ok(tool) => tool.path,
                Err(err) => {
                    refused.push(err);
                    continue;
                }
            };
            wanted.insert(name.clone());
            let link = bin_dir.join(name);
            if std::fs::read_link(&link).ok().as_deref() == Some(target.as_path()) {
                continue;
            }
            let tmp = bin_dir.join(format!(".{name}.{}.tmp", std::process::id()));
            let _ = std::fs::remove_file(&tmp);
            std::os::unix::fs::symlink(&target, &tmp)?;
            std::fs::rename(&tmp, &link)?;
        }
        for entry in std::fs::read_dir(bin_dir)?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_link = entry.file_type().is_ok_and(|t| t.is_symlink());
            if is_link && !wanted.contains(&name) {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(refused)
    }
}

static GLOBAL: OnceLock<Toolchain> = OnceLock::new();

/// Install the server's toolchain once, at start. Later calls are ignored and
/// return the toolchain they could not install.
pub fn install_global(toolchain: Toolchain) -> Result<(), Toolchain> {
    GLOBAL.set(toolchain)
}

/// The server's toolchain; an empty (all-legacy) one when none was installed,
/// as in unit tests and short CLI commands.
pub fn global() -> &'static Toolchain {
    GLOBAL.get_or_init(Toolchain::empty)
}

/// Resolve through [`global`].
pub fn tool(name: &str, needed_by: &str) -> Result<ResolvedTool, ToolchainError> {
    global().tool(name, needed_by)
}

#[cfg(test)]
mod tests;
