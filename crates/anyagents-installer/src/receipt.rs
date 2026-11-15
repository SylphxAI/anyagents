//! The records on disk that say what is installed and which version runs.
//!
//! - `<harnesses>/<id>/<version>/receipt.json`: one per installed version.
//! - `<harnesses>/<id>/current.json`: `{version, previous}`, the only selector.
//! - `<harnesses>/<id>/bad.json`: versions that failed doctor or their first
//!   turn. It outlives the version directory, so a bad version is never
//!   re-promoted even after it is collected.
//!
//! Every record is replaced by an atomic rename; a reader sees the old record
//! or the new one, never a torn one. An unreadable record is an error, never
//! "absent".

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::InstallError;

pub const RECEIPT: &str = "receipt.json";
pub const CURRENT: &str = "current.json";
pub const BAD: &str = "bad.json";

/// Whether a version may run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReceiptState {
    Ok,
    Bad,
}

/// The outcome of the last doctor run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorRecord {
    pub ok: bool,
    /// Milliseconds since the Unix epoch.
    pub at: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

/// What one installed version is and how it was verified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub version: String,
    /// The source label, e.g. `npm:<package>`, or `adopted` for an install
    /// that predates receipts.
    pub source: String,
    /// The verified digest: `sha256:<hex>`, `sha512-<base64>`, or for an
    /// adopted install `tree-sha256:<hex>` over the whole directory.
    pub digest: String,
    /// The launcher, relative to the version directory.
    pub launcher: String,
    /// Milliseconds since the Unix epoch.
    pub installed_at: u64,
    /// `None` until doctor has run (an adopted install has never run it here).
    #[serde(default)]
    pub doctor: Option<DoctorRecord>,
    /// When the first real turn on this version succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_at: Option<u64>,
    pub state: ReceiptState,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

/// The selector: which version runs, and which one a rollback returns to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Current {
    pub version: String,
    #[serde(default)]
    pub previous: Option<String>,
}

/// One version that must never be promoted again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BadVersion {
    pub version: String,
    pub reason: String,
    pub at: u64,
}

fn io(path: &Path, err: impl std::fmt::Display) -> InstallError {
    InstallError::Io(format!("{}: {err}", path.display()))
}

/// Read a JSON record. `Ok(None)` only when the file does not exist.
pub fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, InstallError> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| InstallError::Corrupt(format!("{}: {err}", path.display()))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(io(path, err)),
    }
}

/// Replace a JSON record atomically.
pub fn write<T: Serialize>(path: &Path, value: &T) -> Result<(), InstallError> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|err| io(path, err))?;
    bytes.push(b'\n');
    anyagents_harness::atomic_write_nonempty_file(path, &bytes, 0o644).map_err(|err| io(path, err))
}

/// The directory of one managed entry: `<harnesses>/<id>/`.
pub fn entry_dir(harnesses: &Path, id: &str) -> PathBuf {
    harnesses.join(id)
}

/// The directory of one version: `<harnesses>/<id>/<version>/`.
pub fn version_dir(harnesses: &Path, id: &str, version: &str) -> PathBuf {
    harnesses.join(id).join(version)
}

pub fn read_receipt(
    harnesses: &Path,
    id: &str,
    version: &str,
) -> Result<Option<Receipt>, InstallError> {
    read(&version_dir(harnesses, id, version).join(RECEIPT))
}

pub fn write_receipt(harnesses: &Path, id: &str, receipt: &Receipt) -> Result<(), InstallError> {
    write(
        &version_dir(harnesses, id, &receipt.version).join(RECEIPT),
        receipt,
    )
}

pub fn read_current(harnesses: &Path, id: &str) -> Result<Option<Current>, InstallError> {
    read(&entry_dir(harnesses, id).join(CURRENT))
}

pub fn write_current(harnesses: &Path, id: &str, current: &Current) -> Result<(), InstallError> {
    write(&entry_dir(harnesses, id).join(CURRENT), current)
}

pub fn read_bad(harnesses: &Path, id: &str) -> Result<Vec<BadVersion>, InstallError> {
    Ok(read(&entry_dir(harnesses, id).join(BAD))?.unwrap_or_default())
}

pub fn add_bad(harnesses: &Path, id: &str, bad: BadVersion) -> Result<(), InstallError> {
    let mut list = read_bad(harnesses, id)?;
    if !list.iter().any(|b| b.version == bad.version) {
        list.push(bad);
        list.sort_by(|a, b| a.version.cmp(&b.version));
        write(&entry_dir(harnesses, id).join(BAD), &list)?;
    }
    Ok(())
}

/// Every version directory that carries a receipt. Directories without one
/// (older layouts, staging, collection in progress) are not managed and are
/// never touched.
pub fn receipts(harnesses: &Path, id: &str) -> Result<Vec<Receipt>, InstallError> {
    let dir = entry_dir(harnesses, id);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io(&dir, err)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| io(&dir, err))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') || !entry.path().is_dir() {
            continue;
        }
        if let Some(receipt) = read::<Receipt>(&entry.path().join(RECEIPT))? {
            out.push(receipt);
        }
    }
    out.sort_by(|a, b| {
        a.installed_at
            .cmp(&b.installed_at)
            .then(a.version.cmp(&b.version))
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_corrupt_selector_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("h")).unwrap();
        assert_eq!(read_current(dir.path(), "h").unwrap(), None);
        std::fs::write(dir.path().join("h").join(CURRENT), "{").unwrap();
        assert!(matches!(
            read_current(dir.path(), "h"),
            Err(InstallError::Corrupt(_))
        ));
    }

    #[test]
    fn a_bad_version_is_recorded_once() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("h")).unwrap();
        for _ in 0..2 {
            add_bad(
                dir.path(),
                "h",
                BadVersion {
                    version: "2".into(),
                    reason: "doctor".into(),
                    at: 1,
                },
            )
            .unwrap();
        }
        assert_eq!(read_bad(dir.path(), "h").unwrap().len(), 1);
    }
}
