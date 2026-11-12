//! The credential store: typed records under `<data>/credentials/records`.
//!
//! Layout (ADR 0004 decision 1):
//!
//! ```text
//! <data>/credentials/                         0700, server UID only
//!   records/<kind>/<provider>/<name>/current  -> gen-<n>   (atomic symlink swap)
//!   records/<kind>/<provider>/<name>/gen-<n>/record.json   (metadata, never a secret)
//!   records/<kind>/<provider>/<name>/gen-<n>/material/...  (vendor-native files)
//!   state/<kind>/<provider>/<name>/...        probe caches, never credential bytes
//!   .staging/<id>/                            login/import scratch, never enumerated
//!   audit.jsonl                               one line per consequential write
//! ```
//!
//! Only this module writes under `records/`. A generation is immutable once it
//! is published: every change builds a complete new generation in staging and
//! publishes it with one `rename(2)` of the `current` symlink, so a crash leaves
//! either the previous or the next generation and never a mixture.

use std::fmt;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyagents_core::Error;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Generations kept besides `current` for rollback and forensics.
const RETAINED_PREVIOUS_GENERATIONS: usize = 2;

/// Credential record kinds (ADR 0004 decision 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    OauthSubscription,
    ApiKey,
    GithubUser,
    GithubApp,
    OpServiceAccount,
    ChannelBot,
    BrowserProfile,
    Kubeconfig,
    BuildKey,
}

impl Kind {
    pub const ALL: [Kind; 9] = [
        Kind::OauthSubscription,
        Kind::ApiKey,
        Kind::GithubUser,
        Kind::GithubApp,
        Kind::OpServiceAccount,
        Kind::ChannelBot,
        Kind::BrowserProfile,
        Kind::Kubeconfig,
        Kind::BuildKey,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::OauthSubscription => "oauth_subscription",
            Kind::ApiKey => "api_key",
            Kind::GithubUser => "github_user",
            Kind::GithubApp => "github_app",
            Kind::OpServiceAccount => "op_service_account",
            Kind::ChannelBot => "channel_bot",
            Kind::BrowserProfile => "browser_profile",
            Kind::Kubeconfig => "kubeconfig",
            Kind::BuildKey => "build_key",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Kind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| Error::Message(format!("unknown credential kind `{s}`")))
    }
}

/// `[a-z0-9][a-z0-9._-]*`, the grammar shared with the binding slots.
pub fn valid_segment(s: &str) -> bool {
    let mut bytes = s.bytes();
    matches!(bytes.next(), Some(b) if b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes.all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
        && s.len() <= 128
}

/// `<kind>/<provider>/<name>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RecordRef {
    pub kind: Kind,
    pub provider: String,
    pub name: String,
}

impl RecordRef {
    pub fn new(kind: Kind, provider: &str, name: &str) -> Result<Self, Error> {
        if !valid_segment(provider) {
            return Err(Error::Message(format!("invalid credential provider `{provider}`")));
        }
        if !valid_segment(name) {
            return Err(Error::Message(format!("invalid credential name `{name}`")));
        }
        Ok(Self {
            kind,
            provider: provider.to_string(),
            name: name.to_string(),
        })
    }

    fn rel(&self) -> PathBuf {
        PathBuf::from(self.kind.as_str())
            .join(&self.provider)
            .join(&self.name)
    }
}

impl fmt::Display for RecordRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.kind, self.provider, self.name)
    }
}

impl FromStr for RecordRef {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('/');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(k), Some(p), Some(n), None) => RecordRef::new(k.parse()?, p, n),
            _ => Err(Error::Message(format!(
                "expected <kind>/<provider>/<name>, got `{s}`"
            ))),
        }
    }
}

impl TryFrom<String> for RecordRef {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RecordRef> for String {
    fn from(value: RecordRef) -> Self {
        value.to_string()
    }
}

/// The account a record authenticates as.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// Stable comparison key: `id:<account id>`, `email:<lowercased email>` or
    /// `key:<sha256 prefix>` for keys without an account.
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

impl Identity {
    pub fn from_parts(email: Option<String>, account_id: Option<String>) -> Option<Self> {
        let email = email.map(|e| e.trim().to_string()).filter(|e| !e.is_empty());
        let account_id = account_id
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty());
        let subject = match (&email, &account_id) {
            (Some(e), _) => format!("email:{}", e.to_ascii_lowercase()),
            (None, Some(a)) => format!("id:{a}"),
            (None, None) => return None,
        };
        Some(Self {
            subject,
            email,
            account_id,
        })
    }

    pub fn from_key(key: &str) -> Self {
        let digest = Sha256::digest(key.trim().as_bytes());
        let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        Self {
            subject: format!("key:{hex}"),
            email: None,
            account_id: None,
        }
    }

    /// Two identities name the same account when either their account ids or
    /// their e-mail addresses match.
    pub fn same_account(&self, other: &Identity) -> bool {
        if let (Some(a), Some(b)) = (&self.account_id, &other.account_id)
            && a == b
        {
            return true;
        }
        if let (Some(a), Some(b)) = (&self.email, &other.email)
            && a.eq_ignore_ascii_case(b)
        {
            return true;
        }
        self.subject == other.subject
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshOwner {
    /// The store refreshes; agents receive access-only material.
    Store,
    /// The vendor tool refreshes; `absorb` accepts same-identity write-backs.
    Vendor,
    /// The credential does not expire (keys, service-account tokens).
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Login,
    Import,
    Absorb,
    Refresh,
    Migration,
    Set,
}

/// `record.json`: everything about a generation except the secret bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMeta {
    pub record: RecordRef,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Identity>,
    pub created_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    pub refresh_owner: RefreshOwner,
    pub source: Source,
    /// sha256 over the sorted `(path, bytes)` pairs of `material/`.
    pub material_sha256: String,
    /// Set when the generation was imported but its credential is known bad.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid_reason: Option<String>,
}

/// What a caller supplies for a new generation; the store fills the rest.
#[derive(Clone, Debug)]
pub struct NewGeneration {
    pub identity: Option<Identity>,
    pub verified_at_ms: Option<i64>,
    pub expires_at_ms: Option<i64>,
    pub refresh_owner: RefreshOwner,
    pub source: Source,
    pub invalid_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CredentialStore {
    root: PathBuf,
}

impl CredentialStore {
    /// The one documented location: `<data>/credentials`.
    pub fn root_for(data: &Path) -> PathBuf {
        data.join("credentials")
    }

    /// Opens (creating when absent) the store with 0700 directories.
    pub fn open(data: &Path) -> Result<Self, Error> {
        let root = Self::root_for(data);
        for dir in [
            root.clone(),
            root.join("records"),
            root.join("state"),
            root.join(".staging"),
        ] {
            ensure_private_dir(&dir)?;
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn record_dir(&self, r: &RecordRef) -> PathBuf {
        self.root.join("records").join(r.rel())
    }

    /// Non-secret per-record runtime state (usage caches, refresh attempts).
    pub fn state_dir(&self, r: &RecordRef) -> Result<PathBuf, Error> {
        let dir = self.root.join("state").join(r.rel());
        ensure_private_dir_all(&dir)?;
        Ok(dir)
    }

    /// The published generation directory of `r`, or `None` when the record
    /// does not exist. A dangling or malformed `current` is an error, not absence.
    fn current_generation_dir(&self, r: &RecordRef) -> Result<Option<PathBuf>, Error> {
        let link = self.record_dir(r).join("current");
        match std::fs::read_link(&link) {
            Ok(target) => {
                let name = target
                    .file_name()
                    .and_then(|n| n.to_str())
                    .filter(|n| parse_generation(n).is_some())
                    .ok_or_else(|| {
                        Error::Message(format!("credential record {r}: malformed current link"))
                    })?;
                let dir = self.record_dir(r).join(name);
                if !dir.join("record.json").is_file() {
                    return Err(Error::Message(format!(
                        "credential record {r}: current generation is missing"
                    )));
                }
                Ok(Some(dir))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn get(&self, r: &RecordRef) -> Result<Option<RecordMeta>, Error> {
        let Some(dir) = self.current_generation_dir(r)? else {
            return Ok(None);
        };
        let raw = std::fs::read(dir.join("record.json"))?;
        let meta: RecordMeta = serde_json::from_slice(&raw)
            .map_err(|e| Error::Message(format!("credential record {r}: record.json: {e}")))?;
        if meta.record != *r {
            return Err(Error::Message(format!(
                "credential record {r}: record.json names {}",
                meta.record
            )));
        }
        Ok(Some(meta))
    }

    /// The server-only material directory of the current generation.
    pub fn material_dir(&self, r: &RecordRef) -> Result<Option<PathBuf>, Error> {
        Ok(self.current_generation_dir(r)?.map(|d| d.join("material")))
    }

    /// Reads one material file of the current generation.
    pub fn read_material(&self, r: &RecordRef, rel: &str) -> Result<Option<Vec<u8>>, Error> {
        let Some(dir) = self.material_dir(r)? else {
            return Ok(None);
        };
        read_regular_file(&dir, rel)
    }

    /// Every published record. Staging and state are never enumerated.
    pub fn list(&self) -> Result<Vec<RecordMeta>, Error> {
        let mut out = Vec::new();
        for kind in Kind::ALL {
            let kind_dir = self.root.join("records").join(kind.as_str());
            for provider in read_dir_names(&kind_dir)? {
                if !valid_segment(&provider) {
                    continue;
                }
                for name in read_dir_names(&kind_dir.join(&provider))? {
                    if !valid_segment(&name) {
                        continue;
                    }
                    let r = RecordRef::new(kind, &provider, &name)?;
                    if let Some(meta) = self.get(&r)? {
                        out.push(meta);
                    }
                }
            }
        }
        out.sort_by(|a, b| a.record.cmp(&b.record));
        Ok(out)
    }

    pub fn list_provider(&self, kind: Option<Kind>, provider: &str) -> Result<Vec<RecordMeta>, Error> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|m| m.record.provider == provider && kind.is_none_or(|k| m.record.kind == k))
            .collect())
    }

    /// A fresh private staging directory, outside every enumerated namespace.
    pub fn new_staging(&self, label: &str) -> Result<PathBuf, Error> {
        let dir = self
            .root
            .join(".staging")
            .join(format!("{label}-{}", random_id()?));
        std::fs::create_dir(&dir)?;
        set_mode(&dir, 0o700)?;
        Ok(dir)
    }

    pub fn staging_root(&self) -> PathBuf {
        self.root.join(".staging")
    }

    /// Publishes `material` (a directory the caller filled, normally from
    /// [`Self::new_staging`]) as the next generation of `r`. The directory is
    /// moved, not copied; on success it no longer exists at its old path.
    pub fn publish(
        &self,
        r: &RecordRef,
        material: &Path,
        new: NewGeneration,
        actor: &str,
        reason: &str,
    ) -> Result<RecordMeta, Error> {
        let record_dir = self.record_dir(r);
        ensure_private_dir_all(&record_dir)?;
        let _lock = RecordLock::acquire(&record_dir)?;
        let previous = self.get(r)?;
        let generation = previous.as_ref().map_or(1, |m| m.generation + 1);
        let material_sha256 = material_digest(material)?;
        let meta = RecordMeta {
            record: r.clone(),
            generation,
            identity: new.identity,
            created_at_ms: anyagents_core::now_ms(),
            verified_at_ms: new.verified_at_ms,
            expires_at_ms: new.expires_at_ms,
            refresh_owner: new.refresh_owner,
            source: new.source,
            material_sha256,
            invalid_reason: new.invalid_reason,
        };
        let gen_name = format!("gen-{generation:06}");
        let tmp = record_dir.join(format!(".{gen_name}.tmp"));
        if tmp.exists() {
            std::fs::remove_dir_all(&tmp)?;
        }
        std::fs::create_dir(&tmp)?;
        set_mode(&tmp, 0o700)?;
        std::fs::rename(material, tmp.join("material"))?;
        let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(|e| Error::Message(e.to_string()))?;
        anyagents_harness::atomic_write_nonempty_file(&tmp.join("record.json"), &meta_bytes, 0o600)?;
        sync_tree(&tmp)?;
        let gen_dir = record_dir.join(&gen_name);
        std::fs::rename(&tmp, &gen_dir)?;
        let link_tmp = record_dir.join(".current.tmp");
        let _ = std::fs::remove_file(&link_tmp);
        std::os::unix::fs::symlink(&gen_name, &link_tmp)?;
        std::fs::rename(&link_tmp, record_dir.join("current"))?;
        sync_dir(&record_dir)?;
        self.audit(&AuditLine {
            at_ms: meta.created_at_ms,
            actor,
            action: "publish",
            record: &r.to_string(),
            generation,
            before_sha256: previous.as_ref().map(|m| m.material_sha256.as_str()),
            after_sha256: Some(&meta.material_sha256),
            reason,
        })?;
        self.prune(&record_dir, generation)?;
        Ok(meta)
    }

    /// Builds a new generation from files given in memory.
    pub fn publish_files(
        &self,
        r: &RecordRef,
        files: &[(String, Vec<u8>)],
        new: NewGeneration,
        actor: &str,
        reason: &str,
    ) -> Result<RecordMeta, Error> {
        let staging = self.new_staging("write")?;
        let result = (|| {
            let material = staging.join("material");
            ensure_private_dir(&material)?;
            for (rel, bytes) in files {
                write_material_file(&material, rel, bytes)?;
            }
            self.publish(r, &material, new, actor, reason)
        })();
        let _ = std::fs::remove_dir_all(&staging);
        result
    }

    /// Builds the next generation from the current one with `overrides`
    /// replacing (or adding) individual files. Used by store-owned refresh and
    /// identity-checked absorb.
    pub fn publish_overrides(
        &self,
        r: &RecordRef,
        overrides: &[(String, Vec<u8>)],
        new: NewGeneration,
        actor: &str,
        reason: &str,
    ) -> Result<RecordMeta, Error> {
        let current = self
            .material_dir(r)?
            .ok_or_else(|| Error::Message(format!("credential record {r} does not exist")))?;
        let staging = self.new_staging("write")?;
        let result = (|| {
            let material = staging.join("material");
            copy_tree(&current, &material)?;
            for (rel, bytes) in overrides {
                write_material_file(&material, rel, bytes)?;
            }
            self.publish(r, &material, new, actor, reason)
        })();
        let _ = std::fs::remove_dir_all(&staging);
        result
    }

    fn prune(&self, record_dir: &Path, current: u64) -> Result<(), Error> {
        let mut gens: Vec<u64> = read_dir_names(record_dir)?
            .iter()
            .filter_map(|n| parse_generation(n))
            .filter(|g| *g < current)
            .collect();
        gens.sort_unstable();
        let excess = gens.len().saturating_sub(RETAINED_PREVIOUS_GENERATIONS);
        for g in gens.into_iter().take(excess) {
            std::fs::remove_dir_all(record_dir.join(format!("gen-{g:06}")))?;
        }
        Ok(())
    }

    /// Removes staging attempts older than `max_age_ms` that hold no declared
    /// credential bytes. Attempts that still hold credential material are kept
    /// for `login --resume` and reported by doctor.
    pub fn sweep_staging(&self, max_age_ms: i64, keep: impl Fn(&Path) -> bool) -> Result<Vec<PathBuf>, Error> {
        let mut kept = Vec::new();
        let now = std::time::SystemTime::now();
        for name in read_dir_names(&self.staging_root())? {
            let path = self.staging_root().join(&name);
            let age_ms = std::fs::symlink_metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|m| now.duration_since(m).ok())
                .map_or(0, |d| d.as_millis() as i64);
            if age_ms < max_age_ms {
                continue;
            }
            if keep(&path) {
                kept.push(path);
            } else {
                std::fs::remove_dir_all(&path)?;
            }
        }
        Ok(kept)
    }

    pub fn audit(&self, line: &AuditLine<'_>) -> Result<(), Error> {
        let mut text = serde_json::to_string(line).map_err(|e| Error::Message(e.to_string()))?;
        text.push('\n');
        let path = self.root.join("audit.jsonl");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        set_mode(&path, 0o600)?;
        file.write_all(text.as_bytes())?;
        file.sync_data()?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct AuditLine<'a> {
    pub at_ms: i64,
    pub actor: &'a str,
    pub action: &'a str,
    pub record: &'a str,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_sha256: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_sha256: Option<&'a str>,
    pub reason: &'a str,
}

struct RecordLock(std::fs::File);

impl RecordLock {
    fn acquire(record_dir: &Path) -> Result<Self, Error> {
        use std::os::fd::AsRawFd as _;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(record_dir.join(".lock"))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self(file))
    }
}

impl Drop for RecordLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd as _;
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn parse_generation(name: &str) -> Option<u64> {
    name.strip_prefix("gen-")?.parse().ok()
}

pub(crate) fn random_id() -> Result<String, Error> {
    let mut bytes = [0u8; 12];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn set_mode(path: &Path, mode: u32) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn ensure_private_dir(dir: &Path) -> Result<(), Error> {
    match std::fs::create_dir(dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err.into()),
    }
    let meta = std::fs::symlink_metadata(dir)?;
    if !meta.is_dir() {
        return Err(Error::Message(format!(
            "{} is not a directory",
            dir.display()
        )));
    }
    set_mode(dir, 0o700)
}

fn ensure_private_dir_all(dir: &Path) -> Result<(), Error> {
    if let Some(parent) = dir.parent()
        && !parent.is_dir()
    {
        ensure_private_dir_all(parent)?;
    }
    ensure_private_dir(dir)
}

fn read_dir_names(dir: &Path) -> Result<Vec<String>, Error> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            out.push(name.to_string());
        }
    }
    out.sort();
    Ok(out)
}

/// Rejects absolute paths, `..`, and empty components.
pub(crate) fn safe_rel(rel: &str) -> Result<&Path, Error> {
    let path = Path::new(rel);
    let ok = !rel.is_empty()
        && path
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)));
    if ok {
        Ok(path)
    } else {
        Err(Error::Message(format!("unsafe credential path `{rel}`")))
    }
}

/// Reads `root/rel` only when every component is a real directory and the leaf
/// a regular file (no symlinks, FIFOs or devices).
pub(crate) fn read_regular_file(root: &Path, rel: &str) -> Result<Option<Vec<u8>>, Error> {
    let rel_path = safe_rel(rel)?;
    let mut cur = root.to_path_buf();
    let comps: Vec<_> = rel_path.components().collect();
    for (i, comp) in comps.iter().enumerate() {
        cur.push(comp.as_os_str());
        let meta = match std::fs::symlink_metadata(&cur) {
            Ok(m) => m,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let leaf = i + 1 == comps.len();
        if meta.file_type().is_symlink() || (!leaf && !meta.is_dir()) {
            return Err(Error::Message(format!(
                "credential path `{rel}` crosses a non-directory or symlink"
            )));
        }
        if leaf && !meta.is_file() {
            return Err(Error::Message(format!(
                "credential path `{rel}` is not a regular file"
            )));
        }
    }
    Ok(Some(std::fs::read(&cur)?))
}

pub(crate) fn write_material_file(root: &Path, rel: &str, bytes: &[u8]) -> Result<(), Error> {
    let rel_path = safe_rel(rel)?;
    let dst = root.join(rel_path);
    if let Some(parent) = dst.parent() {
        ensure_private_dir_all(parent)?;
    }
    anyagents_harness::atomic_write_file(&dst, bytes, 0o600)?;
    Ok(())
}

/// Every regular file under `root` as `(relative path, bytes)`, sorted.
/// Symlinks and special files are skipped, never followed.
pub(crate) fn material_files(root: &Path) -> Result<Vec<(String, Vec<u8>)>, Error> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> Result<(), Error> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        for entry in entries {
            let entry = entry?;
            let ty = entry.file_type()?;
            let path = entry.path();
            if ty.is_dir() {
                walk(root, &path, out)?;
            } else if ty.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .map_err(|e| Error::Message(e.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, std::fs::read(&path)?));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(root, root, &mut out)?;
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub(crate) fn material_digest(root: &Path) -> Result<String, Error> {
    let mut hasher = Sha256::new();
    for (rel, bytes) in material_files(root)? {
        hasher.update((rel.len() as u64).to_le_bytes());
        hasher.update(rel.as_bytes());
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    Ok(hasher.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn copy_tree(src: &Path, dst: &Path) -> Result<(), Error> {
    ensure_private_dir_all(dst)?;
    for (rel, bytes) in material_files(src)? {
        write_material_file(dst, &rel, &bytes)?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<(), Error> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn sync_tree(dir: &Path) -> Result<(), Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        }
    }
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_gen(source: Source) -> NewGeneration {
        NewGeneration {
            identity: Identity::from_parts(Some("dev@example.invalid".into()), None),
            verified_at_ms: None,
            expires_at_ms: None,
            refresh_owner: RefreshOwner::Store,
            source,
            invalid_reason: None,
        }
    }

    #[test]
    fn record_refs_round_trip_and_reject_bad_segments() {
        let r: RecordRef = "oauth_subscription/anthropic/acct-a".parse().unwrap();
        assert_eq!(r.to_string(), "oauth_subscription/anthropic/acct-a");
        for bad in [
            "oauth_subscription/anthropic",
            "nope/anthropic/a",
            "api_key/Upper/a",
            "api_key/p/.hidden",
            "api_key/p/a/b",
            "api_key/p/../x",
        ] {
            assert!(bad.parse::<RecordRef>().is_err(), "{bad}");
        }
    }

    #[test]
    fn publish_swaps_generations_atomically_and_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path()).unwrap();
        let r = RecordRef::new(Kind::ApiKey, "example", "main").unwrap();
        assert!(store.get(&r).unwrap().is_none());
        for i in 1..=5u8 {
            let meta = store
                .publish_files(
                    &r,
                    &[("key".into(), vec![b'0' + i])],
                    new_gen(Source::Set),
                    "test",
                    "write",
                )
                .unwrap();
            assert_eq!(meta.generation, u64::from(i));
        }
        assert_eq!(store.read_material(&r, "key").unwrap().unwrap(), b"5");
        let record_dir = store.record_dir(&r);
        let gens: Vec<_> = read_dir_names(&record_dir)
            .unwrap()
            .into_iter()
            .filter(|n| n.starts_with("gen-"))
            .collect();
        assert_eq!(gens, ["gen-000003", "gen-000004", "gen-000005"]);
        let audit = std::fs::read_to_string(store.root().join("audit.jsonl")).unwrap();
        assert_eq!(audit.lines().count(), 5);
        assert!(!audit.contains("\"5\""));
    }

    #[test]
    fn overrides_keep_other_material_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path()).unwrap();
        let r = RecordRef::new(Kind::OauthSubscription, "example", "a").unwrap();
        store
            .publish_files(
                &r,
                &[("a.json".into(), b"A".to_vec()), ("sub/b.json".into(), b"B".to_vec())],
                new_gen(Source::Login),
                "test",
                "login",
            )
            .unwrap();
        store
            .publish_overrides(&r, &[("a.json".into(), b"A2".to_vec())], new_gen(Source::Refresh), "test", "refresh")
            .unwrap();
        assert_eq!(store.read_material(&r, "a.json").unwrap().unwrap(), b"A2");
        assert_eq!(store.read_material(&r, "sub/b.json").unwrap().unwrap(), b"B");
    }

    #[test]
    fn a_crash_before_the_link_swap_leaves_the_previous_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path()).unwrap();
        let r = RecordRef::new(Kind::ApiKey, "example", "main").unwrap();
        store
            .publish_files(&r, &[("key".into(), b"old".to_vec())], new_gen(Source::Set), "t", "w")
            .unwrap();
        // Simulate a crash after the next generation directory was written but
        // before `current` moved: a complete gen dir and a stray temp link.
        let record_dir = store.record_dir(&r);
        std::fs::create_dir_all(record_dir.join("gen-000002/material")).unwrap();
        std::fs::write(record_dir.join("gen-000002/material/key"), b"new").unwrap();
        std::os::unix::fs::symlink("gen-000002", record_dir.join(".current.tmp")).unwrap();
        assert_eq!(store.read_material(&r, "key").unwrap().unwrap(), b"old");
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    fn staging_and_state_are_never_listed() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path()).unwrap();
        let staging = store.new_staging("login").unwrap();
        std::fs::write(staging.join("token"), b"x").unwrap();
        let r = RecordRef::new(Kind::ApiKey, "example", "main").unwrap();
        std::fs::write(store.state_dir(&r).unwrap().join("usage"), b"1").unwrap();
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn store_directories_are_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path()).unwrap();
        for sub in ["", "records", "state", ".staging"] {
            let mode = std::fs::metadata(store.root().join(sub)).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "{sub}");
        }
    }

    #[test]
    fn symlinked_material_is_never_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("secret"), b"x").unwrap();
        std::os::unix::fs::symlink(dir.path().join("secret"), dir.path().join("link")).unwrap();
        assert!(read_regular_file(dir.path(), "link").is_err());
        assert!(read_regular_file(dir.path(), "missing").unwrap().is_none());
        assert!(read_regular_file(dir.path(), "../etc").is_err());
    }
}
