//! Portable browser profile archive packing, safe unpacking, and coordinator
//! sync-down / sync-up for `anyagents-mcp-browser`.
//!
//! Credentials (`ANYAGENTS_S3_*`, `ANYAGENTS_MASTER_KEY`) never enter the
//! sandbox: `anyagents-mcp-browser` authenticates to `<ANYAGENTS_API_URL>` using
//! the agent's own capability token (`x-anyagents-agent`), downloading/uploading
//! `.tar.zst` archives of `<profile_dir>/user-data/`.

use std::fmt;
use std::io::{Cursor, ErrorKind, Read as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::config::BrowserServerConfig;
use crate::session::BrowserError;

/// Maximum compressed profile archive size (256 MiB).
pub const MAX_PROFILE_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;
/// Maximum uncompressed profile size accepted during extraction (1 GiB).
const MAX_UNPACKED_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
/// Maximum number of entries accepted during extraction.
const MAX_ARCHIVE_FILES: usize = 100_000;
/// Maximum length of a browser profile name.
const MAX_PROFILE_NAME_LEN: usize = 64;

const ZSTD_MAGIC: &[u8; 4] = b"\x28\xb5\x2f\xfd";
const GZIP_MAGIC: &[u8; 2] = b"\x1f\x8b";

/// Environment variables checked when resolving coordinator profile sync.
pub const API_URL_ENV: &str = "ANYAGENTS_API_URL";
pub const AGENT_TOKEN_ENV: &str = "ANYAGENTS_AGENT_TOKEN";
pub const CAPABILITY_TOKEN_ENV: &str = "ANYAGENTS_CAPABILITY_TOKEN";
pub const CAPABILITY_ENV: &str = "ANYAGENTS_CAPABILITY";
pub const SESSION_TOKEN_ENV: &str = "ANYAGENTS_SESSION_TOKEN";
pub const MCP_TOKEN_ENV: &str = "ANYAGENTS_MCP_TOKEN";
pub const MCP_TOKEN_FILE_ENV: &str = "ANYAGENTS_MCP_TOKEN_FILE";
pub const OPERATOR_TOKEN_ENV: &str = "ANYAGENTS_OPERATOR_TOKEN";
pub const OPERATOR_TOKEN_FILE_ENV: &str = "ANYAGENTS_OPERATOR_TOKEN_FILE";
pub const AGENT_DATA_DIR_ENV: &str = "ANYAGENTS_AGENT_DATA_DIR";
pub const SANDBOX_DATA_DIR_ENV: &str = "ANYAGENTS_SANDBOX_DATA_DIR";
pub const GUEST_WORKDIR_ENV: &str = "ANYAGENTS_GUEST_WORKDIR";
pub const SANDBOX_WORKDIR_ENV: &str = "ANYAGENTS_SANDBOX_WORKDIR";
pub const DEFAULT_SANDBOX_DATA_DIR: &str = "/home/user/agent-data";

/// Pure, testable sandbox-aligned directory resolution for `browser_screenshot`.
///
/// Prefers explicit config -> `<ANYAGENTS_AGENT_DATA_DIR>/tmp/screenshots` ->
/// `<ANYAGENTS_SANDBOX_DATA_DIR>/tmp/screenshots` -> `<workdir>/.tmp/screenshots`
/// -> OS temp dir, never returning coordinator-only host paths.
pub fn resolve_screenshot_dir_from(
    explicit_dir: Option<&Path>,
    agent_data_dir: Option<&Path>,
    sandbox_data_dir: Option<&Path>,
    workdir: Option<&Path>,
    fallback_tmp: &Path,
) -> PathBuf {
    if let Some(dir) = explicit_dir.filter(|p| !p.as_os_str().is_empty()) {
        return dir.to_path_buf();
    }
    if let Some(data) = agent_data_dir.filter(|p| !p.as_os_str().is_empty()) {
        return data.join("tmp").join("screenshots");
    }
    if let Some(data) = sandbox_data_dir.filter(|p| !p.as_os_str().is_empty()) {
        return data.join("tmp").join("screenshots");
    }
    if let Some(wd) = workdir.filter(|p| !p.as_os_str().is_empty()) {
        return wd.join(".tmp").join("screenshots");
    }
    fallback_tmp.join("anyagents-screenshots")
}

/// Pick the first existing sandbox data dir, skipping nonexistent host paths.
pub fn pick_existing_data_dir<'a>(candidates: &[Option<&'a Path>]) -> Option<&'a Path> {
    candidates
        .iter()
        .copied()
        .flatten()
        .find(|p| !p.as_os_str().is_empty() && p.is_dir())
}

/// Resolve the default sandbox-accessible directory for `browser_screenshot`.
pub fn default_screenshot_dir(config: &BrowserServerConfig) -> PathBuf {
    let agent_data = non_empty_env(AGENT_DATA_DIR_ENV).map(PathBuf::from);
    let sandbox_data = non_empty_env(SANDBOX_DATA_DIR_ENV).map(PathBuf::from);
    let workdir = non_empty_env(GUEST_WORKDIR_ENV)
        .or_else(|| non_empty_env(SANDBOX_WORKDIR_ENV))
        .map(PathBuf::from);
    let existing = pick_existing_data_dir(&[
        agent_data.as_deref(),
        sandbox_data.as_deref(),
        Some(Path::new(DEFAULT_SANDBOX_DATA_DIR)),
    ]);
    resolve_screenshot_dir_from(
        config.screenshot_dir.as_deref(),
        existing.or(agent_data.as_deref()),
        sandbox_data.as_deref(),
        workdir.as_deref(),
        &std::env::temp_dir(),
    )
}

/// Resolve the destination file path for `browser_screenshot`.
///
/// - Absolute requested paths are preserved as-is.
/// - Relative requested paths are placed inside [`default_screenshot_dir`].
/// - Omitted paths generate a unique `shot-<epoch_ms>-<seq>.png` inside
///   [`default_screenshot_dir`] so sandbox scripts and `send(media)` resolve
///   the exact returned path inside the sandbox.
pub fn resolve_screenshot_path(config: &BrowserServerConfig, requested: Option<&str>) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(1);

    let dir = default_screenshot_dir(config);
    if let Some(raw) = requested.map(str::trim).filter(|s| !s.is_empty()) {
        let p = Path::new(raw);
        return if p.is_absolute() {
            p.to_path_buf()
        } else {
            dir.join(p)
        };
    }
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("shot-{ms}-{seq}.png"))
}

/// Single-authority coordinator credential (never mixes agent + operator headers).
#[derive(Clone, PartialEq, Eq)]
pub enum CoordinatorAuth {
    Agent(String),
    Operator(String),
}

impl fmt::Debug for CoordinatorAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = if self.is_agent() {
            "Agent([redacted])"
        } else {
            "Operator([redacted])"
        };
        f.write_str(label)
    }
}

impl CoordinatorAuth {
    pub fn is_agent(&self) -> bool {
        matches!(self, Self::Agent(_))
    }

    pub fn headers(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::Agent(t) => vec![
                ("x-anyagents-agent", t.clone()),
                ("Authorization", format!("Bearer {t}")),
            ],
            Self::Operator(t) => vec![("x-anyagents-operator", t.clone())],
        }
    }
}

/// Coordinator remote sync target for a named browser profile.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteProfileSync {
    pub api_url: String,
    pub auth: CoordinatorAuth,
    pub profile_name: String,
}

impl fmt::Debug for RemoteProfileSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteProfileSync")
            .field("api_url", &self.api_url)
            .field("auth", &self.auth)
            .field("profile_name", &self.profile_name)
            .finish()
    }
}

/// Outcome of syncing a profile archive down from the coordinator on open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncDownOutcome {
    Restored { archive_bytes: usize, files: usize },
    NotFoundFresh,
}

/// Strictly validate a browser profile name (`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`,
/// rejecting path traversal and separators).
pub fn validate_profile_name(raw: &str) -> Result<String, String> {
    if raw.is_empty()
        || raw.len() > MAX_PROFILE_NAME_LEN
        || raw.contains("..")
        || raw.contains('/')
        || raw.contains('\\')
        || raw.contains('\0')
    {
        return Err(format!(
            "invalid browser profile name `{raw}`: must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{{0,63}}$ with no `..`"
        ));
    }
    let bytes = raw.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(format!(
            "invalid browser profile name `{raw}`: must start with an ASCII letter or digit"
        ));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "invalid browser profile name `{raw}`: allowed characters are ASCII letters, digits, `.`, `_`, `-`"
        ));
    }
    Ok(raw.to_string())
}

fn non_empty_trimmed(opt: Option<&str>) -> Option<String> {
    opt.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn non_empty_env(name: &str) -> Option<String> {
    let raw = std::env::var(name).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Resolve coordinator auth (agent tokens always take precedence over operator tokens).
pub fn resolve_coordinator_auth_from<F>(explicit: Option<&str>, env: F) -> Option<CoordinatorAuth>
where
    F: Fn(&str) -> Option<String>,
{
    let get = |k| env(k).and_then(|v| non_empty_trimmed(Some(&v)));
    let file =
        |k| get(k).and_then(|p| non_empty_trimmed(std::fs::read_to_string(p).ok().as_deref()));
    let keys = "ANYAGENTS_AGENT_TOKEN ANYAGENTS_CAPABILITY_TOKEN ANYAGENTS_CAPABILITY \
        ANYAGENTS_SESSION_TOKEN ANYAGENTS_MCP_TOKEN";
    if let Some(t) = non_empty_trimmed(explicit)
        .or_else(|| keys.split_whitespace().find_map(get))
        .or_else(|| file(MCP_TOKEN_FILE_ENV))
    {
        return Some(CoordinatorAuth::Agent(t));
    }
    get(OPERATOR_TOKEN_ENV)
        .or_else(|| file(OPERATOR_TOKEN_FILE_ENV))
        .map(CoordinatorAuth::Operator)
}

pub fn resolve_agent_token(explicit: Option<&str>) -> Option<String> {
    match resolve_coordinator_auth(explicit) {
        Some(CoordinatorAuth::Agent(t)) => Some(t),
        _ => None,
    }
}

pub fn resolve_coordinator_auth(explicit: Option<&str>) -> Option<CoordinatorAuth> {
    resolve_coordinator_auth_from(explicit, non_empty_env)
}

pub fn resolve_api_url(config: &BrowserServerConfig) -> Result<Option<String>, BrowserError> {
    let Some(u) =
        non_empty_trimmed(config.api_url.as_deref()).or_else(|| non_empty_env(API_URL_ENV))
    else {
        return Ok(None);
    };
    let parsed = url::Url::parse(&u)
        .map_err(|e| BrowserError::invalid_input(format!("invalid `ANYAGENTS_API_URL`: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(BrowserError::invalid_input(
            "`ANYAGENTS_API_URL` must use http/https",
        ));
    }
    Ok(Some(u.trim_end_matches('/').to_string()))
}

/// Resolve optional coordinator profile sync configuration for the active named
/// profile. Returns `Ok(None)` when no named profile or no `ANYAGENTS_API_URL`
/// is configured; fails closed with `UNAUTHORIZED` when `ANYAGENTS_API_URL` and
/// a named profile are set without credentials.
pub fn resolve_sync_config(
    config: &BrowserServerConfig,
    override_profile: Option<&str>,
) -> Result<Option<RemoteProfileSync>, BrowserError> {
    let raw_profile = non_empty_trimmed(override_profile)
        .or_else(|| non_empty_trimmed(config.profile.as_deref()));
    let Some(raw_profile) = raw_profile else {
        return Ok(None);
    };
    let profile_name = validate_profile_name(&raw_profile).map_err(BrowserError::invalid_input)?;
    let Some(api_url) = resolve_api_url(config)? else {
        return Ok(None);
    };
    let auth = resolve_coordinator_auth(config.agent_token.as_deref()).ok_or_else(|| {
        BrowserError::new(
            "UNAUTHORIZED",
            format!(
                "cannot sync browser profile `{profile_name}` with coordinator: missing agent or operator token"
            ),
        )
    })?;
    Ok(Some(RemoteProfileSync {
        api_url,
        auth,
        profile_name,
    }))
}

/// Single path segments stripped from Chromium `user-data/` during packing:
/// disposable caches and process-scoped singleton lock files.
const STRIPPED_PATH_COMPONENTS: &[&str] = &[
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "ShaderCache",
    "DawnCache",
    "GraphiteDawnCache",
    "Crashpad",
    "SingletonLock",
    "SingletonCookie",
    "SingletonSocket",
    "LOCK",
    "takeover.lock",
    "DevToolsActivePort",
    "devtools.log",
];

/// Return `true` when `rel_path` (relative to `user-data/`) is a disposable
/// cache or Chromium lock file that must be stripped before archiving.
pub fn should_strip_profile_path(rel_path: &Path) -> bool {
    let comps: Vec<&str> = rel_path
        .components()
        .filter_map(|c| match c {
            Component::Normal(os) => os.to_str(),
            _ => None,
        })
        .collect();
    for seg in &comps {
        if STRIPPED_PATH_COMPONENTS.contains(seg) {
            return true;
        }
    }
    for pair in comps.windows(2) {
        if pair[0] == "Service Worker" && pair[1] == "CacheStorage" {
            return true;
        }
    }
    false
}

/// Pack `user_data_dir` into a compressed tar archive (`.tar.zst` when `zstd`
/// is available on `$PATH`, with transparent gzip fallback), stripping
/// disposable caches, Chromium locks, and non-regular files (symlinks/sockets).
///
/// Returns `(archive_bytes, content_type)`.
pub fn pack_user_data_dir(user_data_dir: &Path) -> Result<(Vec<u8>, &'static str), String> {
    if !user_data_dir.is_dir() {
        return Err(format!(
            "browser user-data directory `{}` does not exist",
            user_data_dir.display()
        ));
    }
    let mut builder = tar::Builder::new(Vec::new());
    append_dir_filtered(&mut builder, user_data_dir, user_data_dir)?;
    let tar_bytes = builder
        .into_inner()
        .map_err(|e| format!("failed finalizing profile tar archive: {e}"))?;
    let (compressed, content_type) = compress_tar_bytes(&tar_bytes)?;
    if compressed.len() > MAX_PROFILE_ARCHIVE_BYTES {
        return Err(format!(
            "packed browser profile archive ({} bytes) exceeds {} byte limit",
            compressed.len(),
            MAX_PROFILE_ARCHIVE_BYTES
        ));
    }
    Ok((compressed, content_type))
}

fn append_dir_filtered(
    builder: &mut tar::Builder<Vec<u8>>,
    root: &Path,
    dir: &Path,
) -> Result<(), String> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("failed reading `{}`: {e}", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();

    for path in entries {
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        if should_strip_profile_path(rel) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let ft = meta.file_type();
        // Never archive symlinks, Unix sockets (e.g. Chromium singleton sockets), FIFOs, or devices.
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_size(0);
            header.set_mode(0o755);
            header.set_mtime(0);
            header.set_cksum();
            builder
                .append_data(&mut header, rel, std::io::empty())
                .map_err(|e| format!("failed adding dir `{}` to tar: {e}", rel.display()))?;
            append_dir_filtered(builder, root, &path)?;
        } else if ft.is_file() {
            let bytes = std::fs::read(&path)
                .map_err(|e| format!("failed reading `{}`: {e}", path.display()))?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            header.set_cksum();
            builder
                .append_data(&mut header, rel, bytes.as_slice())
                .map_err(|e| format!("failed adding file `{}` to tar: {e}", rel.display()))?;
        }
    }
    Ok(())
}

fn compress_tar_bytes(tar_bytes: &[u8]) -> Result<(Vec<u8>, &'static str), String> {
    match run_zstd_pipe(
        &["-q", "-3", "-c"],
        tar_bytes,
        MAX_PROFILE_ARCHIVE_BYTES as u64,
    ) {
        Ok(compressed) => Ok((compressed, "application/zstd")),
        Err(ZstdPipeError::BinaryNotFound) => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder
                .write_all(tar_bytes)
                .map_err(|e| format!("gzip compression failed: {e}"))?;
            let out = encoder
                .finish()
                .map_err(|e| format!("gzip finish failed: {e}"))?;
            Ok((out, "application/gzip"))
        }
        Err(ZstdPipeError::Failed(msg)) => Err(msg),
    }
}

fn decompress_tar_bytes(archive: &[u8]) -> Result<Vec<u8>, String> {
    if archive.starts_with(ZSTD_MAGIC) {
        return match run_zstd_pipe(&["-d", "-q", "-c"], archive, MAX_UNPACKED_ARCHIVE_BYTES) {
            Ok(bytes) => Ok(bytes),
            Err(ZstdPipeError::BinaryNotFound) => Err(
                "cannot decompress `.tar.zst` browser profile archive: `zstd` binary not found on PATH"
                    .into(),
            ),
            Err(ZstdPipeError::Failed(msg)) => Err(msg),
        };
    }
    if archive.starts_with(GZIP_MAGIC) {
        let mut decoder = flate2::read::GzDecoder::new(Cursor::new(archive));
        let mut out = Vec::new();
        decoder
            .by_ref()
            .take(MAX_UNPACKED_ARCHIVE_BYTES.saturating_add(1))
            .read_to_end(&mut out)
            .map_err(|e| format!("gzip decompression failed: {e}"))?;
        if out.len() as u64 > MAX_UNPACKED_ARCHIVE_BYTES {
            return Err("decompressed browser profile exceeds maximum allowed size".into());
        }
        return Ok(out);
    }
    Err("unsupported browser profile archive format (expected zstd or gzip)".into())
}

enum ZstdPipeError {
    BinaryNotFound,
    Failed(String),
}

const ZSTD_CANDIDATE_PATHS: &[&str] = &[
    "/usr/bin/zstd",
    "/usr/local/bin/zstd",
    "/opt/homebrew/bin/zstd",
    "/bin/zstd",
];

fn find_zstd_binary() -> Option<PathBuf> {
    ZSTD_CANDIDATE_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
}

fn run_zstd_pipe(args: &[&str], input: &[u8], max_output: u64) -> Result<Vec<u8>, ZstdPipeError> {
    let Some(zstd_bin) = find_zstd_binary() else {
        return Err(ZstdPipeError::BinaryNotFound);
    };
    let mut child = Command::new(&zstd_bin)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                ZstdPipeError::BinaryNotFound
            } else {
                ZstdPipeError::Failed(format!("failed spawning `{}`: {err}", zstd_bin.display()))
            }
        })?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| ZstdPipeError::Failed("zstd stdin pipe unavailable".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ZstdPipeError::Failed("zstd stdout pipe unavailable".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ZstdPipeError::Failed("zstd stderr pipe unavailable".into()))?;

    let (read_res, err_res, write_res) = std::thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let res = stdin.write_all(input);
            drop(stdin);
            res
        });
        let err_reader = scope.spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr.take(4096).read_to_end(&mut buf);
            buf
        });
        let mut out = Vec::new();
        let read_res = stdout
            .take(max_output.saturating_add(1))
            .read_to_end(&mut out)
            .map(|_| out);
        let write_res = writer
            .join()
            .unwrap_or_else(|_| Err(std::io::Error::other("zstd writer thread panicked")));
        let err_buf = err_reader.join().unwrap_or_default();
        (read_res, err_buf, write_res)
    });

    let status = child
        .wait()
        .map_err(|e| ZstdPipeError::Failed(format!("failed waiting on zstd: {e}")))?;
    let out =
        read_res.map_err(|e| ZstdPipeError::Failed(format!("zstd stdout read failed: {e}")))?;
    if out.len() as u64 > max_output {
        return Err(ZstdPipeError::Failed(format!(
            "zstd output exceeded maximum allowed size ({max_output} bytes)"
        )));
    }
    if !status.success() {
        let stderr_msg = String::from_utf8_lossy(&err_res).trim().to_string();
        return Err(ZstdPipeError::Failed(format!(
            "zstd exited with {status}: {stderr_msg}"
        )));
    }
    if let Err(err) = write_res {
        return Err(ZstdPipeError::Failed(format!(
            "zstd stdin write failed: {err}"
        )));
    }
    Ok(out)
}

/// Safely extract a `.tar.zst` (or `.tar.gz`) archive into `user_data_dir`.
///
/// Rejects path traversal (`..`, absolute paths, Windows prefixes, backslashes,
/// NUL bytes), symlinks, hardlinks, special files, and oversized archives.
pub fn unpack_user_data_archive(archive: &[u8], user_data_dir: &Path) -> Result<usize, String> {
    let tar_bytes = decompress_tar_bytes(archive)?;
    std::fs::create_dir_all(user_data_dir).map_err(|e| {
        format!(
            "failed creating profile directory `{}`: {e}",
            user_data_dir.display()
        )
    })?;

    let mut tar = tar::Archive::new(Cursor::new(tar_bytes));
    let entries = tar
        .entries()
        .map_err(|e| format!("invalid tar archive: {e}"))?;

    let mut extracted_files = 0usize;
    let mut total_bytes = 0u64;
    let mut entry_count = 0usize;

    for entry_res in entries {
        entry_count += 1;
        if entry_count > MAX_ARCHIVE_FILES {
            return Err(format!(
                "browser profile archive exceeds maximum entry count ({MAX_ARCHIVE_FILES})"
            ));
        }
        let mut entry = entry_res.map_err(|e| format!("corrupt tar entry: {e}"))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(
                "browser profile archive rejected: symlinks and hardlinks are forbidden".into(),
            );
        }
        if matches!(
            entry_type,
            tar::EntryType::XHeader
                | tar::EntryType::XGlobalHeader
                | tar::EntryType::GNULongName
                | tar::EntryType::GNULongLink
        ) {
            continue;
        }
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(format!(
                "browser profile archive rejected: unsupported entry type `{entry_type:?}`"
            ));
        }

        let raw_path_bytes = entry.path_bytes();
        if raw_path_bytes.contains(&0)
            || raw_path_bytes.contains(&b'\\')
            || raw_path_bytes.starts_with(b"/")
        {
            return Err(format!(
                "browser profile archive rejected: unsafe path `{}`",
                String::from_utf8_lossy(&raw_path_bytes)
            ));
        }
        let entry_path = entry
            .path()
            .map_err(|e| format!("invalid tar entry path: {e}"))?
            .into_owned();
        let clean_rel = sanitize_relative_tar_path(&entry_path)?;
        if clean_rel.as_os_str().is_empty() {
            if entry_type.is_dir() {
                continue;
            }
            return Err("browser profile archive rejected: empty file path".into());
        }

        let dest = user_data_dir.join(&clean_rel);
        ensure_no_symlink_ancestors(user_data_dir, &clean_rel)?;

        if entry_type.is_dir() {
            std::fs::create_dir_all(&dest)
                .map_err(|e| format!("failed creating dir `{}`: {e}", dest.display()))?;
        } else {
            let entry_size = entry.header().size().unwrap_or(0);
            total_bytes = total_bytes.saturating_add(entry_size);
            if total_bytes > MAX_UNPACKED_ARCHIVE_BYTES {
                return Err(format!(
                    "browser profile archive exceeds maximum unpacked size ({MAX_UNPACKED_ARCHIVE_BYTES} bytes)"
                ));
            }
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("failed creating parent `{}`: {e}", parent.display()))?;
            }
            let mut out_file = std::fs::File::create(&dest)
                .map_err(|e| format!("failed writing `{}`: {e}", dest.display()))?;
            let copied = std::io::copy(
                &mut (&mut entry).take(MAX_UNPACKED_ARCHIVE_BYTES.saturating_add(1)),
                &mut out_file,
            )
            .map_err(|e| format!("failed extracting `{}`: {e}", dest.display()))?;
            if copied > MAX_UNPACKED_ARCHIVE_BYTES {
                return Err("browser profile file exceeds maximum unpacked size".into());
            }
            extracted_files += 1;
        }
    }

    Ok(extracted_files)
}

fn sanitize_relative_tar_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Err(format!(
            "browser profile archive rejected: absolute path `{}`",
            path.display()
        ));
    }
    let mut clean = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => {
                let s = part.to_string_lossy();
                if s.is_empty()
                    || s == ".."
                    || s.contains('/')
                    || s.contains('\\')
                    || s.contains('\0')
                {
                    return Err(format!(
                        "browser profile archive rejected: unsafe path component `{s}`"
                    ));
                }
                clean.push(part);
            }
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "browser profile archive rejected: path traversal in `{}`",
                    path.display()
                ));
            }
        }
    }
    Ok(clean)
}

fn ensure_no_symlink_ancestors(base: &Path, rel: &Path) -> Result<(), String> {
    let mut cur = base.to_path_buf();
    for comp in rel.components() {
        if let Component::Normal(part) = comp {
            cur.push(part);
            if let Ok(meta) = std::fs::symlink_metadata(&cur)
                && meta.file_type().is_symlink()
            {
                return Err(format!(
                    "refusing to extract through symlink `{}`",
                    cur.display()
                ));
            }
        }
    }
    Ok(())
}

fn build_http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(15)))
        .timeout_global(Some(Duration::from_secs(120)))
        .http_status_as_error(false)
        .build()
        .new_agent()
}

fn read_error_detail(resp: &mut ureq::http::Response<ureq::Body>) -> String {
    let mut buf = Vec::new();
    let _ = resp.body_mut().as_reader().take(512).read_to_end(&mut buf);
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&buf)
        && let Some(msg) = val.get("error").and_then(serde_json::Value::as_str)
    {
        return msg.to_string();
    }
    String::from_utf8_lossy(&buf).trim().to_string()
}

macro_rules! with_auth {
    ($req:expr, $auth:expr) => {{
        let mut r = $req;
        for (k, v) in ($auth).headers() {
            r = r.header(k, &v);
        }
        r
    }};
}

/// Download and safely unpack a named browser profile archive from the
/// coordinator before launching Chromium.
///
/// - `200 OK`: extracts safely into a clean staging directory and replaces `user_data_dir`.
/// - `404 Not Found`: returns [`SyncDownOutcome::NotFoundFresh`] to start fresh.
/// - `403 Forbidden`: fails clearly stating the profile is not granted.
pub fn sync_down_profile(
    sync: &RemoteProfileSync,
    user_data_dir: &Path,
) -> Result<SyncDownOutcome, BrowserError> {
    let url = format!(
        "{}/v1/browser-profiles/{}/archive",
        sync.api_url.trim_end_matches('/'),
        sync.profile_name
    );
    let agent = build_http_agent();
    let mut resp = with_auth!(agent.get(&url), &sync.auth)
        .call()
        .map_err(|err| {
            BrowserError::new(
                "PROFILE_SYNC_ERROR",
                format!(
                    "failed downloading browser profile `{}` from coordinator: {err}",
                    sync.profile_name
                ),
            )
        })?;
    let status = resp.status().as_u16();
    match status {
        404 => Ok(SyncDownOutcome::NotFoundFresh),
        403 => {
            let detail = read_error_detail(&mut resp);
            let msg = if detail.is_empty() {
                format!(
                    "browser profile `{}` is not granted to this agent",
                    sync.profile_name
                )
            } else {
                format!(
                    "browser profile `{}` is not granted: {detail}",
                    sync.profile_name
                )
            };
            Err(BrowserError::new("PROFILE_FORBIDDEN", msg))
        }
        401 => Err(BrowserError::new(
            "UNAUTHORIZED",
            format!(
                "coordinator rejected agent credential while fetching browser profile `{}`",
                sync.profile_name
            ),
        )),
        200..=299 => {
            let mut archive = Vec::new();
            resp.body_mut()
                .as_reader()
                .take((MAX_PROFILE_ARCHIVE_BYTES as u64).saturating_add(1))
                .read_to_end(&mut archive)
                .map_err(|err| {
                    BrowserError::new(
                        "PROFILE_SYNC_ERROR",
                        format!("failed reading browser profile archive body: {err}"),
                    )
                })?;
            if archive.len() > MAX_PROFILE_ARCHIVE_BYTES {
                return Err(BrowserError::new(
                    "PROFILE_SYNC_ERROR",
                    format!(
                        "browser profile `{}` archive exceeds {} byte limit",
                        sync.profile_name, MAX_PROFILE_ARCHIVE_BYTES
                    ),
                ));
            }
            let parent = user_data_dir.parent().unwrap_or_else(|| Path::new("."));
            std::fs::create_dir_all(parent).map_err(|e| {
                BrowserError::new(
                    "PROFILE_SYNC_ERROR",
                    format!("failed creating profile parent `{}`: {e}", parent.display()),
                )
            })?;
            let stage = tempfile::Builder::new()
                .prefix(".profile-sync-stage-")
                .tempdir_in(parent)
                .map_err(|e| {
                    BrowserError::new(
                        "PROFILE_SYNC_ERROR",
                        format!("failed creating profile staging dir: {e}"),
                    )
                })?;
            let files = unpack_user_data_archive(&archive, stage.path())
                .map_err(|e| BrowserError::new("PROFILE_SYNC_ERROR", e))?;

            if user_data_dir.exists() {
                std::fs::remove_dir_all(user_data_dir).map_err(|e| {
                    BrowserError::new(
                        "PROFILE_SYNC_ERROR",
                        format!(
                            "failed clearing previous profile dir `{}`: {e}",
                            user_data_dir.display()
                        ),
                    )
                })?;
            }
            let stage_path = stage.keep();
            if let Err(err) = std::fs::rename(&stage_path, user_data_dir) {
                let _ = std::fs::remove_dir_all(&stage_path);
                return Err(BrowserError::new(
                    "PROFILE_SYNC_ERROR",
                    format!(
                        "failed moving extracted profile into `{}`: {err}",
                        user_data_dir.display()
                    ),
                ));
            }
            Ok(SyncDownOutcome::Restored {
                archive_bytes: archive.len(),
                files,
            })
        }
        other => {
            let detail = read_error_detail(&mut resp);
            Err(BrowserError::new(
                "PROFILE_SYNC_ERROR",
                format!(
                    "coordinator returned HTTP {other} for browser profile `{}`{}",
                    sync.profile_name,
                    if detail.is_empty() {
                        String::new()
                    } else {
                        format!(": {detail}")
                    }
                ),
            ))
        }
    }
}

/// Pack `user_data_dir` (stripping disposable caches and Chromium locks) and
/// upload it to the coordinator via `PUT /v1/browser-profiles/<profile>/archive`.
pub fn sync_up_profile(
    sync: &RemoteProfileSync,
    user_data_dir: &Path,
) -> Result<usize, BrowserError> {
    let (archive, content_type) = pack_user_data_dir(user_data_dir)
        .map_err(|e| BrowserError::new("PROFILE_SYNC_ERROR", e))?;
    let url = format!(
        "{}/v1/browser-profiles/{}/archive",
        sync.api_url.trim_end_matches('/'),
        sync.profile_name
    );
    let agent = build_http_agent();
    let mut resp = with_auth!(agent.put(&url), &sync.auth)
        .header("content-type", content_type)
        .send(archive.as_slice())
        .map_err(|err| {
            BrowserError::new(
                "PROFILE_SYNC_ERROR",
                format!(
                    "failed uploading browser profile `{}` to coordinator: {err}",
                    sync.profile_name
                ),
            )
        })?;
    let status = resp.status().as_u16();
    match status {
        200..=299 => Ok(archive.len()),
        401 => Err(BrowserError::new(
            "UNAUTHORIZED",
            format!(
                "coordinator rejected credential while uploading browser profile `{}`",
                sync.profile_name
            ),
        )),
        403 => {
            let detail = read_error_detail(&mut resp);
            Err(BrowserError::new(
                "PROFILE_FORBIDDEN",
                format!(
                    "browser profile `{}` upload denied (not granted){}",
                    sync.profile_name,
                    if detail.is_empty() {
                        String::new()
                    } else {
                        format!(": {detail}")
                    }
                ),
            ))
        }
        other => {
            let detail = read_error_detail(&mut resp);
            Err(BrowserError::new(
                "PROFILE_SYNC_ERROR",
                format!(
                    "coordinator returned HTTP {other} uploading browser profile `{}`{}",
                    sync.profile_name,
                    if detail.is_empty() {
                        String::new()
                    } else {
                        format!(": {detail}")
                    }
                ),
            ))
        }
    }
}

/// Query capability-scoped profiles from `GET <api_url>/v1/browser-profiles`.
pub fn list_remote_profiles(
    api_url: &str,
    auth: &CoordinatorAuth,
    active: Option<&str>,
) -> Result<serde_json::Value, BrowserError> {
    use serde_json::{Value, json};
    let url = format!("{}/v1/browser-profiles", api_url.trim_end_matches('/'));
    let mut resp = with_auth!(build_http_agent().get(&url), auth)
        .call()
        .map_err(|e| BrowserError::new("PROFILE_SYNC_ERROR", format!("list failed: {e}")))?;
    match resp.status().as_u16() {
        401 => Err(BrowserError::new("UNAUTHORIZED", "rejected credential")),
        403 => Err(BrowserError::new(
            "PROFILE_FORBIDDEN",
            read_error_detail(&mut resp),
        )),
        200..=299 => {
            let mut body = Vec::new();
            let _ = resp
                .body_mut()
                .as_reader()
                .take(4 * 1024 * 1024)
                .read_to_end(&mut body);
            let p: Value = serde_json::from_slice(&body)
                .map_err(|e| BrowserError::new("PROFILE_SYNC_ERROR", format!("bad JSON: {e}")))?;
            let (mut profiles, mut names) = (Vec::new(), Vec::new());
            for item in p["profiles"].as_array().into_iter().flatten() {
                if let Some(mut o) = item.as_object().cloned()
                    && let Some(n) = o.get("profile").and_then(Value::as_str).map(str::to_owned)
                {
                    o.insert("active".into(), json!(active == Some(n.as_str())));
                    names.push(n);
                    profiles.push(Value::Object(o));
                }
            }
            let s3 = p["s3_configured"].as_bool().unwrap_or(false);
            Ok(json!({
                "source": "coordinator", "scoped": auth.is_agent(), "s3_configured": s3,
                "active_profile": active, "count": profiles.len(),
                "profiles": profiles, "browser_profiles": names,
            }))
        }
        s => Err(BrowserError::new("PROFILE_SYNC_ERROR", format!("HTTP {s}"))),
    }
}

/// List local persistent browser profiles under `profile_dir` (standalone mode).
pub fn list_local_profiles(
    profile_dir: Option<&Path>,
    active: Option<&str>,
    running: bool,
) -> Result<serde_json::Value, BrowserError> {
    use serde_json::{Value, json};
    let mut names = std::collections::BTreeSet::new();
    if let Some(dir) = profile_dir
        && dir.is_dir()
    {
        for e in std::fs::read_dir(dir)
            .map_err(|e| BrowserError::new("IO_ERROR", format!("read: {e}")))?
            .flatten()
        {
            if let Ok(m) = std::fs::symlink_metadata(e.path())
                && !m.file_type().is_symlink()
                && m.file_type().is_dir()
                && let Some(n) = e.file_name().to_str()
                && n != "user-data"
                && validate_profile_name(n).is_ok()
            {
                names.insert(n.to_string());
            }
        }
    }
    let list: Vec<String> = names.iter().cloned().collect();
    let profiles: Vec<Value> = names
        .into_iter()
        .map(|n| {
            let act = active == Some(n.as_str());
            let st = if act && running { "running" } else { "stopped" };
            let ud = profile_dir.is_some_and(|d| d.join(&n).join("user-data").is_dir());
            json!({"profile": n, "status": st, "active": act, "has_user_data": ud})
        })
        .collect();
    Ok(json!({
        "source": "local", "scoped": false, "active_profile": active,
        "count": profiles.len(), "profiles": profiles, "browser_profiles": list,
    }))
}

pub fn list_profiles(
    config: &BrowserServerConfig,
    active: Option<&str>,
    running: bool,
) -> Result<serde_json::Value, BrowserError> {
    if let Some(api_url) = resolve_api_url(config)? {
        let auth = resolve_coordinator_auth(config.agent_token.as_deref())
            .ok_or_else(|| BrowserError::new("UNAUTHORIZED", "missing token"))?;
        return list_remote_profiles(&api_url, &auth, active);
    }
    list_local_profiles(config.profile_dir.as_deref(), active, running)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    #[test]
    fn pack_strips_caches_and_chromium_locks_and_preserves_session_data() {
        let src = tempfile::tempdir().unwrap();
        let ud = src.path().join("user-data");
        std::fs::create_dir_all(ud.join("Default/Cache")).unwrap();
        std::fs::create_dir_all(ud.join("Default/Code Cache/js")).unwrap();
        std::fs::create_dir_all(ud.join("Default/GPUCache")).unwrap();
        std::fs::create_dir_all(ud.join("GrShaderCache")).unwrap();
        std::fs::create_dir_all(ud.join("ShaderCache")).unwrap();
        std::fs::create_dir_all(ud.join("DawnCache")).unwrap();
        std::fs::create_dir_all(ud.join("GraphiteDawnCache")).unwrap();
        std::fs::create_dir_all(ud.join("Default/Service Worker/CacheStorage")).unwrap();
        std::fs::create_dir_all(ud.join("Default/Service Worker/Database")).unwrap();
        std::fs::create_dir_all(ud.join("Crashpad/reports")).unwrap();
        std::fs::create_dir_all(ud.join("Default/Local Storage/leveldb")).unwrap();

        // Disposable files that MUST be stripped.
        std::fs::write(ud.join("Default/Cache/big.bin"), b"cache-junk").unwrap();
        std::fs::write(ud.join("Default/Code Cache/js/a.bin"), b"code-cache").unwrap();
        std::fs::write(ud.join("Default/GPUCache/g.bin"), b"gpu-cache").unwrap();
        std::fs::write(ud.join("GrShaderCache/s.bin"), b"gr-shader").unwrap();
        std::fs::write(ud.join("ShaderCache/s.bin"), b"shader").unwrap();
        std::fs::write(ud.join("DawnCache/d.bin"), b"dawn").unwrap();
        std::fs::write(ud.join("GraphiteDawnCache/gd.bin"), b"gdawn").unwrap();
        std::fs::write(
            ud.join("Default/Service Worker/CacheStorage/c.bin"),
            b"sw-cache",
        )
        .unwrap();
        std::fs::write(ud.join("Crashpad/reports/dump.dmp"), b"crash").unwrap();
        std::fs::write(ud.join("SingletonLock"), b"lock").unwrap();
        std::fs::write(ud.join("SingletonCookie"), b"cookie").unwrap();
        std::fs::write(ud.join("SingletonSocket"), b"sock").unwrap();
        std::fs::write(ud.join("Default/LOCK"), b"db-lock").unwrap();
        std::fs::write(ud.join("takeover.lock"), b"takeover").unwrap();

        // Session data that MUST be preserved.
        std::fs::write(ud.join("Local State"), b"{\"os_crypt\":\"basic\"}").unwrap();
        std::fs::write(ud.join("Default/Cookies"), b"sqlite-cookies-bytes").unwrap();
        std::fs::write(
            ud.join("Default/Local Storage/leveldb/000001.ldb"),
            b"local-storage-data",
        )
        .unwrap();
        std::fs::write(ud.join("Default/Service Worker/Database/db"), b"sw-db-keep").unwrap();

        let (archive, _ct) = pack_user_data_dir(&ud).unwrap();
        #[cfg(target_os = "linux")]
        if find_zstd_binary().is_some() {
            assert!(
                archive.starts_with(ZSTD_MAGIC),
                "expected zstd frame magic when zstd is installed"
            );
        }

        let dst = tempfile::tempdir().unwrap();
        let restored = dst.path().join("user-data");
        let files = unpack_user_data_archive(&archive, &restored).unwrap();
        assert_eq!(files, 4);

        // Preserved session files exist with exact content.
        assert_eq!(
            std::fs::read(restored.join("Local State")).unwrap(),
            b"{\"os_crypt\":\"basic\"}"
        );
        assert_eq!(
            std::fs::read(restored.join("Default/Cookies")).unwrap(),
            b"sqlite-cookies-bytes"
        );
        assert_eq!(
            std::fs::read(restored.join("Default/Local Storage/leveldb/000001.ldb")).unwrap(),
            b"local-storage-data"
        );
        assert_eq!(
            std::fs::read(restored.join("Default/Service Worker/Database/db")).unwrap(),
            b"sw-db-keep"
        );

        // Stripped caches & lock files are completely absent.
        for stripped in [
            "Default/Cache",
            "Default/Code Cache",
            "Default/GPUCache",
            "GrShaderCache",
            "ShaderCache",
            "DawnCache",
            "GraphiteDawnCache",
            "Default/Service Worker/CacheStorage",
            "Crashpad",
            "SingletonLock",
            "SingletonCookie",
            "SingletonSocket",
            "Default/LOCK",
            "takeover.lock",
        ] {
            assert!(
                !restored.join(stripped).exists(),
                "expected `{stripped}` to be stripped"
            );
        }
    }

    fn compress_raw_tar(tar_bytes: &[u8]) -> Vec<u8> {
        compress_tar_bytes(tar_bytes).unwrap().0
    }

    #[test]
    fn unpack_rejects_path_traversal_absolute_paths_and_symlinks() {
        let dst = tempfile::tempdir().unwrap();

        // 1. Path traversal (`../escape.txt`) written directly in tar header bytes.
        let mut traversal_tar = Vec::new();
        {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(4);
            header.set_mode(0o644);
            // Write raw name bytes directly to bypass tar::Builder's own guard.
            let raw_name = b"../escape.txt";
            header.as_mut_bytes()[..raw_name.len()].copy_from_slice(raw_name);
            header.set_cksum();
            traversal_tar.extend_from_slice(header.as_bytes());
            let mut block = [0u8; 512];
            block[..4].copy_from_slice(b"evil");
            traversal_tar.extend_from_slice(&block);
            traversal_tar.extend_from_slice(&[0u8; 1024]);
        }
        let err =
            unpack_user_data_archive(&compress_raw_tar(&traversal_tar), dst.path()).unwrap_err();
        assert!(err.contains("traversal"), "unexpected error: {err}");
        assert!(!dst.path().parent().unwrap().join("escape.txt").exists());

        // 2. Symlink entry rejected.
        let mut symlink_tar = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut symlink_tar);
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            builder
                .append_link(&mut header, "evil-link", "/etc/passwd")
                .unwrap();
            builder.finish().unwrap();
        }
        let err =
            unpack_user_data_archive(&compress_raw_tar(&symlink_tar), dst.path()).unwrap_err();
        assert!(err.contains("symlink"), "unexpected error: {err}");
        assert!(!dst.path().join("evil-link").exists());
    }

    #[test]
    fn sync_down_and_sync_up_roundtrip_and_handle_404_and_403() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stored: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let stored_srv = Arc::clone(&stored);

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("/").to_string();
                let mut content_length = 0usize;
                let mut token = String::new();
                let mut bearer = String::new();
                let mut op_tok = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0
                        || line == "\r\n"
                        || line == "\n"
                    {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        if k.trim().eq_ignore_ascii_case("content-length") {
                            content_length = v.trim().parse().unwrap_or(0);
                        } else if k.trim().eq_ignore_ascii_case("x-anyagents-agent") {
                            token = v.trim().to_string();
                        } else if k.trim().eq_ignore_ascii_case("authorization") {
                            bearer = v.trim().to_string();
                        } else if k.trim().eq_ignore_ascii_case("x-anyagents-operator") {
                            op_tok = v.trim().to_string();
                        }
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = reader.read_exact(&mut body);
                }
                let mut stream = stream;
                if path == "/v1/browser-profiles" || (!token.is_empty() && !op_tok.is_empty()) {
                    let (st, m) = match (token.as_str(), bearer.as_str(), op_tok.as_str()) {
                        ("valid-agent-token", "Bearer valid-agent-token", "") => (
                            "200 OK",
                            r#"{"s3_configured":true,"profiles":[{"profile":"demo-profile","granted":true}]}"#,
                        ),
                        ("other-agent-token", "Bearer other-agent-token", "") => {
                            ("200 OK", r#"{"s3_configured":true,"profiles":[]}"#)
                        }
                        ("", "", "op-token") => (
                            "200 OK",
                            r#"{"s3_configured":true,"profiles":[{"profile":"demo-profile"},{"profile":"private-profile"}]}"#,
                        ),
                        _ => ("401 Unauthorized", r#"{"error":"unauthorized"}"#),
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {st}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{m}",
                        m.len()
                    );
                    continue;
                }
                if token != "valid-agent-token" || bearer != "Bearer valid-agent-token" {
                    let msg = r#"{"error":"profile not granted"}"#;
                    let _ = write!(
                        stream,
                        "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        msg.len(),
                        msg
                    );
                    continue;
                }
                let mut map = stored_srv.lock().unwrap();
                match method.as_str() {
                    "GET" => {
                        if let Some(data) = map.get(&path) {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Type: application/zstd\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                            let _ = stream.write_all(data);
                        } else {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            );
                        }
                    }
                    "PUT" => {
                        map.insert(path, body);
                        let msg = r#"{"stored":true}"#;
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            msg.len(),
                            msg
                        );
                    }
                    _ => {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                }
            }
        });

        let api_url = format!("http://{addr}");
        let sync = RemoteProfileSync {
            api_url: api_url.clone(),
            auth: CoordinatorAuth::Agent("valid-agent-token".into()),
            profile_name: "demo-profile".into(),
        };

        // 1. 404 -> NotFoundFresh.
        let work = tempfile::tempdir().unwrap();
        let ud = work.path().join("user-data");
        assert_eq!(
            sync_down_profile(&sync, &ud).unwrap(),
            SyncDownOutcome::NotFoundFresh
        );

        // 2. Populate local profile, sync up, then sync down into a brand new dir.
        std::fs::create_dir_all(ud.join("Default")).unwrap();
        std::fs::write(ud.join("Default/Cookies"), b"cookie-jar-v1").unwrap();
        let uploaded = sync_up_profile(&sync, &ud).unwrap();
        assert!(uploaded > 0);

        let fresh = tempfile::tempdir().unwrap();
        let fresh_ud = fresh.path().join("user-data");
        let outcome = sync_down_profile(&sync, &fresh_ud).unwrap();
        assert!(matches!(
            outcome,
            SyncDownOutcome::Restored { files: 1, .. }
        ));
        assert_eq!(
            std::fs::read(fresh_ud.join("Default/Cookies")).unwrap(),
            b"cookie-jar-v1"
        );

        // 3. 403 -> PROFILE_FORBIDDEN with clear message.
        let denied_sync = RemoteProfileSync {
            api_url: api_url.clone(),
            auth: CoordinatorAuth::Agent("ungranted-token".into()),
            profile_name: "demo-profile".into(),
        };
        let err = sync_down_profile(&denied_sync, &fresh_ud).unwrap_err();
        assert_eq!(err.code, "PROFILE_FORBIDDEN");
        assert!(err.message.contains("not granted"));

        // 4. Capability-scoped GET /v1/browser-profiles isolates agents & fails closed.
        let a = CoordinatorAuth::Agent("valid-agent-token".into());
        let b = CoordinatorAuth::Agent("other-agent-token".into());
        let op = CoordinatorAuth::Operator("op-token".into());
        let la = list_remote_profiles(&api_url, &a, Some("demo-profile")).unwrap();
        assert!(la["scoped"] == true && la["profiles"][0]["active"] == true);
        assert_eq!(la["browser_profiles"], serde_json::json!(["demo-profile"]));
        let lb = list_remote_profiles(&api_url, &b, None).unwrap();
        let lo = list_remote_profiles(&api_url, &op, None).unwrap();
        assert!(lb["count"] == 0 && lo["scoped"] == false && lo["count"] == 2);
        let bad = CoordinatorAuth::Agent("bad".into());
        assert_eq!(
            list_remote_profiles(&api_url, &bad, None).unwrap_err().code,
            "UNAUTHORIZED"
        );
    }

    #[test]
    fn coordinator_auth_precedence_single_authority_and_local_profile_filter() {
        let dir = tempfile::tempdir().unwrap();
        let (mcp_f, op_f) = (dir.path().join("mcp"), dir.path().join("op"));
        std::fs::write(&mcp_f, "file-agent\n").unwrap();
        std::fs::write(&op_f, "file-op\n").unwrap();
        let exp = resolve_coordinator_auth_from(Some("exp"), |k| match k {
            AGENT_TOKEN_ENV => Some("env-a".into()),
            OPERATOR_TOKEN_ENV => Some("env-op".into()),
            _ => None,
        })
        .unwrap();
        let h = exp.headers();
        assert!(exp.is_agent() && !format!("{exp:?}").contains("exp"));
        assert!(h[0].0 == "x-anyagents-agent" && h[1].1 == "Bearer exp");
        for key in [
            AGENT_TOKEN_ENV,
            CAPABILITY_TOKEN_ENV,
            CAPABILITY_ENV,
            SESSION_TOKEN_ENV,
            MCP_TOKEN_ENV,
        ] {
            let a = resolve_coordinator_auth_from(None, |k| (k == key).then(|| "tok".into()));
            assert!(
                a.unwrap()
                    .headers()
                    .iter()
                    .all(|(k, _)| *k != "x-anyagents-operator")
            );
        }
        let (ms, os) = (mcp_f.display().to_string(), op_f.display().to_string());
        let fa = resolve_coordinator_auth_from(None, |k| match k {
            MCP_TOKEN_FILE_ENV => Some(ms.clone()),
            OPERATOR_TOKEN_ENV => Some("op".into()),
            _ => None,
        });
        assert_eq!(fa, Some(CoordinatorAuth::Agent("file-agent".into())));
        let op = resolve_coordinator_auth_from(None, |k| {
            (k == OPERATOR_TOKEN_FILE_ENV).then(|| os.clone())
        })
        .unwrap();
        assert!(
            op.headers()[0] == ("x-anyagents-operator", "file-op".into())
                && !format!("{op:?}").contains("file-op")
        );

        let pdir = dir.path().join("profiles");
        for d in ["work/user-data", "research", "user-data", ".stage"] {
            std::fs::create_dir_all(pdir.join(d)).unwrap();
        }
        std::fs::write(pdir.join("file"), b"x").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(pdir.join("work"), pdir.join("linked")).unwrap();
        let l = list_local_profiles(Some(&pdir), Some("work"), true).unwrap();
        assert_eq!(
            l["browser_profiles"],
            serde_json::json!(["research", "work"])
        );
        assert!(l["profiles"][1]["status"] == "running");
        assert!(l["profiles"][1]["has_user_data"] == true);
    }

    #[test]
    fn screenshot_dir_aligns_with_sandbox_agent_data_dir() {
        let fallback = Path::new("/tmp");
        let agent_data = Path::new("/home/user/agent-data");
        assert_eq!(
            resolve_screenshot_dir_from(None, Some(agent_data), None, None, fallback),
            PathBuf::from("/home/user/agent-data/tmp/screenshots")
        );
        assert_eq!(
            resolve_screenshot_dir_from(None, None, Some(agent_data), None, fallback),
            PathBuf::from("/home/user/agent-data/tmp/screenshots")
        );
        let explicit = Path::new("/custom/shots");
        assert_eq!(
            resolve_screenshot_dir_from(Some(explicit), Some(agent_data), None, None, fallback),
            PathBuf::from("/custom/shots")
        );

        // Skips nonexistent host path in favor of existing sandbox dir.
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("agent-data");
        std::fs::create_dir_all(&real).unwrap();
        assert_eq!(
            pick_existing_data_dir(&[Some(&tmp.path().join("missing")), Some(&real)]),
            Some(real.as_path())
        );

        let cfg = BrowserServerConfig {
            screenshot_dir: Some(PathBuf::from("/home/user/agent-data/tmp/screenshots")),
            ..Default::default()
        };
        let auto = resolve_screenshot_path(&cfg, None);
        assert!(auto.starts_with("/home/user/agent-data/tmp/screenshots"));
        assert!(
            auto.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("shot-") && n.ends_with(".png"))
        );
        assert_eq!(
            resolve_screenshot_path(&cfg, Some("page.png")),
            PathBuf::from("/home/user/agent-data/tmp/screenshots/page.png")
        );
        assert_eq!(
            resolve_screenshot_path(&cfg, Some("/tmp/abs.png")),
            PathBuf::from("/tmp/abs.png")
        );
    }
}
