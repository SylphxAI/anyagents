//! The agent's desktop session: one headless Wayland desktop per browser
//! profile, running one headful browser, from the `agent-workstation` crate.
//!
//! The server owns every session ([`session`]); `tools.browser` drives the
//! browser over the crate's web operations ([`browser`]) and session
//! `tools.computer` requests drive the same desktop's pixels ([`desktop`]).
//! One agent holds a session at a time (the crate's lease); another agent's
//! call for that profile gets `DESKTOP_IN_USE`. AnyAgents supplies only
//! caller policy: profile names and directories, who the caller is, secret
//! references ([`secrets`]), proxy and browser settings from the environment
//! and the toolchain, idle reaping, and takeover delivery ([`takeover`]).
//!
//! Sessions need Linux. Other hosts answer every call with a typed
//! `UNSUPPORTED` error that names [`REMOTE_WORKSTATION_ENV`]; there the
//! session helpers below are compiled but unused.
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::path::{Path, PathBuf};

use anyagents_core::Error;

use crate::computer::{ErrorDisposition, format_computer_error};

pub mod browser;
#[cfg(target_os = "linux")]
pub mod desktop;
pub(crate) mod jail;
#[cfg(target_os = "linux")]
mod proxy;
pub mod secrets;
#[cfg(target_os = "linux")]
pub mod session;
#[cfg(target_os = "linux")]
pub mod takeover;

#[cfg(all(test, target_os = "linux"))]
mod headful_tests;

pub use browser::BrowserRequest;

/// The lease principal of operator calls (the CLI and operator API).
pub const OPERATOR: &str = "operator";

/// Who a desktop call is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    /// Lease principal: the agent's name, or [`OPERATOR`].
    pub principal: String,
    pub agent_id: Option<String>,
    /// The caller's own browser profile (used when a request names none).
    pub profile: String,
}

#[cfg(any(test, not(target_os = "linux")))]
/// Setting that remote routing reads: the base URL of a Linux workstation
/// that serves desktop sessions for a host without one.
pub const REMOTE_WORKSTATION_ENV: &str = "ANYAGENTS_WORKSTATION_URL";

/// Root of every browser profile.
pub fn profiles_root(data: &Path) -> PathBuf {
    data.join("browser-profiles")
}

/// Directory of `profile` (`user-data/` and `fingerprint.json` inside).
pub fn profile_dir(data: &Path, profile: &str) -> PathBuf {
    profiles_root(data).join(profile)
}

// Profile trees are server-owned. Host ancestors are operator-managed:
// callers must ensure seats cannot rename those host directories.
#[cfg(target_os = "linux")]
fn trusted_directory(
    dir: &std::fs::File,
    server_only: bool,
    sticky_parent: bool,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = dir.metadata()?;
    let server = crate::sandbox::server_uid();
    if !meta.is_dir() || (meta.uid() != server && (server_only || meta.uid() != 0)) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "a directory outside server/root ownership",
        ));
    }
    if meta.mode() & 0o022 != 0 && !(sticky_parent && meta.mode() & 0o1000 != 0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "a group/other-writable directory permits profile replacement",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_directory_at(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
) -> std::io::Result<Option<std::fs::File>> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    if Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "expected one directory name",
        ));
    }
    let c = std::ffi::CString::new(name.as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: a live directory descriptor and a NUL-terminated single component.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            c.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(None),
            Some(libc::ELOOP | libc::ENOTDIR) => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "a link or a file where a directory belongs",
            )),
            _ => Err(error),
        };
    }
    // SAFETY: openat returned a new descriptor.
    Ok(Some(unsafe { std::fs::File::from_raw_fd(fd) }))
}

/// Walk from an anchored root, never following even an ancestor symlink.
/// Host ancestor ownership/modes are operator-managed and not profile policy.
/// Ownership/mode requirements are applied by the caller on its boundary.
#[cfg(target_os = "linux")]
fn directory_chain(data: &Path) -> std::io::Result<(std::fs::File, PathBuf)> {
    let absolute = if data.is_absolute() {
        data.to_path_buf()
    } else {
        std::env::current_dir()?.join(data)
    };
    let mut dir = std::fs::File::open("/")?;
    let mut path = PathBuf::from("/");
    for component in absolute.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => {
                dir = open_directory_at(&dir, name)?.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "data directory ancestor is missing",
                    )
                })?;
                path.push(name);
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "data path must not contain parent traversal",
                ));
            }
        }
    }
    Ok((dir, path))
}

#[cfg(target_os = "linux")]
fn trusted_data_dir(data: &Path) -> std::io::Result<(std::fs::File, PathBuf)> {
    let (dir, path) = directory_chain(data)?;
    trusted_root_directory(&dir)?;
    Ok((dir, path))
}

/// Shared containers allow sticky group writes, never world writes.
#[cfg(target_os = "linux")]
fn trusted_root_directory(dir: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = dir.metadata()?;
    if !meta.is_dir()
        || meta.uid() != crate::sandbox::server_uid()
        || meta.mode() & 0o002 != 0
        || (meta.mode() & 0o020 != 0 && meta.mode() & 0o1000 == 0)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "shared profile container requires server ownership, no world writes, and sticky group writes",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn owned_root_dir(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
    mode: libc::mode_t,
    create: bool,
) -> std::io::Result<Option<std::fs::File>> {
    trusted_root_directory(parent)?;
    child_dir(parent, name, mode, create, true)
}

#[cfg(target_os = "linux")]
fn owned_child_dir(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
    mode: libc::mode_t,
    create: bool,
) -> std::io::Result<Option<std::fs::File>> {
    child_dir(parent, name, mode, create, false)
}

#[cfg(target_os = "linux")]
fn child_dir(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
    mode: libc::mode_t,
    create: bool,
    shared_root: bool,
) -> std::io::Result<Option<std::fs::File>> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;
    trusted_directory(parent, false, true)?;
    // Validate before mkdirat as well as openat: neither accepts traversal.
    if Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "expected one directory name",
        ));
    }
    let c = std::ffi::CString::new(name.as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: a directory descriptor and a NUL-terminated single name.
    if create && unsafe { libc::mkdirat(parent.as_raw_fd(), c.as_ptr(), mode) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    let dir = open_directory_at(parent, name)?;
    if let Some(dir) = &dir {
        if shared_root {
            trusted_root_directory(dir)?;
        } else {
            trusted_directory(dir, true, false)?;
        }
    }
    Ok(dir)
}

// These leaves are consumed by path-based upstream startup before the jail runs.
// Protected directory boundaries alone do not make an existing leaf safe.
#[cfg(target_os = "linux")]
fn validate_startup_leaves(profile: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if let Some(dir) = open_directory_at(profile, "user-data".as_ref())? {
        trusted_directory(&dir, true, false)?;
        if dir.metadata()?.mode() & 0o077 != 0 {
            dir.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let name = c"fingerprint.json";
    // SAFETY: profile is a live directory descriptor and name is NUL terminated.
    let fd = unsafe {
        libc::openat(
            profile.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let e = std::io::Error::last_os_error();
        return if e.kind() == std::io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(e)
        };
    }
    // SAFETY: openat returned a new descriptor exclusively owned here.
    let file = unsafe { std::fs::File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != crate::sandbox::server_uid()
        || metadata.mode() & 0o022 != 0
        || metadata.nlink() != 1
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "fingerprint is not a protected server-owned regular file",
        ));
    }
    Ok(())
}

/// The profile's server-owned directory. Ancestors are descriptor-walked from
/// root without following links; their access policy belongs to the operator.
/// Data and browser-profiles allow sticky group writes, never world writes.
/// Individual profiles and leaves must not be group/other-writable.
/// Missing profiles return None without creation when create is false.
#[cfg(target_os = "linux")]
pub(crate) fn server_profile_dir(
    data: &Path,
    profile: &str,
    create: bool,
) -> Result<Option<PathBuf>, Error> {
    if profile.is_empty()
        || Path::new(profile).components().count() != 1
        || !matches!(
            Path::new(profile).components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(Error::Message(
            "a browser profile is one directory name".into(),
        ));
    }
    let refuse = |error: std::io::Error| {
        Error::Message(format!(
            "browser profile `{profile}` is not the server's own directory: {error}"
        ))
    };
    let (data_dir, data_path) = trusted_data_dir(data).map_err(refuse)?;
    let Some(root) =
        owned_root_dir(&data_dir, "browser-profiles".as_ref(), 0o755, create).map_err(refuse)?
    else {
        return Ok(None);
    };
    match owned_child_dir(&root, profile.as_ref(), 0o755, create).map_err(refuse)? {
        Some(dir) => {
            validate_startup_leaves(&dir).map_err(refuse)?;
            Ok(Some(profile_dir(&data_path, profile)))
        }
        None => Ok(None),
    }
}

#[cfg(target_os = "linux")]
fn private_dir(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(Error::Message(format!("{} has no parent", path.display())));
    };
    let (parent, _) = directory_chain(parent)?;
    let dir = owned_child_dir(&parent, name, 0o700, true)
        .map_err(|error| Error::Message(format!("{}: {error}", path.display())))?
        .ok_or_else(|| Error::Message(format!("{} is missing", path.display())))?;
    if dir.metadata()?.mode() & 0o077 != 0 {
        dir.set_permissions(std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// A filesystem-safe profile name; `fallback` when `raw` is empty.
pub fn sanitize_profile_name(raw: Option<&str>, fallback: &str) -> String {
    let chosen = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(fallback);
    let cleaned: String = chosen
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('-').trim_matches('.');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

/// A tool failure with a stable machine-readable code.
///
/// Rendered through [`format_computer_error`], so agents see the same
/// `code` + `disposition` shape as every other desktop tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    /// Stable code such as `UNSUPPORTED`, `STALE_REF`, `HUMAN_IN_CONTROL`.
    pub code: &'static str,
    /// Human-readable message (never a secret value).
    pub message: String,
    /// What the agent should do next.
    pub hint: &'static str,
    /// Whether the same call can succeed later without changes.
    pub retryable: bool,
}

impl ToolError {
    /// The capability does not exist on this host or engine.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: "UNSUPPORTED",
            message: message.into(),
            hint: "This capability is not available here; do not retry.",
            retryable: false,
        }
    }

    /// The caller passed an argument the tool refuses.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_INPUT",
            message: message.into(),
            hint: "Fix the request arguments.",
            retryable: false,
        }
    }

    /// A snapshot-qualified ref names an older snapshot.
    pub fn stale_ref(message: impl Into<String>) -> Self {
        Self {
            code: "STALE_REF",
            message: message.into(),
            hint: "Call browser({action: 'snapshot'}) to refresh refs.",
            retryable: false,
        }
    }

    /// An explicit `target_id` is absent from the live CDP target list.
    pub fn stale_target(message: impl Into<String>) -> Self {
        Self {
            code: "STALE_TARGET",
            message: message.into(),
            hint: "Target tab no longer exists in the live browser session (closed or restarted). Call browser({action: 'tabs'}) to list live targets.",
            retryable: false,
        }
    }

    fn disposition(&self) -> ErrorDisposition {
        let mut d = match (self.code, self.retryable) {
            ("STALE_REF", _) => ErrorDisposition::stale_ref(self.hint),
            ("NOT_FOUND" | "STALE_TARGET", _) => ErrorDisposition::settled_absence(self.hint),
            (_, true) => ErrorDisposition::retryable(self.hint),
            (_, false) => ErrorDisposition::terminal(self.hint),
        };
        d.code = Some(self.code.to_string());
        d
    }

    /// A timed-out mutation may have reached the browser. Never offer a
    /// blind retry for it; a startup refusal has its own STARTING code.
    pub fn into_action_error(self, redact: bool, mutating: bool) -> Error {
        if !mutating || self.code != "TIMEOUT" {
            return self.into_error(redact);
        }
        let mut disposition = ErrorDisposition::delivery_uncertain(
            "Inspect the current page before repeating this action; it may already have completed.",
        );
        disposition.code = Some(self.code.to_string());
        let message = if redact {
            "browser secret input failed (provider/page details withheld)".to_string()
        } else {
            self.message
        };
        Error::Message(format_computer_error(message, disposition))
    }

    /// Render for the tool surface; `redact` replaces the message for calls
    /// that carried a secret reference.
    pub fn into_error(self, redact: bool) -> Error {
        let message = if redact {
            "browser secret input failed (provider/page details withheld)".to_string()
        } else {
            self.message.clone()
        };
        Error::Message(format_computer_error(message, self.disposition()))
    }
}

impl From<ToolError> for Error {
    fn from(e: ToolError) -> Self {
        e.into_error(false)
    }
}

#[cfg(target_os = "linux")]
impl From<agent_workstation::Error> for ToolError {
    fn from(e: agent_workstation::Error) -> Self {
        use agent_workstation::Error as W;
        let (code, hint) = match &e {
            W::HumanInControl { .. } => (
                "HUMAN_IN_CONTROL",
                "A person controls this desktop; call browser({action: 'wait_for_user'}) or release_takeover.",
            ),
            W::Leased { .. } => (
                "DESKTOP_IN_USE",
                "Another agent holds this profile's desktop; use another `profile`.",
            ),
            W::StaleRef(_) => (
                "STALE_REF",
                "Call browser({action: 'snapshot'}) to refresh refs.",
            ),
            W::Script(_) => ("SCRIPT_ERROR", "The page script threw; fix the expression."),
            W::NotFound(_) => (
                "NOT_FOUND",
                "Inspect the page with a new browser snapshot before acting.",
            ),
            W::InvalidInput(_) => ("INVALID_INPUT", "Fix the request arguments."),
            W::Unsupported(_) => (
                "UNSUPPORTED",
                "This capability is not available here; do not retry.",
            ),
            W::Refused(_) => (
                "REFUSED",
                "A precondition refused the action; inspect the page first.",
            ),
            W::Starting { .. } => (
                "STARTING",
                "The desktop is starting; inspect browser status or retry later.",
            ),
            W::Timeout(_) => (
                "TIMEOUT",
                "Transient browser failure; retry once or inspect browser status.",
            ),
            W::Profile(_) => (
                "PROFILE",
                "The profile directory is unusable or held by another browser.",
            ),
            W::Closed => (
                "CLOSED",
                "The desktop session closed; the next call starts a new one.",
            ),
            _ => (
                "DESKTOP_FAILURE",
                "Check browser status, the desktop runtime, and takeover state.",
            ),
        };
        Self {
            code,
            message: e.to_string(),
            hint,
            retryable: e.is_transient() || matches!(e, W::Closed),
        }
    }
}

/// The error every session call returns on a host without session desktops.
#[cfg(any(test, not(target_os = "linux")))]
pub fn unsupported_host() -> ToolError {
    ToolError::unsupported(format!(
        "desktop sessions (tools.browser and session tools.computer) need a Linux host, and this \
         AnyAgents host runs {os}. Point this host at a Linux workstation with \
         {REMOTE_WORKSTATION_ENV}, for example \
         {REMOTE_WORKSTATION_ENV}=https://linux-workstation.example:7600; that workstation runs the \
         agent-workstation daemon (sway, wayvnc and a Chromium-family browser installed, started \
         with AGENT_WORKSTATION_TOKEN). This release does not route through \
         {REMOTE_WORKSTATION_ENV} yet, so until it does, run browser work on a Linux AnyAgents \
         host. Host desktop control (tools.computer with `platform`) is unaffected.",
        os = std::env::consts::OS,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_names_are_sanitized() {
        assert_eq!(sanitize_profile_name(Some(" Work Bot "), "x"), "work-bot");
        assert_eq!(sanitize_profile_name(Some(""), "agent-a"), "agent-a");
        let climbing = sanitize_profile_name(Some("../.."), "x");
        assert!(
            !climbing.contains('/') && climbing != ".." && climbing != ".",
            "{climbing}"
        );
        assert_eq!(sanitize_profile_name(Some("--"), "x"), "default");
    }

    #[test]
    fn unsupported_host_error_is_typed_and_names_the_routing_setting() {
        let rendered = unsupported_host().into_error(false).to_string();
        let json: serde_json::Value =
            serde_json::from_str(rendered.split_once(" | ").unwrap().1).unwrap();
        assert_eq!(json["code"], "UNSUPPORTED");
        assert_eq!(json["disposition"]["retry"], "never");
        let message = json["error"].as_str().unwrap();
        assert!(message.contains(REMOTE_WORKSTATION_ENV), "{message}");
        assert!(
            message.contains("ANYAGENTS_WORKSTATION_URL=https://"),
            "names an example value: {message}"
        );
        assert!(message.contains(std::env::consts::OS), "{message}");
    }

    #[test]
    fn secret_calls_redact_messages_but_keep_codes() {
        let rendered = ToolError::invalid("op://vault/item/password is wrong")
            .into_error(true)
            .to_string();
        assert!(rendered.contains("INVALID_INPUT"), "{rendered}");
        assert!(!rendered.contains("vault"), "{rendered}");
    }
}

#[cfg(all(test, target_os = "linux"))]
mod profile_authority_tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir_in("/tmp").unwrap()
    }
    fn mode(path: &Path, value: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(value)).unwrap();
    }
    fn create(data: &Path) -> PathBuf {
        server_profile_dir(data, "profile", true).unwrap().unwrap()
    }

    #[test]
    fn private_runtime_directories_accept_tmp_and_refuse_links_or_writable_parents() {
        let root = temp();
        super::private_dir(root.path()).unwrap();
        let child = root.path().join("chromium-tmp");
        super::private_dir(&child).unwrap();
        assert_eq!(
            fs::metadata(&child).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let link = root.path().join("linked");
        symlink(&child, &link).unwrap();
        assert!(super::private_dir(&link).is_err());
        mode(root.path(), 0o777);
        let blocked = root.path().join("blocked");
        assert!(super::private_dir(&blocked).is_err());
        assert!(!blocked.exists());
        mode(root.path(), 0o700);
    }

    #[test]
    fn protected_profile_creation_and_lookup_preserve_existing_data() {
        let data = temp();
        assert!(
            server_profile_dir(data.path(), "profile", false)
                .unwrap()
                .is_none()
        );
        assert!(!profiles_root(data.path()).exists());
        let profile = create(data.path());
        fs::create_dir(profile.join("user-data")).unwrap();
        mode(&profile.join("user-data"), 0o755);
        fs::write(profile.join("user-data/cookies"), "login").unwrap();
        fs::write(profile.join("fingerprint.json"), "fingerprint").unwrap();
        mode(&profile.join("fingerprint.json"), 0o644);
        assert_eq!(
            server_profile_dir(data.path(), "profile", false).unwrap(),
            Some(profile.clone())
        );
        assert_eq!(
            fs::read_to_string(profile.join("user-data/cookies")).unwrap(),
            "login"
        );
        assert_eq!(
            fs::metadata(profile.join("user-data"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for name in ["", ".", "..", "../escape", "one/two", "/absolute"] {
            assert!(
                server_profile_dir(data.path(), name, true).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn writable_data_roots_and_profiles_are_refused() {
        let outer = temp();
        let data = outer.path().join("data");
        fs::create_dir(&data).unwrap();
        mode(&data, 0o700);
        let profile = create(&data);
        for target in [data.clone(), profiles_root(&data), profile.clone()] {
            for permissions in [0o775, 0o777, 0o1777] {
                mode(&target, permissions);
                assert!(
                    server_profile_dir(&data, "profile", false).is_err(),
                    "{} mode {permissions:o}",
                    target.display()
                );
                mode(&target, 0o755);
            }
        }
        mode(&data, 0o3771);
        mode(&profiles_root(&data), 0o3770);
        mode(&profile, 0o2755);
        assert_eq!(
            server_profile_dir(&data, "profile", false).unwrap(),
            Some(profile.clone())
        );
        for target in [profiles_root(&data), profile] {
            mode(&target, 0o1777);
            assert!(server_profile_dir(&data, "profile", false).is_err());
            mode(&target, 0o755);
        }
    }

    #[test]
    fn host_ancestor_modes_are_outside_profile_policy() {
        let outer = temp();
        let data = outer.path().join("data");
        fs::create_dir(&data).unwrap();
        mode(&data, 0o700);
        mode(outer.path(), 0o1777);
        mode(&data, 0o700);
        let profile = create(&data);
        assert_eq!(
            server_profile_dir(&data, "profile", false).unwrap(),
            Some(profile)
        );
        mode(outer.path(), 0o2775);
        assert!(
            server_profile_dir(&data, "profile", false)
                .unwrap()
                .is_some()
        );
        mode(outer.path(), 0o700);
    }

    #[test]
    fn no_ancestor_root_profile_or_user_data_link_is_followed() {
        let base = temp();
        let actual = base.path().join("actual");
        fs::create_dir(&actual).unwrap();
        mode(&actual, 0o700);
        let alias = base.path().join("alias");
        symlink(&actual, &alias).unwrap();
        assert!(server_profile_dir(&alias, "profile", true).is_err());
        assert!(!profiles_root(&actual).exists());
        let outside = temp();
        symlink(outside.path(), profiles_root(&actual)).unwrap();
        assert!(server_profile_dir(&actual, "profile", true).is_err());
        fs::remove_file(profiles_root(&actual)).unwrap();
        fs::create_dir(profiles_root(&actual)).unwrap();
        mode(&profiles_root(&actual), 0o755);
        let profile = profile_dir(&actual, "profile");
        symlink(outside.path(), &profile).unwrap();
        assert!(server_profile_dir(&actual, "profile", true).is_err());
        fs::remove_file(&profile).unwrap();
        let profile = create(&actual);
        symlink(outside.path(), profile.join("user-data")).unwrap();
        assert!(server_profile_dir(&actual, "profile", false).is_err());
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
    }

    #[test]
    fn fingerprint_links_writable_files_and_nonregular_files_are_refused() {
        let data = temp();
        let profile = create(data.path());
        let leaf = profile.join("fingerprint.json");
        let outside = temp();
        let target = outside.path().join("private");
        fs::write(&target, "untouched").unwrap();
        symlink(&target, &leaf).unwrap();
        assert!(server_profile_dir(data.path(), "profile", false).is_err());
        fs::remove_file(&leaf).unwrap();
        fs::hard_link(&target, &leaf).unwrap();
        assert!(server_profile_dir(data.path(), "profile", false).is_err());
        fs::remove_file(&leaf).unwrap();
        fs::write(&leaf, "valid").unwrap();
        mode(&leaf, 0o666);
        assert!(server_profile_dir(data.path(), "profile", false).is_err());
        fs::remove_file(&leaf).unwrap();
        fs::create_dir(&leaf).unwrap();
        assert!(server_profile_dir(data.path(), "profile", false).is_err());
        assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
    }

    #[test]
    fn descriptor_stays_on_old_directory_but_replacement_path_is_refused() {
        let data = temp();
        let profile = create(data.path());
        let root = fs::File::open(profiles_root(data.path())).unwrap();
        let held = open_directory_at(&root, "profile".as_ref())
            .unwrap()
            .unwrap();
        let saved = profile.with_file_name("saved");
        fs::rename(&profile, &saved).unwrap(); // simulate a privileged migration
        symlink(temp().path(), &profile).unwrap();
        assert!(trusted_directory(&held, true, false).is_ok());
        assert!(server_profile_dir(data.path(), "profile", false).is_err());
        assert!(saved.is_dir());
    }
}
