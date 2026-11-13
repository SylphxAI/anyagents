//! `anyagents agent-run --config <dir> -- <command…>`: start one agent
//! instance inside the deployment's OS boundary.
//!
//! A server started with an operator token serves agents that do not share its
//! user, so this launcher is the only door into it. It starts the command (a
//! harness, or a shell for the agent's own bash) only when the boundary holds,
//! and otherwise prints the one admission it needs and exits non-zero. It never
//! falls back to running the agent in the server's own files, and never with the
//! server's own uid.
//!
//! The mechanism is a distinct managed uid, not a user namespace: this host's
//! `user.max_user_namespaces` may be zero, and a managed uid needs no such
//! admission. The launcher is the operator's `/usr/local/bin/anyagents-setpriv`,
//! which starts
//! `setpriv --reuid <uid> --regid <gid> --groups <gid>,<socket group> --no-new-privs --`.
//!
//! A successful `/bin/true` proves nothing on its own. Before a command starts,
//! this launcher reads what the new process actually is: its effective uid is
//! the configured one and not the server's, `NoNewPrivs` is 1, the server's
//! database, secrets, socket and operator token and this agent's peers' config
//! directories are denied, and its own config directory is writable by it and
//! its capability readable by no other seat.

use std::path::{Path, PathBuf};

use anyagents_core::Error;
use anyagents_core::boundary::{self, Boundary};

/// The privileged launcher an operator installs for this host. A
/// `ANYAGENTS_LAUNCHER` names another one for a test or a pilot.
const SETPRIV: &str = "/usr/local/bin/anyagents-setpriv";

/// The launcher this deployment starts agents with.
pub(crate) fn launcher() -> PathBuf {
    for name in ["ANYAGENTS_LAUNCHER", "ANYAGENTS_SETPRIV"] {
        if let Some(path) = std::env::var_os(name)
            && !path.is_empty()
        {
            return PathBuf::from(path);
        }
    }
    PathBuf::from(SETPRIV)
}

/// Read the boundary contract of `data`, or fail with what is missing.
pub fn boundary_of(data: &Path) -> Result<Boundary, Error> {
    let path = data.join(boundary::BOUNDARY_FILE);
    let text = std::fs::read_to_string(&path).map_err(|err| {
        Error::Message(format!(
            "no identity boundary at {}: {err}; this deployment runs on the single-user route",
            path.display()
        ))
    })?;
    serde_json::from_str(&text).map_err(|err| Error::Message(err.to_string()))
}

fn number(name: &str) -> Option<u32> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
}

/// The uid the server itself runs as.
pub fn server_uid() -> u32 {
    current_uid()
}

/// The gid the server itself runs as.
pub fn server_gid() -> u32 {
    // SAFETY: getgid takes no arguments and cannot fail.
    unsafe { libc::getgid() }
}

fn current_uid() -> u32 {
    // SAFETY: getuid takes no arguments and cannot fail.
    unsafe { libc::getuid() }
}

/// The agent's own numeric identity and the socket's group, from the operator's
/// configuration. An agent's environment never decides them.
pub fn configured_seat() -> Option<(u32, u32, u32)> {
    Some((
        number("ANYAGENTS_AGENT_UID")?,
        number("ANYAGENTS_AGENT_GID")?,
        number("ANYAGENTS_SOCKET_GID")?,
    ))
}

/// Whether the process is running inside a unit or integration test harness
/// that must never read the host user's personal home directory or host seats.
pub fn is_isolated_test_env() -> bool {
    isolated_test_env(cfg!(test))
}

fn isolated_test_env(test_build: bool) -> bool {
    test_build || std::env::var_os("ANYAGENTS_TESTS").is_some()
}

/// The server user's home directory on the host, resolved from
/// `ANYAGENTS_HOST_HOME`, the OS user database (`getpwuid`), or `HOME`.
/// Returns `None` in isolated test environments so tests never read the
/// developer's personal home directory.
pub fn host_home_dir() -> Option<PathBuf> {
    if is_isolated_test_env() {
        return None;
    }
    if let Some(explicit) = std::env::var_os("ANYAGENTS_HOST_HOME")
        && !explicit.is_empty()
    {
        let path = PathBuf::from(explicit);
        if path.is_dir() {
            return Some(path);
        }
    }
    #[cfg(unix)]
    unsafe {
        let pwd = libc::getpwuid(libc::getuid());
        if !pwd.is_null() && !(*pwd).pw_dir.is_null() {
            let dir = std::ffi::CStr::from_ptr((*pwd).pw_dir).to_string_lossy();
            if !dir.is_empty() {
                let path = PathBuf::from(dir.into_owned());
                if path.is_dir() {
                    return Some(path);
                }
            }
        }
    }
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

/// Whether a single-agent seat override (`ANYAGENTS_AGENT_UID` or
/// `ANYAGENTS_AGENT_GID`) is configured. `ANYAGENTS_SOCKET_GID` may be set on
/// its own alongside host `seat-<name>` accounts.
pub fn seat_named() -> bool {
    ["ANYAGENTS_AGENT_UID", "ANYAGENTS_AGENT_GID"]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
}

/// Whether multi-UID OS seats are configured for this deployment (either via
/// explicit `ANYAGENTS_*` environment variables). Host account names alone
/// do not select an execution mode or admit a principal.
pub fn seats_configured() -> bool {
    // Presence is selection: an empty or malformed map must not downgrade to
    // single-UID execution or fall back to another principal source.
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_some()
        || std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY").is_some_and(|path| {
            !matches!(std::fs::symlink_metadata(path), Err(err) if err.kind() == std::io::ErrorKind::NotFound)
        })
    {
        return true;
    }
    if [
        "ANYAGENTS_PRINCIPALS",
        "ANYAGENTS_AGENTS",
        "ANYAGENTS_AGENT_UID",
        "ANYAGENTS_AGENT_GID",
        "ANYAGENTS_SOCKET_GID",
    ]
    .iter()
    .any(|name| std::env::var_os(name).is_some_and(|v| !v.is_empty()))
    {
        return true;
    }
    false
}

/// Whether every harness process this server starts satisfies the deployment's
/// launch boundary. When no OS seats are configured (`local`, `docker`, or `k8s`
/// single-UID mode), capability authentication holds without requiring
/// `anyagents-setpriv`. When OS seats are configured, the launcher must be
/// installed and every pinned uid must be distinct from root and the server uid.
pub fn launch_boundary(agents: &[String]) -> bool {
    if !seats_configured() {
        return true;
    }
    if !launcher().is_file() {
        return false;
    }
    agents.iter().all(|agent| {
        seat_for_agent(agent).is_some_and(|(_, uid, _, _)| uid != 0 && uid != current_uid())
    })
}

/// The socket group: the configured one, else the host's `anyagents-sock`
/// when running outside isolated tests.
pub(crate) fn sock_gid_for() -> Option<u32> {
    number("ANYAGENTS_SOCKET_GID").or_else(|| {
        (!is_isolated_test_env())
            .then(|| lookup_group("anyagents-sock"))
            .flatten()
    })
}

fn lookup_group(name: &str) -> Option<u32> {
    let c_name = std::ffi::CString::new(name).ok()?;
    // SAFETY: `c_name` is NUL-terminated; the field is copied at once.
    unsafe {
        let entry = libc::getgrnam(c_name.as_ptr());
        if entry.is_null() {
            None
        } else {
            Some((*entry).gr_gid)
        }
    }
}

mod principals;
#[cfg(test)]
use principals::read_principal_registry;
pub(crate) use principals::{bind_principal, load_principal_registry, reconcile_principals};

/// The seat one agent runs in: `(seat user, uid, gid, socket gid)`.
///
/// Only the operator-owned ANYAGENTS_PRINCIPALS map keyed by immutable agent
/// identity admits a principal. Names and filesystem ownership cannot admit one.
#[allow(dead_code)]
pub fn seat_for_agent(agent_name: &str) -> Option<(String, u32, u32, u32)> {
    seat_for_agent_with_hint(agent_name, None)
}

pub fn seat_for_agent_with_hint(
    agent_name: &str,
    parent_name: Option<&str>,
) -> Option<(String, u32, u32, u32)> {
    let _ = parent_name; // Display/ancestry metadata is never execution authority.
    if let Some(raw) = std::env::var_os("ANYAGENTS_PRINCIPALS") {
        return admitted_principal(raw.to_str()?, agent_name);
    }
    let path = std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY")?;
    principals::read_admitted_principal(Path::new(&path).parent()?, agent_name)
        .ok()
        .flatten()
}

/// Operator-owned map keyed by immutable agent ID, never inferred from names or files.
pub(super) fn admitted_principal_entry(
    entry: &serde_json::Value,
) -> Option<(String, u32, u32, u32)> {
    let number = |key| entry.get(key)?.as_u64().and_then(|n| u32::try_from(n).ok());
    let uid = number("uid")?;
    if uid == 0 || uid == current_uid() {
        return None;
    }
    Some((
        entry
            .get("user")?
            .as_str()
            .filter(|user| !user.trim().is_empty())?
            .to_string(),
        uid,
        number("gid")?,
        number("socket_gid")?,
    ))
}

/// Resolve immutable-ID seat labels once for a roster response. Parent names
/// are display metadata and never confer execution authority.
pub(crate) fn roster_seats() -> std::collections::HashMap<String, (String, u32, u32, u32)> {
    if let Ok(raw) = std::env::var("ANYAGENTS_PRINCIPALS") {
        return serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&raw)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(id, entry)| admitted_principal_entry(&entry).map(|seat| (id, seat)))
            .collect();
    }
    std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY")
        .and_then(|path| principals::read_admitted_principals(Path::new(&path).parent()?).ok())
        .unwrap_or_default()
}

fn admitted_principal(raw: &str, agent_id: &str) -> Option<(String, u32, u32, u32)> {
    let values: serde_json::Value = serde_json::from_str(raw).ok()?;
    admitted_principal_entry(values.get(agent_id)?)
}

fn server_user_in_group_db(target_gid: u32) -> bool {
    let me = current_uid();
    let Some(my_name) = lookup_user_name(me) else {
        return false;
    };
    let Ok(text) = std::fs::read_to_string("/etc/group") else {
        return false;
    };
    for line in text.lines() {
        let mut parts = line.split(':');
        let (Some(_group_name), Some(_pw), Some(gid_s), Some(members)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if gid_s.trim().parse::<u32>().ok() == Some(target_gid) {
            return members.split(',').any(|m| m.trim() == my_name);
        }
    }
    false
}

fn lookup_user_name(uid: u32) -> Option<String> {
    // SAFETY: `getpwuid` takes a numeric uid and returns a static pointer or null.
    unsafe {
        let entry = libc::getpwuid(uid);
        if entry.is_null() || (*entry).pw_name.is_null() {
            None
        } else {
            Some(
                std::ffi::CStr::from_ptr((*entry).pw_name)
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    }
}

fn chown_to_seat_group(path: &Path, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    if let Ok(meta) = std::fs::symlink_metadata(path)
        && meta.gid() == gid
    {
        return Ok(());
    }
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| std::io::Error::other("seat path"))?;
    // SAFETY: `c_path` is NUL-terminated; `u32::MAX` keeps the owner.
    if unsafe { libc::chown(c_path.as_ptr(), u32::MAX, gid) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EPERM)
        && !is_isolated_test_env()
        && server_user_in_group_db(gid)
        && launcher().is_file()
    {
        let (f_uid, f_gid) = std::fs::symlink_metadata(path)
            .map(|m| (m.uid(), m.gid()))
            .unwrap_or_else(|_| (current_uid(), server_gid()));
        let uid_s = f_uid.to_string();
        let gid_s = f_gid.to_string();
        let target_gid = gid.to_string();
        let groups_s = if f_gid != gid {
            format!("{f_gid},{gid}")
        } else {
            target_gid.clone()
        };
        if let Ok(status) = std::process::Command::new(launcher())
            .args([
                "--reuid",
                &uid_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_s,
                "--no-new-privs",
                "--",
                "chgrp",
                "-h",
                &target_gid,
            ])
            .arg(path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            && status.success()
        {
            return Ok(());
        }
    }
    Err(err)
}

/// The launch spec a harness adapter carries for this agent:
/// `<launcher>\t<uid>\t<gid>\t<groups>`.
#[allow(dead_code)]
pub fn seat_launch_spec(agent_name: &str) -> Option<(String, String)> {
    seat_launch_spec_with_hint(agent_name, None)
}

pub fn seat_launch_spec_with_hint(
    agent_name: &str,
    parent_name: Option<&str>,
) -> Option<(String, String)> {
    let (user, uid, gid, sock_gid) = seat_for_agent_with_hint(agent_name, parent_name)?;
    if !launcher().is_file() {
        return None;
    }
    Some((
        user,
        format!("{}\t{uid}\t{gid}\t{gid},{sock_gid}", launcher().display()),
    ))
}

/// Historical directory ownership is not an admission record. Kept as a
/// compatibility query that deliberately grants no execution authority.
pub fn existing_config_seat(config_dir: &Path) -> Option<(String, u32, u32, Option<String>)> {
    let _ = config_dir;
    // Historical ownership and mutable vendor-file owners cannot admit a principal.
    None
}

/// Give a minted capability to its seat's group: 0640, owned by the server's
/// user, readable by that one seat. The mode and group are read back; a file
/// any other seat could read is a refusal.
pub fn share_capability_with_seat(path: &Path, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if let Err(err) = chown_to_seat_group(path, gid) {
        // The server may hand the file to a group it already belongs to and to
        // no other; anything else is a refusal, said as itself.
        if err.raw_os_error() != Some(libc::EPERM) {
            return Err(err);
        }
        return Err(std::io::Error::other(format!(
            "the server cannot give {} to group {gid}: {err}",
            path.display()
        )));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o640))?;
    let meta = std::fs::metadata(path)?;
    if meta.permissions().mode() & 0o777 != 0o640 || meta.gid() != gid {
        return Err(std::io::Error::other(format!(
            "{} is {:o} in group {}",
            path.display(),
            meta.permissions().mode() & 0o777,
            meta.gid()
        )));
    }
    Ok(())
}

/// Restore execute/traverse bits on `config_dir.parent()` (`<data>/agents/<id>`)
/// if owned by the server user and blocking seat traversal (e.g. `0o700`/`0o2700`),
/// preserving any existing setgid bit.
#[cfg(unix)]
fn ensure_agent_root_traversable(config_dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
    let Some(root) = config_dir.parent() else {
        return Ok(());
    };
    let meta = match std::fs::symlink_metadata(root) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if !meta.is_dir() || meta.file_type().is_symlink() || meta.uid() != server_uid() {
        return Ok(());
    }
    if (meta.mode() & 0o011) != 0 {
        return Ok(());
    }
    let dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(root)?;
    let opened_meta = dir.metadata()?;
    if opened_meta.uid() != server_uid() {
        return Ok(());
    }
    let opened_mode = opened_meta.mode() & 0o7777;
    if (opened_mode & 0o011) == 0 {
        dir.set_permissions(std::fs::Permissions::from_mode(
            (opened_mode | 0o755) & 0o7777,
        ))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_agent_root_traversable(_config_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Give one agent's config directory to its explicitly admitted principal.
/// Workdirs have separate fail-closed sharing before admission.
pub fn share_config_with_seat_uid(config_dir: &Path, uid: u32, gid: u32) {
    if let Err(err) = ensure_agent_root_traversable(config_dir) {
        eprintln!(
            "seat: cannot restore traverse permissions on parent of {}: {err}",
            config_dir.display()
        );
    }
    let (eff_uid, eff_gid) = match existing_config_seat(config_dir) {
        Some((_, existing_uid, existing_gid, _)) => (existing_uid, existing_gid),
        None => (uid, gid),
    };
    if config_dir.is_dir()
        && let Err(err) = crate::seat_tree::share_config(config_dir, eff_uid, eff_gid)
    {
        eprintln!("seat: cannot share {}: {err}", config_dir.display());
    }
    // Learned facts and agent-authored skills are the only writable host
    // authority outside config/work. Session pins, raw history and migration
    // receipts remain owned by the daemon. Never follow an overlay symlink.
    for name in ["memory", "skills"] {
        let path = config_dir.with_file_name(name);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                eprintln!(
                    "seat: refusing non-directory host overlay {}",
                    path.display()
                );
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if ensure_seat_dir_as_owner(&path, eff_uid, eff_gid, 0o2770).is_err() {
                    continue;
                }
            }
            Err(_) => continue,
            _ => (),
        }
        if let Err(error) = give_tree(&path, eff_uid, eff_gid) {
            eprintln!("seat: cannot share {}: {error}", path.display());
        }
    }
    // Sessions holds the harness session database and must stay admitted
    // to the seat principal (0o2770 + seat group) exactly like config, or
    // seat-side resume fails with Permission denied (os error 13).
    // Transcripts remain host-private under daemon ownership.
    let sessions_dir = config_dir.with_file_name("sessions");
    let _ = std::fs::create_dir_all(&sessions_dir);
    if sessions_dir.is_dir()
        && let Err(err) = crate::seat_tree::share_config(&sessions_dir, eff_uid, eff_gid)
    {
        eprintln!("seat: cannot share {}: {err}", sessions_dir.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let transcripts_dir = config_dir.with_file_name("transcripts");
        if transcripts_dir.is_dir() {
            let _ =
                std::fs::set_permissions(&transcripts_dir, std::fs::Permissions::from_mode(0o700));
        }
    }
}

pub(crate) fn ensure_seat_dir_as_owner(
    path: &Path,
    uid: u32,
    gid: u32,
    mode: u32,
) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    // Materialise with the final mode in one step. A bare `create_dir_all`
    // under a setgid parent yields umask defaults (2755 seen live), never
    // the caller's mode; a returned success must not depend on a later
    // repair pass.
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(mode).recursive(true);
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() != std::io::ErrorKind::PermissionDenied => {
            return Err(error);
        }
        Err(_) => {}
    }
    if let Ok(meta) = std::fs::symlink_metadata(path)
        && meta.is_dir()
        && !meta.file_type().is_symlink()
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let owner = path
            .parent()
            .into_iter()
            .flat_map(Path::ancestors)
            .find_map(|p| std::fs::symlink_metadata(p).ok().filter(|m| m.is_dir()));
        let (eff_uid, eff_gid) = owner.map(|m| (m.uid(), m.gid())).unwrap_or((uid, gid));
        if eff_uid != server_uid()
            && let Some(launcher) = descriptor_launcher()
        {
            let groups = match sock_gid_for() {
                Some(sg) if sg != eff_gid && sg != 0 => format!("{eff_gid},{sg}"),
                _ => eff_gid.to_string(),
            };
            let mode_str = format!("{mode:04o}");
            let status = std::process::Command::new(launcher)
                .args([
                    "--reuid",
                    &eff_uid.to_string(),
                    "--regid",
                    &eff_gid.to_string(),
                    "--groups",
                    &groups,
                    "--no-new-privs",
                    "--",
                    "/bin/sh",
                    "-c",
                    r#"if [ -L "$1" ]; then exit 1; fi; mkdir -p -m "$2" "$1" && chmod "$2" "$1""#,
                    "seat-mkdir-owner",
                ])
                .arg(path.as_os_str())
                .arg(&mode_str)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()?;
            if status.success() {
                return Ok(());
            }
        }
    }
    Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
}

pub(crate) fn prepare_config_with_seat_uid(config: &Path, uid: u32, gid: u32) -> Result<(), Error> {
    ensure_agent_root_traversable(config).map_err(|error| Error::WorkdirPermission {
        path: config.parent().unwrap_or(config).display().to_string(),
        message: format!("cannot restore traverse permissions on agent root: {error}"),
    })?;
    crate::seat_tree::share_config(config, uid, gid).map_err(|error| Error::WorkdirPermission {
        path: config.display().to_string(),
        message: format!("cannot prepare shared harness configuration: {error}"),
    })
}

/// Share a working directory and both per-worktree and common Git metadata.
/// A protected shared root or failed permission change is a typed refusal.
pub fn share_worktree_with_seat(
    path: &Path,
    uid: u32,
    gid: u32,
    journal: &Path,
) -> Result<(), Error> {
    let canon = std::fs::canonicalize(path).map_err(|error| Error::WorkdirPermission {
        path: path.display().to_string(),
        message: error.to_string(),
    })?;
    let depth = canon
        .components()
        .filter(|c| matches!(c, std::path::Component::Normal(_)))
        .count();
    if depth < 3
        || host_home_dir().is_some_and(|h| canon == h || canon == h.join("workspace"))
        || protected_roots()
            .iter()
            .any(|root| canon == *root || canon.starts_with(root))
    {
        return Err(Error::WorkdirPermission {
            path: path.display().to_string(),
            message: "refusing recursive permission changes to a shared root".into(),
        });
    }
    crate::worktree_access::share(path, uid, gid, journal)
}

fn protected_roots() -> Vec<PathBuf> {
    std::env::var_os("ANYAGENTS_PROTECTED_ROOTS")
        .map(|raw| {
            std::env::split_paths(&raw)
                // Match the canonical target against the canonical protected
                // tree, including macOS /var and /tmp aliases. Missing roots
                // retain their declared path until they can be resolved.
                .map(|root| std::fs::canonicalize(&root).unwrap_or(root))
                .collect()
        })
        .unwrap_or_default()
}

/// Share a Unix socket (`anyagents.sock` or `wake.sock`) with the deployment's
/// `anyagents-sock` group (`0660`) when running in a multi-seat boundary so
/// `seat-<agent>` processes can connect `anyagents mcp` and send wake datagrams.
pub fn share_socket_with_seats(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    if cfg!(test)
        || std::env::var_os("ANYAGENTS_TESTS").is_some()
        || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
    {
        return;
    }
    let Some(gid) = sock_gid_for() else {
        return;
    };
    if gid == server_gid() {
        return;
    }
    if chown_to_seat_group(path, gid).is_ok() {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660));
    }
}

/// Ensure an operator CLI or server invocation running in a pre-existing login
/// shell inherits every supplementary `seat-*` / `anyagents-sock` group granted
/// to the operator user in `/etc/group`.
fn operator_may_hydrate(uid: u32, admitted: Option<u32>) -> bool {
    uid != 0 && admitted == Some(uid)
}

pub fn hydrate_operator_groups_if_needed() {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        if is_isolated_test_env()
            || std::env::var_os("ANYAGENTS_GROUPS_HYDRATED").is_some()
            || !launcher().is_file()
        {
            return;
        }
        let me = current_uid();
        if !operator_may_hydrate(
            me,
            std::env::var("ANYAGENTS_OPERATOR_UID")
                .ok()
                .and_then(|v| v.parse().ok()),
        ) {
            return;
        }
        let Some(my_name) = lookup_user_name(me) else {
            return;
        };
        let Ok(group_text) = std::fs::read_to_string("/etc/group") else {
            return;
        };
        let my_gid = server_gid();
        let mut expected_gids = vec![my_gid];
        for line in group_text.lines() {
            let mut parts = line.split(':');
            let (Some(_gname), Some(_pw), Some(gid_s), Some(members)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let Ok(gid) = gid_s.trim().parse::<u32>() else {
                continue;
            };
            if members.split(',').any(|m| m.trim() == my_name) && !expected_gids.contains(&gid) {
                expected_gids.push(gid);
            }
        }
        if expected_gids.len() <= 1 {
            return;
        }
        let active_gids = unsafe {
            let count = libc::getgroups(0, std::ptr::null_mut());
            if count <= 0 {
                Vec::new()
            } else {
                let mut buf = vec![0 as libc::gid_t; count as usize];
                let got = libc::getgroups(count, buf.as_mut_ptr());
                if got > 0 {
                    buf.truncate(got as usize);
                    buf
                } else {
                    Vec::new()
                }
            }
        };
        if expected_gids.iter().all(|g| active_gids.contains(g)) {
            return;
        }
        let Ok(exe) = crate::data::current_exe_clean() else {
            return;
        };
        let groups_csv = expected_gids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let me_s = me.to_string();
        let gid_s = my_gid.to_string();
        let mut cmd = std::process::Command::new(launcher());
        cmd.env("ANYAGENTS_GROUPS_HYDRATED", "1")
            .args([
                "--reuid",
                &me_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_csv,
                "--",
            ])
            .arg(exe)
            .args(std::env::args_os().skip(1));
        let _ = cmd.exec();
    }
}

fn is_peer_boundary_shared(config: &Path, gid: u32) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    if let Some(root) = config.parent()
        && let Ok(m) = std::fs::symlink_metadata(root)
        && m.is_dir()
        && !m.file_type().is_symlink()
        && m.uid() == server_uid()
        && (m.mode() & 0o011) == 0
    {
        return false;
    }
    let check_dir = |p: &Path| -> bool {
        std::fs::symlink_metadata(p).is_ok_and(|m| {
            m.is_dir() && !m.file_type().is_symlink() && m.gid() == gid && (m.mode() & 0o007) == 0
        })
    };
    if !check_dir(config)
        || !check_dir(&config.with_file_name("memory"))
        || !check_dir(&config.with_file_name("skills"))
    {
        return false;
    }
    let cap = config.join("mcp-capability");
    if let Ok(m) = std::fs::symlink_metadata(&cap)
        && (m.gid() != gid || (m.mode() & 0o007) != 0)
    {
        return false;
    }
    true
}

type CachedPeers = (std::time::Instant, PathBuf, u64, String);

fn peers_cache() -> &'static std::sync::Mutex<Option<CachedPeers>> {
    static PEERS_CACHE: std::sync::OnceLock<std::sync::Mutex<Option<CachedPeers>>> =
        std::sync::OnceLock::new();
    PEERS_CACHE.get_or_init(|| std::sync::Mutex::new(None))
}

pub(super) fn invalidate_peers_cache() {
    if let Ok(mut guard) = peers_cache().lock() {
        *guard = None;
    }
}

fn seat_assignment_digest(store: &anyagents_core::Store, agents: &[anyagents_core::Agent]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut seats: Vec<(String, u32, u32)> = agents
        .iter()
        .filter_map(|agent| {
            crate::create::resolved_agent_seat_info(store, agent)
                .map(|(_, uid, gid, _)| (agent.id.as_str().to_string(), uid, gid))
        })
        .collect();
    seats.sort();
    let mut hasher = DefaultHasher::new();
    for (id, uid, gid) in seats {
        id.hash(&mut hasher);
        uid.hash(&mut hasher);
        gid.hash(&mut hasher);
    }
    hasher.finish()
}

/// Registered root-to-principal resolution is the same authority used by vendor
/// launch. Unregistered directories remain foreign in the shared inventory.
pub(crate) fn peer_principals(store: &anyagents_core::Store) -> Result<String, Error> {
    let agents = store.list_agents()?;
    let cacheable = !is_isolated_test_env() && std::env::var_os("ANYAGENTS_PRINCIPALS").is_none();
    let digest = cacheable.then(|| seat_assignment_digest(store, &agents));
    if cacheable
        && let Ok(guard) = peers_cache().lock()
        && let Some((at, ref dir, cached, ref json)) = *guard
        && dir == store.data_dir()
        && Some(cached) == digest
        && at.elapsed() < std::time::Duration::from_secs(15)
    {
        return Ok(json.clone());
    }

    let mut peers = Vec::new();
    for agent in agents.iter() {
        if let Some((_, uid, gid, _)) = crate::create::resolved_agent_seat_info(store, agent) {
            let config = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
            let root = config.parent().unwrap().to_owned();
            if !root.exists() {
                continue;
            }
            // The immutable-ID grant, not historical ownership, authorizes this
            // projection. Reconcile created peers before another seat probes them.
            if config.is_dir()
                && existing_config_seat(&config).is_none()
                && !is_peer_boundary_shared(&config, gid)
            {
                share_config_with_seat_uid(&config, uid, gid);
            }
            peers.push(anyagents_harness::SeatPeer { root, uid, gid });
        }
    }
    let json = serde_json::to_string(&peers).map_err(|e| Error::Message(e.to_string()))?;
    if let Some(digest) = digest
        && let Ok(mut guard) = peers_cache().lock()
    {
        *guard = Some((
            std::time::Instant::now(),
            store.data_dir().to_path_buf(),
            digest,
            json.clone(),
        ));
    }
    Ok(json)
}

pub(super) fn descriptor_launcher() -> Option<PathBuf> {
    let path = launcher();
    path.is_file().then_some(path)
}

fn give_tree(dir: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    crate::seat_tree::share(dir, uid, gid)
}

/// Prepare per-agent disposable build and temporary directories on the scratch
/// tier and grant read/write ownership to the agent's OS seat.
pub(crate) fn prepare_scratch_dirs_for_seat(
    scratch_root: &Path,
    agent_id: &anyagents_core::AgentId,
    uid: u32,
    gid: u32,
) -> std::io::Result<()> {
    let build_root = anyagents_core::agent_scratch_build_dir(scratch_root, agent_id);
    let tmp_root = anyagents_core::agent_scratch_tmp_dir(scratch_root, agent_id);
    for dir in [
        build_root.clone(),
        build_root.join("cargo"),
        build_root.join("gradle"),
        build_root.join("derived-data"),
        tmp_root.clone(),
    ] {
        ensure_seat_dir_as_owner(&dir, uid, gid, 0o2770)?;
    }
    give_tree(&build_root, uid, gid)?;
    give_tree(&tmp_root, uid, gid)?;
    Ok(())
}

/// Start one admitted agent command, or say what admission is missing.
pub fn run(data: &Path, config: Option<&Path>, command: &[String]) -> Result<(), Error> {
    if command.is_empty() {
        return Err(Error::Message("agent-run needs a command".into()));
    }
    let boundary = boundary_of(data)?;
    let Some((uid, gid, sock_gid)) = configured_seat() else {
        return Err(Error::Message(unadmitted(
            &boundary,
            "ANYAGENTS_AGENT_UID, ANYAGENTS_AGENT_GID and ANYAGENTS_SOCKET_GID are not all set",
        )));
    };
    if uid == 0 || uid == current_uid() {
        return Err(Error::Message(unadmitted(
            &boundary,
            "the agent uid is root or the server's own uid",
        )));
    }
    if !launcher().is_file() {
        return Err(Error::Message(unadmitted(
            &boundary,
            "the privileged launcher is not installed",
        )));
    }
    if let Some(config) = config
        && !config.join(crate::operator::CAPABILITY_FILE).is_file()
    {
        return Err(Error::Message(format!(
            "{} holds no mailbox capability",
            config.display()
        )));
    }
    let mut environment = vec![
        (
            anyagents_harness::SEAT_LAUNCH_ENV.to_owned(),
            format!("{}\t{uid}\t{gid}\t{gid},{sock_gid}", launcher().display()),
        ),
        ("ANYAGENTS_DATA".to_owned(), boundary.data_dir.clone()),
    ];
    if let Some(config) = config {
        environment.push((
            "ANYAGENTS_AGENT_CONFIG".to_owned(),
            config.display().to_string(),
        ));
    }
    // A seat launch without the peer projection is refused, never
    // downgraded: with seats configured, an unopenable store is a
    // named failure. Single-UID deployments stay on the quiet path.
    match anyagents_core::Store::open(data.join("state.db")) {
        Ok(store) => {
            environment.push((
                anyagents_harness::SEAT_PEERS_ENV.to_owned(),
                peer_principals(&store)?,
            ));
        }
        Err(error) if seats_configured() => {
            return Err(Error::Message(format!(
                "cannot open state.db {}: {error}",
                data.join("state.db").display()
            )));
        }
        Err(_) => {}
    }
    let status = anyagents_harness::spawn_command(Path::new(&command[0]), &environment)
        .map_err(|e| Error::Message(e.to_string()))?
        .args(&command[1..])
        .status()
        .map_err(Error::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Message(format!("the agent command exited {status}")))
    }
}

/// The one admission an operator must grant, with the exact command. A managed
/// uid needs no user namespace, so nothing here names a kernel knob.
fn unadmitted(boundary: &Boundary, why: &str) -> String {
    format!(
        "agent not started: {why}; the boundary needs one managed uid per agent \
         (ANYAGENTS_AGENT_UID and ANYAGENTS_AGENT_GID, in group ANYAGENTS_SOCKET_GID for the API \
         socket only) and the privileged launcher {SETPRIV} on {}, which starts \
         `setpriv --reuid <uid> --regid <gid> --groups <gid>,<socket group> --no-new-privs --`",
        boundary.data_dir
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sessions created by the daemon as a private 0700 directory (as a harness
    /// switch or an older daemon leaves them) are re-admitted to the agent's
    /// seat group with 0o2770 by the same sharing pass that admits config.
    #[cfg(target_os = "linux")]
    #[test]
    fn config_sharing_readmits_a_private_sessions_directory_to_the_seat() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("agent");
        let config = root.join("config");
        let sessions = root.join("sessions");
        let transcripts = root.join("transcripts");
        for path in [&config, &sessions, &transcripts] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(sessions.join("session.json"), b"{}").unwrap();
        std::fs::set_permissions(
            sessions.join("session.json"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let (uid, gid) = (server_uid(), server_gid());
        share_config_with_seat_uid(&config, uid, gid);
        let meta = std::fs::metadata(&sessions).unwrap();
        assert_eq!(
            meta.mode() & 0o7777,
            0o2770,
            "sessions must be seat-admitted"
        );
        assert_eq!(meta.gid(), gid);
        let file = std::fs::metadata(sessions.join("session.json")).unwrap();
        assert_eq!(file.gid(), gid);
        assert_ne!(
            file.mode() & 0o060,
            0,
            "the seat group must reach session files"
        );
        assert_eq!(
            std::fs::metadata(&transcripts).unwrap().mode() & 0o7777,
            0o700,
            "transcripts stay host-private"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn prepare_and_share_config_restore_agent_root_traversal_preserving_setgid() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let (uid, gid) = (server_uid(), server_gid());

        for initial_root_mode in [0o700u32, 0o2700u32] {
            let root = dir.path().join(format!("agent-{initial_root_mode:o}"));
            let config = root.join("config");
            let memory = root.join("memory");
            let skills = root.join("skills");
            let transcripts = root.join("transcripts");
            for sub in [&config, &memory, &skills, &transcripts] {
                std::fs::create_dir_all(sub).unwrap();
            }
            for shared in [&memory, &skills] {
                std::fs::set_permissions(shared, std::fs::Permissions::from_mode(0o2770)).unwrap();
            }
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&transcripts, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(initial_root_mode))
                .unwrap();

            prepare_config_with_seat_uid(&config, uid, gid).unwrap();
            let expected_root = (initial_root_mode | 0o755) & 0o7777;
            assert_eq!(
                std::fs::metadata(&root).unwrap().mode() & 0o7777,
                expected_root,
                "prepare_config_with_seat_uid must restore traverse bits from {initial_root_mode:o}"
            );
            assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
            assert_eq!(std::fs::metadata(&config).unwrap().gid(), gid);

            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(initial_root_mode))
                .unwrap();
            assert!(!is_peer_boundary_shared(&config, gid));
            share_config_with_seat_uid(&config, uid, gid);
            assert_eq!(
                std::fs::metadata(&root).unwrap().mode() & 0o7777,
                expected_root,
                "share_config_with_seat_uid must restore traverse bits from {initial_root_mode:o}"
            );
            assert!(is_peer_boundary_shared(&config, gid));
            assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
            assert_eq!(
                std::fs::metadata(&transcripts).unwrap().mode() & 0o7777,
                0o700
            );
        }

        // Already-traversable agent roots (e.g. 0o711) and symlinks must not be widened.
        let traversable = dir.path().join("agent-711");
        let trav_cfg = traversable.join("config");
        std::fs::create_dir_all(&trav_cfg).unwrap();
        std::fs::set_permissions(&traversable, std::fs::Permissions::from_mode(0o711)).unwrap();
        prepare_config_with_seat_uid(&trav_cfg, uid, gid).unwrap();
        assert_eq!(
            std::fs::metadata(&traversable).unwrap().mode() & 0o7777,
            0o711
        );

        let real_target = dir.path().join("real-private");
        std::fs::create_dir_all(real_target.join("config")).unwrap();
        std::fs::set_permissions(&real_target, std::fs::Permissions::from_mode(0o700)).unwrap();
        let symlink_root = dir.path().join("symlink-root");
        std::os::unix::fs::symlink(&real_target, &symlink_root).unwrap();
        assert!(prepare_config_with_seat_uid(&symlink_root.join("config"), uid, gid).is_err());
        share_config_with_seat_uid(&symlink_root.join("config"), uid, gid);
        assert_eq!(
            std::fs::metadata(&real_target).unwrap().mode() & 0o7777,
            0o700,
            "symlinked parent must not have its target widened"
        );
    }

    #[test]
    fn claude_executable_override_does_not_select_test_isolation() {
        const PROBE: &str = "ANYAGENTS_ISOLATION_REGRESSION_PROBE";
        if let Some(expected) = std::env::var_os(PROBE) {
            // Unit binaries are test builds; exercise the exact production
            // predicate with that compile-time input false, without host reads.
            assert_eq!(isolated_test_env(false), expected == "true");
            assert!(is_isolated_test_env());
            return;
        }
        for (override_bin, explicit_test) in [(false, false), (true, false), (true, true)] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .env_clear()
                .env(PROBE, explicit_test.to_string())
                .args([
                    "--exact",
                    "sandbox::tests::claude_executable_override_does_not_select_test_isolation",
                    "--nocapture",
                ]);
            if override_bin {
                child.env("ANYAGENTS_CLAUDE_CODE_BIN", "/opt/vendor/claude");
            }
            if explicit_test {
                child.env("ANYAGENTS_TESTS", "1");
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "override={override_bin}, explicit_test={explicit_test}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn admitted_principals_are_bound_to_immutable_ids_not_names_or_owners() {
        let raw = r#"{"agt_example":{"user":"worker","uid":42001,"gid":42002,"socket_gid":42003}}"#;
        assert_eq!(
            admitted_principal(raw, "agt_example"),
            Some(("worker".into(), 42001, 42002, 42003))
        );
        for alias in ["worker", "seat-worker", "worker-child", "unrelated"] {
            assert_eq!(admitted_principal(raw, alias), None);
        }
        for invalid in [
            "{}",
            "not-json",
            r#"{"agt_example":{"user":"worker","uid":0,"gid":1,"socket_gid":1}}"#,
            r#"{"agt_example":{"user":"worker","uid":42001,"gid":"42002","socket_gid":1}}"#,
        ] {
            assert!(admitted_principal(invalid, "agt_example").is_none());
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".anyagents-seat"), "worker").unwrap();
        std::fs::write(dir.path().join("installation_id"), "mutable vendor file").unwrap();
        assert!(existing_config_seat(dir.path()).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn peer_inventory_reconciles_created_configs_only_for_registered_principals() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let _env = crate::server::tests::env_lock();
        let previous = std::env::var_os("ANYAGENTS_PRINCIPALS");
        let dir = crate::test_tempdir();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let admitted = store
            .create_agent(anyagents_core::NewAgent::new("created-worker", "opencode"))
            .unwrap();
        let foreign = store
            .create_agent(anyagents_core::NewAgent::new(
                "unregistered-worker",
                "opencode",
            ))
            .unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &admitted.id);
        let foreign_config = anyagents_core::agent_config_dir(dir.path(), &foreign.id);
        for path in [&config, &foreign_config] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::write(path.join("sentinel"), "preserved").unwrap();
        }
        assert_eq!(admitted.state, "created");
        let uid = if server_uid() == 42001 { 42002 } else { 42001 };
        let gid = server_gid();
        unsafe {
            std::env::set_var("ANYAGENTS_PRINCIPALS", serde_json::json!({
            admitted.id.as_str(): {"user":"registered-worker", "uid":uid, "gid":gid, "socket_gid":gid}
        }).to_string());
        }
        let peers: Vec<anyagents_harness::SeatPeer> =
            serde_json::from_str(&peer_principals(&store).unwrap()).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].root, config.parent().unwrap());
        assert_eq!((peers[0].uid, peers[0].gid), (uid, gid));
        assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
        assert_eq!(
            std::fs::metadata(&foreign_config).unwrap().mode() & 0o777,
            0o755
        );
        assert_eq!(store.agent(&admitted.id).unwrap().state, "created");
        for path in [&config, &foreign_config] {
            assert_eq!(
                std::fs::read_to_string(path.join("sentinel")).unwrap(),
                "preserved"
            );
        }
        assert_eq!(
            peer_principals(&store).unwrap(),
            serde_json::to_string(&peers).unwrap()
        );
        unsafe {
            match previous {
                Some(value) => std::env::set_var("ANYAGENTS_PRINCIPALS", value),
                None => std::env::remove_var("ANYAGENTS_PRINCIPALS"),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn persisted_principal_registry_preserves_explicit_grants_without_name_inference() {
        use std::os::unix::fs::PermissionsExt;
        let _env = crate::server::tests::env_lock();
        let dir = crate::test_tempdir();
        assert!(read_principal_registry(dir.path()).unwrap().is_none());
        let id = anyagents_core::AgentId::generate();
        let uid = if current_uid() == 42001 { 42002 } else { 42001 };
        let raw = serde_json::json!({id.as_str(): {"user":"worker", "uid":uid, "gid":42003, "socket_gid":42004}}).to_string();
        let path = dir.path().join("execution-principals.json");
        std::fs::write(&path, &raw).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let previous = std::env::var_os("ANYAGENTS_PRINCIPALS");
        let previous_registry = std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY");
        unsafe {
            std::env::remove_var("ANYAGENTS_PRINCIPALS");
        }
        load_principal_registry(dir.path()).unwrap();
        assert_eq!(
            seat_for_agent(id.as_str()),
            Some(("worker".into(), uid, 42003, 42004))
        );
        assert!(seat_for_agent("worker").is_none());
        // An explicit map is authoritative even when the fallback is broken.
        std::fs::write(&path, "corrupted").unwrap();
        unsafe {
            std::env::set_var("ANYAGENTS_PRINCIPALS", "{}");
        }
        load_principal_registry(dir.path()).unwrap();
        assert!(seat_for_agent(id.as_str()).is_none());
        unsafe {
            std::env::remove_var("ANYAGENTS_PRINCIPALS");
        }
        assert!(read_principal_registry(dir.path()).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "corrupted");
        std::fs::write(&path, &raw).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(read_principal_registry(dir.path()).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, "[]").unwrap();
        assert!(read_principal_registry(dir.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(dir.path().join("missing"), &path).unwrap();
        assert!(read_principal_registry(dir.path()).is_err());
        unsafe {
            match previous_registry {
                Some(value) => std::env::set_var("ANYAGENTS_PRINCIPAL_REGISTRY", value),
                None => std::env::remove_var("ANYAGENTS_PRINCIPAL_REGISTRY"),
            }
            match previous {
                Some(value) => std::env::set_var("ANYAGENTS_PRINCIPALS", value),
                None => std::env::remove_var("ANYAGENTS_PRINCIPALS"),
            }
        }
    }

    #[test]
    fn unopenable_store_refuses_seat_launch_naming_state_db() {
        let dir = tempfile::tempdir().unwrap();
        boundary::write(dir.path()).unwrap();
        // A directory where the store file belongs: `Store::open` fails at
        // any uid (root or not), independent of DAC overrides.
        std::fs::create_dir(dir.path().join("state.db")).unwrap();
        let config = dir.path().join("config");
        std::fs::create_dir(&config).unwrap();
        std::fs::write(config.join(crate::operator::CAPABILITY_FILE), "cap\n").unwrap();
        let launcher = dir.path().join("launcher");
        std::fs::write(&launcher, "").unwrap();
        let uid = if current_uid() == 42001 { 42002 } else { 42001 };
        let _env = crate::server::tests::env_lock();
        let keys = [
            "ANYAGENTS_AGENT_UID",
            "ANYAGENTS_AGENT_GID",
            "ANYAGENTS_SOCKET_GID",
            "ANYAGENTS_LAUNCHER",
            "ANYAGENTS_PRINCIPALS",
        ];
        let old: Vec<_> = keys.iter().map(std::env::var_os).collect();
        unsafe {
            std::env::set_var("ANYAGENTS_AGENT_UID", uid.to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", "42003");
            std::env::set_var("ANYAGENTS_SOCKET_GID", "42004");
            std::env::set_var("ANYAGENTS_LAUNCHER", &launcher);
            std::env::set_var("ANYAGENTS_PRINCIPALS", "{}");
        }
        let message = run(
            dir.path(),
            Some(&config),
            &["/bin/echo".to_string(), "launched".to_string()],
        )
        .unwrap_err()
        .to_string();
        assert!(message.contains("state.db"), "{message}");
        assert!(!message.contains("the agent command exited"), "{message}");
        unsafe {
            for (key, value) in keys.into_iter().zip(old) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn an_unconfigured_seat_is_refused_before_anything_starts() {
        let dir = tempfile::tempdir().unwrap();
        boundary::write(dir.path()).unwrap();
        // SAFETY: under the server test lock, set and removed here.
        let _env = crate::server::tests::env_lock();
        unsafe {
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
        }
        let message = run(dir.path(), None, &["/bin/true".to_string()])
            .unwrap_err()
            .to_string();
        assert!(message.contains("agent not started"), "{message}");
        assert!(message.contains("ANYAGENTS_AGENT_UID"), "{message}");
        assert!(!message.contains("max_user_namespaces"), "{message}");
    }

    #[test]
    fn the_server_owner_is_never_an_agent_uid() {
        let dir = tempfile::tempdir().unwrap();
        boundary::write(dir.path()).unwrap();
        let _env = crate::server::tests::env_lock();
        // SAFETY: under the server test lock, set and removed here.
        unsafe {
            std::env::set_var("ANYAGENTS_AGENT_UID", current_uid().to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", "1");
            std::env::set_var("ANYAGENTS_SOCKET_GID", "1");
        }
        let message = run(dir.path(), None, &["/bin/true".to_string()])
            .unwrap_err()
            .to_string();
        assert!(message.contains("the server's own uid"), "{message}");
        // SAFETY: as above.
        unsafe {
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
        }
    }
    #[test]
    fn operator_hydration_requires_explicit_identity_not_a_uid_range() {
        for uid in [1000, 2100, 42001, u32::MAX] {
            assert!(operator_may_hydrate(uid, Some(uid)));
            assert!(!operator_may_hydrate(uid, None));
            assert!(!operator_may_hydrate(uid, Some(uid - 1)));
        }
        assert!(!operator_may_hydrate(0, Some(0)));
    }

    #[test]
    fn protected_roots_are_explicit_and_refuse_only_the_selected_tree() {
        let _env = crate::server::tests::env_lock();
        let key = "ANYAGENTS_PROTECTED_ROOTS";
        let old = std::env::var_os(key);
        let dir = tempfile::tempdir().unwrap();
        let selected = dir.path().join("selected");
        let allowed = dir.path().join("allowed");
        std::fs::create_dir(&selected).unwrap();
        std::fs::create_dir(&allowed).unwrap();
        unsafe {
            std::env::remove_var(key);
        }
        assert!(protected_roots().is_empty());
        unsafe {
            std::env::set_var(key, std::env::join_paths([&selected]).unwrap());
        }
        assert_eq!(protected_roots(), vec![selected.canonicalize().unwrap()]);
        let alias = dir.path().join("selected-alias");
        std::os::unix::fs::symlink(&selected, &alias).unwrap();
        // An explicit alias protects the same tree, not only its spelling.
        unsafe {
            std::env::set_var(key, std::env::join_paths([&alias]).unwrap());
        }
        assert_eq!(protected_roots(), vec![selected.canonicalize().unwrap()]);
        assert!(
            share_worktree_with_seat(
                &selected,
                server_uid(),
                server_gid(),
                &dir.path().join("refused.json")
            )
            .is_err()
        );
        let nested = selected.join("nested");
        std::fs::create_dir(&nested).unwrap();
        assert!(
            share_worktree_with_seat(
                &nested,
                server_uid(),
                server_gid(),
                &dir.path().join("nested-refused.json")
            )
            .is_err()
        );
        share_worktree_with_seat(
            &allowed,
            server_uid(),
            server_gid(),
            &dir.path().join("allowed.json"),
        )
        .unwrap();
        unsafe {
            match old {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}
