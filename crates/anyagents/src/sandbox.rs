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
    test_build
        || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || std::env::var_os("ANYAGENTS_TESTS").is_some()
        || std::env::var_os("ANYAGENTS_FAKE_LOG").is_some()
        || std::env::var_os("ANYAGENTS_HARNESS_BIN").is_some()
        || std::env::var_os("ANYAGENTS_CLAUDE_BIN").is_some()
        || std::env::var_os("ANYAGENTS_CODEX_BIN").is_some()
        || std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_some()
        || std::env::var_os("CARGO_TARGET_TMPDIR").is_some()
        || std::env::var_os("NEXTEST_RUN_ID").is_some()
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
/// explicit `ANYAGENTS_*` environment variables or provisioned host `seat-*`
/// accounts with `anyagents-sock` and `anyagents-setpriv` outside test harnesses).
pub fn seats_configured() -> bool {
    if [
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
    !is_isolated_test_env()
        && launcher().is_file()
        && lookup_group("anyagents-sock").is_some()
        && !host_seat_users().is_empty()
}

/// Whether every harness process this server starts satisfies the deployment's
/// launch boundary. When no OS seats are configured (`local`, `docker`, or `k8s`
/// single-UID mode), capability authentication holds without requiring
/// `anyagents-setpriv`. When OS seats are configured, the launcher must be
/// installed and every pinned uid must be distinct from root and the server uid.
pub fn launch_boundary(agents: &[String]) -> bool {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        // A test machine has no OS accounts of its own to place an agent in.
        return true;
    }
    if !seats_configured() {
        return true;
    }
    if !launcher().is_file() {
        return false;
    }
    agents.iter().all(|agent| match pinned_uid(agent) {
        Some(uid) => uid != 0 && Some(uid) != Some(current_uid()),
        None => true,
    })
}

/// The socket group: the configured one, else the host's `anyagents-sock`
/// when running outside isolated tests.
fn sock_gid_for() -> Option<u32> {
    number("ANYAGENTS_SOCKET_GID").or_else(|| {
        if is_isolated_test_env() {
            None
        } else {
            lookup_group("anyagents-sock")
        }
    })
}

/// The uid the deployment's own line gives one agent: `<agent>=<uid>` entries
/// of `ANYAGENTS_AGENTS`.
pub fn pinned_uid(agent_name: &str) -> Option<u32> {
    let line = std::env::var("ANYAGENTS_AGENTS").ok()?;
    line.split(',')
        .filter_map(|entry| entry.split_once('='))
        .find(|(name, _)| name.trim() == agent_name)
        .and_then(|(_, uid)| uid.trim().parse::<u32>().ok())
}

/// The seat user name a host provisions for one agent: `seat-<name>`.
fn seat_user(agent_name: &str) -> String {
    let lower = agent_name.to_ascii_lowercase();
    if lower.starts_with("seat-") {
        lower
    } else {
        format!("seat-{lower}")
    }
}

fn lookup_user(name: &str) -> Option<(u32, u32)> {
    let c_name = std::ffi::CString::new(name).ok()?;
    // SAFETY: `c_name` is NUL-terminated; the fields are copied at once.
    unsafe {
        let entry = libc::getpwnam(c_name.as_ptr());
        if entry.is_null() {
            None
        } else {
            Some(((*entry).pw_uid, (*entry).pw_gid))
        }
    }
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

/// The seat one agent runs in: `(seat user, uid, gid, socket gid)`.
///
/// A single-agent test deployment takes its configured uid. A deployment with
/// several agents reads the host's own account for each, `seat-<agent name>`,
/// and the `anyagents-sock` group every seat belongs to. A host account is
/// this machine's admission; it is never invented from a name an agent wrote.
#[allow(dead_code)]
pub fn seat_for_agent(agent_name: &str) -> Option<(String, u32, u32, u32)> {
    seat_for_agent_with_hint(agent_name, None)
}

pub fn seat_for_agent_with_hint(
    agent_name: &str,
    parent_name: Option<&str>,
) -> Option<(String, u32, u32, u32)> {
    let configured = configured_seat();
    let agents = std::env::var("ANYAGENTS_AGENTS").unwrap_or_default();
    let explicit = configured.is_some() && agents.trim() == agent_name;
    if let Some((uid, gid, sock_gid)) = configured
        && (explicit || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some())
    {
        return Some((agent_name.to_string(), uid, gid, sock_gid));
    }
    if pinned_uid(agent_name).is_some() && std::env::var_os("ANYAGENTS_TESTS").is_some() {
        // A test machine is its own single seat. Its primary group need not
        // have the same numeric ID as its user (for example, on macOS).
        return Some((
            agent_name.to_string(),
            current_uid(),
            server_gid(),
            server_gid(),
        ));
    }
    if let Some(uid) = pinned_uid(agent_name)
        && let Some(sock_gid) = sock_gid_for()
    {
        // The deployment's own line gives this agent its uid; the server gives
        // the capability to the group that uid shares, and no other.
        return Some((seat_user(agent_name), uid, uid, sock_gid));
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() || !seats_configured() {
        return None;
    }
    // The uid this deployment gives the agent, or the host's provisioned
    // `seat-<name>` account when `ANYAGENTS_AGENTS` is not explicitly set.
    let mut user = seat_user(agent_name);
    let (uid, gid) = match pinned_uid(agent_name) {
        Some(uid) => (uid, lookup_user(&user).map(|(_, gid)| gid).unwrap_or(uid)),
        None => {
            if is_isolated_test_env() {
                return None;
            }
            if let Some(found) = lookup_user(&user) {
                found
            } else if std::env::var_os("ANYAGENTS_AGENTS").is_none() {
                let (fallback_user, found) = fallback_seat(agent_name, parent_name)?;
                user = fallback_user;
                found
            } else {
                return None;
            }
        }
    };
    let sock_gid = sock_gid_for()?;
    if uid == 0 || uid == current_uid() {
        return None;
    }
    Some((user, uid, gid, sock_gid))
}

fn fallback_seat(agent_name: &str, parent_name: Option<&str>) -> Option<(String, (u32, u32))> {
    if let Some(parent) = parent_name {
        let direct = seat_user(parent);
        if let Some((uid, gid)) = lookup_user(&direct)
            && server_has_gid(gid)
        {
            return Some((direct, (uid, gid)));
        }
        if let Some((prefix, _)) = parent.split_once('-') {
            let candidate = seat_user(prefix);
            if let Some((uid, gid)) = lookup_user(&candidate)
                && server_has_gid(gid)
            {
                return Some((candidate, (uid, gid)));
            }
        }
    }
    if let Some((prefix, _)) = agent_name.split_once('-') {
        let candidate = seat_user(prefix);
        if let Some((uid, gid)) = lookup_user(&candidate)
            && server_has_gid(gid)
        {
            return Some((candidate, (uid, gid)));
        }
    }
    let all_seats = host_seat_users();
    if all_seats.is_empty() {
        return None;
    }
    let hash = agent_name.bytes().fold(0usize, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(b as usize)
    });
    all_seats.get(hash % all_seats.len()).cloned()
}

fn host_seat_users() -> Vec<(String, (u32, u32))> {
    let Ok(text) = std::fs::read_to_string("/etc/passwd") else {
        return Vec::new();
    };
    let me = current_uid();
    let mut out = Vec::new();
    for line in text.lines() {
        let mut parts = line.split(':');
        let (Some(name), Some(_), Some(uid_s), Some(gid_s)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if !name.starts_with("seat-") {
            continue;
        }
        if let (Ok(uid), Ok(gid)) = (uid_s.parse::<u32>(), gid_s.parse::<u32>())
            && uid != 0
            && uid != me
            && server_has_gid(gid)
        {
            out.push((name.to_string(), (uid, gid)));
        }
    }
    out
}

fn server_has_gid(target_gid: u32) -> bool {
    if server_gid() == target_gid {
        return true;
    }
    // SAFETY: getgroups(0, null) returns the supplementary group count.
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count > 0 {
        let mut gids = vec![0 as libc::gid_t; count as usize];
        // SAFETY: `gids` has capacity `count`.
        let read = unsafe { libc::getgroups(count, gids.as_mut_ptr()) };
        if read > 0 && gids[..read as usize].contains(&target_gid) {
            return true;
        }
    }
    !is_isolated_test_env() && server_user_in_group_db(target_gid) && launcher().is_file()
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

/// If `config_dir` already exists on disk and its group belongs to a provisioned
/// host `seat-*` account, return that seat's `(user, uid, gid, seat_launch_spec)`
/// so `describe()`, `rename`, and runtime reconciliation never flip a running
/// agent's config directory to a different seat group.
pub fn existing_config_seat(config_dir: &Path) -> Option<(String, u32, u32, Option<String>)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if is_isolated_test_env() {
            return None;
        }
        let meta = std::fs::metadata(config_dir).ok()?;
        let dir_gid = meta.gid();
        if dir_gid == 0 || dir_gid == server_gid() {
            return None;
        }
        let candidates: Vec<(String, (u32, u32))> = host_seat_users()
            .into_iter()
            .filter(|(_, (_, gid))| *gid == dir_gid)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let chosen = if candidates.len() == 1 {
            candidates.into_iter().next()
        } else {
            let marker = std::fs::read_to_string(config_dir.join(".anyagents-seat"))
                .ok()
                .map(|s| seat_user(s.trim()));
            let by_marker = marker.and_then(|m| candidates.iter().find(|(u, _)| u == &m).cloned());
            by_marker
                .or_else(|| {
                    for key_file in ["state_5.sqlite", "installation_id", "logs_2.sqlite"] {
                        if let Ok(m) = std::fs::metadata(config_dir.join(key_file)) {
                            let f_uid = m.uid();
                            if let Some(found) =
                                candidates.iter().find(|(_, (uid, _))| *uid == f_uid)
                            {
                                return Some(found.clone());
                            }
                        }
                    }
                    let mut uid_counts = std::collections::HashMap::<u32, usize>::new();
                    if let Ok(entries) = std::fs::read_dir(config_dir) {
                        for entry in entries.flatten() {
                            if let Ok(em) = entry.metadata() {
                                *uid_counts.entry(em.uid()).or_insert(0) += 1;
                            }
                        }
                    }
                    candidates
                        .iter()
                        .filter(|(_, (uid, _))| uid_counts.contains_key(uid))
                        .max_by_key(|(_, (uid, _))| uid_counts.get(uid).copied().unwrap_or(0))
                        .cloned()
                })
                .or_else(|| candidates.into_iter().next())
        };
        if let Some((user, (uid, gid))) = chosen {
            let spec = sock_gid_for()
                .filter(|_| launcher().is_file())
                .map(|sock_gid| {
                    format!("{}\t{uid}\t{gid}\t{gid},{sock_gid}", launcher().display())
                });
            return Some((user, uid, gid, spec));
        }
    }
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

/// Give one agent's config directory to its seat. Workdirs have their own
/// fail-closed sharing and admission before the agent becomes running.
#[allow(dead_code)]
pub fn share_config_with_seat(config_dir: &Path, gid: u32) {
    let uid = host_seat_users()
        .into_iter()
        .find_map(|(_, (u, g))| (g == gid).then_some(u))
        .unwrap_or(gid);
    share_config_with_seat_uid(config_dir, uid, gid);
}

pub fn share_config_with_seat_uid(config_dir: &Path, uid: u32, gid: u32) {
    let (eff_uid, eff_gid) = match existing_config_seat(config_dir) {
        Some((_, existing_uid, existing_gid, _)) => (existing_uid, existing_gid),
        None => (uid, gid),
    };
    if config_dir.is_dir() {
        if let Some((user, _)) = host_seat_users()
            .into_iter()
            .find(|(_, (u, g))| *u == eff_uid && *g == eff_gid)
        {
            let _ = std::fs::write(config_dir.join(".anyagents-seat"), format!("{user}\n"));
        }
        if let Err(err) = give_tree(config_dir, eff_uid, eff_gid) {
            eprintln!("seat: cannot share {}: {err}", config_dir.display());
        }
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
                if std::fs::create_dir_all(&path).is_err() {
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
        || matches!(
            canon.to_str(),
            Some(
                "/scratch/wt"
                    | "/scratch/wt/owners-main"
                    | "/workspace-data/workspace"
                    | "/workspace-data/workspace/.worktrees"
            )
        )
        || canon.starts_with("/workspace-data/build-targets")
    {
        return Err(Error::WorkdirPermission {
            path: path.display().to_string(),
            message: "refusing recursive permission changes to a shared root".into(),
        });
    }
    crate::worktree_access::share(path, uid, gid, journal)
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
        if me == 0 || me >= 2100 {
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
            let (Some(gname), Some(_pw), Some(gid_s), Some(members)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            if !(gname == "anyagents-sock" || gname.starts_with("seat-")) {
                continue;
            }
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

/// Registered root-to-principal resolution is the same authority used by vendor
/// launch. Unregistered directories remain foreign in the shared inventory.
pub(crate) fn peer_principals(store: &anyagents_core::Store) -> Result<String, Error> {
    let mut peers = Vec::new();
    for agent in store.list_agents()? {
        if let Some((_, uid, gid, _)) = crate::create::resolved_agent_seat_info(store, &agent) {
            peers.push(anyagents_harness::SeatPeer {
                root: anyagents_core::agent_config_dir(store.data_dir(), &agent.id)
                    .parent()
                    .unwrap()
                    .to_owned(),
                uid,
                gid,
            });
        }
    }
    serde_json::to_string(&peers).map_err(|e| Error::Message(e.to_string()))
}

pub(super) fn descriptor_launcher() -> Option<PathBuf> {
    let path = launcher();
    path.is_file().then_some(path)
}

fn give_tree(dir: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    crate::seat_tree::share(dir, uid, gid)
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
    if let Ok(store) = anyagents_core::Store::open(data.join("state.db")) {
        environment.push((
            anyagents_harness::SEAT_PEERS_ENV.to_owned(),
            peer_principals(&store)?,
        ));
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
    fn an_own_seat_test_uses_the_process_group_not_its_uid() {
        let _env = crate::server::tests::env_lock();
        // SAFETY: under the server test lock, restored before releasing it.
        let names = ["ANYAGENTS_TESTS", "ANYAGENTS_AGENTS", "ANYAGENTS_FAKE_MODE"];
        let previous = names.map(std::env::var_os);
        unsafe {
            std::env::set_var("ANYAGENTS_TESTS", "own-seat");
            std::env::set_var("ANYAGENTS_AGENTS", "own-seat=2101");
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
        let seat = seat_for_agent("own-seat");
        // SAFETY: as above.
        unsafe {
            for (name, value) in names.into_iter().zip(previous) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
        assert_eq!(
            seat,
            Some((
                "own-seat".to_string(),
                current_uid(),
                server_gid(),
                server_gid(),
            ))
        );
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
}
