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
fn launcher() -> PathBuf {
    match std::env::var_os("ANYAGENTS_LAUNCHER") {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => PathBuf::from(SETPRIV),
    }
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

/// Whether any part of a seat is configured.
pub fn seat_named() -> bool {
    ["ANYAGENTS_AGENT_UID", "ANYAGENTS_AGENT_GID", "ANYAGENTS_SOCKET_GID"]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
}

/// Whether every harness process this server starts is in an OS seat: the
/// launcher is installed, and there is a real seat for every agent here. A
/// configured uid counts only for the single agent it names.
pub fn launch_boundary(agents: &[String]) -> bool {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        // A test machine has no OS accounts of its own to place an agent in.
        return true;
    }
    // The boundary is ready when the launcher is installed and every agent of
    // this deployment has a seat. The seat is bound to one agent: its uid comes
    // from the deployment's own start line, never from anything an agent
    // writes, and never from the server's own.
    if !launcher().is_file() {
        return false;
    }
    // An agent created after this server started has no line yet; it gets its
    // seat when it is created, and its first turn waits for one.
    agents.iter().all(|agent| match pinned_uid(agent) {
        Some(uid) => uid != 0 && Some(uid) != Some(current_uid()),
        None => true,
    })
}

/// The socket group: the configured one, else the host's `anyagents-sock`.
fn sock_gid_for() -> Option<u32> {
    configured_seat()
        .map(|(_, _, sock)| sock)
        .or_else(|| lookup_group("anyagents-sock"))
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
    format!("seat-{}", agent_name.to_ascii_lowercase())
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
pub fn seat_for_agent(agent_name: &str) -> Option<(String, u32, u32, u32)> {
    let configured = configured_seat();
    let agents = std::env::var("ANYAGENTS_AGENTS").unwrap_or_default();
    let explicit = configured.is_some() && agents.trim() == agent_name;
    if let Some((uid, gid, sock_gid)) = configured
        && (explicit || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some())
    {
        return Some((agent_name.to_string(), uid, gid, sock_gid));
    }
    if let Some(uid) = pinned_uid(agent_name)
        && std::env::var_os("ANYAGENTS_TESTS").is_some()
    {
        // A test machine is its own single seat.
        return Some((agent_name.to_string(), current_uid(), current_uid(), current_uid()));
    }
    if let Some(uid) = pinned_uid(agent_name)
        && let Some(sock_gid) = sock_gid_for()
    {
        // The deployment's own line gives this agent its uid; the server gives
        // the capability to the group that uid shares, and no other.
        return Some((seat_user(agent_name), uid, uid, sock_gid));
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        return None;
    }
    // The uid this deployment gives the agent; the name is only how the OS
    // account is displayed.
    let user = seat_user(agent_name);
    let uid = pinned_uid(agent_name)?;
    let gid = lookup_user(&user).map(|(_, gid)| gid).unwrap_or(uid);
    let sock_gid = configured.map(|(_, _, sock)| sock).or_else(|| lookup_group("anyagents-sock"))?;
    if uid == 0 || uid == current_uid() {
        return None;
    }
    Some((user, uid, gid, sock_gid))
}

/// The launch spec a harness adapter carries for this agent:
/// `<launcher>\t<uid>\t<gid>\t<groups>`.
pub fn seat_launch_spec(agent_name: &str) -> Option<(String, String)> {
    let (user, uid, gid, sock_gid) = seat_for_agent(agent_name)?;
    if !launcher().is_file() {
        return None;
    }
    Some((
        user,
        format!("{}\t{uid}\t{gid}\t{gid},{sock_gid}", launcher().display()),
    ))
}

/// Give a minted capability to its seat's group: 0640, owned by the server's
/// user, readable by that one seat. The mode and group are read back; a file
/// any other seat could read is a refusal.
pub fn share_capability_with_seat(path: &Path, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| std::io::Error::other("capability path"))?;
    // SAFETY: `c_path` is NUL-terminated; `u32::MAX` keeps the owner.
    let rc = unsafe { libc::chown(c_path.as_ptr(), u32::MAX, gid) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
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

/// Give one agent's config directory and the work directory beside it to its
/// seat group, so the seat reads and writes its own files and no peer's.
pub fn share_config_with_seat(config_dir: &Path, gid: u32) {
    for dir in [config_dir.to_path_buf(), config_dir.with_file_name("work")] {
        if dir.is_dir()
            && let Err(err) = give_tree(&dir, gid)
        {
            eprintln!("seat: cannot share {}: {err}", dir.display());
        }
    }
}

fn give_tree(dir: &Path, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let c_path = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes())
        .map_err(|_| std::io::Error::other("path"))?;
    // SAFETY: `c_path` is NUL-terminated; the owner is kept.
    if unsafe { libc::chown(c_path.as_ptr(), u32::MAX, gid) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o2770))?;
    for entry in std::fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            continue;
        }
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
            continue;
        };
        // SAFETY: as above.
        let _ = unsafe { libc::chown(c_path.as_ptr(), u32::MAX, gid) };
        if kind.is_dir() {
            give_tree(&path, gid)?;
        } else if name == crate::operator::CAPABILITY_FILE {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))?;
        } else {
            let exec = entry
                .metadata()
                .map(|meta| meta.permissions().mode() & 0o100 != 0)
                .unwrap_or(false);
            let mode = if exec { 0o770 } else { 0o660 };
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
        }
    }
    Ok(())
}

/// One thing the boundary promises, as a shell program.
struct Check {
    what: &'static str,
    program: String,
}

fn checks(config: Option<&Path>, boundary: &Boundary, uid: u32, gid: u32) -> Vec<Check> {
    let data = PathBuf::from(&boundary.data_dir);
    let deny = |what: &'static str, path: String| Check {
        what,
        program: format!("[ ! -r {path} ] && [ ! -w {path} ]"),
    };
    let mut list = vec![
        Check {
            what: "the agent runs as its own uid, not the server's",
            program: format!(
                "[ \"$(id -u)\" = \"{uid}\" ] && [ \"$(id -u)\" != \"{}\" ]",
                current_uid()
            ),
        },
        Check {
            what: "the agent runs in its own group",
            program: format!("[ \"$(id -g)\" = \"{gid}\" ]"),
        },
        Check {
            what: "no privilege can be gained after the start",
            program: "[ \"$(awk '/^NoNewPrivs:/ {{print $2}}' /proc/self/status)\" = 1 ]"
                .to_string(),
        },
    ];
    for name in [
        "state.db",
        "secrets",
        "anyagents.sock",
        "operator-token",
        "identity-boundary.json",
    ] {
        list.push(deny(
            "the server's own files are denied",
            data.join(name).display().to_string(),
        ));
    }
    list.push(Check {
        what: "a peer seat's config is denied",
        program: "p=; for d in \"$DATA\"/agents/*/config; do [ \"$d\" = \"$MINE\" ] && continue; \
                  [ -e \"$d\" ] || continue; p=$d; break; done; \
                  [ -z \"$p\" ] || { [ ! -r \"$p\" ] && [ ! -r \"$p/mcp-capability\" ]; }"
            .to_string(),
    });
    if let Some(config) = config {
        let mine = config.display().to_string();
        list.push(Check {
            what: "the agent's own config is writable by it",
            program: format!("[ -w {mine} ]"),
        });
        list.push(Check {
            what: "its capability is readable by its own seat only",
            program: format!(
                "m=$(stat -c %a {mine}/mcp-capability); [ \"$m\" = 640 ] || [ \"$m\" = 600 ]"
            ),
        });
    }
    list
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
    let groups = format!("{gid},{sock_gid}");
    let mut script = String::from("set -e\n");
    for check in checks(config, &boundary, uid, gid) {
        script.push_str(&format!("# {}\n{}\n", check.what, check.program));
    }
    // The probe asserts every property of the boundary and then exits; the
    // agent's own command is not started unless it passes.
    let probe_ref = [
        "/bin/sh".to_string(),
        "-c".to_string(),
        script,
        "agent-run".to_string(),
    ];
    let mut probe = prefix(uid, gid, &groups);
    probe.extend(probe_ref);
    let output = std::process::Command::new(&probe[0])
        .args(&probe[1..])
        .env("DATA", &boundary.data_dir)
        .env(
            "MINE",
            config.map(|dir| dir.display().to_string()).unwrap_or_default(),
        )
        .output()
        .map_err(|err| Error::Message(unadmitted(&boundary, &err.to_string())))?;
    if !output.status.success() {
        let why = String::from_utf8_lossy(&output.stderr);
        let first = why.lines().next().unwrap_or("a boundary check failed");
        return Err(Error::Message(unadmitted(&boundary, first)));
    }
    let mut argv = prefix(uid, gid, &groups);
    argv.extend(command.iter().cloned());
    let status = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .env("ANYAGENTS_DATA", &boundary.data_dir)
        .status()
        .map_err(Error::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Message(format!("the agent command exited {status}")))
    }
}

/// The privileged launcher's own arguments, then `--`.
fn prefix(uid: u32, gid: u32, groups: &str) -> Vec<String> {
    let launcher = launcher();
    [
        launcher.to_str().unwrap_or(SETPRIV),
        "--reuid",
        &uid.to_string(),
        "--regid",
        &gid.to_string(),
        "--groups",
        groups,
        "--no-new-privs",
        "--",
    ]
    .iter()
    .map(|part| part.to_string())
    .collect()
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
    fn the_probe_asserts_the_process_and_what_it_is_denied() {
        let dir = tempfile::tempdir().unwrap();
        let contract = boundary::write(dir.path()).unwrap();
        let config = dir.path().join("agents/one/config");
        let text: String = checks(Some(&config), &contract, 2100, 2100)
            .iter()
            .map(|check| check.program.clone())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("id -u"), "{text}");
        assert!(text.contains("NoNewPrivs"), "{text}");
        for denied in ["state.db", "secrets", "anyagents.sock", "operator-token"] {
            assert!(text.contains(denied), "{text}");
        }
        assert!(text.contains("mcp-capability"), "{text}");
    }
}
