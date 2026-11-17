//! Retire state the previous browser implementation left in a profile.
//!
//! It ran each profile's browser as a detached process recorded in
//! `runtime.json`, which outlives a server restart and would hold the
//! profile's lock. Before a session opens a profile, that process is stopped
//! when, and only when, it is still alive and its command line names this
//! profile's `user-data` directory; then the old bookkeeping files go.
//! `user-data/`, `fingerprint.json` and `screenshots/` are never touched.
//!
//! The browser is asked to quit over its own DevTools port first: only a
//! normal quit flushes cookies and storage written in the last half minute
//! (a SIGTERM ends the session without that flush, and its Xvfb dying first
//! is a crash). Signals stop whatever remains.
//!
//! Compatibility path for on-disk state only: delete this module once no
//! deployment has a `runtime.json` or `takeover.json` under
//! `browser-profiles/`.
//! Every profile retires its own the first time it opens.

use std::path::Path;
use std::time::Duration;

const STATE_FILES: [&str; 6] = [
    "runtime.json",
    "refs.json",
    "held_keys.json",
    "takeover.json",
    "takeover-mouse.json",
    "takeover.lock",
];

#[derive(serde::Deserialize)]
struct Runtime {
    pid: u32,
    #[serde(default)]
    cdp_port: Option<u16>,
}

type Result<T> = std::io::Result<T>;
fn invalid(s: &str) -> std::io::Error {
    std::io::Error::other(s)
}

/// Unknown ownership is an error, not permission to discard state/start again.
pub async fn retire(dir: &Path) -> Result<()> {
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(dir.join("runtime.json"))
    {
        Ok(f) => Some(f),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let user_data = dir.join("user-data");
    if let Some(file) = file {
        let meta = file.metadata()?;
        if !meta.is_file() || meta.uid() != crate::sandbox::server_uid() {
            return Err(invalid("legacy runtime is not a server-owned regular file"));
        }
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(invalid("legacy runtime is too large"));
        }
        let rt: Runtime = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        let members = owned_group(rt.pid, &user_data)?;
        if !members.is_empty() {
            let asked = match rt.cdp_port {
                Some(port) => quit_over_devtools(port, &members).await,
                None => false,
            };
            tokio::task::spawn_blocking(move || stop_members(members, &user_data, asked))
                .await
                .map_err(std::io::Error::other)??;
        }
    }
    for name in STATE_FILES {
        // Unlink just this entry, never a symlink's target.
        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Process {
    pid: i32,
    parent: i32,
    group: i32,
    start: u64,
    cmd: Vec<u8>,
}
fn valid_pid(pid: u32) -> Option<i32> {
    i32::try_from(pid).ok().filter(|p| *p > 0)
}
fn parse_stat(stat: &str) -> Result<(bool, i32, i32, u64)> {
    let f: Vec<_> = stat
        .rsplit_once(')')
        .ok_or_else(|| invalid("invalid process stat"))?
        .1
        .split_whitespace()
        .collect();
    if f.len() < 20 {
        return Err(invalid("incomplete process stat"));
    }
    Ok((
        !matches!(f[0], "Z" | "X" | "x"),
        f[1].parse().map_err(std::io::Error::other)?,
        f[2].parse().map_err(std::io::Error::other)?,
        f[19].parse().map_err(std::io::Error::other)?,
    ))
}
fn process(pid: i32) -> Result<Option<Process>> {
    use std::os::unix::fs::MetadataExt as _;
    if pid <= 0 {
        return Ok(None);
    }
    let root = format!("/proc/{pid}");
    let read = || -> Result<Option<Process>> {
        if std::fs::metadata(&root)?.uid() != crate::sandbox::server_uid() {
            return Ok(None);
        }
        let before = std::fs::read_to_string(format!("{root}/stat"))?;
        let (alive, parent, group, start) = parse_stat(&before)?;
        if !alive {
            return Ok(None);
        }
        let mut cmd = std::fs::read(format!("{root}/cmdline"))?;
        if cmd.is_empty() {
            for _ in 0..50 {
                let stat = std::fs::read_to_string(format!("{root}/stat"))?;
                let (alive, _, g, t) = parse_stat(&stat)?;
                if !alive {
                    return Ok(None);
                }
                if g != group || t != start {
                    return Err(invalid("legacy process changed during inspection"));
                }
                std::thread::sleep(Duration::from_millis(10));
                cmd = std::fs::read(format!("{root}/cmdline"))?;
                if !cmd.is_empty() {
                    break;
                }
            }
        }
        if cmd.is_empty() {
            return Err(invalid("cannot establish legacy process ownership"));
        }
        let after = std::fs::read_to_string(format!("{root}/stat"))?;
        let (alive, _, g, t) = parse_stat(&after)?;
        if !alive {
            return Ok(None);
        }
        if g != group || t != start {
            return Err(invalid("legacy process changed during inspection"));
        }
        Ok(Some(Process {
            pid,
            parent,
            group,
            start,
            cmd,
        }))
    };
    match read() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        result => result,
    }
}
fn names_profile(p: &Process, dir: &Path) -> bool {
    let want = format!("--user-data-dir={}", dir.display());
    p.cmd.split(|b| *b == 0).any(|a| a == want.as_bytes())
}
fn members(group: i32) -> Result<Vec<Process>> {
    use std::os::unix::fs::MetadataExt as _;
    let mut out = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        let mut inspect = || -> Result<()> {
            if entry.metadata()?.uid() != crate::sandbox::server_uid() {
                return Ok(());
            }
            let stat = std::fs::read_to_string(entry.path().join("stat"))?;
            let (alive, _, g, _) = parse_stat(&stat)?;
            if alive
                && g == group
                && let Some(p) = process(pid)?
                && p.group == group
            {
                out.push(p);
            }
            Ok(())
        };
        match inspect() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            result => result?,
        }
    }
    Ok(out)
}
fn owned_group(pid: u32, dir: &Path) -> Result<Vec<Process>> {
    let Some(group) = valid_pid(pid) else {
        return Ok(Vec::new());
    };
    // A reused live leader is definitive disownership, not permission to scan
    // its replacement's group for an accidentally matching command line.
    if let Some(leader) = process(group)? {
        if leader.group != group || !names_profile(&leader, dir) {
            return Ok(Vec::new());
        }
    } else if std::fs::metadata(format!("/proc/{group}")).is_ok_and(|m| {
        use std::os::unix::fs::MetadataExt as _;
        m.uid() != crate::sandbox::server_uid()
    }) {
        return Ok(Vec::new());
    }
    let all = members(group)?;
    if all.iter().any(|p| names_profile(p, dir)) || all.is_empty() {
        Ok(all)
    } else {
        Err(invalid("cannot establish surviving legacy group ownership"))
    }
}
fn is_helper(cmd: &[u8]) -> bool {
    cmd.split(|b| *b == 0)
        .any(|arg| arg.starts_with(b"--type="))
}
fn browser_member<'a>(all: &'a [Process], dir: &Path) -> Option<&'a Process> {
    all.iter()
        .filter(|p| !is_helper(&p.cmd) && names_profile(p, dir))
        .find(|p| all.iter().any(|c| c.parent == p.pid && is_helper(&c.cmd)))
        .or_else(|| {
            all.iter()
                .find(|p| !is_helper(&p.cmd) && names_profile(p, dir))
        })
}
#[cfg(test)]
fn browser_in_group(group: i32, dir: &Path) -> Option<i32> {
    browser_member(&members(group).ok()?, dir).map(|p| p.pid)
}
fn still_same(p: &Process) -> Result<bool> {
    // Parent changes on reparenting; command/group changes do not prove exit.
    Ok(process(p.pid)?.is_some_and(|current| current.start == p.start))
}
fn wait_gone(p: &Process, limit: Duration) -> Result<bool> {
    let deadline = std::time::Instant::now() + limit;
    while still_same(p)? {
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(true)
}
/// pidfd pins the inspected process: PID reuse between recheck and signal
/// cannot hit its replacement. Unsupported kernels fail closed.
fn signal(p: &Process, sig: i32) -> Result<()> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    // SAFETY: positive PID, flags zero; kernel validates both.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, p.pid, 0) } as i32;
    if fd < 0 {
        let e = std::io::Error::last_os_error();
        return if e.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(e)
        };
    }
    // SAFETY: pidfd_open returned a fresh descriptor.
    let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
    let Some(current) = process(p.pid)? else {
        return Ok(());
    };
    if current.start != p.start {
        return Ok(());
    }
    if current.group != p.group || current.cmd != p.cmd {
        return Err(invalid("legacy process ownership changed before signaling"));
    }
    // SAFETY: descriptor pins this process; null siginfo and flags zero.
    if unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            sig,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    } < 0
    {
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::ESRCH) {
            return Err(e);
        }
    }
    Ok(())
}
// SIGSTOP delivery is asynchronous: confirm the stop before sweeping children,
// otherwise a launcher already inside fork could publish a child after the scan.
fn freeze(p: &Process) -> Result<()> {
    signal(p, libc::SIGSTOP)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let stat = match std::fs::read_to_string(format!("/proc/{}/stat", p.pid)) {
            Ok(stat) => stat,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let (alive, _, group, start) = parse_stat(&stat)?;
        if !alive || start != p.start {
            return Ok(());
        }
        if group != p.group {
            return Err(invalid("legacy process group changed while freezing"));
        }
        let state = stat
            .rsplit_once(')')
            .and_then(|(_, tail)| tail.split_whitespace().next());
        if matches!(state, Some("T" | "t")) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(invalid("legacy process did not stop"));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn stop_members(all: Vec<Process>, dir: &Path, asked: bool) -> Result<()> {
    if let Some(browser) = browser_member(&all, dir)
        && !(asked && wait_gone(browser, Duration::from_secs(10))?)
    {
        signal(browser, libc::SIGTERM)?;
        if !wait_gone(browser, Duration::from_secs(5))? {
            signal(browser, libc::SIGKILL)?;
            if !wait_gone(browser, Duration::from_secs(5))? {
                return Err(invalid("legacy browser did not exit"));
            }
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while all
        .iter()
        .filter(|p| is_helper(&p.cmd))
        .try_fold(false, |live, p| {
            Ok::<_, std::io::Error>(still_same(p)? || live)
        })?
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(100));
    }
    let group = all[0].group;
    let mut known = all;
    // A known survivor pins this PGID. Freeze launchers before discovering
    // children spawned after the browser exited (e.g. xvfb-run cleanup).
    for _ in 0..8 {
        if !known.iter().any(|p| {
            process(p.pid)
                .ok()
                .flatten()
                .is_some_and(|c| c.start == p.start && c.group == group)
        }) {
            break;
        }
        for p in &known {
            freeze(p)?;
        }
        let mut added = false;
        for p in members(group)? {
            if !known.iter().any(|k| k.pid == p.pid && k.start == p.start) {
                known.push(p);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    for p in &known {
        signal(p, libc::SIGTERM)?;
        signal(p, libc::SIGCONT)?;
    }
    for p in &known {
        if !wait_gone(p, Duration::from_secs(5))? {
            signal(p, libc::SIGKILL)?;
            signal(p, libc::SIGCONT)?;
        }
        if !wait_gone(p, Duration::from_secs(5))? {
            return Err(invalid("legacy group did not exit"));
        }
    }
    if !members(group)?.is_empty() {
        return Err(invalid("legacy group changed during retirement"));
    }
    Ok(())
}
/// The saved port must still be a listening socket held by an inspected,
/// unchanged member of this profile's group; port number alone is not proof.
fn port_owned(port: u16, all: &[Process]) -> bool {
    if port == 0 {
        return false;
    }
    let Ok(tcp) = std::fs::read_to_string("/proc/net/tcp") else {
        return false;
    };
    let sockets: Vec<_> = tcp
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<_> = l.split_whitespace().collect();
            (f.len() > 9 && f[1] == format!("0100007F:{port:04X}") && f[3] == "0A")
                .then(|| format!("socket:[{}]", f[9]))
        })
        .collect();
    all.iter().any(|p| {
        still_same(p).unwrap_or(false)
            && std::fs::read_dir(format!("/proc/{}/fd", p.pid)).is_ok_and(|fds| {
                fds.filter_map(|e| e.ok()).any(|e| {
                    std::fs::read_link(e.path()).is_ok_and(|l| {
                        sockets
                            .iter()
                            .any(|s| l.as_os_str() == std::ffi::OsStr::new(s))
                    })
                })
            })
    })
}

/// Ask the browser on loopback DevTools `port` to quit normally; `true` when
/// the request was sent. The reply may never come (the browser is gone);
/// `stop_members` waits for the exit.
async fn quit_over_devtools(port: u16, members: &[Process]) -> bool {
    if !port_owned(port, members) {
        return false;
    }
    let version = format!("http://127.0.0.1:{port}/json/version");
    let endpoint = tokio::task::spawn_blocking(move || {
        let http = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(3)))
            .max_redirects(0)
            .proxy(None)
            .build()
            .new_agent();
        let v: serde_json::Value = http
            .get(&version)
            .call()
            .ok()?
            .body_mut()
            .read_json()
            .ok()?;
        v["webSocketDebuggerUrl"].as_str().map(String::from)
    })
    .await
    .ok()
    .flatten();
    let Some(endpoint) = endpoint else {
        return false;
    };
    let Ok(url) = url::Url::parse(&endpoint) else {
        return false;
    };
    if url.scheme() != "ws"
        || url.host_str() != Some("127.0.0.1")
        || url.port() != Some(port)
        || !url.path().starts_with("/devtools/browser/")
        || !url.username().is_empty()
        || url.password().is_some()
        || !port_owned(port, members)
    {
        return false;
    }
    let Ok(mut cdp) = agent_workstation::cdp::Cdp::connect(&endpoint).await else {
        return false;
    };
    if !port_owned(port, members) {
        return false;
    }
    let _ = cdp
        .call_in(
            None,
            "Browser.close",
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_browser_stops_before_its_launcher_and_display() {
        let dir = tempfile::tempdir().unwrap();
        let user_data = dir.path().join("user-data");
        std::fs::create_dir_all(&user_data).unwrap();
        let flag = format!("--user-data-dir={}", user_data.display());
        let mark = dir.path().join("stopped-with-display-up");
        // A launcher (like xvfb-run, whose display dies with it) running a
        // browser that has one helper, which ends with the browser. The
        // browser records whether its launcher was still alive when it was
        // asked to stop.
        let browser = dir.path().join("browser.sh");
        std::fs::write(
            &browser,
            format!(
                "trap 'kill -0 $PPID 2>/dev/null && touch {}; exit 0' TERM\n\
                 sh -c 'while kill -0 $PPID 2>/dev/null; do sleep 0.05; done' --type=renderer &\nwait\n",
                mark.display()
            ),
        )
        .unwrap();
        let launcher = dir.path().join("launcher.sh");
        std::fs::write(
            &launcher,
            format!("sh {} \"$1\" &\nwait\nsleep 30\n", browser.display()),
        )
        .unwrap();
        let mut group = {
            use std::os::unix::process::CommandExt as _;
            std::process::Command::new("/bin/sh")
                .arg(&launcher)
                .arg(&flag)
                .process_group(0)
                .spawn()
                .unwrap()
        };
        let pid = i32::try_from(group.id()).unwrap();
        let found = (0..250).find_map(|_| {
            let b = browser_in_group(pid, &user_data).filter(|&b| b != pid);
            if b.is_none() {
                std::thread::sleep(Duration::from_millis(20));
            }
            b
        });
        assert!(
            found.is_some_and(|b| b != pid),
            "the browser, not its launcher"
        );
        std::fs::write(
            dir.path().join("runtime.json"),
            format!(r#"{{"pid": {pid}}}"#),
        )
        .unwrap();
        retire(dir.path()).await.unwrap();
        assert!(
            mark.exists(),
            "the browser stopped while its display was up"
        );
        assert!(group.wait().is_ok());
        assert!(!dir.path().join("runtime.json").exists());
    }

    #[tokio::test]
    async fn retires_only_the_process_started_for_this_profile() {
        let dir = tempfile::tempdir().unwrap();
        let user_data = dir.path().join("user-data");
        std::fs::create_dir_all(&user_data).unwrap();
        std::fs::write(user_data.join("Cookies"), b"keep").unwrap();
        std::fs::write(dir.path().join("fingerprint.json"), b"{}").unwrap();

        // A stand-in for the old detached browser: its own process group,
        // with this profile's user-data flag on its command line.
        let mut ours = {
            use std::os::unix::process::CommandExt as _;
            std::process::Command::new("/bin/sh")
                .args(["-c", "sleep 30; :", "sh"])
                .arg(format!("--user-data-dir={}", user_data.display()))
                .process_group(0)
                .spawn()
                .unwrap()
        };
        let group_pid = i32::try_from(ours.id()).unwrap();
        let settled = (0..250).any(|_| {
            let ready = members(group_pid).is_ok_and(|procs| {
                procs.len() >= 2
                    && procs
                        .iter()
                        .any(|p| p.pid != group_pid && !names_profile(p, &user_data))
            });
            if !ready {
                std::thread::sleep(Duration::from_millis(20));
            }
            ready
        });
        assert!(settled, "detached shell group settled on sleep");
        // Not ours: a live process whose pid the file might name after reuse.
        let mut other = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();

        std::fs::write(
            dir.path().join("runtime.json"),
            format!(r#"{{"pid": {}}}"#, other.id()),
        )
        .unwrap();
        retire(dir.path()).await.unwrap();
        assert!(
            other.try_wait().unwrap().is_none(),
            "unrelated process untouched"
        );
        assert!(!dir.path().join("runtime.json").exists());

        std::fs::write(
            dir.path().join("runtime.json"),
            format!(r#"{{"pid": {}}}"#, ours.id()),
        )
        .unwrap();
        std::fs::write(dir.path().join("held_keys.json"), b"[]").unwrap();
        retire(dir.path()).await.unwrap();
        assert!(ours.wait().is_ok());
        for name in STATE_FILES {
            assert!(!dir.path().join(name).exists(), "{name}");
        }
        assert_eq!(std::fs::read(user_data.join("Cookies")).unwrap(), b"keep");
        assert!(dir.path().join("fingerprint.json").exists());
        let _ = other.kill();
        let _ = other.wait();
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[test]
    fn pid_values_never_wrap_into_group_signals() {
        for pid in [0, i32::MAX as u32 + 1, u32::MAX] {
            assert_eq!(valid_pid(pid), None);
            assert!(
                owned_group(pid, Path::new("/not-a-profile"))
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn process_stat_recognizes_zombies_and_parentheses_in_names() {
        for state in ["Z", "X", "x"] {
            let mut fields = vec!["0"; 20];
            fields[0] = state;
            fields[1] = "1";
            fields[2] = "42";
            assert!(
                !parse_stat(&format!("42 (a tricky ) name) {}", fields.join(" ")))
                    .unwrap()
                    .0
            );
        }
        let fields = [
            "S", "11", "42", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
            "0", "0", "1234",
        ];
        assert_eq!(
            parse_stat(&format!("42 (a tricky ) name) {}", fields.join(" "))).unwrap(),
            (true, 11, 42, 1234)
        );
        assert!(parse_stat("unreadable").is_err());
        assert!(parse_stat("42 (name) S").is_err());
    }

    #[test]
    fn profile_and_helper_flags_are_whole_arguments() {
        let dir = Path::new("/tmp/profile");
        let names_profile = |cmd: &[u8], dir: &Path| {
            super::names_profile(
                &Process {
                    pid: 1,
                    parent: 0,
                    group: 1,
                    start: 1,
                    cmd: cmd.to_vec(),
                },
                dir,
            )
        };
        assert!(names_profile(
            b"browser\0--user-data-dir=/tmp/profile\0",
            dir
        ));
        assert!(!names_profile(
            b"browser\0--user-data-dir=/tmp/profile-other\0",
            dir
        ));
        assert!(!names_profile(
            b"browser\0--fake=--user-data-dir=/tmp/profile\0",
            dir
        ));
        assert!(is_helper(b"browser\0--type=renderer\0"));
        assert!(!is_helper(b"browser\0--log=/tmp/--type=renderer\0"));
    }

    #[test]
    fn identity_survives_reparenting_but_never_signals_reused_pid() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let mut p = process(child.id() as i32).unwrap().unwrap();
        p.parent = p.parent.wrapping_add(1);
        assert!(still_same(&p).unwrap(), "PPID is not process identity");
        p.start = p.start.wrapping_add(1);
        assert!(!still_same(&p).unwrap());
        signal(&p, libc::SIGTERM).unwrap();
        assert!(child.try_wait().unwrap().is_none(), "replacement untouched");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn changed_group_or_command_cannot_authorize_a_signal() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let original = process(child.id() as i32).unwrap().unwrap();
        let mut changed = original.clone();
        changed.group = changed.group.wrapping_add(1);
        assert!(signal(&changed, libc::SIGTERM).is_err());
        changed = original;
        changed.cmd.extend_from_slice(b"unexpected");
        assert!(signal(&changed, libc::SIGTERM).is_err());
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[tokio::test]
    async fn unknown_runtime_or_link_preserves_state_and_refuses_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = dir.path().join("runtime.json");
        let held = dir.path().join("held_keys.json");
        std::fs::write(&held, "[]").unwrap();
        for contents in ["{", &"x".repeat(65_537)] {
            std::fs::write(&runtime, contents).unwrap();
            assert!(retire(dir.path()).await.is_err());
            assert!(runtime.exists() && held.exists());
        }
        std::fs::remove_file(&runtime).unwrap();
        let external = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(external.path(), r#"{"pid":0}"#).unwrap();
        std::os::unix::fs::symlink(external.path(), &runtime).unwrap();
        assert!(retire(dir.path()).await.is_err());
        assert!(runtime.is_symlink() && held.exists());
        assert_eq!(
            std::fs::read_to_string(external.path()).unwrap(),
            r#"{"pid":0}"#
        );
    }

    #[test]
    fn an_unreaped_zombie_is_not_a_live_browser() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
            if !parse_stat(&stat).unwrap().0 {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(process(pid).unwrap().is_none());
        assert!(
            owned_group(child.id(), Path::new("/not-a-profile"))
                .unwrap()
                .is_empty()
        );
        child.wait().unwrap();
    }
}
