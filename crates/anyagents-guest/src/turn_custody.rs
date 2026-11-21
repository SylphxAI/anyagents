//! Single-writer handoff for detached Linux turns. The worker calls `setsid`
//! before exec, so its session contains the harness even when the latter
//! starts its own process group or the worker dies and leaves it orphaned.

use std::fs::{self, File};
use std::io::{self, Read as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub(super) const OWNER_ENV: &str = "ANYAGENTS_DETACHED_TURN_OWNER";
const TERM_GRACE: Duration = Duration::from_millis(800);
const KILL_GRACE: Duration = Duration::from_secs(3);
const PROBE: Duration = Duration::from_millis(25);

/// Serialize cleanup, spawn and record publication on this agent's state dir.
/// Don't unlink the lock inode: concurrent launchers must contend on it.
pub(super) struct Dispatch {
    _lock: File,
    path: PathBuf,
    namespace: Namespace,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct Namespace {
    boot_id: String,
    pid_ns: u64,
}

#[derive(Deserialize, Serialize)]
struct Owner {
    namespace: Namespace,
    pid: i32,
    start: u64,
    token: String,
}

impl Dispatch {
    pub(super) fn acquire(turns_dir: &Path) -> io::Result<Self> {
        let lock = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(turns_dir.join("dispatch.lock"))?;
        let deadline = Instant::now() + TERM_GRACE + KILL_GRACE + Duration::from_secs(1);
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(PROBE);
                }
                Err(e) => return Err(io::Error::from(e)),
            }
        }
        Ok(Self {
            _lock: lock,
            path: turns_dir.join("dispatch-owner.json"),
            namespace: Namespace {
                boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")?,
                pid_ns: fs::metadata("/proc/self/ns/pid")?.ino(),
            },
        })
    }

    pub(super) fn retire_prior(&self) -> io::Result<()> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let owner: Owner = serde_json::from_slice(&bytes)?;
        // The state volume can be remounted in a fresh sandbox. A record
        // from another boot or PID namespace has no local signalling authority.
        if owner.namespace == self.namespace {
            owner.retire()?;
        }
        fs::remove_file(&self.path)
    }

    pub(super) fn record(&self, pid: u32, token: String) -> io::Result<()> {
        let pid = i32::try_from(pid).map_err(io::Error::other)?;
        let proc = Proc::read(pid)?;
        if proc.session != pid || !proc.live() {
            return Err(io::Error::other(
                "detached worker did not start its own session",
            ));
        }
        let owner = Owner {
            pid,
            start: proc.start,
            namespace: Namespace {
                boot_id: self.namespace.boot_id.clone(),
                pid_ns: self.namespace.pid_ns,
            },
            token,
        };
        anyagents_harness::atomic_write_file(&self.path, &serde_json::to_vec(&owner)?, 0o600)
    }
}

/// The worker cannot launch anything until its session is recorded. EOF
/// (e.g. the launcher died before recording it) must not start the turn.
pub(super) fn wait_for_dispatch() -> io::Result<()> {
    let mut ready = [0];
    io::stdin().read_exact(&mut ready)?;
    if ready != [1] {
        return Err(io::Error::other("detached worker was not admitted"));
    }
    Ok(())
}

/// The launcher's kernel identity is unique for concurrent dispatches in
/// this namespace. Descendants inherit this marker; it proves an orphaned
/// session wasn't replaced by an unrelated session reusing the old SID.
pub(super) fn launch_token() -> io::Result<String> {
    let pid = std::process::id().cast_signed();
    Ok(format!("{pid}:{}", Proc::read(pid)?.start))
}

struct Proc {
    state: u8,
    session: i32,
    start: u64,
}

impl Proc {
    fn read(pid: i32) -> io::Result<Self> {
        let raw = fs::read(format!("/proc/{pid}/stat"))?;
        // The executable's comm can contain spaces, parentheses and non-UTF8.
        let invalid = || io::Error::other("invalid /proc stat");
        let end = raw.iter().rposition(|&b| b == b')').ok_or_else(invalid)?;
        let tail = std::str::from_utf8(&raw[end + 1..]).map_err(io::Error::other)?;
        let mut fields = tail.split_ascii_whitespace();
        Ok(Self {
            state: fields.next().ok_or_else(invalid)?.as_bytes()[0],
            session: fields
                .nth(2)
                .and_then(|s| s.parse().ok())
                .ok_or_else(invalid)?,
            start: fields
                .nth(15)
                .and_then(|s| s.parse().ok())
                .ok_or_else(invalid)?,
        })
    }

    fn live(&self) -> bool {
        !matches!(self.state, b'Z' | b'X')
    }
}

impl Owner {
    fn retire(&self) -> io::Result<()> {
        if self.pid <= 1 || self.token.is_empty() {
            return Err(io::Error::other("invalid detached worker identity"));
        }
        let marker = format!("{OWNER_ENV}={}", self.token);
        let mut signal = libc::SIGTERM;
        let mut deadline = Instant::now() + TERM_GRACE;
        loop {
            let members = self.members(marker.as_bytes())?;
            if members.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                if signal == libc::SIGKILL {
                    break;
                }
                signal = libc::SIGKILL;
                deadline = Instant::now() + KILL_GRACE;
            }
            for member in members {
                send_signal(&member, signal)?;
            }
            std::thread::sleep(PROBE);
        }
        Err(io::Error::other(
            "prior detached turn did not exit after SIGKILL",
        ))
    }

    fn members(&self, marker: &[u8]) -> io::Result<Vec<OwnedFd>> {
        match Proc::read(self.pid) {
            // The SID is no longer ours if the kernel has reused its number.
            Ok(proc) if proc.start != self.start || proc.session != self.pid => return Ok(vec![]),
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        let mut members = Vec::new();
        for dir in fs::read_dir("/proc")? {
            let Some(pid) = dir?
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<i32>().ok())
            else {
                continue;
            };
            let found =
                |proc: &Proc| proc.session == self.pid && proc.start >= self.start && proc.live();
            match Proc::read(pid) {
                Ok(proc) if found(&proc) => (),
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
            // Pin the signalling target, then recheck membership. Never
            // send a numeric-PID signal based on a /proc snapshot.
            // SAFETY: pidfd_open accepts scalar arguments and returns an owned fd.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) };
            if fd < 0 {
                let e = io::Error::last_os_error();
                if e.raw_os_error() == Some(libc::ESRCH) {
                    continue;
                }
                return Err(e);
            }
            // SAFETY: a successful syscall above returned a new descriptor.
            let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
            match Proc::read(pid) {
                Ok(proc) if found(&proc) && (pid != self.pid || proc.start == self.start) => (),
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
            let env = fs::read(format!("/proc/{pid}/environ"));
            if has_exited(&fd)? {
                continue;
            }
            if !env?.split(|&b| b == 0).any(|entry| entry == marker) {
                return Err(io::Error::other(
                    "cannot identify prior detached turn session member",
                ));
            }
            members.push(fd);
        }
        Ok(members)
    }
}

fn has_exited(fd: &OwnedFd) -> io::Result<bool> {
    let mut pollfd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: poll reads a single initialized pollfd; timeout is zero.
        match unsafe { libc::poll(&mut pollfd, 1, 0) } {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {
                let err = io::Error::last_os_error();
                if err.kind() != io::ErrorKind::Interrupted {
                    return Err(err);
                }
            }
        }
    }
}

fn send_signal(fd: &OwnedFd, sig: i32) -> io::Result<()> {
    // SAFETY: the fd is owned here, siginfo is null, and flags are zero.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            sig,
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let e = io::Error::last_os_error();
    if e.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(e)
    }
}
