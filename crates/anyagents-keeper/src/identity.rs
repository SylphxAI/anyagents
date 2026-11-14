//! Who a process is, beyond its pid.
//!
//! A pid is reused once its process is reaped; after a pod restart, a node
//! reboot or a restore from backup, a recorded pid names whatever process now
//! has that number. A pid is therefore only signalled when the process behind
//! it is proven to be the one recorded: the same boot, the same pid namespace
//! and the same start time. On a platform without `/proc` nothing is proven,
//! and nothing recorded is ever signalled.

use serde::{Deserialize, Serialize};

/// One process, as recorded when it was started.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    /// Start time in clock ticks after boot (`/proc/<pid>/stat` field 22).
    pub start_ticks: u64,
    /// `/proc/sys/kernel/random/boot_id` when it was recorded.
    pub boot_id: String,
    /// Inode of the pid namespace its pid is numbered in.
    pub pid_ns: u64,
}

/// What a recorded identity names now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    /// The recorded process still runs.
    Running,
    /// The recorded process ended, in the same boot and namespace: no
    /// process has the pid, or it is the recorded one exited and not yet
    /// reaped. A process group it led can still have members, and they can
    /// only be its own, because a pid is not reused while it numbers a live
    /// group or an unreaped process.
    Ended,
    /// Not provable: another boot or namespace, the pid now belongs to
    /// another process, or the platform cannot tell. Nothing may be signalled.
    Unknown,
}

impl ProcessIdentity {
    /// The identity of the live process `pid`, when this platform can prove
    /// one.
    pub fn of(pid: u32) -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            Some(Self {
                pid,
                start_ticks: linux::state_and_start(pid)?.1,
                boot_id: linux::boot_id()?,
                pid_ns: linux::pid_ns()?,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            None
        }
    }

    /// What this record names now.
    pub fn presence(&self) -> Presence {
        #[cfg(target_os = "linux")]
        {
            if linux::boot_id().as_deref() != Some(self.boot_id.as_str())
                || linux::pid_ns() != Some(self.pid_ns)
            {
                return Presence::Unknown;
            }
            match linux::state_and_start(self.pid) {
                Some(('Z', ticks)) if ticks == self.start_ticks => Presence::Ended,
                Some((_, ticks)) if ticks == self.start_ticks => Presence::Running,
                Some(_) => Presence::Unknown,
                None if linux::exists(self.pid) => Presence::Unknown,
                None => Presence::Ended,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Presence::Unknown
        }
    }

    /// End the process group this process led, when the record proves the
    /// group is still its own; `false` when nothing was signalled.
    pub fn end_group(&self, environment: &[(String, String)]) -> bool {
        match self.presence() {
            Presence::Running | Presence::Ended => {
                anyagents_harness::terminate_group(self.pid, environment);
                true
            }
            Presence::Unknown => false,
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::unix::fs::MetadataExt as _;

    /// Fields 3 (state) and 22 (start time) of `/proc/<pid>/stat`. The
    /// command name (field 2) can hold spaces and parentheses, so fields are
    /// counted after its last `)`.
    pub(super) fn state_and_start(pid: u32) -> Option<(char, u64)> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after = stat.get(stat.rfind(')')? + 1..)?;
        let mut fields = after.split_whitespace();
        let state = fields.next()?.chars().next()?;
        let start = fields.nth(18)?.parse().ok()?;
        Some((state, start))
    }

    pub(super) fn exists(pid: u32) -> bool {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    }

    pub(super) fn boot_id() -> Option<String> {
        let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        let id = id.trim();
        (!id.is_empty()).then(|| id.to_string())
    }

    pub(super) fn pid_ns() -> Option<u64> {
        std::fs::metadata("/proc/self/ns/pid")
            .ok()
            .map(|meta| meta.ino())
    }

    /// The session id of `pid` (field 6) and its process group (field 5).
    pub(super) fn session_and_group(pid: u32) -> Option<(u32, u32)> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after = stat.get(stat.rfind(')')? + 1..)?;
        let mut fields = after.split_whitespace();
        let group = fields.nth(2)?.parse().ok()?;
        let session = fields.next()?.parse().ok()?;
        Some((session, group))
    }
}

/// The process groups of the session `keeper` led, counting only members
/// whose session id is the keeper's pid and that started no earlier than
/// the keeper did, in this boot and namespace. For a keeper whose identity
/// is proven ended: no process has its pid now, and a pid is not reused
/// while a session it numbers has members, so the members found are the
/// keeper's own; a member older than the keeper cannot be, and is never
/// counted. Empty where this cannot be read.
pub(crate) fn session_groups_since(keeper: &ProcessIdentity) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        if linux::boot_id().as_deref() != Some(keeper.boot_id.as_str())
            || linux::pid_ns() != Some(keeper.pid_ns)
        {
            return Vec::new();
        }
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return Vec::new();
        };
        let mut groups: Vec<u32> = entries
            .flatten()
            .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
            .filter_map(|pid| {
                let (session, group) = linux::session_and_group(pid)?;
                let (_, started) = linux::state_and_start(pid)?;
                (session == keeper.pid && started >= keeper.start_ticks).then_some(group)
            })
            .collect();
        groups.sort_unstable();
        groups.dedup();
        groups
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = keeper;
        Vec::new()
    }
}

/// Whether `pid` leads its own session, as every keeper does (it calls
/// `setsid` before it runs anything). `false` where this cannot be read.
pub(crate) fn leads_session(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::session_and_group(pid).is_some_and(|(session, _)| session == pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

/// The process groups of every process in the session `leader` leads.
/// Asked only about a keeper this process started and has not reaped, so its
/// pid, and with it the session id, cannot have been reused.
pub(crate) fn session_groups(leader: u32) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return Vec::new();
        };
        let mut groups: Vec<u32> = entries
            .flatten()
            .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
            .filter_map(linux::session_and_group)
            .filter(|(session, _)| *session == leader)
            .map(|(_, group)| group)
            .collect();
        groups.sort_unstable();
        groups.dedup();
        groups
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = leader;
        Vec::new()
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn a_live_process_is_itself_and_a_reused_pid_is_not() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let identity = ProcessIdentity::of(child.id()).unwrap();
        assert_eq!(identity.presence(), Presence::Running);
        let older = ProcessIdentity {
            start_ticks: identity.start_ticks.saturating_sub(1),
            ..identity.clone()
        };
        assert_eq!(older.presence(), Presence::Unknown, "another start time");
        let elsewhere = ProcessIdentity {
            boot_id: "another-boot".into(),
            ..identity.clone()
        };
        assert_eq!(elsewhere.presence(), Presence::Unknown, "another boot");
        let other_namespace = ProcessIdentity {
            pid_ns: identity.pid_ns.wrapping_add(1),
            ..identity.clone()
        };
        assert_eq!(other_namespace.presence(), Presence::Unknown);
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(identity.presence(), Presence::Ended);
        assert!(
            !elsewhere.end_group(&[]),
            "an unproven record was signalled"
        );
    }
}
