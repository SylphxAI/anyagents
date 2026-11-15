//! Local Spiron without a keeper (`docs/adr/0011-spiron-headless-v2.md`, D9).
//!
//! A Spiron that speaks `spiron.headless/v2` runs on its own, listening on
//! `<config dir>/spiron.sock`, and outlives the server. One launch record per
//! agent, `<data>/detached/<agent>.json`, names the process (its identity, so a
//! reused pid is never signalled), its socket, and the seq a new server
//! replays from. The server writes it once per launch, in a directory only
//! the server can write: it names a process the server may signal, so an
//! agent must not be able to change it.
//!
//! After a restart the custody ([`crate::keeper::KeeperCustody`]) takes the
//! agent's lock, attaches with that seq, and the replay rebuilds the adapter
//! as the last server had it. A process whose identity no longer holds, or
//! that will not attach, is ended, and the worker starts a fresh one; the
//! mailbox offers it everything past the cursor again, and Spiron
//! deduplicates by request id.

use std::collections::BTreeMap;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_harness_spiron::{ListenError, Reattach, SpironHarness, SpironLaunch};
use anyagents_keeper::{Presence, ProcessIdentity};
use serde::{Deserialize, Serialize};

/// How long an attach may take: the replay of one process's frames.
const ATTACH_WAIT: Duration = Duration::from_secs(60);

/// The longest Unix socket path (`sun_path` less its terminator).
const SOCKET_PATH_MAX: usize = 107;

/// One launch of a v2 Spiron, as the server that launched it recorded it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    pub agent: String,
    /// What the process was launched for; the agent's row may have changed.
    pub harness: String,
    pub model: Option<String>,
    pub program: PathBuf,
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    pub process: ProcessIdentity,
    /// The OS seat it runs in (`ANYAGENTS_SEAT_LAUNCH`): it is signalled as
    /// that seat.
    pub seat: Option<String>,
    pub session: Option<String>,
    /// The seq before the launch's first `ready`.
    pub after_seq: u64,
    pub started_at_ms: i64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// What the last server kept with the process (`Custody::note`).
    #[serde(default)]
    pub note: Option<String>,
}

impl Record {
    fn environment(&self) -> Vec<(String, String)> {
        self.seat
            .iter()
            .map(|seat| (anyagents_harness::SEAT_LAUNCH_ENV.to_string(), seat.clone()))
            .collect()
    }
}

fn root(data: &Path) -> PathBuf {
    // Generic, not Spiron's: the release guardian, which must not name
    // products, finds detached harness processes here.
    data.join("detached")
}

pub(crate) fn record_path(data: &Path, agent: &str) -> PathBuf {
    root(data).join(format!("{agent}.json"))
}

fn read(path: &Path) -> Option<Record> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn write(path: &Path, record: &Record) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt as _;
    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    }
    let bytes = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    anyagents_harness::atomic_write_file(path, &bytes, 0o600)
        .map_err(|error| format!("cannot write {}: {error}", path.display()))
}

/// Agents with a launch record: processes an earlier server may have left.
pub(crate) fn agents(data: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root(data)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()?
                .strip_suffix(".json")
                .map(str::to_string)
        })
        .collect()
}

/// End the process the agent's launch record names, when its identity
/// proves it is that process, and remove the record.
pub(crate) fn end_recorded(data: &Path, agent: &str) {
    let path = record_path(data, agent);
    if let Some(record) = read(&path) {
        record.process.end_group(&record.environment());
    }
    let _ = std::fs::remove_file(&path);
}

/// Keep `note` in the launch record at `path` for the next server.
pub(crate) fn set_note(path: &Path, note: &str) -> Result<(), String> {
    let mut record = read(path).ok_or_else(|| format!("no launch record at {}", path.display()))?;
    if record.note.as_deref() == Some(note) {
        return Ok(());
    }
    record.note = Some(note.to_string());
    write(path, &record)
}

/// What a launch needs beyond Spiron's own arguments.
pub(crate) struct Launching<'a> {
    pub data: &'a Path,
    pub agent: &'a str,
    pub harness: &'a str,
    pub model: Option<&'a str>,
    pub labels: BTreeMap<String, String>,
}

/// Launch a v2 Spiron that listens on `<config dir>/spiron.sock` and
/// outlives this server, attach, and record the launch.
pub(crate) fn launch(
    spiron: SpironLaunch,
    applied: Vec<String>,
    at: Launching<'_>,
    patience: Duration,
) -> Result<SpironHarness, ListenError> {
    // The agent's config directory: the server and the agent's seat reach it.
    let socket = spiron.state_dir.join("spiron.sock");
    if socket.as_os_str().len() > SOCKET_PATH_MAX {
        return Err(ListenError::Failed(format!(
            "Unix socket path for {} exceeds {SOCKET_PATH_MAX} bytes: {}",
            at.agent,
            socket.display()
        )));
    }
    let seat = spiron
        .environment
        .iter()
        .rev()
        .find(|(key, _)| key == anyagents_harness::SEAT_LAUNCH_ENV)
        .map(|(_, spec)| spec.clone());
    let path = record_path(at.data, at.agent);
    let mut record = Record {
        agent: at.agent.to_string(),
        harness: at.harness.to_string(),
        model: at.model.map(str::to_string),
        program: spiron.program.clone(),
        socket: socket.clone(),
        state_dir: spiron.state_dir.clone(),
        process: ProcessIdentity {
            pid: 0,
            start_ticks: 0,
            boot_id: String::new(),
            pid_ns: 0,
        },
        seat,
        session: spiron.session.clone(),
        // Until the launch's `ready` is known, a replay starts at the start.
        after_seq: 0,
        started_at_ms: now_ms(),
        labels: at.labels,
        note: None,
    };
    let log = spiron.state_dir.join("spiron.log");
    // The process is recorded the moment it exists: a server that dies
    // before the record is complete still leaves it to be found.
    let mut harness = SpironHarness::launch_listening(spiron, &socket, &log, patience, |pid| {
        record.process = ProcessIdentity::of(pid)
            .ok_or_else(|| "cannot prove the Spiron process's identity".to_string())?;
        write(&path, &record)
    })
    .inspect_err(|_| {
        let _ = std::fs::remove_file(&path);
    })?;
    // Named now: from here a failure ends the process with the harness and
    // removes the record.
    harness.set_record(path.clone());
    harness.recognise_applied(applied);
    record.session = harness.session_id();
    record.after_seq = harness.attach_after_seq().unwrap_or(0);
    write(&path, &record).map_err(ListenError::Failed)?;
    Ok(harness)
}

/// Whether the agent's launch record names a process that still runs and
/// listens. A record that does not is cleared (its process ended).
pub(crate) fn alive(data: &Path, agent: &str) -> bool {
    let path = record_path(data, agent);
    let Some(record) = read(&path) else {
        let _ = std::fs::remove_file(&path);
        return false;
    };
    if record.process.presence() == Presence::Running && record.socket.exists() {
        return true;
    }
    end_recorded(data, agent);
    false
}

/// Attach to the process an earlier server left for `agent`. `None` when
/// there is none, or it is gone, or it will not attach (it is then ended,
/// and its record removed).
pub(crate) fn reattach(data: &Path, agent: &str) -> Option<(SpironHarness, Record)> {
    let path = record_path(data, agent);
    let record = read(&path)?;
    let environment = record.environment();
    if record.process.presence() != Presence::Running || !record.socket.exists() {
        end_recorded(data, agent);
        return None;
    }
    let identity = record.process.clone();
    let end_environment = environment.clone();
    let attached = SpironHarness::reattach(
        Reattach {
            program: record.program.clone(),
            socket: record.socket.clone(),
            pid: record.process.pid,
            after_seq: record.after_seq,
            session: record.session.clone(),
            environment,
            state_dir: record.state_dir.clone(),
            end: Box::new(move || {
                identity.end_group(&end_environment);
            }),
            record: path,
        },
        ATTACH_WAIT,
    );
    match attached {
        Ok(harness) => Some((harness, record)),
        Err(error) => {
            // A connection that failed before its harness existed ended
            // nothing: the process is ended here.
            eprintln!("anyagents-spiron: {agent}: cannot attach again ({error}); it is replaced");
            end_recorded(data, agent);
            None
        }
    }
}

/// Take the agent's process lock, which no process holds while no server
/// runs: a running Spiron does not hold it.
pub(crate) fn take_lock(
    data: &Path,
    agent: &anyagents_core::AgentId,
) -> Result<anyagents_core::AgentLock, String> {
    let path = anyagents_core::agent_lock_path(data, agent);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    // SAFETY: flock on a descriptor this function owns.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(format!("agent {agent} is busy"));
    }
    Ok(anyagents_core::AgentLock::adopt(file.into()))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis() as i64)
}
