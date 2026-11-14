//! Process custody that outlives the AnyAgents kernel.
//!
//! A keeper is one small process per running harness. It starts the harness,
//! owns its process group, its pipes and the adapter that speaks the vendor's
//! protocol over them, and holds a copy of the agent's process lock. The
//! kernel (`anyagents serve`) drives the adapter through the keeper's Unix
//! socket. When the kernel stops, crashes or is replaced by a newer binary,
//! the harness keeps running: its stdin stays open, its stdout is still read,
//! the adapter keeps every id it was given. (Its tool calls see the server's
//! socket gone for that moment, and wait for it.) The next kernel
//! connects, takes over, and resumes delivery from the mailbox cursor; a
//! message the old kernel handed over is answered from the adapter's memory
//! instead of being written twice.
//!
//! The design and its reasons are in `docs/adr/0007-process-architecture.md`.
//!
//! - [`run_keeper`] is the keeper process: `anyagents keeper --dir <dir>`.
//! - [`spawn`] starts one for a launch, [`attach`] takes over one a previous
//!   kernel left, and [`RemoteHarness`] is the kernel's [`Harness`] over it.
//!
//! Every [`Harness`] method is an op, named as the method, answered by the
//! keeper's dispatch and forwarded by [`RemoteHarness`]. A method added to the
//! contract must be added to both; until it is, a kept adapter answers the
//! contract's default for it.

mod identity;
mod keeper;
mod remote;
pub mod wire;

use std::path::{Path, PathBuf};

use anyagents_harness::{Harness, HarnessHealth};
use serde::{Deserialize, Serialize};

pub use identity::{Presence, ProcessIdentity};
pub use keeper::{KeeperOptions, keeper_main, run_keeper};
pub use remote::{
    ABANDONED_START, CALL_DEADLINE, HELLO_DEADLINE, Instance, Program, READY_DEADLINE,
    RemoteHarness, STOP_DEADLINE, abandon_starts, attach, attach_all, check_root, inspect,
    instances, lock_path, spawn, stop_agent, stop_agent_with, testing,
};

/// The protocol version this build speaks. A kernel and a keeper agree on the
/// highest version both speak. Ops are added without a version change: a
/// keeper that does not know an op answers `unknown_op`, and the kernel uses
/// the adapter contract's default for it. A change to an existing op's meaning
/// or to a carried type's shape is a new version, and a binary claims only the
/// versions whose shapes it can still write: a kernel that cannot read a
/// keeper's answers stops and replaces it rather than retrying.
///
/// - 1: the first keepers (never released).
/// - 2: health carries the harness's delivery level (`delivery`) in place of
///   its delivery mode and steer support; `deliver` may answer `rejected`;
///   `proven` and `stop_harness` (the contract's `stop`) are added; the
///   between-steps steering and unconfirmed-hold ops are gone.
pub const PROTOCOL: u32 = 2;
/// The oldest protocol version this build still speaks: version 1's health
/// has no shape here.
pub const PROTOCOL_MIN: u32 = 2;

/// The environment variable that pins the protocol versions this binary
/// speaks, as `<min>-<max>`, on both sides. It exists to run a kernel and its
/// keepers as two binaries of different versions would, and is read nowhere
/// else.
pub const PROTOCOL_ENV: &str = "ANYAGENTS_KEEPER_PROTOCOL";

/// The protocol versions this binary speaks: [`PROTOCOL_MIN`]..=[`PROTOCOL`],
/// unless [`PROTOCOL_ENV`] pins another range.
pub fn protocol() -> (u32, u32) {
    std::env::var(PROTOCOL_ENV)
        .ok()
        .and_then(|range| {
            let (min, max) = range.split_once('-')?;
            let (min, max) = (min.trim().parse().ok()?, max.trim().parse().ok()?);
            (min <= max).then_some((min, max))
        })
        .unwrap_or((PROTOCOL_MIN, PROTOCOL))
}

/// Name of the socket in a keeper's directory. Short, as the directory is:
/// a socket path must fit in 104 bytes on every supported platform, and the
/// data directory comes first.
pub const SOCKET: &str = "s";
/// The agent a keeper serves, written by the kernel before the keeper starts,
/// so a keeper that dies early is still attributed.
pub const AGENT: &str = "agent";
/// The path of the agent's process lock, written by the kernel beside
/// [`AGENT`]: a live keeper always holds that lock, so a lock nobody holds
/// proves its keeper gone.
pub const LOCK: &str = "lock";
/// What a keeper records about itself, for an operator and for `list`.
pub const RECORD: &str = "keeper.json";
/// The process groups the keeper started (each by the identity of its
/// leader), written as each starts, before it runs anything: what a keeper
/// that died left is ended by this record alone.
pub const GROUPS: &str = "groups";
/// The keeper process's identity, written by the kernel right after it
/// starts it: a keeper that dies before writing its own record is still
/// proven gone, and its directory never blocks the agent.
pub const SPAWNED: &str = "spawned";
/// The keeper's own diagnostics. Never harness output.
pub const LOG: &str = "keeper.log";

/// The longest socket path every supported platform binds (macOS: 104).
pub const MAX_SOCKET_PATH: usize = 103;

/// What a hosted adapter answers besides the [`Harness`] contract: the facts
/// the kernel records about the vendor session it runs.
pub trait Hosted: Harness {
    /// The vendor session the process runs, once the vendor named it.
    fn session_id(&self) -> Option<String> {
        None
    }

    /// The vendor dropped the session it was resumed on.
    fn session_invalidated(&self) -> bool {
        false
    }
}

/// What the kernel asks a keeper to run. `launch` is the adapter's own launch
/// description; only the keeper's adapter factory reads it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Spec {
    /// The agent the process serves.
    pub agent: String,
    /// The harness and model the process was launched for. A kernel that
    /// takes the keeper over compares them with the agent's configuration.
    pub harness: String,
    pub model: Option<String>,
    /// The OS seat the harness runs in (`ANYAGENTS_SEAT_LAUNCH`), so a kernel
    /// can end the harness group as that seat if the keeper dies.
    pub seat: Option<String>,
    /// What the kernel wants back when it takes the keeper over, as written.
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    /// The agent's process lock, whose copy the keeper holds.
    #[serde(default)]
    pub lock_path: Option<PathBuf>,
    pub launch: serde_json::Value,
}

/// What a keeper says about itself when a kernel connects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    /// The protocol version this connection speaks.
    pub protocol: u32,
    /// The binary the keeper runs, for an operator.
    pub binary: String,
    pub keeper_pid: u32,
    /// The keeper process, when its platform can prove an identity.
    #[serde(default)]
    pub keeper: Option<ProcessIdentity>,
    pub harness_pid: Option<u32>,
    pub agent: String,
    pub harness: String,
    pub model: Option<String>,
    pub seat: Option<String>,
    pub started_at_ms: i64,
    /// The labels the launching kernel gave the keeper.
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    /// What the last kernel asked the keeper to keep for the next one.
    pub note: Option<String>,
    pub health: HarnessHealth,
    /// A copy of the agent's process lock travels with this frame.
    pub lock: bool,
}

/// The directory that holds every keeper of a data directory, one flat
/// directory per keeper named by its start time.
pub fn root(data: &Path) -> PathBuf {
    data.join("keepers")
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
