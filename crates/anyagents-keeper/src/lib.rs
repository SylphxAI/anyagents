//! Process custody that outlives the AnyAgents kernel.
//!
//! A keeper is one small process per running harness. It starts the harness,
//! owns its process group, its pipes and the adapter that speaks the vendor's
//! protocol over them, and holds a copy of the agent's process lock. The
//! kernel (`anyagents serve`) drives the adapter through the keeper's Unix
//! socket. When the kernel stops, crashes or is replaced by a newer binary,
//! nothing the harness can see changes: its stdin stays open, its stdout is
//! still read, the adapter keeps every id it was given. The next kernel
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

mod keeper;
mod remote;
pub mod wire;

use std::path::{Path, PathBuf};

use anyagents_harness::{Harness, HarnessHealth};
use serde::{Deserialize, Serialize};

pub use keeper::{KeeperOptions, keeper_main, run_keeper};
pub use remote::{
    Instance, Program, RemoteHarness, attach, inspect, instances, spawn, stop_agent, testing,
};

/// The protocol version this build speaks. A kernel and a keeper agree on the
/// highest version both speak. Ops are added without a version change: a
/// keeper that does not know an op answers `unknown_op`, and the kernel uses
/// the adapter contract's default for it. A change to an existing op's meaning
/// or to a carried type's shape is a new version, and a kernel keeps speaking
/// the previous one for at least one release so running keepers stay
/// reachable across an upgrade.
pub const PROTOCOL: u32 = 1;
/// The oldest protocol version this build still speaks.
pub const PROTOCOL_MIN: u32 = 1;

/// Name of the socket in a keeper's directory. Short, as the directory is:
/// a socket path must fit in 104 bytes on every supported platform, and the
/// data directory comes first.
pub const SOCKET: &str = "s";
/// The agent a keeper serves, written by the kernel before the keeper starts,
/// so a keeper that dies early is still attributed.
pub const AGENT: &str = "agent";
/// What a keeper records about itself, for an operator and for `list`.
pub const RECORD: &str = "keeper.json";
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

    /// Where the adapter keeps its custody files for this agent.
    fn state_dir(&self) -> Option<PathBuf> {
        None
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
