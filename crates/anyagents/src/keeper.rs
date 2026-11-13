//! Keepers: the processes that hold each harness so the kernel can stop,
//! crash or be replaced by a newer binary without ending any agent's work
//! (`docs/adr/0007-process-architecture.md`).
//!
//! - `anyagents keeper --dir <dir>` is a keeper; [`run`] starts the vendor
//!   adapter its launch names.
//! - [`KeeperCustody`] is the supervisor's view: take over the keepers a
//!   previous kernel left, end the ones whose agent no longer runs, leave
//!   them all to the next kernel on shutdown.
//! - `anyagents keepers list|stop` is the operator's.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyagents_core::{AgentId, AgentLock, Store};
use anyagents_supervisor::{Attached, Custody};

use crate::vendor::{AdapterLaunch, HarnessHost, VendorHarness};

/// The subcommand that runs a keeper. It is dispatched before argument
/// parsing so a keeper does nothing but keep.
pub const SUBCOMMAND: &str = "keeper";

/// Run the keeper when `argv` asks for one; `None` otherwise.
pub fn try_run(argv: &[String]) -> Option<i32> {
    if argv.get(1).map(String::as_str) != Some(SUBCOMMAND) {
        return None;
    }
    let options = anyagents_keeper::KeeperOptions {
        binary: format!("anyagents {}", env!("CARGO_PKG_VERSION")),
        ..anyagents_keeper::KeeperOptions::default()
    };
    Some(anyagents_keeper::keeper_main(&argv[2..], options, |spec| {
        let launch: AdapterLaunch = serde_json::from_value(spec.launch.clone())
            .map_err(|error| format!("not a launch this keeper can start: {error}"))?;
        launch.start()
    }))
}

/// How `anyagents serve` hosts adapters: in keepers run from the binary it
/// was started from. The path is read once, so a binary replaced on disk
/// during an upgrade serves this kernel's later launches from the new file,
/// which speaks this kernel's protocol version.
pub fn host() -> Result<HarnessHost, String> {
    let path = crate::data::current_exe_clean()
        .map_err(|error| format!("cannot find this binary to run keepers: {error}"))?;
    Ok(HarnessHost::Keeper(anyagents_keeper::Program {
        path,
        args: vec![SUBCOMMAND.to_string()],
    }))
}

/// Harness processes in keepers, which outlive the kernel.
pub struct KeeperCustody {
    root: PathBuf,
}

impl KeeperCustody {
    pub fn new(data: &Path) -> Self {
        Self {
            root: anyagents_keeper::root(data),
        }
    }
}

impl Custody<VendorHarness> for KeeperCustody {
    fn outlives_kernel(&self) -> bool {
        true
    }

    fn retained(&self) -> Vec<AgentId> {
        let agents: std::collections::BTreeSet<String> = anyagents_keeper::instances(&self.root)
            .into_iter()
            .map(|instance| instance.agent)
            .collect();
        agents
            .iter()
            .filter_map(|agent| AgentId::parse(agent).ok())
            .collect()
    }

    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<VendorHarness>>, String> {
        let Some(mut remote) = anyagents_keeper::attach(&self.root, agent.as_str())? else {
            return Ok(None);
        };
        let Some(lock) = remote.take_lock() else {
            // A keeper that holds no lock cannot be held to the one-process
            // rule: end it, and let the worker start one under the lock.
            remote.stop();
            return Ok(None);
        };
        let hello = remote.hello().clone();
        Ok(Some(Attached {
            harness: VendorHarness::kept(remote),
            lock: AgentLock::adopt(lock),
            harness_name: hello.harness,
            model: hello.model,
            started_at_ms: Some(hello.started_at_ms),
            note: hello.note,
        }))
    }

    fn release(&self, agent: &AgentId) {
        anyagents_keeper::stop_agent(&self.root, agent.as_str());
    }

    fn retain(&self, harness: VendorHarness) {
        harness.detach();
    }

    fn note(&self, harness: &VendorHarness, note: &str) {
        harness.set_note(note);
    }
}

/// The custody `serve` gives its supervisor for `host`.
pub fn custody(data: &Path, host: &HarnessHost) -> Arc<dyn Custody<VendorHarness>> {
    match host {
        HarnessHost::Keeper(_) => Arc::new(KeeperCustody::new(data)),
        HarnessHost::InProcess => Arc::new(anyagents_supervisor::InProcess),
    }
}

/// `anyagents keepers list`: every keeper, what it runs and which binary.
pub fn list(data: &Path) -> Result<(), anyagents_core::Error> {
    let root = anyagents_keeper::root(data);
    println!(
        "agent\tkeeper_pid\tharness_pid\tharness\tmodel\tprotocol\tbinary\tstarted_at_ms\tnote"
    );
    for instance in anyagents_keeper::instances(&root) {
        match anyagents_keeper::inspect(&instance.dir) {
            Ok(hello) => println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                hello.agent,
                hello.keeper_pid,
                hello
                    .harness_pid
                    .map_or_else(|| "-".to_string(), |pid| pid.to_string()),
                hello.harness,
                hello.model.as_deref().unwrap_or("-"),
                hello.protocol,
                hello.binary,
                hello.started_at_ms,
                if hello.health.process_alive {
                    "alive"
                } else {
                    "harness exited"
                },
            ),
            Err(reason) => println!(
                "{}\t-\t-\t-\t-\t-\t-\t-\t{reason}: {}",
                instance.agent,
                instance.dir.display()
            ),
        }
    }
    Ok(())
}

/// `anyagents keepers stop <agent>|--all`: end keepers and their harnesses.
/// A running kernel restarts a running agent's harness in a new keeper; this
/// is for a rollback to a binary without keepers, or a full teardown.
pub fn stop(data: &Path, agent: Option<&str>) -> Result<(), anyagents_core::Error> {
    let root = anyagents_keeper::root(data);
    let agents: Vec<String> = match agent {
        Some(name) => vec![resolve_agent(data, name)?],
        None => anyagents_keeper::instances(&root)
            .into_iter()
            .map(|instance| instance.agent)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    for agent in agents {
        anyagents_keeper::stop_agent(&root, &agent);
        println!("{agent}\tstopped");
    }
    Ok(())
}

/// An agent id as written, or the id of the agent with that name.
fn resolve_agent(data: &Path, name: &str) -> Result<String, anyagents_core::Error> {
    if AgentId::parse(name).is_ok() {
        return Ok(name.to_string());
    }
    let store = Store::open(data.join("state.db"))?;
    Ok(store.agent_by_name(name)?.id.as_str().to_string())
}
