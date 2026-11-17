//! Keepers: the processes that hold each harness so the kernel can stop,
//! crash or be replaced by a newer binary without ending any agent's work
//! (`docs/adr/0007-process-architecture.md`).
//!
//! - `anyagents keeper --dir <dir>` is a keeper; [`try_run`] starts the
//!   vendor adapter its launch names.
//! - [`KeeperCustody`] is the supervisor's view: take over the keepers a
//!   previous kernel left, end the ones whose agent no longer runs, leave
//!   them all to the next kernel on shutdown.
//! - `anyagents keepers list|stop` is the operator's.
//!
//! Keepers are used on Linux only ([`HOSTED_IN_KEEPERS`]). Taking a keeper
//! over after a kernel restart, and every liveness and signal decision about
//! one, rest on proving a process's identity (start time, boot id and pid
//! namespace from `/proc`). A platform without that proof could never take a
//! keeper over safely, so there `serve` hosts harnesses in its own process.

use std::collections::BTreeSet;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyagents_core::{AgentId, AgentLock, Store};
use anyagents_supervisor::{Attached, Custody};

use crate::ui;
use crate::vendor::{AdapterLaunch, HarnessHost, VendorHarness};

/// Whether `serve` hosts harnesses in keepers on this platform: only where a
/// process's identity can be proven (Linux).
pub const HOSTED_IN_KEEPERS: bool = cfg!(target_os = "linux");

/// What `keepers list|stop` says where keepers are not used.
const NOT_USED: &str = "keepers are not used on this platform: \
     the server hosts each harness in its own process";

/// The subcommand that runs a keeper. It is dispatched before argument
/// parsing so a keeper does nothing but keep.
pub const SUBCOMMAND: &str = "keeper";

/// What a keeper says it runs: this build's version and source commit.
fn binary() -> String {
    format!(
        "anyagents {} {}",
        env!("CARGO_PKG_VERSION"),
        env!("ANYAGENTS_SOURCE_SHA")
    )
}

/// Run the keeper when `argv` asks for one; `None` otherwise.
pub fn try_run(argv: &[String]) -> Option<i32> {
    if argv.get(1).map(String::as_str) != Some(SUBCOMMAND) {
        return None;
    }
    let options = anyagents_keeper::KeeperOptions {
        binary: binary(),
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
/// which speaks this kernel's protocol version. Where keepers are not used,
/// in the server's own process.
pub fn host() -> Result<HarnessHost, String> {
    if !HOSTED_IN_KEEPERS {
        return Ok(HarnessHost::InProcess);
    }
    let path = crate::data::current_exe_clean()
        .map_err(|error| format!("cannot find this binary to run keepers: {error}"))?;
    Ok(HarnessHost::Keeper(anyagents_keeper::Program::new(
        path,
        vec![SUBCOMMAND.to_string()],
    )))
}

/// Harness processes that outlive the kernel: in keepers, and v2 Spiron
/// processes, which need none (`crate::spiron_native`).
pub struct KeeperCustody {
    data: PathBuf,
    root: PathBuf,
    /// Agents whose kept harnesses must not be taken over: they would send
    /// their gateway token to a port this kernel no longer holds.
    refused: std::collections::HashSet<AgentId>,
}

impl KeeperCustody {
    pub fn new(data: &Path, refused: impl IntoIterator<Item = AgentId>) -> Self {
        Self {
            data: data.to_path_buf(),
            root: anyagents_keeper::root(data),
            refused: refused.into_iter().collect(),
        }
    }
}

impl Custody<VendorHarness> for KeeperCustody {
    fn outlives_kernel(&self) -> bool {
        true
    }

    fn retained(&self) -> Vec<AgentId> {
        let mut agents: BTreeSet<String> = crate::spiron_native::agents(&self.data)
            .into_iter()
            .collect();
        if let Err(error) = anyagents_keeper::check_root(&self.root) {
            eprintln!("anyagents-keeper: no keeper is taken over: {error}");
        } else {
            agents.extend(
                anyagents_keeper::instances(&self.root)
                    .into_iter()
                    .map(|instance| instance.agent),
            );
        }
        agents
            .iter()
            .filter_map(|agent| AgentId::parse(agent).ok())
            .collect()
    }

    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<VendorHarness>>, String> {
        if self.refused.contains(agent) {
            // Its harness has the gateway's old origin and token: end it, and
            // the worker starts one with the new ones.
            crate::spiron_native::end_recorded(&self.data, agent.as_str());
            anyagents_keeper::stop_agent(&self.root, agent.as_str())?;
            return Ok(None);
        }
        // A v2 Spiron an earlier kernel left: attached again with no keeper.
        // Keepers of the same agent are older processes beside it: v1 ones a
        // server without v2 started (a rollback), or the one a switch to
        // Spiron was replacing. They hold the agent's lock, which is taken
        // from them; the worker retires them (their proofs committed) before
        // it delivers, and the v2 process goes on.
        if crate::spiron_native::alive(&self.data, agent.as_str()) {
            let mut lock = None;
            let mut older = Vec::new();
            match anyagents_keeper::attach_all(&self.root, agent.as_str()) {
                Ok(found) => {
                    for mut remote in found {
                        if lock.is_none()
                            && let Some(fd) = remote.take_lock()
                        {
                            lock = Some(AgentLock::adopt(fd));
                        }
                        older.push(VendorHarness::kept(remote));
                    }
                }
                Err(error) => eprintln!("anyagents-keeper: {agent}: {error}"),
            }
            let lock = match lock {
                Some(lock) => lock,
                None => crate::spiron_native::take_lock(&self.data, agent)?,
            };
            let Some((harness, record)) =
                crate::spiron_native::reattach(&self.data, agent.as_str())
            else {
                // It was ended: so are the keepers, before their lock goes.
                drop(older);
                drop(lock);
                return Ok(None);
            };
            return Ok(Some(Attached {
                harness: VendorHarness::native(harness, &record.labels),
                lock,
                harness_name: record.harness,
                model: record.model,
                started_at_ms: Some(record.started_at_ms),
                note: record.note,
                older,
            }));
        }
        let mut found = anyagents_keeper::attach_all(&self.root, agent.as_str())?.into_iter();
        let Some(mut remote) = found.next() else {
            return Ok(None);
        };
        let older: Vec<VendorHarness> = found.map(VendorHarness::kept).collect();
        let Some(lock) = remote.take_lock() else {
            // A keeper that holds no lock cannot be held to the one-process
            // rule: end it, and let the worker start one under the lock.
            drop(older);
            remote.end()?;
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
            older,
        }))
    }

    fn release(&self, agent: &AgentId) {
        crate::spiron_native::end_recorded(&self.data, agent.as_str());
        if let Err(error) = anyagents_keeper::stop_agent(&self.root, agent.as_str()) {
            eprintln!("anyagents-keeper: {agent}: {error}");
        }
    }

    fn retain(&self, harness: VendorHarness) {
        harness.detach();
    }

    fn note(&self, harness: &VendorHarness, note: &str) {
        harness.set_note(note);
    }

    fn superseded(&self, harness: &VendorHarness) -> bool {
        harness.superseded()
    }

    fn answers_live(&self, harness: &VendorHarness) -> bool {
        harness.answers_live()
    }
}

/// The custody `serve` gives its supervisor for `host`. `refused` names the
/// agents whose kept harnesses must be replaced, not taken over.
pub fn custody(
    data: &Path,
    host: &HarnessHost,
    refused: Vec<AgentId>,
) -> Arc<dyn Custody<VendorHarness>> {
    match host {
        HarnessHost::Keeper(_) => Arc::new(KeeperCustody::new(data, refused)),
        HarnessHost::InProcess => Arc::new(anyagents_supervisor::InProcess),
    }
}

/// `anyagents keepers list`: every keeper, what it runs and which binary.
pub fn list(data: &Path) -> Result<(), anyagents_core::Error> {
    if !HOSTED_IN_KEEPERS {
        eprintln!("{NOT_USED}");
        return Ok(());
    }
    let root = anyagents_keeper::root(data);
    anyagents_keeper::check_root(&root).map_err(anyagents_core::Error::Message)?;
    if ui::is_pretty() {
        ui::heading("Keepers");
    } else {
        println!(
            "agent\tkeeper_pid\tharness_pid\tharness\tmodel\tprotocol\tbinary\tstarted_at_ms\tstate"
        );
    }
    let mut table = ui::Table::new(&[
        ("AGENT", false),
        ("PID", true),
        ("HARNESS", false),
        ("MODEL", false),
        ("STATE", false),
    ]);
    let mut count = 0;
    for instance in anyagents_keeper::instances(&root) {
        count += 1;
        match anyagents_keeper::inspect(&instance.dir) {
            Ok(hello) if ui::is_pretty() => {
                table.add_row(vec![
                    hello.agent.to_string(),
                    hello.keeper_pid.to_string(),
                    hello.harness.clone(),
                    hello.model.clone().unwrap_or_else(|| "—".into()),
                    if hello.health.process_alive {
                        ui::green("● alive")
                    } else {
                        ui::red("✗ harness exited")
                    },
                ]);
                if ui::is_verbose() {
                    ui::note(&format!(
                        "{} · {} · protocol {}",
                        hello.agent, hello.binary, hello.protocol
                    ));
                }
            }
            Err(reason) if ui::is_pretty() => {
                table.add_row(vec![
                    instance.agent.to_string(),
                    "—".into(),
                    "—".into(),
                    "—".into(),
                    ui::red("✗ unavailable"),
                ]);
                if ui::is_verbose() {
                    ui::note(&format!("{reason}: {}", instance.dir.display()));
                }
            }
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
    if ui::is_pretty() {
        if count == 0 {
            ui::note("No keepers.");
        } else {
            table.print();
        }
    }
    Ok(())
}

/// `anyagents keepers stop <agent>|--all`: end keepers and their harnesses,
/// for a rollback to a binary without keepers or a full teardown.
///
/// It refuses unless this user owns the keepers and no server runs on the
/// data directory (a running server would start the harnesses again), keeps
/// the server lock while it works so none starts meanwhile, and succeeds only
/// when every keeper ended and every agent lock it held is free. With `force`
/// a keeper that does not answer while its process runs is ended by signal,
/// once its recorded identity proves the process is that keeper.
pub fn stop(data: &Path, agent: Option<&str>, force: bool) -> Result<(), anyagents_core::Error> {
    if !HOSTED_IN_KEEPERS {
        eprintln!("{NOT_USED}");
        return Ok(());
    }
    let fail = |message: String| anyagents_core::Error::Message(message);
    let root = anyagents_keeper::root(data);
    let meta = match std::fs::symlink_metadata(&root) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let _server = hold_server_lock(data)?;
            if !stop_native(data, agent)? {
                if ui::is_pretty() {
                    ui::note("No keepers.");
                } else {
                    println!("no keepers");
                }
            }
            return Ok(());
        }
        Err(error) => return Err(fail(format!("cannot read {}: {error}", root.display()))),
    };
    // SAFETY: geteuid takes no arguments and cannot fail.
    let me = unsafe { libc::geteuid() };
    if meta.uid() != me {
        return Err(fail(format!(
            "{} belongs to uid {}; run this as that user, not uid {me}",
            root.display(),
            meta.uid()
        )));
    }
    anyagents_keeper::check_root(&root).map_err(fail)?;
    let _server = hold_server_lock(data)?;
    stop_native(data, agent)?;
    let instances = anyagents_keeper::instances(&root);
    let agents: BTreeSet<String> = match agent {
        Some(name) => [resolve_agent(data, name)?].into(),
        None => instances
            .iter()
            .map(|instance| instance.agent.clone())
            .collect(),
    };
    let mut failures = Vec::new();
    for agent in &agents {
        let locks: Vec<PathBuf> = instances
            .iter()
            .filter(|instance| &instance.agent == agent)
            .filter_map(|instance| anyagents_keeper::lock_path(&instance.dir))
            .collect();
        if let Err(error) = anyagents_keeper::stop_agent_with(&root, agent, force) {
            failures.push(format!("{agent}: {error}"));
            if ui::is_pretty() {
                ui::error(&format!("Could not stop {agent}"));
            } else {
                println!("{agent}\tfailed");
            }
            continue;
        }
        if let Some(held) = locks.iter().find(|path| !lock_is_free(path)) {
            failures.push(format!(
                "{agent}: its lock {} is still held",
                held.display()
            ));
            if ui::is_pretty() {
                ui::error(&format!("Could not stop {agent}"));
            } else {
                println!("{agent}\tfailed");
            }
            continue;
        }
        if ui::is_pretty() {
            ui::success(&format!("Stopped {agent}"));
        } else {
            println!("{agent}\tstopped");
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(fail(format!(
            "not every keeper stopped: {}",
            failures.join("; ")
        )))
    }
}

/// End the v2 Spiron processes (`crate::spiron_native`) of `agent`, or of
/// every agent: they need no keeper, but outlive the server all the same.
/// Whether there was any.
fn stop_native(data: &Path, agent: Option<&str>) -> Result<bool, anyagents_core::Error> {
    let agents = match agent {
        Some(name) => vec![resolve_agent(data, name)?],
        None => crate::spiron_native::agents(data),
    };
    let mut any = false;
    for agent in agents {
        if crate::spiron_native::record_path(data, &agent).exists() {
            crate::spiron_native::end_recorded(data, &agent);
            if ui::is_pretty() {
                ui::success(&format!("Stopped {agent} (spiron)"));
            } else {
                println!("{agent}\tstopped (spiron)");
            }
            any = true;
        }
    }
    Ok(any)
}

/// Hold the data directory's server lock, which a running server holds:
/// refused while one runs.
fn hold_server_lock(data: &Path) -> Result<Option<std::fs::File>, anyagents_core::Error> {
    let path = data.join("server.lock");
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(anyagents_core::Error::Message(format!(
                "cannot open {}: {error}",
                path.display()
            )));
        }
    };
    // SAFETY: flock on a descriptor this function owns.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(anyagents_core::Error::Message(format!(
            "a server is running on {}: stop it first, or it starts the harnesses again",
            data.display()
        )));
    }
    Ok(Some(file))
}

/// Nobody holds the lock at `path`.
fn lock_is_free(path: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        return !path.exists();
    };
    // SAFETY: flock on a descriptor this function owns; dropping it releases.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

/// An agent id as written, or the id of the agent with that name.
fn resolve_agent(data: &Path, name: &str) -> Result<String, anyagents_core::Error> {
    if AgentId::parse(name).is_ok() {
        return Ok(name.to_string());
    }
    let store = Store::open(data.join("state.db"))?;
    Ok(store.agent_by_name(name)?.id.as_str().to_string())
}
