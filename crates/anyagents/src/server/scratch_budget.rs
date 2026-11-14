//! The scratch build budget.
//!
//! Every agent's build output lives under its own storage root on the scratch
//! tier, `{scratch}/agents/{id}/build` (and, for a harness an older server
//! started, `{scratch}/builds/{id}`). All of it can be rebuilt. Together it may
//! use at most `ANYAGENTS_SCRATCH_BUILD_BUDGET` (300 GiB unless configured).
//!
//! The unit of eviction is one build directory inside an agent's build tools:
//! `build/cargo/<one checkout's target>`, a Cargo profile such as
//! `build/cargo/debug`, a Gradle cache, a private tool cache
//! (`build/cache/uv`). When the total is over budget, units
//! are removed until it is back under 90% of the budget, in this order:
//!
//! 1. those of agents that no longer exist or are archived;
//! 2. then those of agents whose harness is not running;
//! 3. then those of running agents that have no turn open;
//!
//! least recently written first within each. A unit written in the last half
//! hour, and every unit of a running agent with a turn open, is never touched.
//! When the budget cannot be met without touching one, or the scratch
//! filesystem itself is nearly full, the operator inbox is told.
//!
//! Build output on the same volume that no agent owns (a host's Cargo targets,
//! `ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS`) counts against the same budget. Each
//! entry of those directories is a unit; they go after stopped agents' and
//! before idle agents'.
//!
//! Until `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=0`, each check only logs what
//! it would evict and removes nothing.

use std::collections::{BTreeMap, HashSet};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_core::{AgentId, Store};
use anyagents_supervisor::HealthBoard;

/// How often the budget is checked.
pub(super) const INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Build output written this recently is in use.
const ACTIVE_WRITE_MS: i64 = 30 * 60_000;
/// Eviction stops once the total is at this share of the budget.
const LOW_WATER_PERCENT: u64 = 90;
/// The operator is told when the scratch filesystem is this full.
const FULL_PERCENT: u64 = 90;
/// Smaller units are not worth evicting: removing them frees nothing
/// toward the budget.
const MIN_EVICT_BYTES: u64 = 64 << 20;
/// The same storage report is not repeated within this window.
const REPORT_EVERY_MS: i64 = 6 * 3_600_000;
/// Units being removed are renamed to this prefix first.
const EVICTING: &str = ".evicting-";

/// How far a unit is from being in use, most evictable first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Standing {
    /// No agent row, or the agent is archived.
    Gone,
    /// The agent exists but its harness is not running.
    Down,
    /// Build output no agent owns, from `ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS`.
    Host,
    /// The harness is running with no turn open.
    Idle,
    /// Written in the last half hour, or its agent is running a turn.
    Active,
}

/// Whose build output a unit is.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Owner {
    Agent(AgentId),
    /// The extra directory it sits in, such as `cargo-kyle`.
    Host(String),
}

/// One build directory.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Usage {
    pub(crate) owner: Owner,
    /// The agent's name; the directory's for host output.
    pub(crate) name: Option<String>,
    pub(crate) dir: PathBuf,
    pub(crate) bytes: u64,
    /// The newest modification time of anything in it.
    pub(crate) last_write_ms: i64,
    pub(crate) standing: Standing,
}

/// What enforcing the budget now would do.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Plan {
    pub(crate) scratch: PathBuf,
    pub(crate) budget_bytes: u64,
    pub(crate) total_bytes: u64,
    /// Eviction stops at or under this total.
    pub(crate) target_bytes: u64,
    /// Indexes into `usages`, in eviction order.
    pub(crate) evict: Vec<usize>,
    pub(crate) usages: Vec<Usage>,
    /// The total after the planned evictions.
    pub(crate) after_bytes: u64,
    pub(crate) filesystem: Option<Filesystem>,
}

impl Plan {
    /// Bytes per agent among `usages` that pass `keep`, largest first.
    fn by_agent(&self, keep: impl Fn(&Usage) -> bool) -> Vec<(String, u64)> {
        let mut agents: BTreeMap<String, u64> = BTreeMap::new();
        for usage in self.usages.iter().filter(|usage| keep(usage)) {
            let name = usage.name.clone().unwrap_or_else(|| match &usage.owner {
                Owner::Agent(id) => id.to_string(),
                Owner::Host(dir) => dir.clone(),
            });
            *agents.entry(name).or_default() += usage.bytes;
        }
        let mut agents: Vec<_> = agents.into_iter().collect();
        agents.sort_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
        agents
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct Filesystem {
    pub(crate) size_bytes: u64,
    pub(crate) used_bytes: u64,
    pub(crate) available_bytes: u64,
}

impl Filesystem {
    fn used_percent(&self) -> u64 {
        (self.used_bytes.saturating_mul(100))
            .checked_div(self.size_bytes)
            .unwrap_or(0)
    }
}

pub(crate) fn filesystem(path: &Path) -> Option<Filesystem> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: c_path is NUL-terminated and stat is written by the call.
    if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: statvfs returned 0, so it filled the struct.
    let stat = unsafe { stat.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    let unit = stat.f_frsize as u64;
    #[allow(clippy::unnecessary_cast)]
    let (blocks, free, avail) = (
        stat.f_blocks as u64,
        stat.f_bfree as u64,
        stat.f_bavail as u64,
    );
    Some(Filesystem {
        size_bytes: blocks * unit,
        used_bytes: blocks.saturating_sub(free) * unit,
        available_bytes: avail * unit,
    })
}

/// Allocated bytes and the newest mtime of `root` and everything under it,
/// on its filesystem, without following links. Hard links count once.
fn measure(root: &Path) -> (u64, i64) {
    let Ok(meta) = std::fs::symlink_metadata(root) else {
        return (0, 0);
    };
    let device = meta.dev();
    let mut bytes = meta.blocks() * 512;
    let mut newest = meta.mtime() * 1000;
    if !meta.is_dir() {
        return (bytes, newest);
    }
    let mut seen = HashSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.dev() != device {
                continue;
            }
            newest = newest.max(meta.mtime() * 1000);
            if meta.nlink() > 1 && !meta.is_dir() && !seen.insert((meta.dev(), meta.ino())) {
                continue;
            }
            bytes += meta.blocks() * 512;
            if meta.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    (bytes, newest)
}

fn children(dir: &Path) -> Vec<std::fs::DirEntry> {
    std::fs::read_dir(dir)
        .map(|entries| entries.flatten().collect())
        .unwrap_or_default()
}

/// Every agent's build directory under `scratch`, current layout and
/// legacy: `agents/<id>/build` and `builds/<id>`.
fn build_roots(scratch: &Path) -> Vec<(AgentId, PathBuf)> {
    let mut roots = Vec::new();
    for (parent, child) in [("agents", Some("build")), ("builds", None)] {
        for entry in children(&scratch.join(parent)) {
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| AgentId::parse(name).ok())
            else {
                continue;
            };
            if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            let dir = match child {
                Some(child) => entry.path().join(child),
                None => entry.path(),
            };
            if dir.symlink_metadata().is_ok_and(|meta| meta.is_dir()) {
                roots.push((id, dir));
            }
        }
    }
    roots
}

/// Not a unit: hidden entries (lock directories, units being evicted).
fn hidden(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_none_or(|name| name.starts_with('.'))
}

/// The eviction units: each entry inside one of an agent's build tool
/// directories (`build/cargo/<entry>`, `build/gradle/<entry>`, ...), and each
/// entry of an extra host directory.
fn units(scratch: &Path, extra: &[PathBuf]) -> Vec<(Owner, PathBuf)> {
    let mut units = Vec::new();
    for (id, root) in build_roots(scratch) {
        for tool in children(&root) {
            if !tool.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            for unit in children(&tool.path()) {
                if !hidden(&unit) {
                    units.push((Owner::Agent(id.clone()), unit.path()));
                }
            }
        }
    }
    for dir in extra {
        let owner = dir
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        for unit in children(dir) {
            if !hidden(&unit) {
                units.push((Owner::Host(owner.clone()), unit.path()));
            }
        }
    }
    units
}

/// What the server knows about an agent's harness right now.
pub(crate) struct Liveness<'a> {
    /// Whether its harness is running; `None` when unknown (the CLI), which
    /// plans as if every existing agent's harness were running.
    pub(crate) alive: &'a dyn Fn(&AgentId) -> Option<bool>,
    /// Whether it has a turn open. A failed read counts as open.
    pub(crate) open_turn: &'a dyn Fn(&AgentId) -> bool,
}

/// Measure all build output under the budget and decide what to evict.
pub(crate) fn plan(
    store: &Store,
    scratch: &Path,
    extra: &[PathBuf],
    budget: u64,
    live: &Liveness<'_>,
    now_ms: i64,
) -> Plan {
    plan_above(store, scratch, extra, budget, live, now_ms, MIN_EVICT_BYTES)
}

/// [`plan`], evicting only units of at least `floor` bytes.
fn plan_above(
    store: &Store,
    scratch: &Path,
    extra: &[PathBuf],
    budget: u64,
    live: &Liveness<'_>,
    now_ms: i64,
    floor: u64,
) -> Plan {
    let mut agents: BTreeMap<String, (Option<String>, Standing)> = BTreeMap::new();
    let mut usages = Vec::new();
    for (owner, dir) in units(scratch, extra) {
        let (bytes, last_write_ms) = measure(&dir);
        let agent_id = match &owner {
            Owner::Agent(id) => id.clone(),
            Owner::Host(name) => {
                let standing = if now_ms.saturating_sub(last_write_ms) < ACTIVE_WRITE_MS {
                    Standing::Active
                } else {
                    Standing::Host
                };
                usages.push(Usage {
                    name: Some(name.clone()),
                    owner,
                    dir,
                    bytes,
                    last_write_ms,
                    standing,
                });
                continue;
            }
        };
        let (name, agent_standing) = agents
            .entry(agent_id.to_string())
            .or_insert_with(|| match store.agent(&agent_id) {
                Err(_) => (None, Standing::Gone),
                Ok(agent) if agent.state == "archived" => (Some(agent.name), Standing::Gone),
                // With no harness process, an unfinished turn record is
                // history, not a build in progress.
                Ok(agent) if (live.alive)(&agent_id) == Some(false) => {
                    (Some(agent.name), Standing::Down)
                }
                Ok(agent) if (live.open_turn)(&agent_id) => (Some(agent.name), Standing::Active),
                Ok(agent) => (Some(agent.name), Standing::Idle),
            })
            .clone();
        // Anything still building writes: a build its harness left running in
        // the background as well as one in a turn.
        let standing = if agent_standing != Standing::Gone
            && now_ms.saturating_sub(last_write_ms) < ACTIVE_WRITE_MS
        {
            Standing::Active
        } else {
            agent_standing
        };
        usages.push(Usage {
            owner,
            name,
            dir,
            bytes,
            last_write_ms,
            standing,
        });
    }
    usages.sort_by(|a, b| {
        (a.standing, a.last_write_ms, &a.dir).cmp(&(b.standing, b.last_write_ms, &b.dir))
    });
    let total_bytes: u64 = usages.iter().map(|usage| usage.bytes).sum();
    let target_bytes = budget / 100 * LOW_WATER_PERCENT;
    let mut after_bytes = total_bytes;
    let mut evict = Vec::new();
    if total_bytes > budget {
        for (index, usage) in usages.iter().enumerate() {
            if after_bytes <= target_bytes {
                break;
            }
            if usage.standing == Standing::Active || usage.bytes < floor.max(1) {
                continue;
            }
            evict.push(index);
            after_bytes -= usage.bytes;
        }
    }
    Plan {
        scratch: scratch.to_path_buf(),
        budget_bytes: budget,
        total_bytes,
        target_bytes,
        evict,
        usages,
        after_bytes,
        filesystem: filesystem(scratch),
    }
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / f64::from(1u32 << 30))
}

/// Remove one unit, unless its agent started a turn since the plan.
fn evict(store: &Store, data_dir: &Path, health: &HealthBoard, usage: &Usage) -> bool {
    let agent_id = match &usage.owner {
        Owner::Host(_) => return remove(None, usage),
        Owner::Agent(id) => id,
    };
    let seat_ids = super::detect_seat_ids(&anyagents_core::agent_config_dir(data_dir, agent_id));
    if usage.standing == Standing::Gone {
        return remove(seat_ids, usage);
    }
    let alive = health.get(agent_id).is_some_and(|live| live.process_alive);
    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(agent_id) else {
        return false;
    };
    let started = match usage.standing {
        Standing::Down => alive,
        _ => alive && store.has_open_turn(agent_id).unwrap_or(true),
    };
    !started && remove(seat_ids, usage)
}

/// A host unit is removed as its owner: `remove_path_with_seat` finds the
/// uid from the path itself when the server cannot remove it.
fn remove(seat_ids: Option<(u32, u32, u32)>, usage: &Usage) -> bool {
    // Move it aside first, so a build that starts now gets a new directory
    // instead of one being deleted under it.
    let doomed = usage.dir.with_file_name(format!(
        "{EVICTING}{}-{}",
        usage
            .dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("build"),
        anyagents_core::now_ms()
    ));
    let target = if std::fs::rename(&usage.dir, &doomed).is_ok() {
        doomed
    } else {
        usage.dir.clone()
    };
    super::remove_path_with_seat(&target, seat_ids)
}

/// Units left over from an eviction a restart interrupted.
fn remove_interrupted(scratch: &Path, extra: &[PathBuf]) {
    let parents = build_roots(scratch)
        .into_iter()
        .flat_map(|(_, root)| children(&root))
        .map(|tool| tool.path())
        .chain(extra.iter().cloned());
    for parent in parents {
        for unit in children(&parent) {
            if unit
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(EVICTING))
            {
                let _ = super::remove_path_with_seat(&unit.path(), None);
            }
        }
    }
}

/// When the last report of each kind went to the operator.
#[derive(Default)]
pub(super) struct Reports {
    over_budget_ms: Option<i64>,
    full_ms: Option<i64>,
}

fn due(last: &mut Option<i64>, now_ms: i64) -> bool {
    if last.is_some_and(|at| now_ms.saturating_sub(at) < REPORT_EVERY_MS) {
        return false;
    }
    *last = Some(now_ms);
    true
}

fn report(store: &Store, body: String) {
    eprintln!("anyagents-storage: {body}");
    if !matches!(
        store.report_to_operator(anyagents_core::OperatorReport {
            sender: super::STORAGE_SENDER.to_string(),
            sender_name: Some(super::STORAGE_SENDER_NAME.to_string()),
            body,
            received_at_ms: None,
        }),
        Ok(anyagents_core::OperatorReceipt::New(_) | anyagents_core::OperatorReceipt::Duplicate(_))
    ) {
        eprintln!("anyagents-storage: the operator report was not stored");
    }
}

/// Check the budget once, evicting and reporting as needed.
pub(super) fn enforce(store: &Store, data_dir: &Path, health: &HealthBoard, reports: &mut Reports) {
    let (Some(scratch), Some(budget)) = (
        anyagents_core::configured_scratch_dir(),
        anyagents_core::configured_scratch_build_budget(),
    ) else {
        return;
    };
    let now = anyagents_core::now_ms();
    let extra = anyagents_core::configured_scratch_build_extra_dirs();
    remove_interrupted(&scratch, &extra);
    let fs = filesystem(&scratch);
    // Build output is part of what the filesystem holds: when the whole
    // filesystem uses less than the budget, nothing needs measuring.
    let mut over = None;
    if fs.is_none_or(|fs| fs.used_bytes > budget) {
        let alive = |id: &AgentId| Some(health.get(id).is_some_and(|live| live.process_alive));
        let open_turn = |id: &AgentId| store.has_open_turn(id).unwrap_or(true);
        let live = Liveness {
            alive: &alive,
            open_turn: &open_turn,
        };
        let plan = plan(store, &scratch, &extra, budget, &live, now);
        let dry_run = anyagents_core::scratch_build_budget_dry_run();
        eprintln!(
            "anyagents-storage: build output {} of budget {}; {} {} in {} unit(s){}",
            gib(plan.total_bytes),
            gib(budget),
            if dry_run { "would evict" } else { "evicting" },
            gib(plan.total_bytes - plan.after_bytes),
            plan.evict.len(),
            if dry_run { " (dry run)" } else { "" }
        );
        let mut freed = 0;
        for &index in &plan.evict {
            let usage = &plan.usages[index];
            if dry_run || evict(store, data_dir, health, usage) {
                freed += if dry_run { 0 } else { usage.bytes };
                eprintln!(
                    "anyagents-storage: {} {} of build output of {} ({:?}, last written {} min ago): {}",
                    if dry_run { "would evict" } else { "evicted" },
                    gib(usage.bytes),
                    match &usage.owner {
                        Owner::Agent(id) =>
                            format!("{} ({id})", usage.name.as_deref().unwrap_or("no agent")),
                        Owner::Host(dir) => format!("host {dir}"),
                    },
                    usage.standing,
                    now.saturating_sub(usage.last_write_ms) / 60_000,
                    usage.dir.display()
                );
            }
        }
        let remaining = plan.total_bytes.saturating_sub(freed);
        if remaining > budget {
            over = Some((plan, remaining));
        }
    }
    if let Some((plan, remaining)) = over {
        if due(&mut reports.over_budget_ms, now) {
            let holders: Vec<String> = plan
                .by_agent(|usage| usage.standing == Standing::Active)
                .into_iter()
                .take(5)
                .map(|(name, bytes)| format!("{name} ({})", gib(bytes)))
                .collect();
            report(
                store,
                format!(
                    "Build output on {} is {} against a budget of {}, and the rest is in use (written in the last half hour, or its agent is in a turn): {}. Raise {} or stop one of them.",
                    plan.scratch.display(),
                    gib(remaining),
                    gib(budget),
                    holders.join(", "),
                    anyagents_core::SCRATCH_BUILD_BUDGET_ENV
                ),
            );
        }
    } else {
        reports.over_budget_ms = None;
    }
    let fs = filesystem(&scratch).or(fs);
    match fs {
        Some(fs) if fs.used_percent() >= FULL_PERCENT => {
            if due(&mut reports.full_ms, now) {
                report(
                    store,
                    format!(
                        "The scratch filesystem {} is {}% full ({} free). Build output is held under its budget of {}; the rest is other data on that filesystem.",
                        scratch.display(),
                        fs.used_percent(),
                        gib(fs.available_bytes),
                        gib(budget)
                    ),
                );
            }
        }
        _ => reports.full_ms = None,
    }
}

/// Run [`enforce`] every [`INTERVAL`] on a blocking thread.
pub(super) fn start(store: std::sync::Arc<Store>, data_dir: PathBuf, health: HealthBoard) {
    if anyagents_core::configured_scratch_dir().is_none() {
        return;
    }
    tokio::spawn(async move {
        let reports = std::sync::Arc::new(std::sync::Mutex::new(Reports::default()));
        let mut interval = tokio::time::interval(INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let store = std::sync::Arc::clone(&store);
            let data_dir = data_dir.clone();
            let health = health.clone();
            let reports = std::sync::Arc::clone(&reports);
            let _ = tokio::task::spawn_blocking(move || {
                let mut reports = reports
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                enforce(&store, &data_dir, &health, &mut reports);
            })
            .await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, bytes: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![7u8; bytes]).unwrap();
    }

    fn age(path: &Path, minutes: i64) {
        let at = std::time::SystemTime::now() - Duration::from_secs(minutes as u64 * 60);
        let times = std::fs::FileTimes::new().set_modified(at);
        for entry in walk(path) {
            let file = std::fs::File::open(&entry).unwrap();
            file.set_times(times).unwrap();
        }
    }

    fn walk(path: &Path) -> Vec<PathBuf> {
        let mut out = vec![path.to_path_buf()];
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                out.extend(walk(&entry.unwrap().path()));
            }
        }
        // Children first, so a directory's own time is set last.
        out.reverse();
        out
    }

    fn plan_with(
        store: &Store,
        scratch: &Path,
        budget: u64,
        alive: &dyn Fn(&AgentId) -> Option<bool>,
        open: &dyn Fn(&AgentId) -> bool,
    ) -> Plan {
        plan_extra(store, scratch, &[], budget, alive, open)
    }

    fn plan_extra(
        store: &Store,
        scratch: &Path,
        extra: &[PathBuf],
        budget: u64,
        alive: &dyn Fn(&AgentId) -> Option<bool>,
        open: &dyn Fn(&AgentId) -> bool,
    ) -> Plan {
        let live = Liveness {
            alive,
            open_turn: open,
        };
        plan_above(
            store,
            scratch,
            extra,
            budget,
            &live,
            anyagents_core::now_ms(),
            0,
        )
    }

    fn order(plan: &Plan) -> Vec<String> {
        plan.evict
            .iter()
            .map(|&i| {
                let usage = &plan.usages[i];
                format!(
                    "{}:{}",
                    usage.name.clone().unwrap_or_default(),
                    usage.dir.file_name().unwrap().to_string_lossy()
                )
            })
            .collect()
    }

    #[test]
    fn plan_evicts_gone_then_down_then_idle_least_recent_first_and_never_active() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let agent = |name: &str| {
            store
                .create_agent(anyagents_core::NewAgent::new(name, "opencode"))
                .unwrap()
        };
        let down = agent("down");
        let idle = agent("idle");
        let busy = agent("busy");
        let archived = agent("archived");
        store.set_agent_state(&archived.id, "archived").unwrap();
        let gone = AgentId::generate();
        let cargo =
            |id: &AgentId| anyagents_core::agent_scratch_build_dir(&scratch, id).join("cargo");
        let mib = 1 << 20;
        // One unit per checkout target (the desk wrapper) or Cargo profile.
        for (id, unit, minutes) in [
            (&down.id, "old-checkout--0001", 600),
            (&down.id, "new-checkout--0002", 120),
            (&down.id, "fresh-checkout--0003", 1),
            (&idle.id, "debug", 900),
            (&busy.id, "debug", 1000),
            (&archived.id, "debug", 60),
        ] {
            write(&cargo(id).join(unit).join("deps/lib.rlib"), mib);
            age(&cargo(id).join(unit), minutes);
        }
        // An older server's layout still counts as that agent's.
        let legacy = scratch
            .join("builds")
            .join(gone.as_str())
            .join("cargo/debug");
        write(&legacy.join("app"), mib);
        age(&legacy, 30_000);
        // `busy` runs a turn: none of its units is planned for eviction.
        let alive = |id: &AgentId| Some(id == &idle.id || id == &busy.id);
        let open = |id: &AgentId| id == &busy.id;
        let plan = plan_with(&store, &scratch, 4 * mib as u64, &alive, &open);
        assert_eq!(plan.usages.len(), 7);
        assert_eq!(
            order(&plan),
            [
                ":debug",
                "archived:debug",
                "down:old-checkout--0001",
                "down:new-checkout--0002"
            ],
            "{plan:#?}"
        );
        assert!(plan.after_bytes <= plan.target_bytes, "{plan:#?}");
        let standing = |name: &str, unit: &str| {
            plan.usages
                .iter()
                .find(|u| u.name.as_deref() == Some(name) && u.dir.ends_with(unit))
                .unwrap()
                .standing
        };
        assert_eq!(standing("down", "fresh-checkout--0003"), Standing::Active);
        assert_eq!(standing("busy", "debug"), Standing::Active);
        assert_eq!(standing("idle", "debug"), Standing::Idle);

        // A tighter budget reaches the idle agent, never what is in use.
        let plan = plan_with(&store, &scratch, mib as u64, &alive, &open);
        assert_eq!(
            order(&plan),
            [
                ":debug",
                "archived:debug",
                "down:old-checkout--0001",
                "down:new-checkout--0002",
                "idle:debug"
            ]
        );
        assert_eq!(
            plan.by_agent(|u| u.standing == Standing::Active)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>()
                .len(),
            2
        );

        // Under budget, nothing is evicted.
        let plan = plan_with(&store, &scratch, 100 * mib as u64, &alive, &open);
        assert!(plan.evict.is_empty());

        // An unfinished turn record of an agent whose harness is down is
        // history: its units are evictable.
        let down_with_turn = |id: &AgentId| id == &busy.id || id == &down.id;
        let plan = plan_with(&store, &scratch, 3 * mib as u64, &alive, &down_with_turn);
        assert!(order(&plan).contains(&"down:old-checkout--0001".to_string()));
    }

    #[test]
    fn eviction_moves_the_unit_aside_and_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let id = AgentId::generate();
        let cargo = anyagents_core::agent_scratch_build_dir(&scratch, &id).join("cargo");
        write(&cargo.join("debug/app"), 4096);
        write(&cargo.join("release/app"), 4096);
        let usage = Usage {
            owner: Owner::Agent(id.clone()),
            name: None,
            dir: cargo.join("debug"),
            bytes: 4096,
            last_write_ms: 0,
            standing: Standing::Gone,
        };
        assert!(evict(&store, dir.path(), &HealthBoard::default(), &usage));
        assert!(!cargo.join("debug").exists());
        assert!(cargo.join("release/app").exists(), "only the unit goes");
        assert_eq!(std::fs::read_dir(&cargo).unwrap().count(), 1);
        // One interrupted by a restart is finished on the next check, and is
        // never counted as a unit meanwhile.
        let stale = cargo.join(format!("{EVICTING}debug-1"));
        write(&stale.join("x"), 10);
        assert!(units(&scratch, &[]).iter().all(|(_, unit)| unit != &stale));
        remove_interrupted(&scratch, &[]);
        assert!(!stale.exists());
        assert!(cargo.join("release/app").exists());
    }

    /// A seat that cannot write the shared tool caches gets private ones in
    /// its build root (`build/cache/<tool>`, #283). uv, pip and npm never
    /// trim their caches, so each is a unit like a build target.
    #[test]
    fn private_tool_caches_are_units_on_the_same_rules() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let down = store
            .create_agent(anyagents_core::NewAgent::new("down", "opencode"))
            .unwrap();
        let busy = store
            .create_agent(anyagents_core::NewAgent::new("busy", "opencode"))
            .unwrap();
        let mib = 1 << 20;
        for (id, tool, minutes) in [
            (&down.id, "uv", 600),
            (&down.id, "npm", 10),
            (&busy.id, "pip", 600),
        ] {
            let unit = anyagents_core::agent_scratch_build_dir(&scratch, id)
                .join("cache")
                .join(tool);
            write(&unit.join("wheels/blob"), mib);
            age(&unit, minutes);
        }
        // The same under an older server's layout.
        let legacy = scratch
            .join("builds")
            .join(down.id.as_str())
            .join("cache/bun");
        write(&legacy.join("blob"), mib);
        age(&legacy, 900);
        let alive = |id: &AgentId| Some(id == &busy.id);
        let open = |id: &AgentId| id == &busy.id;
        let plan = plan_with(&store, &scratch, 1, &alive, &open);
        assert_eq!(plan.usages.len(), 4, "{plan:#?}");
        // Written ten minutes ago, or the agent is in a turn: kept.
        assert_eq!(order(&plan), ["down:bun", "down:uv"], "{plan:#?}");
    }

    #[test]
    fn host_build_output_shares_the_budget_after_stopped_agents() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let down = store
            .create_agent(anyagents_core::NewAgent::new("down", "opencode"))
            .unwrap();
        let idle = store
            .create_agent(anyagents_core::NewAgent::new("idle", "opencode"))
            .unwrap();
        let mib = 1 << 20;
        for (id, minutes) in [(&down.id, 60), (&idle.id, 6000)] {
            let unit = anyagents_core::agent_scratch_build_dir(&scratch, id).join("cargo/debug");
            write(&unit.join("app"), mib);
            age(&unit, minutes);
        }
        // A host's Cargo targets, one unit per checkout; the lock directory
        // the desk wrapper keeps beside them is never a unit.
        let host = dir.path().join("build-targets/cargo-kyle");
        for (unit, minutes) in [("old--0001", 3000), ("building--0002", 5)] {
            write(&host.join(unit).join("debug/app"), mib);
            age(&host.join(unit), minutes);
        }
        write(&host.join(".desk-cache-locks/0001.lock"), 1);
        let alive = |id: &AgentId| Some(id == &idle.id);
        let none = |_: &AgentId| false;
        let plan = plan_extra(
            &store,
            &scratch,
            std::slice::from_ref(&host),
            mib as u64,
            &alive,
            &none,
        );
        assert_eq!(plan.usages.len(), 4, "{plan:#?}");
        assert_eq!(
            order(&plan),
            ["down:debug", "cargo-kyle:old--0001", "idle:debug"],
            "{plan:#?}"
        );
        let building = plan
            .usages
            .iter()
            .find(|u| u.dir.ends_with("building--0002"))
            .unwrap();
        assert_eq!(building.owner, Owner::Host("cargo-kyle".into()));
        assert_eq!(building.standing, Standing::Active);
        // A host unit is removed without any agent's lock.
        let old = plan
            .usages
            .iter()
            .find(|u| u.dir.ends_with("old--0001"))
            .unwrap();
        assert!(evict(&store, dir.path(), &HealthBoard::default(), old));
        assert!(!host.join("old--0001").exists());
        assert!(host.join(".desk-cache-locks/0001.lock").exists());
    }

    #[test]
    fn hard_links_count_once() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("build");
        write(&root.join("deps/lib.rlib"), 1 << 20);
        std::fs::hard_link(root.join("deps/lib.rlib"), root.join("lib.rlib")).unwrap();
        let (bytes, newest) = measure(&root);
        assert!((1 << 20..2 << 20).contains(&bytes), "{bytes}");
        assert!(newest > 0);
    }
}
