//! The scratch build budget.
//!
//! Every agent's build output lives in its build root on the scratch tier,
//! `{scratch}/builds/{id}`. Together with the host build output named by
//! `ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS` it may use at most
//! `ANYAGENTS_SCRATCH_BUILD_BUDGET` (300 GiB unless configured).
//!
//! Only output the server can prove it may rebuild is ever evicted, one
//! directory at a time:
//!
//! - a Cargo target directory (`.rustc_info.json` at its root) or profile
//!   (`.fingerprint` in it), or any directory marked with a valid
//!   `CACHEDIR.TAG`, and every entry of a Cargo target directory;
//! - a tool cache the server itself assigned in the build root: a seat's
//!   private tool cache (`cache/uv`, #283), Gradle's caches under `gradle/`,
//!   Xcode's DerivedData entries.
//!
//! A directory with `.git` at its top is never build output. Anything else in
//! a build root (a checkout an agent keeps there, say) is counted, so the
//! operator report can name who holds the space, and never touched.
//!
//! When the total is over budget, build output is removed until it is back
//! under 90% of the budget, in this order:
//!
//! 1. that of agents that no longer exist or are archived;
//! 2. then that of agents whose harness is not running;
//! 3. then host output no agent owns;
//! 4. then that of running agents that have no turn open;
//!
//! least recently written first within each. Never removed: anything written
//! in the last half hour, anything of an agent with a turn open, anything of
//! an agent whose row cannot be read, and anything while the server has not
//! yet heard from every running agent's harness since it started. Each
//! removal takes the agent's lifecycle lock and reads the agent again, takes
//! the desk Cargo wrapper's scope lease and Cargo's own build locks, and
//! measures the last write again, before it renames the directory aside.
//!
//! When the budget cannot be met, or the scratch filesystem is nearly full,
//! the operator inbox is told.
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
/// The desk Cargo wrapper's scope leases, beside the targets they guard.
const DESK_CACHE_LOCKS: &str = ".desk-cache-locks";
/// The locks Cargo holds in a target directory or profile while it builds.
const CARGO_LOCKS: &[&str] = &[".cargo-lock", ".cargo-build-lock", ".cargo-artifact-lock"];
/// The first line of a valid `CACHEDIR.TAG` (bford.info/cachedir).
const CACHEDIR_SIGNATURE: &[u8] = b"Signature: 8a477f597d28d172789f06886806bc55";
/// What Gradle keeps under its user home that it downloads or builds again.
const GRADLE_CACHES: &[&str] = &["caches", "daemon", "jdks", "native", "wrapper"];

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
    /// Its agent's row could not be read: nothing is decided this pass.
    Unknown,
}

/// What a unit is, as far as the server can prove it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    /// A Cargo target or profile, or a directory with a valid `CACHEDIR.TAG`.
    BuildOutput,
    /// A tool cache the server assigned (a seat's private cache, Gradle's
    /// caches, Xcode's DerivedData).
    ToolCache,
    /// Anything else. Counted, never evicted.
    Unverified,
}

/// Whose build output a unit is.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Owner {
    Agent(AgentId),
    /// The extra directory it sits in, such as `cargo-seat-sage`.
    Host(String),
}

/// One directory under the budget.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Usage {
    pub(crate) owner: Owner,
    /// The agent's name; the directory's for host output.
    pub(crate) name: Option<String>,
    pub(crate) dir: PathBuf,
    pub(crate) kind: Kind,
    pub(crate) bytes: u64,
    /// The newest modification time of anything in it.
    pub(crate) last_write_ms: i64,
    pub(crate) standing: Standing,
}

impl Usage {
    /// The plan may remove it.
    fn evictable(&self) -> bool {
        self.kind != Kind::Unverified && self.standing < Standing::Active
    }
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

/// The directories in `dir`, not following links.
fn subdirs(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut found: Vec<(String, PathBuf)> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .filter_map(|entry| {
                    let name = entry.file_name().to_str()?.to_string();
                    Some((name, entry.path()))
                })
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}

/// Every agent's build root under `scratch`: `builds/<id>`.
fn build_roots(scratch: &Path) -> Vec<(AgentId, PathBuf)> {
    subdirs(&scratch.join("builds"))
        .into_iter()
        .filter_map(|(name, path)| Some((AgentId::parse(&name).ok()?, path)))
        .collect()
}

/// Never a unit: a lock directory, or a unit being evicted.
fn bookkeeping(name: &str) -> bool {
    name == DESK_CACHE_LOCKS || name.starts_with(EVICTING)
}

fn is_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.is_file())
}

/// `dir/CACHEDIR.TAG` is a regular file that starts with the signature.
fn cachedir_tagged(dir: &Path) -> bool {
    use std::io::Read as _;
    let tag = dir.join("CACHEDIR.TAG");
    if !is_file(&tag) {
        return false;
    }
    let mut head = [0u8; CACHEDIR_SIGNATURE.len()];
    std::fs::File::open(&tag)
        .and_then(|mut file| file.read_exact(&mut head))
        .is_ok()
        && head == CACHEDIR_SIGNATURE
}

/// A checkout, or anything else with `.git` at its top.
fn has_git(dir: &Path) -> bool {
    dir.join(".git").symlink_metadata().is_ok()
}

/// A Cargo target directory: Cargo writes `.rustc_info.json` at its root.
fn cargo_target(dir: &Path) -> bool {
    is_file(&dir.join(".rustc_info.json")) || cachedir_tagged(dir)
}

/// Build output the server can prove it may rebuild.
fn build_output(dir: &Path) -> Kind {
    let proven = !has_git(dir)
        && (cargo_target(dir)
            || dir
                .join(".fingerprint")
                .symlink_metadata()
                .is_ok_and(|meta| meta.is_dir()));
    if proven {
        Kind::BuildOutput
    } else {
        Kind::Unverified
    }
}

/// `kind`, unless `dir` has `.git` at its top.
fn unless_checkout(dir: &Path, kind: Kind) -> Kind {
    if has_git(dir) { Kind::Unverified } else { kind }
}

/// The directories in `parent` that are units, each judged by `judge`.
fn units_in(parent: &Path, judge: impl Fn(&str, &Path) -> Kind) -> Vec<(PathBuf, Kind)> {
    subdirs(parent)
        .into_iter()
        .filter(|(name, _)| !bookkeeping(name))
        .map(|(name, path)| {
            let kind = judge(&name, &path);
            (path, kind)
        })
        .collect()
}

/// The units of one agent build root. The tool directories the server
/// assigns (`cargo`, `gradle`, `derived-data`, `cache`) are split into their
/// entries; any other entry of the root is one unit, build output only when
/// proven.
fn root_units(root: &Path) -> Vec<(PathBuf, Kind)> {
    let mut units = Vec::new();
    for (name, path) in subdirs(root) {
        if bookkeeping(&name) {
            continue;
        }
        match name.as_str() {
            "cargo" => {
                // Every entry of a Cargo target directory is Cargo's.
                let whole_target = cargo_target(&path) && !has_git(&path);
                units.extend(units_in(&path, |_, unit| {
                    if whole_target {
                        unless_checkout(unit, Kind::BuildOutput)
                    } else {
                        build_output(unit)
                    }
                }));
            }
            "gradle" => units.extend(units_in(&path, |entry, unit| {
                if GRADLE_CACHES.contains(&entry) {
                    unless_checkout(unit, Kind::ToolCache)
                } else {
                    Kind::Unverified
                }
            })),
            "derived-data" => units.extend(units_in(&path, |_, unit| {
                unless_checkout(unit, Kind::ToolCache)
            })),
            crate::sandbox::SEAT_CACHE_DIR => units.extend(units_in(&path, |tool, unit| {
                if crate::vendor::WRITTEN_TOOL_CACHES
                    .iter()
                    .any(|(_, name)| *name == tool)
                {
                    unless_checkout(unit, Kind::ToolCache)
                } else {
                    Kind::Unverified
                }
            })),
            _ => {
                let kind = build_output(&path);
                units.push((path, kind));
            }
        }
    }
    units
}

/// Every unit under the budget: agents' build roots, then each entry of an
/// extra host directory.
fn units(scratch: &Path, extra: &[PathBuf]) -> Vec<(Owner, PathBuf, Kind)> {
    let mut units = Vec::new();
    for (id, root) in build_roots(scratch) {
        for (dir, kind) in root_units(&root) {
            units.push((Owner::Agent(id.clone()), dir, kind));
        }
    }
    for dir in extra {
        let owner = dir
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        for (unit, kind) in units_in(dir, |_, unit| build_output(unit)) {
            units.push((Owner::Host(owner.clone()), unit, kind));
        }
    }
    units
}

/// What the server knows about an agent's harness right now.
pub(crate) struct Liveness<'a> {
    /// Whether its harness is running; `None` when unknown, which plans as
    /// if every existing agent's harness were running.
    pub(crate) alive: &'a dyn Fn(&AgentId) -> Option<bool>,
    /// Whether it has a turn open. A failed read counts as open.
    pub(crate) open_turn: &'a dyn Fn(&AgentId) -> bool,
}

/// Where an agent stands now, from its row and its harness.
fn agent_standing(
    store: &Store,
    agent_id: &AgentId,
    live: &Liveness<'_>,
) -> (Option<String>, Standing) {
    match store.agent(agent_id) {
        Err(anyagents_core::Error::AgentNotFound(_)) => (None, Standing::Gone),
        // A failed read decides nothing.
        Err(_) => (None, Standing::Unknown),
        Ok(agent) if agent.state == "archived" => (Some(agent.name), Standing::Gone),
        // With no harness process, an unfinished turn record is history, not
        // a build in progress.
        Ok(agent) if (live.alive)(agent_id) == Some(false) => (Some(agent.name), Standing::Down),
        Ok(agent) if (live.open_turn)(agent_id) => (Some(agent.name), Standing::Active),
        Ok(agent) => (Some(agent.name), Standing::Idle),
    }
}

fn written_recently(last_write_ms: i64, now_ms: i64) -> bool {
    now_ms.saturating_sub(last_write_ms) < ACTIVE_WRITE_MS
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
    for (owner, dir, kind) in units(scratch, extra) {
        let (bytes, last_write_ms) = measure(&dir);
        let (name, owner_standing) = match &owner {
            Owner::Host(name) => (Some(name.clone()), Standing::Host),
            Owner::Agent(id) => agents
                .entry(id.to_string())
                .or_insert_with(|| agent_standing(store, id, live))
                .clone(),
        };
        // Anything still being written is in use, whoever owns it: a build its
        // harness left running in the background, or one an archived agent's
        // process still finishes.
        let standing = if written_recently(last_write_ms, now_ms) {
            owner_standing.max(Standing::Active)
        } else {
            owner_standing
        };
        usages.push(Usage {
            owner,
            name,
            dir,
            kind,
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
            if !usage.evictable() || usage.bytes < floor.max(1) {
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

/// Locks held while a unit is renamed aside. Dropping it releases them.
struct Leases(#[allow(dead_code)] Vec<std::fs::File>);

/// The key of a per-checkout target the desk Cargo wrapper names
/// `<checkout>-<16 hex digits>`: its scope lease is
/// `.desk-cache-locks/<key>.lock` beside it.
fn scope_key(dir: &Path) -> Option<&str> {
    let (_, key) = dir.file_name()?.to_str()?.rsplit_once('-')?;
    (key.len() == 16 && key.bytes().all(|b| b.is_ascii_hexdigit())).then_some(key)
}

/// An existing lock file, opened without following a link; `Ok(None)` when
/// there is none.
fn open_lock(path: &Path) -> std::io::Result<Option<std::fs::File>> {
    use std::os::unix::fs::OpenOptionsExt as _;
    match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) if file.metadata()?.is_file() => Ok(Some(file)),
        Ok(_) => Err(std::io::Error::other("not a regular lock file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn lock_exclusive(file: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd as _;
    // SAFETY: flock on an open descriptor this function borrows.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

/// Take, without waiting, every lock a build of `dir` holds: the desk Cargo
/// wrapper's scope lease (held shared for a whole build, never created or
/// removed here) and Cargo's own locks in the unit or a profile directly in
/// it. `None` when any is held or cannot be read: the unit is in use.
fn take_leases(dir: &Path) -> Option<Leases> {
    let mut held = Vec::new();
    let mut candidates = Vec::new();
    if let (Some(key), Some(parent)) = (scope_key(dir), dir.parent()) {
        candidates.push(parent.join(DESK_CACHE_LOCKS).join(format!("{key}.lock")));
    }
    let mut levels = vec![dir.to_path_buf()];
    levels.extend(subdirs(dir).into_iter().map(|(_, sub)| sub));
    for level in levels {
        candidates.extend(CARGO_LOCKS.iter().map(|name| level.join(name)));
    }
    for path in candidates {
        match open_lock(&path) {
            Ok(None) => {}
            Ok(Some(file)) if lock_exclusive(&file) => held.push(file),
            Ok(Some(_)) | Err(_) => return None,
        }
    }
    Some(Leases(held))
}

/// Remove one unit, after checking again under every lock that it is still
/// not in use.
fn evict(store: &Store, data_dir: &Path, health: &HealthBoard, usage: &Usage) -> bool {
    if !usage.evictable() {
        return false;
    }
    let mut seat_ids = None;
    let _lifecycle = match &usage.owner {
        Owner::Host(_) => None,
        Owner::Agent(id) => {
            let Ok(lifecycle) = store.try_lock_agent_lifecycle(id) else {
                return false;
            };
            // Read the agent again under its lock: it may have been
            // unarchived, woken or given a turn since the plan.
            let alive = |agent: &AgentId| Some(health.get(agent).is_some_and(|l| l.process_alive));
            let open_turn = |agent: &AgentId| store.has_open_turn(agent).unwrap_or(true);
            let live = Liveness {
                alive: &alive,
                open_turn: &open_turn,
            };
            let (_, now) = agent_standing(store, id, &live);
            if now > usage.standing || now >= Standing::Active {
                return false;
            }
            seat_ids = super::detect_seat_ids(&anyagents_core::agent_config_dir(data_dir, id));
            Some(lifecycle)
        }
    };
    let Some(_leases) = take_leases(&usage.dir) else {
        return false;
    };
    // Measure the last write again: a build may have started since the plan.
    let (_, last_write_ms) = measure(&usage.dir);
    if written_recently(last_write_ms, anyagents_core::now_ms()) {
        return false;
    }
    remove(seat_ids, usage)
}

/// Move the unit aside, so a build that starts now gets a new directory
/// instead of one being deleted under it, then remove it. The caller holds
/// the unit's build locks throughout, so where the server may not rename it
/// (another user's host directory) it is removed in place. A host unit is
/// removed as its owner: `remove_path_with_seat` finds the uid from the path
/// itself when the server cannot remove it.
fn remove(seat_ids: Option<(u32, u32, u32)>, usage: &Usage) -> bool {
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
        .flat_map(|(_, root)| {
            let mut dirs = vec![root.clone()];
            dirs.extend(subdirs(&root).into_iter().map(|(_, tool)| tool));
            dirs
        })
        .chain(extra.iter().cloned());
    for parent in parents {
        for (name, unit) in subdirs(&parent) {
            if name.starts_with(EVICTING) {
                let _ = super::remove_path_with_seat(&unit, None);
            }
        }
    }
}

/// Every running agent's harness has reported to the supervisor since this
/// server started. Until then a harness that is running reads as down.
fn supervisor_reported(store: &Store, health: &HealthBoard) -> bool {
    store.list_live_agents().is_ok_and(|agents| {
        agents
            .iter()
            .filter(|agent| agent.state == "running")
            .all(|agent| health.get(&agent.id).is_some())
    })
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

/// Check the budget once, evicting and reporting as needed. `ready` is false
/// until the supervisor has reported every running agent's harness since
/// the server started: until then nothing is evicted.
pub(super) fn enforce(
    store: &Store,
    data_dir: &Path,
    health: &HealthBoard,
    reports: &mut Reports,
    ready: bool,
) {
    let (Some(scratch), Some(budget)) = (
        anyagents_core::configured_scratch_dir(),
        anyagents_core::configured_scratch_build_budget(),
    ) else {
        return;
    };
    let now = anyagents_core::now_ms();
    let extra = anyagents_core::configured_scratch_build_extra_dirs();
    let fs = filesystem(&scratch);
    // Build output is part of what the filesystem holds: when the whole
    // filesystem uses less than the budget, nothing needs measuring.
    let mut over = None;
    if fs.is_none_or(|fs| fs.used_bytes > budget) {
        let dry_run = anyagents_core::scratch_build_budget_dry_run();
        let hold = !dry_run && !ready;
        if !dry_run && ready {
            remove_interrupted(&scratch, &extra);
        }
        let alive = |id: &AgentId| Some(health.get(id).is_some_and(|live| live.process_alive));
        let open_turn = |id: &AgentId| store.has_open_turn(id).unwrap_or(true);
        let live = Liveness {
            alive: &alive,
            open_turn: &open_turn,
        };
        let plan = plan(store, &scratch, &extra, budget, &live, now);
        let unverified: u64 = plan
            .usages
            .iter()
            .filter(|usage| usage.kind == Kind::Unverified)
            .map(|usage| usage.bytes)
            .sum();
        let verb = if dry_run || hold {
            "would evict"
        } else {
            "evicting"
        };
        eprintln!(
            "anyagents-storage: build output {} of budget {} ({} not provably rebuildable, never evicted); {verb} {} in {} unit(s){}",
            gib(plan.total_bytes),
            gib(budget),
            gib(unverified),
            gib(plan.total_bytes - plan.after_bytes),
            plan.evict.len(),
            if dry_run {
                " (dry run)"
            } else if hold {
                " (held: not every running agent's harness has reported since the server started)"
            } else {
                ""
            }
        );
        let mut freed = 0;
        for &index in &plan.evict {
            let usage = &plan.usages[index];
            let removed = !dry_run && !hold && evict(store, data_dir, health, usage);
            if dry_run || hold || removed {
                freed += if removed { usage.bytes } else { 0 };
                eprintln!(
                    "anyagents-storage: {} {} of {:?} of {} ({:?}, last written {} min ago): {}",
                    if removed { "evicted" } else { "would evict" },
                    gib(usage.bytes),
                    usage.kind,
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
                .by_agent(|usage| !usage.evictable())
                .into_iter()
                .take(5)
                .map(|(name, bytes)| format!("{name} ({})", gib(bytes)))
                .collect();
            report(
                store,
                format!(
                    "Build output on {} is {} against a budget of {}. The rest is in use (written in the last half hour, or its agent is in a turn) or not provably rebuildable (a checkout or other files kept in a build directory), so it is never evicted: {}. Clear it, raise {}, or stop one of them.",
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

/// Run [`enforce`] every [`INTERVAL`] on a blocking thread. Nothing is
/// evicted until the supervisor has reported every running agent's harness:
/// before that, a running harness reads as down.
pub(super) fn start(store: std::sync::Arc<Store>, data_dir: PathBuf, health: HealthBoard) {
    if anyagents_core::configured_scratch_dir().is_none() {
        return;
    }
    tokio::spawn(async move {
        let reports = std::sync::Arc::new(std::sync::Mutex::new(Reports::default()));
        let mut interval = tokio::time::interval(INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first check waits a full interval: the supervisor takes over
        // the harnesses the last server left running before then.
        interval.tick().await;
        let mut ready = false;
        loop {
            interval.tick().await;
            let store = std::sync::Arc::clone(&store);
            let data_dir = data_dir.clone();
            let health = health.clone();
            let reports = std::sync::Arc::clone(&reports);
            ready = tokio::task::spawn_blocking(move || {
                let ready = ready || supervisor_reported(&store, &health);
                let mut reports = reports
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                enforce(&store, &data_dir, &health, &mut reports, ready);
                ready
            })
            .await
            .unwrap_or(ready);
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

    /// A Cargo profile directory, as Cargo leaves it.
    fn profile(path: &Path, bytes: usize) {
        write(&path.join(".fingerprint/app-0/lib-app"), 16);
        write(&path.join("deps/libapp.rlib"), bytes);
    }

    /// A Cargo target directory, as the desk wrapper's per-checkout targets are.
    fn target(path: &Path, bytes: usize) {
        write(&path.join(".rustc_info.json"), 16);
        std::fs::write(
            path.join("CACHEDIR.TAG"),
            [CACHEDIR_SIGNATURE, b"\n# cargo\n"].concat(),
        )
        .unwrap();
        profile(&path.join("debug"), bytes);
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

    fn root(scratch: &Path, id: &AgentId) -> PathBuf {
        anyagents_core::agent_scratch_build_dir(scratch, id)
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

    fn usage_of<'a>(plan: &'a Plan, dir: &Path) -> &'a Usage {
        plan.usages
            .iter()
            .find(|u| u.dir == dir)
            .unwrap_or_else(|| panic!("no usage for {}: {plan:#?}", dir.display()))
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
        let cargo = |id: &AgentId| root(&scratch, id).join("cargo");
        let mib = 1 << 20;
        // One unit per checkout target (the desk wrapper) or Cargo profile.
        for (id, unit, minutes) in [
            (&down.id, "old-checkout-0000000000000001", 600),
            (&down.id, "new-checkout-0000000000000002", 120),
            (&down.id, "fresh-checkout-0000000000000003", 1),
            (&idle.id, "debug", 900),
            (&busy.id, "debug", 1000),
            (&archived.id, "debug", 60),
            (&gone, "debug", 30_000),
        ] {
            if unit == "debug" {
                profile(&cargo(id).join(unit), mib);
            } else {
                target(&cargo(id).join(unit), mib);
            }
            age(&cargo(id).join(unit), minutes);
        }
        // `busy` runs a turn: none of its units is planned for eviction.
        let alive = |id: &AgentId| Some(id == &idle.id || id == &busy.id);
        let open = |id: &AgentId| id == &busy.id;
        let plan = plan_with(&store, &scratch, 4 * mib as u64, &alive, &open);
        assert_eq!(plan.usages.len(), 7);
        assert!(
            plan.usages.iter().all(|u| u.kind == Kind::BuildOutput),
            "{plan:#?}"
        );
        assert_eq!(
            order(&plan),
            [
                ":debug",
                "archived:debug",
                "down:old-checkout-0000000000000001",
                "down:new-checkout-0000000000000002"
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
        assert_eq!(
            standing("down", "fresh-checkout-0000000000000003"),
            Standing::Active
        );
        assert_eq!(standing("busy", "debug"), Standing::Active);
        assert_eq!(standing("idle", "debug"), Standing::Idle);

        // A tighter budget reaches the idle agent, never what is in use.
        let plan = plan_with(&store, &scratch, mib as u64, &alive, &open);
        assert_eq!(
            order(&plan),
            [
                ":debug",
                "archived:debug",
                "down:old-checkout-0000000000000001",
                "down:new-checkout-0000000000000002",
                "idle:debug"
            ]
        );
        assert_eq!(plan.by_agent(|u| !u.evictable()).len(), 2);

        // Under budget, nothing is evicted.
        let plan = plan_with(&store, &scratch, 100 * mib as u64, &alive, &open);
        assert!(plan.evict.is_empty());

        // An unfinished turn record of an agent whose harness is down is
        // history: its units are evictable.
        let down_with_turn = |id: &AgentId| id == &busy.id || id == &down.id;
        let plan = plan_with(&store, &scratch, 3 * mib as u64, &alive, &down_with_turn);
        assert!(order(&plan).contains(&"down:old-checkout-0000000000000001".to_string()));
    }

    /// Review P1: athena keeps its sub-agents' checkouts, with unpushed
    /// commits and uncommitted files, under `builds/<id>/workers/`. Only
    /// output the server can prove it may rebuild is ever a unit to evict;
    /// the rest is counted and named, and `evict` refuses it too.
    #[test]
    fn checkouts_and_unproven_directories_are_counted_and_never_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let athena = store
            .create_agent(anyagents_core::NewAgent::new("athena", "opencode"))
            .unwrap();
        let root = root(&scratch, &athena.id);
        let mib = 1 << 20;
        let worker = root.join("workers/ceph-block-tiering");
        write(&worker.join(".git/HEAD"), 32);
        write(&worker.join("src/unsaved.rs"), mib);
        // A checkout inside the Cargo directory, and Cargo-looking output
        // with `.git` at its top, are not build output either.
        write(&root.join("cargo/scratch-clone/.git/HEAD"), 32);
        write(&root.join("cargo/scratch-clone/notes.md"), mib);
        profile(&root.join("cargo/vendored"), mib);
        write(&root.join("cargo/vendored/.git/HEAD"), 32);
        // A directory with no proof, and Gradle's own settings.
        write(&root.join("cargo/notes/plan.md"), mib);
        write(&root.join("gradle/init.d/creds.gradle"), mib);
        write(&root.join("cache/mystery/blob"), mib);
        // A forged tag proves nothing.
        write(&root.join("reports/CACHEDIR.TAG"), 8);
        write(&root.join("reports/out.json"), mib);
        // Proven output beside them.
        profile(&root.join("cargo/debug"), mib);
        write(&root.join("gradle/caches/jars/a.jar"), mib);
        write(&root.join("cache/uv/wheels/a.whl"), mib);
        age(&root, 6000);
        let host = dir.path().join("build-targets/cargo-seat-sage");
        write(&host.join("clone-0123456789abcdef/.git/HEAD"), 32);
        write(&host.join("clone-0123456789abcdef/src/lib.rs"), mib);
        age(&host, 6000);
        let down = |_: &AgentId| Some(false);
        let none = |_: &AgentId| false;
        let plan = plan_extra(
            &store,
            &scratch,
            std::slice::from_ref(&host),
            1,
            &down,
            &none,
        );
        let kind = |path: &Path| usage_of(&plan, path).kind;
        for unproven in [
            root.join("workers"),
            root.join("cargo/scratch-clone"),
            root.join("cargo/vendored"),
            root.join("cargo/notes"),
            root.join("gradle/init.d"),
            root.join("cache/mystery"),
            root.join("reports"),
            host.join("clone-0123456789abcdef"),
        ] {
            assert_eq!(kind(&unproven), Kind::Unverified, "{}", unproven.display());
            assert!(
                !plan.evict.iter().any(|&i| plan.usages[i].dir == unproven),
                "{} was planned",
                unproven.display()
            );
            let usage = usage_of(&plan, &unproven);
            assert!(!evict(&store, dir.path(), &HealthBoard::default(), usage));
            assert!(unproven.exists());
        }
        assert_eq!(kind(&root.join("cargo/debug")), Kind::BuildOutput);
        assert_eq!(kind(&root.join("gradle/caches")), Kind::ToolCache);
        assert_eq!(kind(&root.join("cache/uv")), Kind::ToolCache);
        assert_eq!(
            order(&plan),
            ["athena:uv", "athena:debug", "athena:caches"],
            "{plan:#?}"
        );
        // Who holds what cannot be evicted is named.
        let held = plan.by_agent(|u| !u.evictable());
        assert_eq!(held.len(), 2, "{held:?}");
        assert!(held.iter().any(|(name, _)| name == "athena"));
        assert!(worker.join("src/unsaved.rs").exists());
    }

    /// Review P2: a unit written in the last half hour is in use whoever
    /// owns it, an archived or deleted agent too; and `evict` measures again
    /// before it removes anything.
    #[test]
    fn recent_writes_protect_a_gone_agents_output_and_are_measured_again() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let gone = AgentId::generate();
        let unit = root(&scratch, &gone).join("cargo/debug");
        profile(&unit, 1 << 20);
        age(&unit, 5);
        let none = |_: &AgentId| None;
        let open = |_: &AgentId| false;
        let plan = plan_with(&store, &scratch, 1, &none, &open);
        assert_eq!(usage_of(&plan, &unit).standing, Standing::Active);
        assert!(plan.evict.is_empty(), "{plan:#?}");
        // A plan made before the write does not remove it.
        let stale = Usage {
            owner: Owner::Agent(gone.clone()),
            name: None,
            dir: unit.clone(),
            kind: Kind::BuildOutput,
            bytes: 1 << 20,
            last_write_ms: 0,
            standing: Standing::Gone,
        };
        assert!(!evict(&store, dir.path(), &HealthBoard::default(), &stale));
        assert!(unit.exists());
    }

    /// A failed read of the agent's row decides nothing: its units are
    /// skipped this pass, never taken as a deleted agent's.
    #[test]
    fn an_unreadable_agent_row_skips_its_units() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let scratch = dir.path().join("scratch");
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("kept", "opencode"))
            .unwrap();
        let unit = root(&scratch, &agent.id).join("cargo/debug");
        profile(&unit, 1 << 20);
        age(&unit, 6000);
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("ALTER TABLE agents RENAME TO agents_unreadable;")
            .unwrap();
        assert!(!matches!(
            store.agent(&agent.id),
            Ok(_) | Err(anyagents_core::Error::AgentNotFound(_))
        ));
        let down = |_: &AgentId| Some(false);
        let open = |_: &AgentId| false;
        let plan = plan_with(&store, &scratch, 1, &down, &open);
        let usage = usage_of(&plan, &unit);
        assert_eq!(usage.standing, Standing::Unknown);
        assert!(plan.evict.is_empty(), "{plan:#?}");
        let as_gone = Usage {
            standing: Standing::Gone,
            ..usage.clone()
        };
        assert!(!evict(
            &store,
            dir.path(),
            &HealthBoard::default(),
            &as_gone
        ));
        assert!(unit.exists());
    }

    /// After a restart every harness reads as down until the supervisor has
    /// heard from it: nothing is evicted before each running agent reported.
    #[test]
    fn eviction_waits_until_every_running_agent_has_reported() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let running = store
            .create_agent(anyagents_core::NewAgent::new("running", "opencode"))
            .unwrap();
        store.set_agent_state(&running.id, "running").unwrap();
        let idle = store
            .create_agent(anyagents_core::NewAgent::new("idle", "opencode"))
            .unwrap();
        store.set_agent_state(&idle.id, "idle").unwrap();
        assert!(!supervisor_reported(&store, &health));
        health.publish(
            &running.id,
            anyagents_supervisor::LiveState {
                process_alive: true,
                turn_state: anyagents_harness::TurnState::Idle,
                delivery: None,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: None,
                live_model: None,
                process_started_at_ms: None,
                last_error: None,
            },
        );
        assert!(supervisor_reported(&store, &health));
    }

    #[test]
    fn eviction_moves_the_unit_aside_and_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let scratch = dir.path().join("scratch");
        let id = AgentId::generate();
        let cargo = root(&scratch, &id).join("cargo");
        profile(&cargo.join("debug"), 4096);
        profile(&cargo.join("release"), 4096);
        age(&cargo, 600);
        let usage = Usage {
            owner: Owner::Agent(id.clone()),
            name: None,
            dir: cargo.join("debug"),
            kind: Kind::BuildOutput,
            bytes: 4096,
            last_write_ms: 0,
            standing: Standing::Gone,
        };
        assert!(evict(&store, dir.path(), &HealthBoard::default(), &usage));
        assert!(!cargo.join("debug").exists());
        assert!(
            cargo.join("release/deps/libapp.rlib").exists(),
            "only the unit goes"
        );
        assert_eq!(std::fs::read_dir(&cargo).unwrap().count(), 1);
        // One interrupted by a restart is finished on the next check, and is
        // never counted as a unit meanwhile.
        let stale = cargo.join(format!("{EVICTING}debug-1"));
        profile(&stale, 10);
        assert!(
            units(&scratch, &[])
                .iter()
                .all(|(_, unit, _)| unit != &stale)
        );
        remove_interrupted(&scratch, &[]);
        assert!(!stale.exists());
        assert!(cargo.join("release/deps/libapp.rlib").exists());
    }

    /// The desk wrapper holds its scope lease shared for a whole build, and
    /// Cargo holds `.cargo-lock`: either held, the unit stays this pass.
    #[test]
    fn a_held_scope_lease_or_cargo_lock_keeps_the_unit() {
        use std::os::fd::AsRawFd as _;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let host = dir.path().join("build-targets/cargo-seat-sage");
        let leased = host.join("app-0123456789abcdef");
        let locked = host.join("tool-fedcba9876543210");
        target(&leased, 4096);
        target(&locked, 4096);
        write(&locked.join("debug/.cargo-lock"), 0);
        let lease = host.join(".desk-cache-locks/0123456789abcdef.lock");
        write(&lease, 0);
        age(&host, 600);
        let usage = |dir: &Path| Usage {
            owner: Owner::Host("cargo-seat-sage".into()),
            name: Some("cargo-seat-sage".into()),
            dir: dir.to_path_buf(),
            kind: Kind::BuildOutput,
            bytes: 4096,
            last_write_ms: 0,
            standing: Standing::Host,
        };
        let hold = |path: &Path, op: i32| {
            let file = std::fs::File::open(path).unwrap();
            // SAFETY: flock on a descriptor this test owns.
            assert_eq!(
                unsafe { libc::flock(file.as_raw_fd(), op | libc::LOCK_NB) },
                0
            );
            file
        };
        let building = hold(&lease, libc::LOCK_SH);
        let cargo = hold(&locked.join("debug/.cargo-lock"), libc::LOCK_EX);
        let board = HealthBoard::default();
        assert!(!evict(&store, dir.path(), &board, &usage(&leased)));
        assert!(!evict(&store, dir.path(), &board, &usage(&locked)));
        assert!(leased.exists() && locked.exists());
        drop((building, cargo));
        assert!(evict(&store, dir.path(), &board, &usage(&leased)));
        assert!(evict(&store, dir.path(), &board, &usage(&locked)));
        assert!(!leased.exists() && !locked.exists());
        // The lease file itself is never removed: a waiting build keeps its inode.
        assert!(lease.exists());
    }

    /// A seat that cannot write the shared tool caches gets private ones in
    /// its build root (`cache/<tool>`, #283). uv, pip and npm never trim
    /// their caches, so each is a unit like a build target.
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
            (&down.id, "bun", 900),
            (&busy.id, "pip", 600),
        ] {
            let unit = root(&scratch, id).join("cache").join(tool);
            write(&unit.join("wheels/blob"), mib);
            age(&unit, minutes);
        }
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
            let unit = root(&scratch, id).join("cargo/debug");
            profile(&unit, mib);
            age(&unit, minutes);
        }
        // A host's Cargo targets, one unit per checkout; the lock directory
        // the desk wrapper keeps beside them is never a unit.
        let host = dir.path().join("build-targets/cargo-seat-sage");
        for (unit, minutes) in [
            ("old-0000000000000001", 3000),
            ("building-0000000000000002", 5),
        ] {
            target(&host.join(unit), mib);
            age(&host.join(unit), minutes);
        }
        write(&host.join(".desk-cache-locks/0000000000000001.lock"), 0);
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
            [
                "down:debug",
                "cargo-seat-sage:old-0000000000000001",
                "idle:debug"
            ],
            "{plan:#?}"
        );
        let building = usage_of(&plan, &host.join("building-0000000000000002"));
        assert_eq!(building.owner, Owner::Host("cargo-seat-sage".into()));
        assert_eq!(building.standing, Standing::Active);
        // A host unit is removed without any agent's lock, under its lease.
        let old = usage_of(&plan, &host.join("old-0000000000000001"));
        assert!(evict(&store, dir.path(), &HealthBoard::default(), old));
        assert!(!host.join("old-0000000000000001").exists());
        assert!(
            host.join(".desk-cache-locks/0000000000000001.lock")
                .exists()
        );
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
