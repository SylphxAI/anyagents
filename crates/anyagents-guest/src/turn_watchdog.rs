//! Bounded silent-deadlock and max-duration turn watchdog for `anyagents-guest turn`.
//!
//! Tracks both direct harness events (activity callbacks, delta growth, usage growth,
//! mid-turn steer consumption, session transitions) and observable harness transcript,
//! session, WAL, and log file updates under `state_dir` (strictly excluding guest
//! bookkeeping files such as `.guest-activity.json` and `turns/`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

const DEFAULT_SILENT_PROGRESS_TIMEOUT_SECS: u64 = 600;
const DEFAULT_MAX_TURN_DURATION_SECS: u64 = 1_800;
const SILENT_PROGRESS_TIMEOUT_ENV: &str = "ANYAGENTS_GUEST_SILENT_TIMEOUT_SECS";
const MAX_TURN_DURATION_ENV: &str = "ANYAGENTS_GUEST_MAX_TURN_SECS";
pub(super) const STATE_DIR_PROGRESS_PROBE_INTERVAL: Duration = Duration::from_secs(2);
const MAX_PROGRESS_WALK_DEPTH: usize = 8;
const MAX_PROGRESS_WALK_ENTRIES: usize = 4_096;

pub(super) fn resolve_timeout_secs_from(raw: Option<&str>, default_secs: u64) -> Option<Duration> {
    let secs = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_secs);
    (secs > 0).then(|| Duration::from_secs(secs))
}

fn resolve_timeout_secs(env_key: &str, default_secs: u64) -> Option<Duration> {
    resolve_timeout_secs_from(std::env::var(env_key).ok().as_deref(), default_secs)
}

/// Bounded deadlock and max-duration watchdog shared between the foreground
/// turn loop and `HeartbeatGuard`.
///
/// Prevents a deadlocked inner harness (0 stdout/stderr/activity/file progress)
/// from being masked indefinitely by background heartbeats.
#[derive(Clone)]
pub(crate) struct TurnWatchdog {
    started_at: Instant,
    last_activity_ms: Arc<AtomicU64>,
    silent_timeout: Option<Duration>,
    max_duration: Option<Duration>,
}

impl TurnWatchdog {
    pub(super) fn new(silent_timeout: Option<Duration>, max_duration: Option<Duration>) -> Self {
        Self {
            started_at: Instant::now(),
            last_activity_ms: Arc::new(AtomicU64::new(0)),
            silent_timeout,
            max_duration,
        }
    }

    pub(super) fn from_env() -> Self {
        Self::new(
            resolve_timeout_secs(
                SILENT_PROGRESS_TIMEOUT_ENV,
                DEFAULT_SILENT_PROGRESS_TIMEOUT_SECS,
            ),
            resolve_timeout_secs(MAX_TURN_DURATION_ENV, DEFAULT_MAX_TURN_DURATION_SECS),
        )
    }

    pub(crate) fn touch(&self) {
        let elapsed = u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_activity_ms.store(elapsed, Ordering::Relaxed);
    }

    fn silent_elapsed(&self) -> Duration {
        let now = u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        Duration::from_millis(now.saturating_sub(last))
    }

    pub(super) fn expired_reason(&self, in_turn: bool, harness: &str) -> Option<String> {
        if let Some(max_d) = self.max_duration
            && self.started_at.elapsed() >= max_d
        {
            return Some(format!(
                "guest harness `{harness}` exceeded max turn duration ({}s)",
                max_d.as_secs()
            ));
        }
        if in_turn
            && let Some(silent_d) = self.silent_timeout
            && self.silent_elapsed() >= silent_d
        {
            return Some(format!(
                "guest harness `{harness}` produced no output or progress for {}s (deadlock watchdog)",
                silent_d.as_secs()
            ));
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StateDirFingerprint {
    file_count: u64,
    total_bytes: u64,
    max_mtime_ns: u128,
    checksum: u64,
}

/// Tracks observable harness transcript, session, WAL, and log file changes
/// under `state_dir` (excluding guest bookkeeping) so healthy multi-step tool
/// turns reset [`TurnWatchdog`] even before final text or token usage arrives.
pub(super) struct StateDirProgressTracker {
    state_dir: PathBuf,
    probe_interval: Duration,
    last_probe: Instant,
    last_fingerprint: StateDirFingerprint,
}

impl StateDirProgressTracker {
    pub(super) fn new(state_dir: &Path, probe_interval: Duration) -> Self {
        Self {
            state_dir: state_dir.to_path_buf(),
            probe_interval,
            last_probe: Instant::now(),
            last_fingerprint: scan_state_dir_fingerprint(state_dir),
        }
    }

    pub(super) fn poll_changed(&mut self) -> bool {
        if self.last_probe.elapsed() < self.probe_interval {
            return false;
        }
        self.last_probe = Instant::now();
        let next = scan_state_dir_fingerprint(&self.state_dir);
        if next != self.last_fingerprint {
            self.last_fingerprint = next;
            return true;
        }
        false
    }
}

fn is_guest_bookkeeping_entry(depth: usize, name: &str, is_dir: bool) -> bool {
    if name.ends_with(".anyagents-tmp") {
        return true;
    }
    if is_dir {
        if matches!(name, ".git" | "node_modules") {
            return true;
        }
        return depth == 0 && matches!(name, "turns" | ".agents");
    }
    depth == 0
        && (matches!(
            name,
            crate::activity::ACTIVITY_STATE_FILE
                | "guest-config.json"
                | "workspace-snapshot.json"
                | "repo-spec.json"
                | "opencode-service-pid"
                | "opencode-service-port"
        ) || name.ends_with("-session-id"))
}

fn scan_state_dir_fingerprint(root: &Path) -> StateDirFingerprint {
    let mut fp = StateDirFingerprint::default();
    let mut visited = 0usize;
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if visited >= MAX_PROGRESS_WALK_ENTRIES {
                return fp;
            }
            visited += 1;
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_symlink() || is_guest_bookkeeping_entry(depth, name, ft.is_dir()) {
                continue;
            }
            let path = entry.path();
            if ft.is_dir() {
                if depth < MAX_PROGRESS_WALK_DEPTH {
                    stack.push((path, depth + 1));
                }
                continue;
            }
            if !ft.is_file() {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let len = meta.len();
            let mtime_ns = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(0u128, |d| d.as_nanos());
            fp.file_count = fp.file_count.saturating_add(1);
            fp.total_bytes = fp.total_bytes.saturating_add(len);
            fp.max_mtime_ns = fp.max_mtime_ns.max(mtime_ns);
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for b in path.as_os_str().as_encoded_bytes() {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            for b in len.to_le_bytes().into_iter().chain(mtime_ns.to_le_bytes()) {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            fp.checksum = fp.checksum.wrapping_add(h);
        }
    }
    fp
}
