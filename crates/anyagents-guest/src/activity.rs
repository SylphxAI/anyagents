//! Remote guest activity tracking & Linux `/proc` descendant inspection.
//!
//! Reports live active turns and descendant child subprocesses (`inflight_children`)
//! both via periodic `stream-json` heartbeat frames and via `anyagents-guest health`
//! so a coordinator's idle sweeper never releases a sandbox while an agent turn or
//! detached build/test subprocess is still running.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Atomic activity snapshot persisted inside `<state_dir>/.guest-activity.json`.
pub const ACTIVITY_STATE_FILE: &str = ".guest-activity.json";
/// Default interval between emitted `system/heartbeat` frames in milliseconds (5s).
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 5_000;

/// Live descendant counts derived from the OS process table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessTreeCounts {
    /// Total live non-zombie descendants of the guest supervisor PID.
    pub descendant_processes: usize,
    /// Live non-zombie work subprocesses (grandchildren, build/test commands, extra children),
    /// excluding the single idle resident harness CLI and `anyagents-guest mcp-proxy`.
    pub inflight_children: usize,
}

/// Snapshot of live guest supervisor turn and child-process activity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestActivitySnapshot {
    pub pid: u32,
    pub timestamp_ms: u64,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub turn_state: String,
    pub active_turns: usize,
    pub queued_turns: usize,
    pub inflight_children: usize,
    pub descendant_processes: usize,
    pub busy: bool,
}

impl GuestActivitySnapshot {
    /// Capture a fresh snapshot for the current `anyagents-guest serve` process.
    pub fn capture_current(
        harness: &str,
        session_id: Option<String>,
        turn_state: &str,
        active_turns: usize,
        queued_turns: usize,
    ) -> Self {
        let pid = std::process::id();
        let counts = inspect_process_tree(pid);
        let busy = active_turns > 0
            || queued_turns > 0
            || turn_state != "idle"
            || counts.inflight_children > 0;
        Self {
            pid,
            timestamp_ms: now_unix_ms(),
            harness: harness.to_string(),
            session_id,
            turn_state: turn_state.to_string(),
            active_turns,
            queued_turns,
            inflight_children: counts.inflight_children,
            descendant_processes: counts.descendant_processes,
            busy,
        }
    }

    /// Render this snapshot as a `stream-json` heartbeat frame emitted on stdout.
    pub fn to_heartbeat_event(&self) -> Value {
        json!({
            "type": "system",
            "subtype": "heartbeat",
            "pid": self.pid,
            "timestamp_ms": self.timestamp_ms,
            "harness": self.harness,
            "session_id": self.session_id,
            "turn_state": self.turn_state,
            "active_turns": self.active_turns,
            "queued_turns": self.queued_turns,
            "inflight_children": self.inflight_children,
            "descendant_processes": self.descendant_processes,
            "busy": self.busy,
        })
    }
}

/// Persist `snapshot` atomically into `<state_dir>/.guest-activity.json`.
pub fn write_activity_snapshot(
    state_dir: &Path,
    snapshot: &GuestActivitySnapshot,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(snapshot).map_err(|e| e.to_string())?;
    let path = state_dir.join(ACTIVITY_STATE_FILE);
    anyagents_harness::atomic_write_nonempty_file(&path, &bytes, 0o600).map_err(|e| e.to_string())
}

/// Remove `<state_dir>/.guest-activity.json` on clean supervisor exit.
pub fn clear_activity_snapshot(state_dir: &Path) {
    let _ = std::fs::remove_file(state_dir.join(ACTIVITY_STATE_FILE));
}

/// Load `<state_dir>/.guest-activity.json` and refresh its process counts from live `/proc`.
///
/// Returns `None` if no snapshot exists or the recorded `serve` PID is no longer alive.
pub fn read_live_activity_snapshot(state_dir: &Path) -> Option<GuestActivitySnapshot> {
    let raw = std::fs::read_to_string(state_dir.join(ACTIVITY_STATE_FILE)).ok()?;
    let mut snap: GuestActivitySnapshot = serde_json::from_str(&raw).ok()?;
    if !is_pid_alive(snap.pid) {
        return None;
    }
    let counts = inspect_process_tree(snap.pid);
    snap.descendant_processes = counts.descendant_processes;
    snap.inflight_children = counts.inflight_children;
    snap.busy = snap.active_turns > 0
        || snap.queued_turns > 0
        || snap.turn_state != "idle"
        || snap.inflight_children > 0;
    snap.timestamp_ms = now_unix_ms();
    Some(snap)
}

/// Build the JSON payload printed by `anyagents-guest health`.
///
/// Preserves the legacy fields (`status`, `binary`, `harnesses`) while adding live
/// remote activity fields (`active_turns`, `inflight_children`, `descendant_processes`, `busy`).
pub fn build_health_payload(state_dir: &Path, workdir: &Path) -> Value {
    if let Some(live) = read_live_activity_snapshot(state_dir) {
        json!({
            "status": "ok",
            "binary": "anyagents-guest",
            "harnesses": 9,
            "serve_pid": live.pid,
            "harness": live.harness,
            "session_id": live.session_id,
            "turn_state": live.turn_state,
            "active_turns": live.active_turns,
            "queued_turns": live.queued_turns,
            "inflight_children": live.inflight_children,
            "descendant_processes": live.descendant_processes,
            "busy": live.busy,
            "state_dir": state_dir.display().to_string(),
            "workdir": workdir.display().to_string(),
        })
    } else {
        json!({
            "status": "ok",
            "binary": "anyagents-guest",
            "harnesses": 9,
            "serve_pid": Value::Null,
            "turn_state": "idle",
            "active_turns": 0,
            "queued_turns": 0,
            "inflight_children": 0,
            "descendant_processes": 0,
            "busy": false,
            "state_dir": state_dir.display().to_string(),
            "workdir": workdir.display().to_string(),
        })
    }
}

/// Inspect live non-zombie descendants of `root_pid` using Linux `/proc`.
pub fn inspect_process_tree(root_pid: u32) -> ProcessTreeCounts {
    #[cfg(target_os = "linux")]
    {
        inspect_process_tree_in(Path::new("/proc"), root_pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root_pid;
        ProcessTreeCounts::default()
    }
}

/// Check whether `pid` currently refers to a live, non-zombie process.
pub fn is_pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        let stat_path = Path::new("/proc").join(pid.to_string()).join("stat");
        let Ok(raw) = std::fs::read_to_string(stat_path) else {
            return false;
        };
        parse_proc_stat(&raw).is_some_and(|(state, _)| state != 'Z' && state != 'X')
    }
    #[cfg(not(target_os = "linux"))]
    {
        pid == std::process::id()
    }
}

#[derive(Debug, Clone)]
struct ProcEntry {
    pid: u32,
    ppid: u32,
    state: char,
    is_mcp_proxy: bool,
}

/// Inspect a Linux `/proc`-formatted directory rooted at `proc_root` for descendants of `root_pid`.
pub fn inspect_process_tree_in(proc_root: &Path, root_pid: u32) -> ProcessTreeCounts {
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return ProcessTreeCounts::default();
    };

    let mut children_by_ppid: HashMap<u32, Vec<ProcEntry>> = HashMap::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        let Ok(pid) = name_str.parse::<u32>() else {
            continue;
        };
        let dir = entry.path();
        let Ok(stat_raw) = std::fs::read_to_string(dir.join("stat")) else {
            continue;
        };
        let Some((state, ppid)) = parse_proc_stat(&stat_raw) else {
            continue;
        };
        if state == 'Z' || state == 'X' {
            continue;
        }
        let is_mcp_proxy = std::fs::read(dir.join("cmdline"))
            .ok()
            .is_some_and(|bytes| is_mcp_proxy_cmdline(&bytes));
        children_by_ppid.entry(ppid).or_default().push(ProcEntry {
            pid,
            ppid,
            state,
            is_mcp_proxy,
        });
    }

    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(root_pid);
    let mut queue: VecDeque<(u32, usize)> = VecDeque::new();
    queue.push_back((root_pid, 0));

    let mut descendant_processes = 0usize;
    let mut inflight_children = 0usize;
    let mut resident_direct_harness_seen = false;

    while let Some((cur_pid, depth)) = queue.pop_front() {
        let Some(kids) = children_by_ppid.get(&cur_pid) else {
            continue;
        };
        let mut sorted_kids = kids.clone();
        sorted_kids.sort_by_key(|k| k.pid);
        for kid in sorted_kids {
            if !visited.insert(kid.pid) {
                continue;
            }
            let _ = (kid.ppid, kid.state);
            let next_depth = depth + 1;
            descendant_processes += 1;

            if !kid.is_mcp_proxy {
                if next_depth == 1 && !resident_direct_harness_seen {
                    // The single resident harness CLI directly supervised by `anyagents-guest serve`
                    // remains alive while idle between turns.
                    resident_direct_harness_seen = true;
                } else {
                    inflight_children += 1;
                }
            }
            queue.push_back((kid.pid, next_depth));
        }
    }

    ProcessTreeCounts {
        descendant_processes,
        inflight_children,
    }
}

/// Parse `(state_char, ppid)` from Linux `/proc/<pid>/stat`, tolerating spaces and parentheses in `comm`.
pub(crate) fn parse_proc_stat(raw: &str) -> Option<(char, u32)> {
    let close_paren = raw.rfind(')')?;
    let rest = raw.get(close_paren + 1..)?.trim_start();
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse::<u32>().ok()?;
    Some((state, ppid))
}

fn is_mcp_proxy_cmdline(cmdline_nul: &[u8]) -> bool {
    let args: Vec<&str> = cmdline_nul
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .filter_map(|s| std::str::from_utf8(s).ok())
        .collect();
    args.contains(&"mcp-proxy")
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_proc_node(
        root: &Path,
        pid: u32,
        comm: &str,
        state: char,
        ppid: u32,
        cmdline: &[&str],
    ) {
        let dir = root.join(pid.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("stat"),
            format!("{pid} ({comm}) {state} {ppid} 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 100"),
        )
        .unwrap();
        let mut raw = Vec::new();
        for arg in cmdline {
            raw.extend_from_slice(arg.as_bytes());
            raw.push(0);
        }
        std::fs::write(dir.join("cmdline"), raw).unwrap();
    }

    #[test]
    fn proc_stat_parser_handles_spaces_and_parentheses_in_comm() {
        let line = "42 (node (worker pool) 1) S 10 1 1 0 -1";
        assert_eq!(parse_proc_stat(line), Some(('S', 10)));
    }

    #[test]
    fn synthetic_proc_tree_distinguishes_idle_harness_from_inflight_subprocesses() {
        let tmp = tempfile::tempdir().unwrap();
        let proc_root = tmp.path();

        // PID 100 = anyagents-guest serve
        write_proc_node(
            proc_root,
            100,
            "anyagents-guest",
            'S',
            1,
            &["anyagents-guest", "serve"],
        );
        // PID 101 = resident harness CLI (direct child, idle between turns)
        write_proc_node(proc_root, 101, "claude", 'S', 100, &["claude", "-p"]);
        // PID 102 = resident mcp-proxy child of harness (excluded from inflight_children)
        write_proc_node(
            proc_root,
            102,
            "anyagents-guest",
            'S',
            101,
            &[
                "/usr/local/bin/anyagents-guest",
                "mcp-proxy",
                "--url",
                "https://coord/mcp",
            ],
        );
        // PID 103 = zombie child (must be ignored)
        write_proc_node(proc_root, 103, "defunct", 'Z', 101, &["sh"]);

        let idle_counts = inspect_process_tree_in(proc_root, 100);
        assert_eq!(idle_counts.descendant_processes, 2);
        assert_eq!(idle_counts.inflight_children, 0);

        // Now add a detached build subprocess (PID 200 -> PID 201 cargo) under harness PID 101.
        write_proc_node(
            proc_root,
            200,
            "sh",
            'S',
            101,
            &["/bin/sh", "-c", "cargo test"],
        );
        write_proc_node(proc_root, 201, "cargo (test)", 'R', 200, &["cargo", "test"]);

        let busy_counts = inspect_process_tree_in(proc_root, 100);
        assert_eq!(busy_counts.descendant_processes, 4);
        assert_eq!(busy_counts.inflight_children, 2);
    }

    #[test]
    fn health_payload_reports_idle_defaults_when_no_serve_running() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let work_dir = tmp.path().join("workspace");
        let payload = build_health_payload(&state_dir, &work_dir);
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["binary"], "anyagents-guest");
        assert_eq!(payload["harnesses"], 9);
        assert_eq!(payload["active_turns"], 0);
        assert_eq!(payload["inflight_children"], 0);
        assert_eq!(payload["busy"], false);
    }
}
