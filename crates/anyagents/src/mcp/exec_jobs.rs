//! Background command jobs for `workspace.exec`.
//!
//! A command that outlives one tool call runs as a job. Its output goes to
//! files under `<config>/state-home/anyagents-jobs/<id>/`, and a shell
//! wrapper writes the exit code beside them, so the record survives the tool
//! server that started the job. Later calls read, await or kill it by id.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Under the agent's `config/state-home/`.
pub(super) const JOBS_DIR: &str = "anyagents-jobs";
const DEFAULT_TAIL_BYTES: usize = 8 * 1024;
const MAX_TAIL_BYTES: usize = 64 * 1024;
/// A finished job's record is kept this long, then removed when the next job
/// starts.
const KEEP_FINISHED: Duration = Duration::from_secs(24 * 3600);
const MAX_LISTED: usize = 50;
const POLL_EVERY: Duration = Duration::from_millis(100);

/// The wrapper the command runs under: the command's exit code lands in
/// `exit` when it ends, whoever is still watching.
const WRAPPER: &str = r#""$BASH" -c "$1"; code=$?; printf '%s\n' "$code" > "$2/exit""#;

#[derive(serde::Serialize, serde::Deserialize)]
struct JobRecord {
    id: String,
    cmd: String,
    cwd: String,
    pid: u32,
    started_at_ms: i64,
    /// Unix ms after which the job is killed, when it was started with a
    /// `timeout_ms`.
    #[serde(default)]
    deadline_ms: Option<i64>,
}

fn now_ms() -> i64 {
    anyagents_core::now_ms()
}

/// Start `cmd` in `cwd` as a job and return its handle at once.
pub(super) fn start(
    jobs_root: &Path,
    cmd: &str,
    cwd: &Path,
    timeout_ms: Option<u64>,
) -> Result<serde_json::Value, String> {
    prune(jobs_root);
    let id = format!(
        "job_{}",
        anyagents_core::AgentId::generate()
            .as_str()
            .trim_start_matches("agt_")
    );
    let dir = jobs_root.join(&id);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("cannot create job directory {}: {e}", dir.display()))?;
    let stdout = std::fs::File::create(dir.join("stdout"))
        .map_err(|e| format!("cannot create job output file: {e}"))?;
    let stderr = std::fs::File::create(dir.join("stderr"))
        .map_err(|e| format!("cannot create job output file: {e}"))?;
    let mut command = std::process::Command::new("/bin/bash");
    command
        .arg("-c")
        .arg(WRAPPER)
        .arg("anyagents-job")
        .arg(cmd)
        .arg(&dir)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    let child = command
        .spawn()
        .map_err(|e| format!("failed to start job: {e}"))?;
    let started_at_ms = now_ms();
    let record = JobRecord {
        id: id.clone(),
        cmd: cmd.to_string(),
        cwd: cwd.display().to_string(),
        pid: child.id(),
        started_at_ms,
        deadline_ms: timeout_ms.map(|ms| started_at_ms.saturating_add(ms as i64)),
    };
    let text = serde_json::to_vec_pretty(&record).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("job.json"), text)
        .map_err(|e| format!("cannot record job {id}: {e}"))?;
    watch(child, dir.clone(), timeout_ms.map(Duration::from_millis));
    Ok(serde_json::json!({
        "action": "exec",
        "job_id": id,
        "status": "running",
        "cmd": cmd,
        "cwd": record.cwd,
        "pid": record.pid,
        "timeout_ms": timeout_ms,
        "next": format!("workspace.job({{id: \"{id}\", wait_ms: 60000}}) returns status, exit_code and the output tail; add `kill: true` to stop it"),
    }))
}

/// Reap the child and enforce its deadline while this process lives. A job
/// whose watcher is gone still records its own exit through the wrapper.
fn watch(mut child: std::process::Child, dir: PathBuf, timeout: Option<Duration>) {
    let started = Instant::now();
    std::thread::spawn(move || {
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    record_end(&dir, status.code());
                    return;
                }
                Ok(None) => {}
                Err(_) => return,
            }
            if timeout.is_some_and(|limit| started.elapsed() >= limit) {
                let _ = std::fs::write(dir.join("timed_out"), b"");
                kill_group(child.id());
                let _ = child.wait();
                record_end(&dir, None);
                return;
            }
            std::thread::sleep(POLL_EVERY);
        }
    });
}

/// The wrapper writes `exit` when the command ends; a job killed by a signal
/// never reaches that line, so the watcher writes it.
fn record_end(dir: &Path, code: Option<i32>) {
    if read_exit(dir).is_none() {
        let text = code.map_or_else(|| "signal\n".to_string(), |c| format!("{c}\n"));
        let _ = std::fs::write(dir.join("exit"), text);
    }
}

fn kill_group(pid: u32) {
    #[cfg(unix)]
    if let Ok(pgid) = i32::try_from(pid)
        && pgid > 0
    {
        // SAFETY: signalling a process group this job created.
        unsafe {
            libc::kill(-pgid, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && group_alive(pgid) {
            std::thread::sleep(Duration::from_millis(50));
        }
        // SAFETY: as above.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

#[cfg(unix)]
fn group_alive(pgid: i32) -> bool {
    // SAFETY: signal 0 only checks that the group exists.
    unsafe { libc::kill(-pgid, 0) == 0 }
}

#[cfg(not(unix))]
fn group_alive(_pgid: i32) -> bool {
    false
}

/// `Some(Some(code))` for an exit, `Some(None)` for a signal, `None` while
/// the job runs.
fn read_exit(dir: &Path) -> Option<Option<i32>> {
    let text = std::fs::read_to_string(dir.join("exit")).ok()?;
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    Some(text.parse::<i32>().ok())
}

fn job_dir(jobs_root: &Path, id: &str) -> Result<PathBuf, String> {
    let id = id.trim();
    let valid = id
        .strip_prefix("job_")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_alphanumeric()));
    if !valid {
        return Err(format!(
            "`{id}` is not a job id; pass the `job_id` that `workspace.exec({{cmd, background: true}})` returned"
        ));
    }
    let dir = jobs_root.join(id);
    if !dir.join("job.json").is_file() {
        return Err(format!(
            "no job `{id}`; `workspace.job({{}})` lists this agent's jobs"
        ));
    }
    Ok(dir)
}

fn read_record(dir: &Path) -> Result<JobRecord, String> {
    let text = std::fs::read(dir.join("job.json")).map_err(|e| e.to_string())?;
    serde_json::from_slice(&text).map_err(|e| e.to_string())
}

/// The job's state, after waiting up to `wait_ms` for it to end.
pub(super) fn status(
    jobs_root: &Path,
    id: &str,
    wait_ms: u64,
    tail_bytes: Option<usize>,
) -> Result<serde_json::Value, String> {
    let dir = job_dir(jobs_root, id)?;
    let record = read_record(&dir)?;
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    let mut gone_without_record = false;
    loop {
        enforce_deadline(&dir, &record);
        if read_exit(&dir).is_some() || Instant::now() >= deadline {
            break;
        }
        if !alive(&record) {
            // The watcher records a signalled end just after it reaps the
            // process; give it one poll before calling the job lost.
            if gone_without_record {
                break;
            }
            gone_without_record = true;
        }
        std::thread::sleep(POLL_EVERY);
    }
    Ok(view(
        &dir,
        &record,
        tail_bytes.unwrap_or(DEFAULT_TAIL_BYTES).min(MAX_TAIL_BYTES),
    ))
}

/// A job past its deadline whose watcher is gone is killed by whoever looks.
fn enforce_deadline(dir: &Path, record: &JobRecord) {
    if read_exit(dir).is_none()
        && record.deadline_ms.is_some_and(|at| now_ms() >= at)
        && alive(record)
    {
        let _ = std::fs::write(dir.join("timed_out"), b"");
        kill_group(record.pid);
        record_end(dir, None);
    }
}

fn alive(record: &JobRecord) -> bool {
    i32::try_from(record.pid).is_ok_and(|pgid| pgid > 0 && group_alive(pgid))
}

/// Stop the job and return its final state.
pub(super) fn kill(jobs_root: &Path, id: &str) -> Result<serde_json::Value, String> {
    let dir = job_dir(jobs_root, id)?;
    let record = read_record(&dir)?;
    if read_exit(&dir).is_none() {
        let _ = std::fs::write(dir.join("killed"), b"");
        kill_group(record.pid);
        record_end(&dir, None);
    }
    Ok(view(&dir, &record, DEFAULT_TAIL_BYTES))
}

/// This agent's jobs, newest first.
pub(super) fn list(jobs_root: &Path) -> Result<serde_json::Value, String> {
    let mut jobs: Vec<(i64, serde_json::Value)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(jobs_root) {
        for entry in entries.flatten() {
            let dir = entry.path();
            let Ok(record) = read_record(&dir) else {
                continue;
            };
            let (state, exit_code) = state_of(&dir, &record);
            jobs.push((
                record.started_at_ms,
                serde_json::json!({
                    "job_id": record.id,
                    "status": state,
                    "exit_code": exit_code,
                    "cmd": preview(&record.cmd, 200),
                    "started_at_ms": record.started_at_ms,
                }),
            ));
        }
    }
    jobs.sort_by_key(|job| std::cmp::Reverse(job.0));
    let total = jobs.len();
    let jobs: Vec<_> = jobs.into_iter().take(MAX_LISTED).map(|(_, v)| v).collect();
    Ok(serde_json::json!({ "action": "job", "jobs": jobs, "total": total }))
}

fn state_of(dir: &Path, record: &JobRecord) -> (&'static str, Option<i32>) {
    match read_exit(dir) {
        Some(code) => {
            let state = if dir.join("timed_out").exists() {
                "timed_out"
            } else if dir.join("killed").exists() {
                "killed"
            } else if code.is_some() {
                "exited"
            } else {
                "killed"
            };
            (state, code)
        }
        None if alive(record) => ("running", None),
        // Neither an exit record nor a process: the machine or the
        // wrapper went away under it.
        None => ("lost", None),
    }
}

fn view(dir: &Path, record: &JobRecord, tail_bytes: usize) -> serde_json::Value {
    let (state, exit_code) = state_of(dir, record);
    let (stdout, stdout_bytes) = tail(&dir.join("stdout"), tail_bytes);
    let (stderr, stderr_bytes) = tail(&dir.join("stderr"), tail_bytes);
    let ended_ms = std::fs::metadata(dir.join("exit"))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);
    let duration_ms = ended_ms
        .unwrap_or_else(now_ms)
        .saturating_sub(record.started_at_ms)
        .max(0);
    serde_json::json!({
        "action": "job",
        "job_id": record.id,
        "status": state,
        "exit_code": exit_code,
        "cmd": record.cmd,
        "cwd": record.cwd,
        "duration_ms": duration_ms,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
        "output_dir": dir.display().to_string(),
    })
}

/// The last `max` bytes of a file, cut at a character boundary, and the
/// file's whole size.
fn tail(path: &Path, max: usize) -> (String, u64) {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return (String::new(), 0);
    };
    let size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let start = size.saturating_sub(max as u64);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return (String::new(), size);
    }
    let mut buf = Vec::with_capacity(max.min(size as usize));
    let _ = file.take(max as u64).read_to_end(&mut buf);
    let mut text = String::from_utf8_lossy(&buf).into_owned();
    if start > 0 {
        // Drop a character the cut split in two.
        if let Some(pos) = text.find(|c: char| c != '\u{FFFD}') {
            text.drain(..pos);
        }
        text.insert_str(0, "...[earlier output omitted]...\n");
    }
    (text, size)
}

fn preview(text: &str, max_chars: usize) -> String {
    let mut out: String = text.chars().take(max_chars).collect();
    if text.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

/// Remove the records of jobs that ended more than a day ago.
fn prune(jobs_root: &Path) {
    let Ok(entries) = std::fs::read_dir(jobs_root) else {
        return;
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        let ended_long_ago = std::fs::metadata(dir.join("exit"))
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= KEEP_FINISHED);
        if ended_long_ago {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_for_status(root: &Path, id: &str, wanted: &str) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let view = status(root, id, 200, None).unwrap();
            if view["status"] == wanted || Instant::now() >= deadline {
                return view;
            }
        }
    }

    #[test]
    fn a_job_runs_past_its_call_and_reports_exit_and_output() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(JOBS_DIR);
        let started = start(
            &root,
            "echo begin; sleep 0.3; echo out-line; echo err-line >&2; exit 3",
            dir.path(),
            None,
        )
        .unwrap();
        assert_eq!(started["status"], "running");
        let id = started["job_id"].as_str().unwrap().to_string();
        assert!(id.starts_with("job_"), "{id}");
        assert!(started["next"].as_str().unwrap().contains(&id));

        let done = status(&root, &id, 20_000, None).unwrap();
        assert_eq!(done["status"], "exited", "{done}");
        assert_eq!(done["exit_code"], 3, "{done}");
        assert_eq!(done["stdout"], "begin\nout-line\n");
        assert_eq!(done["stderr"], "err-line\n");
        assert_eq!(done["stdout_bytes"], 15);

        let listed = list(&root).unwrap();
        assert_eq!(listed["jobs"][0]["job_id"], id.as_str());
        assert_eq!(listed["jobs"][0]["status"], "exited");
    }

    #[test]
    fn polling_without_waiting_returns_at_once_and_kill_stops_the_group() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(JOBS_DIR);
        let marker = dir.path().join("leaked");
        let cmd = format!("(sleep 1; echo leaked > '{}') & sleep 30", marker.display());
        let started = start(&root, &cmd, dir.path(), None).unwrap();
        let id = started["job_id"].as_str().unwrap().to_string();
        let begun = Instant::now();
        let running = status(&root, &id, 0, None).unwrap();
        assert!(begun.elapsed() < Duration::from_secs(2));
        assert_eq!(running["status"], "running", "{running}");
        let killed = kill(&root, &id).unwrap();
        assert_eq!(killed["status"], "killed", "{killed}");
        std::thread::sleep(Duration::from_millis(1_500));
        assert!(
            !marker.exists(),
            "a background process of the job outlived the kill"
        );
        assert_eq!(status(&root, &id, 0, None).unwrap()["status"], "killed");
    }

    #[test]
    fn a_job_with_a_timeout_is_killed_at_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(JOBS_DIR);
        let started = start(&root, "sleep 30", dir.path(), Some(300)).unwrap();
        let id = started["job_id"].as_str().unwrap().to_string();
        let view = wait_for_status(&root, &id, "timed_out");
        assert_eq!(view["status"], "timed_out", "{view}");
        assert_eq!(view["exit_code"], serde_json::Value::Null);
    }

    #[test]
    fn job_ids_are_checked_before_any_path_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(JOBS_DIR);
        for bad in ["../config", "job_../../x", "sch_123", "", "job_"] {
            let err = status(&root, bad, 0, None).unwrap_err();
            assert!(err.contains("is not a job id"), "{bad}: {err}");
        }
        let err = status(&root, "job_01doesnotexist", 0, None).unwrap_err();
        assert!(err.contains("no job"), "{err}");
    }

    #[test]
    fn the_tail_keeps_the_end_of_long_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let body: String = (0..2000).map(|i| format!("line {i}\n")).collect();
        std::fs::write(&path, &body).unwrap();
        let (text, size) = tail(&path, 64);
        assert_eq!(size, body.len() as u64);
        assert!(
            text.starts_with("...[earlier output omitted]...\n"),
            "{text}"
        );
        assert!(text.ends_with("line 1999\n"), "{text}");
        let (whole, _) = tail(&path, MAX_TAIL_BYTES);
        assert!(whole.ends_with("line 1999\n"));
    }

    #[test]
    fn finished_jobs_are_pruned_after_a_day_and_running_ones_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(JOBS_DIR);
        let old = root.join("job_old");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("exit"), "0\n").unwrap();
        let day_ago = std::time::SystemTime::now() - KEEP_FINISHED - Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(old.join("exit"))
            .unwrap()
            .set_modified(day_ago)
            .unwrap();
        let running = root.join("job_running");
        std::fs::create_dir_all(&running).unwrap();
        prune(&root);
        assert!(!old.exists());
        assert!(running.exists());
    }
}
