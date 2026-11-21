//! Non-blocking live native transcript tailer & coordinator relay for `anyagents-guest turn`.
//!
//! Discovers and tails active native harness transcripts under `state_dir` during a turn
//! (`agy`, `claude`, `codex`, `omp`, `muse`, `grok`, `spiron` JSONL & `sessions.sqlite3`)
//! and flushes normalized, **unmasked / lossless** batches to the coordinator via
//! `POST /v1/internal/guest/events` (`event_type: "transcript_entries"`).
//!
//! Safety invariants:
//! - Snapshots pre-existing file lengths / SQLite rowids at turn start so old turns are
//!   never replayed; brand-new files created during this turn start from byte offset `0`.
//! - Never splits partial writes: only complete `\n`-terminated JSON lines advance offsets.
//! - Keeps committed offsets un-advanced when a batch POST transiently fails so the next
//!   tick retries cleanly (idempotent on `(agent_id, source, seq)`).
//! - Never fails or blocks a turn on transient relay errors.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::turn::post_callback_with_retry;

mod parse;

#[cfg(test)]
use self::parse::parse_rfc3339_ms;
use self::parse::{normalize_json_event, now_unix_ms};

const RESCAN_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const FLUSH_INTERVAL: Duration = Duration::from_millis(1_500);
const FLUSH_BYTE_THRESHOLD: u64 = 16 * 1024;
const MAX_BATCH_ENTRIES: usize = 64;
const MAX_BATCH_BYTES: usize = 512 * 1024;
const MAX_READ_CHUNK_BYTES: usize = 512 * 1024;
const MAX_SINGLE_LINE_BYTES: usize = 8 * 1024 * 1024;
const RELAY_TIMEOUT_SECS: u64 = 5;
const MAX_BATCHES_PER_TICK: usize = 4;
const MAX_BATCHES_ON_FINAL_FLUSH: usize = 64;
const MAX_WALK_DEPTH: usize = 8;
const MAX_WALK_ENTRIES: usize = 4_096;

/// One normalized, lossless, unmasked turn transcript event posted to the coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestTranscriptEntry {
    pub source: String,
    pub seq: i64,
    pub timestamp_ms: i64,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    pub text: String,
    pub raw_json: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceFormat {
    Agy,
    Claude,
    Codex,
    SpironJsonl,
    GenericJsonl,
}

#[derive(Clone, Debug)]
struct FileCursor {
    path: PathBuf,
    source: String,
    format: SourceFormat,
    committed_offset: u64,
    call_names: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct SqliteCursor {
    path: PathBuf,
    source: String,
    committed_rowid: i64,
    last_stamp: (u64, u64),
    call_names: HashMap<String, String>,
}

struct PendingBatch {
    entries: Vec<GuestTranscriptEntry>,
    file_advances: Vec<(String, u64)>,
    sqlite_advance: Option<i64>,
}

/// Live transcript tailer owned by `anyagents-guest turn`.
pub(crate) struct TranscriptRelay {
    state_dir: PathBuf,
    harness: String,
    turn_id: String,
    agent_id: String,
    callback_url: String,
    callback_token: String,
    files: BTreeMap<String, FileCursor>,
    sqlite: Option<SqliteCursor>,
    last_rescan: Instant,
    last_poll: Instant,
    last_flush: Instant,
    rescan_interval: Duration,
    poll_interval: Duration,
    flush_interval: Duration,
}

impl TranscriptRelay {
    /// Snapshot all currently existing transcript files at EOF so prior turns are
    /// never re-uploaded, while brand-new files created during this turn start at 0.
    pub(crate) fn new(
        state_dir: &Path,
        harness: &str,
        turn_id: &str,
        agent_id: &str,
        callback_url: &str,
        callback_token: &str,
    ) -> Self {
        let now = Instant::now();
        let mut relay = Self {
            state_dir: state_dir.to_path_buf(),
            harness: harness.to_string(),
            turn_id: turn_id.to_string(),
            agent_id: agent_id.to_string(),
            callback_url: callback_url.to_string(),
            callback_token: callback_token.to_string(),
            files: BTreeMap::new(),
            sqlite: None,
            last_rescan: now,
            last_poll: now,
            last_flush: now,
            rescan_interval: RESCAN_INTERVAL,
            poll_interval: POLL_INTERVAL,
            flush_interval: FLUSH_INTERVAL,
        };
        relay.discover_sources(true);
        relay
    }

    #[cfg(test)]
    fn with_intervals(
        mut self,
        rescan_interval: Duration,
        poll_interval: Duration,
        flush_interval: Duration,
    ) -> Self {
        self.rescan_interval = rescan_interval;
        self.poll_interval = poll_interval;
        self.flush_interval = flush_interval;
        self.last_rescan = Instant::now() - rescan_interval;
        self.last_poll = Instant::now() - poll_interval;
        self.last_flush = Instant::now() - flush_interval;
        self
    }

    /// Non-blocking poll tick called inside the turn loop. Returns `true` when any
    /// new transcript entries were successfully flushed (useful for watchdog reset).
    pub(crate) fn tick(&mut self, active_session_id: Option<&str>) -> bool {
        if self.last_poll.elapsed() < self.poll_interval {
            return false;
        }
        self.last_poll = Instant::now();
        if self.last_rescan.elapsed() >= self.rescan_interval {
            self.last_rescan = Instant::now();
            self.discover_sources(false);
        }

        let unread_bytes = self.estimate_unread_bytes();
        let sqlite_changed = self.sqlite_has_changes();
        if unread_bytes == 0 && !sqlite_changed {
            return false;
        }
        let flush_due = unread_bytes >= FLUSH_BYTE_THRESHOLD
            || self.last_flush.elapsed() >= self.flush_interval;
        if !flush_due {
            return false;
        }
        self.flush_batches(active_session_id, MAX_BATCHES_PER_TICK, 0) > 0
    }

    /// Final drain right before posting `turn_complete` or `turn_failed`.
    /// Scans for any newly created files and flushes all complete pending lines.
    pub(crate) fn flush_all(&mut self, active_session_id: Option<&str>) -> usize {
        self.discover_sources(false);
        self.flush_batches(active_session_id, MAX_BATCHES_ON_FINAL_FLUSH, 2)
    }

    fn flush_batches(
        &mut self,
        active_session_id: Option<&str>,
        max_batches: usize,
        max_retries: usize,
    ) -> usize {
        let mut total_flushed = 0usize;
        for _ in 0..max_batches {
            let batch = self.collect_next_batch(active_session_id);
            if batch.entries.is_empty() {
                // Still advance offsets if only blank/unparseable lines were skipped.
                if !batch.file_advances.is_empty() || batch.sqlite_advance.is_some() {
                    self.commit_advances(&batch.file_advances, batch.sqlite_advance);
                    continue;
                }
                break;
            }
            let count = batch.entries.len();
            let payload = json!({
                "event_type": "transcript_entries",
                "turn_id": self.turn_id,
                "agent_id": self.agent_id,
                "harness": self.harness,
                "session_id": active_session_id,
                "timestamp_ms": now_unix_ms(),
                "entries": batch.entries,
            });
            self.last_flush = Instant::now();
            match post_callback_with_retry(
                &self.callback_url,
                &self.callback_token,
                &payload,
                max_retries,
                RELAY_TIMEOUT_SECS,
            ) {
                Ok(()) => {
                    self.commit_advances(&batch.file_advances, batch.sqlite_advance);
                    total_flushed = total_flushed.saturating_add(count);
                    if count < MAX_BATCH_ENTRIES {
                        // Check if more unread bytes exist on another source; if not, stop.
                        if self.estimate_unread_bytes() == 0 && !self.sqlite_has_changes() {
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Keep committed offsets un-advanced so the next tick retries cleanly.
                    break;
                }
            }
        }
        total_flushed
    }

    fn commit_advances(&mut self, file_advances: &[(String, u64)], sqlite_advance: Option<i64>) {
        for (source, offset) in file_advances {
            if let Some(cursor) = self.files.get_mut(source) {
                cursor.committed_offset = *offset;
            }
        }
        if let (Some(rowid), Some(sq)) = (sqlite_advance, self.sqlite.as_mut()) {
            sq.committed_rowid = rowid;
            sq.last_stamp = sqlite_file_stamp(&sq.path);
        }
    }

    fn discover_sources(&mut self, initial_snapshot: bool) {
        for (path, format) in discover_jsonl_sources(&self.state_dir) {
            let Some(source) = normalize_relative_source(&self.state_dir, &path) else {
                continue;
            };
            if self.files.contains_key(&source) {
                continue;
            }
            let committed_offset = if initial_snapshot {
                std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
            } else {
                0
            };
            self.files.insert(
                source.clone(),
                FileCursor {
                    path,
                    source,
                    format,
                    committed_offset,
                    call_names: HashMap::new(),
                },
            );
        }

        if self.sqlite.is_none() {
            let db_path = self.state_dir.join("sessions.sqlite3");
            if is_regular_non_symlink(&db_path) {
                let committed_rowid = if initial_snapshot {
                    read_spiron_sqlite_max_rowid(&db_path).unwrap_or(0)
                } else {
                    0
                };
                self.sqlite = Some(SqliteCursor {
                    last_stamp: sqlite_file_stamp(&db_path),
                    path: db_path,
                    source: "sessions.sqlite3".to_string(),
                    committed_rowid,
                    call_names: HashMap::new(),
                });
            }
        }
    }

    fn estimate_unread_bytes(&self) -> u64 {
        let mut total = 0u64;
        for cursor in self.files.values() {
            let Ok(meta) = std::fs::symlink_metadata(&cursor.path) else {
                continue;
            };
            if !meta.file_type().is_file() {
                continue;
            }
            let len = meta.len();
            if len < cursor.committed_offset {
                total = total.saturating_add(len);
            } else {
                total = total.saturating_add(len - cursor.committed_offset);
            }
        }
        total
    }

    fn sqlite_has_changes(&self) -> bool {
        let Some(ref sq) = self.sqlite else {
            return false;
        };
        sqlite_file_stamp(&sq.path) != sq.last_stamp || sq.committed_rowid == 0
    }

    fn collect_next_batch(&mut self, active_session_id: Option<&str>) -> PendingBatch {
        let mut entries = Vec::new();
        let mut batch_bytes = 0usize;
        let mut file_advances = Vec::new();

        for cursor in self.files.values_mut() {
            if entries.len() >= MAX_BATCH_ENTRIES || batch_bytes >= MAX_BATCH_BYTES {
                break;
            }
            let Ok(meta) = std::fs::symlink_metadata(&cursor.path) else {
                continue;
            };
            if !meta.file_type().is_file() {
                continue;
            }
            let file_len = meta.len();
            let start_offset = if file_len < cursor.committed_offset {
                0
            } else {
                cursor.committed_offset
            };
            if file_len == start_offset {
                continue;
            }
            let Some((buf, _eof)) = read_complete_lines_chunk(&cursor.path, start_offset) else {
                continue;
            };
            if buf.is_empty() {
                continue;
            }

            let mut pos = 0usize;
            let mut last_committed_offset = start_offset;
            while pos < buf.len() {
                let Some(rel_nl) = buf[pos..].iter().position(|&b| b == b'\n') else {
                    break;
                };
                let line_start_file_offset = start_offset + pos as u64;
                let line_end_in_buf = pos + rel_nl + 1;
                let line_end_file_offset = start_offset + line_end_in_buf as u64;
                let raw_line = &buf[pos..pos + rel_nl];
                pos = line_end_in_buf;

                let trimmed = trim_ascii_whitespace(raw_line);
                if trimmed.is_empty() {
                    last_committed_offset = line_end_file_offset;
                    continue;
                }

                let Ok(value) = serde_json::from_slice::<Value>(trimmed) else {
                    // Complete newline-terminated non-JSON line; advance past it.
                    last_committed_offset = line_end_file_offset;
                    continue;
                };

                let raw_json_full = String::from_utf8_lossy(trimmed).into_owned();
                let parsed = normalize_json_event(
                    &value,
                    &raw_json_full,
                    &cursor.source,
                    cursor.format,
                    line_start_file_offset as i64,
                    active_session_id,
                    &mut cursor.call_names,
                );
                let parsed_bytes: usize = parsed.iter().map(estimate_entry_bytes).sum();

                // Never split a single line across batches; always accept at least one line
                // when the batch is currently empty so oversized single lines cannot deadlock.
                if !entries.is_empty()
                    && (entries.len() + parsed.len() > MAX_BATCH_ENTRIES
                        || batch_bytes + parsed_bytes > MAX_BATCH_BYTES)
                {
                    break;
                }

                batch_bytes = batch_bytes.saturating_add(parsed_bytes);
                entries.extend(parsed);
                last_committed_offset = line_end_file_offset;
            }

            if last_committed_offset != cursor.committed_offset {
                file_advances.push((cursor.source.clone(), last_committed_offset));
            }
        }

        let mut sqlite_advance = None;
        if entries.len() < MAX_BATCH_ENTRIES
            && batch_bytes < MAX_BATCH_BYTES
            && let Some(sq) = self.sqlite.as_mut()
            && let Ok(rows) = read_spiron_sqlite_rows_after(
                &sq.path,
                sq.committed_rowid,
                MAX_BATCH_ENTRIES.saturating_sub(entries.len()).max(1),
            )
        {
            let mut last_rowid = sq.committed_rowid;
            for (rowid, row_sid, event_raw) in rows {
                let Ok(value) = serde_json::from_str::<Value>(&event_raw) else {
                    last_rowid = rowid;
                    continue;
                };
                let sid = row_sid
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .or(active_session_id);
                let parsed = normalize_json_event(
                    &value,
                    &event_raw,
                    &sq.source,
                    SourceFormat::SpironJsonl,
                    rowid,
                    sid,
                    &mut sq.call_names,
                );
                let parsed_bytes: usize = parsed.iter().map(estimate_entry_bytes).sum();
                if !entries.is_empty()
                    && (entries.len() + parsed.len() > MAX_BATCH_ENTRIES
                        || batch_bytes + parsed_bytes > MAX_BATCH_BYTES)
                {
                    break;
                }
                batch_bytes = batch_bytes.saturating_add(parsed_bytes);
                entries.extend(parsed);
                last_rowid = rowid;
            }
            if last_rowid > sq.committed_rowid {
                sqlite_advance = Some(last_rowid);
            }
        }

        PendingBatch {
            entries,
            file_advances,
            sqlite_advance,
        }
    }
}

fn estimate_entry_bytes(e: &GuestTranscriptEntry) -> usize {
    e.source.len() + e.text.len() + e.raw_json.len() + 128
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while let Some(&first) = bytes.first() {
        if first.is_ascii_whitespace() {
            bytes = &bytes[1..];
        } else {
            break;
        }
    }
    while let Some(&last) = bytes.last() {
        if last.is_ascii_whitespace() {
            bytes = &bytes[..bytes.len() - 1];
        } else {
            break;
        }
    }
    bytes
}

/// Read starting at `start_offset` up to the last complete `\n` in a bounded chunk.
/// If a single line exceeds `MAX_READ_CHUNK_BYTES`, extends reading up to
/// `MAX_SINGLE_LINE_BYTES` until `\n` so large tool outputs never stall.
fn read_complete_lines_chunk(path: &Path, start_offset: u64) -> Option<(Vec<u8>, bool)> {
    let mut file = File::open(path).ok()?;
    file.seek(SeekFrom::Start(start_offset)).ok()?;
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 32 * 1024];
    let mut eof = false;

    loop {
        let to_read = chunk
            .len()
            .min(MAX_SINGLE_LINE_BYTES.saturating_sub(buf.len()));
        if to_read == 0 {
            break;
        }
        let n = file.read(&mut chunk[..to_read]).ok()?;
        if n == 0 {
            eof = true;
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() >= MAX_READ_CHUNK_BYTES && buf.contains(&b'\n') {
            break;
        }
    }

    let last_nl = buf.iter().rposition(|&b| b == b'\n')?;
    buf.truncate(last_nl + 1);
    Some((buf, eof))
}

fn is_regular_non_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
}

fn normalize_relative_source(state_dir: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(state_dir).ok()?;
    let joined = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    (!joined.is_empty()).then_some(joined)
}

fn discover_jsonl_sources(state_dir: &Path) -> Vec<(PathBuf, SourceFormat)> {
    let mut out = Vec::new();

    // 1. AGY: .gemini/antigravity-cli/brain/*/.system_generated/logs/transcript_full.jsonl
    let brain = state_dir.join(".gemini/antigravity-cli/brain");
    for conv in list_subdirs(&brain) {
        let candidate = conv.join(".system_generated/logs/transcript_full.jsonl");
        if is_regular_non_symlink(&candidate) {
            out.push((candidate, SourceFormat::Agy));
        }
    }

    // 2. Claude Code: {.claude/,}projects/*/*.jsonl and subagents/*.jsonl
    for projects_root in [
        state_dir.join(".claude/projects"),
        state_dir.join("projects"),
    ] {
        for project in list_subdirs(&projects_root) {
            for f in list_jsonl_files(&project, false) {
                out.push((f, SourceFormat::Claude));
            }
            for sess_dir in list_subdirs(&project) {
                for f in list_jsonl_files(&sess_dir.join("subagents"), false) {
                    out.push((f, SourceFormat::Claude));
                }
            }
        }
    }

    // 3. Codex: sessions/**/*.jsonl and .codex/sessions/**/*.jsonl
    for codex_root in [
        state_dir.join("sessions"),
        state_dir.join(".codex/sessions"),
    ] {
        for f in list_jsonl_files(&codex_root, true) {
            out.push((f, SourceFormat::Codex));
        }
    }

    // 4. Spiron legacy JSONL
    let sylphx = state_dir.join("sylphx-transcript.jsonl");
    if is_regular_non_symlink(&sylphx) {
        out.push((sylphx, SourceFormat::SpironJsonl));
    }

    // 5. OMP: .omp/agent/sessions/*/*.jsonl
    for cwd in list_subdirs(&state_dir.join(".omp/agent/sessions")) {
        for f in list_jsonl_files(&cwd, false) {
            out.push((f, SourceFormat::GenericJsonl));
        }
    }

    // 6. Muse: data-home/muse/sessions/**/session.jsonl (skipping hidden subdirs)
    let muse = state_dir.join("data-home/muse/sessions");
    for f in list_jsonl_files(&muse, true) {
        if f.file_name().is_some_and(|n| n == "session.jsonl")
            && f.strip_prefix(&muse).is_ok_and(|rel| {
                !rel.components()
                    .any(|c| c.as_os_str().to_string_lossy().starts_with('.'))
            })
        {
            out.push((f, SourceFormat::GenericJsonl));
        }
    }

    // 7. Grok: .grok/sessions/*/*/chat_history.jsonl
    for cwd in list_subdirs(&state_dir.join(".grok/sessions")) {
        for sess in list_subdirs(&cwd) {
            let candidate = sess.join("chat_history.jsonl");
            if is_regular_non_symlink(&candidate) {
                out.push((candidate, SourceFormat::GenericJsonl));
            }
        }
    }

    // 8. Generic transcripts/**/*.jsonl
    for f in list_jsonl_files(&state_dir.join("transcripts"), true) {
        out.push((f, SourceFormat::GenericJsonl));
    }

    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn list_subdirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut dirs = Vec::new();
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    dirs
}

fn list_jsonl_files(root: &Path, recursive: bool) -> Vec<PathBuf> {
    if !std::fs::symlink_metadata(root).is_ok_and(|m| m.file_type().is_dir()) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut visited = 0usize;
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if visited >= MAX_WALK_ENTRIES {
                return out;
            }
            visited += 1;
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_symlink() {
                continue;
            }
            let path = entry.path();
            if ft.is_dir() {
                if recursive && depth < MAX_WALK_DEPTH {
                    stack.push((path, depth + 1));
                }
            } else if ft.is_file()
                && path
                    .extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| e.eq_ignore_ascii_case("jsonl"))
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn sqlite_file_stamp(db: &Path) -> (u64, u64) {
    let main_len = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    let wal_len = std::fs::metadata(PathBuf::from(wal))
        .map(|m| m.len())
        .unwrap_or(0);
    (main_len, wal_len)
}

fn open_spiron_sqlite_readonly(db: &Path) -> Option<Connection> {
    if !is_regular_non_symlink(db) {
        return None;
    }
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    let mut shm = db.as_os_str().to_owned();
    shm.push("-shm");
    let live = PathBuf::from(wal).exists() && PathBuf::from(shm).exists();
    let mut encoded = String::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        for &b in db.as_os_str().as_bytes() {
            if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.' | b'~') {
                encoded.push(b as char);
            } else {
                encoded.push_str(&format!("%{b:02X}"));
            }
        }
    }
    #[cfg(not(unix))]
    {
        encoded.push_str(&db.to_string_lossy());
    }
    let uri = if live {
        format!("file:{encoded}?mode=ro")
    } else {
        format!("file:{encoded}?mode=ro&immutable=1")
    };
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let mut stmt = conn.prepare("PRAGMA table_info(session_entries)").ok()?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .ok()?
        .filter_map(Result::ok)
        .collect();
    if ["session_id", "idx", "event"]
        .iter()
        .all(|w| cols.iter().any(|c| c == w))
    {
        drop(stmt);
        Some(conn)
    } else {
        None
    }
}

fn read_spiron_sqlite_max_rowid(db: &Path) -> Option<i64> {
    let conn = open_spiron_sqlite_readonly(db)?;
    conn.query_row(
        "SELECT COALESCE(MAX(rowid), 0) FROM session_entries",
        [],
        |r| r.get(0),
    )
    .ok()
}

fn read_spiron_sqlite_rows_after(
    db: &Path,
    after_rowid: i64,
    limit: usize,
) -> Result<Vec<(i64, Option<String>, String)>, String> {
    let Some(conn) = open_spiron_sqlite_readonly(db) else {
        return Ok(Vec::new());
    };
    let mut stmt = conn
        .prepare(
            "SELECT rowid, session_id, CAST(event AS TEXT)
             FROM session_entries
             WHERE rowid > ?1
             ORDER BY rowid ASC
             LIMIT ?2",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(rusqlite::params![after_rowid, limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in rows.flatten() {
        out.push(row);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write as _};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    struct MockServer {
        url: String,
        received: Arc<Mutex<Vec<Value>>>,
        fail_next: Arc<Mutex<usize>>,
        stop: Arc<std::sync::atomic::AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl MockServer {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let received = Arc::new(Mutex::new(Vec::new()));
            let fail_next = Arc::new(Mutex::new(0usize));
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let rec_bg = Arc::clone(&received);
            let fail_bg = Arc::clone(&fail_next);
            let stop_bg = Arc::clone(&stop);

            let handle = std::thread::spawn(move || {
                while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
                    let Ok((mut stream, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    };
                    let _ = stream.set_nonblocking(false);
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut content_len = 0usize;
                    let mut line = String::new();
                    while reader.read_line(&mut line).unwrap_or(0) > 0 {
                        if line == "\r\n" || line == "\n" {
                            break;
                        }
                        if let Some((k, v)) = line.split_once(':')
                            && k.trim().eq_ignore_ascii_case("content-length")
                        {
                            content_len = v.trim().parse().unwrap_or(0);
                        }
                        line.clear();
                    }
                    let mut body = vec![0u8; content_len];
                    let _ = reader.read_exact(&mut body);

                    let should_fail = {
                        let mut guard = fail_bg.lock().unwrap();
                        if *guard > 0 {
                            *guard -= 1;
                            true
                        } else {
                            false
                        }
                    };
                    if should_fail {
                        let _ = stream.write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                        );
                    } else {
                        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
                            rec_bg.lock().unwrap().push(val);
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                        );
                    }
                }
            });

            Self {
                url: format!("http://{addr}/v1/internal/guest/events"),
                received,
                fail_next,
                stop,
                handle: Some(handle),
            }
        }

        fn entries(&self) -> Vec<GuestTranscriptEntry> {
            self.received
                .lock()
                .unwrap()
                .iter()
                .filter_map(|v| v.get("entries"))
                .filter_map(|arr| {
                    serde_json::from_value::<Vec<GuestTranscriptEntry>>(arr.clone()).ok()
                })
                .flatten()
                .collect()
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    #[test]
    fn rfc3339_parser_decodes_utc_and_offset_timestamps() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:01.250Z"), Some(1_250));
        assert_eq!(
            parse_rfc3339_ms("2026-11-21T01:45:29.5Z"),
            Some(1_795_225_529_500)
        );
        assert_eq!(parse_rfc3339_ms("1970-01-01T01:00:00+01:00"), Some(0));
    }

    #[test]
    fn tails_agy_claude_codex_incrementally_unmasked_handles_partial_lines_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path();
        let server = MockServer::start();

        // Pre-existing AGY transcript line written BEFORE turn starts -> must NOT be replayed.
        let agy_logs =
            state_dir.join(".gemini/antigravity-cli/brain/conv-1/.system_generated/logs");
        std::fs::create_dir_all(&agy_logs).unwrap();
        let agy_path = agy_logs.join("transcript_full.jsonl");
        std::fs::write(
            &agy_path,
            "{\"type\":\"PLANNER_RESPONSE\",\"content\":\"old turn should not replay\"}\n",
        )
        .unwrap();

        let mut relay = TranscriptRelay::new(
            state_dir,
            "agy",
            "turn_live_1",
            "agt_01test",
            &server.url,
            "cb-tok-1",
        )
        .with_intervals(Duration::ZERO, Duration::ZERO, Duration::ZERO);

        let raw_secret = "sk-live-secret-unmasked-99887766";

        // 1. Append an AGY line with thinking + tool_calls + content + a PARTIAL line (no '\n').
        let agy_line = serde_json::to_string(&json!({
            "type": "PLANNER_RESPONSE",
            "created_at": "2026-11-21T01:45:29.100Z",
            "thinking": format!("Need to inspect credential {raw_secret}"),
            "tool_calls": [
                {"id": "tc_1", "name": "run_command", "arguments": format!("echo {raw_secret}")}
            ],
            "content": "Checking now."
        }))
        .unwrap();
        let mut agy_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&agy_path)
            .unwrap();
        write!(
            agy_file,
            "{agy_line}\n{{\"type\":\"PLANNER_RESPONSE\",\"content\":\"half"
        )
        .unwrap();
        agy_file.flush().unwrap();

        // 2. Create brand-new Claude JSONL during this turn (starts at offset 0).
        let claude_dir = state_dir.join(".claude/projects/proj1");
        std::fs::create_dir_all(&claude_dir).unwrap();
        let claude_path = claude_dir.join("sess-c1.jsonl");
        let claude_lines = format!(
            "{}\n{}\n",
            json!({
                "type": "assistant",
                "sessionId": "sess-c1",
                "timestamp": "2026-11-21T01:45:30Z",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": format!("Claude thinking with {raw_secret}")},
                        {"type": "tool_use", "id": "tu_1", "name": "bash", "input": {"cmd": "ls"}}
                    ]
                }
            }),
            json!({
                "type": "user",
                "sessionId": "sess-c1",
                "timestamp": "2026-11-21T01:45:31Z",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "tu_1", "content": format!("ok {raw_secret}")}
                    ]
                }
            })
        );
        std::fs::write(&claude_path, claude_lines).unwrap();

        // 3. Create brand-new Codex rollout JSONL during this turn.
        let codex_dir = state_dir.join("sessions/2026/11/21");
        std::fs::create_dir_all(&codex_dir).unwrap();
        let codex_path = codex_dir.join("rollout-1.jsonl");
        let codex_lines = format!(
            "{}\n{}\n{}\n",
            json!({
                "timestamp": "2026-11-21T01:45:32Z",
                "type": "response_item",
                "payload": {"type": "reasoning", "summary": [{"text": "Codex step plan"}]}
            }),
            json!({
                "timestamp": "2026-11-21T01:45:33Z",
                "type": "response_item",
                "payload": {"type": "function_call", "call_id": "fc_1", "name": "shell", "arguments": "{\"cmd\":\"pwd\"}"}
            }),
            json!({
                "timestamp": "2026-11-21T01:45:34Z",
                "type": "response_item",
                "payload": {"type": "function_call_output", "call_id": "fc_1", "output": "/workspace"}
            }),
        );
        std::fs::write(&codex_path, codex_lines).unwrap();

        // Inject 1 transient 503 failure: offset must NOT advance, next tick retries cleanly.
        *server.fail_next.lock().unwrap() = 1;
        assert!(!relay.tick(Some("sess-1")));
        assert!(server.entries().is_empty());

        // Now tick succeeds and flushes all complete lines (AGY 3 + Claude 3 + Codex 3 = 9),
        // skipping the old pre-turn AGY line AND holding back the incomplete trailing AGY line.
        assert!(relay.tick(Some("sess-1")));
        let first_pass = server.entries();
        assert_eq!(first_pass.len(), 9, "entries: {first_pass:#?}");
        assert!(
            !first_pass
                .iter()
                .any(|e| e.text.contains("old turn should not replay"))
        );
        // Verify unmasked secret preserved verbatim in both text and raw_json!
        assert!(first_pass.iter().any(|e| e.role == "thinking"
            && e.text.contains(raw_secret)
            && e.raw_json.contains(raw_secret)));
        // Verify tool_result resolved tool_name via call_id correlation for Claude & Codex!
        assert!(first_pass.iter().any(|e| e.role == "tool_result"
            && e.tool_name.as_deref() == Some("bash")
            && e.text.contains(raw_secret)));
        assert!(first_pass.iter().any(|e| e.role == "tool_result"
            && e.tool_name.as_deref() == Some("shell")
            && e.text.contains("/workspace")));

        // Re-ticking with no new data must NOT re-emit already-flushed offsets.
        assert!(!relay.tick(Some("sess-1")));
        assert_eq!(server.entries().len(), 9);

        // Complete the trailing partial AGY line -> final flush emits exactly 1 more event!
        writeln!(agy_file, " written now\"}}").unwrap();
        agy_file.flush().unwrap();
        assert_eq!(relay.flush_all(Some("sess-1")), 1);
        let all = server.entries();
        assert_eq!(all.len(), 10);
        assert_eq!(all.last().unwrap().text, "half written now");
    }

    #[test]
    fn tails_spiron_sqlite_session_entries_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path();
        let db = state_dir.join("sessions.sqlite3");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE session_entries(
                 session_id TEXT NOT NULL,
                 idx INTEGER NOT NULL,
                 event TEXT NOT NULL,
                 PRIMARY KEY(session_id, idx)
             );",
        )
        .unwrap();
        // Pre-existing row before turn start -> must NOT replay.
        conn.execute(
            "INSERT INTO session_entries VALUES ('s1', 1, ?1)",
            [r#"{"type":"text","data":{"role":"assistant","content":"old spiron row"}}"#],
        )
        .unwrap();

        let server = MockServer::start();
        let mut relay = TranscriptRelay::new(
            state_dir,
            "spiron",
            "turn_sp_1",
            "agt_01sp",
            &server.url,
            "cb-tok-sp",
        )
        .with_intervals(Duration::ZERO, Duration::ZERO, Duration::ZERO);

        // Write new rows mid-turn.
        conn.execute(
            "INSERT INTO session_entries VALUES ('s1', 2, ?1)",
            [r#"{"type":"tool_call","data":{"callId":"c1","toolName":"grep","args":{"q":"secret_val"}}}"#],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_entries VALUES ('s1', 3, ?1)",
            [r#"{"type":"tool_result","data":{"callId":"c1","output":"matched secret_val"}}"#],
        )
        .unwrap();

        assert_eq!(relay.flush_all(Some("s1")), 2);
        let entries = server.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].source, "sessions.sqlite3");
        assert_eq!(entries[0].role, "tool_call");
        assert_eq!(entries[0].tool_name.as_deref(), Some("grep"));
        assert_eq!(entries[1].role, "tool_result");
        assert_eq!(entries[1].tool_name.as_deref(), Some("grep"));
        assert!(entries[1].text.contains("secret_val"));

        // Second flush without new rows emits 0.
        assert_eq!(relay.flush_all(Some("s1")), 0);
    }
}
