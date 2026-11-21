//! Memory-index projection bridge and RAG text normalization / tokenization.

use std::path::Path;
use std::time::Duration;

use super::{ReadConn, Store, memory_index, registry, retired_search};
use crate::db::DbExecExt;
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// Project committed mailbox rows into the search index, without waiting:
    /// when another writer holds the index, the next sync (at the latest the
    /// next search) projects them. Never call it with the state writer held.
    pub(crate) fn project_mailbox(&self) {
        debug_assert!(!registry::holds_store_writer());
        // PostgreSQL serves mailbox conversation/search from the committed
        // source rows. Its standalone tool summaries are written separately;
        // do not synchronously re-project canonical mail on the send path.
        if self.is_postgres() {
            return;
        }
        let Some(state) = self.index_state_reader() else {
            return;
        };
        if let Err(error) = self
            .memory_index()
            .sync(&state, false, memory_index::READ_BUDGET)
        {
            eprintln!("anyagents memory index: projection deferred: {error}");
        }
    }

    /// Bring the search index up to date before reading it. At most
    /// [`memory_index::READ_BUDGET`] rows are projected here; a larger
    /// backlog (a new or rebuilt index) is filled in the background.
    pub(crate) fn sync_memory_index(&self) -> Result<(), Error> {
        match self.index_state_reader() {
            Some(state) => {
                self.memory_index()
                    .sync(&state, true, memory_index::READ_BUDGET)?;
            }
            // Only the state writer can read state: never wait on the index
            // while holding it.
            None => {
                let state = self.sqlite_shared().writer();
                self.memory_index()
                    .sync(&*state, false, memory_index::READ_BUDGET)?;
            }
        }
        Ok(())
    }

    /// Re-project stored bodies a duplicate merged a fuller copy into,
    /// without waiting: a busy index gets them at its next sync, and a
    /// process stopped before then has them found by the tail check on the
    /// next open (`MemoryIndex::check_tail`).
    pub(super) fn reproject_merged(&self, merged: &[String]) {
        if merged.is_empty() {
            return;
        }
        let Some(state) = self.index_state_reader() else {
            self.memory_index().defer(merged);
            return;
        };
        if let Err(error) = self.memory_index().try_reproject(&state, merged) {
            eprintln!("anyagents memory index: re-projection deferred: {error}");
        }
    }

    /// A pooled state reader for index maintenance, never the state writer:
    /// index work must not hold the state writer while it waits.
    fn index_state_reader(&self) -> Option<ReadConn<'static>> {
        match &self.backend {
            registry::Backend::Sqlite(shared) => shared.reader(),
            registry::Backend::Postgres(pg) => Some(ReadConn::Postgres(pg.handle.pool.clone())),
        }
    }

    /// Re-project `chunks` and refile `agents`' rows around a state change:
    /// `change` runs between recording the intent in the index and
    /// re-projecting from what state committed, so an interruption anywhere
    /// leaves an entry the next sync repairs. The chunks' old text leaves
    /// the index before `change` runs.
    pub(crate) fn reindex_around<T>(
        &self,
        chunks: &[&str],
        agents: &[&str],
        change: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        debug_assert!(!registry::holds_store_writer());
        self.invalidate_index(chunks, agents)?;
        let result = change();
        let synced = self.sync_memory_index();
        let value = result?;
        synced?;
        Ok(value)
    }

    /// Record pending index entries before a state change. With only the
    /// state writer to read state through, the index is not waited for: a
    /// busy index fails the change before it starts.
    pub(super) fn invalidate_index(&self, chunks: &[&str], agents: &[&str]) -> Result<(), Error> {
        match self.index_state_reader() {
            Some(state) => self.memory_index().invalidate(&state, chunks, agents, true),
            None => {
                let state = self.sqlite_shared().writer();
                self.memory_index()
                    .invalidate(&*state, chunks, agents, false)
            }
        }
    }

    /// Open an index write transaction to commit before a state change
    /// (`memory_index::Staged`). Never call it with the state writer held.
    pub(crate) fn begin_staged_index(&self) -> Result<memory_index::Staged<'_>, Error> {
        debug_assert!(!registry::holds_store_writer());
        self.memory_index().begin_staged()
    }

    /// Forget a standalone redaction's staged receipt once its audit is in
    /// state.
    pub(crate) fn clear_staged_index_redaction(&self, chunk_id: &str) -> Result<(), Error> {
        self.memory_index().clear_staged_redaction(chunk_id)
    }

    /// The search index's file.
    pub fn memory_index_path(&self) -> &Path {
        self.memory_index().path()
    }

    /// Whether state still has pre-split search tables left to dismantle
    /// (`store/retired_search.rs`).
    pub(crate) fn retired_search_pending(&self) -> Result<bool, Error> {
        retired_search::pending(&self.read_conn())
    }

    /// The text of a standalone tool summary (a chunk with no mailbox row).
    pub(crate) fn standalone_index_text(
        &self,
        agent_id: &AgentId,
        chunk_id: &str,
    ) -> Result<Option<String>, Error> {
        let in_mailbox: bool = self.read_conn().db_query_one(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)
                 OR EXISTS(SELECT 1 FROM outbound_messages WHERE id = ?1)",
            &db_params![chunk_id],
            |r| r.get(0),
        )?;
        if in_mailbox {
            return Ok(None);
        }
        self.memory_index()
            .standalone_text(agent_id.as_str(), chunk_id)
    }

    /// Checkpoint the index's log back into its file and truncate it, without
    /// waiting. `false` when a reader still needs the log.
    pub(crate) fn truncate_index_wal(&self) -> Result<bool, Error> {
        let Some(conn) = self.memory_index().sqlite_writer() else {
            return Ok(true);
        };
        conn.busy_timeout(Duration::ZERO)?;
        let result = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        });
        conn.busy_timeout(Duration::from_secs(30))?;
        Ok(matches!(result?, (0, 0, 0)))
    }
}

/// Strip envelope headers so identical room messages share the same `content_hash`.
pub fn clean_rag_text(text: &str) -> String {
    let mut t = text.trim();
    if let Some(after_hdr) = t.strip_prefix("----- message -----")
        && let Some(idx) = after_hdr.find("-------------------")
    {
        t = after_hdr[idx + "-------------------".len()..].trim();
    }
    t.to_owned()
}

/// Deterministic 128-bit FNV-1a content hash (`32` hex chars) shared by Rust and Python indexers.
pub fn rag_content_hash(text: &str) -> String {
    let cleaned = clean_rag_text(text);
    let bytes = cleaned.as_bytes();
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x84222325cbf29ce4;
    for (i, &b) in bytes.iter().enumerate() {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= u64::from(b) ^ ((i as u64) & 0xff);
        h2 = h2.wrapping_mul(0x100000001b3);
    }
    format!("{h1:016x}{h2:016x}")
}

pub(super) fn is_cjk_char(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2EE5F
            | 0x2F800..=0x2FA1F
            | 0x30000..=0x323AF
            | 0x3041..=0x3096 // Hiragana
            | 0x3099..=0x309F // Hiragana marks and iteration signs
            | 0x30A1..=0x30FA // Katakana
            | 0x30FC..=0x30FF // Katakana prolonged sound and iteration signs
            | 0x31F0..=0x31FF
            | 0xFF66..=0xFF9F // Half-width Katakana
            | 0x1100..=0x11FF // Hangul jamo
            | 0x3131..=0x318E
            | 0xA960..=0xA97C
            | 0xAC00..=0xD7A3 // Hangul syllables
            | 0xD7B0..=0xD7FF
    )
}

pub(super) fn cjk_runs(text: &str) -> impl Iterator<Item = &str> {
    text.split(|ch| !is_cjk_char(ch))
        .filter(|run| !run.is_empty())
}

fn cjk_bigrams(run: &str) -> impl Iterator<Item = String> + '_ {
    run.chars()
        .zip(run.chars().skip(1))
        .map(|(a, b)| [a, b].into_iter().collect())
}

/// Keep single characters only for single-character queries; otherwise use
/// ordered, overlapping bigrams from the same tokenizer as the index.
pub(super) fn cjk_phrase_tokens(run: &str) -> Vec<String> {
    let bigrams: Vec<_> = cjk_bigrams(run).collect();
    if bigrams.is_empty() {
        vec![run.to_owned()]
    } else {
        bigrams
    }
}

/// Hybrid ASCII + CJK tokenizer: emits lowercased alphanumeric/identifier tokens
/// (`len >= 2`, plus sub-tokens split on `_` and `-`) together with individual CJK
/// characters and CJK bigrams so English, code identifiers, and Chinese/Cantonese
/// queries achieve high BM25 recall without dense vector embeddings.
pub fn tokenize_hybrid_search_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut ascii_buf = String::new();
    let mut cjk_run = String::new();

    let flush_ascii = |buf: &mut String, out: &mut Vec<String>| {
        let trimmed = buf.trim_matches(['-', '_']);
        if trimmed.len() >= 2 {
            let lower = trimmed.to_ascii_lowercase();
            out.push(lower.clone());
            if lower.contains('_') || lower.contains('-') {
                for part in lower.split(['_', '-']) {
                    if part.len() >= 2 {
                        out.push(part.to_string());
                    }
                }
            }
        }
        buf.clear();
    };
    let flush_cjk = |run: &mut String, out: &mut Vec<String>| {
        out.extend(run.chars().map(|ch| ch.to_string()));
        out.extend(cjk_bigrams(run));
        run.clear();
    };

    for ch in text.chars() {
        if is_cjk_char(ch) {
            flush_ascii(&mut ascii_buf, &mut tokens);
            cjk_run.push(ch);
        } else if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            flush_cjk(&mut cjk_run, &mut tokens);
            ascii_buf.push(ch);
        } else {
            flush_ascii(&mut ascii_buf, &mut tokens);
            flush_cjk(&mut cjk_run, &mut tokens);
        }
    }
    flush_ascii(&mut ascii_buf, &mut tokens);
    flush_cjk(&mut cjk_run, &mut tokens);
    tokens
}

pub(super) fn classify_chunk_kind_and_tool(
    kind_override: Option<&str>,
    tool_override: Option<&str>,
    sender: &str,
    destination: &str,
    text: &str,
) -> (String, String) {
    if let Some(k) = kind_override.map(str::trim).filter(|s| !s.is_empty()) {
        let t = tool_override
            .map(str::trim)
            .unwrap_or("")
            .to_ascii_lowercase();
        return (k.to_ascii_lowercase(), t);
    }
    let trimmed = text.trim();
    if let Some(rest) = trimmed
        .strip_prefix("[tool:")
        .or_else(|| trimmed.strip_prefix("[tool_call:"))
        .or_else(|| trimmed.strip_prefix("[tool_output:"))
        .or_else(|| trimmed.strip_prefix("[Tool:"))
    {
        let tool = rest
            .split([']', ':', ' '])
            .next()
            .unwrap_or("tool")
            .trim()
            .to_ascii_lowercase();
        return ("tool".to_string(), tool);
    }
    if trimmed.starts_with("[Sub-Agent Completed]") {
        return ("tool".to_string(), "subagents".to_string());
    }
    if trimmed.starts_with("[Schedule") || trimmed.starts_with("[Watch") {
        return ("tool".to_string(), "schedule".to_string());
    }
    if let Some(tool) = sender
        .strip_prefix("tool:")
        .or_else(|| destination.strip_prefix("tool:"))
    {
        return ("tool".to_string(), tool.trim().to_ascii_lowercase());
    }
    ("message".to_string(), String::new())
}
