//! Exceptional, audited erasure. Ordering metadata is immutable; the receipt
//! intentionally contains neither the removed text nor any digest of it.
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::{
    AgentId, Error, MessageId, Store, agent_config_dir, clean_rag_text, now_ms, rag_content_hash,
    tokenize_hybrid_search_tokens,
};

pub const PERMISSION: &str = "conversation.redact";
const TOMBSTONE: &str = "[redacted]";

/// Identity comes from authentication, never from request JSON.
pub enum Actor {
    Operator,
    Agent(AgentId),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpanUnit {
    Bytes,
    Chars,
}

/// End-exclusive coordinates in the current, decoded message text. Replaying
/// precisely the same selector/span returns the original receipt, not a second edit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Span {
    pub unit: SpanUnit,
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Inbound/outbound/tool id, or `transcript:<zero-based-line>` for Sylphx.
    pub message_id: Option<String>,
    /// A mailbox sequence, mutually exclusive with message_id.
    pub seq: Option<i64>,
    pub span: Option<Span>,
    /// Explain the reason without repeating the sensitive text.
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CopyFailure {
    pub path: String,
    pub error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub audit_id: String,
    pub permission: String,
    pub actor: String,
    pub agent_id: String,
    pub target_id: String,
    pub timestamp_ms: i64,
    pub reason: String,
    pub span: Span,
    pub removed_bytes: usize,
    pub removed_chars: usize,
    pub scrubbed_paths: Vec<String>,
    pub unscrubbed: Vec<CopyFailure>,
    /// Only local managed copies; providers, backups and live model context
    /// are explicitly outside this operation's scope.
    pub full_scrub: bool,
}

fn invalid(text: &str) -> Error {
    Error::Message(text.into())
}

fn authorize(
    conn: &rusqlite::Connection,
    actor: &Actor,
    target: &AgentId,
) -> Result<String, Error> {
    match actor {
        Actor::Operator => Ok("operator".into()),
        Actor::Agent(id) => {
            let (admin, parent, name): (bool, Option<String>, String) = conn.query_row(
                "SELECT is_admin, parent_id, name FROM agents WHERE id = ?1",
                [id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if admin && parent.is_none() {
                return Ok(format!("agent:{name}"));
            }
            let mut visited = HashSet::new();
            let mut current: Option<String> = conn.query_row(
                "SELECT parent_id FROM agents WHERE id = ?1",
                [target.as_str()],
                |r| r.get(0),
            )?;
            while let Some(pid) = current {
                if !visited.insert(pid.clone()) {
                    break;
                }
                if pid == id.as_str() {
                    return Ok(format!("agent:{name}"));
                }
                current =
                    conn.query_row("SELECT parent_id FROM agents WHERE id = ?1", [pid], |r| {
                        r.get(0)
                    })?;
            }
            Err(Error::PermissionDenied(
                "conversation.redact requires a target ancestor or admin; self and peers are refused".into(),
            ))
        }
    }
}

fn bounds(text: &str, span: Option<&Span>) -> Result<(Span, usize, usize), Error> {
    let resolved = span.cloned().unwrap_or(Span {
        unit: SpanUnit::Bytes,
        start: 0,
        end: text.len(),
    });
    if resolved.start >= resolved.end {
        return Err(invalid("redaction span must be nonempty and end-exclusive"));
    }
    let (start, end) = match resolved.unit {
        SpanUnit::Bytes => (resolved.start, resolved.end),
        SpanUnit::Chars => {
            let positions: Vec<usize> = text
                .char_indices()
                .map(|(i, _)| i)
                .chain([text.len()])
                .collect();
            let start = *positions
                .get(resolved.start)
                .ok_or_else(|| invalid("redaction span out of range"))?;
            let end = *positions
                .get(resolved.end)
                .ok_or_else(|| invalid("redaction span out of range"))?;
            (start, end)
        }
    };
    if end > text.len() || !text.is_char_boundary(start) || !text.is_char_boundary(end) {
        return Err(invalid(
            "redaction span out of range or splits a UTF-8 character",
        ));
    }
    Ok((resolved, start, end))
}

fn replace_span(text: &str, start: usize, end: usize) -> String {
    format!("{}{}{}", &text[..start], TOMBSTONE, &text[end..])
}

// JSON string token offsets let us change only affected string tokens, not
// reserialize unrelated records/whitespace/keys. JSON escaping is decoded before
// matching, so quotes, Unicode escapes and nested serialized tool output work.
fn rewrite_json_strings(raw: &str, old: &str, replacement: &str) -> Result<(String, usize), Error> {
    rewrite_json_strings_inner(raw, old, replacement, false)
}

fn rewrite_json_strings_inner(
    raw: &str,
    old: &str,
    replacement: &str,
    nested_payload: bool,
) -> Result<(String, usize), Error> {
    let _: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| invalid("invalid session JSON"))?;
    let bytes = raw.as_bytes();
    let mut out = String::new();
    let mut cursor = 0;
    let mut i = 0;
    let mut count = 0;
    let mut last_key = String::new();
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let start = i;
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'\\' {
                i += 2;
            } else if bytes[i] == b'"' {
                i += 1;
                break;
            } else {
                i += 1;
            }
        }
        let decoded: String = serde_json::from_str(&raw[start..i])
            .map_err(|_| invalid("invalid session JSON string"))?;
        let is_key = raw[i..].trim_start().starts_with(':');
        if is_key {
            last_key = decoded;
            continue;
        }
        if !nested_payload
            && matches!(
                last_key.as_str(),
                "id" | "uuid"
                    | "parentUuid"
                    | "sessionId"
                    | "session_id"
                    | "call_id"
                    | "type"
                    | "role"
                    | "name"
                    | "timestamp"
                    | "timestamp_ms"
                    | "seq"
                    | "from"
                    | "to"
                    | "sender"
                    | "destination"
            )
        {
            continue;
        }
        let changed = if decoded.contains(old) {
            count += decoded.matches(old).count();
            decoded.replace(old, replacement)
        } else if matches!(decoded.trim().chars().next(), Some('{' | '[')) {
            // Function output can itself be serialized JSON inside a JSON string.
            if let Ok((nested, n)) = rewrite_json_strings_inner(&decoded, old, replacement, true) {
                count += n;
                nested
            } else {
                decoded.clone()
            }
        } else {
            decoded.clone()
        };
        if changed != decoded {
            out.push_str(&raw[cursor..start]);
            out.push_str(
                &serde_json::to_string(&changed)
                    .map_err(|_| invalid("session serialization failed"))?,
            );
            cursor = i;
        }
    }
    out.push_str(&raw[cursor..]);
    Ok((out, count))
}

// No symlink traversal: opening each directory relative to an already opened
// descriptor binds validation and writes to the same object. Hard-linked files
// are refused so another agent's inode can never be changed through this tree.
fn open_child(
    parent: &File,
    name: &std::ffi::OsStr,
    directory: bool,
    write: bool,
) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes()).map_err(std::io::Error::other)?;
    let access = if write { libc::O_RDWR } else { libc::O_RDONLY };
    let flags = access
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_managed_config(data: &Path, target: &AgentId) -> std::io::Result<File> {
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(data)?;
    let agents = open_child(&root, std::ffi::OsStr::new("agents"), true, false)?;
    let agent = open_child(&agents, std::ffi::OsStr::new(target.as_str()), true, false)?;
    open_child(&agent, std::ffi::OsStr::new("config"), true, false)
}

struct SessionFile {
    path: PathBuf,
    file: File,
    original: String,
    replacement: String,
}

fn failure(path: &Path, error: &str) -> CopyFailure {
    CopyFailure {
        path: path.display().to_string(),
        error: error.into(),
    }
}

fn collect_sessions(
    dir: File,
    path: &Path,
    files: &mut Vec<(PathBuf, File)>,
    failures: &mut Vec<CopyFailure>,
) {
    // read_dir is enumeration only; every effect uses openat on the bound dir.
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => {
            failures.push(failure(path, "cannot enumerate managed session directory"));
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => {
                failures.push(failure(path, "cannot enumerate directory entry"));
                continue;
            }
        };
        let child_path = path.join(entry.file_name());
        let kind = match entry.file_type() {
            Ok(k) => k,
            Err(_) => {
                failures.push(failure(&child_path, "cannot inspect path"));
                continue;
            }
        };
        if kind.is_dir() {
            match open_child(&dir, &entry.file_name(), true, false) {
                Ok(child) => collect_sessions(child, &child_path, files, failures),
                Err(_) => failures.push(failure(
                    &child_path,
                    "cannot open directory without following links",
                )),
            }
        } else if child_path.extension().is_some_and(|ext| ext == "jsonl") {
            match open_child(&dir, &entry.file_name(), false, true) {
                Ok(file) if file.metadata().is_ok_and(|m| m.is_file() && m.nlink() == 1) => {
                    files.push((child_path, file))
                }
                _ => failures.push(failure(
                    &child_path,
                    "session is inaccessible, linked, or not a regular file",
                )),
            }
        } else if kind.is_symlink() {
            // Credential/skill symlinks are not session stores. Session directory
            // symlinks are reported because their descendants cannot be inspected.
            if matches!(
                entry.file_name().to_str(),
                Some("projects" | "sessions" | "session" | "data-home" | "state-home")
            ) {
                failures.push(failure(
                    &child_path,
                    "linked session directory is not traversed",
                ));
            }
        } else if matches!(
            child_path.extension().and_then(|s| s.to_str()),
            Some("db" | "sqlite" | "sqlite3" | "db-wal" | "sqlite-wal" | "sqlite3-wal")
        ) || entry
            .file_name()
            .to_str()
            .is_some_and(|s| s.contains(".jsonl."))
        {
            failures.push(failure(
                &child_path,
                "binary harness store is not supported; no full scrub claimed",
            ));
        }
    }
}

fn transcript_text(raw: &str) -> Result<String, Error> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| invalid("invalid transcript JSON"))?;
    value
        .get("output")
        .or_else(|| value.get("content"))
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| invalid("transcript selector needs a decoded output or content string"))
}

impl Store {
    /// Erase one logical body/span and its exact copies from local managed JSONL.
    /// The target must be quiescent; this does not stop/restart a harness or erase
    /// provider history, backups, remote sessions or an already-running model.
    pub fn redact_conversation(
        &self,
        actor: Actor,
        target: &AgentId,
        request: &Request,
    ) -> Result<Receipt, Error> {
        if request.reason.trim().is_empty() {
            return Err(invalid(
                "redaction reason is required (do not include sensitive text)",
            ));
        }
        if request.message_id.is_some() == request.seq.is_some() {
            return Err(invalid("supply exactly one message_id or mailbox seq"));
        }
        let _lifecycle = self.lock_agent_lifecycle(target)?;
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actor = authorize(&tx, &actor, target)?;
        let id = if let Some(id) = &request.message_id {
            id.clone()
        } else {
            tx.query_row(
                "SELECT id FROM messages WHERE agent_id = ?1 AND seq = ?2",
                params![target.as_str(), request.seq],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| invalid("redaction target not found"))?
        };
        let key = serde_json::to_string(&request.span).map_err(|_| invalid("invalid span"))?;
        let replay = tx.query_row(
            "SELECT receipt FROM conversation_redactions WHERE agent_id = ?1 AND target_id = ?2 AND request_key = ?3",
            params![target.as_str(), id, key], |r| r.get::<_, String>(0)).optional()?
            .map(|raw| serde_json::from_str::<Receipt>(&raw).map_err(|_| invalid("unreadable redaction receipt")))
            .transpose()?;
        let state: String = tx.query_row(
            "SELECT state FROM agents WHERE id = ?1",
            [target.as_str()],
            |r| r.get(0),
        )?;
        if state == "running" {
            return Err(invalid(
                "conversation.redact requires a quiescent target; pause/archive it first",
            ));
        }
        let _process = self.try_lock_agent(target)?;
        let config = agent_config_dir(self.data_dir(), target);
        let mut sessions = Vec::new();
        let mut unscrubbed = Vec::new();
        if config.exists() {
            match open_managed_config(self.data_dir(), target) {
                Ok(dir) => collect_sessions(dir, &config, &mut sessions, &mut unscrubbed),
                Err(_) => {
                    unscrubbed.push(failure(&config, "cannot inspect managed config directory"))
                }
            }
        }
        let cloud_metadata = config.join("sylphx-cloud-session.json");
        if cloud_metadata.exists() {
            unscrubbed.push(failure(
                &cloud_metadata,
                "remote harness session exists; remote erasure is unsupported",
            ));
        }
        let transcript_index = id
            .strip_prefix("transcript:")
            .map(|i| {
                i.parse::<usize>()
                    .map_err(|_| invalid("invalid transcript index"))
            })
            .transpose()?;
        let original = if let Some(index) = transcript_index {
            let path = config.join("sylphx-transcript.jsonl");
            let (_, file) = sessions
                .iter_mut()
                .find(|(p, _)| p == &path)
                .ok_or_else(|| invalid("managed Sylphx transcript is inaccessible"))?;
            let mut text = String::new();
            file.read_to_string(&mut text)?;
            file.rewind()?;
            transcript_text(
                text.lines()
                    .nth(index)
                    .ok_or_else(|| invalid("transcript index out of range"))?,
            )?
        } else {
            tx.query_row("SELECT body FROM messages WHERE agent_id = ?1 AND id = ?2 UNION ALL SELECT body FROM outbound_messages WHERE agent_id = ?1 AND id = ?2 UNION ALL SELECT text FROM rag_chunks WHERE agent_id = ?1 AND chunk_id = ?2 LIMIT 1",
                params![target.as_str(), id], |r| r.get::<_, String>(0)).optional()?.ok_or_else(|| invalid("redaction target not found"))?
        };
        if let Some(receipt) = replay {
            // A historical receipt is not evidence about today's body. Verify
            // the replacement at the original selector before returning it;
            // refuse drift rather than falsely repeat full_scrub=true. No old
            // content or digest is retained to perform this verification.
            let still_redacted = if request.span.is_none() {
                original == TOMBSTONE
            } else {
                let start = match receipt.span.unit {
                    SpanUnit::Bytes => Some(receipt.span.start),
                    SpanUnit::Chars => original.char_indices().nth(receipt.span.start).map(|(i, _)| i),
                };
                start.and_then(|i| i.checked_add(TOMBSTONE.len()).and_then(|end| original.get(i..end))) == Some(TOMBSTONE)
            };
            if !still_redacted {
                return Err(invalid("redaction target changed; existing receipt does not verify the current body"));
            }
            return Ok(receipt);
        }
        let (span, start, end) = bounds(&original, request.span.as_ref())?;
        if original[start..end] == *TOMBSTONE
            && let Some(raw) = tx.query_row(
                "SELECT receipt FROM conversation_redactions WHERE agent_id = ?1 AND target_id = ?2 AND (SELECT count(*) FROM conversation_redactions WHERE agent_id = ?1 AND target_id = ?2) = 1 LIMIT 1",
                params![target.as_str(), id], |r| r.get::<_, String>(0)).optional()? {
            return serde_json::from_str(&raw).map_err(|_| invalid("unreadable redaction receipt"));
        }
        if request.reason.contains(&original[start..end])
            || request.reason.contains(&rag_content_hash(&original))
            || request
                .reason
                .contains(&rag_content_hash(&original[start..end]))
        {
            return Err(invalid(
                "audit reason must not repeat the removed text or its digest",
            ));
        }
        let redacted = replace_span(&original, start, end);
        let mut prepared = Vec::new();
        for (path, mut file) in sessions {
            let mut raw = String::new();
            if file.read_to_string(&mut raw).is_err() {
                unscrubbed.push(failure(&path, "cannot read UTF-8 session"));
                continue;
            }
            let mut replacement = String::new();
            let mut changed = false;
            let mut valid = true;
            let mut remains = false;
            for line in raw.split_inclusive('\n') {
                if line.trim().is_empty() {
                    replacement.push_str(line);
                    continue;
                }
                match rewrite_json_strings(line, &original, &redacted) {
                    Ok((text, n)) => {
                        remains |= rewrite_json_strings_inner(
                            &text,
                            &original[start..end],
                            TOMBSTONE,
                            true,
                        )
                        .is_ok_and(|(_, n)| n > 0);
                        replacement.push_str(&text);
                        changed |= n > 0;
                    }
                    Err(_) => {
                        valid = false;
                        break;
                    }
                }
            }
            if remains {
                unscrubbed.push(failure(&path, "selected text remains in a nonmatching session record; address that record separately"));
            }
            if !valid {
                unscrubbed.push(failure(&path, "invalid session JSON; file not changed"));
            } else if changed {
                prepared.push(SessionFile {
                    path,
                    file,
                    original: raw,
                    replacement,
                });
            }
        }
        let timestamp_ms = now_ms();
        let mut receipt = Receipt {
            audit_id: format!("red_{}", MessageId::generate()),
            permission: PERMISSION.into(),
            actor,
            agent_id: target.to_string(),
            target_id: id.clone(),
            timestamp_ms,
            reason: request.reason.clone(),
            span,
            removed_bytes: end - start,
            removed_chars: original[start..end].chars().count(),
            scrubbed_paths: Vec::new(),
            unscrubbed,
            full_scrub: false,
        };
        // Pending paths are durable before touching any copy: an interruption is
        // never mistaken for success on replay, and no sensitive recovery payload is stored.
        for file in &prepared {
            receipt
                .unscrubbed
                .push(failure(&file.path, "scrub pending/interrupted"));
        }
        tx.pragma_update(None, "secure_delete", "ON")?;
        tx.execute(
            "INSERT INTO rag_chunks_fts(rag_chunks_fts, rank) VALUES('secure-delete', 1)",
            [],
        )?;
        if transcript_index.is_none() {
            tx.execute(
                "UPDATE messages SET body = ?3 WHERE agent_id = ?1 AND id = ?2",
                params![target.as_str(), id, redacted],
            )?;
            tx.execute(
                "UPDATE outbound_messages SET body = ?3 WHERE agent_id = ?1 AND id = ?2",
                params![target.as_str(), id, redacted],
            )?;
            // Telegram's edit baseline is a second body copy for this same
            // provider message, not a separate mailbox entry. Preserve its keys/time.
            let versions: Vec<(i64, i64, String)> = tx.prepare(
                "SELECT v.chat_id, v.message_id, v.body FROM message_versions v JOIN messages m ON m.agent_id = v.agent_id AND json_extract(m.origin, '$.chat_id') = v.chat_id AND json_extract(m.origin, '$.message_id') = v.message_id WHERE m.agent_id = ?1 AND m.id = ?2")?
                .query_map(params![target.as_str(), id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<_, _>>()?;
            for (chat, message, body) in versions {
                if body.contains(&original[start..end]) {
                    tx.execute("UPDATE message_versions SET body = ?4 WHERE agent_id = ?1 AND chat_id = ?2 AND message_id = ?3",
                        params![target.as_str(), chat, message, body.replace(&original[start..end], TOMBSTONE)])?;
                }
            }
            let cleaned = clean_rag_text(&redacted);
            tx.execute("UPDATE rag_chunks SET text = ?3, content_hash = ?4 WHERE agent_id = ?1 AND chunk_id = ?2", params![target.as_str(), id, cleaned, rag_content_hash(&cleaned)])?;
            tx.execute(
                "UPDATE rag_chunks_fts SET text = ?2, search_tokens = ?3 WHERE chunk_id = ?1",
                params![
                    id,
                    cleaned,
                    tokenize_hybrid_search_tokens(&cleaned).join(" ")
                ],
            )?;
            // Remove pre-existing FTS tombstone segments too, not just MATCH visibility.
            tx.execute(
                "INSERT INTO rag_chunks_fts(rag_chunks_fts) VALUES('rebuild')",
                [],
            )?;
        }
        // Bind erasure to the provider identity in the same IMMEDIATE transaction
        // as the body/index rewrite. Ingress checks this before any enrichment or
        // edit-baseline write; neither a new source key nor an aged-out row escapes.
        tx.execute("INSERT INTO conversation_redactions (
                id, agent_id, target_id, request_key, actor, permission, reason,
                created_at_ms, span_unit, span_start, span_end, removed_bytes,
                removed_chars, receipt, origin_chat_id, origin_message_id
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,
                (SELECT json_extract(origin, '$.chat_id') FROM messages WHERE agent_id = ?2 AND id = ?3),
                (SELECT json_extract(origin, '$.message_id') FROM messages WHERE agent_id = ?2 AND id = ?3))",
            params![receipt.audit_id, target.as_str(), id, key, receipt.actor, PERMISSION, receipt.reason, timestamp_ms, if receipt.span.unit == SpanUnit::Bytes { "bytes" } else { "chars" }, receipt.span.start as i64, receipt.span.end as i64, receipt.removed_bytes as i64, receipt.removed_chars as i64, serde_json::to_string(&receipt).map_err(|_| invalid("receipt serialization failed"))?])?;
        tx.commit()?;
        for mut session in prepared {
            receipt.unscrubbed.retain(|f| {
                f.path != session.path.display().to_string()
                    || f.error != "scrub pending/interrupted"
            });
            // A stale descriptor snapshot must not overwrite an append or edit.
            let mut current = String::new();
            let written = (|| -> std::io::Result<()> {
                let same_file = || -> std::io::Result<()> {
                    let opened = session.file.metadata()?;
                    let named = std::fs::symlink_metadata(&session.path)?;
                    if opened.dev() != named.dev()
                        || opened.ino() != named.ino()
                        || named.nlink() != 1
                    {
                        return Err(std::io::Error::other("session path changed"));
                    }
                    Ok(())
                };
                same_file()?;
                session.file.rewind()?;
                session.file.read_to_string(&mut current)?;
                if current != session.original {
                    return Err(std::io::Error::other("session changed"));
                }
                session.file.rewind()?;
                // Overwrite the old extent before shrinking, so no old tail survives.
                let new = session.replacement.as_bytes();
                session.file.write_all(new)?;
                if current.len() > new.len() {
                    session
                        .file
                        .write_all(&vec![b' '; current.len() - new.len()])?;
                }
                session.file.set_len(new.len() as u64)?;
                session.file.sync_all()?;
                session.file.seek(SeekFrom::Start(0))?;
                let mut verified = String::new();
                session.file.read_to_string(&mut verified)?;
                let opened = session.file.metadata()?;
                let named = std::fs::symlink_metadata(&session.path)?;
                if opened.dev() != named.dev() || opened.ino() != named.ino() {
                    return Err(std::io::Error::other("session path changed"));
                }
                if verified != session.replacement {
                    return Err(std::io::Error::other("verification failed"));
                }
                Ok(())
            })();
            if written.is_ok() {
                receipt
                    .scrubbed_paths
                    .push(session.path.display().to_string());
            } else {
                receipt.unscrubbed.push(failure(
                    &session.path,
                    "session write/verification failed or copy changed",
                ));
            }
        }
        let checkpoint: (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
        if checkpoint.0 != 0 {
            receipt.unscrubbed.push(failure(
                &self.data_dir().join("state.db-wal"),
                "SQLite reader prevented WAL truncation",
            ));
        }
        receipt.full_scrub = receipt.unscrubbed.is_empty();
        conn.execute(
            "UPDATE conversation_redactions SET receipt = ?2 WHERE id = ?1",
            params![
                receipt.audit_id,
                serde_json::to_string(&receipt)
                    .map_err(|_| invalid("receipt serialization failed"))?
            ],
        )?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
