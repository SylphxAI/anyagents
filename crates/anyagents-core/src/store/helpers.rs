//! Shared row mappers, validation helpers, hashing, and launch-env encoding.

use std::path::{Path, PathBuf};

use anyagents_harness::{ExecutionRuntime, resolve_stored_budget_micros, resolve_stored_runtime};

use super::agents::agent_config_dir;
use super::types::{Ack, Agent, AgentSummary, Message};
use crate::db::{DbExec, DbExecExt, DbRow};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

/// A capability as it is stored: a digest under this server's key, which lives
/// only in the server's memory. The table is useless to anyone who reads it,
/// and a different key never reproduces the digest.
pub(super) fn hash(key: &str, token: &str) -> String {
    let mut bytes = Vec::with_capacity(key.len() + token.len() + 1);
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(token.as_bytes());
    format!("{:016x}", fnv1a(&bytes))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

pub(super) fn data_dir_for(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// `operator` addresses the operator's inbox in `send`, so no agent may hold
/// that name, in any case.
pub(super) fn refuse_reserved_agent_name(name: &str) -> Result<(), Error> {
    if name.eq_ignore_ascii_case("operator") {
        return Err(Error::Message(
            "`operator` is reserved for the operator's inbox and cannot name an agent".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_token(value: &str, what: &str) -> Result<(), Error> {
    if value.is_empty() || value.contains(':') || value.chars().any(char::is_whitespace) {
        Err(Error::Message(format!(
            "{what} is empty or contains whitespace or a colon"
        )))
    } else {
        Ok(())
    }
}

/// The fault text an older runtime persisted in the agent's config directory.
pub(super) fn legacy_last_error(data_dir: &Path, id: &AgentId) -> Option<String> {
    let raw =
        std::fs::read_to_string(agent_config_dir(data_dir, id).join(".anyagents-last-error.json"))
            .ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    value
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

pub(super) fn agent_by_id(conn: &dyn DbExec, id: &AgentId) -> Result<Agent, Error> {
    let found = conn.db_query_opt(
        concat!("SELECT ", agent_cols!(), " FROM agents WHERE id = ?1"),
        &db_params![id.as_str()],
        agent_from_row_db,
    )?;
    found.ok_or_else(|| Error::AgentNotFound(id.to_string()))
}

/// `Ok` when the agent exists, else [`Error::AgentNotFound`]: the guard most
/// mailbox and turn operations need, without reading or decoding the row.
pub(super) fn agent_exists(conn: &dyn DbExec, id: &AgentId) -> Result<(), Error> {
    let found = conn.db_query_opt(
        "SELECT 1 FROM agents WHERE id = ?1",
        &db_params![id.as_str()],
        |_| Ok(()),
    )?;
    found.ok_or_else(|| Error::AgentNotFound(id.to_string()))
}

/// A launch environment is a list of name and value pairs, stored as JSON so a
/// value with an `=` or a space survives. Values are never keys.
pub(super) fn encode_env(env: &[(String, String)]) -> String {
    // A flat list of names and values. `decode_env` reads exactly this shape,
    // so the two are tested against each other.
    let mut out = String::from("[");
    for (index, (name, value)) in env.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&quote(name));
        out.push(',');
        out.push_str(&quote(value));
    }
    out.push(']');
    out
}

fn quote(text: &str) -> String {
    let mut out = String::from("\"");
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            control if control < ' ' => out.push_str(&format!("\\u{:04x}", u32::from(control))),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Read what [`encode_env`] wrote: a flat JSON array of quoted names and values,
/// e.g. `["K","v","K2","v2"]`. Anything unreadable reads as no environment.
pub(super) fn decode_env(text: &str) -> Vec<(String, String)> {
    let bytes = text.as_bytes();
    let Some(open) = text.find('[') else {
        return Vec::new();
    };
    let mut at = open + 1;
    let mut env = Vec::new();
    loop {
        let Some((name, next)) = read_string(bytes, at) else {
            return env;
        };
        let Some(comma) = text[next..].find(',') else {
            return env;
        };
        let Some((value, end)) = read_string(bytes, next + comma + 1) else {
            return env;
        };
        env.push((name, value));
        // Skip the comma before the next name, if there is one.
        let rest = &text[end..];
        let Some(comma) = rest.find(',') else {
            return env;
        };
        let after = end + comma + 1;
        if !rest[comma + 1..].trim_start().starts_with('"') {
            return env;
        }
        at = after + (rest[comma + 1..].len() - rest[comma + 1..].trim_start().len());
    }
}

/// One JSON string starting at `at`, and where the closing quote is.
fn read_string(bytes: &[u8], at: usize) -> Option<(String, usize)> {
    if bytes.get(at) != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut index = at + 1;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'"' => return Some((out, index + 1)),
            b'\\' => {
                match bytes.get(index + 1)? {
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'u' => {
                        let hex = std::str::from_utf8(bytes.get(index + 2..index + 6)?).ok()?;
                        let code = u32::from_str_radix(hex, 16).ok()?;
                        out.push(char::from_u32(code)?);
                        index += 4;
                    }
                    other => out.push(char::from(*other)),
                }
                index += 2;
            }
            _ => {
                let rest = std::str::from_utf8(&bytes[index..]).ok()?;
                let ch = rest.chars().next()?;
                out.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    None
}

pub(super) fn decode_runtime_column(
    raw: Option<&str>,
    harness: &str,
    launch_env: &[(String, String)],
) -> ExecutionRuntime {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| ExecutionRuntime::parse(s).ok())
        .unwrap_or_else(|| resolve_stored_runtime(harness, launch_env))
}

pub(super) fn decode_budget_column(
    raw: Option<i64>,
    launch_env: &[(String, String)],
) -> Option<u64> {
    raw.and_then(|v| u64::try_from(v).ok())
        .filter(|&cap| cap > 0)
        .or_else(|| resolve_stored_budget_micros(launch_env))
}

pub(super) fn agent_from_row_db(row: &DbRow<'_>) -> Result<Agent, Error> {
    let agent_id = row.parse_str(0)?;
    let harness: String = row.get(2)?;
    let launch_env = row
        .get::<Option<String>>(8)?
        .map(|json| decode_env(&json))
        .unwrap_or_default();
    let parent_id = match row.get::<Option<String>>(9)? {
        Some(raw) if !raw.is_empty() => Some(AgentId::parse(&raw)?),
        _ => None,
    };
    let runtime_raw: Option<String> = row.get(13)?;
    let budget_raw: Option<i64> = row.get(14)?;
    let runtime = decode_runtime_column(runtime_raw.as_deref(), &harness, &launch_env);
    let remote_budget_micros = decode_budget_column(budget_raw, &launch_env);
    Ok(Agent {
        is_admin: row.get(10)?,
        id: agent_id,
        name: row.get(1)?,
        role: row.get(11)?,
        description: row.get(12)?,
        harness,
        runtime,
        remote_budget_micros,
        model: row.get(3)?,
        workdir: row.get(4)?,
        instructions: row.get(5)?,
        state: row.get(6)?,
        created_at_ms: row.get(7)?,
        launch_env,
        parent_id,
    })
}

pub(super) fn agent_summary_from_row_db(row: &DbRow<'_>) -> Result<AgentSummary, Error> {
    let harness: String = row.get(2)?;
    let runtime_raw: Option<String> = row.get(9)?;
    let budget_raw: Option<i64> = row.get(10)?;
    let runtime = decode_runtime_column(runtime_raw.as_deref(), &harness, &[]);
    let remote_budget_micros = decode_budget_column(budget_raw, &[]);
    Ok(AgentSummary {
        id: row.parse_str(0)?,
        name: row.get(1)?,
        harness,
        runtime,
        remote_budget_micros,
        model: row.get(3)?,
        state: row.get(4)?,
        parent_id: match row.get::<Option<String>>(5)? {
            Some(raw) if !raw.is_empty() => Some(AgentId::parse(&raw)?),
            _ => None,
        },
        role: row.get(6)?,
        is_admin: row.get(7)?,
        created_at_ms: row.get(8)?,
    })
}

pub(super) fn message_from_row_with_cursor_db(
    row: &DbRow<'_>,
    delivered: i64,
) -> Result<Message, Error> {
    let agent_id = row.parse_str(1)?;
    let seq: i64 = row.get(2)?;
    Ok(Message {
        id: row.parse_str(0)?,
        agent_id,
        seq,
        source_time_ms: row.get(3)?,
        source_key: row.get(4)?,
        sender: row.get(5)?,
        text: row.get(6)?,
        reply_to: row.get(7)?,
        received_at_ms: row.get(8)?,
        ack: if seq <= delivered {
            Ack::Delivered
        } else {
            Ack::Received
        },
        delivered_at_ms: row.get(9)?,
        origin: row
            .get::<Option<String>>(10)?
            .and_then(|text| serde_json::from_str(&text).ok()),
        context: row
            .get::<Option<String>>(11)?
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default(),
    })
}
