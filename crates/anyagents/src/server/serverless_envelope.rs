//! Serverless turn message-envelope assembly, deployment-skill hydration, and
//! non-revoking remote MCP session token reuse.

use std::path::Path;

use anyagents_core::{AgentId, Message, Store, now_ms};
use anyagents_harness::{
    Context, GUEST_MESSAGE_B64_ENV, OutboundMessage, compose_message, encode_outbound_message_b64,
};

use crate::sandbox_providers::providers::ExecRequest;

const SERVERLESS_REMOTE_SESSION_TTL_MS: i64 = 3_782_000;
const SERVERLESS_TOKEN_REUSE_MARGIN_MS: i64 = 600_000;

/// Format Unix epoch milliseconds as an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`).
pub(crate) fn format_utc_timestamp_ms(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn enrich_message_context(m: &Message) -> Context {
    let mut ctx = m.context.clone();
    if ctx.timestamp.is_none() {
        let ts = m.source_time_ms.unwrap_or(m.received_at_ms);
        ctx.timestamp = format_utc_timestamp_ms(ts);
    }
    if ctx.sender.is_none() {
        ctx.sender = m.sender.clone();
    }
    if ctx.destination.is_none() {
        ctx.destination = m.reply_to.clone();
    }
    ctx
}

/// Build the full 6-dimension [`OutboundMessage`] (and matching prompt text)
/// for a non-empty serverless mailbox batch.
///
/// - Single-message batch: preserves the message's real `id`, `seq`, `sender`,
///   `reply_to`, `source_time_ms`, and enriched `Context` (with UTC `timestamp`).
/// - Multi-message batch: keeps `last`'s real `id`, `seq`, `sender`, `reply_to`,
///   `source_time_ms`, and `Context` (merging the first non-`None` `topology`
///   across earlier messages if `last.context.topology` is `None`), while
///   rendering messages `0..n-1` via [`compose_message`] before `last.text` so
///   every message in the batch retains `<topology>`, `<message ...>`,
///   `<quoted_message>`, and UTC `at` metadata.
pub(crate) fn build_serverless_outbound_message(msgs: &[Message]) -> (String, OutboundMessage) {
    let last = msgs.last().expect("non-empty batch");
    if msgs.len() == 1 {
        let ctx = enrich_message_context(last);
        let outbound = OutboundMessage {
            id: last.id.to_string(),
            seq: last.seq,
            text: last.text.clone(),
            sender: last.sender.clone(),
            reply_to: last.reply_to.clone(),
            source_time_ms: last.source_time_ms.or(Some(last.received_at_ms)),
            context: ctx,
        };
        return (last.text.clone(), outbound);
    }

    let mut last_ctx = enrich_message_context(last);
    if last_ctx.topology.is_none() {
        last_ctx.topology = msgs.iter().find_map(|m| m.context.topology.clone());
    }

    let mut combined = String::new();
    for m in &msgs[..msgs.len() - 1] {
        let ctx = enrich_message_context(m);
        if !combined.is_empty() {
            combined.push_str("\n\n");
        }
        combined.push_str(&compose_message(m.id.as_str(), &ctx, &m.text));
    }
    if !combined.is_empty() {
        combined.push_str("\n\n");
    }
    combined.push_str(&last.text);

    let outbound = OutboundMessage {
        id: last.id.to_string(),
        seq: last.seq,
        text: combined.clone(),
        sender: last.sender.clone(),
        reply_to: last.reply_to.clone(),
        source_time_ms: last.source_time_ms.or(Some(last.received_at_ms)),
        context: last_ctx,
    };
    (combined, outbound)
}

fn xml_attr_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Return sorted deployment skill names materialized under `<data_dir>/skills/`.
pub(crate) fn list_deployment_skill_names(data_dir: &Path) -> Vec<String> {
    let skills_root = data_dir.join("skills");
    let Ok(entries) = std::fs::read_dir(&skills_root) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with('.') {
            names.push(name.to_string());
        }
    }
    names.sort();
    names
}

/// Annotate an initial `<topology event="initial" ... />` tag with the
/// deployment's hydrated skill inventory when skills are present.
pub(crate) fn attach_deployment_skills_to_topology(
    outbound: &mut OutboundMessage,
    data_dir: &Path,
) {
    let Some(ref topo) = outbound.context.topology else {
        return;
    };
    if !topo.starts_with("<topology event=\"initial\"") || topo.contains(" skills=\"") {
        return;
    }
    let skills = list_deployment_skill_names(data_dir);
    if skills.is_empty() {
        return;
    }
    if let Some(prefix) = topo.strip_suffix(" />") {
        outbound.context.topology = Some(format!(
            "{prefix} skills=\"{}\" />",
            xml_attr_escape(&skills.join(", "))
        ));
    }
}

/// Ensure PostgreSQL `deployment_blobs(namespace = 'skills')` are materialized
/// into `<data_dir>/skills/` so serverless sandbox tar sync and `<topology>`
/// always see deployment skills across cold starts.
pub(crate) fn ensure_deployment_skills_hydrated(store: &Store) {
    if !store.is_postgres() {
        return;
    }
    let skills_root = store.data_dir().join("skills");
    let already_populated = std::fs::read_dir(&skills_root)
        .ok()
        .is_some_and(|mut it| it.next().is_some());
    if already_populated {
        return;
    }
    let Ok(entries) = store.blob_store().get_deployment_blobs_all("skills") else {
        return;
    };
    for entry in entries {
        if anyagents_core::stateless::validate_blob_path(&entry.path).is_err() {
            continue;
        }
        let dest = skills_root.join(&entry.path);
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if entry.content.is_empty() {
            let _ = std::fs::write(&dest, []);
        } else {
            let _ = anyagents_harness::atomic_write_nonempty_file(&dest, &entry.content, 0o644);
        }
    }
}

/// Deduplicate repeated markdown sections (`## ` / `### ` headings) and
/// repeated long instruction paragraphs (>= 80 chars after whitespace
/// normalization) so hydrated `SYSTEM_PROMPT.md` files never carry duplicate
/// role/instruction blocks.
pub(crate) fn deduplicate_markdown_sections(raw: &str) -> String {
    let normalize = |s: &str| -> String { s.split_whitespace().collect::<Vec<_>>().join(" ") };
    let mut sections: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in raw.lines() {
        let is_heading = line.starts_with("## ") || line.starts_with("### ");
        if is_heading && !current.is_empty() {
            sections.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        sections.push(current);
    }

    let mut seen_sections: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_paragraphs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut kept_sections: Vec<String> = Vec::new();
    for sec in sections {
        let sec_key = normalize(&sec);
        if sec_key.is_empty() || !seen_sections.insert(sec_key) {
            continue;
        }
        let mut kept_paras: Vec<&str> = Vec::new();
        for para in sec.split("\n\n") {
            let p_key = normalize(para);
            if p_key.len() >= 80 && !seen_paragraphs.insert(p_key) {
                continue;
            }
            kept_paras.push(para);
        }
        if !kept_paras.is_empty() {
            let joined = kept_paras.join("\n\n");
            let trimmed = joined.trim_matches('\n');
            if !trimmed.is_empty() {
                kept_sections.push(trimmed.to_string());
            }
        }
    }
    let mut out = kept_sections.join("\n\n");
    if raw.ends_with('\n') && !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Deduplicate hydrated system prompt files inside `cfg_dir` before syncing the
/// agent bundle into a serverless sandbox.
pub(crate) fn deduplicate_hydrated_system_prompts(cfg_dir: &Path) {
    for rel in [
        "SYSTEM_PROMPT.md",
        "AGENTS.md",
        "CLAUDE.md",
        ".gemini/config/GEMINI.md",
    ] {
        let path = cfg_dir.join(rel);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let deduped = deduplicate_markdown_sections(&raw);
        if deduped != raw {
            let _ = std::fs::write(&path, deduped);
        }
    }
}

/// Update `outbound.text` to the media-rewritten `effective_prompt` and attach
/// the base64-encoded JSON [`OutboundMessage`] to `exec_req.env`.
pub(crate) fn attach_outbound_message_env(
    exec_req: &mut ExecRequest,
    outbound: Option<OutboundMessage>,
    effective_prompt: &str,
) {
    let Some(mut msg) = outbound else {
        return;
    };
    msg.text = effective_prompt.to_string();
    if let Ok(encoded) = encode_outbound_message_b64(&msg) {
        exec_req
            .env
            .insert(GUEST_MESSAGE_B64_ENV.to_string(), encoded);
    }
}

/// Obtain a live remote MCP session token for `agent_id` in serverless dispatch
/// without revoking sibling active tokens of an already-live session.
///
/// Reuses the existing `<cfg_dir>/remote-session-token` when it authenticates
/// for `agent_id` with more than 10 minutes remaining. Otherwise issues one
/// fresh sibling token via [`Store::ensure_remote_session_token`], writes both
/// `.anyagents-mcp-url` (`0644`) and `remote-session-token` (`0600`) to
/// `cfg_dir`, and returns that exact token for both `mcp_servers` and sandbox
/// bundle tar sync.
pub(crate) fn ensure_serverless_remote_session_token(
    store: &Store,
    agent_id: &AgentId,
    base_url: &str,
) -> Result<String, String> {
    let cfg_dir = anyagents_core::agent_config_dir(store.data_dir(), agent_id);
    let mcp_url = format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        crate::remote::MCP_PATH
    );
    let now = now_ms();

    if let Ok(existing) = crate::remote::session_token(&cfg_dir)
        && let Ok(grant) = store.authenticate_remote_session(&existing)
        && grant.agent_id == *agent_id
        && grant.expires_at_ms > now.saturating_add(SERVERLESS_TOKEN_REUSE_MARGIN_MS)
    {
        let _ = std::fs::create_dir_all(&cfg_dir);
        let _ = anyagents_harness::atomic_write_nonempty_file(
            &cfg_dir.join(crate::remote::MCP_URL_FILE),
            format!("{mcp_url}\n").as_bytes(),
            0o644,
        );
        return Ok(existing);
    }

    let issued = store
        .ensure_remote_session_token(agent_id, SERVERLESS_REMOTE_SESSION_TTL_MS)
        .map_err(|err| format!("failed to ensure remote session token: {err}"))?;
    let written = std::fs::create_dir_all(&cfg_dir)
        .map_err(anyagents_core::Error::from)
        .and_then(|()| {
            anyagents_harness::atomic_write_nonempty_file(
                &cfg_dir.join(crate::remote::MCP_URL_FILE),
                format!("{mcp_url}\n").as_bytes(),
                0o644,
            )
            .map_err(anyagents_core::Error::from)
        })
        .and_then(|()| {
            crate::operator::write_agent_secret(
                store,
                agent_id,
                crate::remote::TOKEN_FILE,
                &issued.token,
            )
        });
    if let Err(err) = written {
        let _ = store.revoke_remote_session_token(&issued.token);
        return Err(format!(
            "cannot persist serverless remote session files for {agent_id}: {err}"
        ));
    }
    Ok(issued.token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{Ack, MessageId, NewAgent};

    fn sample_message(
        id: &str,
        seq: i64,
        sender: &str,
        reply_to: &str,
        text: &str,
        ts_ms: i64,
        ctx: Context,
    ) -> Message {
        Message {
            id: MessageId::parse(id).unwrap_or_else(|_| MessageId::generate()),
            agent_id: AgentId::generate(),
            seq,
            text: text.to_string(),
            sender: Some(sender.to_string()),
            source_time_ms: Some(ts_ms),
            source_key: None,
            reply_to: Some(reply_to.to_string()),
            received_at_ms: ts_ms,
            ack: Ack::Received,
            delivered_at_ms: None,
            origin: None,
            context: ctx,
        }
    }

    #[test]
    fn serverless_envelope_preserves_full_6d_awareness_for_single_and_multi_message_batches() {
        let dir = crate::test_tempdir();
        std::fs::create_dir_all(dir.path().join("skills/deploy-check")).unwrap();
        std::fs::create_dir_all(dir.path().join("skills/incident-triage")).unwrap();

        let ts1 = chrono::DateTime::parse_from_rfc3339("2026-11-20T01:23:06Z")
            .unwrap()
            .timestamp_millis();
        let ts2 = chrono::DateTime::parse_from_rfc3339("2026-11-20T01:24:10Z")
            .unwrap()
            .timestamp_millis();

        let quoted_body = "agreed on rollout\n\n----- quoted message -----\nThe person replied to Alex:\n\"ready to ship?\"\n----- end of quoted message -----";
        let m1 = sample_message(
            "msg_01aaaaaaaaaaaaaaaaaaaaaaaaaa",
            1,
            "telegram:user:100",
            "telegram:chat:-10055:thread:9",
            quoted_body,
            ts1,
            Context {
                sender: Some("telegram:user:100".into()),
                chat_id: Some(-10055),
                chat_title: Some("Release Ops".into()),
                thread_id: Some(9),
                destination: Some("telegram:chat:-10055:thread:9".into()),
                event: Some("edited".into()),
                reply_to_author: Some("Alex".into()),
                topology: Some("<topology event=\"initial\" self=\"agent:ops\" />".into()),
                ..Default::default()
            },
        );

        // Single-message batch preserves all 6 dimensions + UTC `at` + skills on initial topology.
        let (single_prompt, mut single_out) =
            build_serverless_outbound_message(std::slice::from_ref(&m1));
        attach_deployment_skills_to_topology(&mut single_out, dir.path());
        assert_eq!(single_prompt, quoted_body);
        assert_eq!(single_out.id, m1.id.as_str());
        assert_eq!(
            single_out.context.timestamp.as_deref(),
            Some("2026-11-20T01:23:06Z")
        );
        let rendered_single =
            compose_message(&single_out.id, &single_out.context, &single_out.text);
        assert!(rendered_single.contains("from=\"telegram:user:100\""));
        assert!(rendered_single.contains("chat_title=\"Release Ops\""));
        assert!(rendered_single.contains("topic=\"9\""));
        assert!(rendered_single.contains("event=\"edited\""));
        assert!(rendered_single.contains("at=\"2026-11-20T01:23:06Z\""));
        assert!(rendered_single.contains("<quoted_message author=\"Alex\""));
        assert!(rendered_single.contains("skills=\"deploy-check, incident-triage\""));
        assert!(!rendered_single.contains("from=\"unknown\""));
        assert!(!rendered_single.contains("anyagents:reply:turn_"));

        // Multi-message batch merges earlier topology into `last` and composes earlier messages.
        let m2 = sample_message(
            "msg_01bbbbbbbbbbbbbbbbbbbbbbbbbb",
            2,
            "agent:planner",
            "agent:planner",
            "please proceed now",
            ts2,
            Context {
                sender: None,      // verify backfill from Message::sender
                destination: None, // verify backfill from Message::reply_to
                topology: None,    // verify topology merge from m1
                ..Default::default()
            },
        );
        let (multi_prompt, multi_out) =
            build_serverless_outbound_message(&[m1.clone(), m2.clone()]);
        assert_eq!(multi_out.id, m2.id.as_str());
        assert_eq!(multi_out.seq, 2);
        assert_eq!(multi_out.context.sender.as_deref(), Some("agent:planner"));
        assert_eq!(
            multi_out.context.destination.as_deref(),
            Some("agent:planner")
        );
        assert_eq!(
            multi_out.context.timestamp.as_deref(),
            Some("2026-11-20T01:24:10Z")
        );
        assert!(
            multi_out
                .context
                .topology
                .as_deref()
                .is_some_and(|t| t.contains("event=\"initial\""))
        );
        assert!(multi_prompt.contains("<quoted_message author=\"Alex\""));
        assert!(multi_prompt.contains("chat_title=\"Release Ops\""));
        assert!(multi_prompt.contains("at=\"2026-11-20T01:23:06Z\""));
        assert!(multi_prompt.ends_with("please proceed now"));

        let rendered_multi = compose_message(&multi_out.id, &multi_out.context, &multi_out.text);
        assert!(!rendered_multi.contains("from=\"unknown\""));
        assert!(!rendered_multi.contains("anyagents:reply:turn_"));
    }

    #[test]
    fn ensure_serverless_remote_session_token_reuses_live_and_never_revokes_siblings() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("worker", "codex"))
            .unwrap();
        let cfg_dir = anyagents_core::agent_config_dir(dir.path(), &agent.id);

        // First call mints token #1 and writes both remote-session-token and .anyagents-mcp-url.
        let tok1 =
            ensure_serverless_remote_session_token(&store, &agent.id, "https://coord.example.com")
                .unwrap();
        assert_eq!(crate::remote::session_token(&cfg_dir).unwrap(), tok1);
        assert_eq!(
            std::fs::read_to_string(cfg_dir.join(crate::remote::MCP_URL_FILE))
                .unwrap()
                .trim(),
            "https://coord.example.com/v1/webhooks/mcp"
        );

        // Second call reuses the exact same token while >10 minutes remain.
        let tok2 =
            ensure_serverless_remote_session_token(&store, &agent.id, "https://coord.example.com")
                .unwrap();
        assert_eq!(tok1, tok2);

        // Simulate a cold stateless replica whose ephemeral cfg_dir has no token file yet:
        // issuing token #3 must NOT revoke token #1 (parallel MCP calls keep succeeding).
        std::fs::remove_file(cfg_dir.join(crate::remote::TOKEN_FILE)).unwrap();
        let tok3 =
            ensure_serverless_remote_session_token(&store, &agent.id, "https://coord.example.com")
                .unwrap();
        assert_ne!(tok3, tok1);
        assert!(store.authenticate_remote_session(&tok1).is_ok());
        assert!(store.authenticate_remote_session(&tok3).is_ok());
    }

    #[test]
    fn deduplicate_markdown_sections_collapses_duplicate_role_blocks_and_preserves_unique() {
        let role_block = "You are an autonomous software engineer responsible for planning, implementing, testing, and verifying changes cleanly end to end.";
        let raw = format!(
            "# Identity\n\n## 2. Instructions\n\n{role_block}\n\n{role_block}\n\n## 3. Rules\n\nKeep replies concise.\n\n## 3. Rules\n\nKeep replies concise.\n"
        );
        let deduped = deduplicate_markdown_sections(&raw);
        assert_eq!(deduped.matches(role_block).count(), 1);
        assert_eq!(deduped.matches("## 3. Rules").count(), 1);
        assert!(deduped.contains("Keep replies concise."));
        // Idempotent on already-clean markdown.
        assert_eq!(deduplicate_markdown_sections(&deduped), deduped);
    }
}
