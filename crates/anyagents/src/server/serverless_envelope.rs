//! Serverless turn message-envelope assembly, deployment-skill hydration, and
//! non-revoking remote MCP session token reuse.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyagents_core::{Agent, AgentId, Message, Store, now_ms};
use anyagents_harness::{
    Context, GUEST_MESSAGE_B64_ENV, MESSAGE_MARKER_PREFIX, OutboundMessage, compose_message,
    encode_outbound_message_b64,
};

use crate::sandbox_providers::providers::{
    ExecRequest, ImageSpec, LeasePolicy, SandboxHandle, SandboxProvider, build_bundle_tar_gz,
};

const SERVERLESS_REMOTE_SESSION_TTL_MS: i64 = 3_782_000;
const SERVERLESS_TOKEN_REUSE_MARGIN_MS: i64 = 600_000;
/// Maximum byte length of base64-encoded `ANYAGENTS_GUEST_MESSAGE_B64` placed
/// in `execve` environment variables. Payloads above this threshold are staged
/// into `<state_dir>/turns/<turn_id>.message.json` and passed via
/// `--message-file` to stay safely below Linux's 128 KiB `MAX_ARG_STRLEN`.
pub(crate) const MAX_INLINE_GUEST_MESSAGE_B64_BYTES: usize = 64 * 1024;

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

fn render_message_element(id: &str, ctx: &Context, body: &str) -> String {
    let mut c = ctx.clone();
    c.topology = None;
    c.workspace = None;
    let rendered = compose_message(id, &c, body);
    let marker = format!("{MESSAGE_MARKER_PREFIX}{id}\n");
    rendered.strip_prefix(&marker).unwrap_or(&rendered).into()
}

fn split_topology_and_siblings(topo: &str) -> (Option<&str>, Option<&str>) {
    if topo.starts_with("<message ") {
        (None, Some(topo))
    } else if let Some(i) = topo.find("\n\n<message ") {
        (Some(&topo[..i]), Some(&topo[i + 2..]))
    } else if let Some(i) = topo.find("\n<message ") {
        (Some(&topo[..i]), Some(&topo[i + 1..]))
    } else {
        (Some(topo), None)
    }
}

fn extract_body_from_message_tail(tail: &str) -> Option<&str> {
    let start = tail.strip_prefix("<body>\n").or_else(|| {
        tail.strip_prefix("<quoted_message ")?
            .split_once("</quoted_message>\n<body>\n")
            .map(|(_, r)| r)
    })?;
    start.strip_suffix("\n</body>\n</message>")
}

/// Build the full 6-dimension [`OutboundMessage`] (and matching prompt text)
/// for a non-empty serverless mailbox batch.
///
/// - Single-message batch: preserves the message's real `id`, `seq`, `sender`,
///   `reply_to`, `source_time_ms`, and enriched `Context` (with UTC `timestamp`).
/// - Multi-message batch: emits every coalesced message as a **flat top-level
///   sibling `<message ...><body>...</body></message>` element** (preserving
///   each message's own `id`, `from`, `to`, `channel`, `reply_to`, `answer_to`,
///   `at`, and `<quoted_message>`), placing earlier sibling `<message>` elements
///   before `last`'s `<message>` via top-level `context.topology` so `last`'s
///   `<body>` NEVER wraps or nests another sender's `<message>` envelope.
pub(crate) fn build_serverless_outbound_message(msgs: &[Message]) -> (String, OutboundMessage) {
    let last = msgs.last().expect("non-empty batch");
    let mut last_ctx = enrich_message_context(last);
    let mut outbound = OutboundMessage {
        id: last.id.to_string(),
        seq: last.seq,
        text: last.text.clone(),
        sender: last.sender.clone(),
        reply_to: last.reply_to.clone(),
        source_time_ms: last.source_time_ms.or(Some(last.received_at_ms)),
        context: last_ctx.clone(),
    };
    if msgs.len() == 1 {
        return (last.text.clone(), outbound);
    }

    let mut topologies: Vec<String> = Vec::new();
    for m in msgs {
        if let Some(topo) = m
            .context
            .topology
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            && !topologies.iter().any(|seen| seen == topo)
        {
            topologies.push(topo.to_string());
        }
    }
    let earlier_siblings = msgs[..msgs.len() - 1]
        .iter()
        .map(|m| render_message_element(m.id.as_str(), &enrich_message_context(m), &m.text))
        .collect::<Vec<_>>()
        .join("\n\n");
    let last_sibling = render_message_element(last.id.as_str(), &last_ctx, &last.text);
    let flat_prompt = format!("{earlier_siblings}\n\n{last_sibling}");
    last_ctx.topology = Some(if topologies.is_empty() {
        earlier_siblings
    } else {
        format!("{}\n\n{earlier_siblings}", topologies.join("\n"))
    });
    outbound.context = last_ctx;
    (flat_prompt, outbound)
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
    let (first_line, rest) = match topo.split_once('\n') {
        Some((first, r)) => (first, Some(r)),
        None => (topo.as_str(), None),
    };
    if !first_line.starts_with("<topology event=\"initial\"") || first_line.contains(" skills=\"") {
        return;
    }
    let skills = list_deployment_skill_names(data_dir);
    if skills.is_empty() {
        return;
    }
    if let Some(prefix) = first_line.strip_suffix(" />") {
        let updated = format!(
            "{prefix} skills=\"{}\" />",
            xml_attr_escape(&skills.join(", "))
        );
        let new_topo = match rest {
            Some(r) => format!("{updated}\n{r}"),
            None => updated,
        };
        outbound.context.topology = Some(new_topo);
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

/// Update `outbound` with the media-rewritten `effective_prompt` (preserving
/// flat sibling `<message>` separation for multi-message batches) and attach
/// the base64-encoded JSON [`OutboundMessage`] to `exec_req.env` when small.
///
/// When the base64 payload exceeds [`MAX_INLINE_GUEST_MESSAGE_B64_BYTES`], omits
/// `ANYAGENTS_GUEST_MESSAGE_B64` from `exec_req.env` (preventing Linux `E2BIG`)
/// and returns the raw JSON bytes so the caller can stage `--message-file`.
pub(crate) fn attach_outbound_message_env(
    exec_req: &mut ExecRequest,
    outbound: Option<OutboundMessage>,
    effective_prompt: &str,
) -> Option<Vec<u8>> {
    let mut msg = outbound?;
    let is_multi_batch = msg
        .context
        .topology
        .as_deref()
        .is_some_and(|t| split_topology_and_siblings(t).1.is_some());
    if is_multi_batch {
        let last_open = msg.context.open_tag(&msg.id);
        let separator = format!("\n\n{last_open}\n");
        if let Some((rewritten_earlier, rewritten_last_tail)) =
            effective_prompt.rsplit_once(&separator)
        {
            if let Some(existing_topo) = msg.context.topology.as_deref() {
                let updated_topo = match split_topology_and_siblings(existing_topo).0 {
                    Some(leading) if !leading.is_empty() => {
                        format!("{leading}\n\n{rewritten_earlier}")
                    }
                    _ => rewritten_earlier.to_string(),
                };
                msg.context.topology = Some(updated_topo);
            }
            let expected_last = render_message_element(&msg.id, &msg.context, &msg.text);
            let actual_last = format!("{last_open}\n{rewritten_last_tail}");
            if actual_last != expected_last
                && let Some(rewritten_body) = extract_body_from_message_tail(rewritten_last_tail)
            {
                const QUOTE_FENCE: &str = "\n\n----- quoted message -----\n";
                msg.text = match msg.text.split_once(QUOTE_FENCE) {
                    Some((_, quote_rest)) => {
                        format!("{rewritten_body}{QUOTE_FENCE}{quote_rest}")
                    }
                    None => rewritten_body.to_string(),
                };
            }
        }
    } else {
        msg.text = effective_prompt.to_string();
    }
    if let Ok(encoded) = encode_outbound_message_b64(&msg) {
        if encoded.len() <= MAX_INLINE_GUEST_MESSAGE_B64_BYTES {
            exec_req
                .env
                .insert(GUEST_MESSAGE_B64_ENV.to_string(), encoded);
            return None;
        }
        exec_req.env.remove(GUEST_MESSAGE_B64_ENV);
        return serde_json::to_vec(&msg).ok();
    }
    None
}

/// Upload an oversized JSON-serialized [`OutboundMessage`] into
/// `<state_dir>/turns/<turn_id>.message.json` inside the sandbox and return its
/// absolute remote path for `anyagents-guest turn --message-file`.
pub(crate) async fn stage_oversized_outbound_message_file(
    provider: &dyn SandboxProvider,
    handle: &SandboxHandle,
    turn_id: &str,
    json_bytes: &[u8],
) -> Result<String, String> {
    let rel = format!("turns/{turn_id}.message.json");
    let tar_gz = build_bundle_tar_gz(&[(rel.clone(), json_bytes.to_vec())])?;
    provider
        .sync_bundle_tar(handle, &handle.state_dir, &tar_gz)
        .await?;
    Ok(format!("{}/{rel}", handle.state_dir.trim_end_matches('/')))
}

/// Build the `anyagents-guest turn --detached` argument vector for a sandbox turn.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_detached_guest_turn_argv(
    guest_bin: String,
    turn_id: &str,
    agent: &anyagents_core::Agent,
    callback_url: &str,
    callback_token: &str,
    handle: &SandboxHandle,
    guest_program: Option<&str>,
    heartbeat_interval_ms: Option<u64>,
    message_file: Option<&str>,
    guest_extra_args: &[String],
) -> Vec<String> {
    let mut argv = vec![
        guest_bin,
        "turn".to_string(),
        "--detached".to_string(),
        "--turn-id".to_string(),
        turn_id.to_string(),
        "--agent-id".to_string(),
        agent.id.as_str().to_string(),
        "--harness".to_string(),
        agent.harness.clone(),
        "--prompt-stdin".to_string(),
        "--callback-url".to_string(),
        callback_url.to_string(),
        "--callback-token".to_string(),
        callback_token.to_string(),
        "--state-dir".to_string(),
        handle.state_dir.clone(),
        "--workdir".to_string(),
        handle.workdir.clone(),
    ];
    if let Some(mf) = message_file {
        argv.push("--message-file".to_string());
        argv.push(mf.to_string());
    }
    if let Some(prog) = guest_program {
        argv.push("--program".to_string());
        argv.push(prog.to_string());
    }
    if let Some(ref model) = agent.model
        && !model.trim().is_empty()
    {
        argv.push("--model".to_string());
        argv.push(model.clone());
    }
    if let Some(hb) = heartbeat_interval_ms {
        argv.push("--heartbeat-interval-ms".to_string());
        argv.push(hb.to_string());
    }
    if !guest_extra_args.is_empty() {
        argv.push("--".to_string());
        argv.extend(guest_extra_args.iter().cloned());
    }
    argv
}

/// Ensure a remote sandbox lease for `agent` with the appropriate workload tier,
/// hydrate durable config/secrets/memory/media,
/// and sync the agent bundle archive into the sandbox.
///
/// Returns `Err((Some(handle), msg))` if preparation or tar sync fails after
/// `ensure_sandbox_with_provider` succeeded so the caller can release/retry.
pub(crate) async fn ensure_and_prepare_sandbox_for_turn(
    store: Arc<Store>,
    provider: Arc<dyn SandboxProvider>,
    agent: Agent,
    prompt_for_media: String,
    base_url: String,
    extra_args: Vec<String>,
) -> Result<(crate::sandbox_exec::EnsuredSandbox, String), (Option<Box<SandboxHandle>>, String)> {
    let store_for_media = Arc::clone(&store);
    let provider_for_media = Arc::clone(&provider);
    let agent_id = agent.id.clone();
    let ensured = tokio::task::spawn_blocking(move || {
        // Serialize lease provisioning/persistence with archive and delete. A
        // dispatched turn may have waited here while its agent was archived.
        let _lifecycle = store
            .lock_agent_lifecycle(&agent.id)
            .map_err(|e| (None, e.to_string()))?;
        if store
            .agent(&agent.id)
            .map_err(|e| (None, e.to_string()))?
            .state
            == "archived"
        {
            return Err((None, "agent is archived".into()));
        }
        let cfg_dir = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
        let remote_workdir = super::resolve_remote_sandbox_workdir(&store, &agent);
        let lease_env: BTreeMap<String, String> =
            crate::sandbox_exec::filter_sandbox_launch_env(&agent.launch_env)
                .into_iter()
                .collect();
        let (shape, vol_gib) = crate::sandbox_exec::resolve_sandbox_shape_and_volume_for_agent(
            &store,
            &agent.id,
            crate::sandbox_exec::read_synced_repo_spec(&cfg_dir).is_some(),
            None,
            None,
            std::env::vars(),
        )
        .map_err(|e| (None, e.to_string()))?;
        let ensured = crate::sandbox_exec::ensure_sandbox_with_provider(
            &store,
            Arc::clone(&provider),
            &ImageSpec::default(),
            &LeasePolicy::default(),
            shape,
            vol_gib,
            &agent.id,
            Some(&agent.name),
            &cfg_dir,
            Some(&remote_workdir),
            &lease_env,
        )
        .map_err(|e| {
            super::runtime_issues::capture(
                Arc::clone(&store),
                Some(agent.id.clone()),
                anyagents_core::RuntimeComponent::Sandbox,
                "Sandbox provisioning failed",
                "Coordinator could not establish a sandbox lease",
            );
            (None, e)
        })?;

        let prep: Result<(), String> = (|| {
            let _ =
                crate::sandbox_exec::hydrate_agent_files_from_store(&store, &agent.id, &cfg_dir)?;
            ensure_deployment_skills_hydrated(&store);
            let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(&store, &agent.id);
            let _ = super::serverless_memory::refresh_hydrated_prompt_learned_memory(
                &store, &agent, &cfg_dir,
            );
            deduplicate_hydrated_system_prompts(&cfg_dir);
            if anyagents_harness::normalize_harness_name(&agent.harness) == "agy" {
                let _ = crate::vendor::ensure_agy_steering_settings(
                    &cfg_dir,
                    Path::new(&ensured.handle.workdir),
                    &store.data_dir().join(anyagents_harness::media::MEDIA_DIR),
                );
            }
            if crate::remote::require_public_url(store.data_dir()).is_ok() {
                let _ = ensure_serverless_remote_session_token(&store, &agent.id, &base_url)?;
            }
            if crate::remote::session_token(&cfg_dir).is_ok()
                && crate::remote::resolve_public_mcp_url(&cfg_dir).is_ok()
            {
                crate::sandbox_exec::sync_agent_bundle_tar_to_provider(
                    provider.as_ref(),
                    &ensured.handle,
                    Some(&store),
                    Some(store.data_dir()),
                    &cfg_dir,
                    agent.id.as_str(),
                    &agent.harness,
                    &extra_args,
                )?;
            }
            Ok(())
        })();
        match prep {
            Ok(()) => Ok(ensured),
            Err(e) => Err((Some(Box::new(ensured.handle)), e)),
        }
    })
    .await
    .map_err(|e| (None, format!("sandbox ensure join error: {e}")))??;
    let effective_prompt = super::sandbox_media::hydrate_prompt(
        &store_for_media,
        &agent_id,
        provider_for_media.as_ref(),
        &ensured.handle,
        &prompt_for_media,
    )
    .await;
    Ok((ensured, effective_prompt))
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
        let (multi_prompt, mut multi_out) =
            build_serverless_outbound_message(&[m1.clone(), m2.clone()]);
        attach_deployment_skills_to_topology(&mut multi_out, dir.path());
        assert_eq!(multi_out.id, m2.id.as_str());
        assert_eq!(multi_out.seq, 2);
        assert_eq!(multi_out.text, "please proceed now");
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
                .is_some_and(|t| t.contains("event=\"initial\"")
                    && t.contains("skills=\"deploy-check, incident-triage\""))
        );
        assert!(multi_prompt.contains("<quoted_message author=\"Alex\""));
        assert!(multi_prompt.contains("chat_title=\"Release Ops\""));
        assert!(multi_prompt.contains("at=\"2026-11-20T01:23:06Z\""));
        assert!(multi_prompt.contains("please proceed now"));
        assert!(multi_prompt.ends_with("</message>"));

        let rendered_multi = compose_message(&multi_out.id, &multi_out.context, &multi_out.text);
        assert!(!rendered_multi.contains("from=\"unknown\""));
        assert!(!rendered_multi.contains("anyagents:reply:turn_"));
        assert_eq!(extract_flat_message_blocks(&rendered_multi).len(), 2);
    }

    fn extract_flat_message_blocks(rendered: &str) -> Vec<(&str, &str)> {
        let open_count = rendered.matches("<message ").count();
        assert_eq!(
            open_count,
            rendered.matches("</message>").count(),
            "{rendered}"
        );
        let (mut blocks, mut rest) = (Vec::new(), rendered);
        while let Some(i) = rest.find("<message ") {
            let after = &rest[i..];
            let end = after.find('>').expect("open tag");
            let close = after.find("</message>").expect("close tag");
            let inner = &after[end + 1..close];
            assert!(
                !inner.contains("<message "),
                "nested <message>:\n{rendered}"
            );
            let b0 = inner.find("<body>\n").expect("<body>") + "<body>\n".len();
            let b1 = inner.rfind("\n</body>").expect("</body>");
            let body = &inner[b0..b1];
            assert!(
                !body.contains("<message") && !body.contains("</message>"),
                "{rendered}"
            );
            blocks.push((&after[..=end], body));
            rest = &after[close + "</message>".len()..];
        }
        assert_eq!(blocks.len(), open_count);
        blocks
    }

    #[test]
    fn coalesced_multi_message_batch_emits_flat_siblings_without_nesting_private_bodies() {
        let dir = crate::test_tempdir();
        std::fs::create_dir_all(dir.path().join("skills/deploy-check")).unwrap();
        let ts = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis()
        };
        let mk = |id, seq, from: &str, reply: Option<&str>, text, at, mut ctx: Context| {
            ctx.sender = Some(from.into());
            ctx.recipient = Some("agent:coordinator".into());
            ctx.destination = reply.map(str::to_string);
            let mut m = sample_message(id, seq, from, reply.unwrap_or(""), text, ts(at), ctx);
            m.reply_to = reply.map(str::to_string);
            m
        };
        let render = |msgs: &[Message]| {
            let (prompt, mut out) = build_serverless_outbound_message(msgs);
            attach_deployment_skills_to_topology(&mut out, dir.path());
            let eff = prompt.replace("/v1/media/agt_1/", "/home/user/agent-data/state/media/");
            let mut req = ExecRequest::argv(["anyagents-guest"]);
            attach_outbound_message_env(&mut req, Some(out), &eff);
            let dec =
                anyagents_harness::decode_outbound_message_b64(&req.env[GUEST_MESSAGE_B64_ENV])
                    .unwrap();
            (prompt, compose_message(&dec.id, &dec.context, &dec.text))
        };

        let child = mk(
            "msg_01cccccccccccccccccccccccccc",
            10,
            "agent:worker-child",
            Some("agent:worker-child"),
            "CONFIDENTIAL_INTERNAL_REPORT see /v1/media/agt_1/pic1.png",
            "2026-11-20T08:00:01Z",
            Context {
                topology: Some("<topology event=\"initial\" self=\"agent:coordinator\" />".into()),
                ..Default::default()
            },
        );
        let tick = mk(
            "msg_01dddddddddddddddddddddddddd",
            11,
            "schedule:sch_patrol_01",
            None,
            "Run hourly health patrol",
            "2026-11-20T08:00:05Z",
            Context::default(),
        );
        let tg = mk(
            "msg_01eeeeeeeeeeeeeeeeeeeeeeeeee",
            12,
            "telegram:user:777",
            Some("telegram:chat:-100998877:thread:4"),
            "Hello group, check /v1/media/agt_1/pic3.png",
            "2026-11-20T08:00:10Z",
            Context {
                channel: Some("telegram".into()),
                chat_id: Some(-100998877),
                chat_title: Some("Public Community".into()),
                thread_id: Some(4),
                ..Default::default()
            },
        );

        let (prompt, rendered) = render(&[child, tick, tg.clone()]);
        assert_eq!(extract_flat_message_blocks(&prompt).len(), 3);
        assert!(rendered.contains("self=\"agent:coordinator\" skills=\"deploy-check\" />"));
        let b = extract_flat_message_blocks(&rendered);
        assert_eq!(b.len(), 3);
        assert!(b[0].0.contains("from=\"agent:worker-child\"") && !b[0].0.contains("telegram"));
        assert!(b[0].0.contains("answer_to=\"agent:worker-child\""));
        assert_eq!(
            b[0].1,
            "CONFIDENTIAL_INTERNAL_REPORT see /home/user/agent-data/state/media/pic1.png"
        );
        assert!(
            b[1].0.contains("from=\"schedule:sch_patrol_01\"") && !b[1].0.contains("answer_to=")
        );
        assert_eq!(b[1].1, "Run hourly health patrol");
        assert!(
            b[2].0.contains("from=\"telegram:user:777\"")
                && b[2].0.contains("channel=\"telegram\"")
        );
        assert!(
            b[2].0
                .contains("answer_to=\"telegram:chat:-100998877:thread:4\"")
        );
        assert_eq!(
            b[2].1,
            "Hello group, check /home/user/agent-data/state/media/pic3.png"
        );
        assert!(!b[2].1.contains("CONFIDENTIAL_INTERNAL_REPORT") && !b[2].1.contains("patrol"));

        let (_, single) = render(std::slice::from_ref(&tg));
        let sb = extract_flat_message_blocks(&single);
        assert_eq!(sb.len(), 1);
        assert!(sb[0].0.contains("from=\"telegram:user:777\""));
        assert_eq!(
            sb[0].1,
            "Hello group, check /home/user/agent-data/state/media/pic3.png"
        );
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

    #[test]
    fn oversized_outbound_message_omits_env_and_returns_json_for_message_file() {
        let big_body = "A".repeat(MAX_INLINE_GUEST_MESSAGE_B64_BYTES + 16 * 1024);
        let msg = sample_message(
            "msg_big_1",
            1,
            "operator",
            "operator",
            &big_body,
            1_700_000_000_000,
            Context::default(),
        );
        let (prompt, outbound) = build_serverless_outbound_message(&[msg]);
        let mut req = ExecRequest::argv(["anyagents-guest", "turn"]);
        let staged = attach_outbound_message_env(&mut req, Some(outbound), &prompt)
            .expect("oversized outbound message must return JSON bytes for --message-file");
        assert!(
            !req.env.contains_key(GUEST_MESSAGE_B64_ENV),
            "oversized message must NEVER be placed into ANYAGENTS_GUEST_MESSAGE_B64 env var"
        );
        let decoded: OutboundMessage = serde_json::from_slice(&staged).unwrap();
        assert_eq!(decoded.text, big_body);
    }
}
