//! MCP registration, session continuity, seat resolution, runtime config reconciliation/verification, wake IPC, and runtime switching.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Error, Store, agent_config_dir, agent_work_dir};
use anyagents_harness::normalize_harness_name;

use crate::{mcp_config, muse_config, opencode_config};

use super::credentials::copy_profile_tree;
use super::spawn::create_private_config_dir;
#[cfg(unix)]
use super::spawn::own_mode;
use super::{
    bound_profile_name, clear_stale_agy_quota_error_steps, ensure_profile_compatible_and_mapped,
    ensure_seat_environment, ensure_seat_op_config, mark_instructions_rotated_if_changed,
    seed_harness_credentials, upstream_provider_of,
    write_agent_instructions_with_continuity_and_store, write_profile_marker, write_required_file,
    write_seat_shared_file,
};

pub(super) fn heal_mcp_config_file(path: &Path) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    let needs_heal = match std::fs::read(path) {
        Ok(bytes) => {
            bytes.is_empty()
                || !serde_json::from_slice::<serde_json::Value>(&bytes)
                    .is_ok_and(|val| val.is_object())
        }
        Err(_) => false,
    };
    if needs_heal {
        write_seat_shared_file(path, b"{\"mcpServers\":{}}\n", 0o660)
    } else {
        false
    }
}

/// Arguments of the `anyagents mcp` server a seat's harness launches.
pub(crate) fn mcp_server_args(data: &Path, config: &Path) -> Vec<String> {
    vec![
        "--data".to_string(),
        data.display().to_string(),
        "mcp".to_string(),
        "--config".to_string(),
        config.display().to_string(),
    ]
}

fn config_writes_suppressed(data: &Path) -> bool {
    std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some()
        || data.join(".anyagents-suppress-config-write").exists()
}

/// Whether an Antigravity `mcp_config.json` registers an enabled `anyagents`
/// server launched with exactly `args` (this seat's own `--config`).
/// Antigravity reloads this file while it runs: the moment it names no such
/// server, `call_mcp_tool` leaves the tool list (`unknown tool`) and
/// `list_resources` answers `server anyagents is not allowed in this context`
/// until the registration is written back.
pub(crate) fn agy_mcp_registered(path: &Path, args: &[String]) -> bool {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|root| root.pointer("/mcpServers/anyagents").cloned())
        .is_some_and(|server| {
            server
                .get("command")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|command| !command.trim().is_empty())
                && server.get("disabled").and_then(serde_json::Value::as_bool) != Some(true)
                && server
                    .get("args")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|got| {
                        got.len() == args.len()
                            && got
                                .iter()
                                .zip(args)
                                .all(|(g, a)| g.as_str() == Some(a.as_str()))
                    })
        })
}

/// Restore the `anyagents` registration of an Antigravity seat. Other
/// servers in the file are kept; an empty or unparsable file is replaced.
pub(crate) fn ensure_agy_mcp_registration(data: &Path, config: &Path) -> Result<bool, Error> {
    let path = config.join(".gemini/config/mcp_config.json");
    let args = mcp_server_args(data, config);
    if agy_mcp_registered(&path, &args) || config_writes_suppressed(data) {
        return Ok(false);
    }
    let command = crate::data::current_exe_clean()?;
    mcp_config::write_claude(&path, &command, &args)?;
    Ok(true)
}

/// Profiles keep a parsable MCP file for a sign-in run inside them. A seat's
/// Antigravity file must name its `anyagents` server: a parsable but empty
/// server map there removes every AnyAgents tool from the running cascade.
pub(crate) fn ensure_agy_mcp_config(
    store: &Store,
    data: &Path,
    agent: &Agent,
    profile_roots: &[PathBuf],
    target_config: &Path,
) {
    for profile_root in profile_roots {
        let _ = heal_mcp_config_file(&profile_root.join(".gemini/config/mcp_config.json"));
        let _ = heal_mcp_config_file(&profile_root.join("mcp_config.json"));
    }
    let seat_agy = target_config.join(".gemini/config/mcp_config.json");
    if normalize_harness_name(&agent.harness) == "agy" && target_config.is_dir() {
        match ensure_agy_mcp_registration(data, target_config) {
            // A rewrite replaces the file: give the seat's principal its
            // access back, as every other seat config rewrite does.
            Ok(true) => {
                if let Some((_, uid, gid, _)) = resolved_agent_seat_info(store, agent) {
                    crate::sandbox::share_config_with_seat_uid(target_config, uid, gid);
                }
            }
            Ok(false) => {}
            Err(err) => eprintln!("cannot restore {}: {err}", seat_agy.display()),
        }
    } else {
        let _ = heal_mcp_config_file(&seat_agy);
    }
    let _ = heal_mcp_config_file(&target_config.join("mcp_config.json"));
}

/// Caller holds the lifecycle lock and has re-read management authority.
/// Process admission is checked before touching any host pin or native pointer.
pub(crate) fn reset_agent_session_locked(
    store: &Store,
    agent: &Agent,
) -> Result<serde_json::Value, Error> {
    if agent.state == "running" {
        return Err(Error::AgentBusy);
    }
    let _process = store.try_lock_agent(&agent.id)?;
    let root = store.data_dir().join("agents").join(agent.id.as_str());
    let session = anyagents_core::host_session::reset_session_with_source(
        &root,
        &agent.harness,
        agent_session_filename(agent),
        crate::vendor::memory_source(agent),
    )?;
    Ok(serde_json::json!({
        "agent_id": agent.id, "name": agent.name, "state": agent.state,
        "generation": session.generation, "harness": session.harness,
        "memory_sha256": session.memory.sha256,
    }))
}

pub(crate) fn agent_session_filename(agent: &Agent) -> Option<&'static str> {
    if crate::remote::is_agent_cloud(agent, None, None) {
        harness_session_filename(&crate::remote::agent_cloud_selector(agent))
    } else {
        harness_session_filename(&agent.harness)
    }
}

pub(crate) fn harness_session_filename(harness: &str) -> Option<&'static str> {
    match normalize_harness_name(harness) {
        "codex" => Some("codex-session-id"),
        "opencode" => Some("opencode-session-id"),
        "claude-code" => Some("claude-session-id"),
        "agy" => Some("agy-session-id"),
        "muse" => Some("muse-session-id"),
        "omp" => Some("omp-session-id"),
        "dsh" => Some("dsh-session-id"),
        "grok" => Some("grok-session-id"),
        "spiron-cloud" => Some("spiron-cloud-session-id"),
        "claude-cloud" => Some("claude-cloud-session-id"),
        "openai-cloud" => Some("openai-cloud-session-id"),
        "cursor-cloud" => Some("cursor-cloud-session-id"),
        selector if crate::vendor::is_spiron_harness(selector) => Some("spiron-session-id"),
        _ => None,
    }
}

pub(crate) fn build_harness_continuity_summary(
    store: &Store,
    agent: &Agent,
    previous_harness: Option<&str>,
    target_harness: &str,
) -> Option<String> {
    let turns = store.conversation(&agent.id, None, None, Some(6)).ok()?;
    let last_out_idx = turns.iter().rposition(|t| t.direction == "out")?;
    let completed_turns = &turns[..=last_out_idx];
    if completed_turns.is_empty() {
        return None;
    }
    let header_pair = match previous_harness.filter(|p| !p.is_empty() && *p != target_harness) {
        Some(prev) => format!("`{prev}` -> `{target_harness}`"),
        None => format!("`state.db` -> `{target_harness}`"),
    };
    let mut out = format!(
        "## 4. Cross-Harness Session Continuity ({header_pair})\n\n         - Identity `{}` (`{}`) resumed on `{target_harness}` (`model={}`, `workdir={}`).\n         - Do NOT replay completed `[OUT]` turns below (`delivered_seq` is already advanced); act only on the current `<message>`. Query older turns on demand via `conversation`.\n\n         ### Recent Completed Two-Way Turns (`state.db`)\n\n",
        agent.name,
        agent.id.as_str(),
        agent.model.as_deref().unwrap_or("default"),
        agent.workdir.as_deref().unwrap_or("."),
    );
    for turn in completed_turns {
        let dir_tag = if turn.direction == "out" {
            "OUT (already sent/completed)"
        } else {
            "IN (already delivered)"
        };
        let peer = if turn.direction == "out" {
            turn.to.as_str()
        } else {
            turn.from.as_str()
        };
        let snippet: String = turn
            .text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let truncated: String = if snippet.chars().count() > 360 {
            let head: String = snippet.chars().take(360).collect();
            format!("{head}...")
        } else {
            snippet
        };
        out.push_str(&format!(
            "- `[{dir_tag}]` `{}` (`ts={}`, peer=`{peer}`): {}\n",
            turn.id, turn.timestamp_ms, truncated
        ));
    }
    Some(out)
}

/// Declaratively computes cross-harness session continuity from `state.db` (`SSOT`)
/// whenever an identity with prior history in `state.db` starts a cold session on
/// `agent.harness` or switches back after turns on another harness, while keeping
/// the bridge static across subsequent turns of the same active harness session
/// so LLM prompt-prefix KV caches stay warm.
pub(crate) fn compute_live_session_continuity(
    store: &Store,
    agent: &Agent,
    config: &Path,
) -> Option<String> {
    let _ = std::fs::remove_file(config.join(".anyagents-harness-continuity.md"));
    let bridge_cache_path = config.join(".anyagents-session-bridge.json");
    let recovery_cache_path = config.join(".anyagents-session-recovery.md");
    let turns = store.conversation(&agent.id, None, None, Some(10)).ok()?;
    if turns.is_empty() {
        let _ = std::fs::remove_file(&bridge_cache_path);
        let _ = std::fs::remove_file(&recovery_cache_path);
        return None;
    }
    if let Some(recovery_summary) =
        build_harness_continuity_summary(store, agent, None, &agent.harness)
    {
        let _ = write_seat_shared_file(&recovery_cache_path, recovery_summary.as_bytes(), 0o664);
    } else {
        let _ = std::fs::remove_file(&recovery_cache_path);
    }

    let active_selector = if crate::remote::is_agent_cloud(agent, None, None) {
        crate::remote::agent_cloud_selector(agent)
    } else {
        normalize_harness_name(&agent.harness).to_string()
    };
    let active_sess_file = harness_session_filename(&active_selector).map(|f| config.join(f));
    let active_sess_mtime = active_sess_file.as_ref().and_then(|p| {
        let text = std::fs::read_to_string(p).ok()?;
        if text.trim().is_empty() {
            return None;
        }
        std::fs::metadata(p).ok()?.modified().ok()
    });

    let mut newest_other: Option<(&str, std::time::SystemTime)> = None;
    for candidate in [
        "codex",
        "opencode",
        "claude-code",
        "agy",
        "muse",
        "omp",
        "dsh",
        "grok",
        "spiron",
        "spiron-cloud",
        "claude-cloud",
        "openai-cloud",
        "cursor-cloud",
    ] {
        if candidate == active_selector {
            continue;
        }
        if let Some(fname) = harness_session_filename(candidate) {
            let p = config.join(fname);
            if let Ok(text) = std::fs::read_to_string(&p)
                && !text.trim().is_empty()
                && let Ok(meta) = std::fs::metadata(&p)
                && let Ok(mtime) = meta.modified()
                && newest_other.is_none_or(|(_, prev_t)| mtime > prev_t)
            {
                newest_other = Some((candidate, mtime));
            }
        }
    }

    let other_is_newer = match (active_sess_mtime, newest_other) {
        (Some(act_t), Some((_, oth_t))) => oth_t > act_t,
        (None, _) => true,
        (Some(_), None) => false,
    };

    if other_is_newer {
        if active_sess_mtime.is_some()
            && let Some(ref p) = active_sess_file
        {
            let _ = std::fs::remove_file(p);
        }
        let prev_harness = newest_other.map(|(h, _)| h);
        let summary = build_harness_continuity_summary(store, agent, prev_harness, &agent.harness)?;
        let payload = serde_json::json!({
            "harness": agent.harness,
            "previous_harness": prev_harness,
            "summary": summary,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&payload) {
            let _ = write_seat_shared_file(&bridge_cache_path, &bytes, 0o664);
        }
        return Some(summary);
    }

    if let Ok(text) = std::fs::read_to_string(&bridge_cache_path)
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
        && val.get("harness").and_then(|v| v.as_str()) == Some(agent.harness.as_str())
        && let Some(cached) = val
            .get("summary")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
    {
        return Some(cached.to_string());
    }

    if let Some((prev_h, _)) = newest_other {
        let summary = build_harness_continuity_summary(store, agent, Some(prev_h), &agent.harness)?;
        let payload = serde_json::json!({
            "harness": agent.harness,
            "previous_harness": prev_h,
            "summary": summary,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&payload) {
            let _ = write_seat_shared_file(&bridge_cache_path, &bytes, 0o664);
        }
        return Some(summary);
    }

    None
}

/// Walks `parent_id` in `state.db` up to the root ancestor so multi-hop sub-agents
/// inherit the root ancestor's hint consistently.
pub(crate) fn resolve_root_seat_hint(store: &Store, agent: &Agent) -> Option<String> {
    let mut curr = agent.parent_id.clone();
    let mut hops = 0usize;
    let mut last_name = None;
    while let Some(pid) = curr {
        if hops >= 16 {
            break;
        }
        hops += 1;
        let Ok(parent) = store.agent(&pid) else {
            break;
        };
        last_name = Some(parent.name);
        curr = parent.parent_id;
    }
    last_name.or_else(|| {
        agent
            .launch_env
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
            .map(|(_, v)| v.clone())
    })
}

/// Resolve only the operator-admitted principal keyed by this immutable agent ID.
/// Display names, ancestry, and historical filesystem ownership grant no authority.
pub(crate) fn resolved_agent_seat_info(
    store: &Store,
    agent: &Agent,
) -> Option<(String, u32, u32, Option<String>)> {
    let _ = store;
    let seat_hint: Option<String> = None;
    if let Some((user, spec)) =
        crate::sandbox::seat_launch_spec_with_hint(agent.id.as_str(), seat_hint.as_deref())
    {
        let mut parts = spec.split('\t');
        let (_launcher, Some(uid_s), Some(gid_s)) = (parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if let (Ok(uid), Ok(gid)) = (uid_s.parse::<u32>(), gid_s.parse::<u32>()) {
            return Some((user, uid, gid, Some(spec)));
        }
    }
    let (user, uid, gid, _) =
        crate::sandbox::seat_for_agent_with_hint(agent.id.as_str(), seat_hint.as_deref())?;
    Some((user, uid, gid, None))
}

// Seat preparation owns the config mode and ACL mask. Never replay a mode
// captured before preparation over the permissions it established.
pub(super) fn prepare_runtime_config_dir(
    config: &Path,
    seat: Option<(u32, u32)>,
) -> Result<(), Error> {
    create_private_config_dir(config)?;
    if let Some((uid, gid)) = seat {
        crate::sandbox::prepare_config_with_seat_uid(config, uid, gid)?;
    } else {
        #[cfg(unix)]
        repair_unseated_config_mode(config, own_mode(config)?.unwrap_or(0o0700))?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn repair_unseated_config_mode(config: &Path, base_mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
    let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
    let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
    std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode))
}

/// Canonical, idempotent runtime reconciliation for an agent: resolves the
/// authoritative parent from `state.db`, seeds credentials and model catalogs,
/// ensures the mailbox capability, computes cross-harness continuity from
/// `state.db` when needed, writes all instruction surfaces and the active
/// harness's MCP/model config, and shares permissions with the agent's OS seat.
pub(crate) fn reconcile_agent_runtime_config(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    work_dir: &Path,
) -> Result<Option<String>, Error> {
    reconcile_agent_runtime_config_inner(store, data, agent, config, work_dir, true, None)
}

pub(crate) fn reconcile_agent_runtime_config_for_live(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    work_dir: &Path,
    min_started_ms: Option<i64>,
) -> Result<Option<String>, Error> {
    reconcile_agent_runtime_config_inner(store, data, agent, config, work_dir, true, min_started_ms)
}

pub(crate) fn reconcile_agent_runtime_config_for_launch(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    work_dir: &Path,
) -> Result<Option<String>, Error> {
    reconcile_agent_runtime_config_inner(store, data, agent, config, work_dir, false, None)
}

fn reconcile_agent_runtime_config_inner(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    work_dir: &Path,
    mark_instruction_rotation: bool,
    min_started_ms: Option<i64>,
) -> Result<Option<String>, Error> {
    #[cfg(unix)]
    let pre_existing_mode = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(config)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    };
    prepare_runtime_config_dir(
        config,
        resolved_agent_seat_info(store, agent).map(|(_, uid, gid, _)| (uid, gid)),
    )?;
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name)
        .or_else(|| {
            agent
                .launch_env
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    ensure_seat_environment(config, work_dir)?;
    seed_harness_credentials(store, data, agent, config)?;
    if !config.join("mcp-capability").is_file()
        && let Err(mint_error) = crate::operator::ensure(store, &agent.id)
    {
        let mut repaired = false;
        #[cfg(unix)]
        if let Some(op_tok) = crate::operator::operator_token(data)
            && let Ok(mut stream) =
                std::os::unix::net::UnixStream::connect(data.join("anyagents.sock"))
        {
            use std::io::{Read as _, Write as _};
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(2)));
            let req = format!(
                "POST /v1/operator/agents/{}/capability HTTP/1.1\r\nHost: localhost\r\n{}: {op_tok}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}",
                agent.id,
                crate::operator::HEADER
            );
            stream.write_all(req.as_bytes())?;
            let mut buf = String::new();
            stream.read_to_string(&mut buf)?;
            if !buf.starts_with("HTTP/1.1 200 ") && !buf.starts_with("HTTP/1.1 201 ") {
                return Err(Error::Message("capability repair refused".into()));
            }
            repaired = true;
        }
        if !repaired {
            return Err(mint_error);
        }
    }
    let capability = std::fs::read_to_string(config.join("mcp-capability"))?;
    crate::operator::verify_capability(store, &agent.id, capability.trim())?;
    let sandbox_mode = crate::sandbox_providers::SandboxMode::from_env();
    if !sandbox_mode.is_remote_required()
        && crate::sandbox::seats_configured()
        && resolved_agent_seat_info(store, agent).is_none()
    {
        return Err(Error::Message(format!(
            "no admitted execution principal for {}",
            agent.id
        )));
    }

    let before_prompt = mark_instruction_rotation
        .then(|| std::fs::read(config.join("SYSTEM_PROMPT.md")).ok())
        .flatten();
    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity_and_store(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
        Some(store),
    )?;
    if mark_instruction_rotation {
        mark_instructions_rotated_if_changed(
            store,
            config,
            agent,
            before_prompt.as_deref(),
            min_started_ms,
        )?;
    }

    let spiron_cloud_active = agent.canonical_harness() == "spiron"
        && crate::remote::is_agent_cloud(agent, Some(data), Some(config));
    if spiron_cloud_active {
        ensure_spiron_cloud_config(data, config, agent.model.as_deref())?;
    }

    if !config_writes_suppressed(data)
        && let Ok(command) = crate::data::current_exe_clean()
    {
        let args = mcp_server_args(data, config);
        for (other_harness, stale_file) in [
            ("opencode", "opencode.json"),
            ("codex", "config.toml"),
            ("muse", "settings.json"),
            ("claude-code", ".claude.json"),
            ("claude-code", "mcp.json"),
        ] {
            if agent.harness != other_harness
                && !(stale_file == "config.toml"
                    && crate::vendor::is_spiron_harness(&agent.harness))
            {
                let _ = std::fs::remove_file(config.join(stale_file));
            }
        }
        match normalize_harness_name(&agent.harness) {
            "codex" => {
                let codex_toml = config.join("config.toml");
                mcp_config::write_codex_with_model(
                    &codex_toml,
                    agent.model.as_deref(),
                    &command,
                    &args,
                )?;
                mcp_config::sync_codex_developer_instructions(&codex_toml, continuity.as_deref())?;
            }
            "opencode" => {
                opencode_config::write(config, agent.model.as_deref(), &command, &args)?;
            }
            "agy" => {
                mcp_config::write_claude(
                    &config.join(".gemini/config/mcp_config.json"),
                    &command,
                    &args,
                )?;
                crate::vendor::ensure_agy_steering_settings(
                    config,
                    work_dir,
                    &data.join(anyagents_harness::media::MEDIA_DIR),
                )?;
            }
            "muse" => {
                muse_config::write(config, agent.model.as_deref(), &command, &args)?;
                muse_config::point_vendor_home(config)?;
            }
            "omp" => {
                crate::mcp_config::write_omp_mcp_config(config, &command, &args)?;
            }
            "spiron" | "spiron-local" | "spiron-cloud" => {
                if !spiron_cloud_active || config.join("config.toml").exists() {
                    crate::spiron::write_spiron_config(
                        config,
                        agent.model.as_deref(),
                        &command,
                        &args,
                    )?;
                }
            }
            _ => {
                mcp_config::sanitize_claude_settings(config)?;
                mcp_config::write_claude(&config.join(".claude.json"), &command, &args)?;
                mcp_config::write_claude(&config.join("mcp.json"), &command, &args)?;
            }
        }
    }

    if !sandbox_mode.is_remote_required()
        && let Some((_, uid, gid, spec_opt)) = resolved_agent_seat_info(store, agent)
    {
        if let Some(ref spec) = spec_opt {
            ensure_seat_op_config(config, spec);
        }
        crate::sandbox::share_config_with_seat_uid_checked(config, uid, gid)?;
        // Workdir admission belongs to create, not config-only reconciliation:
        // an archived agent may already have had its managed workdir reaped.
        if agent.harness == "muse"
            && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && !crate::sandbox::is_isolated_test_env()
        {
            let mut seat_environment = Vec::new();
            if let Some(spec) = spec_opt {
                seat_environment.push((anyagents_harness::SEAT_LAUNCH_ENV.into(), spec));
                seat_environment.push((
                    anyagents_harness::SEAT_PEERS_ENV.into(),
                    crate::sandbox::peer_principals(store)?,
                ));
            }
            muse_config::ensure_workspace_session(
                data,
                agent.id.as_str(),
                config,
                work_dir,
                agent.model.as_deref(),
                &seat_environment,
            )?;
        }
    } else {
        #[cfg(unix)]
        {
            repair_unseated_config_mode(config, pre_existing_mode.unwrap_or(0o0700))?;
        }
    }
    let verify_selector = if spiron_cloud_active {
        "spiron-cloud"
    } else if agent.canonical_harness() == "spiron" {
        "spiron-local"
    } else {
        &agent.harness
    };
    verify_harness_runtime_config(config, verify_selector, agent.model.as_deref())?;
    Ok(continuity)
}

/// Seed `<agent>/config/cloud.json` when Spiron runs via Cloud (`spiron` default
/// or explicit `spiron-cloud`), preserving any caller-provided keys while
/// discovering `spiron_sk_...`, JWTs, or Sylphx/Spiron API keys cleanly.
fn ensure_spiron_cloud_config(
    data: &Path,
    config: &Path,
    _model: Option<&str>,
) -> Result<(), Error> {
    if config_writes_suppressed(data) {
        return Ok(());
    }
    let cloud_path = config.join("cloud.json");
    let mut map = if let Ok(raw) = std::fs::read_to_string(&cloud_path)
        && let Ok(serde_json::Value::Object(existing)) = serde_json::from_str(&raw)
    {
        existing
    } else {
        serde_json::Map::new()
    };

    if !map.contains_key("base_url")
        && !map.contains_key("endpoint")
        && let Ok(url) = std::env::var("ANYAGENTS_SPIRON_CLOUD_URL")
            .or_else(|_| std::env::var("SPIRON_CLOUD_URL"))
        && !url.trim().is_empty()
    {
        map.insert(
            "base_url".to_string(),
            serde_json::Value::String(url.trim().trim_end_matches('/').to_string()),
        );
    }

    let has_token = ["api_key", "apiKey", "token", "bearer_token", "access_token"]
        .iter()
        .any(|k| {
            map.get(*k)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        });
    if !has_token && let Some(tok) = discover_spiron_cloud_auth_token(config) {
        let field = if tok.starts_with("spiron_sk_") {
            "api_key"
        } else {
            "token"
        };
        map.insert(field.to_string(), serde_json::Value::String(tok));
    }

    let bytes = serde_json::to_vec_pretty(&serde_json::Value::Object(map))
        .map_err(|err| Error::Message(err.to_string()))?;
    write_required_file(&cloud_path, &bytes, 0o600)?;
    Ok(())
}

fn discover_spiron_cloud_auth_token(config: &Path) -> Option<String> {
    for env_key in [
        "ANYAGENTS_SPIRON_API_KEY",
        "SPIRON_API_KEY",
        "SYLPHX_API_KEY",
    ] {
        if let Ok(v) = std::env::var(env_key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    for rel in [
        ".spiron/auth.json",
        ".spiron/credentials.json",
        ".config/sylphx/auth.json",
        ".config/sylphx/credentials.json",
        "auth.json",
        "credentials.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(config.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
        {
            for ptr in [
                "/api_key",
                "/apiKey",
                "/token",
                "/bearer_token",
                "/access_token",
                "/accessToken",
            ] {
                if let Some(s) = val.pointer(ptr).and_then(serde_json::Value::as_str) {
                    let t = s.trim();
                    if !t.is_empty() {
                        return Some(t.to_string());
                    }
                }
            }
        }
    }
    for rel in ["sylphx.key", ".config/opencode/sylphx.key", "spiron.key"] {
        if let Ok(raw) = std::fs::read_to_string(config.join(rel)) {
            let t = raw.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

/// Verifies that the active harness's runtime configuration file exists on disk
/// and contains the required AnyAgents MCP server, model, and provider bindings
/// before any harness process is allowed to start.
pub(crate) fn verify_harness_runtime_config(
    config: &Path,
    harness: &str,
    model: Option<&str>,
) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some() {
        return Err(Error::Message(format!(
            "harness runtime config verification failed for `{harness}`: config write suppressed"
        )));
    }
    let has_model = model.map(str::trim).is_some_and(|m| !m.is_empty());
    match harness {
        "codex" => {
            let path = config.join("config.toml");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("codex runtime config missing: {}", path.display()))
            })?;
            if !text.contains("[mcp_servers.anyagents]") || !text.contains("command =") {
                return Err(Error::Message(
                    "codex config.toml missing [mcp_servers.anyagents]".into(),
                ));
            }
            if has_model
                && !text.lines().any(|l| {
                    let t = l.trim_start();
                    t.starts_with("model =") || t.starts_with("model=")
                })
            {
                return Err(Error::Message("codex config.toml missing model".into()));
            }
            let profile_is_sylphx = std::fs::read_to_string(config.join(".anyagents-profile"))
                .ok()
                .is_some_and(|s| {
                    let t = s.trim();
                    t == "sylphx" || t.starts_with("sylphx/")
                });
            let model_is_sylphx = model
                .map(str::trim)
                .is_some_and(|m| m.starts_with("sylphx/"));
            if (profile_is_sylphx || model_is_sylphx)
                && (!text.contains("model_provider = \"sylphx\"")
                    || !text.contains("[model_providers.sylphx]"))
            {
                return Err(Error::Message(
                    "codex config.toml missing sylphx model_provider".into(),
                ));
            }
        }
        "opencode" => {
            for rel in ["opencode/opencode.json", ".config/opencode/opencode.json"] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!(
                        "opencode runtime config missing: {}",
                        path.display()
                    ))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "opencode runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.pointer("/mcp/anyagents/command").is_none() {
                    return Err(Error::Message(format!(
                        "opencode config missing mcp.anyagents: {}",
                        path.display()
                    )));
                }
                if has_model
                    && val
                        .get("model")
                        .and_then(|v| v.as_str())
                        .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(Error::Message(format!(
                        "opencode config missing model: {}",
                        path.display()
                    )));
                }
            }
        }
        "muse" => {
            for rel in [
                "settings.json",
                "muse/settings.json",
                ".config/muse/settings.json",
            ] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!("muse runtime config missing: {}", path.display()))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "muse runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.get("schema_version").is_none()
                    || val.pointer("/mcpServers/anyagents/command").is_none()
                {
                    return Err(Error::Message(format!(
                        "muse settings.json missing schema_version or mcpServers.anyagents: {}",
                        path.display()
                    )));
                }
            }
        }
        "agy" => {
            let path = config.join(".gemini/config/mcp_config.json");
            let bytes = std::fs::read(&path).map_err(|_| {
                Error::Message(format!("agy runtime config missing: {}", path.display()))
            })?;
            let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                Error::Message(format!(
                    "agy runtime config invalid JSON: {}",
                    path.display()
                ))
            })?;
            if val.pointer("/mcpServers/anyagents/command").is_none() {
                return Err(Error::Message(
                    "agy mcp_config.json missing mcpServers.anyagents".into(),
                ));
            }
        }
        "omp" => {
            let path = config.join(".omp/agent/mcp.json");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("omp runtime config missing: {}", path.display()))
            })?;
            if !text.contains("\"anyagents\"") {
                return Err(Error::Message("omp mcp.json missing anyagents".into()));
            }
        }
        selector
            if crate::vendor::is_spiron_harness(selector)
                || anyagents_harness::decompose_harness_selector(selector)
                    .is_ok_and(|(h, _)| h == "spiron") =>
        {
            let data_dir = config
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent);
            let implied_rt = anyagents_harness::decompose_harness_selector(selector)
                .ok()
                .and_then(|(_, rt)| rt);
            let local_exec = matches!(implied_rt, Some(anyagents_core::ExecutionRuntime::Local))
                || (implied_rt.is_none()
                    && crate::vendor::spiron_uses_local_execution(
                        selector,
                        data_dir,
                        Some(config),
                    ));
            let cloud_path = config.join("cloud.json");
            if !local_exec {
                if cloud_path.is_file() {
                    let bytes = std::fs::read(&cloud_path).map_err(|err| {
                        Error::Message(format!(
                            "spiron cloud.json unreadable {}: {err}",
                            cloud_path.display()
                        ))
                    })?;
                    serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|_| {
                        Error::Message(format!(
                            "spiron cloud.json invalid JSON: {}",
                            cloud_path.display()
                        ))
                    })?;
                    return Ok(());
                }
                if !config.join("spiron-mcp.json").is_file() {
                    return Err(Error::Message(format!(
                        "spiron cloud runtime config missing: {}",
                        cloud_path.display()
                    )));
                }
            }
            let path = config.join("spiron-mcp.json");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("spiron runtime config missing: {}", path.display()))
            })?;
            if !text.contains("\"anyagents\"") {
                return Err(Error::Message(
                    "spiron-mcp.json missing anyagents MCP server".into(),
                ));
            }
            let cfg_toml = config.join("config.toml");
            let toml_text = std::fs::read_to_string(&cfg_toml).map_err(|_| {
                Error::Message(format!(
                    "spiron config.toml missing: {}",
                    cfg_toml.display()
                ))
            })?;
            if !toml_text.contains("always_approve = true") {
                return Err(Error::Message(
                    "spiron config.toml missing always_approve = true".into(),
                ));
            }
        }
        _ => {
            for rel in ["mcp.json", ".claude.json"] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!(
                        "claude-code runtime config missing: {}",
                        path.display()
                    ))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "claude-code runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.pointer("/mcpServers/anyagents/command").is_none() {
                    return Err(Error::Message(format!(
                        "claude-code {} missing mcpServers.anyagents",
                        path.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Notifies a sub-agent's parent once when the sub-agent enters `turn_state == "errored"`.
/// Uses both a marker file in `config` (`.anyagents-notified-parent-error.json`) and
/// `Store::receive`'s ACID `source_key` deduplication so the parent never receives
/// duplicate alerts for the same error episode.
pub(crate) fn maybe_notify_parent_on_error(
    store: &Store,
    agent: &Agent,
    config: &Path,
    err_msg: &str,
) {
    let Some(ref parent_id) = agent.parent_id else {
        return;
    };
    let trimmed = err_msg.trim();
    if trimmed.is_empty() || crate::host_memory::is_migration_wait(trimmed) {
        return;
    }
    let rotated_ms = std::fs::read_to_string(config.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let marker_path = config.join(".anyagents-notified-parent-error.json");
    let source_key = format!(
        "subagent-error:{}:{rotated_ms}:{trimmed}",
        agent.id.as_str()
    );
    if let Ok(existing) = std::fs::read_to_string(&marker_path)
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&existing)
        && val.get("source_key").and_then(|v| v.as_str()) == Some(source_key.as_str())
    {
        return;
    }
    let marker = serde_json::json!({
        "source_key": source_key,
        "error": trimmed,
        "timestamp_ms": anyagents_core::now_ms(),
    });
    write_seat_shared_file(&marker_path, marker.to_string().as_bytes(), 0o664);
    let text = format!(
        "[Sub-Agent Error] Sub-agent `{}` (`{}`) entered `turn_state: \"errored\"` on harness `{}`: {}",
        agent.name,
        agent.id.as_str(),
        agent.harness,
        trimmed
    );
    if let Ok(anyagents_core::ReceiveOutcome::New(_)) = store.receive(
        parent_id,
        anyagents_core::Incoming {
            text,
            sender: Some(format!("agent:{}", agent.name)),
            source_time_ms: None,
            source_key: Some(source_key),
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        },
    ) {
        poke_agent_wake_socket_mail(store.data_dir(), parent_id);
    }
}

pub(crate) fn poke_agent_wake_socket_mail(data: &Path, agent_id: &AgentId) {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let sock_path = data.join("wake.sock");
        if sock_path.exists()
            && let Ok(sock) = UnixDatagram::unbound()
        {
            let msg = format!("m{}", agent_id.as_str());
            let _ = sock.send_to(msg.as_bytes(), &sock_path);
        }
    }
}

/// Install the harness a runtime change moves an agent onto. A change of model
/// or profile alone keeps the binary the agent already runs, so it installs
/// nothing and cannot fail on that harness's source.
pub(super) fn install_changed_harness(
    data: &Path,
    previous_harness: &str,
    target_harness: &str,
    record: impl FnOnce(&Path, &str) -> Result<crate::install::Install, String>,
) -> Result<(), Error> {
    if normalize_harness_name(previous_harness) == target_harness {
        return Ok(());
    }
    record(data, target_harness)
        .map(|_| ())
        .map_err(Error::Message)
}

pub(crate) fn configure_agent_runtime(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<serde_json::Value, Error> {
    configure_agent_runtime_with_options(
        store, data, actor, name, harness, None, model, profile, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn configure_agent_runtime_with_options(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: Option<&str>,
    runtime: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
    budget_cap_micros: Option<Option<u64>>,
) -> Result<serde_json::Value, Error> {
    let existing = store.agent_by_name(name)?;
    // One transition per agent at a time, across processes: a harness, model
    // or profile switch rewrites the seat's markers and vendor configs, and
    // pause, wake, reset and archive hold the same lock while they touch it.
    // An automatic rotation skips a busy agent instead of queueing behind it.
    let _transition = if actor.starts_with("system:") {
        store.try_lock_agent_lifecycle(&existing.id)?
    } else {
        store.lock_agent_lifecycle(&existing.id)?
    };
    let existing = store.agent(&existing.id)?;
    let previous_harness = existing.harness.clone();
    let previous_runtime = existing.runtime.clone();
    let resolved_model = model
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| existing.model.clone());
    let config = agent_config_dir(data, &existing.id);
    let raw_harness = match harness.map(str::trim) {
        Some("") => return Err(Error::Message("harness is empty".into())),
        Some(h) => h.to_string(),
        None => existing.canonical_harness(),
    };
    let resolved = anyagents_harness::resolve_harness_and_runtime(
        &raw_harness,
        runtime,
        existing.runtime.clone(),
    )
    .map_err(|err| Error::Message(err.to_string()))?;
    let remote_required = crate::sandbox_providers::SandboxMode::from_env().is_remote_required();
    if remote_required && resolved.runtime == anyagents_core::ExecutionRuntime::Local {
        return Err(Error::Message(
            "runtime `local` is refused when ANYAGENTS_SANDBOX_MODE=remote_required; use `sandbox` or `cloud`"
                .into(),
        ));
    }
    let target_harness = resolved.harness.clone();
    let target_runtime = resolved.runtime.clone();
    let adapter_selector = resolved.legacy_harness_selector();
    crate::install::adapter(&target_harness)
        .or_else(|_| crate::install::adapter(&adapter_selector))?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && !crate::sandbox::is_isolated_test_env()
        && !remote_required
        && actor != "system:profile-rebalance"
    {
        let install_target = if target_runtime.is_cloud() {
            adapter_selector.as_str()
        } else {
            target_harness.as_str()
        };
        install_changed_harness(
            data,
            &previous_harness,
            install_target,
            crate::install::record,
        )?;
    }
    let _ = crate::sandbox_exec::hydrate_agent_files_from_store(store, &existing.id, &config);
    let current_bound = bound_profile_name(&config);

    let (provider, profile_name) =
        if let Some(explicit_prof) = profile.map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((p, n)) = explicit_prof.split_once('/') {
                (p.trim().to_string(), n.trim().to_string())
            } else {
                (
                    upstream_provider_of(&target_harness, resolved_model.as_deref()),
                    explicit_prof.to_string(),
                )
            }
        } else if let Some(ref bound) = current_bound {
            if let Some((p, n)) = bound.split_once('/') {
                let target_prov = upstream_provider_of(&target_harness, resolved_model.as_deref());
                if model.is_some() && p.trim() != target_prov {
                    (target_prov, "default".to_string())
                } else {
                    (p.trim().to_string(), n.trim().to_string())
                }
            } else {
                (
                    upstream_provider_of(&target_harness, resolved_model.as_deref()),
                    bound.trim().to_string(),
                )
            }
        } else {
            (
                upstream_provider_of(&target_harness, resolved_model.as_deref()),
                "default".to_string(),
            )
        };

    ensure_profile_compatible_and_mapped(
        data,
        &provider,
        &profile_name,
        &target_harness,
        resolved_model.as_deref(),
        Some(&config),
    )?;
    let profile_dir = data.join("profiles").join(&provider).join(&profile_name);
    if data.join("profiles").is_dir() && !profile_dir.is_dir() && provider != target_harness {
        return Err(Error::Message(format!(
            "provider profile not found: `{provider}/{profile_name}`"
        )));
    }

    let open_turn = store.has_open_turn(&existing.id).unwrap_or(false);
    let lock_busy = matches!(store.try_lock_agent(&existing.id), Err(Error::AgentBusy));
    let in_progress_turn = existing.state == "running" && (open_turn || lock_busy);

    let audit_action = if runtime.is_some() || budget_cap_micros.is_some() {
        "agents.runtime"
    } else if harness.is_some() {
        "agents.harness"
    } else {
        "agents.model"
    };
    store.record_admin_audit(actor, audit_action, &existing.name)?;
    if existing.harness != target_harness || existing.runtime != target_runtime {
        store.set_agent_harness_and_runtime(
            &existing.id,
            &target_harness,
            &target_runtime,
            resolved_model.as_deref(),
        )?;
    } else if let Some(ref m) = resolved_model
        && existing.model.as_deref() != Some(m.as_str())
    {
        store.set_agent_model(&existing.id, m)?;
    }
    if let Some(cap) = budget_cap_micros {
        store.set_agent_budget_micros(&existing.id, cap)?;
    }
    let updated = store.agent(&existing.id)?;

    create_private_config_dir(&config)?;
    let normalized_profile = format!("{provider}/{profile_name}");
    if profile.is_some() && profile_dir.is_dir() {
        copy_profile_tree(&profile_dir, &config)?;
    }
    let now_rot = anyagents_core::now_ms();
    let _ = write_profile_marker(
        &config,
        ".anyagents-quota-cleared-ms",
        &format!("{now_rot}\n"),
    );
    if !in_progress_turn || current_bound.as_deref() != Some(normalized_profile.as_str()) {
        write_profile_marker(
            &config,
            ".anyagents-profile-rotated-ms",
            &format!("{now_rot}\n"),
        )?;
    }
    write_profile_marker(
        &config,
        ".anyagents-profile",
        &format!("{normalized_profile}\n"),
    )?;
    let _ = std::fs::remove_file(config.join(".anyagents-last-error.json"));
    if updated.harness == "agy" {
        clear_stale_agy_quota_error_steps(&config);
    }

    let work_dir = updated
        .workdir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| agent_work_dir(data, &updated.id));
    reconcile_agent_runtime_config(store, data, &updated, &config, &work_dir)?;
    super::persist_spawned_agent_config_to_store(store, &updated.id, &config);
    poke_agent_wake_socket(data, &updated.id);

    let process = if lock_busy { "alive" } else { "down" };
    let turn_state = if in_progress_turn { "working" } else { "idle" };
    let cutover_status = if in_progress_turn {
        "pending_active_turn"
    } else if lock_busy && previous_harness == target_harness && previous_runtime == target_runtime
    {
        "running"
    } else {
        "ready_on_next_wake"
    };

    Ok(serde_json::json!({
        "id": updated.id.as_str(),
        "name": updated.name,
        "agent": updated.name,
        "parent": updated.parent_id.as_ref().map(|p| p.as_str()),
        "previous_harness": previous_harness,
        "previous_runtime": previous_runtime.as_canonical_string(),
        "harness": updated.harness,
        "runtime": updated.runtime.as_canonical_string(),
        "remote_budget_micros": updated.remote_budget_micros,
        "model": updated.model,
        "profile": normalized_profile,
        "workdir": updated.workdir,
        "state": updated.state,
        "process": process,
        "turn_state": turn_state,
        "in_progress_turn": in_progress_turn,
        "cutover_status": cutover_status,
        "description_preserved": updated.instructions.is_some(),
    }))
}

#[cfg(test)]
pub(crate) fn set_agent_harness(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: &str,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<serde_json::Value, Error> {
    set_agent_harness_with_options(
        store, data, actor, name, harness, None, model, profile, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn set_agent_harness_with_options(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: &str,
    runtime: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
    budget_cap_micros: Option<Option<u64>>,
) -> Result<serde_json::Value, Error> {
    if harness.trim().is_empty() {
        return Err(Error::Message("harness is empty".into()));
    }
    configure_agent_runtime_with_options(
        store,
        data,
        actor,
        name,
        Some(harness),
        runtime,
        model,
        profile,
        budget_cap_micros,
    )
}

pub(crate) fn poke_agent_wake_socket(data: &Path, agent_id: &AgentId) {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let sock_path = data.join("wake.sock");
        if sock_path.exists()
            && let Ok(sock) = UnixDatagram::unbound()
        {
            let msg = format!("a{}", agent_id.as_str());
            let _ = sock.send_to(msg.as_bytes(), &sock_path);
        }
    }
}
