//! Profile markers, credential seeding, and propagation.

use std::path::Path;

use anyagents_core::{Agent, Error, Store, agent_config_dir};
use anyagents_harness::normalize_harness_name;

use super::profile_rotation::select_or_bind_profile;
use super::profiles::{copy_seat_file_if_newer, json_oauth_expires_at, json_oauth_identity};
use super::runtime::heal_mcp_config_file;
use super::{
    bound_profile_name, bound_profile_of_provider, chmod_seat_file_readable,
    compatible_harnesses_for, ensure_agy_mcp_config, extract_file_tilde_refs,
    poke_agent_wake_socket, resolved_agent_seat_info, upstream_provider_of, write_required_file,
    write_seat_shared_file,
};

/// Markers live in an agent tree that creation made private. Writing one never
/// makes that tree: a writer holding a row read before its creation made the
/// tree, or after a failed creation removed it, would otherwise leave an
/// orphan `<data>/agents/<id>` with umask permissions, which every seat's
/// peer-boundary check refuses. Only a config directory missing inside an
/// existing agent root is made, and privately.
pub(crate) fn write_profile_marker(
    config_dir: &Path,
    marker_file: &str,
    content: &str,
) -> Result<(), Error> {
    if crate::server::serverless::is_serverless_mode()
        && !config_dir.is_dir()
        && !config_dir.parent().is_some_and(Path::is_dir)
    {
        // Stateless serverless coordinators store durable state in PostgreSQL and do
        // not maintain local per-agent trees under `<data>/agents/<id>`; skip local
        // marker persistence cleanly instead of spamming ENOENT errors.
        return Ok(());
    }
    if !config_dir.is_dir() {
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(config_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && config_dir.is_dir() => {}
            Err(err) => return Err(err.into()),
        }
    }
    anyagents_harness::atomic_write_in_existing_dir(
        &config_dir.join(marker_file),
        content.as_bytes(),
        0o664,
    )
    .map_err(Error::from)
}

pub(crate) fn seeded_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-seeded-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

// Both paths are aliases within one selected profile, not independent accounts.
// Prefer the usable token with the latest expiry rather than a stale root alias.
pub(super) fn read_anthropic_profile_credentials(profile_root: &Path) -> Option<Vec<u8>> {
    [".credentials.json", ".claude/.credentials.json"]
        .into_iter()
        .filter_map(|rel| std::fs::read(profile_root.join(rel)).ok())
        .reduce(|current, candidate| {
            if json_oauth_expires_at(&candidate) > json_oauth_expires_at(&current) {
                candidate
            } else {
                current
            }
        })
}

// Do not roll back a token pair already rotated by the CLI for the same account.
fn retain_anthropic_credentials(source: &[u8], destination: &[u8]) -> bool {
    json_oauth_expires_at(destination) > json_oauth_expires_at(source)
        && !matches!(
            (json_oauth_identity(source), json_oauth_identity(destination)),
            (Some(source_id), Some(destination_id)) if source_id != destination_id
        )
}

pub(crate) fn sync_anthropic_agent_credentials_from_profile(
    store: Option<&Store>,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
    profile_root: &Path,
) {
    if !target_config.is_dir() || !profile_root.is_dir() {
        return;
    }
    let Some(raw_src_bytes) = read_anthropic_profile_credentials(profile_root) else {
        return;
    };
    let now_ms = anyagents_core::now_ms();
    let mut rewrote_any = false;
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let dst = target_config.join(rel);
        let needs_rewrite = match std::fs::read(&dst) {
            Ok(dst_bytes) => {
                dst_bytes != raw_src_bytes
                    && !retain_anthropic_credentials(&raw_src_bytes, &dst_bytes)
            }
            Err(_) => !dst.is_dir(),
        };
        if needs_rewrite && write_seat_shared_file(&dst, &raw_src_bytes, 0o660) {
            rewrote_any = true;
        }
    }
    if rewrote_any && let Some(s) = store {
        if let Some((_, uid, gid, _)) = resolved_agent_seat_info(s, agent) {
            crate::sandbox::share_config_with_seat_uid(target_config, uid, gid);
        }
        if !s.has_open_turn(&agent.id).unwrap_or(false) {
            let _ = write_profile_marker(
                target_config,
                ".anyagents-profile-rotated-ms",
                &format!("{now_ms}\n"),
            );
            let _ = std::fs::remove_file(target_config.join(".anyagents-last-error.json"));
            poke_agent_wake_socket(data, &agent.id);
        }
    }
}

pub(crate) fn seed_harness_credentials(
    store: &Store,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
) -> Result<(), Error> {
    super::profiles::hydrate_profiles_from_store_if_postgres(store, data);
    let mut all_agents = store.list_agents()?;
    if all_agents.len() > 32 {
        all_agents.retain(|a| a.state != "archived");
    }
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let static_rel_paths: &[&str] = match normalize_harness_name(&agent.harness) {
        "claude-code" | "claude-cloud" => &[
            ".credentials.json",
            ".claude/.credentials.json",
            ".claude.json",
            ".claude/.claude.json",
        ],
        "codex" | "openai-cloud" if provider == "openai" => &[
            "auth.json",
            ".codex/auth.json",
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
        ],
        "codex" | "openai-cloud" => &[
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
        ],
        "opencode" => &[
            "opencode/opencode.json",
            ".config/opencode/opencode.json",
            "opencode.json",
            "data-home/opencode/auth.json",
            ".local/share/opencode/auth.json",
        ],
        "agy" => &[
            ".gemini/antigravity-cli/antigravity-oauth-token",
            ".gemini/antigravity-cli/antigravity_state.pbtxt",
            ".gemini/antigravity-cli/jetski_state.pbtxt",
            ".gemini/config/config.json",
            ".gemini/config/.migrated",
        ],
        "muse" => &[
            "settings.json",
            "muse/settings.json",
            "muse/trust.json",
            "muse/auth.json",
            ".config/muse/settings.json",
            ".config/muse/trust.json",
            ".config/muse/auth.json",
            ".local/share/muse/auth.json",
        ],
        "omp" => &[
            ".omp/agent/agent.db",
            ".omp/agent/models.yml",
            ".omp/agent/models.db",
            "agent.db",
            "models.yml",
            "models.db",
        ],
        "dsh" => &[
            ".dsh/.credentials.yaml",
            ".dsh/settings.yaml",
            ".dsh/.anonymous-user-id",
            ".dsh/profiles/headless/cordis.yml",
        ],
        "grok" => &[
            ".grok/auth.json",
            ".grok/models_cache.json",
            ".grok/settings_cache.json",
        ],
        "spiron" | "spiron-cloud" => &[
            "spiron-agent.json",
            "sylphx-agent.json",
            "opencode.json",
            "opencode/opencode.json",
            "auth.json",
            "credentials.json",
            "config.toml",
            ".config/sylphx/auth.json",
            ".spiron/auth.json",
            ".config/sylphx/credentials.json",
            ".config/sylphx/context.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/kimi-code-plan.key",
            ".config/opencode/kimi.key",
            ".config/opencode/qwen-token-plan.key",
            ".config/opencode/qwen-coding-plan.key",
            ".config/opencode/glm-code-plan.key",
            ".config/opencode/minimax-code-plan.key",
            ".config/opencode/openrouter.key",
            ".config/opencode/opencode.json",
        ],
        "cursor" | "cursor-cloud" => &["cursor-auth.json", "auth.json", "credentials.json"],
        _ => &[],
    };
    let mut rel_paths: Vec<String> = static_rel_paths.iter().map(|s| (*s).to_string()).collect();

    let profile_roots = select_or_bind_profile(data, &all_agents, agent, target_config);
    let current_bound = bound_profile_name(target_config);
    let last_seeded = seeded_profile_name(target_config);
    let explicit_non_default_profile = current_bound.as_ref().is_some_and(|b| {
        let prof = b.split_once('/').map(|(_, p)| p).unwrap_or(b.as_str());
        !prof.is_empty() && prof != "default"
    });
    let profile_changed = current_bound.is_some()
        && (current_bound != last_seeded
            || (last_seeded.is_none() && explicit_non_default_profile));

    if agent.harness == "codex" && provider != "openai" {
        for stale_rel in ["auth.json", ".codex/auth.json"] {
            let stale_path = target_config.join(stale_rel);
            if stale_path.exists() {
                let _ = std::fs::remove_file(&stale_path);
            }
        }
    }

    if let (Some(bound), Some(primary_profile_root)) =
        (current_bound.clone(), profile_roots.first().cloned())
    {
        if bound.starts_with("sylphx/") || bound.starts_with("spiron/") {
            crate::spiron::ensure_spiron_seat_auth(&primary_profile_root);
        }
        let harvest_peers = all_agents.len() <= 32;
        for other in &all_agents {
            if !harvest_peers && other.id != agent.id {
                continue;
            }
            let other_cfg = agent_config_dir(data, &other.id);
            let other_bound = bound_profile_name(&other_cfg);
            if (other.id == agent.id && profile_changed && last_seeded.is_some())
                || seeded_profile_name(&other_cfg)
                    .is_some_and(|seeded| Some(seeded.as_str()) != other_bound.as_deref())
            {
                continue;
            }
            let same_or_compatible_bound = other_bound.as_deref() == Some(bound.as_str())
                || other_bound.as_deref().is_some_and(|ob| {
                    let (b_prov, b_prof) = bound.split_once('/').unwrap_or((&bound, "default"));
                    let (o_prov, o_prof) = ob.split_once('/').unwrap_or((ob, "default"));
                    b_prof == o_prof && compatible_harnesses_for(b_prov).contains(&o_prov)
                });
            if same_or_compatible_bound {
                for rel in [
                    ".credentials.json",
                    ".claude/.credentials.json",
                    "auth.json",
                    ".codex/auth.json",
                    ".config/sylphx/auth.json",
                    ".spiron/auth.json",
                    ".config/sylphx/credentials.json",
                    "credentials.json",
                    "models_cache.json",
                    "model-catalog-direct.json",
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                ] {
                    if rel == ".codex/auth.json" && !bound.starts_with("openai/") {
                        continue;
                    }
                    if rel == "auth.json"
                        && !bound.starts_with("openai/")
                        && !bound.starts_with("sylphx/")
                        && !bound.starts_with("spiron/")
                    {
                        continue;
                    }
                    let other_src = other_cfg.join(rel);
                    if other_src.is_file() {
                        let _ =
                            copy_seat_file_if_newer(&other_src, &primary_profile_root.join(rel));
                    }
                }
            }
        }
    }
    ensure_agy_mcp_config(store, data, agent, &profile_roots, target_config);

    for profile_root in &profile_roots {
        for candidate in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Ok(text) = std::fs::read_to_string(profile_root.join(candidate)) {
                for r in extract_file_tilde_refs(&text) {
                    if !rel_paths.contains(&r) {
                        rel_paths.push(r);
                    }
                }
            }
        }
    }
    for rel in &rel_paths {
        let dst = target_config.join(rel);
        let is_anthropic_cred = matches!(
            rel.as_str(),
            ".credentials.json" | ".claude/.credentials.json"
        );
        let is_auth_cred = rel.ends_with(".key")
            || is_anthropic_cred
            || matches!(
                rel.as_str(),
                ".gemini/antigravity-cli/antigravity-oauth-token"
                    | ".gemini/antigravity-cli/antigravity_state.pbtxt"
                    | ".gemini/antigravity-cli/jetski_state.pbtxt"
                    | "auth.json"
                    | ".codex/auth.json"
                    | "credentials.json"
                    | ".config/sylphx/auth.json"
                    | ".spiron/auth.json"
                    | ".config/sylphx/credentials.json"
                    | "muse/auth.json"
                    | ".config/muse/auth.json"
                    | ".grok/auth.json"
                    | ".dsh/.credentials.yaml"
                    | ".omp/agent/agent.db"
                    | "agent.db"
            );
        let is_seat_selector = matches!(rel.as_str(), "spiron-agent.json" | "sylphx-agent.json");
        let dst_non_empty = dst.metadata().is_ok_and(|m| m.len() > 0);
        if !profile_changed && !is_seat_selector && dst_non_empty && !is_auth_cred {
            continue;
        }
        let mut found_in_profile = false;
        for profile_root in &profile_roots {
            let mut src = profile_root.join(rel);
            if !src.is_file() {
                let fallbacks: &[&str] = match rel.as_str() {
                    ".claude/.credentials.json" => &[".credentials.json"],
                    ".codex/auth.json" => &["auth.json"],
                    ".config/muse/auth.json" => &["muse/auth.json"],
                    "auth.json" => &[".config/sylphx/auth.json", ".spiron/auth.json"],
                    ".config/sylphx/auth.json" => &["auth.json", ".spiron/auth.json"],
                    ".spiron/auth.json" => &["auth.json", ".config/sylphx/auth.json"],
                    "credentials.json" => &[".config/sylphx/credentials.json"],
                    ".config/sylphx/credentials.json" => &["credentials.json"],
                    _ => &[],
                };
                for fb in fallbacks {
                    let candidate = profile_root.join(fb);
                    if candidate.is_file() {
                        src = candidate;
                        break;
                    }
                }
            }
            let source = if is_anthropic_cred {
                read_anthropic_profile_credentials(profile_root)
            } else if src.is_file() {
                match std::fs::read(&src) {
                    Ok(bytes) => Some(bytes),
                    Err(_) => continue,
                }
            } else {
                None
            };
            if let Some(src_bytes) = source {
                let now_ms = anyagents_core::now_ms();
                if src_bytes.is_empty() && !is_auth_cred {
                    let already_empty = dst.is_file() && dst.metadata().is_ok_and(|m| m.len() == 0);
                    if !already_empty {
                        anyagents_harness::atomic_write_file(&dst, &src_bytes, 0o660)
                            .map_err(Error::from)?;
                    }
                    found_in_profile = true;
                    break;
                }
                if dst_non_empty {
                    if std::fs::read(&dst).is_err() {
                        chmod_seat_file_readable(&dst);
                    }
                    if let Ok(dst_bytes) = std::fs::read(&dst) {
                        if src_bytes == dst_bytes {
                            found_in_profile = true;
                            break;
                        }
                        if !profile_changed
                            && is_anthropic_cred
                            && retain_anthropic_credentials(&src_bytes, &dst_bytes)
                        {
                            found_in_profile = true;
                            break;
                        }
                        if !profile_changed && !is_anthropic_cred {
                            let different_identity = matches!(
                                (
                                    json_oauth_identity(&src_bytes),
                                    json_oauth_identity(&dst_bytes)
                                ),
                                (Some(s_id), Some(d_id)) if s_id != d_id
                            );
                            let src_exp = json_oauth_expires_at(&src_bytes);
                            let dst_exp = json_oauth_expires_at(&dst_bytes);
                            let newer_oauth = different_identity
                                || matches!(
                                    (src_exp, dst_exp),
                                    (Some(s_exp), Some(d_exp)) if s_exp > d_exp
                                )
                                || (src_exp.is_some_and(|s_exp| s_exp > now_ms)
                                    && dst_exp.is_none());
                            if !newer_oauth {
                                found_in_profile = true;
                                break;
                            }
                        }
                    } else if !profile_changed && !is_anthropic_cred {
                        found_in_profile = true;
                        break;
                    }
                }
                if !(dst_non_empty
                    && !is_auth_cred
                    && std::fs::read(&dst).is_ok_and(|cur| cur == src_bytes))
                {
                    write_required_file(&dst, &src_bytes, 0o660)?;
                }
                found_in_profile = true;
                break;
            }
        }
        if !found_in_profile
            && profile_roots.iter().any(|p| p.is_dir())
            && dst.exists()
            && (is_seat_selector
                || (profile_changed
                    && (is_auth_cred
                        || matches!(
                            rel.as_str(),
                            "config.toml"
                                | "opencode.json"
                                | "opencode/opencode.json"
                                | ".config/opencode/opencode.json"
                                | "settings.json"
                                | "models_cache.json"
                                | "model-catalog-direct.json"
                        ))))
        {
            let _ = std::fs::remove_file(&dst);
        }
    }
    if crate::vendor::is_spiron_harness(&agent.harness) {
        crate::spiron::ensure_spiron_seat_auth_for_model(target_config, agent.model.as_deref());
    }
    if store.is_postgres() {
        seed_missing_credentials_from_pg(store, &all_agents, agent, target_config);
    }
    if let Some(ref bound) = current_bound {
        write_profile_marker(
            target_config,
            ".anyagents-seeded-profile",
            &format!("{bound}\n"),
        )?;
        if profile_changed {
            write_profile_marker(
                target_config,
                ".anyagents-profile-rotated-ms",
                &format!("{}\n", anyagents_core::now_ms()),
            )?;
            let _ = std::fs::remove_file(target_config.join(".anyagents-last-error.json"));
        }
    }
    Ok(())
}

/// Stateless PostgreSQL fallback when local profile seeding left Claude Code or
/// Codex without credentials: consults `credential_bindings` -> `deployment_blobs("profiles")`
/// and parent/live-sibling `agent_files(namespace = 'secrets')` (plus safe Claude
/// `.claude.json` onboarding/account metadata, never copying another agent's `mcpServers`).
fn seed_missing_credentials_from_pg(
    store: &Store,
    all_agents: &[Agent],
    agent: &Agent,
    target_config: &Path,
) {
    let norm = normalize_harness_name(&agent.harness);
    let cred_rels: &[&str] = match norm {
        "claude-code" | "claude-cloud" => &[".credentials.json", ".claude/.credentials.json"],
        "codex" | "openai-cloud" => &["auth.json", ".codex/auth.json"],
        _ => return,
    };
    let has_cred = |dir: &Path| {
        cred_rels
            .iter()
            .any(|rel| dir.join(rel).metadata().is_ok_and(|m| m.len() > 2))
    };
    let blobs = store.blob_store();
    let write_all = |bytes: &[u8]| {
        for rel in cred_rels {
            let _ = write_seat_shared_file(&target_config.join(rel), bytes, 0o600);
        }
    };
    if !has_cred(target_config) {
        // 1. Explicit credential bindings on this agent or its parent.
        let mut binding_owners = vec![agent.id.clone()];
        if let Some(ref pid) = agent.parent_id {
            binding_owners.push(pid.clone());
        }
        'bind: for owner in binding_owners {
            if let Ok(bindings) = store.credential_bindings_of(&owner) {
                for b in bindings {
                    if let anyagents_core::CredentialTarget::Record { provider, name, .. } =
                        b.target
                    {
                        for rel in cred_rels {
                            let path = format!("{provider}/{name}/{rel}");
                            if let Ok(Some(entry)) = blobs.get_deployment_blob("profiles", &path)
                                && !entry.content.is_empty()
                            {
                                write_all(&entry.content);
                                break 'bind;
                            }
                        }
                    }
                }
            }
        }
    }
    // 2. Parent / compatible live sibling `agent_files(namespace = 'secrets')`.
    let mut donors: Vec<&Agent> = Vec::new();
    if let Some(ref pid) = agent.parent_id
        && let Some(p) = all_agents.iter().find(|a| &a.id == pid)
        && normalize_harness_name(&p.harness) == norm
    {
        donors.push(p);
    }
    for other in all_agents {
        if other.id != agent.id
            && other.state != "archived"
            && normalize_harness_name(&other.harness) == norm
            && !donors.iter().any(|d| d.id == other.id)
        {
            donors.push(other);
        }
    }
    for donor in donors {
        if !has_cred(target_config) {
            for rel in cred_rels {
                if let Ok(Some(entry)) = blobs.get_agent_file(&donor.id, "secrets", rel)
                    && !entry.content.is_empty()
                {
                    write_all(&entry.content);
                    break;
                }
            }
        }
        if matches!(norm, "claude-code" | "claude-cloud") {
            merge_donor_claude_account_meta(blobs.as_ref(), &donor.id, target_config);
        }
        if has_cred(target_config) {
            break;
        }
    }
}

fn merge_donor_claude_account_meta(
    blobs: &dyn anyagents_core::StateBlobStore,
    donor_id: &anyagents_core::AgentId,
    target_config: &Path,
) {
    let dst_path = target_config.join(".claude.json");
    let mut dst_val = std::fs::read_to_string(&dst_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::json!({}));
    if dst_val.get("oauthAccount").is_some() {
        return;
    }
    let donor_bytes = blobs
        .get_agent_file(donor_id, "secrets", ".claude/.claude.json")
        .ok()
        .flatten()
        .or_else(|| {
            blobs
                .get_agent_file(donor_id, "config", ".claude.json")
                .ok()
                .flatten()
        })
        .map(|e| e.content);
    let Some(bytes) = donor_bytes else {
        return;
    };
    let Ok(src_val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return;
    };
    if let Some(dst_obj) = dst_val.as_object_mut() {
        let mut changed = false;
        if let Some(acct) = src_val.get("oauthAccount") {
            dst_obj.insert("oauthAccount".into(), acct.clone());
            changed = true;
        }
        if dst_obj.get("hasCompletedOnboarding") != Some(&serde_json::Value::Bool(true)) {
            dst_obj.insert(
                "hasCompletedOnboarding".into(),
                serde_json::Value::Bool(true),
            );
            changed = true;
        }
        if changed && let Ok(out) = serde_json::to_vec_pretty(&dst_val) {
            let _ = write_seat_shared_file(&dst_path, &out, 0o600);
        }
    }
}

/// Declaratively update an existing agent's runtime configuration (`harness`, `model`, `profile`)
/// in `state.db` without deleting or recreating its identity, killing an in-progress turn,
/// replaying already-delivered messages, or reopening paused scope.
/// Copy a provider profile's files into an agent's seat on a profile switch.
///
/// Vendor runtime trees stay behind. So do the files the seat owns, which the
/// switch and its reconcile write for this agent: every `.anyagents-*` marker
/// and stamp (the profile's describe the profile), MCP registrations (a
/// profile's empty server map would remove every AnyAgents tool from a running
/// Antigravity cascade), vendor settings and hooks (the seat's carry its tool
/// guard and deny list), and instruction mirrors. Declared credential files
/// reach the seat through seeding. An in-flight atomic write's temporary file
/// is not copied, and a zero-byte file carries nothing: refusing it would abort
/// the switch half-way through the tree.
pub(super) fn copy_profile_tree(source: &Path, target: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let fname = entry.file_name();
        let name = fname.to_string_lossy();
        if name.starts_with(".anyagents-") || name.ends_with(".anyagents-tmp") {
            continue;
        }
        if matches!(
            name.as_ref(),
            "annotations"
                | "bin"
                | "brain"
                | "builtin"
                | "cache"
                | "cache-home"
                | "conversations"
                | "crashes"
                | "implicit"
                | "log"
                | "presence"
                | "updater"
                | "projects"
                | "sessions"
                | "session-env"
                | "shell-snapshots"
                | "backups"
                | "cli.log"
                | "conversation_summaries.db"
                | "conversation_summaries.db-shm"
                | "conversation_summaries.db-wal"
                | "profile.json"
                | "mcp_config.json"
                | "mcp.json"
                | ".claude.json"
                | "mcp-capability"
                | "settings.json"
                | "hooks.json"
                | "CLAUDE.md"
                | "AGENTS.md"
                | "GEMINI.md"
        ) {
            continue;
        }
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        let target = target.join(&fname);
        if kind.is_dir() {
            copy_profile_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            let bytes = match std::fs::read(entry.path()) {
                Ok(bytes) => bytes,
                // Replaced or removed by a concurrent writer since the listing.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if !bytes.is_empty() {
                write_required_file(&target, &bytes, 0o660)?;
            }
        }
    }
    Ok(())
}

/// How a credential write relates to the processes already running on it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CredentialChange {
    /// New credentials: running processes move to them at their next boundary.
    Replaced,
    /// The same account renewed its token. The harness renews tokens by itself,
    /// so this only refreshes files for future launches.
    Renewed,
}

pub(crate) fn propagate_profile_files_to_bound_agents(
    data: &Path,
    provider: &str,
    profile: &str,
    rel_paths: &[&str],
) -> Result<(), Error> {
    propagate_credentials(
        data,
        provider,
        profile,
        rel_paths,
        CredentialChange::Replaced,
    )
}

pub(super) fn propagate_credentials(
    data: &Path,
    provider: &str,
    profile: &str,
    rel_paths: &[&str],
    change: CredentialChange,
) -> Result<(), Error> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    if crate::server::serverless::is_serverless_mode() && !prof_dir.is_dir() {
        return Ok(());
    }
    let target_tag = format!("{provider}/{profile}");
    let store_opt = Store::open(data.join("state.db")).ok();
    let all_agents = store_opt
        .as_ref()
        .and_then(|s| s.list_agents().ok())
        .unwrap_or_default();
    let active_agents: Vec<&Agent> = all_agents
        .iter()
        .filter(|a| a.state != "archived")
        .collect();
    for agent_rec in active_agents {
        let cfg = agent_config_dir(data, &agent_rec.id);
        let dir_id = agent_rec.id.as_str();
        if !cfg.is_dir() {
            continue;
        }
        let bound = bound_profile_name(&cfg);
        let matches_target = match bound.as_deref() {
            Some(b) if b == target_tag => true,
            Some(b) if b == profile => {
                upstream_provider_of(&agent_rec.harness, agent_rec.model.as_deref()) == provider
            }
            None if profile == "default" => {
                upstream_provider_of(&agent_rec.harness, agent_rec.model.as_deref()) == provider
            }
            _ => false,
        };
        if !matches_target {
            continue;
        }
        let mut installed = 0usize;
        for rel in rel_paths {
            let mut src = prof_dir.join(rel);
            if !src.is_file() {
                let fb = match *rel {
                    ".claude/.credentials.json" => Some(".credentials.json"),
                    ".codex/auth.json" => Some("auth.json"),
                    ".config/muse/auth.json" => Some("muse/auth.json"),
                    _ => None,
                };
                if let Some(f) = fb {
                    src = prof_dir.join(f);
                }
            }
            let bytes = match std::fs::read(&src) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue, // Optional aliases may be absent.
                Err(err) => return Err(err.into()),
            };
            {
                if bytes.is_empty() {
                    return Err(Error::Message(format!(
                        "empty credential source for {dir_id}: {rel}"
                    )));
                }
                if let Err(err) = write_required_file(&cfg.join(rel), &bytes, 0o660) {
                    // No seeded/rotation marker or wake is published for this failed agent.
                    eprintln!("credential propagation failed for {dir_id}: {err}");
                    return Err(err);
                }
                installed += 1;
            }
        }
        if !rel_paths.is_empty() && installed == 0 {
            return Err(Error::Message(format!(
                "no profile credentials installed for {dir_id}"
            )));
        }
        // Success markers must describe credentials that the seat can read.
        if !crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
            && let Some(s) = store_opt.as_ref()
            && let Some((_, uid, gid, _)) = resolved_agent_seat_info(s, agent_rec)
        {
            crate::sandbox::share_config_with_seat_uid_checked(&cfg, uid, gid)?;
        }
        write_profile_marker(
            &cfg,
            ".anyagents-seeded-profile",
            &format!("{target_tag}\n"),
        )?;
        if change == CredentialChange::Replaced {
            let now_prop = anyagents_core::now_ms();
            let _ = write_profile_marker(
                &cfg,
                ".anyagents-quota-cleared-ms",
                &format!("{now_prop}\n"),
            );
            // A profile change holds no mail and never preempts a live turn:
            // the supervisor moves the process at its next idle or faulted point.
            write_profile_marker(
                &cfg,
                ".anyagents-profile-rotated-ms",
                &format!("{now_prop}\n"),
            )?;
            let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        }
        if store_opt.is_some() && change == CredentialChange::Replaced {
            poke_agent_wake_socket(data, &agent_rec.id);
        }
    }
    Ok(())
}

pub(crate) fn sync_fresher_bound_agent_credentials(data: &Path) {
    if let Ok(store) = Store::open(data.join("state.db")) {
        sync_fresher_bound_agent_credentials_in(&store, data);
    }
}

/// Copy fresher credentials between live agents' seats and their bound
/// profiles. Archived agents run no harness, so their seats never refresh a
/// token; a pair of files unchanged since the last pass is not read again.
pub(crate) fn sync_fresher_bound_agent_credentials_in(store: &Store, data: &Path) {
    let profiles_root = data.join("profiles");
    let now_ms = anyagents_core::now_ms();
    let sync_stamp = profiles_root.join(".last-agent-cred-sync-ms");
    if !crate::sandbox::is_isolated_test_env()
        && let Ok(raw) = std::fs::read_to_string(&sync_stamp)
        && let Ok(last_ms) = raw.trim().parse::<i64>()
        && now_ms.saturating_sub(last_ms) < 15_000
    {
        return;
    }
    write_seat_shared_file(&sync_stamp, format!("{now_ms}\n").as_bytes(), 0o600);
    if let Ok(entries) = std::fs::read_dir(profiles_root.join("google-antigravity")) {
        for entry in entries.flatten() {
            let prof_path = entry.path();
            if prof_path.is_dir() {
                let _ = heal_mcp_config_file(&prof_path.join(".gemini/config/mcp_config.json"));
                let _ = heal_mcp_config_file(&prof_path.join("mcp_config.json"));
            }
        }
    }
    if let Ok(agents) = store.list_live_agents() {
        for agent in &agents {
            // A seat still being created (or whose creation failed) has no
            // credentials to sync, and a write could recreate a removed tree.
            if agent.state == "created" {
                continue;
            }
            // A harness, profile or lifecycle transition holds this lock while
            // it rewrites the seat; the pass leaves that seat to it.
            let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent.id) else {
                continue;
            };
            let cfg = agent_config_dir(data, &agent.id);
            let bound = bound_profile_name(&cfg);
            let seeded = seeded_profile_name(&cfg);
            let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
            let Some(prof_name) = bound_profile_of_provider(&cfg, &provider) else {
                ensure_agy_mcp_config(store, data, agent, &[], &cfg);
                continue;
            };
            let profile_dir = profiles_root.join(&provider).join(&prof_name);
            ensure_agy_mcp_config(store, data, agent, std::slice::from_ref(&profile_dir), &cfg);
            if seeded.is_some() && bound.is_some() && seeded != bound {
                continue;
            }
            let rels: &[&str] = match provider.as_str() {
                "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
                "openai" => &["auth.json", ".codex/auth.json"],
                "google-antigravity" => &[".gemini/antigravity-cli/antigravity-oauth-token"],
                _ => &[],
            };
            let mut updated_any = false;
            for src_rel in rels {
                let src = cfg.join(src_rel);
                if src.is_file() {
                    for dst_rel in rels {
                        if copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel)) {
                            updated_any = true;
                        }
                    }
                }
            }
            if updated_any {
                let change = if provider == "google-antigravity" {
                    CredentialChange::Renewed
                } else {
                    CredentialChange::Replaced
                };
                propagate_credentials(data, &provider, &prof_name, rels, change)
                    .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
            } else if provider == "anthropic" {
                sync_anthropic_agent_credentials_from_profile(
                    Some(store),
                    data,
                    agent,
                    &cfg,
                    &profile_dir,
                );
            }
        }
    }
}
