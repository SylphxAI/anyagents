//! Profile markers, OAuth refresh disk reservation, Anthropic OAuth refresh, credential seeding, and propagation.

use std::path::{Path, PathBuf};

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

pub(super) const OAUTH_REFRESH_RESERVATION_FILE: &str = ".anyagents-refresh-reservation";

pub(super) const OAUTH_REFRESH_ATTEMPT_FILE: &str = ".anyagents-oauth-refresh-attempt-ms";

const OAUTH_REFRESH_MIN_RESERVE_BYTES: usize = 16_384;

pub(super) fn persist_refreshed_credentials(
    root: &Path,
    rels: &[&str],
    bytes: &[u8],
) -> Result<(), Error> {
    let recovery = root.join(".anyagents-pending-credentials.json");
    write_required_file(&recovery, bytes, 0o600)?;
    for rel in rels {
        write_required_file(&root.join(rel), bytes, 0o600)?;
    }
    std::fs::remove_file(recovery)?;
    Ok(())
}

pub(super) struct OAuthRefreshDiskReservation {
    profile_root: PathBuf,
    reservation_path: PathBuf,
}

impl OAuthRefreshDiskReservation {
    /// Fail-closed pre-flight guard before issuing any single-use or rotating
    /// OAuth `refresh_token` network request:
    /// 1. Ensures target credential parent directories exist under `profile_root`.
    /// 2. Pre-writes and `sync_all()`s `.anyagents-refresh-reservation`
    ///    (`>= max(existing_cred_bytes * 2, 8192)` bytes, scaled by target copies)
    ///    plus syncs `profile_root` so `ENOSPC`/`EROFS`/`EACCES` abort BEFORE the
    ///    OAuth server rotates and revokes the existing `refresh_token`.
    /// 3. Records `.anyagents-oauth-refresh-attempt-ms` via checked `write_required_file`.
    pub(super) fn acquire(
        profile_root: &Path,
        target_rels: &[&str],
        existing_cred_bytes: usize,
        attempt_ms: i64,
    ) -> Result<Self, Error> {
        for rel in target_rels {
            if let Some(parent) = profile_root.join(rel).parent()
                && parent != profile_root
            {
                std::fs::create_dir_all(parent)?;
            }
        }
        let copies = target_rels.len().saturating_add(2).max(2);
        let reserve_bytes = existing_cred_bytes
            .saturating_mul(copies)
            .max(existing_cred_bytes.saturating_mul(2))
            .max(OAUTH_REFRESH_MIN_RESERVE_BYTES);
        let reservation_path = profile_root.join(OAUTH_REFRESH_RESERVATION_FILE);
        if std::fs::symlink_metadata(&reservation_path)
            .is_ok_and(|meta| meta.file_type().is_symlink())
        {
            std::fs::remove_file(&reservation_path)?;
        }
        let write_res = (|| -> Result<(), Error> {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&reservation_path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            let payload = vec![b'0'; reserve_bytes];
            file.write_all(&payload)?;
            file.sync_all()?;
            drop(file);
            std::fs::File::open(profile_root)?.sync_all()?;
            Ok(())
        })();
        if let Err(err) = write_res {
            let _ = std::fs::remove_file(&reservation_path);
            return Err(err);
        }
        let guard = Self {
            profile_root: profile_root.to_path_buf(),
            reservation_path,
        };
        write_required_file(
            &profile_root.join(OAUTH_REFRESH_ATTEMPT_FILE),
            format!("{attempt_ms}\n").as_bytes(),
            0o600,
        )?;
        Ok(guard)
    }

    pub(super) fn persist_credentials(mut self, rels: &[&str], bytes: &[u8]) -> Result<(), Error> {
        let _ = std::fs::remove_file(&self.reservation_path);
        self.reservation_path.clear();
        persist_refreshed_credentials(&self.profile_root, rels, bytes)
    }
}

impl Drop for OAuthRefreshDiskReservation {
    fn drop(&mut self) {
        if !self.reservation_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.reservation_path);
        }
    }
}

pub(super) fn anthropic_oauth_usage_url() -> String {
    std::env::var("ANYAGENTS_ANTHROPIC_OAUTH_USAGE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://api.anthropic.com/api/oauth/usage".to_string())
}

pub(super) fn post_anthropic_oauth_refresh(
    http: &ureq::Agent,
    req_body: &str,
) -> Option<serde_json::Value> {
    let custom_url = std::env::var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let urls: Vec<&str> = match custom_url.as_deref() {
        Some(u) => vec![u],
        None => vec![
            "https://api.anthropic.com/v1/oauth/token",
            "https://platform.claude.com/v1/oauth/token",
        ],
    };
    for url in urls {
        if let Ok(rresp) = http
            .post(url)
            .header("content-type", "application/json")
            .header("user-agent", "claude-code/2.1.310")
            .send(req_body.as_bytes())
            && let Ok(rbody) = rresp.into_body().read_to_string()
            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
            && rval
                .get("access_token")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        {
            return Some(rval);
        }
    }
    None
}

fn seat_sanitized_anthropic_credentials(src_bytes: &[u8], now_ms: i64) -> Vec<u8> {
    if crate::sandbox::is_isolated_test_env()
        && std::env::var_os("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL").is_none()
    {
        return src_bytes.to_vec();
    }
    let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(src_bytes) else {
        return src_bytes.to_vec();
    };
    if let Some(oauth) = val
        .get_mut("claudeAiOauth")
        .and_then(serde_json::Value::as_object_mut)
    {
        let exp = oauth
            .get("expiresAt")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        oauth.insert("refreshToken".to_string(), serde_json::Value::Null);
        if exp <= now_ms {
            oauth.insert(
                "accessToken".to_string(),
                serde_json::Value::String(String::new()),
            );
            oauth.insert("expiresAt".to_string(), serde_json::Value::Number(0.into()));
        }
        if let Ok(out) = serde_json::to_vec(&val) {
            return out;
        }
    }
    src_bytes.to_vec()
}

// Both paths are aliases within one selected profile, not independent accounts.
// Prefer the usable token with the latest expiry rather than a stale root alias.
fn read_anthropic_profile_credentials(profile_root: &Path) -> Option<Vec<u8>> {
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

pub(super) fn refresh_anthropic_profile_oauth_if_due(
    data: &Path,
    profile_root: &Path,
) -> Result<(), Error> {
    if crate::sandbox::is_isolated_test_env()
        && std::env::var_os("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL").is_none()
    {
        return Ok(());
    }
    let Some(cred_bytes) = read_anthropic_profile_credentials(profile_root) else {
        return Ok(());
    };
    let Ok(mut cred) = serde_json::from_slice::<serde_json::Value>(&cred_bytes) else {
        return Ok(());
    };
    let now_ms = anyagents_core::now_ms();
    let expires_at = cred
        .pointer("/claudeAiOauth/expiresAt")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(i64::MAX);
    if expires_at > now_ms.saturating_add(45 * 60 * 1_000) {
        return Ok(());
    }
    let attempt_path = profile_root.join(".anyagents-oauth-refresh-attempt-ms");
    let last_attempt_ms = std::fs::read_to_string(&attempt_path)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let min_retry_ms = if expires_at <= now_ms.saturating_add(30 * 60 * 1_000) {
        60_000
    } else {
        300_000
    };
    if now_ms.saturating_sub(last_attempt_ms) < min_retry_ms {
        return Ok(());
    }
    let Some(rtok) = cred
        .pointer("/claudeAiOauth/refreshToken")
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Ok(());
    };
    let reservation = OAuthRefreshDiskReservation::acquire(
        profile_root,
        &[".credentials.json", ".claude/.credentials.json"],
        cred_bytes.len(),
        now_ms,
    )?;
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(8)))
        .build()
        .new_agent();
    let req_body = serde_json::json!({
        "grant_type": "refresh_token",
        "refresh_token": rtok,
        "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
    })
    .to_string();
    let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body) else {
        return Ok(());
    };
    let Some(new_acc) = rval
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Ok(());
    };
    if let Some(oauth) = cred
        .get_mut("claudeAiOauth")
        .and_then(serde_json::Value::as_object_mut)
    {
        oauth.insert(
            "accessToken".to_string(),
            serde_json::Value::String(new_acc),
        );
        if let Some(new_ref) = rval
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            oauth.insert(
                "refreshToken".to_string(),
                serde_json::Value::String(new_ref.to_string()),
            );
        }
        if let Some(exp_in) = rval.get("expires_in").and_then(serde_json::Value::as_i64) {
            oauth.insert(
                "expiresAt".to_string(),
                serde_json::Value::Number((now_ms + exp_in * 1_000).into()),
            );
        }
        if let Ok(updated) = serde_json::to_string(&cred) {
            if let Err(err) = reservation.persist_credentials(
                &[".credentials.json", ".claude/.credentials.json"],
                updated.as_bytes(),
            ) {
                let _ = std::fs::remove_file(&attempt_path);
                return Err(err);
            }
            let _ = std::fs::remove_file(profile_root.join(".anyagents-usage-backoff-until-ms"));
            if let Some(profile_name) = profile_root.file_name().and_then(|n| n.to_str())
                && let Err(err) = propagate_profile_files_to_bound_agents(
                    data,
                    "anthropic",
                    profile_name,
                    &[".credentials.json", ".claude/.credentials.json"],
                )
            {
                let _ = std::fs::remove_file(&attempt_path);
                return Err(err);
            }
        }
    }
    Ok(())
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
    if let Err(err) = refresh_anthropic_profile_oauth_if_due(data, profile_root) {
        eprintln!(
            "anthropic oauth refresh skipped for {}: {err}",
            profile_root.display()
        );
    }
    let Some(raw_src_bytes) = read_anthropic_profile_credentials(profile_root) else {
        return;
    };
    let now_ms = anyagents_core::now_ms();
    let Some(src_exp) = json_oauth_expires_at(&raw_src_bytes) else {
        return;
    };
    if src_exp <= now_ms {
        return;
    }
    let sanitized = seat_sanitized_anthropic_credentials(&raw_src_bytes, now_ms);
    let mut rewrote_any = false;
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let dst = target_config.join(rel);
        let needs_rewrite = match std::fs::read(&dst) {
            Ok(dst_bytes) => {
                if dst_bytes == sanitized {
                    false
                } else {
                    let dst_exp = json_oauth_expires_at(&dst_bytes).unwrap_or(0);
                    dst_bytes.is_empty()
                        || dst_exp <= now_ms.saturating_add(30 * 60 * 1_000)
                        || src_exp > dst_exp
                }
            }
            Err(_) => !dst.is_dir(),
        };
        if needs_rewrite && write_seat_shared_file(&dst, &sanitized, 0o660) {
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
    let mut all_agents = store.list_agents()?;
    if all_agents.len() > 32 {
        all_agents.retain(|a| a.state != "archived");
    }
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let static_rel_paths: &[&str] = match normalize_harness_name(&agent.harness) {
        "claude-code" | "claude-cloud" => &[".credentials.json", ".claude/.credentials.json"],
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
        if provider == "anthropic" {
            refresh_anthropic_profile_oauth_if_due(data, &primary_profile_root)?;
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
            if let Some(raw_src_bytes) = source {
                let now_ms = anyagents_core::now_ms();
                let src_bytes = if is_anthropic_cred {
                    seat_sanitized_anthropic_credentials(&raw_src_bytes, now_ms)
                } else {
                    raw_src_bytes
                };
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
                        // Do not regress a newer isolated seat token to an expired
                        // or older profile snapshot. A profile/account cutover must
                        // still replace it, and retained seat bytes stay sanitized.
                        if !profile_changed
                            && is_anthropic_cred
                            && json_oauth_expires_at(&dst_bytes).is_some_and(|dst_exp| {
                                dst_exp > now_ms
                                    && Some(dst_exp) > json_oauth_expires_at(&src_bytes)
                            })
                            && !matches!(
                                (json_oauth_identity(&src_bytes), json_oauth_identity(&dst_bytes)),
                                (Some(src_id), Some(dst_id)) if src_id != dst_id
                            )
                        {
                            let sanitized =
                                seat_sanitized_anthropic_credentials(&dst_bytes, now_ms);
                            if sanitized != dst_bytes {
                                write_required_file(&dst, &sanitized, 0o660)?;
                            }
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
            let raw_bytes = match std::fs::read(&src) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue, // Optional aliases may be absent.
                Err(err) => return Err(err.into()),
            };
            {
                let bytes = if matches!(*rel, ".credentials.json" | ".claude/.credentials.json") {
                    seat_sanitized_anthropic_credentials(&raw_bytes, anyagents_core::now_ms())
                } else {
                    raw_bytes
                };
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
