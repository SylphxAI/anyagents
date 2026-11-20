//! Profile usage rebalancing, forced credential refresh, exhausted-profile rotation, and healthy profile binding.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Error, Store, agent_config_dir};

use super::credentials::{
    OAUTH_REFRESH_ATTEMPT_FILE, OAUTH_REFRESH_RESERVATION_FILE, OAuthRefreshDiskReservation,
    anthropic_oauth_usage_url, persist_refreshed_credentials, post_anthropic_oauth_refresh,
};
use super::quota::sync_anthropic_profile_metadata;
use super::{
    ProviderProfileEntry, bound_profile_name, bound_profile_of_provider,
    clear_stale_agy_quota_error_steps, compatible_harnesses_for, configure_agent_runtime,
    list_harness_profiles, parse_remaining_pct_from_usage, poke_agent_wake_socket,
    probe_profile_remaining_pct, propagate_profile_files_to_bound_agents,
    sync_fresher_bound_agent_credentials, upstream_provider_of, write_profile_marker,
};

fn parse_effective_rebalance_pct(usage: &str) -> Option<i64> {
    let min_pct = parse_remaining_pct_from_usage(usage, None)?;
    if min_pct <= 0 {
        return Some(0);
    }
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    if let (Some(p5), Some(p7)) = (extract_after("5h:"), extract_after("7d:"))
        && p5 >= 20
        && p7 >= 10
    {
        return Some((p5 + p7) / 2);
    }
    Some(min_pct)
}

pub(super) fn rebalance_provider_profiles(
    store: &Store,
    data: &Path,
    agents: &[Agent],
    entries: &[ProviderProfileEntry],
) {
    let mut providers: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for e in entries {
        if e.kind == "subscription" {
            providers.insert(e.provider.clone());
        }
    }
    for provider in providers {
        let mut prof_rem: std::collections::BTreeMap<String, i64> =
            std::collections::BTreeMap::new();
        for e in entries.iter().filter(|e| e.provider == provider) {
            if let Some(ref u) = e.usage {
                if provider == "google-antigravity" && !u.contains("(ultra)") {
                    continue;
                }
                if let Some(pct) = parse_effective_rebalance_pct(u) {
                    prof_rem.insert(e.profile.clone(), pct);
                }
            }
        }
        if prof_rem.len() < 2 {
            continue;
        }
        let mut bound: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for p in prof_rem.keys() {
            bound.insert(p.clone(), Vec::new());
        }
        let mut active_agents: Vec<&Agent> =
            agents.iter().filter(|a| a.state != "archived").collect();
        active_agents.sort_by(|a, b| a.name.cmp(&b.name));
        for a in active_agents {
            let exp_prov = upstream_provider_of(&a.harness, a.model.as_deref());
            if exp_prov != provider {
                continue;
            }
            let cfg = agent_config_dir(data, &a.id);
            let Some(prof_name) = bound_profile_of_provider(&cfg, &provider) else {
                continue;
            };
            bound.entry(prof_name).or_default().push(a.name.clone());
        }
        // Working seats still contribute load, but automatic maintenance must
        // not rewrite their credentials or request a process rotation mid-turn.
        // Read the durable turn state afresh rather than trusting the census.
        // An agent still being created is never moved: its tree may be gone.
        let can_move = |name: &str| {
            store.agent_by_name(name).is_ok_and(|agent| {
                agent.state != "created" && store.has_open_turn(&agent.id).is_ok_and(|open| !open)
            })
        };
        // Step 1: evacuate exhausted profiles (0% left) onto any profile with remaining quota (> 0% left).
        let prof_keys: Vec<String> = bound.keys().cloned().collect();
        for src_prof in &prof_keys {
            let Some(src_pct) = prof_rem.get(src_prof).copied() else {
                continue;
            };
            if src_pct > 0 {
                continue;
            }
            let to_move = bound.get(src_prof).cloned().unwrap_or_default();
            for agent_name in to_move {
                if !can_move(&agent_name) {
                    continue;
                }
                let mut best_dst: Option<(String, i64, i64)> = None;
                for (cand_prof, &cand_pct) in &prof_rem {
                    if cand_prof == src_prof || cand_pct <= 0 {
                        continue;
                    }
                    let cnt = bound.get(cand_prof).map(|v| v.len() as i64).unwrap_or(0);
                    let score = (cand_pct * 100) / (cnt + 1);
                    if best_dst
                        .as_ref()
                        .is_none_or(|(_, bs, br)| (score, cand_pct) > (*bs, *br))
                    {
                        best_dst = Some((cand_prof.clone(), score, cand_pct));
                    }
                }
                if let Some((dst_prof, _, _)) = best_dst {
                    let target = format!("{provider}/{dst_prof}");
                    if configure_agent_runtime(
                        store,
                        data,
                        "system:profile-rebalance",
                        &agent_name,
                        None,
                        None,
                        Some(&target),
                    )
                    .is_ok()
                    {
                        if let Some(v) = bound.get_mut(src_prof) {
                            v.retain(|n| n != &agent_name);
                        }
                        bound.entry(dst_prof).or_default().push(agent_name);
                    }
                }
            }
        }
        // Step 2: balance overloaded profiles onto healthy profiles so agent counts stay even
        // (differing by at most 1 across active profiles with healthy quota) and higher-quota
        // profiles carry the remainder.
        for _ in 0..16 {
            let mut best_move: Option<(String, String, i64)> = None;
            for (src_prof, src_agents) in &bound {
                let src_len = src_agents.len() as i64;
                if src_len < 2 || !src_agents.iter().any(|name| can_move(name)) {
                    continue;
                }
                let Some(src_pct) = prof_rem.get(src_prof).copied() else {
                    continue;
                };
                let src_share = (src_pct * 100) / src_len;
                for (dst_prof, &dst_pct) in &prof_rem {
                    if dst_prof == src_prof || dst_pct < 25 {
                        continue;
                    }
                    let dst_len = bound.get(dst_prof).map(|v| v.len() as i64).unwrap_or(0);
                    let new_dst_share = (dst_pct * 100) / (dst_len + 1);
                    let share_gain = new_dst_share - src_share;
                    let count_gap = src_len - dst_len;
                    let should_move = if count_gap >= 2 {
                        true
                    } else if count_gap == 1 {
                        dst_pct > src_pct + 15
                    } else {
                        share_gain >= 2500
                    };
                    if should_move {
                        let priority = count_gap * 10_000 + share_gain;
                        if best_move
                            .as_ref()
                            .is_none_or(|(_, _, best_pri)| priority > *best_pri)
                        {
                            best_move = Some((src_prof.clone(), dst_prof.clone(), priority));
                        }
                    }
                }
            }
            let Some((src_prof, dst_prof, _)) = best_move else {
                break;
            };
            let Some(agent_name) = bound
                .get(&src_prof)
                .and_then(|agents| agents.iter().rev().find(|name| can_move(name)).cloned())
            else {
                continue;
            };
            let target = format!("{provider}/{dst_prof}");
            if configure_agent_runtime(
                store,
                data,
                "system:profile-rebalance",
                &agent_name,
                None,
                None,
                Some(&target),
            )
            .is_ok()
            {
                if let Some(v) = bound.get_mut(&src_prof) {
                    v.retain(|n| n != &agent_name);
                }
                bound.entry(dst_prof).or_default().push(agent_name);
            } else {
                break;
            }
        }
    }
}

pub(crate) fn force_refresh_provider_profiles(
    data: &Path,
    target: Option<&str>,
) -> Result<(), Error> {
    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("refresh") && !s.eq_ignore_ascii_case("reset")
    });
    let _ = std::fs::remove_file(data.join("profiles/.last-agent-cred-sync-ms"));
    sync_fresher_bound_agent_credentials(data);
    let now_ms = anyagents_core::now_ms();
    if let Ok(store) = Store::open(data.join("state.db"))
        && let Ok(agents) = store.list_agents()
    {
        for agent in agents {
            if agent.state == "archived" {
                continue;
            }
            let cfg = agent_config_dir(data, &agent.id);
            let _ = std::fs::remove_file(cfg.join(".anyagents-last-quota-check-ms"));
        }
    }
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(4)))
        .build()
        .new_agent();
    let profiles_root = data.join("profiles");
    let Ok(providers) = std::fs::read_dir(&profiles_root) else {
        return Ok(());
    };
    for prov_entry in providers.flatten() {
        let prov_path = prov_entry.path();
        if !prov_path.is_dir() {
            continue;
        }
        let provider = prov_entry.file_name().to_string_lossy().into_owned();
        let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
            continue;
        };
        for prof_entry in prof_dirs.flatten() {
            let prof_dir = prof_entry.path();
            if !prof_dir.is_dir() {
                continue;
            }
            let profile = prof_entry.file_name().to_string_lossy().into_owned();
            if profile.starts_with('.') {
                continue;
            }
            let full_name = format!("{provider}/{profile}");
            if let Some(f) = filter
                && f != provider
                && f != profile
                && f != full_name
            {
                continue;
            }
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-profile-meta-sync-ms"));
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-last-usage.txt"));
            let _ = std::fs::remove_file(prof_dir.join(OAUTH_REFRESH_ATTEMPT_FILE));
            let _ = std::fs::remove_file(prof_dir.join(OAUTH_REFRESH_RESERVATION_FILE));
            match provider.as_str() {
                "anthropic" => {
                    let Ok(cred_text) = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                        .or_else(|_| {
                            std::fs::read_to_string(prof_dir.join(".claude/.credentials.json"))
                        })
                    else {
                        continue;
                    };
                    let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text) else {
                        continue;
                    };
                    let mut tok = cred
                        .pointer("/claudeAiOauth/accessToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let rtok = cred
                        .pointer("/claudeAiOauth/refreshToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let expires_at = cred
                        .pointer("/claudeAiOauth/expiresAt")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                    let usage_url = anthropic_oauth_usage_url();
                    let tok_valid = !tok.is_empty()
                        && expires_at > now_ms + 300_000
                        && http
                            .get(&usage_url)
                            .header("authorization", &format!("Bearer {tok}"))
                            .header("anthropic-beta", "oauth-2025-04-20")
                            .header("cache-control", "no-cache")
                            .header("user-agent", "claude-code/2.1.310")
                            .call()
                            .is_ok();
                    let last_attempt_ms =
                        std::fs::read_to_string(prof_dir.join(OAUTH_REFRESH_ATTEMPT_FILE))
                            .ok()
                            .and_then(|s| s.trim().parse::<i64>().ok())
                            .unwrap_or(0);
                    let mut refreshed_token = false;
                    if !tok_valid
                        && !rtok.is_empty()
                        && now_ms.saturating_sub(last_attempt_ms) >= 300_000
                    {
                        let reservation = OAuthRefreshDiskReservation::acquire(
                            &prof_dir,
                            &[".credentials.json", ".claude/.credentials.json"],
                            cred_text.len(),
                            now_ms,
                        )?;
                        let req_body = serde_json::json!({
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                            "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                        })
                        .to_string();
                        if let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                        {
                            tok = new_acc.clone();
                            refreshed_token = true;
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
                                {
                                    oauth.insert(
                                        "refreshToken".to_string(),
                                        serde_json::Value::String(new_ref.to_string()),
                                    );
                                }
                                if let Some(exp_in) =
                                    rval.get("expires_in").and_then(serde_json::Value::as_i64)
                                {
                                    oauth.insert(
                                        "expiresAt".to_string(),
                                        serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                                    );
                                }
                            }
                            let updated = serde_json::to_string(&cred)
                                .map_err(|e| Error::Message(e.to_string()))?;
                            reservation.persist_credentials(
                                &[".credentials.json", ".claude/.credentials.json"],
                                updated.as_bytes(),
                            )?;
                        }
                    }
                    if !tok.is_empty() {
                        let _ = sync_anthropic_profile_metadata(
                            &http, data, &prof_dir, &profile, &tok, &mut cred, true,
                        );
                        if refreshed_token && let Ok(updated) = serde_json::to_string(&cred) {
                            persist_refreshed_credentials(
                                &prof_dir,
                                &[".credentials.json", ".claude/.credentials.json"],
                                updated.as_bytes(),
                            )?;
                            propagate_profile_files_to_bound_agents(
                                data,
                                "anthropic",
                                &profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            )?;
                        }
                    }
                }
                "openai" => {
                    let Ok(auth_text) = std::fs::read_to_string(prof_dir.join("auth.json"))
                        .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                    else {
                        continue;
                    };
                    let Ok(mut auth) = serde_json::from_str::<serde_json::Value>(&auth_text) else {
                        continue;
                    };
                    let rtok = auth
                        .pointer("/tokens/refresh_token")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !rtok.is_empty() {
                        let reservation = OAuthRefreshDiskReservation::acquire(
                            &prof_dir,
                            &["auth.json", ".codex/auth.json"],
                            auth_text.len(),
                            now_ms,
                        )?;
                        let req_body = serde_json::json!({
                            "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                        })
                        .to_string();
                        if let Ok(rresp) = http
                            .post("https://auth.openai.com/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                            && let Some(tokens_obj) = auth
                                .get_mut("tokens")
                                .and_then(serde_json::Value::as_object_mut)
                        {
                            tokens_obj.insert(
                                "access_token".to_string(),
                                serde_json::Value::String(new_acc),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                tokens_obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                                reservation.persist_credentials(
                                    &["auth.json", ".codex/auth.json"],
                                    updated.as_bytes(),
                                )?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    "openai",
                                    &profile,
                                    &["auth.json", ".codex/auth.json"],
                                )?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Checks if `agent`'s currently bound profile has recovered quota (`> 5% left`
/// after a plan upgrade or vendor mid-window reset) OR is exhausted (`0% left`)
/// while another profile under the same upstream provider has healthy quota (`>= 20%`).
/// Rate-limited by `.anyagents-last-quota-check-ms` (120s cooldown) when called
/// during status reconciliation so background checks never spam vendor APIs.
pub(crate) fn try_rotate_exhausted_profile(
    store: &Store,
    data: &Path,
    agent: &Agent,
    has_active_quota_hold: bool,
) -> Option<String> {
    try_rotate_exhausted_profile_in(store, data, agent, has_active_quota_hold, false)
}

pub(super) fn try_rotate_exhausted_profile_in(
    store: &Store,
    data: &Path,
    agent: &Agent,
    has_active_quota_hold: bool,
    allow_in_test_env: bool,
) -> Option<String> {
    if !allow_in_test_env && crate::sandbox::is_isolated_test_env() {
        return None;
    }
    let cfg = agent_config_dir(data, &agent.id);
    let now_ms = anyagents_core::now_ms();
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let current_prof = bound_profile_of_provider(&cfg, &provider)?;
    let current_provider = bound_profile_name(&cfg)
        .and_then(|bound| bound.split_once('/').map(|(p, _)| p.to_string()))
        .filter(|bp| bp == &provider || compatible_harnesses_for(&provider).contains(&bp.as_str()))
        .or_else(|| {
            if data
                .join("profiles")
                .join(&provider)
                .join(&current_prof)
                .is_dir()
            {
                Some(provider.clone())
            } else {
                compatible_harnesses_for(&provider)
                    .iter()
                    .find(|compat| {
                        data.join("profiles")
                            .join(compat)
                            .join(&current_prof)
                            .is_dir()
                    })
                    .map(|compat| (*compat).to_string())
            }
        })
        .unwrap_or_else(|| provider.clone());
    let prof_dir = data
        .join("profiles")
        .join(&current_provider)
        .join(&current_prof);
    let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        .max(rotated_ms);
    if has_active_quota_hold {
        let last_check = std::fs::read_to_string(cfg.join(".anyagents-last-quota-check-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if now_ms.saturating_sub(last_check) < 120_000 {
            if cleared_ms >= last_check && now_ms.saturating_sub(cleared_ms) < 120_000 {
                return Some(format!("{current_provider}/{current_prof}"));
            }
            return None;
        }
        write_profile_marker(
            &cfg,
            ".anyagents-last-quota-check-ms",
            &format!("{now_ms}\n"),
        )
        .map_err(|err| eprintln!("runtime persistence failed: {err}"))
        .ok()?;
    }
    let post_clear_error = std::fs::read_to_string(cfg.join(".anyagents-last-error.json"))
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .is_some_and(|val| {
            let ts = val
                .get("timestamp_ms")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let q = anyagents_harness::quota::recorded_reset_at_ms(&val).unwrap_or(0);
            cleared_ms > 0 && ts > cleared_ms && q > now_ms && now_ms.saturating_sub(ts) < 900_000
        });
    let cur_pct = if post_clear_error {
        Some(0)
    } else {
        probe_profile_remaining_pct(
            data,
            &current_provider,
            &current_prof,
            agent.model.as_deref(),
        )
    };
    if has_active_quota_hold && cur_pct.is_some_and(|p| p > 0) {
        write_profile_marker(&cfg, ".anyagents-quota-cleared-ms", &format!("{now_ms}\n"))
            .map_err(|err| eprintln!("runtime persistence failed: {err}"))
            .ok()?;
        let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
        if agent.harness == "agy" {
            clear_stale_agy_quota_error_steps(&cfg);
        }
        super::persist_spawned_agent_config_to_store(store, &agent.id, &cfg);
        poke_agent_wake_socket(data, &agent.id);
        return Some(format!("{current_provider}/{current_prof}"));
    }
    if has_active_quota_hold && cur_pct.is_some_and(|pct| pct <= 0) {
        let bound_counts = count_bound_profile_agents(
            data,
            &store.list_live_agents().unwrap_or_default(),
            &provider,
            &agent.id,
        );
        let mut best: Option<(String, String, i64, i64)> = None;
        for (cand_prof, cand_path) in list_harness_profiles(data, &provider) {
            let cand_provider = cand_path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(&provider)
                .to_string();
            if cand_prof == current_prof && cand_provider == current_provider {
                continue;
            }
            if let Some(rem_pct) = probe_profile_remaining_pct(
                data,
                &cand_provider,
                &cand_prof,
                agent.model.as_deref(),
            ) && rem_pct > 0
            {
                let cnt = bound_counts.get(&cand_prof).copied().unwrap_or(0);
                let score = (rem_pct * 100) / (cnt + 1);
                if best.as_ref().is_none_or(|(_, _, best_score, best_rem)| {
                    (score, rem_pct) > (*best_score, *best_rem)
                }) {
                    best = Some((cand_provider, cand_prof, score, rem_pct));
                }
            }
        }
        if let Some((best_provider, best_prof, _, _)) = best {
            let target_prof = format!("{best_provider}/{best_prof}");
            if configure_agent_runtime(
                store,
                data,
                "system:quota-rotate",
                &agent.name,
                None,
                None,
                Some(&target_prof),
            )
            .is_ok()
            {
                return Some(target_prof);
            }
        }
    }
    None
}

pub(super) fn count_bound_profile_agents(
    data: &Path,
    agents: &[Agent],
    provider: &str,
    excluded_agent: &AgentId,
) -> std::collections::HashMap<String, i64> {
    let mut counts = std::collections::HashMap::new();
    for agent in agents {
        if agent.state != "archived" && &agent.id != excluded_agent {
            let cfg = agent_config_dir(data, &agent.id);
            if let Some(bound) = bound_profile_name(&cfg) {
                let (bound_provider, profile) = bound
                    .split_once('/')
                    .map(|(p, n)| (p.to_string(), n.to_string()))
                    .unwrap_or_else(|| {
                        (
                            upstream_provider_of(&agent.harness, agent.model.as_deref()),
                            bound.clone(),
                        )
                    });
                if bound_provider == provider
                    || compatible_harnesses_for(provider).contains(&bound_provider.as_str())
                {
                    *counts.entry(profile).or_insert(0) += 1;
                }
            }
        }
    }
    counts
}

const PROFILE_HEALTHY_USAGE_FRESHNESS_MS: i64 = 65_000;

const PROFILE_HEALTHY_OAUTH_ATTEMPT_FRESHNESS_MS: i64 = 300_000;

/// Positive-signal health check for a candidate profile directory. Returns true only when
/// the profile directory itself carries conclusive unhealthy evidence: a quota reading
/// of zero percent left written within the last minute, or an expired OAuth token whose
/// most recent refresh attempt also happened recently (so the token cannot silently
/// recover on its own). Never probes the network and never treats a missing or stale
/// signal as unhealthy evidence.
fn google_oauth_expiry_ms(val: &serde_json::Value) -> Option<i64> {
    for ptr in ["/token/expiry", "/expiry"] {
        if let Some(raw) = val.pointer(ptr).and_then(serde_json::Value::as_str)
            && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw.trim())
        {
            return Some(dt.timestamp_millis());
        }
    }
    for ptr in [
        "/token/expires_at",
        "/expires_at",
        "/token/expiry_date",
        "/expiry_date",
    ] {
        if let Some(n) = val.pointer(ptr).and_then(serde_json::Value::as_i64) {
            return Some(if n > 0 && n < 10_000_000_000 {
                n.saturating_mul(1_000)
            } else {
                n
            });
        }
    }
    None
}

fn profile_has_unhealthy_evidence(dir: &Path) -> bool {
    let now_ms = anyagents_core::now_ms();
    let usage_path = dir.join(".anyagents-last-usage.txt");
    let usage_text_and_age = std::fs::read_to_string(&usage_path).ok().map(|usage| {
        let age_ms = std::fs::metadata(&usage_path)
            .and_then(|meta| meta.modified())
            .map(|mtime| {
                std::time::SystemTime::now()
                    .duration_since(mtime)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0)
            })
            .unwrap_or(i64::MAX);
        (usage, age_ms)
    });
    if let Some((ref usage, age_ms)) = usage_text_and_age {
        let trimmed = usage.trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("oauth expired") && age_ms < PROFILE_HEALTHY_OAUTH_ATTEMPT_FRESHNESS_MS
        {
            return true;
        }
        if parse_remaining_pct_from_usage(trimmed, None) == Some(0)
            && age_ms < PROFILE_HEALTHY_USAGE_FRESHNESS_MS
        {
            return true;
        }
    }
    let last_attempt_ms = std::fs::read_to_string(dir.join(OAUTH_REFRESH_ATTEMPT_FILE))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let recent_refresh_attempt = last_attempt_ms > 0
        && now_ms.saturating_sub(last_attempt_ms) < PROFILE_HEALTHY_OAUTH_ATTEMPT_FRESHNESS_MS;

    if let Ok(cred_text) = std::fs::read_to_string(dir.join(".credentials.json"))
        .or_else(|_| std::fs::read_to_string(dir.join(".claude/.credentials.json")))
        && let Ok(cred) = serde_json::from_str::<serde_json::Value>(&cred_text)
    {
        let expires_at = cred
            .pointer("/claudeAiOauth/expiresAt")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX);
        if expires_at <= now_ms && recent_refresh_attempt {
            return true;
        }
    }

    if let Ok(tok_text) =
        std::fs::read_to_string(dir.join(".gemini/antigravity-cli/antigravity-oauth-token"))
            .or_else(|_| std::fs::read_to_string(dir.join(".gemini/oauth_creds.json")))
        && let Ok(tok) = serde_json::from_str::<serde_json::Value>(&tok_text)
    {
        let expires_at = google_oauth_expiry_ms(&tok).unwrap_or(i64::MAX);
        let has_refresh_token = tok
            .pointer("/token/refresh_token")
            .or_else(|| tok.get("refresh_token"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.trim().is_empty());
        let usage_failed = usage_text_and_age.as_ref().is_none_or(|(u, _)| {
            u.trim() == "__NONE__" || u.trim().to_ascii_lowercase().starts_with("oauth expired")
        });
        if (expires_at <= now_ms && (!has_refresh_token || recent_refresh_attempt))
            || (recent_refresh_attempt && usage_failed)
        {
            return true;
        }
    }

    false
}

/// Selects (or reuses the already-bound) Upstream Provider Profile for `agent`
/// from `<data>/profiles/<provider>/<profile>` with zero automatic load-balancing
/// ("分流"): an agent uses its explicitly assigned `.anyagents-profile` (defaulting
/// deterministically to `<provider>/default`).
///
/// Initial bind is health-aware. When the implicit default profile carries positive
/// unhealthy evidence (see profile_has_unhealthy_evidence), the first healthy sibling
/// profile of the same provider wins instead. The default is kept whenever it is
/// healthy or has no healthy replacement, and an explicit harness binding is never
/// overridden by this routing.
pub(super) fn select_or_bind_profile(
    data: &Path,
    _all_agents: &[Agent],
    agent: &Agent,
    target_config: &Path,
) -> Vec<PathBuf> {
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let available = list_harness_profiles(data, &provider);
    if let Some(existing) = bound_profile_name(target_config) {
        if let Some((bound_prov, prof_name)) = existing.split_once('/') {
            let explicit_dir = data.join("profiles").join(bound_prov).join(prof_name);
            if explicit_dir.is_dir() {
                return vec![explicit_dir];
            }
        }
        let prof_name = existing
            .split_once('/')
            .map(|(_, p)| p)
            .unwrap_or(existing.as_str());
        if let Some((_, dir)) = available.iter().find(|(n, _)| n == prof_name) {
            return vec![dir.clone()];
        }
        let exact_dir = data.join("profiles").join(&provider).join(prof_name);
        if exact_dir.is_dir() {
            return vec![exact_dir];
        }
        for compat in compatible_harnesses_for(&provider) {
            let compat_dir = data.join("profiles").join(compat).join(prof_name);
            if compat_dir.is_dir() {
                return vec![compat_dir];
            }
        }
    }
    let default_entry = available.iter().find(|(n, _)| n == "default");
    let chosen_entry = match default_entry {
        Some((_, default_dir)) if profile_has_unhealthy_evidence(default_dir) => available
            .iter()
            .find(|(n, dir)| n != "default" && !profile_has_unhealthy_evidence(dir))
            .or(default_entry),
        _ => default_entry,
    }
    .or_else(|| available.first());
    if let Some((chosen_name, chosen_dir)) = chosen_entry {
        let chosen_prov = chosen_dir
            .parent()
            .and_then(Path::file_name)
            .and_then(|n| n.to_str())
            .unwrap_or(provider.as_str());
        write_profile_marker(
            target_config,
            ".anyagents-profile",
            &format!("{chosen_prov}/{chosen_name}\n"),
        )
        .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
        return vec![chosen_dir.clone()];
    }
    write_profile_marker(
        target_config,
        ".anyagents-profile",
        &format!("{provider}/default\n"),
    )
    .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
    vec![data.join("profiles").join(&provider).join("default")]
}
