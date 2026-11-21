//! Profile rebalancing, exhausted-profile rotation, and healthy profile binding.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Store, agent_config_dir};

use super::subscription_status::{is_passive_subscription, local_subscription_status};
use super::{
    ProviderProfileEntry, bound_profile_name, bound_profile_of_provider,
    clear_stale_agy_quota_error_steps, compatible_harnesses_for, configure_agent_runtime,
    list_harness_profiles, parse_remaining_pct_from_usage, poke_agent_wake_socket,
    probe_profile_remaining_pct, upstream_provider_of, write_profile_marker,
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

/// Move a held agent to a profile without observed exhaustion. Only a live
/// public API-key reading may clear a hold before its harness-reported reset.
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
    let auth_error = is_passive_subscription(&provider)
        && !has_active_quota_hold
        && crate::vendor::read_persisted_last_error(&cfg).is_some_and(|(error, _)| {
            crate::server::compute_block_reason(None, error.as_deref(), now_ms).as_deref()
                == Some("sign_in_required")
        });
    if !has_active_quota_hold {
        if !auth_error || store.has_open_turn(&agent.id).unwrap_or(true) {
            return None;
        }
        super::subscription_status::record_auth_rejection(&prof_dir, &current_provider);
    }
    let cur_pct = if auth_error || post_clear_error || is_passive_subscription(&provider) {
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
    if cur_pct.is_some_and(|pct| pct <= 0) {
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
            let mut rem_pct = probe_profile_remaining_pct(
                data,
                &cand_provider,
                &cand_prof,
                agent.model.as_deref(),
            );
            if rem_pct.is_none()
                && is_passive_subscription(&provider)
                && local_subscription_status(&cand_path, &provider, now_ms).usable
            {
                // Rank usable, unmeasured subscriptions by load, not an invented quota.
                rem_pct = Some(1);
            }
            if let Some(rem_pct) = rem_pct.filter(|&pct| pct > 0) {
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

fn profile_has_unhealthy_evidence(dir: &Path, provider: &str) -> bool {
    if is_passive_subscription(provider) {
        let now_ms = anyagents_core::now_ms();
        return local_subscription_status(dir, provider, now_ms).expired
            || super::quota::overlay_active_quota_reset_on_cached_usage(dir, None, "", now_ms)
                .starts_with(super::quota::USAGE_LIMITED_PREFIX);
    }
    let path = dir.join(".anyagents-last-usage.txt");
    path.metadata()
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| mtime.elapsed().ok())
        .is_some_and(|age| age.as_millis() < PROFILE_HEALTHY_USAGE_FRESHNESS_MS as u128)
        && std::fs::read_to_string(path)
            .ok()
            .and_then(|usage| parse_remaining_pct_from_usage(&usage, None))
            == Some(0)
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
        Some((_, default_dir)) if profile_has_unhealthy_evidence(default_dir, &provider) => {
            available
                .iter()
                .find(|(n, dir)| {
                    n != "default"
                        && !profile_has_unhealthy_evidence(dir, &provider)
                        && (!is_passive_subscription(&provider)
                            || local_subscription_status(dir, &provider, anyagents_core::now_ms())
                                .usable)
                })
                .or(default_entry)
        }
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
