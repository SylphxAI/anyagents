//! Quota reset tracking, reset ETA formatting, usage enrichment, Anthropic metadata sync, and model catalog probing.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, agent_config_dir};

use super::credentials::persist_refreshed_credentials;
use super::profile_identity::read_first_non_empty_file;
use super::{
    ProviderProfileEntry, QuotaWindow, bound_profile_of_provider, extract_profile_account_identity,
    probe_one_profile_usage, probe_one_profile_usage_with_options,
    propagate_profile_files_to_bound_agents, sync_fresher_bound_agent_credentials,
    update_profile_json_identity, upstream_provider_of, write_seat_shared_file,
};

/// Unknown, malformed and out-of-range observations carry no quota authority.
pub(super) fn observed_remaining_pct(value: &serde_json::Value) -> Option<i64> {
    let used = value.as_f64()?;
    (used.is_finite() && (0.0..=100.0).contains(&used)).then(|| (100.0 - used).round() as i64)
}

pub(super) fn observed_fraction_pct(value: &serde_json::Value) -> Option<i64> {
    let left = value.as_f64()?;
    (left.is_finite() && (0.0..=1.0).contains(&left)).then(|| (left * 100.0).round() as i64)
}

pub(super) fn observed_window_min(first: Option<i64>, second: Option<i64>) -> Option<i64> {
    Some(first?.min(second?))
}

pub(super) fn observed_agy_tier(value: &serde_json::Value) -> Option<String> {
    let id = value
        .pointer("/paidTier/id")
        .or_else(|| value.pointer("/currentTier/id"))?
        .as_str()?;
    if id.contains("ultra") {
        Some("ultra".into())
    } else if id.contains("pro") {
        Some("pro".into())
    } else if id.contains("free") {
        Some("free".into())
    } else {
        None
    }
}

pub(crate) fn record_profile_quota_reset(data: &Path, agent: &Agent, reset_at_ms: i64) {
    let now_ms = anyagents_core::now_ms();
    if reset_at_ms <= now_ms {
        return;
    }
    let cfg = agent_config_dir(data, &agent.id);
    let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        .max(rotated_ms);
    if cleared_ms > 0 && now_ms.saturating_sub(cleared_ms) < 45_000 {
        return;
    }
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let Some(prof_name) = bound_profile_of_provider(&cfg, &provider) else {
        return;
    };
    let profile_dir = data.join("profiles").join(&provider).join(&prof_name);
    if profile_dir.is_dir() {
        let marker = profile_dir.join(".anyagents-quota-reset-ms");
        let existing = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if existing != reset_at_ms {
            write_seat_shared_file(&marker, format!("{reset_at_ms}\n").as_bytes(), 0o600);
        }
    }
}

fn parse_reset_eta_secs(eta: &str) -> Option<i64> {
    anyagents_harness::quota::parse_duration_ms(eta.trim()).map(|ms| ms / 1_000)
}

fn advance_cached_usage_etas(cached: &str, elapsed_secs: i64) -> String {
    if elapsed_secs <= 0 || cached.is_empty() {
        return cached.to_string();
    }
    let (body, tier_suffix) = if cached.ends_with(')')
        && let Some(idx) = cached.rfind(" (")
    {
        (&cached[..idx], &cached[idx..])
    } else {
        (cached, "")
    };
    if body.is_empty() {
        return cached.to_string();
    }
    let mut updated_parts = Vec::new();
    for part in body.split(" · ") {
        if let Some((prefix, eta_str)) = part.split_once(" ↻")
            && let Some(orig_secs) = parse_reset_eta_secs(eta_str)
        {
            let rem_secs = orig_secs - elapsed_secs;
            if rem_secs > 0 {
                if let Some(new_eta) = format_reset_eta_secs(rem_secs) {
                    updated_parts.push(format!("{prefix} ↻{new_eta}"));
                } else {
                    updated_parts.push(prefix.to_string());
                }
            } else if let Some((win, rest)) = prefix.split_once(": ")
                && rest.ends_with("% left")
            {
                updated_parts.push(format!("{win}: 100% left"));
            } else {
                updated_parts.push(prefix.to_string());
            }
        } else {
            updated_parts.push(part.to_string());
        }
    }
    format!("{}{tier_suffix}", updated_parts.join(" · "))
}

pub(super) fn overlay_active_quota_reset_on_cached_usage(
    prof_dir: &Path,
    last_usage_path: &Path,
    cached: &str,
    now_ms: i64,
) -> String {
    let usage_mtime_ms = std::fs::metadata(last_usage_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let elapsed_secs = if usage_mtime_ms > 0 {
        now_ms.saturating_sub(usage_mtime_ms) / 1_000
    } else {
        0
    };
    let cached_advanced = advance_cached_usage_etas(cached, elapsed_secs);
    let reset_marker = prof_dir.join(".anyagents-quota-reset-ms");
    let raw_reset_ms = std::fs::read_to_string(&reset_marker)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok());
    if raw_reset_ms.is_some_and(|ms| ms <= now_ms) {
        let _ = std::fs::remove_file(&reset_marker);
    }
    let Some(reset_ms) = raw_reset_ms.filter(|&ms| ms > now_ms) else {
        return cached_advanced;
    };
    let reset_mtime_ms = std::fs::metadata(&reset_marker)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(now_ms);
    if usage_mtime_ms > reset_mtime_ms && now_ms.saturating_sub(usage_mtime_ms) < 60_000 {
        return cached_advanced;
    }
    let eta = format_reset_eta_epoch_ms(reset_ms)
        .map(|e| format!(" ↻{e}"))
        .unwrap_or_default();
    // A reading with a 5h window shows the hit as that window run out. A plan
    // without one (Ultra has only a weekly window) gets no invented 5h window:
    // the hit is stated as an active limit beside the windows it does report.
    if cached_advanced.starts_with("5h:") {
        let zero_5h = format!("5h: 0% left{eta}");
        if let Some((_, rest)) = cached_advanced.split_once(" · ") {
            return format!("{zero_5h} · {rest}");
        }
        let tier_suffix = cached_advanced
            .rfind(" (")
            .map(|idx| &cached_advanced[idx..])
            .unwrap_or_default();
        return format!("{zero_5h}{tier_suffix}");
    }
    let limited = format!("{USAGE_LIMITED_PREFIX}{eta}");
    if cached_advanced.is_empty() {
        limited
    } else if cached_advanced.starts_with('(') {
        format!("{limited} {cached_advanced}")
    } else {
        format!("{limited} · {cached_advanced}")
    }
}

/// Leads a usage reading whose profile is rate limited outside any window the
/// reading reports; such a reading has no capacity left until the limit lifts.
pub(crate) const USAGE_LIMITED_PREFIX: &str = "limited";

/// Parse structured `(quota_5h, quota_weekly)` windows from any provider's
/// canonical usage summary string (e.g. `"5h: 72% left ↻2h15m · 7d: 45% left ↻3d04h (pro)"`).
pub(crate) fn parse_quota_windows_from_usage(
    usage: &str,
) -> (Option<QuotaWindow>, Option<QuotaWindow>) {
    let trimmed = usage.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    let body = if trimmed.ends_with(')')
        && let Some(idx) = trimmed.rfind(" (")
    {
        &trimmed[..idx]
    } else {
        trimmed
    };
    let mut quota_5h = None;
    let mut quota_weekly = None;
    for part in body.split(" · ") {
        let Some((label_raw, rest_raw)) = part.split_once(':') else {
            continue;
        };
        let label = label_raw.trim().to_ascii_lowercase();
        let rest = rest_raw.trim();
        let Some((pct_raw, after_pct)) = rest.split_once("% left") else {
            continue;
        };
        let Ok(pct) = pct_raw.trim().parse::<i64>() else {
            continue;
        };
        let reset_eta = after_pct
            .trim()
            .strip_prefix('↻')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let window = QuotaWindow {
            remaining_pct: pct.clamp(0, 100),
            reset_eta,
        };
        match label.as_str() {
            "5h" => quota_5h = Some(window),
            "7d" | "weekly" => quota_weekly = Some(window),
            _ => {}
        }
    }
    (quota_5h, quota_weekly)
}

/// Format an OpenAI/Codex `/backend-api/wham/usage` payload into a unified
/// `5h` + `7d` usage summary string, capturing BOTH `primary_window` and
/// `secondary_window` when reported.
pub(crate) fn format_openai_usage_summary(val: &serde_json::Value) -> Option<String> {
    let parse_window = |ptr: &str| -> Option<(Option<i64>, i64, Option<String>)> {
        let win = val.pointer(ptr)?;
        let used = win
            .get("used_percent")
            .and_then(serde_json::Value::as_f64)?;
        if !used.is_finite() {
            return None;
        }
        let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
        let window_secs = win
            .get("limit_window_seconds")
            .and_then(serde_json::Value::as_i64);
        let eta = (rem < 100)
            .then(|| {
                win.get("reset_after_seconds")
                    .and_then(serde_json::Value::as_i64)
                    .and_then(format_reset_eta_secs)
            })
            .flatten();
        Some((window_secs, rem, eta))
    };

    let primary = parse_window("/rate_limit/primary_window");
    let secondary = parse_window("/rate_limit/secondary_window");
    if primary.is_none() && secondary.is_none() {
        return None;
    }

    let mut win_5h: Option<(i64, Option<String>)> = None;
    let mut win_7d: Option<(i64, Option<String>)> = None;

    match (primary, secondary) {
        (Some((p_secs, p_rem, p_eta)), Some((s_secs, s_rem, s_eta))) => {
            for (secs, rem, eta, is_primary) in
                [(p_secs, p_rem, p_eta, true), (s_secs, s_rem, s_eta, false)]
            {
                let is_short = secs.map_or(is_primary, |s| s <= 86_400);
                if is_short {
                    win_5h = Some((rem, eta));
                } else {
                    win_7d = Some((rem, eta));
                }
            }
        }
        (Some((p_secs, p_rem, p_eta)), None) => {
            if p_secs.is_some_and(|s| s <= 86_400) {
                win_5h = Some((p_rem, p_eta));
            } else {
                win_7d = Some((p_rem, p_eta));
            }
        }
        (None, Some((s_secs, s_rem, s_eta))) => {
            if s_secs.is_some_and(|s| s <= 86_400) {
                win_5h = Some((s_rem, s_eta));
            } else {
                win_7d = Some((s_rem, s_eta));
            }
        }
        (None, None) => return None,
    }

    let mut parts = Vec::new();
    if let Some((rem, eta)) = win_5h {
        let eta_str = eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
        parts.push(format!("5h: {rem}% left{eta_str}"));
    }
    if let Some((rem, eta)) = win_7d {
        let eta_str = eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
        parts.push(format!("7d: {rem}% left{eta_str}"));
    }
    if parts.is_empty() {
        return None;
    }
    let joined = parts.join(" · ");
    match val.get("plan_type").and_then(serde_json::Value::as_str) {
        Some(plan) if !plan.is_empty() => Some(format!("{joined} ({plan})")),
        _ => Some(joined),
    }
}

/// Syncs any fresher OAuth credentials from active seats back into
/// `<data>/profiles/<provider>/<profile>` and probes native upstream usage,
/// quota, or balance for each profile in parallel. When `bypass_cache` is
/// `true` (`GET /v1/profiles?fresh=1`), stale on-disk usage caches are bypassed.
pub(crate) fn enrich_profiles_usage_with_options(
    data: &Path,
    entries: &mut [ProviderProfileEntry],
    bypass_cache: bool,
) {
    if entries.is_empty() {
        return;
    }
    if crate::sandbox::is_isolated_test_env() {
        for entry in entries.iter_mut() {
            if let Some(ref usage) = entry.usage {
                let (q5, q7) = parse_quota_windows_from_usage(usage);
                if entry.quota_5h.is_none() {
                    entry.quota_5h = q5;
                }
                if entry.quota_weekly.is_none() {
                    entry.quota_weekly = q7;
                }
            }
        }
        return;
    }
    sync_fresher_bound_agent_credentials(data);

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for (idx, entry) in entries.iter().enumerate() {
            if entry.usage.is_some() && !bypass_cache {
                continue;
            }
            let provider = entry.provider.clone();
            let profile = entry.profile.clone();
            handles.push(s.spawn(move || {
                (
                    idx,
                    probe_one_profile_usage_with_options(data, &provider, &profile, bypass_cache),
                )
            }));
        }
        for handle in handles {
            if let Ok((idx, Some(usage))) = handle.join()
                && let Some(entry) = entries.get_mut(idx)
            {
                let (q5, q7) = parse_quota_windows_from_usage(&usage);
                entry.usage = Some(usage);
                entry.quota_5h = q5;
                entry.quota_weekly = q7;
            }
        }
    });
    for entry in entries.iter_mut() {
        if let Some(ref usage) = entry.usage
            && (entry.quota_5h.is_none() || entry.quota_weekly.is_none())
        {
            let (q5, q7) = parse_quota_windows_from_usage(usage);
            if entry.quota_5h.is_none() {
                entry.quota_5h = q5;
            }
            if entry.quota_weekly.is_none() {
                entry.quota_weekly = q7;
            }
        }
        if entry.account.is_none() {
            let prof_dir = data
                .join("profiles")
                .join(&entry.provider)
                .join(&entry.profile);
            let (em, id) = extract_profile_account_identity(&prof_dir, &entry.provider);
            entry.account = em.or(id);
        }
    }
}

/// The plan badge of an Anthropic subscription. Ultra is named by either its
/// organization type (`claude_ultra`) or its rate-limit tier
/// (`default_claude_ultra*`), and wins over a Max tier an upgrade left behind.
pub(super) fn anthropic_tier_label(
    sub_type: Option<&str>,
    rate_tier: Option<&str>,
) -> Option<&'static str> {
    let sub_type = sub_type.unwrap_or("");
    let rate_tier = rate_tier.unwrap_or("");
    if sub_type.contains("ultra") || rate_tier.contains("ultra") {
        return Some("ultra");
    }
    if rate_tier.contains("max_20x") {
        return Some("max 20x");
    }
    if rate_tier.contains("max_5x") {
        return Some("max 5x");
    }
    match sub_type {
        "max" | "claude_max" => Some("max"),
        "pro" | "claude_pro" => Some("pro"),
        "team" | "claude_team" => Some("team"),
        "enterprise" | "claude_enterprise" => Some("enterprise"),
        _ => None,
    }
}

pub(super) fn sync_anthropic_profile_metadata(
    http: &ureq::Agent,
    data: &Path,
    prof_dir: &Path,
    profile: &str,
    token: &str,
    cred: &mut serde_json::Value,
    force: bool,
) -> Option<&'static str> {
    let existing_sub = cred
        .pointer("/claudeAiOauth/subscriptionType")
        .and_then(serde_json::Value::as_str);
    let existing_tier = cred
        .pointer("/claudeAiOauth/rateLimitTier")
        .and_then(serde_json::Value::as_str);
    let existing_email = cred
        .pointer("/claudeAiOauth/email")
        .and_then(serde_json::Value::as_str);
    let now_ms = anyagents_core::now_ms();
    let meta_sync_path = prof_dir.join(".anyagents-profile-meta-sync-ms");
    let last_sync_ms = std::fs::read_to_string(&meta_sync_path)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let mut changed = false;
    if (force
        || existing_sub.is_none()
        || existing_tier.is_none()
        || existing_email.is_none()
        || now_ms.saturating_sub(last_sync_ms) >= 300_000)
        && let Ok(resp) = http
            .get("https://api.anthropic.com/api/oauth/profile")
            .header("authorization", &format!("Bearer {token}"))
            .header("anthropic-beta", "oauth-2025-04-20")
            .header("cache-control", "no-cache")
            .header("user-agent", "claude-code/2.1.310")
            .call()
        && let Ok(body) = resp.into_body().read_to_string()
        && let Ok(pval) = serde_json::from_str::<serde_json::Value>(&body)
    {
        write_seat_shared_file(&meta_sync_path, format!("{now_ms}\n").as_bytes(), 0o600);
        let org_type = pval
            .pointer("/organization/organization_type")
            .and_then(serde_json::Value::as_str);
        let mapped_sub = match org_type {
            Some(t) if t.contains("ultra") => Some("ultra"),
            Some("claude_max") => Some("max"),
            Some("claude_pro") => Some("pro"),
            Some("claude_team") => Some("team"),
            Some("claude_enterprise") => Some("enterprise"),
            _ => None,
        };
        let live_tier = pval
            .pointer("/organization/rate_limit_tier")
            .and_then(serde_json::Value::as_str);
        let acct_uuid = pval
            .pointer("/account/uuid")
            .and_then(serde_json::Value::as_str);
        let acct_email = pval
            .pointer("/account/email")
            .and_then(serde_json::Value::as_str);
        if !profile.starts_with('.') {
            update_profile_json_identity(prof_dir, "anthropic", acct_email, acct_uuid);
        }
        if let Some(oauth) = cred
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(em) = acct_email
                && oauth.get("email").and_then(serde_json::Value::as_str) != Some(em)
            {
                oauth.insert(
                    "email".to_string(),
                    serde_json::Value::String(em.to_string()),
                );
                changed = true;
            }
            if let Some(au) = acct_uuid
                && oauth.get("accountUuid").and_then(serde_json::Value::as_str) != Some(au)
            {
                oauth.insert(
                    "accountUuid".to_string(),
                    serde_json::Value::String(au.to_string()),
                );
                changed = true;
            }
            if let Some(ms) = mapped_sub
                && oauth
                    .get("subscriptionType")
                    .and_then(serde_json::Value::as_str)
                    != Some(ms)
            {
                oauth.insert(
                    "subscriptionType".to_string(),
                    serde_json::Value::String(ms.to_string()),
                );
                oauth.insert(
                    "subscriptionTypeSource".to_string(),
                    serde_json::Value::String("profile".to_string()),
                );
                changed = true;
            }
            if let Some(lt) = live_tier
                && oauth
                    .get("rateLimitTier")
                    .and_then(serde_json::Value::as_str)
                    != Some(lt)
            {
                oauth.insert(
                    "rateLimitTier".to_string(),
                    serde_json::Value::String(lt.to_string()),
                );
                changed = true;
            }
        }
    }
    if (changed || !prof_dir.join(".claude/.credentials.json").is_file())
        && let Ok(updated) = serde_json::to_string(cred)
    {
        persist_refreshed_credentials(
            prof_dir,
            &[".credentials.json", ".claude/.credentials.json"],
            updated.as_bytes(),
        )
        .map_err(|err| {
            eprintln!(
                "cannot persist anthropic profile metadata ({}): {err}",
                prof_dir.display()
            )
        })
        .ok()?;
        if !profile.starts_with('.') {
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            )
            .map_err(|err| eprintln!("runtime persistence failed: {err}"))
            .ok()?;
        }
    }
    let sub_type = cred
        .pointer("/claudeAiOauth/subscriptionType")
        .and_then(serde_json::Value::as_str);
    let rate_tier = cred
        .pointer("/claudeAiOauth/rateLimitTier")
        .and_then(serde_json::Value::as_str);
    anthropic_tier_label(sub_type, rate_tier)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m_adj = if m > 2 {
        (m - 3) as u64
    } else {
        (m + 9) as u64
    };
    let doy = (153 * m_adj + 2) / 5 + (d as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + (doe as i64) - 719_468
}

fn parse_iso_utc_epoch_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 19 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let min: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3_600 + min * 60 + sec)
}

pub(super) fn format_reset_eta_secs(rem_secs: i64) -> Option<String> {
    if rem_secs <= 0 {
        return None;
    }
    if rem_secs < 3_600 {
        let m = ((rem_secs + 59) / 60).max(1);
        return Some(format!("{m}m"));
    }
    if rem_secs < 86_400 {
        let h = rem_secs / 3_600;
        let m = (rem_secs % 3_600) / 60;
        return if m == 0 {
            Some(format!("{h}h"))
        } else {
            Some(format!("{h}h{m:02}m"))
        };
    }
    let d = rem_secs / 86_400;
    let h = (rem_secs % 86_400) / 3_600;
    if h == 0 {
        Some(format!("{d}d"))
    } else {
        Some(format!("{d}d{h:02}h"))
    }
}

pub(super) fn format_reset_eta_iso(iso_str: &str) -> Option<String> {
    let target_secs = parse_iso_utc_epoch_secs(iso_str)?;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    format_reset_eta_secs(target_secs - now_secs)
}

pub(super) fn format_reset_eta_epoch_ms(target_ms: i64) -> Option<String> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    format_reset_eta_secs((target_ms - now_ms) / 1_000)
}

pub(super) fn format_anthropic_usage_summary(
    val: &serde_json::Value,
    tier_badge: Option<&str>,
) -> Option<String> {
    let tier_suffix = tier_badge.map(|t| format!(" ({t})")).unwrap_or_default();
    let limits = val.get("limits").and_then(serde_json::Value::as_array);

    let find_limit = |want_kind: &str, want_group: &str| -> Option<(f64, Option<&str>)> {
        let arr = limits?;
        for item in arr {
            if item.get("is_active").and_then(serde_json::Value::as_bool) == Some(false) {
                continue;
            }
            let kind = item
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let group = item
                .get("group")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let scope_null = item.get("scope").is_none_or(serde_json::Value::is_null);
            if kind == want_kind || (group == want_group && scope_null) {
                let pct = item.get("percent").and_then(serde_json::Value::as_f64)?;
                let resets = item.get("resets_at").and_then(serde_json::Value::as_str);
                return Some((pct, resets));
            }
        }
        None
    };

    let mut parts = Vec::new();

    let window_5h = val
        .pointer("/five_hour/utilization")
        .and_then(serde_json::Value::as_f64)
        .map(|u| {
            (
                u,
                val.pointer("/five_hour/resets_at")
                    .and_then(serde_json::Value::as_str),
            )
        })
        .or_else(|| find_limit("five_hour_all", "five_hour"));

    if let Some((u5, resets_5h)) = window_5h {
        let rem_5h = (100.0 - u5).clamp(0.0, 100.0).round() as i64;
        let eta_5h = (rem_5h < 100)
            .then(|| resets_5h.and_then(format_reset_eta_iso))
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("5h: {rem_5h}% left{eta_5h}"));
    }

    let window_7d = val
        .pointer("/seven_day/utilization")
        .and_then(serde_json::Value::as_f64)
        .map(|u| {
            (
                u,
                val.pointer("/seven_day/resets_at")
                    .and_then(serde_json::Value::as_str),
            )
        })
        .or_else(|| find_limit("weekly_all", "weekly"));

    if let Some((u7, resets_7d)) = window_7d {
        let rem_7d = (100.0 - u7).clamp(0.0, 100.0).round() as i64;
        let eta_7d = (rem_7d < 100)
            .then(|| resets_7d.and_then(format_reset_eta_iso))
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("7d: {rem_7d}% left{eta_7d}"));
    }

    if let Some(u_opus) = val
        .pointer("/seven_day_opus/utilization")
        .and_then(serde_json::Value::as_f64)
    {
        let rem_opus = (100.0 - u_opus).clamp(0.0, 100.0).round() as i64;
        let eta_opus = (rem_opus < 100)
            .then(|| {
                val.pointer("/seven_day_opus/resets_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(format_reset_eta_iso)
            })
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("opus: {rem_opus}% left{eta_opus}"));
    }

    if let Some(minor) = val
        .pointer("/spend/balance/amount_minor")
        .and_then(serde_json::Value::as_i64)
    {
        let exp = val
            .pointer("/spend/balance/exponent")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(2) as i32;
        let dollars = (minor as f64) / 10f64.powi(exp);
        parts.push(format!("${dollars:.2} left"));
    } else if let Some(rem_d) = val
        .pointer("/seven_day/remaining_dollars")
        .and_then(serde_json::Value::as_f64)
    {
        parts.push(format!("${rem_d:.2} left"));
    } else if val
        .pointer("/extra_usage/is_enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && let Some(limit) = val
            .pointer("/extra_usage/monthly_limit")
            .and_then(serde_json::Value::as_f64)
    {
        let used = val
            .pointer("/extra_usage/used_credits")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let exp = val
            .pointer("/extra_usage/decimal_places")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(2) as i32;
        let scale = if limit > 1000.0 && exp == 2 {
            100.0
        } else {
            1.0
        };
        let rem_d = ((limit - used) / scale).max(0.0);
        parts.push(format!("extra: ${rem_d:.2} left"));
    }

    if parts.is_empty() {
        return None;
    }
    Some(format!("{}{tier_suffix}", parts.join(" · ")))
}

fn json_value_has_secret(val: &serde_json::Value) -> bool {
    match val {
        serde_json::Value::Object(map) => map.iter().any(|(k, v)| {
            let key_is_secret = matches!(
                k.as_str(),
                "key"
                    | "apiKey"
                    | "api_key"
                    | "access"
                    | "accessToken"
                    | "access_token"
                    | "refresh"
                    | "refreshToken"
                    | "refresh_token"
                    | "token"
                    | "bearer"
                    | "bearer_token"
                    | "bearerToken"
            );
            if key_is_secret
                && v.as_str().is_some_and(|s| {
                    let t = s.trim();
                    !t.is_empty() && !t.starts_with("${") && !t.eq_ignore_ascii_case("null")
                })
            {
                return true;
            }
            json_value_has_secret(v)
        }),
        serde_json::Value::Array(arr) => arr.iter().any(json_value_has_secret),
        _ => false,
    }
}

pub(super) fn profile_has_sylphx_credentials(prof_dir: &Path) -> bool {
    if read_first_non_empty_file(
        prof_dir,
        &[
            "sylphx.key",
            ".config/opencode/sylphx.key",
            "spiron.key",
            "api-key",
            ".env",
            "env",
        ],
    )
    .is_some()
    {
        return true;
    }
    for rel in [
        ".credentials.json",
        "auth.json",
        ".local/share/opencode/auth.json",
        ".config/opencode/opencode.json",
        "opencode.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(prof_dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && json_value_has_secret(&val)
        {
            return true;
        }
    }
    false
}

pub(super) fn probe_opencode_profile_credentials(
    prof_dir: &Path,
    provider: &str,
    now_ms: i64,
) -> Option<String> {
    let mut saw_expired_oauth = false;
    for rel in [
        ".local/share/opencode/auth.json",
        "auth.json",
        ".omp/auth.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(prof_dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
        {
            if let Some(map) = val.as_object() {
                for entry in map.values() {
                    if let Some(obj) = entry.as_object() {
                        let has_refresh = obj
                            .get("refresh")
                            .or_else(|| obj.get("refreshToken"))
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|s| !s.trim().is_empty());
                        let expired = obj
                            .get("expires")
                            .and_then(serde_json::Value::as_i64)
                            .is_some_and(|exp| exp > 0 && exp <= now_ms);
                        if expired && !has_refresh {
                            saw_expired_oauth = true;
                            continue;
                        }
                    }
                    if json_value_has_secret(entry) {
                        return Some(format!("active ({provider}-auth)"));
                    }
                }
            } else if json_value_has_secret(&val) {
                return Some(format!("active ({provider}-auth)"));
            }
        }
    }
    if read_first_non_empty_file(
        prof_dir,
        &[
            ".config/opencode/sylphx.key",
            "sylphx.key",
            "api-key",
            ".env",
            "env",
        ],
    )
    .is_some()
    {
        return Some(format!("active ({provider}-auth)"));
    }
    for rel in [
        ".config/opencode/opencode.json",
        "opencode.json",
        ".omp/agent.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(prof_dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && json_value_has_secret(&val)
        {
            return Some(format!("active ({provider}-auth)"));
        }
    }
    if saw_expired_oauth {
        Some("oauth expired".to_string())
    } else {
        None
    }
}

pub(crate) fn parse_remaining_pct_from_usage(usage: &str, model: Option<&str>) -> Option<i64> {
    let lower = usage.trim().to_ascii_lowercase();
    if lower.starts_with("oauth expired")
        || lower.starts_with(USAGE_LIMITED_PREFIX)
        || lower.contains("quota exhausted")
        || lower.contains("rate limited")
    {
        return Some(0);
    }
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    let wants_opus = model.is_some_and(|m| {
        let lower_m = m.to_ascii_lowercase();
        lower_m.contains("opus") || lower_m.contains("claude")
    });
    if wants_opus && usage.contains("opus:") {
        return extract_after("opus:");
    }
    if usage.contains("gemini:") {
        return extract_after("gemini:");
    }
    let mut min_pct: Option<i64> = None;
    for prefix in ["5h:", "7d:", "mo:"] {
        if let Some(p) = extract_after(prefix) {
            min_pct = Some(min_pct.map_or(p, |cur| cur.min(p)));
        }
    }
    if min_pct.is_some() {
        return min_pct;
    }
    // Generic `<N>% left` without a window prefix.
    let mut scan = usage;
    let mut generic_min: Option<i64> = None;
    while let Some(pos) = scan.find("% left") {
        let before = &scan[..pos];
        let digits: String = before
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_whitespace() || c.is_ascii_digit())
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        if let Ok(p) = digits.trim().parse::<i64>() {
            let clamped = p.clamp(0, 100);
            generic_min = Some(generic_min.map_or(clamped, |cur| cur.min(clamped)));
        }
        scan = &scan[pos + "% left".len()..];
    }
    if generic_min.is_some() {
        return generic_min;
    }
    if let Some(pct) = parse_currency_balance_pct(usage) {
        return Some(pct);
    }
    if lower.starts_with("online") || lower.starts_with("active") {
        return Some(100);
    }
    None
}

fn parse_currency_balance_pct(usage: &str) -> Option<i64> {
    for sym in ['$', '¥'] {
        if let Some(idx) = usage.find(sym) {
            let after = &usage[idx + sym.len_utf8()..];
            let rem_str: String = after
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            let Ok(rem) = rem_str.parse::<f64>() else {
                continue;
            };
            let rest = after[rem_str.len()..].trim_start();
            if let Some(after_slash) = rest.strip_prefix('/') {
                let after_sym = after_slash.trim_start().trim_start_matches(sym);
                let tot_str: String = after_sym
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.')
                    .collect();
                if let Ok(tot) = tot_str.parse::<f64>()
                    && tot > 0.0
                {
                    return Some(((rem / tot) * 100.0).round().clamp(0.0, 100.0) as i64);
                }
            }
            return Some(if rem <= 0.0 { 0 } else { 100 });
        }
    }
    None
}

pub(crate) fn probe_profile_remaining_pct(
    data: &Path,
    provider: &str,
    profile: &str,
    model: Option<&str>,
) -> Option<i64> {
    sync_fresher_bound_agent_credentials(data);
    let usage = probe_one_profile_usage(data, provider, profile)?;
    let pct = parse_remaining_pct_from_usage(&usage, model)?;
    if provider == "anthropic" && pct > 0 {
        let prof_dir = data.join("profiles").join(provider).join(profile);
        let now_ms = anyagents_core::now_ms();
        let fresh_live_read = std::fs::metadata(prof_dir.join(".anyagents-last-usage.txt"))
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .is_some_and(|d| now_ms.saturating_sub(d.as_millis() as i64) < 65_000);
        if !fresh_live_read {
            return None;
        }
    }
    Some(pct)
}

/// Clears historical `CORTEX_STEP_TYPE_ERROR_MESSAGE` (`step_type = 17`) rows
/// in `<config_dir>/.gemini/antigravity-cli/conversations/*.db` so `agy` does not
/// re-emit stale `"status":"ERROR","error":"...Resets in..."` result lines at the
/// end of subsequent healthy turns via `StepIndicesByType(17)`.
pub(crate) fn clear_stale_agy_quota_error_steps(config_dir: &Path) {
    let conv_dir = config_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("conversations");
    if !conv_dir.is_dir() {
        return;
    }
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(conv_meta) = std::fs::metadata(&conv_dir)
    {
        use std::os::unix::fs::MetadataExt as _;
        let (my_uid, my_gid) = std::fs::metadata("/proc/self")
            .map(|m| (m.uid(), m.gid()))
            .unwrap_or((conv_meta.uid(), conv_meta.gid()));
        let mut seat_uid_gid: Option<(u32, u32)> = None;
        if conv_meta.uid() != my_uid {
            seat_uid_gid = Some((conv_meta.uid(), conv_meta.gid()));
        } else if conv_meta.gid() != my_gid {
            seat_uid_gid = Some((conv_meta.gid(), conv_meta.gid()));
        }
        if let Ok(entries) = std::fs::read_dir(&conv_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("db")
                    && let Ok(m) = entry.metadata()
                    && m.uid() != my_uid
                {
                    seat_uid_gid = Some((m.uid(), m.gid()));
                    break;
                }
            }
        }
        if let Some((target_uid, target_gid)) = seat_uid_gid {
            let sock_gid = config_dir
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| std::fs::metadata(p).ok())
                .map(|m| m.gid())
                .unwrap_or(my_gid);
            let uid_s = target_uid.to_string();
            let gid_s = target_gid.to_string();
            let groups_s = format!("{target_gid},{sock_gid},{my_gid}");
            let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &groups_s,
                    "--no-new-privs",
                    "--",
                    "python3",
                    "-c",
                    "import glob, os, sqlite3, sys\nfor p in glob.glob(os.path.join(sys.argv[1], '*.db')):\n    try:\n        c = sqlite3.connect(p, timeout=0.5)\n        if c.execute('SELECT 1 FROM steps WHERE step_type = 17 LIMIT 1').fetchone():\n            c.execute('UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = ? WHERE step_type = 17', (b' \\x05*\\x03\\xd2\\x01\\x00',))\n            c.commit()\n        c.close()\n    except Exception:\n        pass\n",
                ])
                .arg(&conv_dir)
                .stderr(std::process::Stdio::null())
                .status();
            return;
        }
    }
    let Ok(entries) = std::fs::read_dir(&conv_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        if let Ok(conn) = rusqlite::Connection::open(&path) {
            let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
            let _ = conn.execute(
                "UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = X'20052a03d20100' WHERE step_type = 17",
                [],
            );
        }
    }
}

/// Probes the exact model catalog and per-model quota availability for upstream
/// provider profiles (especially `google-antigravity/<profile>`).
pub(crate) fn probe_profile_models_catalog(data: &Path, target: Option<&str>) -> serde_json::Value {
    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("models") && !s.eq_ignore_ascii_case("all")
    });
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(6)))
        .build()
        .new_agent();

    let mut agy_cli_models = Vec::new();
    let agy_bin = crate::sandbox::host_home_dir()
        .map(|h| h.join(".local/bin/agy"))
        .filter(|p| p.is_file())
        .unwrap_or_else(|| PathBuf::from("agy"));
    if let Ok(out) = std::process::Command::new(&agy_bin).arg("models").output()
        && out.status.success()
    {
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("Available models:") {
                continue;
            }
            let (left, right) = trimmed.split_once("  ").unwrap_or((trimmed, ""));
            let id = left.trim();
            if id.is_empty() {
                continue;
            }
            let is_default = right.contains("(default)");
            let label = right.replace("(default)", "").trim().to_string();
            agy_cli_models.push(serde_json::json!({
                "id": id,
                "label": label,
                "default": is_default,
            }));
        }
    }

    let mut profiles_out = Vec::new();
    let profiles_root = data.join("profiles");
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        let mut prov_list: Vec<_> = providers.flatten().collect();
        prov_list.sort_by_key(|e| e.file_name());
        for prov_entry in prov_list {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            if filter.is_none() && provider != "google-antigravity" {
                continue;
            }
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            let mut prof_list: Vec<_> = prof_dirs.flatten().collect();
            prof_list.sort_by_key(|e| e.file_name());
            for prof_entry in prof_list {
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
                    && !(f == "agy" && provider == "google-antigravity")
                {
                    continue;
                }
                if provider == "google-antigravity" {
                    let _ = probe_one_profile_usage(data, &provider, &profile);
                    let tok_path = prof_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
                    let mut backend_models = Vec::new();
                    if let Ok(tok_text) = std::fs::read_to_string(&tok_path)
                        && let Ok(tok_val) = serde_json::from_str::<serde_json::Value>(&tok_text)
                        && let Some(tok) = tok_val
                            .pointer("/token/access_token")
                            .or_else(|| tok_val.get("access_token"))
                            .and_then(|v| v.as_str())
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                        && let Ok(resp) = http
                            .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels")
                            .header("authorization", &format!("Bearer {tok}"))
                            .header("content-type", "application/json")
                            .header("user-agent", "antigravity")
                            .send(b"{}")
                        && let Ok(body) = resp.into_body().read_to_string()
                        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
                        && let Some(models_obj) = val.get("models").and_then(|m| m.as_object())
                    {
                        for (model_id, m) in models_obj {
                            let qi = m.get("quotaInfo");
                            let rem_pct = qi
                                .and_then(|q| q.get("remainingFraction"))
                                .and_then(|v| v.as_f64())
                                .map(|f| (f * 100.0).clamp(0.0, 100.0).round() as i64)
                                .unwrap_or(0);
                            let reset_time = qi
                                .and_then(|q| q.get("resetTime"))
                                .and_then(|v| v.as_str());
                            backend_models.push(serde_json::json!({
                                "id": model_id,
                                "display_name": m.get("displayName").and_then(|v| v.as_str()),
                                "remaining_pct": rem_pct,
                                "reset_time": reset_time,
                            }));
                        }
                    }
                    profiles_out.push(serde_json::json!({
                        "profile": full_name,
                        "provider": provider,
                        "cli_models": agy_cli_models,
                        "backend_models": backend_models,
                    }));
                }
            }
        }
    }

    // In thin/serverless coordinators without a host `agy` binary, synthesize
    // `agy_cli_models` from the live backend model catalog fetched via synced
    // OAuth tokens so `agents(action="models")` never returns empty `[]`.
    if agy_cli_models.is_empty() {
        let mut seen = std::collections::BTreeMap::<String, String>::new();
        for p in &profiles_out {
            if let Some(bms) = p.get("backend_models").and_then(|v| v.as_array()) {
                for bm in bms {
                    if let Some(id) = bm.get("id").and_then(|v| v.as_str()) {
                        let label = bm
                            .get("display_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or(id)
                            .to_string();
                        seen.entry(id.to_string()).or_insert(label);
                    }
                }
            }
        }
        for (id, label) in seen {
            agy_cli_models.push(serde_json::json!({
                "id": id,
                "label": label,
                "default": false,
            }));
        }
        if !agy_cli_models.is_empty() {
            for p in &mut profiles_out {
                if let Some(obj) = p.as_object_mut()
                    && obj
                        .get("cli_models")
                        .and_then(|v| v.as_array())
                        .is_some_and(Vec::is_empty)
                {
                    obj.insert(
                        "cli_models".to_string(),
                        serde_json::Value::Array(agy_cli_models.clone()),
                    );
                }
            }
        }
    }

    serde_json::json!({
        "agy_cli_models": agy_cli_models,
        "profiles": profiles_out,
    })
}
