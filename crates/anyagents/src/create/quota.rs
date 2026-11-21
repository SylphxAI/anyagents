//! Quota reset tracking, usage enrichment, and CLI model discovery.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, agent_config_dir};

use super::profile_identity::{extract_profile_account_identity, read_first_non_empty_file};
use super::{
    ProviderProfileEntry, QuotaWindow, bound_profile_of_provider, probe_one_profile_usage,
    probe_one_profile_usage_with_options, sync_fresher_bound_agent_credentials,
    upstream_provider_of, write_seat_shared_file,
};

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
    last_usage_path: Option<&Path>,
    cached: &str,
    now_ms: i64,
) -> String {
    let usage_mtime_ms = last_usage_path
        .and_then(|path| std::fs::metadata(path).ok())
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

pub(super) fn format_reset_eta_epoch_ms(target_ms: i64) -> Option<String> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    format_reset_eta_secs((target_ms - now_ms) / 1_000)
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
    parse_remaining_pct_from_usage(&usage, model)
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
                    profiles_out.push(serde_json::json!({
                        "profile": full_name,
                        "provider": provider,
                        "cli_models": agy_cli_models,
                        "backend_models": [],
                    }));
                }
            }
        }
    }

    serde_json::json!({
        "agy_cli_models": agy_cli_models,
        "profiles": profiles_out,
    })
}
