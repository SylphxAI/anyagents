//! Append-Only KV-Cache Pinned Context Window, Autonomous Compaction (`context_window_open`),
//! and Lossless `Raw-RAG` Hybrid BM25 + Contiguous Episode Recall (`memory_recall`).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

// Conservative offline budget for supported long-context models when neither the
// live nor persisted catalog identifies the active model. Never a per-model limit.
pub const DEFAULT_CONTEXT_WINDOW_TOKENS: usize = 128_000;
pub const MAX_WINDOW_EST_TOKENS: usize = DEFAULT_CONTEXT_WINDOW_TOKENS * 95 / 100;
pub const CONTEXT_WATERMARK_PERCENT: usize = 85;
pub const WINDOW_MAX_ITEMS: usize = 4_000;
pub const WINDOW_STRIDE_ITEMS: usize = 1_000;

type RemoteModelLimitsMap = HashMap<String, (usize, usize, Option<usize>)>;

type RemoteModelsCache = HashMap<(std::path::PathBuf, String), (Instant, RemoteModelLimitsMap)>;

fn remote_models_cache() -> &'static Mutex<RemoteModelsCache> {
    static CACHE: OnceLock<Mutex<RemoteModelsCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn parse_models_catalog_json(val: &Value, map: &mut RemoteModelLimitsMap) {
    let arr = val
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| val.get("models").and_then(Value::as_array));
    let Some(items) = arr else {
        return;
    };
    for item in items {
        let Some(slug) = item
            .get("id")
            .or_else(|| item.get("slug"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let cw = item
            .pointer("/limits/constraints/context_window_tokens")
            .and_then(Value::as_u64)
            .or_else(|| {
                item.pointer("/limits/context_window")
                    .and_then(Value::as_u64)
            })
            .or_else(|| item.get("context_window").and_then(Value::as_u64))
            .or_else(|| item.get("max_context_window").and_then(Value::as_u64))
            .or_else(|| item.get("context_length").and_then(Value::as_u64))
            .unwrap_or(0) as usize;
        if cw == 0 {
            continue;
        }
        let eff_pct = item
            .get("effective_context_window_percent")
            .and_then(Value::as_u64)
            .map(|p| (p as usize).clamp(40, 95))
            .unwrap_or(CONTEXT_WATERMARK_PERCENT);
        let max_output = item
            .pointer("/limits/constraints/max_output_tokens")
            .or_else(|| item.pointer("/limits/max_output_tokens"))
            .or_else(|| item.get("max_output_tokens"))
            .and_then(Value::as_u64)
            .filter(|n| *n > 0)
            .map(|n| n as usize);
        map.insert(slug.to_ascii_lowercase(), (cw, eff_pct, max_output));
    }
}

fn canonicalize_model_lookup_key(raw_model: &str) -> Vec<String> {
    let base = raw_model
        .split('#')
        .next()
        .unwrap_or(raw_model)
        .trim()
        .to_ascii_lowercase();
    let stripped = base.strip_prefix("sylphx/").unwrap_or(&base);
    let mut keys = vec![stripped.to_string(), base.clone()];
    if stripped.contains("k3.1-pro") || stripped.contains("kimi-k3") {
        keys.push("moonshotai/kimi-k3.1-pro".to_string());
        keys.push("moonshotai/kimi-k3".to_string());
    } else if stripped.contains("k2.7") || stripped.contains("kimi-for-coding") {
        keys.push("moonshotai/kimi-k2.7-code".to_string());
    } else if stripped.contains("gpt-6.1-sol") {
        keys.push("openai/gpt-6.1-sol".to_string());
    } else if stripped.contains("claude-opus-5") {
        keys.push("anthropic/claude-opus-5.5".to_string());
    } else if stripped.contains("argon") {
        keys.push("google/gemini-4-argon".to_string());
    }
    keys
}

/// Resolves `(window_tokens, max_window_est_tokens, watermark_percent)` dynamically from the
/// remote Sylphx Model Gateway (`GET /v1/models`) and cached catalog for the agent's model.
pub(super) fn resolve_context_window_for_transcript(
    transcript_path: &Path,
) -> (usize, usize, usize) {
    let Some(config_dir) = transcript_path.parent() else {
        return (
            DEFAULT_CONTEXT_WINDOW_TOKENS,
            MAX_WINDOW_EST_TOKENS,
            CONTEXT_WATERMARK_PERCENT,
        );
    };
    let agent_json = std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok());
    let raw_model = agent_json
        .as_ref()
        .and_then(|v| v.get("model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(super::auth::DEFAULT_SYLPHX_MODEL);

    let limits = resolve_model_context_limits(config_dir, raw_model, &[]);
    (
        limits.window_tokens,
        limits.window_tokens * 95 / 100,
        limits.compact_percent,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ContextLimitSource {
    Catalog,
    ConservativeFallback,
}

#[derive(Clone, Debug)]
pub(super) struct ModelContextLimits {
    pub window_tokens: usize,
    pub compact_percent: usize,
    pub max_output_tokens: Option<usize>,
    pub source: ContextLimitSource,
}

/// Resolve the actual dispatch model, not a possibly stale model in agent.json.
/// Persisted catalogs remain usable through outages; unknown IDs use a visible,
/// conservative 128k budget rather than either assuming 1M or stalling all turns.
pub(super) fn resolve_model_context_limits(
    config_dir: &Path,
    raw_model: &str,
    env: &[(String, String)],
) -> ModelContextLimits {
    let agent_json = std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok());
    let lookup_keys = canonicalize_model_lookup_key(raw_model);
    let env_gateway = env
        .iter()
        .find(|(k, _)| k == "SYLPHX_GATEWAY_URL")
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var("SYLPHX_GATEWAY_URL").ok());
    let gw_url = env_gateway
        .as_deref()
        .or_else(|| {
            agent_json
                .as_ref()
                .and_then(|v| v.get("gateway_base_url"))
                .and_then(Value::as_str)
        })
        .map(|s| s.trim().trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .unwrap_or(super::auth::DEFAULT_SYLPHX_GATEWAY_URL);
    // Catalogs from another gateway or agent/profile are not authoritative for
    // this dispatch, even when model IDs happen to match.
    let cache_scope = (config_dir.to_path_buf(), gw_url.to_string());

    // 1. Check process-wide remote `/v1/models` cache (TTL 10 minutes).
    if let Ok(guard) = remote_models_cache().lock()
        && let Some((fetched_at, map)) = guard.get(&cache_scope)
        && fetched_at.elapsed() < Duration::from_secs(600)
    {
        for k in &lookup_keys {
            if let Some(&(cw, eff_pct, max_output_tokens)) = map.get(k) {
                return ModelContextLimits {
                    window_tokens: cw,
                    compact_percent: eff_pct,
                    max_output_tokens,
                    source: ContextLimitSource::Catalog,
                };
            }
        }
    }

    // 2. Fetch live from remote `GET {gateway_base_url}/models` (unless in isolated unit test).
    let mut map = remote_models_cache()
        .lock()
        .ok()
        .and_then(|g| g.get(&cache_scope).map(|(_, map)| map.clone()))
        .unwrap_or_default();
    for local_file in [".sylphx-remote-models.json", "model-catalog-direct.json"] {
        if let Ok(raw) = std::fs::read_to_string(config_dir.join(local_file))
            && let Ok(val) = serde_json::from_str::<Value>(&raw)
        {
            if local_file == ".sylphx-remote-models.json" {
                // A persisted response from another gateway (or an old cache
                // with no provenance) is not evidence for this endpoint.
                if val.get("gateway_base_url").and_then(Value::as_str) == Some(gw_url)
                    && let Some(catalog) = val.get("catalog")
                {
                    parse_models_catalog_json(catalog, &mut map);
                }
            } else {
                parse_models_catalog_json(&val, &mut map);
            }
        }
    }
    if !super::sandbox::is_isolated_test_env()
        && let Some(api_key) = super::auth::resolve_sylphx_gateway_key(config_dir, env)
    {
        let models_url = format!("{gw_url}/models");
        if let Ok((status, raw)) = super::curl_get(&models_url, Some(&api_key))
            && (200..300).contains(&status)
            && let Ok(val) = serde_json::from_str::<Value>(&raw)
        {
            parse_models_catalog_json(&val, &mut map);
            let cache = json!({"gateway_base_url": gw_url, "catalog": val});
            if let Ok(serialized) = serde_json::to_vec(&cache) {
                let _ = std::fs::write(config_dir.join(".sylphx-remote-models.json"), serialized);
            }
        }
    }
    if !map.is_empty()
        && let Ok(mut guard) = remote_models_cache().lock()
    {
        guard.insert(cache_scope, (Instant::now(), map.clone()));
    }
    for k in &lookup_keys {
        if let Some(&(cw, eff_pct, max_output_tokens)) = map.get(k) {
            return ModelContextLimits {
                window_tokens: cw,
                compact_percent: eff_pct,
                max_output_tokens,
                source: ContextLimitSource::Catalog,
            };
        }
    }
    ModelContextLimits {
        window_tokens: map
            .values()
            .map(|(window, _, _)| *window)
            .min()
            .unwrap_or(DEFAULT_CONTEXT_WINDOW_TOKENS)
            .min(DEFAULT_CONTEXT_WINDOW_TOKENS),
        compact_percent: CONTEXT_WATERMARK_PERCENT,
        max_output_tokens: None,
        source: ContextLimitSource::ConservativeFallback,
    }
}

/// Move a suffix boundary back over every completed call/output pair it splits.
/// Parallel calls can interleave, so inspecting only the first retained item is
/// insufficient. Extending the suffix preserves actual results, never invents a
/// result or executes a call, and keeps the prefix stable between stride changes.
fn paired_suffix_start(history: &[(usize, Value)], mut start: usize) -> usize {
    let mut open_calls = std::collections::HashMap::new();
    let mut pairs = Vec::new();
    for (index, (_, item)) in history.iter().enumerate() {
        let Some(call_id) = item.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                open_calls.insert(call_id, index);
            }
            Some("function_call_output") => {
                if let Some(call) = open_calls.remove(call_id) {
                    pairs.push((call, index));
                }
            }
            _ => {}
        }
    }
    // Descending output order also closes pairs exposed by an earlier extension.
    for (call, output) in pairs.into_iter().rev() {
        if call < start && output >= start {
            start = call;
        }
    }
    start
}

/// Last boundary before serialization: malformed raw history must not become a
/// malformed model request. Open calls are allowed (e.g. an interrupted batch),
/// but outputs must consume a preceding open call exactly once. No raw content
/// or call arguments are included in the diagnostic.
pub(super) fn validate_responses_input(items: &[Value]) -> Result<(), String> {
    let mut open_calls = std::collections::HashSet::new();
    for (index, item) in items.iter().enumerate() {
        let kind = item.get("type").and_then(Value::as_str);
        if !matches!(kind, Some("function_call" | "function_call_output")) {
            continue;
        }
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| format!("invalid Responses input[{index}].call_id: missing call ID"))?;
        match kind {
            Some("function_call") if !open_calls.insert(call_id) => {
                return Err(format!(
                    "invalid Responses input[{index}].call_id: duplicate open call"
                ));
            }
            Some("function_call_output") if !open_calls.remove(call_id) => {
                return Err(format!(
                    "invalid Responses input[{index}].call_id: output has no preceding open call; raw transcript preserved"
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_range_spec_set(spec_val: &Value) -> std::collections::HashSet<usize> {
    fn push_str(set: &mut std::collections::HashSet<usize>, raw: &str) {
        for part in raw.split(',') {
            let clean = part
                .trim()
                .strip_prefix("transcript:")
                .unwrap_or(part.trim())
                .trim_start_matches('#');
            if clean.is_empty() {
                continue;
            }
            if let Some((a, b)) = clean.split_once('-') {
                let start = a.trim().trim_start_matches('#').parse::<usize>().ok();
                let end = b.trim().trim_start_matches('#').parse::<usize>().ok();
                if let (Some(s), Some(e)) = (start, end) {
                    for idx in s.min(e)..=s.max(e) {
                        set.insert(idx);
                    }
                }
            } else if let Ok(single) = clean.parse::<usize>() {
                set.insert(single);
            }
        }
    }
    let mut set = std::collections::HashSet::new();
    match spec_val {
        Value::String(s) => push_str(&mut set, s),
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                set.insert(u as usize);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                if let Some(s) = item.as_str() {
                    push_str(&mut set, s);
                } else if let Some(u) = item.as_u64() {
                    set.insert(u as usize);
                }
            }
        }
        _ => {}
    }
    set
}

fn apply_context_edit_to_history(
    history: &mut Vec<(usize, Value)>,
    edit_event: &Value,
) -> std::collections::HashSet<String> {
    // 1. In-place `replace` / `mask`: compress verbose tool outputs or messages into 1-line notes
    if let Some(replacements) = edit_event
        .get("replace")
        .or_else(|| edit_event.get("mask"))
        .and_then(Value::as_array)
    {
        for rep in replacements {
            let target_spec = rep
                .get("range")
                .or_else(|| rep.get("index"))
                .or_else(|| rep.get("indices"))
                .unwrap_or(&Value::Null);
            let target_indices = parse_range_spec_set(target_spec);
            let with_note = rep
                .get("with")
                .or_else(|| rep.get("summary"))
                .or_else(|| rep.get("note"))
                .and_then(Value::as_str)
                .unwrap_or("pruned")
                .trim();
            if target_indices.is_empty() {
                continue;
            }
            for (raw_idx, item) in history.iter_mut() {
                if !target_indices.contains(raw_idx) {
                    continue;
                }
                let replacement_text = format!(
                    "[context_edit (#{raw_idx}): {with_note} | raw preserved at transcript:{raw_idx}-{raw_idx}]"
                );
                if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
                    if let Some(obj) = item.as_object_mut() {
                        obj.insert("output".to_string(), json!(replacement_text));
                    }
                } else if (item.get("type").and_then(Value::as_str) == Some("message")
                    || item.get("role").is_some())
                    && let Some(obj) = item.as_object_mut()
                {
                    obj.remove("content");
                    obj.insert("content".to_string(), json!(replacement_text));
                }
            }
        }
    }

    // 2. Surgical `drop` or `keep_only`: remove obsolete turns while enforcing `/v1/responses`
    //    `call_id` pair integrity (never leaving an orphaned `function_call` or `function_call_output`).
    let drop_set = edit_event
        .get("drop")
        .map(parse_range_spec_set)
        .unwrap_or_default();
    let keep_only_set = edit_event
        .get("keep_only")
        .map(parse_range_spec_set)
        .unwrap_or_default();
    if drop_set.is_empty() && keep_only_set.is_empty() {
        return std::collections::HashSet::new();
    }

    let should_drop_idx = |raw_idx: usize| -> bool {
        if !keep_only_set.is_empty() && !keep_only_set.contains(&raw_idx) {
            return true;
        }
        drop_set.contains(&raw_idx)
    };

    let mut protected_call_ids: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut dropped_call_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut open_call_ids = std::collections::HashSet::new();
    for (raw_idx, item) in history.iter() {
        if let Some(cid) = item.get("call_id").and_then(Value::as_str) {
            match item.get("type").and_then(Value::as_str) {
                Some("function_call") => {
                    open_call_ids.insert(cid.to_string());
                }
                Some("function_call_output") => {
                    open_call_ids.remove(cid);
                }
                _ => {}
            }
        }
        if item.get("type").and_then(Value::as_str) == Some("function_call")
            && matches!(
                item.get("name")
                    .and_then(Value::as_str)
                    .map(|name| name.strip_prefix("mcp__anyagents__").unwrap_or(name)),
                Some("context_edit" | "context_prune")
            )
            && let Some(cid) = item.get("call_id").and_then(Value::as_str)
        {
            protected_call_ids.insert(cid.to_string());
        }
        if should_drop_idx(*raw_idx)
            && let Some(cid) = item.get("call_id").and_then(Value::as_str)
        {
            dropped_call_ids.insert(cid.to_string());
        }
    }

    history.retain(|(raw_idx, item)| {
        if let Some(cid) = item.get("call_id").and_then(Value::as_str)
            && protected_call_ids.contains(cid)
        {
            return true;
        }
        if should_drop_idx(*raw_idx) {
            return false;
        }
        if let Some(cid) = item.get("call_id").and_then(Value::as_str)
            && dropped_call_ids.contains(cid)
        {
            return false;
        }
        true
    });
    open_call_ids.retain(|cid| dropped_call_ids.contains(cid) && !protected_call_ids.contains(cid));
    open_call_ids
}

fn include_after_context_edit(
    item: &Value,
    pruned_open_calls: &mut std::collections::HashSet<String>,
) -> bool {
    let Some(cid) = item.get("call_id").and_then(Value::as_str) else {
        return true;
    };
    match item.get("type").and_then(Value::as_str) {
        Some("function_call_output") => !pruned_open_calls.remove(cid),
        Some("function_call") => {
            pruned_open_calls.remove(cid);
            true
        }
        _ => true,
    }
}

/// Project an anchor without writing it. Preflight must validate the exact same
/// representation the persisted loader will use before committing a new window.
pub(super) fn project_context_anchor(anchor: &Value) -> Vec<Value> {
    let handoff = anchor
        .get("handoff")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let mut items = vec![json!({
        "role": "user",
        "content": format!(
            "<context-event kind=\"context.window_opened\">\n<handoff>\n{handoff}\n</handoff>\nPrior raw transcript entries remain indexed in full; call `memory_recall` with `query` or `range` (`transcript:<from>-<to>`) to inspect exact prior turns.\n</context-event>"
        ),
    })];
    if let Some(retained) = anchor.get("retained_items").and_then(Value::as_array) {
        items.extend(retained.iter().cloned());
    }
    items
}

/// Loads persisted transcript items along with their raw transcript line indices `(raw_idx, item)`.
pub(super) fn load_transcript_indexed_items(transcript_path: &Path) -> Vec<(usize, Value)> {
    load_transcript_indexed_items_before(transcript_path, usize::MAX)
}

fn load_transcript_indexed_items_before(transcript_path: &Path, end: usize) -> Vec<(usize, Value)> {
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return Vec::new();
    };
    let mut history: Vec<(usize, Value)> = Vec::new();
    let mut budget_managed_window = false;
    let mut pruned_open_calls = std::collections::HashSet::new();
    for (raw_idx, line) in BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .enumerate()
    {
        if raw_idx >= end {
            break;
        }
        let Ok(m) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if m.get("type").and_then(Value::as_str) == Some("context_window_open") {
            history.clear();
            budget_managed_window = m.get("retained_items").is_some();
            pruned_open_calls.clear();
            history.extend(
                project_context_anchor(&m)
                    .into_iter()
                    .map(|item| (raw_idx, item)),
            );
            continue;
        }
        if m.get("type").and_then(Value::as_str) == Some("context_edit") {
            // An edit can run inside a batch, before another already-issued
            // call's result is appended. Carry pairing through those later
            // results, without applying keep_only to future unrelated messages.
            pruned_open_calls.extend(apply_context_edit_to_history(&mut history, &m));
            continue;
        }
        if m.get("type").and_then(Value::as_str) == Some("native_turn_state") {
            continue;
        }
        if m.get("type").is_some() {
            if include_after_context_edit(&m, &mut pruned_open_calls) {
                history.push((raw_idx, m));
            }
            continue;
        }
        match m.get("role").and_then(Value::as_str) {
            Some("user") => {
                history.push((raw_idx, m));
            }
            Some("assistant") => {
                if let Some(s) = m
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    history.push((
                        raw_idx,
                        json!({
                            "role": "assistant",
                            "content": s,
                        }),
                    ));
                }
                if let Some(tcs) = m.get("tool_calls").and_then(Value::as_array) {
                    for tc in tcs {
                        if let (Some(cid), Some(fname)) = (
                            tc.get("id").and_then(Value::as_str),
                            tc.pointer("/function/name").and_then(Value::as_str),
                        ) {
                            pruned_open_calls.remove(cid);
                            let fargs = tc
                                .pointer("/function/arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}");
                            history.push((
                                raw_idx,
                                json!({
                                    "type": "function_call",
                                    "call_id": cid,
                                    "name": fname,
                                    "arguments": fargs,
                                }),
                            ));
                        }
                    }
                }
            }
            Some("tool") => {
                if let Some(cid) = m.get("tool_call_id").and_then(Value::as_str) {
                    if pruned_open_calls.remove(cid) {
                        continue;
                    }
                    let out_str = m.get("content").and_then(Value::as_str).unwrap_or("");
                    history.push((
                        raw_idx,
                        json!({
                            "type": "function_call_output",
                            "call_id": cid,
                            "output": out_str,
                        }),
                    ));
                }
            }
            _ => {}
        }
    }
    let mut open_calls = std::collections::HashSet::new();
    history.retain(|(_, item)| match item.get("type").and_then(Value::as_str) {
        Some("function_call") => item
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .is_some_and(|cid| open_calls.insert(cid.to_string())),
        Some("function_call_output") => item
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .is_some_and(|cid| open_calls.remove(cid)),
        _ => true,
    });
    // Automatic anchors retain exact pending state. Never silently discard it
    // using the legacy bytes/4 sliding heuristic: the wire preflight owns this
    // window and either fits it or refuses before dispatch.
    if budget_managed_window {
        return history;
    }
    if history.len() > WINDOW_MAX_ITEMS {
        let excess = history.len() - WINDOW_STRIDE_ITEMS;
        let block_start = paired_suffix_start(
            &history,
            (excess / WINDOW_STRIDE_ITEMS) * WINDOW_STRIDE_ITEMS,
        );
        history.drain(0..block_start);
        while history
            .first()
            .is_some_and(|(_, item)| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        {
            history.remove(0);
        }
        let mask_bound = (WINDOW_STRIDE_ITEMS / 2).min(history.len());
        for (_, item) in &mut history[..mask_bound] {
            if item.get("type").and_then(Value::as_str) == Some("function_call_output")
                && let Some(out_str) = item.get("output").and_then(Value::as_str)
                && out_str.len() > 4_000
            {
                let masked = super::auth::truncate_tool_output_middle_utf8(
                    out_str.to_string(),
                    2_000,
                    900,
                    900,
                );
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("output".to_string(), json!(masked));
                }
            }
        }
    }
    // Enforce hard token budget (`MAX_WINDOW_EST_TOKENS`) even when `history.len() <= WINDOW_MAX_ITEMS`
    // so large tool outputs or reasoning blocks can never grow the active window past the model context limit.
    let est_chars = |items: &[(usize, Value)]| -> usize {
        items
            .iter()
            .map(|(_, v)| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
            .sum()
    };
    let (_, max_window_est_tokens, _) = resolve_context_window_for_transcript(transcript_path);
    let max_chars = max_window_est_tokens * 4;
    if est_chars(&history) > max_chars {
        let older_bound = (history.len() * 3) / 4;
        for (raw_idx, item) in &mut history[..older_bound] {
            if item.get("type").and_then(Value::as_str) == Some("function_call_output")
                && let Some(out_str) = item.get("output").and_then(Value::as_str)
                && out_str.len() > 1_600
            {
                let masked = format!(
                    "{}\n[context_budget (#{raw_idx}): compressed; raw preserved at transcript:{raw_idx}-{raw_idx}]",
                    super::auth::truncate_tool_output_middle_utf8(
                        out_str.to_string(),
                        1_200,
                        500,
                        500,
                    )
                );
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("output".to_string(), json!(masked));
                }
            }
        }
        while est_chars(&history) > max_chars && history.len() > 8 {
            let step_cut = (history.len() / 4).max(4);
            let block_start = paired_suffix_start(&history, step_cut);
            if block_start == 0 {
                // If an outer call spans the entire window, compress the largest tool output in the window.
                if let Some((raw_idx, largest)) = history
                    .iter_mut()
                    .filter(|(_, it)| {
                        it.get("type").and_then(Value::as_str) == Some("function_call_output")
                    })
                    .max_by_key(|(_, it)| {
                        it.get("output")
                            .and_then(Value::as_str)
                            .map(str::len)
                            .unwrap_or(0)
                    })
                    && let Some(out_str) = largest.get("output").and_then(Value::as_str)
                    && out_str.len() > 1_600
                {
                    let masked = format!(
                        "{}\n[context_budget (#{raw_idx}): compressed; raw preserved at transcript:{raw_idx}-{raw_idx}]",
                        super::auth::truncate_tool_output_middle_utf8(
                            out_str.to_string(),
                            1_200,
                            500,
                            500,
                        )
                    );
                    if let Some(obj) = largest.as_object_mut() {
                        obj.insert("output".to_string(), json!(masked));
                        continue;
                    }
                }
                break;
            }
            history.drain(0..block_start);
            while history.first().is_some_and(|(_, item)| {
                item.get("type").and_then(Value::as_str) == Some("reasoning")
            }) {
                history.remove(0);
            }
        }
    }
    history
}

/// Loads persisted transcript items for `/v1/responses` (`input`), starting from
/// the most recent `context_window_open` anchor, applying any surgical `context_edit`
/// pruning/masking events (`Context Language Models`, `arXiv:2609.37725`), and using
/// block-aligned stride windowing (`WINDOW_STRIDE_ITEMS = 80`) so item 0 stays cache-pinned.
pub(super) fn load_transcript_input_items(transcript_path: &Path) -> Vec<Value> {
    load_transcript_indexed_items(transcript_path)
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

pub(super) fn estimate_items_tokens(items: &[Value]) -> usize {
    let chars: usize = items
        .iter()
        .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
        .sum();
    chars / 4
}

fn has_watermark_in_window(items: &[Value]) -> bool {
    items.iter().any(|item| {
        item.get("content")
            .and_then(Value::as_str)
            .is_some_and(|s| s.contains("kind=\"context.watermark\""))
    })
}

pub(super) fn maybe_emit_context_watermark_event(
    transcript_path: &Path,
    input_items: &mut Vec<Value>,
) {
    if has_watermark_in_window(input_items) {
        return;
    }
    let (window_tokens, _, watermark_percent) =
        resolve_context_window_for_transcript(transcript_path);
    let used_tokens = estimate_items_tokens(input_items);
    let used_percent = (used_tokens * 100) / window_tokens.max(1);
    let item_count = input_items.len();
    if used_percent >= watermark_percent {
        let evt = json!({
            "role": "user",
            "content": format!(
                "<context-event kind=\"context.watermark\" used_percent=\"{used_percent}\" active_items=\"{item_count}\">\nContext window watermark reached ({used_tokens} / {window_tokens} est. tokens, {item_count} active items). At your next coherent milestone, you can either:\n1. Call `context_edit` (`drop: [\"from-to\"]` or `replace: [{{\"range\": \"from-to\", \"with\": \"1-line note\"}}]`) to surgically prune obsolete turns or compress verbose tool outputs without writing a full summary (`Context Language Models`), OR\n2. Call `context_window_open` with a structured `handoff` (`Goal`, `Verified Facts`, `Decisions`, `Next Action`) to open a fresh window.\n100% of raw turns remain searchable via `memory_recall`.\n</context-event>"
            ),
        });
        append_transcript_message(transcript_path, &evt);
        input_items.push(evt);
    }
}

pub(super) fn append_transcript_message(transcript_path: &Path, msg: &Value) {
    let Ok(line) = serde_json::to_string(msg) else {
        return;
    };
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(transcript_path)
    {
        Ok(mut f) => {
            let _ = writeln!(f, "{line}");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                if let Ok(meta) = f.metadata()
                    && (meta.permissions().mode() & 0o060) != 0o060
                {
                    let _ = f.set_permissions(std::fs::Permissions::from_mode(0o660));
                }
            }
        }
        #[cfg(unix)]
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            use std::os::unix::fs::MetadataExt as _;
            use std::process::{Command, Stdio};
            let meta = std::fs::metadata(transcript_path).or_else(|_| {
                transcript_path
                    .parent()
                    .ok_or_else(|| std::io::Error::other("missing parent"))
                    .and_then(std::fs::metadata)
            });
            let launcher = std::env::var_os("ANYAGENTS_LAUNCHER")
                .or_else(|| std::env::var_os("ANYAGENTS_SETPRIV"))
                .filter(|p| !p.is_empty())
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("/usr/local/bin/anyagents-setpriv"));
            if let Ok(meta) = meta
                && launcher.is_file()
            {
                let uid = meta.uid().to_string();
                let gid = meta.gid().to_string();
                if let Ok(mut child) = Command::new(&launcher)
                    .args([
                        "--reuid",
                        &uid,
                        "--regid",
                        &gid,
                        "--groups",
                        &gid,
                        "--no-new-privs",
                        "--",
                        "sh",
                        "-c",
                        "umask 007 && cat >> \"$1\" && chmod 0660 \"$1\"",
                        "sh",
                    ])
                    .arg(transcript_path)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    if let Some(mut stdin) = child.stdin.take() {
                        let _ = writeln!(stdin, "{line}");
                    }
                    let _ = child.wait();
                }
            }
        }
        Err(_) => {}
    }
}

#[derive(Clone, Debug)]
struct RawTranscriptItem {
    index: usize,
    raw: Value,
    rendered: String,
    is_tool: bool,
    tool_name: String,
    role: String,
}

fn recall_item_content(item: &RawTranscriptItem, active: &[(usize, Value)]) -> String {
    let verbatim_active = active.iter().any(|(idx, value)| {
        value == &item.raw
            || (item.raw.get("type").and_then(Value::as_str) == Some("context_window_open")
                && *idx == item.index)
            || (value.get("type").and_then(Value::as_str) == Some("function_call_output")
                && item.raw.get("role").and_then(Value::as_str) == Some("tool")
                && value.get("call_id") == item.raw.get("tool_call_id")
                && value.get("output") == item.raw.get("content"))
    });
    if verbatim_active {
        format!(
            "[Already present in active context; verbatim replay omitted. Raw pointer: transcript:{0}-{0}. Evict this item before recalling its full contents.]",
            item.index
        )
    } else {
        item.rendered.clone()
    }
}

fn read_all_raw_transcript_items(transcript_path: &Path) -> Vec<RawTranscriptItem> {
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return Vec::new();
    };
    let mut call_id_to_tool: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut out = Vec::new();
    for (idx, line) in BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .enumerate()
    {
        let Ok(val) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let mut is_tool = false;
        let mut tool_name = String::new();
        let mut role = val
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("item")
            .to_ascii_lowercase();

        if let Some(t) = val.get("type").and_then(Value::as_str) {
            match t {
                "function_call" => {
                    is_tool = true;
                    role = "tool".to_string();
                    tool_name = val
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if let Some(cid) = val.get("call_id").and_then(Value::as_str)
                        && !tool_name.is_empty()
                    {
                        call_id_to_tool.insert(cid.to_string(), tool_name.clone());
                    }
                }
                "function_call_output" => {
                    is_tool = true;
                    role = "tool".to_string();
                    if let Some(cid) = val.get("call_id").and_then(Value::as_str)
                        && let Some(tname) = call_id_to_tool.get(cid)
                    {
                        tool_name = tname.clone();
                    }
                }
                "message" => {
                    role = "assistant".to_string();
                }
                "context_window_open" => {
                    role = "system".to_string();
                }
                _ => {}
            }
        } else if role == "tool" {
            is_tool = true;
            if let Some(cid) = val.get("tool_call_id").and_then(Value::as_str)
                && let Some(tname) = call_id_to_tool.get(cid)
            {
                tool_name = tname.clone();
            }
        } else if role == "assistant"
            && let Some(tcs) = val.get("tool_calls").and_then(Value::as_array)
        {
            for tc in tcs {
                if let (Some(cid), Some(fname)) = (
                    tc.get("id").and_then(Value::as_str),
                    tc.pointer("/function/name").and_then(Value::as_str),
                ) {
                    let lower = fname.to_ascii_lowercase();
                    call_id_to_tool.insert(cid.to_string(), lower.clone());
                    if tool_name.is_empty() {
                        tool_name = lower;
                    }
                }
            }
            if val
                .get("content")
                .and_then(Value::as_str)
                .is_none_or(|s| s.trim().is_empty())
            {
                is_tool = true;
            }
        }

        let rendered = render_transcript_item_text(&val, &tool_name);
        out.push(RawTranscriptItem {
            index: idx,
            raw: val,
            rendered,
            is_tool,
            tool_name,
            role,
        });
    }
    out
}

fn render_transcript_item_text(val: &Value, linked_tool_name: &str) -> String {
    if let Some(t) = val.get("type").and_then(Value::as_str) {
        match t {
            "context_window_open" => {
                let h = val.get("handoff").and_then(Value::as_str).unwrap_or("");
                return format!("[context_window_open] {h}");
            }
            "function_call" => {
                let name = val.get("name").and_then(Value::as_str).unwrap_or("");
                let args = val.get("arguments").and_then(Value::as_str).unwrap_or("");
                return format!("[tool_call:{name}] {args}");
            }
            "function_call_output" => {
                let cid = val.get("call_id").and_then(Value::as_str).unwrap_or("");
                let output = val.get("output").and_then(Value::as_str).unwrap_or("");
                if !linked_tool_name.is_empty() {
                    return format!("[tool_output:{linked_tool_name}:{cid}] {output}");
                }
                return format!("[tool_output:{cid}] {output}");
            }
            "message" => {
                if let Some(parts) = val.get("content").and_then(Value::as_array) {
                    let joined: Vec<&str> = parts
                        .iter()
                        .filter_map(|p| p.get("text").and_then(Value::as_str))
                        .collect();
                    return format!("[assistant] {}", joined.join("\n"));
                }
            }
            _ => {}
        }
    }
    let role = val.get("role").and_then(Value::as_str).unwrap_or("item");
    let content = val.get("content").and_then(Value::as_str).unwrap_or("");
    if !content.is_empty() {
        format!("[{role}] {content}")
    } else {
        serde_json::to_string(val).unwrap_or_default()
    }
}

fn is_cjk_char(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
    )
}

/// Hybrid ASCII + CJK tokenizer: emits lowercased alphanumeric/identifier tokens
/// (`len >= 2`, plus sub-tokens split on `_` and `-`) and individual CJK characters +
/// CJK bigrams so English, code, and Cantonese/Chinese queries achieve high BM25 recall
/// without external embeddings.
fn tokenize_hybrid_raw_rag(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut ascii_buf = String::new();
    let mut cjk_run: Vec<char> = Vec::new();

    let flush_ascii = |buf: &mut String, out: &mut Vec<String>| {
        let trimmed = buf.trim_matches(['-', '_']);
        if trimmed.len() >= 2 {
            let lower = trimmed.to_ascii_lowercase();
            out.push(lower.clone());
            if lower.contains('_') || lower.contains('-') {
                for part in lower.split(['_', '-']) {
                    if part.len() >= 2 {
                        out.push(part.to_string());
                    }
                }
            }
        }
        buf.clear();
    };
    let flush_cjk = |run: &mut Vec<char>, out: &mut Vec<String>| {
        for &ch in run.iter() {
            out.push(ch.to_string());
        }
        for pair in run.windows(2) {
            let mut s = String::with_capacity(6);
            s.push(pair[0]);
            s.push(pair[1]);
            out.push(s);
        }
        run.clear();
    };

    for ch in text.chars() {
        if is_cjk_char(ch) {
            flush_ascii(&mut ascii_buf, &mut tokens);
            cjk_run.push(ch);
        } else if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            flush_cjk(&mut cjk_run, &mut tokens);
            ascii_buf.push(ch);
        } else {
            flush_ascii(&mut ascii_buf, &mut tokens);
            flush_cjk(&mut cjk_run, &mut tokens);
        }
    }
    flush_ascii(&mut ascii_buf, &mut tokens);
    flush_cjk(&mut cjk_run, &mut tokens);
    tokens
}

pub(super) fn execute_context_budget_read(transcript_path: &Path) -> String {
    let raw_items = read_all_raw_transcript_items(transcript_path);
    let active_indexed = load_transcript_indexed_items(transcript_path);
    let active_items: Vec<Value> = active_indexed.iter().map(|(_, v)| v.clone()).collect();
    let (window_tokens, _, watermark_percent) =
        resolve_context_window_for_transcript(transcript_path);
    let used_tokens = estimate_items_tokens(&active_items);
    let remaining_tokens = window_tokens.saturating_sub(used_tokens);
    let used_percent = (used_tokens * 100) / window_tokens.max(1);
    let remaining_percent = 100usize.saturating_sub(used_percent);
    let handoff_due = used_percent >= watermark_percent;
    let windows_opened = raw_items
        .iter()
        .filter(|item| item.raw.get("type").and_then(Value::as_str) == Some("context_window_open"))
        .count();
    let edits_applied = raw_items
        .iter()
        .filter(|item| item.raw.get("type").and_then(Value::as_str) == Some("context_edit"))
        .count();

    serde_json::to_string_pretty(&json!({
        "window_tokens": window_tokens,
        "used_tokens": used_tokens,
        "remaining_tokens": remaining_tokens,
        "used_context_percent": used_percent,
        "remaining_context_percent": remaining_percent,
        "handoff_threshold_percent": watermark_percent,
        "handoff_due": handoff_due,
        "active_window_items": active_items.len(),
        "total_raw_transcript_items": raw_items.len(),
        "windows_opened": windows_opened,
        "context_edits_applied": edits_applied,
    }))
    .unwrap_or_default()
}

/// Agent-controlled surgical context editing (`Context Language Models`, Meta FAIR / UW / MIT,
/// `arXiv:2609.37725`): lets the agent inspect active context items by index/token weight,
/// drop obsolete turns (`drop` / `keep_only`), or replace verbose tool outputs in-place with
/// 1-line notes (`replace` / `mask`) without writing a full summary, while keeping 100% of
/// raw history intact in `sylphx-transcript.jsonl` for `memory_recall`.
pub(super) fn execute_context_edit(transcript_path: &Path, args: &Value) -> String {
    let before_indexed = load_transcript_indexed_items(transcript_path);
    let before_items: Vec<Value> = before_indexed.iter().map(|(_, v)| v.clone()).collect();
    let tokens_before = estimate_items_tokens(&before_items);

    let has_drop = args
        .get("drop")
        .is_some_and(|v| !v.is_null() && v.as_array().is_none_or(|a| !a.is_empty()));
    let has_keep = args
        .get("keep_only")
        .is_some_and(|v| !v.is_null() && v.as_array().is_none_or(|a| !a.is_empty()));
    let has_replace = args
        .get("replace")
        .or_else(|| args.get("mask"))
        .is_some_and(|v| v.as_array().is_some_and(|a| !a.is_empty()));
    let action = args
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or(if has_drop || has_keep || has_replace {
            "edit"
        } else {
            "inspect"
        })
        .trim()
        .to_ascii_lowercase();

    if action == "inspect" || (!has_drop && !has_keep && !has_replace) {
        let mut item_summaries = Vec::with_capacity(before_indexed.len());
        let mut hogs: Vec<(usize, usize, String, String)> = Vec::new();
        for (raw_idx, item) in &before_indexed {
            let serialized = serde_json::to_string(item).unwrap_or_default();
            let est_tok = serialized.len() / 4;
            let kind = item
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| item.get("role").and_then(Value::as_str))
                .unwrap_or("item")
                .to_string();
            let text_preview = render_transcript_item_text(item, "");
            let mut cut = 120.min(text_preview.len());
            while cut > 0 && !text_preview.is_char_boundary(cut) {
                cut -= 1;
            }
            let preview = if cut < text_preview.len() {
                format!("{}...", &text_preview[..cut])
            } else {
                text_preview
            };
            item_summaries.push(json!({
                "index": raw_idx,
                "kind": kind,
                "est_tokens": est_tok,
                "preview": preview,
            }));
            hogs.push((est_tok, *raw_idx, kind, preview));
        }
        hogs.sort_by_key(|a| std::cmp::Reverse(a.0));
        let top_hogs: Vec<Value> = hogs
            .into_iter()
            .take(10)
            .map(|(tok, idx, kind, prev)| {
                json!({
                    "index": idx,
                    "kind": kind,
                    "est_tokens": tok,
                    "preview": prev,
                })
            })
            .collect();
        return serde_json::to_string_pretty(&json!({
            "mode": "inspect",
            "active_items": before_indexed.len(),
            "used_tokens": tokens_before,
            "top_token_hogs": top_hogs,
            "items": item_summaries,
        }))
        .unwrap_or_default();
    }

    let edit_event = json!({
        "type": "context_edit",
        "drop": args.get("drop").cloned().unwrap_or(Value::Null),
        "keep_only": args.get("keep_only").cloned().unwrap_or(Value::Null),
        "replace": args.get("replace").or_else(|| args.get("mask")).cloned().unwrap_or(Value::Null),
        "timestamp_ms": anyagents_core::now_ms(),
    });
    append_transcript_message(transcript_path, &edit_event);

    let after_items = load_transcript_input_items(transcript_path);
    let tokens_after = estimate_items_tokens(&after_items);
    let tokens_freed = tokens_before.saturating_sub(tokens_after);

    serde_json::to_string_pretty(&json!({
        "status": "edited",
        "active_items_before": before_items.len(),
        "active_items_after": after_items.len(),
        "tokens_before": tokens_before,
        "tokens_after": tokens_after,
        "tokens_freed": tokens_freed,
        "raw_recall_preserved": true,
    }))
    .unwrap_or_default()
}

/// Lossless `Raw-RAG` lexical + structured recall over the full append-only
/// `sylphx-transcript.jsonl` (`arXiv:2610.30755` + `arXiv:2310.10971`):
/// - Zero lossy ingestion transformation (`ADD`-only raw log, zero vector embeddings).
/// - Supports multi-query (`query` + `queries`), negative terms (`exclude`), tool history
///   toggles (`include_tools`, `only_tools`, `tool_name`), `role` filter, ordering (`sort`:
///   `"relevance"`, `"newest"`, `"oldest"`), exact range coordinates (`range`), and
///   contiguous episode expansion (`radius`).
// Bound the final JSON bytes, including escaping and pagination metadata. A
// page is a fragment of the original JSON document; concatenating `content`
// across cursors recovers it exactly. Snapshot coordinates make later tool
// receipts and compactions invisible to an in-flight continuation.
pub(super) const MEMORY_RECALL_MAX_BYTES: usize = 32_768;

pub(super) fn execute_memory_recall(transcript_path: &Path, args: &Value) -> String {
    let all_items = read_all_raw_transcript_items(transcript_path);
    let default_snapshot = all_items.last().map_or(0, |item| item.index + 1);
    let (snapshot, offset) = match args.get("cursor").and_then(Value::as_str) {
        None => (default_snapshot, 0),
        Some(cursor) => match cursor.split_once(':').and_then(|(a, b)| Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()?))) {
            Some((snapshot, offset)) if snapshot <= default_snapshot => (snapshot, offset),
            _ => return "error: invalid memory_recall cursor; reuse next_cursor with the original arguments".into(),
        },
    };
    let all_items: Vec<_> = all_items
        .into_iter()
        .take_while(|item| item.index < snapshot)
        .collect();
    let active = load_transcript_indexed_items_before(transcript_path, snapshot);
    let output = render_memory_recall(&all_items, &active, args);
    if offset == 0 && output.len() <= MEMORY_RECALL_MAX_BYTES {
        return output;
    }
    if offset >= output.len() || !output.is_char_boundary(offset) {
        return "error: memory_recall cursor offset is outside the result; reuse next_cursor with the original arguments".into();
    }
    let page = |end: usize| {
        serde_json::to_string(&json!({
            "mode": "page",
            "format": "json_fragment",
            "content": &output[offset..end],
            "next_cursor": if end < output.len() { Some(format!("{snapshot}:{end}")) } else { None },
            "continuation": "Repeat the original arguments with next_cursor. Concatenate content fragments to recover the JSON result.",
        })).unwrap_or_default()
    };
    let mut end = (offset + (MEMORY_RECALL_MAX_BYTES - 512) / 6).min(output.len());
    while !output.is_char_boundary(end) {
        end -= 1;
    }
    // Every UTF-8 byte needs at most six JSON bytes (e.g. a control byte).
    // The conservative chunk bound avoids repeatedly serializing huge results.
    let result = page(end);
    debug_assert!(result.len() <= MEMORY_RECALL_MAX_BYTES);
    result
}

fn render_memory_recall(
    all_items: &[RawTranscriptItem],
    active: &[(usize, Value)],
    args: &Value,
) -> String {
    if all_items.is_empty() {
        return serde_json::to_string_pretty(&json!({
            "mode": "empty",
            "total_raw_items": 0,
            "hits": [],
        }))
        .unwrap_or_default();
    }

    let tool_filter = args
        .get("tool_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase);
    let only_tools = args
        .get("only_tools")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || tool_filter.is_some();
    let include_tools = args
        .get("include_tools")
        .and_then(Value::as_bool)
        .unwrap_or(true)
        || only_tools;
    let role_filter = args
        .get("role")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
        .map(str::to_ascii_lowercase);
    let exclude_lower: Vec<String> = args
        .get("exclude")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let sort_mode = args
        .get("sort")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "relevance".to_string());

    let item_passes_filters = |item: &RawTranscriptItem| -> bool {
        if only_tools && !item.is_tool {
            return false;
        }
        if !include_tools && item.is_tool {
            return false;
        }
        if let Some(ref wanted_tool) = tool_filter
            && item.tool_name != *wanted_tool
        {
            return false;
        }
        if let Some(ref wanted_role) = role_filter
            && item.role != *wanted_role
        {
            return false;
        }
        if !exclude_lower.is_empty() {
            let text_l = item.rendered.to_lowercase();
            if exclude_lower.iter().any(|ex| text_l.contains(ex)) {
                return false;
            }
        }
        true
    };

    if let Some(range_raw) = args
        .get("range")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let stripped = range_raw
            .strip_prefix("transcript:")
            .unwrap_or(range_raw)
            .trim();
        let (from_idx, to_idx) = if let Some((a, b)) = stripped.split_once('-') {
            (
                a.trim().parse::<usize>().unwrap_or(0),
                b.trim().parse::<usize>().unwrap_or(0),
            )
        } else {
            let single = stripped.parse::<usize>().unwrap_or(0);
            (single, single)
        };
        let end_clamped = to_idx.min(all_items.len().saturating_sub(1));
        let start_clamped = from_idx.min(end_clamped);
        let slice: Vec<Value> = all_items[start_clamped..=end_clamped]
            .iter()
            .filter(|item| item_passes_filters(item))
            .map(|item| {
                json!({
                    "index": item.index,
                    "role": item.role,
                    "tool_name": if item.tool_name.is_empty() { Value::Null } else { Value::String(item.tool_name.clone()) },
                    "content": recall_item_content(item, active),
                })
            })
            .collect();
        return serde_json::to_string_pretty(&json!({
            "mode": "range",
            "range": format!("transcript:{start_clamped}-{end_clamped}"),
            "total_raw_items": all_items.len(),
            "items": slice,
        }))
        .unwrap_or_default();
    }

    let mut all_queries: Vec<String> = Vec::new();
    if let Some(q) = args
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        all_queries.push(q.to_string());
    }
    if let Some(q_arr) = args.get("queries").and_then(Value::as_array) {
        for qv in q_arr.iter().filter_map(Value::as_str) {
            let trimmed = qv.trim();
            if !trimmed.is_empty() && !all_queries.iter().any(|existing| existing == trimmed) {
                all_queries.push(trimmed.to_string());
            }
        }
    }

    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(5)
        .clamp(1, 50) as usize;
    let radius = args
        .get("radius")
        .and_then(Value::as_u64)
        .unwrap_or(2)
        .clamp(0, 10) as usize;

    let filtered_items: Vec<&RawTranscriptItem> = all_items
        .iter()
        .filter(|item| item_passes_filters(item))
        .collect();

    if all_queries.is_empty() {
        if !only_tools
            && include_tools
            && role_filter.is_none()
            && exclude_lower.is_empty()
            && args.get("sort").is_none()
        {
            return "error: `memory_recall` requires `query`, `queries`, `range` (`transcript:<from>-<to>`), or a structured filter (`only_tools`, `tool_name`, `include_tools: false`, `role`)".to_string();
        }
        let mut iter_items: Vec<&RawTranscriptItem> = filtered_items;
        if sort_mode != "oldest" && sort_mode != "asc" {
            iter_items.reverse();
        }
        let items_json: Vec<Value> = iter_items
            .into_iter()
            .take(limit)
            .map(|item| {
                json!({
                    "index": item.index,
                    "role": item.role,
                    "tool_name": if item.tool_name.is_empty() { Value::Null } else { Value::String(item.tool_name.clone()) },
                    "content": recall_item_content(item, active),
                })
            })
            .collect();
        return serde_json::to_string_pretty(&json!({
            "mode": "filter",
            "total_raw_items": all_items.len(),
            "hits": items_json,
        }))
        .unwrap_or_default();
    }

    if filtered_items.is_empty() {
        return serde_json::to_string_pretty(&json!({
            "mode": "query",
            "query": all_queries.join(" | "),
            "total_raw_items": all_items.len(),
            "hits": [],
        }))
        .unwrap_or_default();
    }

    // Build overlapping episode windows over `filtered_items` so tool call/output pairs stay together.
    let mut windows: Vec<(usize, usize, String, Vec<String>)> = Vec::new();
    let window_size = if only_tools || !include_tools {
        2usize
    } else {
        3usize
    };
    let stride = if window_size > 1 { 2usize } else { 1usize };
    let mut start = 0usize;
    while start < filtered_items.len() {
        let end = (start + window_size - 1).min(filtered_items.len() - 1);
        let raw_start_idx = filtered_items[start].index;
        let raw_end_idx = filtered_items[end].index;
        let combined = filtered_items[start..=end]
            .iter()
            .map(|item| format!("#{}: {}", item.index, item.rendered))
            .collect::<Vec<_>>()
            .join("\n");
        let toks = tokenize_hybrid_raw_rag(&combined);
        windows.push((raw_start_idx, raw_end_idx, combined, toks));
        if end + 1 >= filtered_items.len() {
            break;
        }
        start += stride;
    }

    let mut q_tokens: Vec<String> = Vec::new();
    for q in &all_queries {
        for tok in tokenize_hybrid_raw_rag(q) {
            if !q_tokens.contains(&tok) {
                q_tokens.push(tok);
            }
        }
    }
    let queries_lower: Vec<String> = all_queries.iter().map(|q| q.to_lowercase()).collect();
    let n_docs = windows.len().max(1) as f64;
    let avg_dl =
        (windows.iter().map(|(_, _, _, t)| t.len()).sum::<usize>() as f64 / n_docs).max(1.0);

    let mut df: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for q in &q_tokens {
        let count = windows
            .iter()
            .filter(|(_, _, _, toks)| toks.iter().any(|t| t == q))
            .count();
        df.insert(q.as_str(), count);
    }

    let mut scored: Vec<(f64, usize, usize)> = Vec::new();
    for (start_idx, end_idx, combined, toks) in &windows {
        let mut bm25 = 0.0f64;
        let mut matched_unique = 0usize;
        let dl = toks.len() as f64;
        for q in &q_tokens {
            let tf = toks.iter().filter(|t| *t == q).count() as f64;
            if tf > 0.0 {
                matched_unique += 1;
                let doc_freq = *df.get(q.as_str()).unwrap_or(&0) as f64;
                let idf = ((n_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
                let k1 = 1.2;
                let b = 0.75;
                let norm_tf = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avg_dl)));
                bm25 += idf * norm_tf;
            }
        }
        let combined_lower = combined.to_lowercase();
        let exact_matches = queries_lower
            .iter()
            .filter(|ql| !ql.is_empty() && combined_lower.contains(ql.as_str()))
            .count();
        let exact_boost = (exact_matches as f64) * 3.0;
        let coverage = if !q_tokens.is_empty() {
            matched_unique as f64 / q_tokens.len() as f64
        } else {
            0.0
        };
        let total_score = bm25 + exact_boost + coverage * 1.5;
        if total_score > 0.0 {
            scored.push((total_score, *start_idx, *end_idx));
        }
    }

    match sort_mode.as_str() {
        "newest" | "desc" | "recent" => {
            scored.sort_by(|a, b| {
                b.2.cmp(&a.2)
                    .then_with(|| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal))
            });
        }
        "oldest" | "asc" | "chronological" => {
            scored.sort_by(|a, b| {
                a.1.cmp(&b.1)
                    .then_with(|| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal))
            });
        }
        _ => {
            scored.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.2.cmp(&a.2))
            });
        }
    }

    let mut hits = Vec::new();
    let mut covered_ranges: Vec<(usize, usize)> = Vec::new();
    let max_idx = all_items.len().saturating_sub(1);

    for (score, w_start, w_end) in scored {
        if hits.len() >= limit {
            break;
        }
        let exp_start = w_start.saturating_sub(radius);
        let exp_end = (w_end + radius).min(max_idx);
        if covered_ranges
            .iter()
            .any(|(cs, ce)| exp_start >= *cs && exp_end <= *ce)
        {
            continue;
        }
        covered_ranges.push((exp_start, exp_end));
        let episode_items: Vec<Value> = all_items[exp_start..=exp_end]
            .iter()
            .filter(|item| item_passes_filters(item))
            .map(|item| {
                let idx = item.index;
                let content = recall_item_content(item, active);
                let text = &content;
                let clipped = if text.len() > 2400 {
                    let mut end_b = 2400;
                    while !text.is_char_boundary(end_b) {
                        end_b -= 1;
                    }
                    format!(
                        "{}...[use range=\"transcript:{idx}-{idx}\" for full item]",
                        &text[..end_b]
                    )
                } else {
                    text.clone()
                };
                json!({
                    "index": idx,
                    "role": item.role,
                    "tool_name": if item.tool_name.is_empty() { Value::Null } else { Value::String(item.tool_name.clone()) },
                    "content": clipped,
                })
            })
            .collect();
        hits.push(json!({
            "range": format!("transcript:{exp_start}-{exp_end}"),
            "matched_window": format!("transcript:{w_start}-{w_end}"),
            "score": (score * 1000.0).round() / 1000.0,
            "items": episode_items,
        }));
    }

    serde_json::to_string_pretty(&json!({
        "mode": "query",
        "query": all_queries.join(" | "),
        "total_raw_items": all_items.len(),
        "hits": hits,
    }))
    .unwrap_or_default()
}

#[cfg(test)]
mod projection_tests {
    use super::*;

    fn recall_pages(path: &Path, mut args: Value) -> String {
        let mut combined = String::new();
        loop {
            let page = execute_memory_recall(path, &args);
            assert!(page.len() <= MEMORY_RECALL_MAX_BYTES);
            let parsed: Value = serde_json::from_str(&page).unwrap();
            if parsed["mode"] != "page" {
                assert!(combined.is_empty());
                return page;
            }
            combined.push_str(parsed["content"].as_str().unwrap());
            let Some(cursor) = parsed["next_cursor"].as_str() else {
                break;
            };
            // New receipts must not shift the result represented by a cursor.
            append_transcript_message(path, &json!({"role":"user", "content":"later receipt"}));
            args["cursor"] = json!(cursor);
        }
        combined
    }

    #[test]
    fn recall_bounds_all_modes_and_recovers_evicted_utf8_output_losslessly() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("sylphx-transcript.jsonl");
        let text = "large needle 中文\n\t\"\\".repeat(4_000);
        let result = json!({"type":"function_call_output", "call_id":"huge", "output":text});
        append_transcript_message(
            &path,
            &json!({"type":"function_call", "call_id":"huge", "name":"bash", "arguments":"{}"}),
        );
        append_transcript_message(&path, &result);
        append_transcript_message(
            &path,
            &json!({"type":"context_window_open", "handoff":"continue", "retained_items":[
                {"type":"function_call", "call_id":"huge", "name":"bash", "arguments":"{}"}, result
            ]}),
        );
        let active = execute_memory_recall(&path, &json!({"range":"transcript:1-1"}));
        assert!(active.contains("Already present in active context"));
        assert!(!active.contains("large needle"));
        // An evicted result's original raw coordinate must remain recoverable,
        // even when its call/output IDs are retained with a pointer instead.
        append_transcript_message(
            &path,
            &json!({"type":"context_window_open", "handoff":"continue", "retained_items":[
                {"type":"function_call", "call_id":"huge", "name":"bash", "arguments":"{}"},
                {"type":"function_call_output", "call_id":"huge", "output":"raw pointer transcript:1-1"}
            ]}),
        );
        for args in [
            json!({"range":"transcript:1-1"}),
            json!({"only_tools":true, "sort":"oldest", "limit":2}),
        ] {
            let recovered: Value = serde_json::from_str(&recall_pages(&path, args)).unwrap();
            let items = recovered
                .get("items")
                .or_else(|| recovered.get("hits"))
                .unwrap()
                .as_array()
                .unwrap();
            let output = items.iter().find(|item| item["index"] == 1).unwrap();
            assert_eq!(output["content"], format!("[tool_output:bash:huge] {text}"));
        }
        // Many individually clipped query episodes must also share the same
        // final-byte ceiling, including JSON escaping and cursor metadata.
        for n in 0..100 {
            append_transcript_message(
                &path,
                &json!({"role":"assistant", "content":format!("needle {n}: {}", "x".repeat(2500))}),
            );
        }
        append_transcript_message(
            &path,
            &json!({"type":"context_window_open", "handoff":"done", "retained_items":[]}),
        );
        let query =
            execute_memory_recall(&path, &json!({"query":"needle", "limit":50, "radius":10}));
        assert!(query.len() <= MEMORY_RECALL_MAX_BYTES);
        assert_eq!(
            serde_json::from_str::<Value>(&query).unwrap()["mode"],
            "page"
        );
        let recovered: Value = serde_json::from_str(&recall_pages(
            &path,
            json!({"query":"needle", "limit":50, "radius":10}),
        ))
        .unwrap();
        assert!(!recovered["hits"].as_array().unwrap().is_empty());
    }

    fn call(id: usize) -> Value {
        json!({"type": "function_call", "call_id": format!("call_{id}"),
            "name": "read_file", "arguments": "{\"path\":\"README.md\"}"})
    }

    fn output(id: usize) -> Value {
        json!({"type": "function_call_output", "call_id": format!("call_{id}"),
            "output": format!("already completed read {id}")})
    }

    fn values(history: &[(usize, Value)]) -> Vec<Value> {
        history.iter().map(|(_, item)| item.clone()).collect()
    }

    #[test]
    fn saved_parallel_call_shape_survives_real_request_builder_without_history_write() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("transcript.jsonl");
        // Saved post-anchor/edit population: 5 * WINDOW_STRIDE_ITEMS + 32 items;
        // the old stride dropped 4 * WINDOW_STRIDE_ITEMS, leaving calls across the cut.
        let cut_point = 4 * WINDOW_STRIDE_ITEMS;
        let total_len = 5 * WINDOW_STRIDE_ITEMS + 32;
        let mut raw: Vec<Value> = (0..(cut_point - 1))
            .map(|i| json!({"role": "user", "content": format!("message {i}")}))
            .collect();
        raw.extend([call(0), call(1), call(2), output(0), output(1), output(2)]);
        raw.extend(
            ((cut_point + 5)..total_len)
                .map(|i| json!({"role": "user", "content": format!("message {i}")})),
        );
        assert_eq!(raw.len(), total_len);
        assert!(validate_responses_input(&raw[cut_point..]).is_err());
        for item in raw {
            append_transcript_message(&path, &item);
        }
        let before = std::fs::read(&path).unwrap();
        let projected = load_transcript_input_items(&path);
        assert_eq!(projected.len(), WINDOW_STRIDE_ITEMS + 33);
        assert_eq!(projected[0], call(0));
        let request = crate::build_native_responses_request(
            "unchanged-model",
            "instructions",
            &projected,
            &[],
            "same-session",
            Some("high"),
        )
        .unwrap();
        assert_eq!(request["input"], json!(projected));
        assert_eq!(request["prompt_cache_key"], "same-session");
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn every_cut_preserves_transitively_overlapping_pairs_and_open_calls() {
        // A closes before B but B starts earlier: extending over A exposes B.
        let history: Vec<_> = [call(1), call(0), output(0), output(1), call(2)]
            .into_iter()
            .enumerate()
            .collect();
        assert_eq!(paired_suffix_start(&history, 3), 0);
        for cut in 0..=history.len() {
            let start = paired_suffix_start(&history, cut);
            let projected = values(&history[start..]);
            validate_responses_input(&projected).unwrap();
            if cut < history.len() {
                assert_eq!(projected.last(), history.last().map(|(_, item)| item));
            }
        }
    }

    #[test]
    fn generated_windows_and_context_edits_preserve_pairing_and_exact_retained_results() {
        // Deterministic exhaustive property check over batch width, interleaved
        // messages, every cut, many max/stride sizes, drop/keep_only/mask edits.
        for width in 1..=8 {
            let mut history = Vec::new();
            for batch in 0..4 {
                for offset in 0..width {
                    history.push(call(batch * width + offset));
                }
                history.push(json!({"role": "user", "content": "interleaved steering"}));
                for offset in (0..width).rev() {
                    history.push(output(batch * width + offset));
                }
            }
            let history: Vec<_> = history.into_iter().enumerate().collect();
            for selected in 0..history.len() {
                for edit in [
                    json!({"drop": [selected]}),
                    json!({"keep_only": [selected]}),
                    json!({"replace": [{"range": selected, "with": "compressed actual result"}]}),
                ] {
                    let mut edited = history.clone();
                    apply_context_edit_to_history(&mut edited, &edit);
                    validate_responses_input(&values(&edited)).unwrap();
                    for cut in 0..=edited.len() {
                        let start = paired_suffix_start(&edited, cut);
                        let projected = values(&edited[start..]);
                        crate::build_native_responses_request("m", "s", &projected, &[], "k", None)
                            .unwrap();
                        // Suffix extension must not discard or alter an actual
                        // completed result retained by the requested boundary.
                        assert_eq!(&projected[cut - start..], &values(&edited[cut..]));
                    }
                    for max in 1..=12 {
                        for stride in 1..=max {
                            if edited.len() > max {
                                let cut = ((edited.len() - stride) / stride) * stride;
                                let start = paired_suffix_start(&edited, cut);
                                validate_responses_input(&values(&edited[start..])).unwrap();
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn edits_inside_a_batch_pair_later_outputs_but_keep_future_messages() {
        for edit in [json!({"drop": [1]}), json!({"keep_only": [0]})] {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("batch.jsonl");
            let mut edit_call = call(0);
            edit_call["name"] = json!("mcp__anyagents__context_edit");
            for item in [
                edit_call.clone(),
                call(1),
                json!({"type": "context_edit", "drop": edit.get("drop"), "keep_only": edit.get("keep_only")}),
                output(0),
                output(1),
                json!({"role": "user", "content": "future message must remain"}),
            ] {
                append_transcript_message(&path, &item);
            }
            let before = std::fs::read(&path).unwrap();
            let projected = load_transcript_input_items(&path);
            assert_eq!(
                projected,
                vec![
                    edit_call,
                    output(0),
                    json!({"role": "user", "content": "future message must remain"})
                ]
            );
            validate_responses_input(&projected).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[test]
    fn final_request_boundary_rejects_orphans_and_duplicate_results_without_repairing_raw() {
        for invalid in [
            vec![call(1), output(0), output(1)],
            vec![call(0), output(0), output(0)],
            vec![output(0), call(0)],
            vec![call(0), call(0)],
            vec![json!({"type": "function_call_output", "output": "private contents"})],
        ] {
            let before = invalid.clone();
            let error = crate::build_native_responses_request("m", "s", &invalid, &[], "k", None)
                .unwrap_err();
            assert!(error.contains("call_id"));
            assert!(!error.contains("private contents"));
            assert_eq!(invalid, before);
        }
        validate_responses_input(&[call(0)]).unwrap(); // Legitimately open, no replay.
    }

    #[test]
    fn legacy_parallel_tool_calls_share_a_raw_index_but_remain_paired() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("legacy.jsonl");
        for _ in 0..159 {
            append_transcript_message(&path, &json!({"role": "user", "content": "old"}));
        }
        append_transcript_message(
            &path,
            &json!({"role": "assistant", "tool_calls": [
                {"id": "a", "function": {"name": "read_file", "arguments": "{}"}},
                {"id": "b", "function": {"name": "read_file", "arguments": "{}"}}
            ]}),
        );
        for id in ["b", "a"] {
            append_transcript_message(
                &path,
                &json!({"role": "tool", "tool_call_id": id, "content": "done"}),
            );
        }
        validate_responses_input(&load_transcript_input_items(&path)).unwrap();
    }

    #[test]
    fn projection_drops_raw_orphan_outputs_without_mutating_transcript() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("orphan.jsonl");
        for item in [
            call(0),
            output(0),
            output(999),
            call(1),
            output(1),
            json!({"role": "user", "content": "continue"}),
        ] {
            append_transcript_message(&path, &item);
        }
        let before = std::fs::read(&path).unwrap();
        let projected = load_transcript_input_items(&path);
        assert_eq!(
            projected,
            vec![
                call(0),
                output(0),
                call(1),
                output(1),
                json!({"role": "user", "content": "continue"}),
            ]
        );
        validate_responses_input(&projected).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
