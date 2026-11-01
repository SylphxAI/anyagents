//! Append-Only KV-Cache Pinned Context Window, Autonomous Compaction (`context_window_open`),
//! and Lossless `Raw-RAG` Hybrid BM25 + Contiguous Episode Recall (`memory_recall`).

use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde_json::{Value, json};

pub const DEFAULT_CONTEXT_WINDOW_TOKENS: usize = 128_000;
pub const CONTEXT_WATERMARK_PERCENT: usize = 75;
pub const WINDOW_MAX_ITEMS: usize = 160;
pub const WINDOW_STRIDE_ITEMS: usize = 80;

/// Loads persisted transcript items for `/v1/responses` (`input`), starting from
/// the most recent `context_window_open` anchor (so autonomous compaction starts a
/// clean KV-cacheable window while keeping 100% of raw history on disk for `memory_recall`),
/// and using block-aligned stride windowing (`WINDOW_STRIDE_ITEMS = 80`) instead of a
/// 1-by-1 sliding window so item 0 stays cache-pinned across 80 items at a time.
pub(super) fn load_transcript_input_items(transcript_path: &Path) -> Vec<Value> {
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return Vec::new();
    };
    let mut history = Vec::new();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(m) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if m.get("type").and_then(Value::as_str) == Some("context_window_open") {
            let handoff = m
                .get("handoff")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            history.clear();
            history.push(json!({
                "role": "user",
                "content": format!(
                    "<context-event kind=\"context.window_opened\">\n<handoff>\n{handoff}\n</handoff>\nPrior raw transcript entries remain indexed in full; call `memory_recall` with `query` or `range` (`transcript:<from>-<to>`) to inspect exact prior turns.\n</context-event>"
                ),
            }));
            continue;
        }
        if m.get("type").is_some() {
            history.push(m);
            continue;
        }
        match m.get("role").and_then(Value::as_str) {
            Some("user") => {
                history.push(m);
            }
            Some("assistant") => {
                if let Some(s) = m
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    history.push(json!({
                        "role": "assistant",
                        "content": s,
                    }));
                }
                if let Some(tcs) = m.get("tool_calls").and_then(Value::as_array) {
                    for tc in tcs {
                        if let (Some(cid), Some(fname)) = (
                            tc.get("id").and_then(Value::as_str),
                            tc.pointer("/function/name").and_then(Value::as_str),
                        ) {
                            let fargs = tc
                                .pointer("/function/arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}");
                            history.push(json!({
                                "type": "function_call",
                                "call_id": cid,
                                "name": fname,
                                "arguments": fargs,
                            }));
                        }
                    }
                }
            }
            Some("tool") => {
                if let Some(cid) = m.get("tool_call_id").and_then(Value::as_str) {
                    let out_str = m.get("content").and_then(Value::as_str).unwrap_or("");
                    history.push(json!({
                        "type": "function_call_output",
                        "call_id": cid,
                        "output": out_str,
                    }));
                }
            }
            _ => {}
        }
    }
    if history.len() > WINDOW_MAX_ITEMS {
        let excess = history.len() - WINDOW_STRIDE_ITEMS;
        let block_start = (excess / WINDOW_STRIDE_ITEMS) * WINDOW_STRIDE_ITEMS;
        history.drain(0..block_start);
        while history.first().is_some_and(|item| {
            item.get("type").and_then(Value::as_str) == Some("function_call_output")
                || item.get("type").and_then(Value::as_str) == Some("reasoning")
        }) {
            history.remove(0);
        }
    }
    history
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
    let used_tokens = estimate_items_tokens(input_items);
    let used_percent = (used_tokens * 100) / DEFAULT_CONTEXT_WINDOW_TOKENS.max(1);
    let item_count = input_items.len();
    if used_percent >= CONTEXT_WATERMARK_PERCENT || item_count >= 120 {
        let evt = json!({
            "role": "user",
            "content": format!(
                "<context-event kind=\"context.watermark\" used_percent=\"{used_percent}\" active_items=\"{item_count}\">\nContext window watermark reached ({used_tokens} / {DEFAULT_CONTEXT_WINDOW_TOKENS} est. tokens, {item_count} active items). At your next coherent milestone, call `context_window_open` with a structured `handoff` (`Goal`, `Verified Facts`, `Decisions`, `Next Action`) to open a fresh KV-cached window while keeping 100% of prior raw turns searchable via `memory_recall`.\n</context-event>"
            ),
        });
        append_transcript_message(transcript_path, &evt);
        input_items.push(evt);
    }
}

pub(super) fn append_transcript_message(transcript_path: &Path, msg: &Value) {
    if let Ok(line) = serde_json::to_string(msg)
        && let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(transcript_path)
    {
        let _ = writeln!(f, "{line}");
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
    let active_items = load_transcript_input_items(transcript_path);
    let used_tokens = estimate_items_tokens(&active_items);
    let remaining_tokens = DEFAULT_CONTEXT_WINDOW_TOKENS.saturating_sub(used_tokens);
    let used_percent = (used_tokens * 100) / DEFAULT_CONTEXT_WINDOW_TOKENS.max(1);
    let remaining_percent = 100usize.saturating_sub(used_percent);
    let handoff_due = used_percent >= CONTEXT_WATERMARK_PERCENT || active_items.len() >= 120;
    let windows_opened = raw_items
        .iter()
        .filter(|item| item.raw.get("type").and_then(Value::as_str) == Some("context_window_open"))
        .count();

    serde_json::to_string_pretty(&json!({
        "window_tokens": DEFAULT_CONTEXT_WINDOW_TOKENS,
        "used_tokens": used_tokens,
        "remaining_tokens": remaining_tokens,
        "used_context_percent": used_percent,
        "remaining_context_percent": remaining_percent,
        "handoff_threshold_percent": CONTEXT_WATERMARK_PERCENT,
        "handoff_due": handoff_due,
        "active_window_items": active_items.len(),
        "total_raw_transcript_items": raw_items.len(),
        "windows_opened": windows_opened,
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
pub(super) fn execute_memory_recall(transcript_path: &Path, args: &Value) -> String {
    let all_items = read_all_raw_transcript_items(transcript_path);
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
                    "content": item.rendered,
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
                    "content": item.rendered,
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
                let text = &item.rendered;
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
