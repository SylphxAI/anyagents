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

fn read_all_raw_transcript_items(transcript_path: &Path) -> Vec<(usize, Value, String)> {
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (idx, line) in BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .enumerate()
    {
        let Ok(val) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let rendered = render_transcript_item_text(&val);
        out.push((idx, val, rendered));
    }
    out
}

fn render_transcript_item_text(val: &Value) -> String {
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
    let content = val
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_else(|| "");
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
/// (`len >= 2`) plus individual CJK characters and CJK bigrams so English, code,
/// and Cantonese/Chinese queries all achieve high BM25 recall without external embeddings.
fn tokenize_hybrid_raw_rag(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut ascii_buf = String::new();
    let mut cjk_run: Vec<char> = Vec::new();

    let flush_ascii = |buf: &mut String, out: &mut Vec<String>| {
        if buf.len() >= 2 {
            out.push(buf.to_ascii_lowercase());
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
        .filter(|(_, v, _)| v.get("type").and_then(Value::as_str) == Some("context_window_open"))
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

/// Lossless `Raw-RAG` recall over the full append-only `sylphx-transcript.jsonl`
/// (`arXiv:2610.30755` + `arXiv:2310.10971`):
/// - Zero lossy ingestion transformation (`ADD`-only raw log).
/// - Groups raw transcript items into overlapping episode windows so causal
///   `(Trigger -> FunctionCall -> FunctionOutput -> Outcome)` chains (`P(path | result)`)
///   are never severed across chunk boundaries.
/// - Scores windows with BM25 (`k1 = 1.2`, `b = 0.75`) + query term-coverage + exact
///   substring boost, and expands each top hit by `radius` neighbor items.
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
            .map(|(idx, _, text)| {
                json!({
                    "index": idx,
                    "content": text,
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

    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if query.is_empty() {
        return "error: `memory_recall` requires either `query` or `range` (`transcript:<from>-<to>`)".to_string();
    }

    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(5)
        .clamp(1, 20) as usize;
    let radius = args
        .get("radius")
        .and_then(Value::as_u64)
        .unwrap_or(2)
        .clamp(0, 10) as usize;

    // Build overlapping episode windows of up to 3 items with stride 2 (1-item overlap)
    // so `function_call -> function_call_output` causal pairs are never split.
    let mut windows: Vec<(usize, usize, String, Vec<String>)> = Vec::new();
    let window_size = 3usize;
    let stride = 2usize;
    let mut start = 0usize;
    while start < all_items.len() {
        let end = (start + window_size - 1).min(all_items.len() - 1);
        let combined = all_items[start..=end]
            .iter()
            .map(|(i, _, t)| format!("#{i}: {t}"))
            .collect::<Vec<_>>()
            .join("\n");
        let toks = tokenize_hybrid_raw_rag(&combined);
        windows.push((start, end, combined, toks));
        if end + 1 >= all_items.len() {
            break;
        }
        start += stride;
    }

    let q_tokens = tokenize_hybrid_raw_rag(query);
    let q_lower = query.to_ascii_lowercase();
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
        let exact_boost = if !q_lower.is_empty() && combined.to_ascii_lowercase().contains(&q_lower)
        {
            3.0
        } else {
            0.0
        };
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

    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.2.cmp(&a.2))
    });

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
            .map(|(idx, _, text)| {
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
        "query": query,
        "total_raw_items": all_items.len(),
        "hits": hits,
    }))
    .unwrap_or_default()
}
