//! Agent roster filtering, prioritization, compaction, and result-budget bounding.

use super::*;

pub(super) fn agent_matches_query(agent: &AgentView, query: &str) -> bool {
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    if tokens.is_empty() {
        return true;
    }
    let name = agent.name.to_ascii_lowercase();
    let id = agent.id.to_ascii_lowercase();
    let role = agent.role.as_deref().unwrap_or("").to_ascii_lowercase();
    let desc = agent
        .description
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    let workdir = agent.workdir.as_deref().unwrap_or("").to_ascii_lowercase();
    let harness = agent.harness.to_ascii_lowercase();
    let model = agent.model.as_deref().unwrap_or("").to_ascii_lowercase();
    let aliases: Vec<String> = agent
        .aliases
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    tokens.iter().all(|tok| {
        name.contains(tok)
            || id.contains(tok)
            || role.contains(tok)
            || desc.contains(tok)
            || workdir.contains(tok)
            || harness.contains(tok)
            || model.contains(tok)
            || aliases.iter().any(|al| al.contains(tok))
    })
}

pub(super) fn agent_list_priority(agent: &AgentView) -> u8 {
    let active_turn = matches!(agent.turn_state.as_str(), "working" | "in_turn");
    if agent.turn_state == "errored" || (agent.state == "running" && active_turn) {
        0
    } else if agent.state == "running" {
        1
    } else if agent.state == "idle" {
        2
    } else if agent.state == "paused" {
        3
    } else {
        4
    }
}

/// Retain either direct children of `root_id` (`include_descendants == false`)
/// or all causal descendants at every depth (`include_descendants == true`, via
/// breadth-first traversal across children, grandchildren, great-grandchildren, ...).
pub(super) fn retain_parent_or_descendants(
    agents: &mut Vec<AgentView>,
    root_id: &str,
    include_descendants: bool,
) {
    if !include_descendants {
        agents.retain(|a| a.parent.as_deref() == Some(root_id));
        return;
    }
    let mut children_by_parent: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    for a in agents.iter() {
        if let Some(pid) = a.parent.as_deref() {
            children_by_parent
                .entry(pid)
                .or_default()
                .push(a.id.as_str());
        }
    }
    let mut descendants: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::new();
    queue.push_back(root_id);
    while let Some(curr) = queue.pop_front() {
        if let Some(kids) = children_by_parent.get(curr) {
            for &kid in kids {
                if descendants.insert(kid.to_string()) {
                    queue.push_back(kid);
                }
            }
        }
    }
    agents.retain(|a| descendants.contains(&a.id));
}

pub(super) const DEFAULT_LIST_LIMIT: usize = 100;
pub(super) const MAX_LIST_LIMIT: usize = 100;

pub(crate) fn filter_and_summarize_agent_list(
    agents: &mut Vec<AgentView>,
    query: Option<&str>,
    limit: Option<usize>,
    cursor: Option<usize>,
) -> serde_json::Value {
    // The caller has already applied visibility, parent and state scope. Query
    // and pagination change the slice, not this population's health counts.
    let total = agents.len();
    let running = agents.iter().filter(|a| a.state == "running").count();
    let working = agents
        .iter()
        .filter(|a| a.state == "running" && matches!(a.turn_state.as_str(), "working" | "in_turn"))
        .count();
    let errored = agents.iter().filter(|a| a.turn_state == "errored").count();
    let scaled_to_zero = agents.iter().filter(|a| a.state == "idle").count();
    let running_idle = agents
        .iter()
        .filter(|a| {
            a.state == "running"
                && !matches!(a.turn_state.as_str(), "working" | "in_turn" | "errored")
        })
        .count();
    let idle = running_idle + scaled_to_zero;
    let paused = agents.iter().filter(|a| a.state == "paused").count();
    let archived = agents.iter().filter(|a| a.state == "archived").count();

    if let Some(q) = query.map(str::trim).filter(|s| !s.is_empty()) {
        agents.retain(|a| agent_matches_query(a, q));
    }
    let matched = agents.len();
    // One total order at every page size: priority, name, then immutable ID.
    // Do not discard archived matches: continuation must reach the full population.
    agents.sort_by(|a, b| {
        agent_list_priority(a)
            .cmp(&agent_list_priority(b))
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    let cap = limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT);
    let start = cursor.unwrap_or(0).min(matched);
    let end = start.saturating_add(cap).min(matched);
    let next_cursor = (end < matched).then_some(end);
    agents.drain(..start);
    agents.truncate(cap);
    // List responses stay compact; full instructions and schedules live on `agents inspect` / `schedules list`.
    for a in agents.iter_mut() {
        a.instructions = None;
        a.schedules.clear();
    }

    serde_json::json!({
        "total": total,
        "matched": matched,
        "returned": agents.len(),
        "next_cursor": next_cursor,
        "running": running,
        "working": working,
        "errored": errored,
        "idle": idle,
        "scaled_to_zero": scaled_to_zero,
        "paused": paused,
        "archived": archived,
    })
}

// A row-count limit alone cannot bound arbitrary descriptions or JSON escaping.
// Budget the encoded rmcp result (including its backward-compatible JSON text
// fallback AND structuredContent), not only the row JSON. HTTP AgentView is unchanged.
// 180 KiB fits a full page of 100 compact agent summaries without prematurely
// truncating caller-requested limits down to ~18 rows.
pub(super) const AGENT_LIST_RESULT_BUDGET: usize = 180_000;

pub(super) fn compact_agent_row(row: &mut serde_json::Value) {
    let Some(row) = row.as_object_mut() else {
        return;
    };
    row.retain(|key, _| {
        matches!(
            key.as_str(),
            "id" | "name"
                | "role"
                | "aliases"
                | "parent"
                | "scope"
                | "lifecycle"
                | "relationship"
                | "can_message"
                | "harness"
                | "model"
                | "live_harness"
                | "live_model"
                | "pending_switch"
                | "state"
                | "process"
                | "turn_state"
                | "delivery"
                | "automations"
                | "description"
                | "has_open_turn"
        )
    });
    if let Some(description) = row.get("description").and_then(|v| v.as_str())
        && description.chars().count() > 160
    {
        let preview: String = description.chars().take(160).collect();
        row.insert("description".into(), serde_json::json!(preview));
        row.insert("description_truncated".into(), serde_json::json!(true));
    }
}

// Measure the same Json wrapper conversion the generated MCP router uses, not
// an approximation of the rows or only structuredContent.
pub(super) fn encoded_agent_list_result(value: &serde_json::Value) -> Result<Vec<u8>, String> {
    use rmcp::handler::server::tool::IntoCallToolResult as _;
    let response = rmcp::handler::server::wrapper::Json(value.clone())
        .into_call_tool_result()
        .map_err(|error| error.to_string())?;
    match response {
        rmcp::model::CallToolResponse::Complete(result) => {
            serde_json::to_vec(&result).map_err(|error| error.to_string())
        }
        _ => Err("agent list unexpectedly returned a deferred tool response".into()),
    }
}

pub(super) fn bound_agent_list(
    mut value: serde_json::Value,
    key: &str,
    cursor: Option<usize>,
) -> Result<serde_json::Value, String> {
    let matched = value["summary"]["matched"].as_u64().unwrap_or(0) as usize;
    let start = cursor.unwrap_or(0).min(matched);
    let rows = value[key].as_array_mut().ok_or("missing agent rows")?;
    for row in rows {
        compact_agent_row(row);
    }
    loop {
        let returned = value[key].as_array().unwrap().len();
        let end = start.saturating_add(returned);
        let next = (end < matched).then_some(end);
        value["next_cursor"] = serde_json::json!(next);
        value["summary"]["next_cursor"] = serde_json::json!(next);
        value["summary"]["returned"] = serde_json::json!(returned);
        let bytes = encoded_agent_list_result(&value)?.len();
        if bytes < AGENT_LIST_RESULT_BUDGET {
            return Ok(value);
        }
        if returned <= 1 {
            return Err(format!(
                "one agent summary exceeds the {AGENT_LIST_RESULT_BUDGET}-byte MCP result budget; use agents inspect with its name or ID"
            ));
        }
        value[key].as_array_mut().unwrap().pop();
    }
}
