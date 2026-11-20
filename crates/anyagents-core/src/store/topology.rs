//! Conversation participant discovery, topology diffing, sender roles, and `@mention` resolution.

use super::helpers::agent_by_id;
use super::ingress::history_read_probe;
use super::types::Agent;
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::id::AgentId;

fn telegram_chat_agent_ids(conn: &dyn DbExec, chat_id: i64) -> Vec<String> {
    let mut ids = Vec::new();
    if let Ok(rows) = conn.db_query_vec(
        "SELECT c.state, ca.agent_id
         FROM channels c
         JOIN channel_agents ca ON ca.channel_id = c.id
         WHERE c.kind = 'telegram'",
        &[],
        |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
    ) {
        for (state, aid) in rows {
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&state) else {
                continue;
            };
            let Some(chats) = val.get("chats").and_then(serde_json::Value::as_array) else {
                continue;
            };
            if chats.iter().any(|link| {
                link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(chat_id)
            }) {
                ids.push(aid);
            }
        }
    }
    ids
}

/// Agents attached to a Telegram chat: those its channel links name, and
/// up to 16 that received a line from it. The second part matches origins by
/// text pattern and can read every stored message, so ingress counts on a
/// read snapshot before it takes the writer.
pub(super) fn chat_attached_agent_count(conn: &dyn DbExec, chat_id: i64) -> usize {
    history_read_probe();
    let mut distinct_agents: std::collections::HashSet<String> =
        telegram_chat_agent_ids(conn, chat_id).into_iter().collect();
    let pattern = format!("%\"chat_id\":{chat_id}%");
    if let Ok(aids) = conn.db_query_vec(
        "SELECT DISTINCT agent_id FROM messages WHERE origin LIKE ?1 LIMIT 16",
        &db_params![pattern],
        |row| row.get::<String>(0),
    ) {
        distinct_agents.extend(aids);
    }
    distinct_agents.len()
}

pub(super) fn resolve_conversation_participants(
    conn: &dyn DbExec,
    chat_id: Option<i64>,
    destination: Option<&str>,
) -> Vec<String> {
    let mut agent_ids = std::collections::BTreeSet::new();
    if let Some(cid) = chat_id
        && cid < 0
    {
        agent_ids.extend(telegram_chat_agent_ids(conn, cid));
    } else if let Some(group_name) = destination.and_then(|d| d.strip_prefix("group:")) {
        if group_name == "all" {
            if let Ok(aids) = conn.db_query_vec(
                "SELECT id FROM agents WHERE parent_id IS NULL AND state IN ('running', 'idle')",
                &[],
                |row| row.get::<String>(0),
            ) {
                agent_ids.extend(aids);
            }
        } else if let Ok(aids) = conn.db_query_vec(
            "SELECT agent_id FROM group_agents WHERE group_name = ?1",
            &db_params![group_name],
            |row| row.get::<String>(0),
        ) {
            agent_ids.extend(aids);
        }
    }
    if agent_ids.len() <= 1 {
        return Vec::new();
    }
    let mut participants = Vec::new();
    for aid in agent_ids {
        if let Ok(Some((name, role, state))) = conn.db_query_opt(
            "SELECT name, role, state FROM agents WHERE id = ?1",
            &db_params![aid],
            |r| {
                Ok((
                    r.get::<String>(0)?,
                    r.get::<Option<String>>(1)?,
                    r.get::<String>(2)?,
                ))
            },
        ) {
            if state == "archived" {
                continue;
            }
            if let Some(role) = role.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                participants.push(format!("{name} ({role})"));
            } else {
                participants.push(name);
            }
        }
    }
    participants.sort();
    participants
}

fn format_agent_topology_tag(name: &str, role: Option<&str>) -> String {
    match role.map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => format!("agent:{name} [{r}]"),
        None => format!("agent:{name}"),
    }
}

pub(super) fn xml_escape_attr_local(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn is_ancestor_in_conn(conn: &dyn DbExec, ancestor_id: &AgentId, target: &Agent) -> bool {
    let mut cursor = target.parent_id.clone();
    let mut depth = 0usize;
    while let Some(pid) = cursor {
        if &pid == ancestor_id {
            return true;
        }
        depth += 1;
        if depth > 32 {
            break;
        }
        cursor = agent_by_id(conn, &pid).ok().and_then(|a| a.parent_id);
    }
    false
}

pub(super) fn resolve_sender_relationship_and_role(
    conn: &dyn DbExec,
    recipient: Option<&Agent>,
    sender: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(sender_raw) = sender.map(str::trim).filter(|s| !s.is_empty()) else {
        return (None, None);
    };
    if sender_raw.starts_with("telegram:user:") {
        return (None, Some("external_user".to_string()));
    }
    if sender_raw == "operator" {
        return (None, Some("operator".to_string()));
    }
    let Some(sender_reference) = sender_raw.strip_prefix("agent:") else {
        return (None, None);
    };
    // An admitted immutable sender cannot change identity while waiting for
    // publication. Syntactically valid IDs are never resolved as display names,
    // even if their exact ID is absent. Non-ID legacy senders keep name lookup.
    let sender_id = match AgentId::parse(sender_reference) {
        Ok(id) => id,
        Err(_) => {
            let Some(id) = conn
                .db_query_opt(
                    "SELECT id FROM agents WHERE name = ?1",
                    &db_params![sender_reference],
                    |r| r.get::<String>(0),
                )
                .ok()
                .flatten()
                .and_then(|id| AgentId::parse(&id).ok())
            else {
                return (None, None);
            };
            id
        }
    };
    let Ok(sender_agent) = agent_by_id(conn, &sender_id) else {
        return (None, None);
    };
    let sender_role = sender_agent
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let Some(rec) = recipient else {
        return (sender_role, None);
    };
    let rel = if sender_agent.id == rec.id {
        "self"
    } else if rec.parent_id.as_ref() == Some(&sender_agent.id) {
        "parent"
    } else if sender_agent.parent_id.as_ref() == Some(&rec.id) {
        "child"
    } else if rec.parent_id.is_some() && rec.parent_id == sender_agent.parent_id {
        "sibling"
    } else if is_ancestor_in_conn(conn, &sender_agent.id, rec) {
        "ancestor"
    } else if is_ancestor_in_conn(conn, &rec.id, &sender_agent) {
        "descendant"
    } else {
        "peer"
    };
    (sender_role, Some(rel.to_string()))
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PersistedTopologyState {
    scope: String,
    parent: Option<String>,
    peers: Vec<String>,
    children: Vec<String>,
    groups: Vec<String>,
}

pub(super) fn compute_and_diff_topology_event(
    conn: &dyn DbExec,
    recipient: Option<&Agent>,
    now_ms: i64,
) -> Option<String> {
    let rec = recipient?;
    let scope = rec.scope().to_string();
    let parent = rec
        .parent_id
        .as_ref()
        .and_then(|pid| agent_by_id(conn, pid).ok())
        .map(|p| format_agent_topology_tag(&p.name, p.role.as_deref()));

    let mut peers = Vec::new();
    if rec.parent_id.is_none() {
        if let Ok(ids) = conn.db_query_vec(
            "SELECT id FROM agents WHERE parent_id IS NULL AND state != 'archived' AND id != ?1 ORDER BY name ASC",
            &db_params![rec.id.as_str()],
            |r| r.get::<String>(0),
        ) {
            for id_str in ids {
                if let Ok(aid) = AgentId::parse(&id_str)
                    && let Ok(peer) = agent_by_id(conn, &aid)
                {
                    peers.push(format_agent_topology_tag(&peer.name, peer.role.as_deref()));
                }
            }
        }
    } else if rec.allows_sibling_messages()
        && let Some(ref pid) = rec.parent_id
        && let Ok(ids) = conn.db_query_vec(
            "SELECT id FROM agents WHERE parent_id = ?1 AND state != 'archived' AND id != ?2 ORDER BY name ASC",
            &db_params![pid.as_str(), rec.id.as_str()],
            |r| r.get::<String>(0),
        )
    {
        for id_str in ids {
            if let Ok(aid) = AgentId::parse(&id_str)
                && let Ok(sib) = agent_by_id(conn, &aid)
                && sib.allows_sibling_messages()
            {
                peers.push(format_agent_topology_tag(&sib.name, sib.role.as_deref()));
            }
        }
    }

    let mut children = Vec::new();
    if let Ok(ids) = conn.db_query_vec(
        "SELECT id FROM agents WHERE parent_id = ?1 AND state != 'archived' ORDER BY name ASC",
        &db_params![rec.id.as_str()],
        |r| r.get::<String>(0),
    ) {
        for id_str in ids {
            if let Ok(aid) = AgentId::parse(&id_str)
                && let Ok(child) = agent_by_id(conn, &aid)
            {
                children.push(format_agent_topology_tag(
                    &child.name,
                    child.role.as_deref(),
                ));
            }
        }
    }

    let mut groups = Vec::new();
    if let Ok(group_names) = conn.db_query_vec(
        "SELECT group_name FROM group_agents WHERE agent_id = ?1 ORDER BY group_name ASC",
        &db_params![rec.id.as_str()],
        |r| r.get::<String>(0),
    ) {
        for g in group_names {
            groups.push(format!("group:{g}"));
        }
    }

    let current = PersistedTopologyState {
        scope: scope.clone(),
        parent: parent.clone(),
        peers: peers.clone(),
        children: children.clone(),
        groups: groups.clone(),
    };
    let current_json = serde_json::to_string(&current).ok()?;

    let prev_json: Option<String> = conn
        .db_query_opt(
            "SELECT state_json FROM agent_topology_state WHERE agent_id = ?1",
            &db_params![rec.id.as_str()],
            |r| r.get(0),
        )
        .ok()
        .flatten();

    match prev_json.and_then(|s| serde_json::from_str::<PersistedTopologyState>(&s).ok()) {
        None => {
            let _ = conn.db_execute(
                "INSERT INTO agent_topology_state (agent_id, state_json, updated_at_ms)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (agent_id) DO UPDATE SET state_json = excluded.state_json, updated_at_ms = excluded.updated_at_ms",
                &db_params![rec.id.as_str(), current_json, now_ms],
            );
            let self_tag = format_agent_topology_tag(&rec.name, rec.role.as_deref());
            let mut tag = format!(
                "<topology event=\"initial\" self=\"{}\" scope=\"{}\"",
                xml_escape_attr_local(&self_tag),
                xml_escape_attr_local(&scope)
            );
            if let Some(ref p) = parent {
                tag.push_str(&format!(" parent=\"{}\"", xml_escape_attr_local(p)));
            }
            if !peers.is_empty() {
                let attr_name = if rec.parent_id.is_some() {
                    "siblings"
                } else {
                    "peers"
                };
                tag.push_str(&format!(
                    " {attr_name}=\"{}\"",
                    xml_escape_attr_local(&peers.join(", "))
                ));
            }
            if !children.is_empty() {
                tag.push_str(&format!(
                    " children=\"{}\"",
                    xml_escape_attr_local(&children.join(", "))
                ));
            }
            if !groups.is_empty() {
                tag.push_str(&format!(
                    " groups=\"{}\"",
                    xml_escape_attr_local(&groups.join(", "))
                ));
            }
            tag.push_str(" />");
            Some(tag)
        }
        Some(prev) if prev == current => None,
        Some(prev) => {
            let _ = conn.db_execute(
                "UPDATE agent_topology_state SET state_json = ?1, updated_at_ms = ?2 WHERE agent_id = ?3",
                &db_params![current_json, now_ms, rec.id.as_str()],
            );
            let mut changes = Vec::new();
            if prev.scope != current.scope {
                changes.push(format!("scope: {} -> {}", prev.scope, current.scope));
            }
            if prev.parent != current.parent {
                changes.push(format!(
                    "parent: {}",
                    current.parent.as_deref().unwrap_or("none")
                ));
            }
            let peer_label = if rec.parent_id.is_some() {
                "sibling"
            } else {
                "peer"
            };
            for p in &current.peers {
                if !prev.peers.contains(p) {
                    changes.push(format!("+{peer_label}: {p}"));
                }
            }
            for p in &prev.peers {
                if !current.peers.contains(p) {
                    changes.push(format!("-{peer_label}: {p}"));
                }
            }
            for c in &current.children {
                if !prev.children.contains(c) {
                    changes.push(format!("+child: {c}"));
                }
            }
            for c in &prev.children {
                if !current.children.contains(c) {
                    changes.push(format!("-child: {c}"));
                }
            }
            for g in &current.groups {
                if !prev.groups.contains(g) {
                    changes.push(format!("+group: {g}"));
                }
            }
            for g in &prev.groups {
                if !current.groups.contains(g) {
                    changes.push(format!("-group: {g}"));
                }
            }
            if changes.is_empty() {
                None
            } else {
                Some(format!(
                    "<topology event=\"delta\" changes=\"{}\" />",
                    xml_escape_attr_local(&changes.join("; "))
                ))
            }
        }
    }
}

pub(super) fn has_quoted_block(text: &str) -> bool {
    text.contains("\n\n----- quoted message -----\n")
        || text.starts_with("----- quoted message -----\n")
}

pub(super) fn strip_quoted_block(text: &str) -> &str {
    text.split("\n\n----- quoted message -----\n")
        .next()
        .unwrap_or(text)
        .trim()
}

pub(super) fn telegram_sender_user_key(sender: Option<&str>) -> Option<String> {
    let s = sender?;
    if let Some(rest) = s.strip_prefix("telegram:user:") {
        let uid = rest.split(':').next()?;
        return (!uid.is_empty()).then(|| format!("telegram:user:{uid}"));
    }
    if s.starts_with("agent:") {
        return Some(s.to_string());
    }
    None
}

pub(super) fn resolve_chat_metadata(
    conn: &dyn DbExec,
    chat_id: i64,
) -> (Option<String>, Option<String>) {
    let mut found_title: Option<String> = None;
    let mut found_type: Option<String> = None;
    if let Ok(states) = conn.db_query_vec(
        "SELECT state FROM channels WHERE kind = 'telegram'",
        &[],
        |row| row.get::<String>(0),
    ) {
        for state in states {
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&state) else {
                continue;
            };
            let Some(chats) = val.get("chats").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for link in chats {
                if link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(chat_id) {
                    if found_title.is_none()
                        && let Some(t) = link
                            .get("title")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty() && *s != "-")
                    {
                        found_title = Some(t.to_string());
                    }
                    if found_type.is_none()
                        && let Some(ct) = link
                            .get("chat_type")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                    {
                        found_type = Some(ct.to_string());
                    }
                }
            }
            if found_title.is_some() && found_type.is_some() {
                break;
            }
        }
    }
    (found_title, found_type)
}

pub(super) fn resolve_mentions(conn: &dyn DbExec, text: &str) -> Vec<String> {
    if !text.contains('@') {
        return Vec::new();
    }
    let handles = extract_at_handles(text);
    if handles.is_empty() {
        return Vec::new();
    }
    let mut bot_to_agent: Vec<(String, String)> = Vec::new();
    if let Ok(rows) = conn.db_query_vec(
        "SELECT c.state, a.name
         FROM channels c
         JOIN channel_agents ca ON ca.channel_id = c.id
         JOIN agents a ON a.id = ca.agent_id
         WHERE c.kind = 'telegram'",
        &[],
        |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
    ) {
        for (state, agent_name) in rows {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&state)
                && let Some(bot) = val.get("bot").and_then(serde_json::Value::as_str)
                && let Some((_, username)) = bot.split_once(':')
                && !username.is_empty()
            {
                bot_to_agent.push((username.to_string(), agent_name));
            }
        }
    }
    let mut out = Vec::new();
    for handle in handles {
        let agent_match: Option<String> = conn
            .db_query_opt(
                "SELECT name FROM agents WHERE LOWER(name) = LOWER(?1)",
                &db_params![handle],
                |row| row.get::<String>(0),
            )
            .ok()
            .flatten()
            .or_else(|| {
                bot_to_agent
                    .iter()
                    .find(|(bot_user, _)| bot_user.eq_ignore_ascii_case(&handle))
                    .map(|(_, agent)| agent.clone())
            });
        let entry = match agent_match {
            Some(agent) => format!("@{handle} (agent:{agent})"),
            None => format!("@{handle}"),
        };
        if !out.contains(&entry) {
            out.push(entry);
        }
    }
    out
}

fn extract_at_handles(text: &str) -> Vec<String> {
    let mut handles = Vec::new();
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].1 == '@' {
            let valid_prefix = i == 0 || {
                let prev = chars[i - 1].1;
                !prev.is_ascii_alphanumeric() && prev != '_' && prev != '.'
            };
            if valid_prefix {
                let mut j = i + 1;
                while j < chars.len() {
                    let c = chars[j].1;
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        j += 1;
                    } else {
                        break;
                    }
                }
                let len = j - (i + 1);
                let followed_by_domain = j + 1 < chars.len()
                    && chars[j].1 == '.'
                    && chars[j + 1].1.is_ascii_alphanumeric();
                if (1..=64).contains(&len) && !followed_by_domain {
                    let start_byte = chars[i + 1].0;
                    let end_byte = if j < chars.len() {
                        chars[j].0
                    } else {
                        text.len()
                    };
                    let handle = text[start_byte..end_byte]
                        .trim_end_matches(['-', '_'])
                        .to_string();
                    if !handle.is_empty() && !handles.contains(&handle) {
                        handles.push(handle);
                    }
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    handles
}
