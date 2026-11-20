//! Agent roster filtering, relationship annotation, and view assembly.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Store, now_ms};
use anyagents_supervisor::HealthBoard;
use axum::Json;
use axum::http::StatusCode;
use serde::Deserialize;

use crate::api::{AgentView, AgentsResponse};
use crate::auth::Principal;
use crate::create;

use super::messaging::{can_sender_message_agent_with, unsettled_view};
use super::types::{ApiError, AppState, is_self_or_ancestor};
use super::{file_cache, outbound_history, quota_rotation, serverless};

#[derive(Clone, Default, Deserialize)]
pub(crate) struct ListAgentsQuery {
    pub state: Option<String>,
    pub parent: Option<String>,
    pub name: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(default, alias = "subtree")]
    pub include_descendants: Option<bool>,
}

pub(super) fn compute_caller_relationship(store: &Store, caller: &Agent, target: &Agent) -> String {
    relationship_with(caller, target, |ancestor, agent| {
        is_self_or_ancestor(store, ancestor, agent)
    })
}

fn relationship_with(
    caller: &Agent,
    target: &Agent,
    is_self_or_ancestor: impl Fn(&anyagents_core::AgentId, &Agent) -> bool,
) -> String {
    if caller.id == target.id {
        "self".to_string()
    } else if caller.parent_id.as_ref() == Some(&target.id) {
        "parent".to_string()
    } else if target.parent_id.as_ref() == Some(&caller.id) {
        "child".to_string()
    } else if caller.parent_id.is_some() && caller.parent_id == target.parent_id {
        "sibling".to_string()
    } else if is_self_or_ancestor(&target.id, caller) {
        "ancestor".to_string()
    } else if is_self_or_ancestor(&caller.id, target) {
        "descendant".to_string()
    } else if caller.parent_id.is_none() && target.parent_id.is_none() {
        "peer".to_string()
    } else {
        "other".to_string()
    }
}

/// An agent caller's view of one roster, read once per request instead of
/// once per listed agent. Every check is the predicate the single-agent routes
/// use; only where the rows come from differs.
struct RosterCaller<'a> {
    store: &'a Store,
    caller: &'a Agent,
    /// The roster rows, so an ancestor walk reads the store only for a parent
    /// outside the roster.
    rows: HashMap<&'a str, &'a Agent>,
    /// Agents that share a group or Telegram chat with the caller.
    sharing: HashSet<anyagents_core::AgentId>,
}

impl<'a> RosterCaller<'a> {
    fn new(store: &'a Store, caller: &'a Agent, roster: &'a [Agent]) -> Self {
        Self {
            store,
            caller,
            rows: roster.iter().map(|a| (a.id.as_str(), a)).collect(),
            sharing: store.agents_sharing_group_or_chat(&caller.id),
        }
    }

    /// [`is_self_or_ancestor`] over the roster rows.
    fn is_self_or_ancestor(&self, ancestor: &anyagents_core::AgentId, target: &Agent) -> bool {
        if ancestor == &target.id {
            return true;
        }
        let mut cursor = target.parent_id.clone();
        let mut depth = 0usize;
        while let Some(pid) = cursor {
            if &pid == ancestor {
                return true;
            }
            cursor = match self.rows.get(pid.as_str()) {
                Some(parent) => parent.parent_id.clone(),
                None => self.store.agent(&pid).ok().and_then(|a| a.parent_id),
            };
            depth += 1;
            if depth > 16 {
                break;
            }
        }
        false
    }

    /// The isolated-scope rule of [`super::agents::get_agent`].
    fn sees(&self, agent: &Agent) -> bool {
        !(self.caller.is_isolated()
            && !self.is_self_or_ancestor(&self.caller.id, agent)
            && self.caller.parent_id.as_ref() != Some(&agent.id)
            && !self.sharing.contains(&agent.id))
    }

    fn annotate(&self, view: &mut AgentView, agent: &Agent) {
        view.relationship = Some(relationship_with(self.caller, agent, |ancestor, target| {
            self.is_self_or_ancestor(ancestor, target)
        }));
        view.can_message = Some(
            can_sender_message_agent_with(
                self.store,
                self.caller,
                agent,
                || self.sharing.contains(&agent.id),
                || self.is_self_or_ancestor(&self.caller.id, agent),
            )
            .is_ok(),
        );
        // [`can_actor_read_agent_private`] for this caller.
        let reads_private = self.is_self_or_ancestor(&self.caller.id, agent);
        if !reads_private {
            view.instructions = None;
            view.secrets.clear();
        }
    }
}

pub(super) fn list_agents_sync_for_principal(
    state: &AppState,
    principal: &Principal,
    query: &ListAgentsQuery,
) -> Result<Json<AgentsResponse>, ApiError> {
    let caller_agent = match principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => Some(
            state
                .store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
    };
    // For unbounded `"all"` / `"archived"` queries by non-isolated callers without
    // an explicit `name` filter, push LIMIT/OFFSET, non-archived-first ordering,
    // NULL archived `instructions`, and recursive descendant traversal into SQL
    // so 10K-agent archives never deserialize megabytes of unused rows.
    let caller_is_isolated = caller_agent.as_ref().is_some_and(Agent::is_isolated);
    let can_sql_page = !caller_is_isolated
        && query.name.is_none()
        && matches!(query.state.as_deref(), Some("all" | "archived"));
    let (roster, archived_count, sql_paged) = if can_sql_page {
        let root_only = matches!(query.parent.as_deref(), Some("root" | "none"));
        let parent_agent = query
            .parent
            .as_deref()
            .filter(|p| !matches!(*p, "root" | "none" | "all" | "subagents"))
            .map(|p| state.store.agent_by_name(p))
            .transpose()
            .map_err(ApiError::from_store)?;
        let (rows, archived) = state
            .store
            .roster_agents_paged(anyagents_core::RosterPageQuery {
                state: query.state.as_deref(),
                parent_id: parent_agent.as_ref().map(|a| &a.id),
                root_only,
                include_descendants: query.include_descendants.unwrap_or(false),
                limit: query.limit,
                offset: query.offset.unwrap_or(0),
            })
            .map_err(ApiError::from_store)?;
        (rows, archived, true)
    } else {
        let (rows, archived) = state
            .store
            .roster_agents(query.state.as_deref())
            .map_err(ApiError::from_store)?;
        (rows, archived, false)
    };
    let caller = caller_agent
        .as_ref()
        .map(|caller| RosterCaller::new(&state.store, caller, &roster));
    let visible: Vec<&Agent> = roster
        .iter()
        .filter(|agent| caller.as_ref().is_none_or(|caller| caller.sees(agent)))
        .collect();
    let effective_query = if sql_paged {
        ListAgentsQuery {
            offset: None,
            parent: if query.include_descendants.unwrap_or(false) {
                None
            } else {
                query.parent.clone()
            },
            ..query.clone()
        }
    } else {
        query.clone()
    };
    let mut listed = roster_views(
        &state.store,
        &state.health,
        &visible,
        archived_count,
        &effective_query,
    )?;
    if let Some(caller) = &caller {
        let by_id: HashMap<&str, &Agent> = visible.iter().map(|a| (a.id.as_str(), *a)).collect();
        for view in &mut listed.agents {
            let agent = by_id
                .get(view.id.as_str())
                .copied()
                .expect("every view comes from a visible row");
            caller.annotate(view, agent);
        }
    }
    Ok(Json(listed))
}

/// Shared by the daemon and offline operator CLI. Filter before runtime reads.
pub(crate) fn roster_views(
    store: &Store,
    health: &HealthBoard,
    all_agents: &[&Agent],
    archived_count: usize,
    query: &ListAgentsQuery,
) -> Result<AgentsResponse, ApiError> {
    if let Some(state) = query.state.as_deref()
        && !matches!(
            state,
            "all" | "archived" | "running" | "idle" | "paused" | "created"
        )
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid agent state",
        ));
    }
    let by_id: HashMap<_, _> = all_agents.iter().map(|a| (a.id.as_str(), *a)).collect();
    let selected_name = query
        .name
        .as_deref()
        .map(|name| store.agent_by_name(name))
        .transpose()
        .map_err(ApiError::from_store)?;
    let parent = query
        .parent
        .as_deref()
        .filter(|p| !matches!(*p, "root" | "none" | "all" | "subagents"))
        .map(|p| store.agent_by_name(p))
        .transpose()
        .map_err(ApiError::from_store)?;
    let include_descendants = query.include_descendants.unwrap_or(false);
    let descendant_ids: Option<HashSet<&str>> = if include_descendants && let Some(ref p) = parent {
        let mut children_by_parent: HashMap<&str, Vec<&str>> = HashMap::new();
        for a in all_agents {
            if let Some(ref pid) = a.parent_id {
                children_by_parent
                    .entry(pid.as_str())
                    .or_default()
                    .push(a.id.as_str());
            }
        }
        let mut out: HashSet<&str> = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(p.id.as_str());
        while let Some(curr) = queue.pop_front() {
            if let Some(kids) = children_by_parent.get(curr) {
                for &kid in kids {
                    if out.insert(kid) {
                        queue.push_back(kid);
                    }
                }
            }
        }
        Some(out)
    } else {
        None
    };
    let mut selected = HashSet::new();
    for agent in all_agents {
        let state_matches = match query.state.as_deref() {
            Some("all") => true,
            Some(state) => agent.state == state,
            None => agent.state != "archived",
        };
        let parent_matches = match query.parent.as_deref() {
            Some("root" | "none") => agent.parent_id.is_none(),
            _ => match (&parent, &descendant_ids) {
                (Some(_), Some(desc)) => desc.contains(agent.id.as_str()),
                (Some(p), None) => agent.parent_id.as_ref() == Some(&p.id),
                (None, _) => true,
            },
        };
        if state_matches
            && parent_matches
            && selected_name.as_ref().is_none_or(|a| a.id == agent.id)
        {
            selected.insert(agent.id.as_str());
        }
    }
    // Retain ancestor nodes for tree rendering without serializing the archive.
    if query
        .state
        .as_deref()
        .is_none_or(|s| !matches!(s, "all" | "archived"))
    {
        let initial: Vec<_> = selected.iter().copied().collect();
        for id in initial {
            let mut cursor = by_id.get(id).and_then(|a| a.parent_id.as_ref());
            let mut visited = HashSet::new();
            while let Some(parent) = cursor {
                if !visited.insert(parent.as_str()) {
                    break;
                }
                let Some(agent) = by_id.get(parent.as_str()) else {
                    break;
                };
                selected.insert(agent.id.as_str());
                cursor = agent.parent_id.as_ref();
            }
        }
    }
    let mut selected_agents: Vec<&Agent> = all_agents
        .iter()
        .copied()
        .filter(|a| selected.contains(a.id.as_str()))
        .collect();
    let is_archive_inclusive = matches!(query.state.as_deref(), Some("all" | "archived"));
    if is_archive_inclusive {
        selected_agents.sort_by(|a, b| {
            (a.state == "archived")
                .cmp(&(b.state == "archived"))
                .then_with(|| b.created_at_ms.cmp(&a.created_at_ms))
                .then_with(|| a.name.cmp(&b.name))
        });
    }
    let page_limit = if is_archive_inclusive {
        Some(
            query
                .limit
                .unwrap_or(anyagents_core::DEFAULT_ARCHIVE_ROSTER_LIMIT)
                .max(1),
        )
    } else {
        query.limit.map(|l| l.max(1))
    };
    let page_offset = query.offset.unwrap_or(0);
    if page_offset > 0 {
        if page_offset >= selected_agents.len() {
            selected_agents.clear();
        } else {
            selected_agents.drain(..page_offset);
        }
    }
    if let Some(lim) = page_limit
        && selected_agents.len() > lim
    {
        selected_agents.truncate(lim);
    }
    let all_archived = selected_agents.iter().all(|a| a.state == "archived");
    let mut metadata = if all_archived {
        anyagents_core::AgentQueryMetadata {
            aliases: store.agent_aliases_map().map_err(ApiError::from_store)?,
            ..Default::default()
        }
    } else {
        store.agent_query_metadata().map_err(ApiError::from_store)?
    };
    let files = file_cache::cache();
    files.catch_up();
    let seats = if all_archived {
        std::sync::Arc::new(HashMap::new())
    } else {
        view_seats(files)
    };
    let mut unsettled_by_agent: HashMap<String, Vec<crate::api::UnsettledOutboundView>> =
        HashMap::new();
    if !all_archived {
        for held in outbound_history::unsettled(store) {
            unsettled_by_agent
                .entry(held.agent_id.as_str().to_string())
                .or_default()
                .push(unsettled_view(&held));
        }
    }
    let mut secrets_by_agent = if all_archived {
        HashMap::new()
    } else {
        store.all_agent_secret_grants().unwrap_or_default()
    };
    let mut agents = Vec::with_capacity(selected_agents.len());
    for agent in selected_agents {
        let mut view = agent_view_preloaded(
            store,
            health,
            agent,
            Some(&mut metadata),
            &seats,
            Some(&unsettled_by_agent),
        )?;
        view.secrets = secrets_by_agent
            .remove(agent.id.as_str())
            .unwrap_or_default();
        agents.push(view);
    }
    Ok(AgentsResponse {
        agents,
        archived_count,
        gateway: crate::gateway::origin(),
    })
}

pub(crate) fn compute_block_reason(
    quota_reset_at_ms: Option<i64>,
    last_error: Option<&str>,
    now_ms: i64,
) -> Option<String> {
    if quota_reset_at_ms.is_some_and(|ms| ms > now_ms) {
        return Some("quota_exhausted".to_string());
    }
    if let Some(err) = last_error.map(str::trim).filter(|s| !s.is_empty()) {
        let lower = err.to_ascii_lowercase();
        if crate::host_memory::is_migration_wait(err) {
            return Some("memory_migration_wait".to_string());
        }
        if lower.contains("quota")
            || lower.contains("resource_exhausted")
            || lower.contains("rate_limit")
        {
            return Some("quota_exhausted".to_string());
        }
        if lower.contains("sign in")
            || lower.contains("not logged in")
            || lower.contains("unauthorized")
        {
            return Some("sign_in_required".to_string());
        }
        return Some("delivery_error".to_string());
    }
    None
}

/// The marker files an agent view shows, all in the agent's config directory.
pub(super) const LAST_ERROR: &str = ".anyagents-last-error.json";

pub(super) const PROFILE_ROTATED: &str = ".anyagents-profile-rotated-ms";

const INSTRUCTIONS_ROTATED: &str = ".anyagents-instructions-rotated-ms";

pub(super) const QUOTA_CLEARED: &str = ".anyagents-quota-cleared-ms";

pub(super) const NOTIFIED_PARENT: &str = ".anyagents-notified-parent-error.json";

pub(super) const BOUND_PROFILE: &str = ".anyagents-profile";

/// One agent's marker files, as the view cache holds them.
pub(super) struct Markers<'a> {
    pub(super) files: &'a file_cache::FileCache,
    pub(super) cfg: &'a Path,
}

impl Markers<'_> {
    fn read(&self, name: &str) -> Option<std::sync::Arc<str>> {
        self.files.file(&self.cfg.join(name))
    }

    /// A millisecond timestamp an operator or rotation marker holds; 0 when
    /// absent.
    pub(super) fn ms(&self, name: &str) -> i64 {
        self.read(name)
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0)
    }

    /// [`create::bound_profile_name`] from the cached marker.
    fn bound_profile(&self) -> Option<String> {
        self.read(BOUND_PROFILE)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Whether [`create::maybe_notify_parent_on_error`] would act for this
    /// error: the same early returns and the same episode key it compares with
    /// the notice marker. Only then does the view call it, and it re-checks
    /// both files before it writes or sends anything.
    fn parent_notice_due(&self, agent: &Agent, rotated_ms: i64, err_msg: &str) -> bool {
        let trimmed = err_msg.trim();
        if agent.parent_id.is_none()
            || trimmed.is_empty()
            || crate::host_memory::is_migration_wait(trimmed)
        {
            return false;
        }
        let source_key = format!(
            "subagent-error:{}:{rotated_ms}:{trimmed}",
            agent.id.as_str()
        );
        !self
            .read(NOTIFIED_PARENT)
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .is_some_and(|val| {
                val.get("source_key").and_then(|v| v.as_str()) == Some(source_key.as_str())
            })
    }
}

/// The quota reset and fault text an adapter or the launcher last persisted in
/// `.anyagents-last-error.json`. A record from before the last profile rotation
/// (for AGY, also before the last quota clear) is stale, and an expired reset
/// clears its record. Claude Code persists no reset, so only its text counts.
pub(super) fn persisted_fault(
    harness: &str,
    markers: &Markers<'_>,
    rotated_ms: i64,
    now: i64,
) -> (Option<i64>, Option<String>) {
    let (quota, error_with_ts) = persisted_fault_with_ts(harness, markers, rotated_ms, now);
    (quota, error_with_ts.map(|(err, _)| err))
}

fn fault_floor_ms(harness: &str, markers: &Markers<'_>, rotated_ms: i64) -> i64 {
    if harness == "agy" {
        rotated_ms.max(markers.ms(QUOTA_CLEARED))
    } else {
        rotated_ms
    }
}

fn persisted_fault_with_ts(
    harness: &str,
    markers: &Markers<'_>,
    rotated_ms: i64,
    now: i64,
) -> (Option<i64>, Option<(String, i64)>) {
    let Some(record) = markers
        .read(LAST_ERROR)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
    else {
        return (None, None);
    };
    let floor = fault_floor_ms(harness, markers, rotated_ms);
    let recorded_at = record
        .get("timestamp_ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if recorded_at <= floor {
        return (None, None);
    }
    let error = record
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map(|s| (s.to_string(), recorded_at));
    if harness == "claude-code" {
        return (None, error);
    }
    match anyagents_harness::quota::recorded_reset_at_ms(&record) {
        Some(reset_ms) if reset_ms > now => (Some(reset_ms), error),
        Some(_) => (None, None),
        None => (None, error),
    }
}

fn pick_fresher_error(
    local: Option<(String, i64)>,
    remote: Option<(String, i64)>,
) -> (Option<String>, bool) {
    match (local, remote) {
        (None, None) => (None, false),
        (Some((err, _)), None) => (Some(err), false),
        (None, Some((err, _))) => (Some(err), true),
        (Some((local_err, local_ts)), Some((remote_err, remote_ts))) => {
            if remote_ts > local_ts {
                (Some(remote_err), true)
            } else {
                (Some(local_err), false)
            }
        }
    }
}

pub(super) fn archived_agent_view_fast(agent: &Agent) -> AgentView {
    let delivery: crate::api::DeliveryView = (&declared_delivery_for_agent_mode(agent)).into();
    AgentView {
        has_open_turn: false,
        open_turn_age_ms: None,
        profile: None,
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases: Vec::new(),
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
        lifecycle: agent.lifecycle().to_string(),
        relationship: None,
        can_message: None,
        harness: agent.harness.clone(),
        runtime: agent.runtime.as_canonical_string(),
        remote_budget_micros: agent.remote_budget_micros,
        model: agent.model.clone(),
        live_harness: None,
        live_model: None,
        process_started_at_ms: None,
        pending_switch: false,
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: "down".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: delivery.legacy_mode(),
        steer_support: delivery.legacy_steer(),
        delivery,
        quota_reset_at_ms: None,
        block_reason: None,
        last_error: None,
        oldest_undelivered_age_ms: None,
        install: "archived".to_string(),
        description: agent.public_description(),
        instructions: None,
        seat_user: None,
        seat_uid: None,
        seat_gid: None,
        automations: Default::default(),
        schedules: Vec::new(),
        turn_errors: None,
        unsettled_outbound: Vec::new(),
        secrets: Vec::new(),
        retry_at_ms: None,
    }
}

pub(crate) fn agent_view(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
) -> Result<AgentView, ApiError> {
    let files = file_cache::cache();
    files.catch_up();
    agent_view_preloaded(store, health, agent, None, &view_seats(files), None)
}

type RosterSeats = HashMap<String, (String, u32, u32, u32)>;

/// [`crate::sandbox::roster_seats`], kept by the view cache: the admitted
/// principals come from the environment or from the registry file it names.
fn view_seats(files: &file_cache::FileCache) -> std::sync::Arc<RosterSeats> {
    let registry = std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY").map(PathBuf::from);
    match registry {
        Some(registry) if std::env::var("ANYAGENTS_PRINCIPALS").is_err() => files.fact(
            format!("seats:{}", registry.display()),
            file_cache::Dep::File(registry),
            || std::sync::Arc::new(crate::sandbox::roster_seats()),
        ),
        // Read from the environment, or nothing to read.
        _ => std::sync::Arc::new(crate::sandbox::roster_seats()),
    }
}

fn agent_view_preloaded(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    mut metadata: Option<&mut anyagents_core::AgentQueryMetadata>,
    seats: &RosterSeats,
    unsettled_by_agent: Option<&HashMap<String, Vec<crate::api::UnsettledOutboundView>>>,
) -> Result<AgentView, ApiError> {
    if agent.state == "archived" {
        let mut view = archived_agent_view_fast(agent);
        if let Some(metadata) = metadata {
            view.aliases = metadata
                .aliases
                .remove(agent.id.as_str())
                .unwrap_or_default();
            // Keep archived list views compact (`instructions: None`); full
            // instructions are returned on single-agent `GET /v1/agents/{name}`.
            view.instructions = None;
        } else {
            view.aliases = store.agent_aliases(&agent.id).unwrap_or_default();
            view.secrets = store.list_agent_secrets(&agent.id).unwrap_or_default();
            view.instructions =
                create::effective_agent_instructions(Some(store), store.data_dir(), agent);
        }
        return Ok(view);
    }
    let now = now_ms();
    let lag = if let Some(metadata) = metadata.as_ref() {
        anyagents_core::Lag {
            oldest_undelivered_age_ms: metadata
                .oldest_undelivered
                .get(agent.id.as_str())
                .map(|ms| now.saturating_sub(*ms).max(0)),
        }
    } else {
        store.lag(&agent.id, now).map_err(ApiError::from_store)?
    };
    let store_has_open_turn = if let Some(metadata) = metadata.as_ref() {
        metadata.open_turns.contains_key(agent.id.as_str())
    } else {
        store
            .has_open_turn(&agent.id)
            .map_err(ApiError::from_store)?
    };
    let active_remote_started_ms = if let Some(metadata) = metadata.as_ref() {
        metadata.active_remote_turns.get(agent.id.as_str()).copied()
    } else {
        let fresh_since =
            now.saturating_sub(anyagents_core::stateless::ACTIVE_REMOTE_TURN_FRESHNESS_MS);
        store
            .remote_turn_store()
            .list_active_for_agent(&agent.id)
            .ok()
            .and_then(|turns| {
                turns
                    .into_iter()
                    .filter(|t| !t.is_terminal() && t.last_event_at_ms >= fresh_since)
                    .map(|t| t.created_at_ms)
                    .min()
            })
    };
    let has_open_turn = store_has_open_turn || active_remote_started_ms.is_some();
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let files = file_cache::cache();
    let markers = Markers { files, cfg: &cfg };
    let rotated_ms = markers.ms(PROFILE_ROTATED);
    let instructions_rotated_ms = markers.ms(INSTRUCTIONS_ROTATED);
    let error_floor_ms = fault_floor_ms(&agent.harness, &markers, rotated_ms);
    // The supervisor's board is the live state; a fault outlives its process
    // only as the record an adapter persisted. A view never reads a vendor
    // transcript, log or session store.
    let (session_quota, session_error_ts) =
        persisted_fault_with_ts(&agent.harness, &markers, rotated_ms, now);
    let remote_turn_error = if active_remote_started_ms.is_some() {
        None
    } else if let Some(metadata) = metadata.as_ref() {
        metadata
            .latest_remote_turn_errors
            .get(agent.id.as_str())
            .cloned()
    } else {
        store
            .remote_turn_store()
            .latest_failed_for_agent(&agent.id)
            .ok()
            .flatten()
    }
    .filter(|err| err.settled_at_ms > error_floor_ms);
    let dispatch_retry = if active_remote_started_ms.is_none()
        && (metadata.is_none() || lag.oldest_undelivered_age_ms.is_some())
    {
        super::serverless_maintenance::read_dispatch_retry_state(store, &agent.id)
    } else {
        None
    };
    let dispatch_retry_error = dispatch_retry.as_ref().and_then(|st| {
        if st.last_error_at_ms <= error_floor_ms {
            return None;
        }
        st.last_error
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|err| (err.to_string(), st.last_error_at_ms))
    });
    let remote_error = match (
        remote_turn_error.map(|e| (e.error_text, e.settled_at_ms)),
        dispatch_retry_error,
    ) {
        (Some(a), Some(b)) => Some(if b.1 > a.1 { b } else { a }),
        (a, b) => a.or(b),
    };
    let dispatch_retry_at_ms = dispatch_retry
        .as_ref()
        .and_then(|st| st.next_retry_at_ms)
        .filter(|&ms| ms > now && lag.oldest_undelivered_age_ms.is_some());
    let (
        process,
        mut turn_state,
        delivery,
        quota,
        live_harness,
        live_model,
        process_started_at_ms,
        pending_switch,
        last_error,
        retry_at_ms,
        error_from_remote,
    ) = match health.get(&agent.id) {
        Some(live) => {
            // The supervisor retries a transient capacity fault by itself and
            // publishes the work as in progress: neither the fault nor the
            // record its adapter persisted for it is an error of the agent's.
            let retry_at_ms = live.retry_at_ms.filter(|_| live.process_alive);
            let (session_quota, session_error_ts, remote_error) = if retry_at_ms.is_some() {
                (None, None, None)
            } else {
                (session_quota, session_error_ts, remote_error)
            };
            let effective_quota = live.quota_reset_at_ms.or(session_quota);
            let (effective_err, from_remote) = match live.last_error.clone() {
                Some(err) => (Some(err), false),
                None => pick_fresher_error(session_error_ts, remote_error),
            };
            let working = live.process_alive
                && effective_quota.is_none()
                && effective_err.is_none()
                && (live.turn_state == anyagents_harness::TurnState::InTurn || has_open_turn);
            let switch_pending = live.process_alive
                && (live
                    .live_harness
                    .as_deref()
                    .is_some_and(|h| h != agent.harness)
                    || live.live_model != agent.model
                    || live
                        .process_started_at_ms
                        .is_some_and(|s| rotated_ms > s || instructions_rotated_ms > s));
            (
                if live.process_alive { "alive" } else { "down" },
                if working { "working" } else { "idle" },
                live.delivery
                    .clone()
                    .unwrap_or_else(|| declared_delivery_for_agent_mode(agent)),
                effective_quota,
                if live.process_alive {
                    live.live_harness.clone()
                } else {
                    Some(agent.harness.clone())
                },
                if live.process_alive {
                    live.live_model.clone()
                } else {
                    agent.model.clone()
                },
                live.process_started_at_ms,
                switch_pending,
                effective_err,
                retry_at_ms,
                from_remote,
            )
        }
        None => {
            if let Some(started_ms) = active_remote_started_ms {
                (
                    "up",
                    "in_turn",
                    declared_delivery_for_agent_mode(agent),
                    session_quota,
                    Some(agent.harness.clone()),
                    agent.model.clone(),
                    Some(started_ms),
                    false,
                    session_error_ts.map(|(err, _)| err),
                    None,
                    false,
                )
            } else {
                let lock_busy = if store.is_postgres() {
                    if let Some(metadata) = metadata.as_ref() {
                        metadata.active_proc_locks.contains(agent.id.as_str())
                    } else {
                        store.is_agent_proc_locked(&agent.id)
                    }
                } else {
                    store.is_agent_proc_locked(&agent.id)
                };
                let proc_str = if lock_busy { "alive" } else { "down" };
                let turn_str = if lock_busy { "working" } else { "idle" };
                let (effective_err, from_remote) =
                    pick_fresher_error(session_error_ts, remote_error);
                (
                    proc_str,
                    turn_str,
                    declared_delivery_for_agent_mode(agent),
                    session_quota,
                    if proc_str == "alive" {
                        Some(agent.harness.clone())
                    } else {
                        None
                    },
                    if proc_str == "alive" {
                        agent.model.clone()
                    } else {
                        None
                    },
                    None,
                    false,
                    effective_err,
                    dispatch_retry_at_ms,
                    from_remote,
                )
            }
        }
    };
    // Rotation rewrites the agent's profile markers; an agent whose creation
    // is in flight or failed is never written to from a view.
    if agent.state != "created"
        && let Some(reset_ms) = quota.filter(|&ms| ms > now)
    {
        quota_rotation::queue(&agent.id, reset_ms);
    }
    if (agent.state == "running" || agent.state == "idle") && quota.is_none_or(|ms| ms <= now) {
        if let Some(ref err_msg) = last_error {
            turn_state = if crate::host_memory::is_migration_wait(err_msg) {
                "idle"
            } else {
                "errored"
            };
            if !error_from_remote && markers.parent_notice_due(agent, rotated_ms, err_msg) {
                create::maybe_notify_parent_on_error(store, agent, &cfg, err_msg);
            }
        } else if markers.read(NOTIFIED_PARENT).is_some() {
            let _ = std::fs::remove_file(cfg.join(NOTIFIED_PARENT));
        }
    }
    // Turns that ended in an error, and a parking for them (docs/delivery.md).
    let turn_errors = if let Some(metadata) = metadata.as_mut() {
        metadata.turn_errors.remove(agent.id.as_str())
    } else {
        store
            .turn_errors(&agent.id)
            .ok()
            .flatten()
            .filter(anyagents_core::TurnErrors::is_current)
    };
    let parked = agent.state == "paused"
        && turn_errors
            .as_ref()
            .is_some_and(|errors| errors.faulted_at_ms.is_some());
    let last_error = if parked {
        last_error.or_else(|| turn_errors.as_ref().and_then(|e| e.last_error.clone()))
    } else {
        last_error
    };
    if parked {
        turn_state = "faulted";
    }
    let block_reason = if parked {
        Some("faulted".to_string())
    } else {
        compute_block_reason(quota, last_error.as_deref(), now)
    };
    let open_turn_age_ms = if has_open_turn {
        let store_age = if let Some(metadata) = metadata.as_ref() {
            metadata
                .open_turns
                .get(agent.id.as_str())
                .map(|ms| now.saturating_sub(*ms).max(0))
        } else {
            store.open_turn(&agent.id).ok().flatten().map(|open| {
                open.messages
                    .first()
                    .map(|m| now.saturating_sub(m.received_at_ms).max(0))
                    .unwrap_or_else(|| now.saturating_sub(open.opened_at_ms).max(0))
            })
        };
        store_age.or_else(|| active_remote_started_ms.map(|ms| now.saturating_sub(ms).max(0)))
    } else {
        None
    };
    let oldest_undelivered_age_ms = match lag.oldest_undelivered_age_ms {
        Some(age) => Some(age),
        None if has_open_turn
            && (quota.is_some()
                || last_error.is_some()
                || !matches!(turn_state, "working" | "in_turn")) =>
        {
            open_turn_age_ms
        }
        None => None,
    };
    let aliases = if let Some(metadata) = metadata.as_mut() {
        metadata
            .aliases
            .remove(agent.id.as_str())
            .unwrap_or_default()
    } else {
        store.agent_aliases(&agent.id).unwrap_or_default()
    };
    let seat_info = seats
        .get(agent.id.as_str())
        .map(|(user, uid, gid, _)| (user.clone(), *uid, *gid));
    let schedules: Vec<crate::api::ScheduleView> = if let Some(metadata) = metadata.as_mut() {
        metadata
            .schedules
            .remove(agent.id.as_str())
            .unwrap_or_default()
    } else {
        store
            .list_schedules(Some(&agent.id), true)
            .unwrap_or_default()
    }
    .into_iter()
    .map(crate::api::ScheduleView::from)
    .collect();
    Ok(AgentView {
        has_open_turn,
        open_turn_age_ms,
        profile: markers.bound_profile(),
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
        lifecycle: agent.lifecycle().to_string(),
        relationship: None,
        can_message: None,
        harness: agent.harness.clone(),
        runtime: agent.runtime.as_canonical_string(),
        remote_budget_micros: agent.remote_budget_micros,
        model: agent.model.clone(),
        live_harness,
        live_model,
        process_started_at_ms,
        pending_switch,
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: process.to_string(),
        turn_state: turn_state.to_string(),
        delivery_mode: crate::api::DeliveryView::from(&delivery).legacy_mode(),
        steer_support: crate::api::DeliveryView::from(&delivery).legacy_steer(),
        delivery: (&delivery).into(),
        quota_reset_at_ms: quota,
        block_reason,
        last_error,
        oldest_undelivered_age_ms,
        install: install_state(store, files, agent),
        description: agent.public_description(),
        instructions: create::effective_agent_instructions(
            (!serverless::is_serverless_mode()).then_some(store),
            store.data_dir(),
            agent,
        ),
        seat_user: seat_info.as_ref().map(|(u, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid)| *gid),
        automations: crate::api::AutomationCountsView::from_schedules(&schedules),
        schedules,
        turn_errors: turn_errors.map(Into::into),
        unsettled_outbound: match unsettled_by_agent {
            Some(map) => map.get(agent.id.as_str()).cloned().unwrap_or_default(),
            None => outbound_history::unsettled(store)
                .iter()
                .filter(|held| held.agent_id == agent.id)
                .map(unsettled_view)
                .collect(),
        },
        secrets: if metadata.is_none() {
            store.list_agent_secrets(&agent.id).unwrap_or_default()
        } else {
            Vec::new()
        },
        retry_at_ms,
    })
}

/// `installed` once this harness is on the machine, `needed` when the reason it
/// is not, `pending` when nothing has been recorded yet.
fn install_state(store: &Store, files: &file_cache::FileCache, agent: &Agent) -> String {
    if crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
        || serverless::is_serverless_mode()
    {
        return "installed".to_string();
    }
    match crate::install::recorded(&agent.harness) {
        Some(Ok(())) => "installed".to_string(),
        Some(Err(reason)) => reason,
        None => {
            if installed_on_disk(files, store.data_dir(), &agent.harness) {
                "installed".to_string()
            } else {
                "pending".to_string()
            }
        }
    }
}

/// Whether a harness this server has not recorded is on disk: its managed
/// receipt or launcher, all under `<root>/harnesses/<id>/`. The deprecated
/// `ANYAGENTS_<ID>_BIN` override names a program outside that tree, so it is
/// checked directly.
fn installed_on_disk(files: &file_cache::FileCache, root: &Path, harness: &str) -> bool {
    let harness = anyagents_harness::normalize_harness_name(harness).to_string();
    let check = {
        let root = root.to_path_buf();
        let harness = harness.clone();
        move || {
            crate::install::is_healthy(&root, &harness)
                || crate::install::program(&root, &harness).is_file()
        }
    };
    let override_var = format!("ANYAGENTS_{}_BIN", harness.to_uppercase().replace('-', "_"));
    if std::env::var_os(override_var).is_some() {
        return check();
    }
    files.fact(
        format!("installed:{}:{harness}", root.display()),
        file_cache::Dep::Dir(root.join("harnesses").join(&harness)),
        check,
    )
}

/// Honest delivery semantics for the current runtime mode: in serverless mode,
/// report `NextStep` (`delivery_mode: "steer"`, `steer_support: "yes"`) ONLY for
/// harnesses whose live mid-turn steering transport is wired and E2E-verified
/// (failing closed to `TurnEnd` on unwired harnesses such as `gemini`).
fn declared_delivery_for_agent_mode(agent: &Agent) -> anyagents_harness::Delivery {
    if serverless::is_serverless_mode() {
        super::serverless_steer::declared_delivery_for_serverless_agent(agent)
    } else {
        crate::remote::declared_delivery_for_agent(agent)
    }
}

#[cfg(test)]
fn declared_delivery_for_serverless(
    harness: &str,
    runtime: &anyagents_core::ExecutionRuntime,
    serverless_mode: bool,
) -> anyagents_harness::Delivery {
    if serverless_mode {
        super::serverless_steer::declared_delivery_for_serverless_parts(harness, runtime)
    } else if runtime.is_cloud() {
        anyagents_harness_claude::CLOUD_DELIVERY
    } else {
        crate::vendor::declared_delivery(harness)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;
    use anyagents_core::stateless::{
        NewRemoteTurn, RemoteTurnEventUpdate, SettleRemoteTurn, hash_callback_token,
    };

    #[test]
    fn agent_view_projects_live_state_from_fresh_pending_remote_turns() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let active_agent = store
            .create_agent(NewAgent::new("remote-worker", "claude-code"))
            .unwrap();
        let stale_agent = store
            .create_agent(NewAgent::new("stale-worker", "codex"))
            .unwrap();
        let done_agent = store
            .create_agent(NewAgent::new("done-worker", "agy"))
            .unwrap();
        for a in [&active_agent, &stale_agent, &done_agent] {
            store.set_agent_running(&a.id, "/work").unwrap();
        }
        let active_agent = store.agent(&active_agent.id).unwrap();
        let stale_agent = store.agent(&stale_agent.id).unwrap();
        let done_agent = store.agent(&done_agent.id).unwrap();

        let created = store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "trn_view_active".into(),
                agent_id: active_agent.id.clone(),
                harness_kind: "claude-code".into(),
                provider_kind: Some("sandbox".into()),
                remote_run_id: Some("run-active".into()),
                callback_token_hash: hash_callback_token("tok-a"),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        let _ = store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "trn_view_done".into(),
                agent_id: done_agent.id.clone(),
                harness_kind: "agy".into(),
                provider_kind: Some("sandbox".into()),
                remote_run_id: Some("run-done".into()),
                callback_token_hash: hash_callback_token("tok-d"),
                cursor: None,
                status: "dispatched".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .settle_turn(
                "trn_view_done",
                &SettleRemoteTurn {
                    status: "completed".into(),
                    result_text: Some("done".into()),
                    error_text: None,
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: created.created_at_ms + 50,
                },
            )
            .unwrap();
        // Record a fresh event timestamp on the active turn.
        store
            .remote_turn_store()
            .record_event(
                "trn_view_active",
                &RemoteTurnEventUpdate {
                    status: Some("running".into()),
                    cursor: None,
                    remote_run_id: None,
                    last_event_at_ms: now_ms(),
                    next_poll_at_ms: None,
                },
            )
            .unwrap();

        // Single-agent inspect projection.
        let single = agent_view(&store, &health, &active_agent).unwrap();
        assert_eq!(single.process, "up");
        assert_eq!(single.turn_state, "in_turn");
        assert!(single.has_open_turn);
        assert!(single.open_turn_age_ms.is_some());

        // Batched `agent_query_metadata` + `roster_views` projection.
        let listed = roster_views(
            &store,
            &health,
            &[&active_agent, &stale_agent, &done_agent],
            0,
            &ListAgentsQuery::default(),
        )
        .unwrap();
        let active_view = listed
            .agents
            .iter()
            .find(|v| v.id == active_agent.id.as_str())
            .unwrap();
        let stale_view = listed
            .agents
            .iter()
            .find(|v| v.id == stale_agent.id.as_str())
            .unwrap();
        let done_view = listed
            .agents
            .iter()
            .find(|v| v.id == done_agent.id.as_str())
            .unwrap();
        assert_eq!(active_view.process, "up");
        assert_eq!(active_view.turn_state, "in_turn");
        assert!(active_view.has_open_turn);
        assert_eq!(stale_view.process, "down");
        assert_eq!(stale_view.turn_state, "idle");
        assert!(!stale_view.has_open_turn);
        assert_eq!(done_view.process, "down");
        assert_eq!(done_view.turn_state, "idle");
        assert!(!done_view.has_open_turn);
    }

    #[test]
    fn serverless_mode_reports_honest_next_step_only_for_wired_harnesses_and_fails_closed() {
        use anyagents_core::ExecutionRuntime;

        for runtime in [
            ExecutionRuntime::Sandbox { provider: None },
            ExecutionRuntime::Local,
        ] {
            for wired in [
                "agy",
                "claude-code",
                "codex",
                "opencode",
                "omp",
                "muse",
                "spiron",
            ] {
                let delivery = declared_delivery_for_serverless(wired, &runtime, true);
                let view = crate::api::DeliveryView::from(&delivery);
                assert_eq!(view.level, "next-step", "{wired} on {runtime:?}");
                assert_eq!(view.legacy_mode(), "steer", "{wired} on {runtime:?}");
                assert_eq!(view.legacy_steer(), "yes", "{wired} on {runtime:?}");
            }
            // Unwired / one-shot harnesses MUST fail closed to turn-end / followup / no.
            for unwired in ["gemini", "unknown-harness"] {
                let delivery = declared_delivery_for_serverless(unwired, &runtime, true);
                let view = crate::api::DeliveryView::from(&delivery);
                assert_eq!(view.level, "turn-end", "{unwired} on {runtime:?}");
                assert_eq!(view.legacy_mode(), "followup", "{unwired} on {runtime:?}");
                assert_eq!(view.legacy_steer(), "no", "{unwired} on {runtime:?}");
            }
        }

        // Cloud runtime: `spiron` cloud has live mid-turn steering wired; unwired cloud fails closed.
        let spiron_cloud =
            declared_delivery_for_serverless("spiron", &ExecutionRuntime::Cloud, true);
        let spiron_view = crate::api::DeliveryView::from(&spiron_cloud);
        assert_eq!(spiron_view.level, "next-step");
        assert_eq!(spiron_view.legacy_mode(), "steer");
        assert_eq!(spiron_view.legacy_steer(), "yes");

        for unwired_cloud in ["claude-code", "http-cloud", "gemini"] {
            let delivery =
                declared_delivery_for_serverless(unwired_cloud, &ExecutionRuntime::Cloud, true);
            let view = crate::api::DeliveryView::from(&delivery);
            assert_eq!(view.level, "turn-end", "{unwired_cloud} cloud");
            assert_eq!(view.legacy_mode(), "followup", "{unwired_cloud} cloud");
            assert_eq!(view.legacy_steer(), "no", "{unwired_cloud} cloud");
        }
    }

    #[test]
    fn archived_roster_fast_path_preserves_aliases_and_proc_lock_probe() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let live = store
            .create_agent(NewAgent::new("live-proc", "codex"))
            .unwrap();
        store.set_agent_running(&live.id, "/work").unwrap();
        let arch = store
            .create_agent(NewAgent::new("arch-alias", "codex"))
            .unwrap();
        store.rename_agent(&arch.id, "arch-one").unwrap();
        store.set_agent_state(&arch.id, "archived").unwrap();
        let arch = store.agent(&arch.id).unwrap();

        assert!(!store.is_agent_proc_locked(&live.id));
        let _lock = store.try_lock_agent(&live.id).unwrap();
        assert!(store.is_agent_proc_locked(&live.id));

        let listed = roster_views(
            &store,
            &health,
            &[&arch],
            1,
            &ListAgentsQuery {
                state: Some("archived".into()),
                ..ListAgentsQuery::default()
            },
        )
        .unwrap();
        assert_eq!(listed.agents.len(), 1);
        assert_eq!(listed.agents[0].aliases, vec!["arch-alias".to_string()]);
    }

    #[test]
    fn agent_view_surfaces_failed_remote_turn_error_and_clears_on_completed_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("remote-parent", "codex"))
            .unwrap();
        store.set_agent_running(&parent.id, "/work").unwrap();
        let worker = store
            .create_agent(NewAgent {
                parent_id: Some(parent.id.clone()),
                ..NewAgent::new("remote-err-worker", "codex")
            })
            .unwrap();
        store.set_agent_running(&worker.id, "/work").unwrap();
        let worker = store.agent(&worker.id).unwrap();

        let t1 = store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "trn_err_1".into(),
                agent_id: worker.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:1".into()),
                callback_token_hash: hash_callback_token("tok-err-1"),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .settle_turn(
                "trn_err_1",
                &SettleRemoteTurn {
                    status: "failed".into(),
                    result_text: None,
                    error_text: Some("sandbox lease Pending Unschedulable".into()),
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: t1.created_at_ms + 100,
                },
            )
            .unwrap();

        // Single-agent inspect and bulk roster both surface the failed remote turn's error_text
        // without injecting duplicate parent error notices on read.
        let single = agent_view(&store, &health, &worker).unwrap();
        assert_eq!(
            single.last_error.as_deref(),
            Some("sandbox lease Pending Unschedulable")
        );
        assert_eq!(single.turn_state, "errored");
        assert_eq!(single.block_reason.as_deref(), Some("delivery_error"));
        assert!(store.list_undelivered(&parent.id, 10).unwrap().is_empty());

        let listed =
            roster_views(&store, &health, &[&worker], 0, &ListAgentsQuery::default()).unwrap();
        assert_eq!(
            listed.agents[0].last_error.as_deref(),
            Some("sandbox lease Pending Unschedulable")
        );
        assert_eq!(listed.agents[0].turn_state, "errored");

        // While a newer remote turn is actively running, the view projects live in-turn status.
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "trn_ok_2".into(),
                agent_id: worker.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:2".into()),
                callback_token_hash: hash_callback_token("tok-ok-2"),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        let active_view = agent_view(&store, &health, &worker).unwrap();
        assert_eq!(active_view.process, "up");
        assert_eq!(active_view.turn_state, "in_turn");
        assert_eq!(active_view.last_error, None);

        // Settling the newer turn as completed clears AgentView.last_error in both paths.
        store
            .remote_turn_store()
            .settle_turn(
                "trn_ok_2",
                &SettleRemoteTurn {
                    status: "completed".into(),
                    result_text: Some("done".into()),
                    error_text: None,
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: t1.created_at_ms + 300,
                },
            )
            .unwrap();
        let recovered = agent_view(&store, &health, &worker).unwrap();
        assert_eq!(recovered.last_error, None);
        assert_eq!(recovered.turn_state, "idle");
        assert_eq!(recovered.block_reason, None);

        let listed_after =
            roster_views(&store, &health, &[&worker], 0, &ListAgentsQuery::default()).unwrap();
        assert_eq!(listed_after.agents[0].last_error, None);
        assert_eq!(listed_after.agents[0].turn_state, "idle");
    }

    #[test]
    fn newer_failed_remote_turn_supersedes_older_local_persisted_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let worker = store
            .create_agent(NewAgent::new("local-vs-remote", "codex"))
            .unwrap();
        store.set_agent_running(&worker.id, "/work").unwrap();
        let worker = store.agent(&worker.id).unwrap();

        let cfg = anyagents_core::agent_config_dir(store.data_dir(), &worker.id);
        std::fs::create_dir_all(&cfg).unwrap();
        let base = now_ms();
        std::fs::write(
            cfg.join(LAST_ERROR),
            serde_json::json!({
                "timestamp_ms": base - 5_000,
                "error": "older local error",
            })
            .to_string(),
        )
        .unwrap();

        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "trn_newer_err".into(),
                agent_id: worker.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: None,
                callback_token_hash: hash_callback_token("tok-newer"),
                cursor: None,
                status: "dispatched".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .settle_turn(
                "trn_newer_err",
                &SettleRemoteTurn {
                    status: "failed".into(),
                    result_text: None,
                    error_text: Some("newer remote timeout 1800s".into()),
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: base + 100,
                },
            )
            .unwrap();

        let view = agent_view(&store, &health, &worker).unwrap();
        assert_eq!(
            view.last_error.as_deref(),
            Some("newer remote timeout 1800s")
        );
    }
}
