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

#[derive(Default, Deserialize)]
pub(crate) struct ListAgentsQuery {
    pub state: Option<String>,
    pub parent: Option<String>,
    pub name: Option<String>,
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
        let reads_private = self.is_self_or_ancestor(&self.caller.id, agent)
            || (self.caller.is_admin && self.caller.parent_id.is_none());
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
    // Only the selected state and its ancestors are read; the archive is
    // counted by index instead of being deserialized row by row.
    let (roster, archived_count) = state
        .store
        .roster_agents(query.state.as_deref())
        .map_err(ApiError::from_store)?;
    let caller = caller_agent
        .as_ref()
        .map(|caller| RosterCaller::new(&state.store, caller, &roster));
    let visible: Vec<&Agent> = roster
        .iter()
        .filter(|agent| caller.as_ref().is_none_or(|caller| caller.sees(agent)))
        .collect();
    let mut listed = roster_views(&state.store, &state.health, &visible, archived_count, query)?;
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
    let mut selected = HashSet::new();
    for agent in all_agents {
        let state_matches = match query.state.as_deref() {
            Some("all") => true,
            Some(state) => agent.state == state,
            None => agent.state != "archived",
        };
        let parent_matches = match query.parent.as_deref() {
            Some("root" | "none") => agent.parent_id.is_none(),
            _ => parent
                .as_ref()
                .is_none_or(|p| agent.parent_id.as_ref() == Some(&p.id)),
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
    let mut metadata = store.agent_query_metadata().map_err(ApiError::from_store)?;
    let files = file_cache::cache();
    files.catch_up();
    let seats = view_seats(files);
    let mut unsettled_by_agent: HashMap<String, Vec<crate::api::UnsettledOutboundView>> =
        HashMap::new();
    for held in outbound_history::unsettled(store) {
        unsettled_by_agent
            .entry(held.agent_id.as_str().to_string())
            .or_default()
            .push(unsettled_view(&held));
    }
    let mut secrets_by_agent = store.all_agent_secret_grants().unwrap_or_default();
    let mut agents = Vec::with_capacity(selected.len());
    for agent in all_agents {
        if !selected.contains(agent.id.as_str()) {
            continue;
        }
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
    let Some(record) = markers
        .read(LAST_ERROR)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
    else {
        return (None, None);
    };
    let floor = if harness == "agy" {
        rotated_ms.max(markers.ms(QUOTA_CLEARED))
    } else {
        rotated_ms
    };
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
        .map(str::to_string);
    if harness == "claude-code" {
        return (None, error);
    }
    match anyagents_harness::quota::recorded_reset_at_ms(&record) {
        Some(reset_ms) if reset_ms > now => (Some(reset_ms), error),
        Some(_) => (None, None),
        None => (None, error),
    }
}

pub(super) fn archived_agent_view_fast(agent: &Agent) -> AgentView {
    let delivery: crate::api::DeliveryView =
        (&crate::vendor::declared_delivery(&agent.harness)).into();
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
        instructions: agent.instructions.clone(),
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
            // Avoid O(N) parent-chain DB queries across thousands of archived agents.
            view.instructions = agent.instructions.clone();
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
    // The supervisor's board is the live state; a fault outlives its process
    // only as the record an adapter persisted. A view never reads a vendor
    // transcript, log or session store.
    let (session_quota, session_error) = persisted_fault(&agent.harness, &markers, rotated_ms, now);
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
    ) = match health.get(&agent.id) {
        Some(live) => {
            // The supervisor retries a transient capacity fault by itself and
            // publishes the work as in progress: neither the fault nor the
            // record its adapter persisted for it is an error of the agent's.
            let retry_at_ms = live.retry_at_ms.filter(|_| live.process_alive);
            let (session_quota, session_error) = if retry_at_ms.is_some() {
                (None, None)
            } else {
                (session_quota, session_error)
            };
            let effective_quota = live.quota_reset_at_ms.or(session_quota);
            let effective_err = live.last_error.clone().or(session_error);
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
                    .unwrap_or_else(|| declared_delivery_for_mode(&agent.harness)),
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
            )
        }
        None => {
            if let Some(started_ms) = active_remote_started_ms {
                (
                    "up",
                    "in_turn",
                    declared_delivery_for_mode(&agent.harness),
                    session_quota,
                    Some(agent.harness.clone()),
                    agent.model.clone(),
                    Some(started_ms),
                    false,
                    session_error,
                    None,
                )
            } else {
                let lock_busy = matches!(
                    store.try_lock_agent(&agent.id),
                    Err(anyagents_core::Error::AgentBusy)
                );
                let proc_str = if lock_busy { "alive" } else { "down" };
                let turn_str = if lock_busy { "working" } else { "idle" };
                (
                    proc_str,
                    turn_str,
                    declared_delivery_for_mode(&agent.harness),
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
                    session_error,
                    None,
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
            if markers.parent_notice_due(agent, rotated_ms, err_msg) {
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

/// Honest delivery semantics for the current runtime mode: in serverless per-turn
/// dispatch, mid-turn stdin steering is inactive, so never advertise `steer_support: "yes"`.
fn declared_delivery_for_mode(harness: &str) -> anyagents_harness::Delivery {
    declared_delivery_for_serverless(harness, serverless::is_serverless_mode())
}

fn declared_delivery_for_serverless(
    harness: &str,
    serverless_mode: bool,
) -> anyagents_harness::Delivery {
    if serverless_mode {
        anyagents_harness_stream::LINE_PER_TURN
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
    fn serverless_mode_reports_honest_followup_and_no_steer_support() {
        let delivery = declared_delivery_for_serverless("claude-code", true);
        let view = crate::api::DeliveryView::from(&delivery);
        assert_eq!(view.legacy_mode(), "followup");
        assert_eq!(view.legacy_steer(), "no");
    }
}
