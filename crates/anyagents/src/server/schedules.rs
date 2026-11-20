//! Agent and fleet schedule CRUD and action HTTP routes.

use std::collections::HashMap;

use anyagents_core::{Agent, Store};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use crate::auth::Principal;

use super::types::{
    ApiError, AppState, is_self_or_ancestor, require_operator_audited, spawn_store,
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/agents/{name}/schedules",
            get(list_schedules_route).post(schedules_action_route),
        )
        .route(
            "/v1/schedules",
            get(list_self_schedules_route).post(self_schedules_action_route),
        )
}

/// Unused fields arrive as `null` or `"null"`; both are absent.
#[derive(Deserialize, Default)]
pub(super) struct ScheduleRequest {
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) action: Option<String>,
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) at: Option<String>,
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) every: Option<String>,
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) watch: Option<String>,
    /// A trigger value or mode under a generic name; see
    /// `schedule::resolve_schedule_call`.
    #[serde(
        default,
        alias = "kind",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) spec: Option<String>,
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) prompt: Option<String>,
    #[serde(
        default,
        alias = "schedule_id",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) id: Option<String>,
    #[serde(
        default,
        alias = "name",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) agent: Option<String>,
}

pub(super) fn authorize_schedule_target(
    store: &Store,
    principal: &Principal,
    target_name: Option<&str>,
) -> Result<Agent, ApiError> {
    match (
        principal,
        target_name.map(str::trim).filter(|s| !s.is_empty()),
    ) {
        (Principal::Agent { agent_id, .. }, None) => {
            store.agent(agent_id).map_err(ApiError::from_store)
        }
        (Principal::Agent { agent_id, .. }, Some(name)) => {
            let clean = name.strip_prefix("agent:").unwrap_or(name);
            if clean.eq_ignore_ascii_case("self") {
                return store.agent(agent_id).map_err(ApiError::from_store);
            }
            let found = store.agent_by_name(clean).map_err(ApiError::from_store)?;
            if &found.id == agent_id
                || (agent_id != &found.id && is_self_or_ancestor(store, agent_id, &found))
            {
                return Ok(found);
            }
            require_operator_audited(store, principal, "agents.schedule", &found.name)?;
            Ok(found)
        }
        (Principal::Operator, Some(name)) => {
            let clean = name.strip_prefix("agent:").unwrap_or(name);
            store.agent_by_name(clean).map_err(ApiError::from_store)
        }
        (Principal::Operator, None) => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "operator schedule call requires an agent name",
        )),
    }
}

fn is_fleet_schedule_target(target: Option<&str>) -> bool {
    target.is_some_and(|name| {
        let name = name.trim();
        name.strip_prefix("agent:")
            .unwrap_or(name)
            .eq_ignore_ascii_case("all")
    })
}

/// Roster visibility is not schedule authority. Reuse the per-owner authorization
/// boundary, skip only permission denials, and propagate unreadable state.
pub(super) fn list_fleet_schedules(
    store: &Store,
    principal: &Principal,
) -> Result<serde_json::Value, ApiError> {
    // Reject unknown/absent agent principals fail-closed even when no schedules exist.
    let caller_agent = match principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => {
            Some(store.agent(agent_id).map_err(ApiError::from_store)?)
        }
    };
    // Load active schedules and live agents in 2 batched queries instead of N+1
    // per-owner ancestry lookups.
    let all_schedules = store
        .list_schedules(None, true)
        .map_err(ApiError::from_store)?;
    if all_schedules.is_empty() {
        return Ok(serde_json::json!({"agent": "all", "schedules": []}));
    }
    let live_agents: HashMap<anyagents_core::AgentId, Agent> = store
        .list_live_agents()
        .map_err(ApiError::from_store)?
        .into_iter()
        .map(|a| (a.id.clone(), a))
        .collect();
    let is_descendant_in_map = |ancestor: &anyagents_core::AgentId, target: &Agent| -> bool {
        if &target.id == ancestor {
            return true;
        }
        let mut cur = target.parent_id.clone();
        for _ in 0..16 {
            let Some(pid) = cur else {
                return false;
            };
            if &pid == ancestor {
                return true;
            }
            cur = live_agents
                .get(&pid)
                .map(|p| p.parent_id.clone())
                .unwrap_or_else(|| store.agent(&pid).ok().and_then(|p| p.parent_id));
        }
        false
    };

    let mut authorized_names: HashMap<String, Option<String>> = HashMap::new();
    let mut schedules = Vec::with_capacity(all_schedules.len());
    for sched in all_schedules {
        let owner_id = sched.agent_id.as_str().to_string();
        let owner_name = if let Some(cached) = authorized_names.get(&owner_id) {
            cached.clone()
        } else {
            let owner_opt = live_agents
                .get(&sched.agent_id)
                .cloned()
                .or_else(|| store.agent(&sched.agent_id).ok());
            let resolved = match (&caller_agent, owner_opt) {
                (_, None) => None,
                (None, Some(agent)) => Some(agent.name),
                (Some(caller), Some(agent)) if is_descendant_in_map(&caller.id, &agent) => {
                    Some(agent.name)
                }
                (Some(_), Some(_)) => None,
            };
            authorized_names.insert(owner_id.clone(), resolved.clone());
            resolved
        };
        let Some(owner_name) = owner_name else {
            continue;
        };
        let schedule = crate::api::ScheduleView::from(sched);
        let mut row = serde_json::to_value(&schedule)
            .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        row["agent_name"] = serde_json::json!(owner_name);
        row["next_fire_at_ms"] = serde_json::json!(schedule.next_fire_at_ms);
        row["last_observed"] = serde_json::json!(schedule.last_observed);
        schedules.push(row);
    }
    Ok(serde_json::json!({"agent": "all", "schedules": schedules}))
}

pub(super) async fn list_schedules_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if is_fleet_schedule_target(Some(&name)) {
            return list_fleet_schedules(&state.store, &principal).map(Json);
        }
        let target = authorize_schedule_target(&state.store, &principal, Some(&name))?;
        let schedules: Vec<crate::api::ScheduleView> = state
            .store
            .list_schedules(Some(&target.id), true)
            .map_err(ApiError::from_store)?
            .into_iter()
            .map(crate::api::ScheduleView::from)
            .collect();
        Ok(Json(serde_json::json!({
            "agent": target.name,
            "agent_id": target.id.as_str(),
            "schedules": schedules,
        })))
    })
    .await
}

async fn list_self_schedules_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || match &principal {
        Principal::Agent { agent_id, name } => {
            let schedules: Vec<crate::api::ScheduleView> = state
                .store
                .list_schedules(Some(agent_id), true)
                .map_err(ApiError::from_store)?
                .into_iter()
                .map(crate::api::ScheduleView::from)
                .collect();
            Ok(Json(serde_json::json!({
                "agent": name,
                "agent_id": agent_id.as_str(),
                "schedules": schedules,
            })))
        }
        Principal::Operator => {
            let schedules: Vec<crate::api::ScheduleView> = state
                .store
                .list_schedules(None, true)
                .map_err(ApiError::from_store)?
                .into_iter()
                .map(crate::api::ScheduleView::from)
                .collect();
            Ok(Json(serde_json::json!({
                "schedules": schedules,
            })))
        }
    })
    .await
}

async fn schedules_action_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(mut body): Json<ScheduleRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    body.agent = Some(name);
    handle_schedule_action(principal, state, body).await
}

async fn self_schedules_action_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<ScheduleRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    handle_schedule_action(principal, state, body).await
}

pub(super) async fn handle_schedule_action(
    principal: Principal,
    state: AppState,
    mut body: ScheduleRequest,
) -> Result<Json<serde_json::Value>, ApiError> {
    use crate::schedule::ScheduleVerb;
    spawn_store(move || {
        let mut trigger = crate::schedule::ScheduleTrigger {
            at: body.at.take(),
            every: body.every.take(),
            watch: body.watch.take(),
            spec: body.spec.take(),
        };
        let verb = crate::schedule::resolve_schedule_call(
            body.action.as_deref(),
            &mut trigger,
            body.id.is_some(),
        )
        .map_err(|msg| ApiError::new(StatusCode::BAD_REQUEST, msg))?;
        body.at = trigger.at;
        body.every = trigger.every;
        body.watch = trigger.watch;

        if is_fleet_schedule_target(body.agent.as_deref()) {
            if !matches!(verb, ScheduleVerb::List) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "fleet schedules are read-only; select one agent to create or cancel",
                ));
            }
            return list_fleet_schedules(&state.store, &principal).map(Json);
        }

        match verb {
            ScheduleVerb::List => {
                if matches!(principal, Principal::Operator) && body.agent.is_none() {
                    let schedules: Vec<crate::api::ScheduleView> = state
                        .store
                        .list_schedules(None, true)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .map(crate::api::ScheduleView::from)
                        .collect();
                    return Ok(Json(serde_json::json!({ "schedules": schedules })));
                }
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "schedules": schedules,
                })))
            }
            ScheduleVerb::Create => {
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let prompt = body.prompt.as_deref().unwrap_or("");
                let (effective_health, _) =
                    super::serverless_watches::effective_health_with_remote_turns(
                        &state.store,
                        &state.health,
                        anyagents_core::now_ms(),
                    );
                let created = crate::schedule::create_agent_schedule(
                    &state.store,
                    &effective_health,
                    &target,
                    body.at.as_deref(),
                    body.every.as_deref(),
                    body.watch.as_deref(),
                    prompt,
                )
                .map_err(|msg| ApiError::new(StatusCode::BAD_REQUEST, msg))?;
                if let Ok(Some(sched_row)) = state.store.get_schedule(&created.id)
                    && sched_row.next_fire_at_ms.is_some()
                {
                    // Cancel any prior external run ID when deduplicating/updating an existing schedule.
                    if sched_row.external_timer_id.is_some() {
                        crate::timer::cancel_schedule_timer(
                            state.timer_backend.as_ref(),
                            &sched_row,
                        );
                    }
                    crate::timer::arm_schedule_timer(
                        &state.store,
                        state.timer_backend.as_ref(),
                        &sched_row,
                    )
                    .map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, err))?;
                }
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "schedule": created,
                    "schedules": schedules,
                })))
            }
            ScheduleVerb::Cancel => {
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let id_or_all = body
                    .id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("all");
                let cancelled_rows = state
                    .store
                    .cancel_schedules_returning(Some(&target.id), id_or_all)
                    .map_err(ApiError::from_store)?;
                for sched in &cancelled_rows {
                    crate::timer::cancel_schedule_timer(state.timer_backend.as_ref(), sched);
                }
                let cancelled = cancelled_rows.len();
                crate::schedule::schedules_changed(state.store.data_dir());
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "cancelled": cancelled,
                    "schedules": schedules,
                })))
            }
        }
    })
    .await
}
