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
    pub(super) persistent: Option<bool>,
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

fn can_read_agent_schedules(caller: &Agent, target: &Agent, is_descendant: bool) -> bool {
    is_descendant || (!caller.is_isolated() && target.scope() == "public")
}

/// Authorize read-only schedule listing for a single target agent:
/// `self`, any descendant, or any `scope == "public"` peer (unless caller is `isolated`),
/// or an Operator principal. Mutations MUST use [`authorize_schedule_target`].
pub(super) fn authorize_schedule_read_target(
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
            let caller = store.agent(agent_id).map_err(ApiError::from_store)?;
            let clean = name.strip_prefix("agent:").unwrap_or(name);
            if clean.eq_ignore_ascii_case("self") {
                return Ok(caller);
            }
            let found = store.agent_by_name(clean).map_err(ApiError::from_store)?;
            let is_desc = &found.id == agent_id || is_self_or_ancestor(store, agent_id, &found);
            if can_read_agent_schedules(&caller, &found, is_desc) {
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

/// Authorize mutating (`create`/`cancel`) schedule authority strictly downward-only
/// (`self + descendants`) or Operator. Public peers are NEVER mutable by other peers.
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
                (Some(caller), Some(agent))
                    if can_read_agent_schedules(
                        caller,
                        &agent,
                        is_descendant_in_map(&caller.id, &agent),
                    ) =>
                {
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
        let target = authorize_schedule_read_target(&state.store, &principal, Some(&name))?;
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

        if body.persistent == Some(true) && !matches!(verb, ScheduleVerb::Create) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "`persistent` is only supported when creating a webhook/event watch",
            ));
        }
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
                let target = authorize_schedule_read_target(
                    &state.store,
                    &principal,
                    body.agent.as_deref(),
                )?;
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
                let created = crate::schedule::create_agent_schedule_with_mode(
                    &state.store,
                    &effective_health,
                    &target,
                    body.at.as_deref(),
                    body.every.as_deref(),
                    body.watch.as_deref(),
                    prompt,
                    body.persistent.unwrap_or(false),
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyagents_core::{NewAgent, NewSchedule};
    use anyagents_supervisor::HealthBoard;

    use super::*;

    fn test_state(data: &std::path::Path) -> AppState {
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
            auth: crate::auth::Mode::Trusted {
                data_dir: data.into(),
            },
        }
    }

    #[tokio::test]
    async fn public_peer_schedules_are_read_only_visible_and_private_peer_subagents_remain_hidden()
    {
        let dir = crate::test_tempdir();
        let state = test_state(dir.path());
        let store = &state.store;

        let alpha = store
            .create_agent(NewAgent::new("peer-alpha", "codex"))
            .unwrap();
        let alpha_task = store
            .create_agent(NewAgent {
                parent_id: Some(alpha.id.clone()),
                ..NewAgent::new("alpha-private-task", "codex")
            })
            .unwrap();
        let beta = store
            .create_agent(NewAgent::new("peer-beta", "codex"))
            .unwrap();
        let beta_private_task = store
            .create_agent(NewAgent {
                parent_id: Some(beta.id.clone()),
                ..NewAgent::new("beta-private-task", "codex")
            })
            .unwrap();
        let beta_public_sub = store
            .create_agent(NewAgent {
                parent_id: Some(beta.id.clone()),
                ..NewAgent::new("beta-public-standing", "codex")
            })
            .unwrap();
        store
            .set_agent_scope_as(&beta_public_sub.id, "public", "operator")
            .unwrap();
        let isolated_caller = store
            .create_agent(NewAgent::new("isolated-peer", "codex"))
            .unwrap();
        store
            .set_agent_scope_as(&isolated_caller.id, "isolated", "operator")
            .unwrap();
        let private_root = store
            .create_agent(NewAgent::new("private-root", "codex"))
            .unwrap();
        store
            .set_agent_scope_as(&private_root.id, "private", "operator")
            .unwrap();

        for agent in [
            &alpha,
            &alpha_task,
            &beta,
            &beta_private_task,
            &beta_public_sub,
            &isolated_caller,
            &private_root,
        ] {
            store
                .create_schedule(NewSchedule {
                    agent_id: agent.id.clone(),
                    kind: "watch".into(),
                    spec: "file:/tmp/peer-schedule-fixture".into(),
                    prompt: format!("schedule for {}", agent.name),
                    interval_ms: None,
                    next_fire_at_ms: None,
                    watch_baseline: None,
                })
                .unwrap();
        }

        let alpha_principal = Principal::Agent {
            agent_id: alpha.id.clone(),
            name: alpha.name.clone(),
        };
        let isolated_principal = Principal::Agent {
            agent_id: isolated_caller.id.clone(),
            name: isolated_caller.name.clone(),
        };

        // (a) Peer sees self + own descendants + scope=="public" peers (root and public sub-agent).
        let list_all: ScheduleRequest =
            serde_json::from_value(serde_json::json!({"action": "list", "agent": "all"})).unwrap();
        let resp = handle_schedule_action(alpha_principal.clone(), state.clone(), list_all)
            .await
            .unwrap()
            .0;
        let visible_owners: std::collections::BTreeSet<String> = resp["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["agent_name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            visible_owners,
            [
                "peer-alpha",
                "alpha-private-task",
                "peer-beta",
                "beta-public-standing",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        // (b) Peer does NOT see another peer's scope=="private" task sub-agent, private root, or isolated peer.
        assert!(!visible_owners.contains("beta-private-task"));
        assert!(!visible_owners.contains("private-root"));
        assert!(!visible_owners.contains("isolated-peer"));

        // An isolated caller sees ONLY self + descendants (never public peers).
        let isolated_all = list_fleet_schedules(store, &isolated_principal).unwrap();
        let isolated_owners: Vec<&str> = isolated_all["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["agent_name"].as_str().unwrap())
            .collect();
        assert_eq!(isolated_owners, vec!["isolated-peer"]);

        // Single-agent read-only list matches: public peer allowed, private peer sub-agent forbidden.
        let read_beta = list_schedules_route(
            alpha_principal.clone(),
            State(state.clone()),
            AxumPath("peer-beta".into()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(read_beta["schedules"].as_array().unwrap().len(), 1);
        let read_private_err = list_schedules_route(
            alpha_principal.clone(),
            State(state.clone()),
            AxumPath("beta-private-task".into()),
        )
        .await
        .unwrap_err();
        assert_eq!(read_private_err.status, StatusCode::FORBIDDEN);

        // (c) Peer CANNOT cancel or create/mutate a public peer's schedule (403 Forbidden).
        for mutation in [
            serde_json::json!({"action": "cancel", "agent": "peer-beta", "id": "all"}),
            serde_json::json!({"action": "cancel", "agent": "beta-public-standing", "id": "all"}),
            serde_json::json!({"action": "every", "agent": "peer-beta", "every": "1h", "prompt": "no"}),
            serde_json::json!({"action": "at", "agent": "beta-public-standing", "at": "1h", "prompt": "no"}),
        ] {
            let req: ScheduleRequest = serde_json::from_value(mutation).unwrap();
            let err = handle_schedule_action(alpha_principal.clone(), state.clone(), req)
                .await
                .unwrap_err();
            assert_eq!(err.status, StatusCode::FORBIDDEN);
        }
        // Public peers' schedules remain untouched in the store.
        assert_eq!(store.list_schedules(Some(&beta.id), true).unwrap().len(), 1);
        assert_eq!(
            store
                .list_schedules(Some(&beta_public_sub.id), true)
                .unwrap()
                .len(),
            1
        );
    }
}
