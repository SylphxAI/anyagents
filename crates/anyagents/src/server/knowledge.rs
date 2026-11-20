//! Coordinator boundary for versioned knowledge. All namespace authorization
//! lives in the store; only this extractor may map OIDC to the owner actor.
use anyagents_core::knowledge::{Actor, Edit, Mode, Scope};
use anyagents_core::{AgentId, Error, Store};
use axum::extract::{DefaultBodyLimit, FromRequestParts, State};
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use serde::Deserialize;
use serde_json::{Value, json};

use super::types::{AppState, spawn_store};
use crate::auth::{AuthError, AuthRejection, Presented, Principal};

mod retrieval;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/knowledge", post(call))
        .layer(DefaultBodyLimit::max(32 * 1024))
}

struct KnowledgeActor(Actor);

impl FromRequestParts<AppState> for KnowledgeActor {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let oidc = Presented::of(parts).is_oidc();
        match Principal::from_request_parts(parts, state).await? {
            Principal::Agent { agent_id, .. } => Ok(Self(Actor::Agent(agent_id))),
            Principal::Operator
                if oidc
                    && parts
                        .extensions
                        .get::<crate::auth::InjectedPrincipal>()
                        .is_none() =>
            {
                Ok(Self(Actor::Owner("owner".into())))
            }
            Principal::Operator => Err(AuthRejection(AuthError::Forbidden(
                "knowledge requires an OIDC owner or an agent session".into(),
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Request {
    List {
        scope: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
        prefix: Option<String>,
    },
    Read {
        namespace: String,
        topic: String,
    },
    #[serde(alias = "upsert")]
    Write {
        namespace: String,
        topic: String,
        expected_version: Option<i64>,
        #[serde(flatten)]
        edit: Edit,
    },
    Delete {
        namespace: String,
        topic: String,
        expected_version: i64,
        reason: String,
    },
    Revisions {
        namespace: String,
        topic: String,
        before_version: Option<i64>,
        limit: Option<usize>,
    },
    Rollback {
        namespace: String,
        topic: String,
        to_version: i64,
        expected_version: Option<i64>,
        reason: String,
    },
    Propose {
        namespace: String,
        topic: String,
        expected_version: i64,
        #[serde(flatten)]
        edit: Edit,
    },
    Proposals {
        namespace: String,
        after: Option<String>,
        limit: Option<usize>,
    },
    Promote {
        namespace: String,
        proposal_id: String,
        expected_version: i64,
    },
    Reject {
        namespace: String,
        proposal_id: String,
    },
    Search {
        scope: Option<String>,
        query: String,
        limit: Option<usize>,
    },
    Grant {
        agent_id: String,
        namespace_pattern: String,
        mode: Mode,
    },
    Grants {
        agent_id: Option<String>,
        after: Option<String>,
        limit: Option<usize>,
    },
    Revoke {
        grant_id: String,
    },
}

async fn call(
    State(state): State<AppState>,
    KnowledgeActor(actor): KnowledgeActor,
    Json(request): Json<Request>,
) -> Response {
    match spawn_store(move || Ok(execute(&state.store, &actor, request))).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(error)) => response_error(error),
        Err(error) => error.into_response(),
    }
}

fn response_error(error: Error) -> Response {
    let (status, value) = match error {
        Error::KnowledgeConflict { current } => (
            StatusCode::CONFLICT,
            json!({"error": error.to_string(), "current_version": current}),
        ),
        Error::KnowledgeReviewed => (StatusCode::CONFLICT, json!({"error": error.to_string()})),
        Error::KnowledgeMissing => (StatusCode::NOT_FOUND, json!({"error": error.to_string()})),
        Error::PermissionDenied(_) => (StatusCode::FORBIDDEN, json!({"error": error.to_string()})),
        Error::KnowledgeInvalid(_) => {
            (StatusCode::BAD_REQUEST, json!({"error": error.to_string()}))
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "knowledge operation failed"}),
        ),
    };
    (status, Json(value)).into_response()
}

fn agent_id(text: String) -> Result<AgentId, Error> {
    AgentId::parse(&text).map_err(|_| Error::KnowledgeInvalid("invalid agent ID".into()))
}

fn execute(store: &Store, actor: &Actor, request: Request) -> Result<Value, Error> {
    Ok(match request {
        Request::List {
            scope,
            limit,
            offset,
            prefix,
        } => {
            let scope = Scope::parse(scope.as_deref().unwrap_or("all"))?;
            let offset = offset.unwrap_or(0);
            let limit = limit.unwrap_or(25).clamp(1, 50);
            let rows = store.knowledge_list(
                actor,
                &scope,
                limit + 1,
                offset,
                prefix.as_deref().unwrap_or_default(),
            )?;
            list_page(&rows, offset, limit)?
        }
        Request::Read { namespace, topic } => {
            json!(store.knowledge_read(actor, &namespace, &topic)?)
        }
        Request::Write {
            namespace,
            topic,
            expected_version,
            edit,
        } => receipt(store.knowledge_write(
            actor,
            &namespace,
            &topic,
            expected_version,
            &edit,
            false,
        )?),
        Request::Delete {
            namespace,
            topic,
            expected_version,
            reason,
        } => receipt(store.knowledge_write(
            actor,
            &namespace,
            &topic,
            Some(expected_version),
            &Edit {
                reason,
                ..Edit::default()
            },
            true,
        )?),
        Request::Revisions {
            namespace,
            topic,
            before_version,
            limit,
        } => {
            let rows = store.knowledge_revisions(
                actor,
                &namespace,
                &topic,
                before_version,
                limit.unwrap_or(5),
            )?;
            json!({"revisions": rows, "before_version": rows.last().map(|row| row.version)})
        }
        Request::Rollback {
            namespace,
            topic,
            to_version,
            expected_version,
            reason,
        } => json!(store.knowledge_rollback(
            actor,
            &namespace,
            &topic,
            to_version,
            expected_version,
            &reason
        )?),
        Request::Propose {
            namespace,
            topic,
            expected_version,
            edit,
        } => json!(store.knowledge_propose(actor, &namespace, &topic, expected_version, &edit)?),
        Request::Proposals {
            namespace,
            after,
            limit,
        } => {
            let rows = store.knowledge_proposals(
                actor,
                &namespace,
                after.as_deref().unwrap_or_default(),
                limit.unwrap_or(5),
            )?;
            json!({"proposals": rows, "after": rows.last().map(|row| &row.id)})
        }
        Request::Promote {
            namespace,
            proposal_id,
            expected_version,
        } => {
            let (proposal, doc) = store.knowledge_review(
                actor,
                &namespace,
                &proposal_id,
                Some(expected_version),
                true,
            )?;
            json!({"proposal": proposal, "doc": doc.map(receipt)})
        }
        Request::Reject {
            namespace,
            proposal_id,
        } => json!(
            store
                .knowledge_review(actor, &namespace, &proposal_id, None, false)?
                .0
        ),
        Request::Search {
            scope,
            query,
            limit,
        } => retrieval::search(
            store,
            actor,
            &Scope::parse(scope.as_deref().unwrap_or("all"))?,
            &query,
            limit.unwrap_or(20),
        )?,
        Request::Grant {
            agent_id: id,
            namespace_pattern,
            mode,
        } => json!(store.knowledge_grant(actor, &agent_id(id)?, &namespace_pattern, mode)?),
        Request::Grants {
            agent_id: id,
            after,
            limit,
        } => {
            let id = id.map(agent_id).transpose()?;
            let mut rows = store.knowledge_grants(
                actor,
                id.as_ref(),
                after.as_deref().unwrap_or_default(),
                limit.unwrap_or(50).clamp(1, 100) + 1,
            )?;
            let limit = limit.unwrap_or(50).clamp(1, 100);
            let more = rows.len() > limit;
            rows.truncate(limit);
            json!({"grants": rows, "next_after": rows.last().filter(|_| more).map(|row| &row.id)})
        }
        Request::Revoke { grant_id } => {
            json!({"revoked": store.knowledge_revoke(actor, &grant_id)?})
        }
    })
}

// A write capability is not a read capability: metadata-only changes must not
// echo the old body or metadata to a write-only agent.
fn receipt(doc: anyagents_core::knowledge::Doc) -> Value {
    json!({"namespace": doc.namespace, "topic": doc.topic, "version": doc.version})
}

fn list_page(
    rows: &[anyagents_core::knowledge::Summary],
    offset: usize,
    limit: usize,
) -> Result<Value, Error> {
    // Account for the actual compact JSON, including escaping and pagination.
    // Every individual summary fits, even with maximum-length control strings.
    let mut count = rows.len().min(limit);
    loop {
        let next_offset = (count < rows.len()).then(|| offset.saturating_add(count));
        let page = json!({"items": &rows[..count], "next_offset": next_offset});
        if page.to_string().len() < 8192 {
            return Ok(page);
        }
        if count <= 1 {
            return Err(Error::KnowledgeInvalid(
                "knowledge summary exceeds page budget".into(),
            ));
        }
        count -= 1;
    }
}

#[cfg(test)]
mod tests;

/// Only a mailbox-backed cloud dispatch requests wake context. The submitted
/// prompt is not copied into durable turn metadata or agent instructions.
pub(super) async fn cloud_wake_prompt(
    store: &std::sync::Arc<Store>,
    input: &super::serverless::ServerlessDispatchInput,
) -> String {
    if input.message_seq.is_some() && input.outbound_message.is_some() {
        let id = input.agent.id.clone();
        let store = store.clone();
        if let Ok(Some(section)) = spawn_store(move || {
            store
                .knowledge_wake_context(&id)
                .map_err(super::types::ApiError::from_store)
        })
        .await
            && let Some((marker, rest)) = input.prompt.split_once('\n')
            && anyagents_harness::message_id_in_text(marker).is_some()
        {
            return format!("{marker}\n{section}\n{rest}");
        }
    }
    input.prompt.clone()
}

// Sandbox prompts are staged on disk. Fetch wake context in the detached
// guest immediately before delivery instead, using its existing per-turn token.
pub(super) fn sandbox_router() -> Router<AppState> {
    Router::new()
        .route("/v1/internal/guest/knowledge-wake", post(sandbox_wake))
        .layer(DefaultBodyLimit::max(1024))
}

#[derive(Deserialize)]
struct WakeRequest {
    turn_id: String,
}

async fn sandbox_wake(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<WakeRequest>,
) -> Result<Json<Value>, super::types::ApiError> {
    use super::types::ApiError;
    let unauthorized = || ApiError::new(StatusCode::UNAUTHORIZED, "invalid wake authority");
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let (scheme, token) = raw
        .and_then(|v| v.split_once(' '))
        .ok_or_else(unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(unauthorized());
    }
    let token = token.to_owned();
    let section = spawn_store(move || {
        let Some(turn) = state
            .store
            .remote_turn_store()
            .verify_callback_token(&body.turn_id, &token)
            .map_err(ApiError::from_store)?
        else {
            return Err(unauthorized());
        };
        if turn.harness_kind != "sandbox"
            || anyagents_core::is_terminal_remote_turn_status(&turn.status)
            || !turn
                .metadata_json
                .as_ref()
                .and_then(|m| m.get("message_seq"))
                .and_then(Value::as_i64)
                .is_some_and(|seq| seq > 0)
        {
            return Err(unauthorized());
        }
        state
            .store
            .knowledge_wake_context(&turn.agent_id)
            .map_err(ApiError::from_store)
    })
    .await?;
    Ok(Json(json!({"context": section})))
}
