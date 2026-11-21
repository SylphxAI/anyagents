//! One-way runtime diagnostics, separate from conversation and operator mail.

use std::sync::Arc;

use anyagents_core::{
    AgentId, Error, RuntimeComponent, RuntimeIssue, RuntimeIssueReport, RuntimeIssueSource,
    RuntimeIssueStatus, Store,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::auth::Principal;

use super::types::{ApiError, AppState, require_operator, spawn_store};

pub(super) fn agent_router() -> Router<AppState> {
    Router::new().route(
        "/v1/runtime-issues",
        post(report).layer(DefaultBodyLimit::max(128 * 1024)),
    )
}

pub(super) fn admin_router() -> Router<AppState> {
    Router::new().route("/admin/api/runtime-issues", get(list))
}

#[derive(JsonSchema)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // Schema remote for the domain enum (the domain has no schema dependency).
enum ComponentSchema {
    Coordinator,
    Sandbox,
    Harness,
    McpTool,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReportBug {
    #[schemars(with = "ComponentSchema")]
    component: RuntimeComponent,
    summary: String,
    exact_error: String,
    repro_steps: String,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct ReportReceipt {
    issue_id: String,
}

fn issue_error(err: Error) -> ApiError {
    match err {
        Error::Message(message) => ApiError::new(StatusCode::BAD_REQUEST, message),
        _ => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime issue store unavailable",
        ),
    }
}

async fn report(
    principal: Principal,
    State(state): State<AppState>,
    Json(args): Json<ReportBug>,
) -> Result<Json<ReportReceipt>, ApiError> {
    let Principal::Agent { agent_id, .. } = principal else {
        return Err(ApiError::forbidden(
            "runtime bug reports require an agent session",
        ));
    };
    spawn_store(move || {
        let values = super::secrets::load_agent_mask_values(&state.store, &agent_id);
        let mask = |text| anyagents_core::masking::mask_with_secrets(text, &values);
        let issue_id = state
            .store
            .record_runtime_issue(RuntimeIssueReport {
                component: args.component,
                summary: &mask(&args.summary),
                exact_error: &mask(&args.exact_error),
                repro_steps: &mask(&args.repro_steps),
                source: RuntimeIssueSource::AgentReport,
                sample_agent_id: Some(&agent_id),
            })
            .map_err(issue_error)?;
        Ok(Json(ReportReceipt { issue_id }))
    })
    .await
}

#[derive(Deserialize)]
struct Filters {
    status: Option<RuntimeIssueStatus>,
    component: Option<RuntimeComponent>,
}

#[derive(Serialize)]
struct IssueList {
    issues: Vec<RuntimeIssue>,
}

async fn list(
    principal: Principal,
    State(state): State<AppState>,
    Query(filters): Query<Filters>,
) -> Result<Json<IssueList>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let issues = state
            .store
            .runtime_issues(filters.status, filters.component)
            .map_err(issue_error)?;
        Ok(Json(IssueList { issues }))
    })
    .await
}

/// Project only known, static diagnostics (never provider output or credentials).
/// Drop telemetry when saturated; do not queue work ahead of failure recovery.
pub(super) fn capture(
    store: Arc<Store>,
    agent_id: Option<AgentId>,
    component: RuntimeComponent,
    summary: &'static str,
    exact_error: &'static str,
) {
    static IN_FLIGHT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);
    let (Ok(runtime), Ok(permit)) = (
        tokio::runtime::Handle::try_current(),
        IN_FLIGHT.try_acquire(),
    ) else {
        return;
    };
    runtime.spawn_blocking(move || {
        let _permit = permit;
        let _ = store.record_runtime_issue(RuntimeIssueReport {
            component,
            summary,
            exact_error,
            repro_steps: "Automatically captured; reproduction steps unavailable.",
            source: RuntimeIssueSource::AutoCapture,
            sample_agent_id: agent_id.as_ref(),
        });
    });
}

#[cfg(test)]
#[path = "runtime_issues_tests.rs"]
mod tests;

/// Match the guest's explicit launch boundary, not arbitrary turn output.
pub(super) fn missing_guest_harness(error: Option<&str>) -> bool {
    error.is_some_and(|text| {
        text.starts_with("failed to start guest harness ")
            && text.trim_end().ends_with("(os error 2)")
    })
}
