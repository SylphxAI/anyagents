//! HTTP/JSON mailbox API on a user-only unix socket.
//!
//! Every call is authenticated before it is routed: an agent instance presents
//! the mailbox capability the server minted for it, and the operator presents
//! its own token. A sender, an inbox, a bot or an agent name in a body is a
//! destination, never an identity.

#[cfg(test)]
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
use anyagents_core::Store;
#[cfg(test)]
use anyagents_supervisor::HealthBoard;
use axum::Router;
#[cfg(test)]
use axum::extract::{Path as AxumPath, Query, State};
use axum::routing::{get, post};

use crate::api::Delivery;
#[cfg(test)]
use crate::auth::{self, Principal};
#[cfg(test)]
use crate::identity::ServingIdentity;

pub(crate) mod admin_auth;
mod admin_ui;
mod agents;
mod agents_lifecycle;
mod agents_roster;
mod bootstrap;
mod channels_groups;
mod file_cache;
mod hybrid;
mod in_flight;
#[cfg(test)]
mod latency_tests;
mod memory_backlog;
mod messaging;
mod messaging_conversation;
mod messaging_delivery;
mod ops;
pub(crate) mod outbound_history;
mod profiles;
mod quota_rotation;
mod reap_queue;
pub(crate) mod s3_media_profiles;
mod schedules;
pub(crate) mod secrets;
pub(crate) mod serverless;
mod serverless_envelope;
mod types;
mod webhooks_mcp;

pub(crate) use profiles::ServedProfile;

#[allow(unused_imports)]
pub use bootstrap::not_confined_note;
#[cfg(test)]
pub use bootstrap::run_with;
pub use bootstrap::{listen_on_loopback, run, run_with_port, serve_gateway, socket_path};
pub use types::owner_workdir;

#[cfg(test)]
pub(crate) use agents_lifecycle::reap_subagent_ephemeral_dirs;
pub(crate) use agents_lifecycle::{
    clear_explicitly_archived, is_explicitly_archived, mark_explicitly_archived_locked,
    reap_archived_agent_locked,
};
#[allow(unused_imports)]
pub(crate) use agents_lifecycle::{
    has_explicit_archive_marker, is_git_worktree_safe_to_reap, reap_archived_agents,
};
#[allow(unused_imports)]
pub(crate) use agents_roster::compute_block_reason;
pub(crate) use agents_roster::{ListAgentsQuery, agent_view, roster_views};
#[allow(unused_imports)]
pub(crate) use bootstrap::{STOP_WITHIN, release_stale_unadmitted_names_on_start};
pub(crate) use messaging::unsettled_view;
pub(crate) use types::{ApiError, AppState, resolve_remote_sandbox_workdir};

#[cfg(all(test, target_os = "linux"))]
use agents_roster::{BOUND_PROFILE, PROFILE_ROTATED, QUOTA_CLEARED};
#[cfg(test)]
use agents_roster::{LAST_ERROR, NOTIFIED_PARENT};
#[cfg(test)]
use messaging::{Actor, held_send};
#[cfg(test)]
use messaging_conversation::conversation_route;
#[cfg(test)]
use types::ProcessRunner;
use types::{SignInLine, require_operator, spawn_store};

/// The public listener (`[server] public_listen`): the only surface a network
/// peer reaches once it is configured. It serves readiness, Telegram webhooks
/// (each verified by its channel's secret token) and MCP for remote session
/// tokens only; every other path is 404. Which credentials it accepts is
/// decided by the listener the request arrived on, never by a source address
/// or a forwarding header.
pub(crate) fn build_public_router(state: AppState) -> Router {
    ops::public_health_router()
        .merge(channels_groups::webhook_router())
        .merge(webhooks_mcp::public_router())
        .route(
            "/v1/internal/guest/events",
            post(serverless::guest_events_route),
        )
        .route(
            "/v1/internal/cloud/events",
            post(serverless::cloud_events_route),
        )
        .route("/v1/internal/tick", post(serverless::tick_route))
        .fallback(ops::public_not_found)
        .with_state(state.clone())
        .merge(crate::gateway::http_router_for_store(Arc::clone(
            &state.store,
        )))
        .layer(axum::middleware::from_fn(in_flight::track))
}

fn build_webhook_router(state: AppState) -> Router {
    let base = ops::public_health_router()
        .merge(channels_groups::webhook_router())
        .merge(webhooks_mcp::local_router())
        .route(
            "/v1/internal/guest/events",
            post(serverless::guest_events_route),
        )
        .route(
            "/v1/internal/cloud/events",
            post(serverless::cloud_events_route),
        )
        .route("/v1/internal/tick", post(serverless::tick_route));
    // In Pure Serverless mode there is no reachable host Unix socket, so expose
    // the full authenticated control/agent API and embedded `/admin` console on
    // the HTTP listener strictly when fail-closed operator auth
    // (`ANYAGENTS_OPERATOR_TOKEN`) or Admin OIDC is configured.
    let router = if serverless::is_serverless_mode()
        && (state.auth.has_operator() || admin_auth::is_oidc_configured())
    {
        admin_ui::mount_admin_routes(mount_authenticated_api_routes(base))
    } else {
        base
    };
    router
        .with_state(state.clone())
        .merge(crate::gateway::http_router_for_store(Arc::clone(
            &state.store,
        )))
        .layer(axum::middleware::from_fn(in_flight::track))
}

fn mount_authenticated_api_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .merge(agents::router())
        .merge(secrets::router())
        .merge(schedules::router())
        .merge(messaging::router())
        .merge(channels_groups::router())
        .merge(ops::router())
        .merge(s3_media_profiles::router())
}

pub(crate) fn build_router(state: AppState) -> Router {
    admin_ui::mount_admin_routes(mount_authenticated_api_routes(Router::new()))
        .route("/v1/webhooks/healthz", get(ops::webhook_healthz))
        .merge(channels_groups::webhook_router())
        .merge(webhooks_mcp::local_router())
        .route(
            "/v1/internal/guest/events",
            post(serverless::guest_events_route),
        )
        .route(
            "/v1/internal/cloud/events",
            post(serverless::cloud_events_route),
        )
        .route("/v1/internal/tick", post(serverless::tick_route))
        .with_state(state.clone())
        .merge(crate::gateway::http_router_for_store(Arc::clone(
            &state.store,
        )))
        .layer(axum::middleware::from_fn(in_flight::track))
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(all(test, target_os = "linux"))]
#[path = "server_history_benchmark.rs"]
mod history_benchmark;
