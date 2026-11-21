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
mod console_html;
mod file_cache;
mod hybrid;
mod in_flight;
mod ingress_blocks;
mod knowledge;
#[cfg(test)]
mod latency_tests;
mod memory_backlog;
mod messaging;
mod messaging_conversation;
mod messaging_delivery;
mod native_signin;
pub(crate) mod nodes;
pub(crate) mod oidc_bearer;
mod ops;
pub(crate) mod outbound_history;
mod private_media;
mod profile_models;
mod profiles;
mod quota_rotation;
mod reap_queue;
mod roles;
mod runtime;
pub(crate) mod runtime_issues;
pub(crate) mod s3_media_profiles;
mod sandbox_media;
mod sandbox_targets;
mod schedules;
pub(crate) mod secrets;
pub(crate) mod serverless;
mod serverless_envelope;
mod serverless_failure;
pub(crate) mod serverless_maintenance;
mod serverless_memory;
pub(crate) mod serverless_steer;
pub(crate) mod serverless_watches;
mod takeover_tunnel;
pub(crate) mod timer_fire;
mod types;
mod versions;
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
use types::{require_operator, spawn_store};

/// The public listener (`[server] public_listen`): the only surface a network
/// peer reaches once it is configured. It serves readiness, Telegram webhooks
/// (each verified by its channel's secret token) and MCP for remote session
/// tokens only, plus authenticated, self-scoped media transfer. Other paths are
/// 404. Credential policy depends on the listener, never on a source address
/// or a forwarding header.
pub(crate) fn build_public_router(state: AppState) -> Router {
    ops::public_health_router()
        .merge(oidc_bearer::discovery_router())
        .merge(channels_groups::webhook_router())
        .merge(webhooks_mcp::public_router())
        .merge(nodes::public_router())
        .merge(takeover_tunnel::router(false))
        .route(
            "/v1/internal/guest/events",
            post(serverless::guest_events_route)
                .layer(axum::extract::DefaultBodyLimit::max(4 * 1024 * 1024)),
        )
        .route(
            "/v1/internal/cloud/events",
            post(serverless::cloud_events_route),
        )
        .merge(timer_fire::router())
        .merge(knowledge::sandbox_router())
        .merge(private_media::router())
        .merge(s3_media_profiles::public_media_router())
        .fallback(ops::public_not_found)
        .with_state(state.clone())
        .merge(crate::gateway::http_router_for_store(Arc::clone(
            &state.store,
        )))
        .layer(axum::middleware::from_fn(in_flight::track))
}

fn build_webhook_router(state: AppState) -> Router {
    // In Pure Serverless mode there is no reachable host Unix socket, so expose
    // the full authenticated control/agent API and embedded Web Console on `/`
    // strictly when fail-closed operator auth (`ANYAGENTS_OPERATOR_TOKEN`) or
    // Admin OIDC is configured.
    let expose_console = serverless::is_serverless_mode()
        && (state.auth.has_operator() || admin_auth::is_oidc_configured());
    let base = if expose_console {
        ops::health_probe_router()
    } else {
        ops::public_health_router()
    }
    .merge(oidc_bearer::discovery_router())
    .merge(channels_groups::webhook_router())
    .merge(webhooks_mcp::local_router())
    .merge(nodes::public_router())
    .merge(takeover_tunnel::router(expose_console))
    .route(
        "/v1/internal/guest/events",
        post(serverless::guest_events_route)
            .layer(axum::extract::DefaultBodyLimit::max(4 * 1024 * 1024)),
    )
    .route(
        "/v1/internal/cloud/events",
        post(serverless::cloud_events_route),
    )
    .merge(timer_fire::router())
    .merge(knowledge::sandbox_router())
    .merge(private_media::router());
    let router = if expose_console {
        admin_ui::mount_admin_routes(mount_authenticated_api_routes(base))
    } else {
        base.merge(s3_media_profiles::public_media_router())
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
        .merge(sandbox_targets::router())
        .merge(schedules::router())
        .merge(messaging::router())
        .merge(runtime_issues::agent_router())
        .merge(ingress_blocks::agent_router())
        .merge(channels_groups::router())
        .merge(roles::router())
        .merge(ops::router())
        .merge(s3_media_profiles::router())
        .merge(knowledge::router())
        .merge(nodes::router())
}

pub(crate) fn build_router(state: AppState) -> Router {
    admin_ui::mount_admin_routes(mount_authenticated_api_routes(Router::new()))
        .merge(oidc_bearer::discovery_router())
        .route("/healthz", get(ops::public_healthz))
        .route("/readyz", get(ops::public_healthz))
        .route("/v1/webhooks/healthz", get(ops::webhook_healthz))
        .merge(channels_groups::webhook_router())
        .merge(webhooks_mcp::local_router())
        .merge(nodes::public_router())
        .merge(takeover_tunnel::router(true))
        .route(
            "/v1/internal/guest/events",
            post(serverless::guest_events_route)
                .layer(axum::extract::DefaultBodyLimit::max(4 * 1024 * 1024)),
        )
        .route(
            "/v1/internal/cloud/events",
            post(serverless::cloud_events_route),
        )
        .merge(timer_fire::router())
        .merge(knowledge::sandbox_router())
        .merge(private_media::router())
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
