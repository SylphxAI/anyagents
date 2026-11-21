//! Unit and in-process integration tests for the HTTP/JSON mailbox server.

mod accounts;
mod agents;
// The ADR 0005 source scan recognizes these command fixtures as test-only by suffix.
#[path = "archive_tests.rs"]
mod archive;
#[path = "checkout_tests.rs"]
mod checkout;
mod helpers;
mod knowledge;
mod lifecycle;
mod listeners;
mod mcp;
mod memory;
mod messaging;
mod operator_inbox;
mod private_media;
mod remote_sessions;
mod roster;
mod schedules;
mod serverless;
mod spawn;
mod usage;
mod volume_reap_tests;
mod wake;
mod webhooks;

pub(crate) use helpers::env_lock;

// Keep this exact test path for the privileged CI acceptance lane.
include!("../../a2a_media_tests.rs");

#[path = "../memory_http_tests.rs"]
mod memory_http_tests;

use std::path::PathBuf;

use anyagents_core::Agent;
use axum::http::StatusCode;

use crate::api::SendRequest;

use super::messaging::{SendPlan, send_sync};
use super::*;
use helpers::{SingleUidEnvironment, archetype_validation_state, http};
