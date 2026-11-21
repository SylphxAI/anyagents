//! Core types for AnyAgents: TypeIDs, send addresses, and the SQLite mailbox.
//!
//! A message is received when its insert commits. That commit assigns the next
//! per-agent sequence. Source time is stored and never used to order delivery.
//! The accepted and processed cursors move only over a contiguous prefix.

mod address;
pub mod boundary;
pub mod browser_profiles_store;
pub mod db;
mod error;
mod host_cursor;
pub mod host_index;
pub mod host_migration;
mod host_native_metadata;
pub mod host_retrieval;
pub mod host_session;
pub mod host_skills;
mod host_sqlite;
mod id;
pub mod learned_memory;
pub mod masking;
mod migrate;
pub mod quote;
pub mod raw_segment;
pub mod redaction;
pub mod s3_media;
pub mod s3_store;
mod secret;
pub mod secrets_store;
pub mod stateless;
pub mod storage;
mod store;
pub use store::knowledge;

pub use address::{Address, AddressError};
pub use anyagents_harness::{
    ExecutionRuntime, atomic_write_file, atomic_write_in_existing_dir, atomic_write_nonempty_file,
    atomic_write_with,
};
pub use browser_profiles_store::{
    BrowserProfileActor, BrowserProfileGrant, MAX_BROWSER_PROFILE_NAME_LEN,
    validate_browser_profile_name,
};
pub use error::Error;
pub use id::{AgentId, ChannelId, MessageId, UsageId};
pub use quote::{Quote, QuoteRef};
pub use s3_store::{
    DEFAULT_MAX_S3_OBJECT_BYTES, ObjectMeta, S3_ACCESS_KEY_ID_ENV, S3_BUCKET_ENV, S3_ENDPOINT_ENV,
    S3_FORCE_PATH_STYLE_ENV, S3_PREFIX_ENV, S3_REGION_ENV, S3_SECRET_ACCESS_KEY_ENV, S3Config,
    S3Store,
};
pub use secret::{CredentialStore, FileCredentialStore, Secret, StoreCredentialStore};
pub use secrets_store::{
    DeploymentSecretMetadata, is_reserved_secret_env_key, require_deployment_master_key,
    validate_secret_env_key, validate_secret_name,
};
pub use stateless::{
    AGENT_FILE_NAMESPACES, BlobEntry, BlobMeta, CAPABILITY_KEY_ENV, CoordinatorLease,
    CoordinatorLeaseGuard, CoordinatorLockBackend, DEFAULT_LEASE_TTL,
    DEFAULT_REMOTE_SANDBOX_WORKDIR, DEPLOYMENT_BLOB_NAMESPACES, DurableObligation,
    DurableObligationStore, FsDurableObligationStore, FsRemoteTurnStore, FsSandboxBindingStore,
    FsStateBlobStore, InMemoryRemoteTurnStore, LocalFlockLockBackend, MASTER_KEY_ENV, MasterKey,
    NewRemoteTurn, PG_WAKE_CHANNEL, PendingRemoteTurn, PgCoordinatorLockBackend, PgCredentialStore,
    PgDurableObligationStore, PgGatewayRouteStore, PgRemoteTurnStore, PgSandboxBindingStore,
    PgStateBlobStore, PgWakeEvent, PgWakeListener, RemoteTurnEventUpdate, RemoteTurnStore,
    SandboxBinding, SandboxBindingStore, SettleOutcome, SettleRemoteTurn, StateBlobStore,
    StoredGatewayRoute, TERMINAL_REMOTE_TURN_STATUSES, UpsertGatewayRoute,
    VALID_REMOTE_TURN_STATUSES, constant_time_eq, decode_wake_payload, encode_wake_payload,
    generate_callback_token, generate_turn_id, hash_callback_token, hash_gateway_token,
    hmac_sha256_hex, is_terminal_remote_turn_status, publish_pg_wake, sanitize_pg_json,
    sanitize_pg_text, validate_blob_path, verify_callback_token_hash, verify_hmac_sha256_signature,
};
pub use storage::{
    GIT_POOL_DIR_ENV, SCRATCH_DIR_ENV, STATE_DIR_ENV, TEST_STORAGE_TIERS_ENV, WORKSPACES_DIR_ENV,
    agent_scratch_build_dir, agent_scratch_tmp_dir, configured_git_pool_dir,
    configured_scratch_dir, configured_state_dir, configured_workspaces_dir, storage_env_enabled,
};
pub use store::holds_store_writer;
pub use store::{
    Ack, Agent, AgentLock, AgentQueryMetadata, AgentSchedule, AgentSummary, BillingMode, Channel,
    ConversationTurn, DEFAULT_ARCHIVE_ROSTER_LIMIT, Group, GroupAction, GroupActor, GroupSummary,
    HistorySearchQuery, HistorySort, Incoming, Lag, MAX_RECOVERY_ATTEMPTS, Message, NewAgent,
    NewChannel, NewSchedule, NewUsage, OPERATOR_BURST, OPERATOR_FLEET_BURST,
    OPERATOR_FLEET_HOURLY_CAP, OPERATOR_HOURLY_CAP, OpenTurn, OperatorMessage, OperatorReceipt,
    OperatorReport, Origin, OutboundEffect, Outcome, OutcomeKind, ProfileUsageBucket,
    ReceiveOutcome, RejectedMessage, RosterPageQuery, Store, TurnOwnershipSnapshot,
    UsageAccessError, UsageBucket, UsageFilter, UsageGroupBy, UsageRecord, UsageReport, UsageTotal,
    UsageTotals, Wake, WalCheckpoint, agent_config_dir, agent_lock_path, agent_work_dir,
    aggregate_usage, aggregate_usage_with_profiles, clean_rag_text, legacy_agent_work_dir, now_ms,
    parse_agent_lifecycle, parse_agent_scope, rag_content_hash, read_children_roster,
    read_root_roster, resolve_usage_agents, sum_usage, tokenize_hybrid_search_tokens, utc_day,
};
pub use store::{
    AgentRuntime, BackfillReport, CredentialBinding, CredentialBindingAudit, CredentialKind,
    CredentialObservation, CredentialSlot, CredentialTarget, Fault, FaultKind, HarnessLease,
    LifecycleKind, LifecycleOp, LifecyclePhase, NewLifecycleOp, ProcessIdentity, QUOTA_ESTIMATE_MS,
    QuotaBlock, QuotaReset, QuotaSource, RecoveryOffer, RuntimeChange, RuntimeHeartbeat,
    RuntimeState, SchedulePending, SessionLease, Turn, TurnOutcome, TurnState,
};
pub use store::{
    AgentTurnSnapshot, ErroredTurn, MAX_ERRORED_TURNS, SYSTEM_SENDER, TurnErrors,
    is_session_turn_error,
};
pub use store::{
    NewTurnTranscriptEvent, TurnTranscriptEvent, TurnTranscriptSearchHit,
    VALID_TRANSCRIPT_EVENT_ROLES, normalize_transcript_event_role,
};
pub use store::{
    REMOTE_SESSION_TOKEN_PREFIX, RemoteSession, RemoteSessionGrant, RemoteSessionToken,
    is_remote_session_token,
};
pub use store::{
    RuntimeComponent, RuntimeIssue, RuntimeIssueReport, RuntimeIssueSource, RuntimeIssueStatus,
};

/// The token a client presents as an agent, when it has one. A capability is
/// written into the agent's own config directory; it is never an argument on a
/// command line and never part of a tool argument.
pub fn authenticate_presentation(token: Option<&str>) -> Option<String> {
    token
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
}

/// Physical fixture roots keep system aliases (for example a temporary-root
/// symlink) outside the boundary. Product readers still refuse all symlinks.
#[cfg(test)]
pub(crate) fn test_tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(
        std::env::temp_dir()
            .canonicalize()
            .expect("physical temporary root"),
    )
    .expect("owned temporary fixture")
}
