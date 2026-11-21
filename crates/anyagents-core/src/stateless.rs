//! Stateless multi-replica coordinator backends.
//!
//! Each capability defines a clean backend trait with:
//! - a PostgreSQL implementation backed by `migrations_pg/002_stateless_coordinator.sql`,
//!   enabling stateless `replicas: N` Thin Coordinator deployments with zero durable local disk state;
//! - a local filesystem / `flock` implementation preserving 100% zero-config local SQLite behavior.

pub mod blobs;
pub mod crypto;
pub mod gateway_routes;
pub mod locks;
pub mod obligations;
pub mod remote_turns;
pub mod remote_turns_ext;
pub mod sandbox_bindings;
pub mod wakes;

pub use blobs::{
    AGENT_FILE_NAMESPACES, BlobEntry, BlobMeta, DEPLOYMENT_BLOB_NAMESPACES, FsStateBlobStore,
    PgCredentialStore, PgStateBlobStore, StateBlobStore, validate_blob_path,
};
pub use crypto::{CAPABILITY_KEY_ENV, MASTER_KEY_ENV, MasterKey};
pub use gateway_routes::{
    PgGatewayRouteStore, StoredGatewayRoute, UpsertGatewayRoute, hash_gateway_token,
};
pub use locks::{
    CoordinatorLease, CoordinatorLeaseGuard, CoordinatorLockBackend, DEFAULT_LEASE_TTL,
    LocalFlockLockBackend, PgCoordinatorLockBackend,
};
pub use obligations::{
    DurableObligation, DurableObligationStore, FsDurableObligationStore, PgDurableObligationStore,
};
pub use remote_turns::{
    FsRemoteTurnStore, InMemoryRemoteTurnStore, NewRemoteTurn, PendingRemoteTurn,
    PgRemoteTurnStore, RemoteTurnEventUpdate, RemoteTurnStore, SettleOutcome, SettleRemoteTurn,
    TERMINAL_REMOTE_TURN_STATUSES, TURN_LIMIT_PAUSED, VALID_REMOTE_TURN_STATUSES, constant_time_eq,
    generate_callback_token, generate_turn_id, hash_callback_token, hmac_sha256_hex,
    is_terminal_remote_turn_status, sanitize_pg_json, sanitize_pg_text, verify_callback_token_hash,
    verify_hmac_sha256_signature,
};
pub use remote_turns_ext::{
    ACTIVE_REMOTE_TURN_FRESHNESS_MS, LatestRemoteTurnError, MAX_INBOX_SEQ_META_KEY,
};
pub use sandbox_bindings::{
    DEFAULT_REMOTE_SANDBOX_WORKDIR, FsSandboxBindingStore, PgSandboxBindingStore, SandboxBinding,
    SandboxBindingStore,
};
pub use wakes::{
    PG_WAKE_CHANNEL, PgWakeEvent, PgWakeListener, decode_wake_payload, encode_wake_payload,
    publish_pg_wake,
};
