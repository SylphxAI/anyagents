use crate::address::AddressError;

/// Failures from the store, the mailbox, and identifier parsing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid identifier: {0}")]
    InvalidId(String),
    #[error(transparent)]
    InvalidAddress(#[from] AddressError),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("agent already exists: {0}")]
    AgentExists(String),
    #[error("channel not found: {0}")]
    ChannelNotFound(String),
    #[error("{cursor} cursor cannot move from {at} to {attempted}")]
    CursorGap {
        cursor: &'static str,
        at: i64,
        attempted: i64,
    },
    #[error("{0}")]
    PermissionDenied(String),
    #[error("agent is already running")]
    AgentBusy,
    #[error("group `{group}`: {reason}")]
    GroupForbidden { group: String, reason: &'static str },
    #[error("not implemented yet: {0}")]
    NotImplemented(String),
    #[error("failed to provision worktree for `{repo}`: {message}")]
    WorktreeProvisioning { repo: String, message: String },
    #[error(
        "cannot provision worktree from `{source_repo}`: {message}; push to a GitHub remote before spawning"
    )]
    WorktreeSourceUnpublished {
        source_repo: String,
        message: String,
    },
    #[error("workdir `{path}` is not writable by its seat: {message}")]
    WorkdirPermission { path: String, message: String },
    #[error("{machine} transition from {from} to {to} is not allowed")]
    InvalidTransition {
        machine: &'static str,
        from: String,
        to: String,
    },
    #[error("{machine} state is {actual}, expected {expected}")]
    StateConflict {
        machine: &'static str,
        expected: String,
        actual: String,
    },
    #[error("{machine} precondition failed: {reason}")]
    Precondition {
        machine: &'static str,
        reason: String,
    },
    #[error("{lease} lease is held by pid {holder_pid}")]
    LeaseHeld {
        lease: &'static str,
        holder_pid: i64,
    },
    #[error("agent {agent} already has an unfinished lifecycle op {op_id}")]
    LifecycleOpActive { agent: String, op_id: String },
    #[error("agent name is reserved by an unfinished spawn: {0}")]
    NameReserved(String),
    #[error("{0}")]
    InvalidCredential(String),
    #[error("{0}")]
    Message(String),
}

impl Error {
    /// Returns true if this error is a transient SQLite lock contention error
    /// (`SQLITE_BUSY` or `SQLITE_LOCKED`) that callers can safely retry after backoff.
    pub fn is_transient_lock_error(&self) -> bool {
        matches!(
            self,
            Self::Db(rusqlite::Error::SqliteFailure(err, _))
                if matches!(
                    err.code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                )
        )
    }
}
