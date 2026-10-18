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
    #[error("agent is already running")]
    AgentBusy,
    #[error("not implemented yet: {0}")]
    NotImplemented(String),
    #[error("{0}")]
    Message(String),
}
