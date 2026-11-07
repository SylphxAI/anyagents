//! Core types for AnyAgents: TypeIDs, send addresses, and the SQLite mailbox.
//!
//! A message is received when its insert commits. That commit assigns the next
//! per-agent sequence. Source time is stored and never used to order delivery.
//! The accepted and processed cursors move only over a contiguous prefix.

mod address;
pub mod boundary;
mod error;
mod host_cursor;
pub mod host_index;
pub mod host_migration;
mod host_native_metadata;
pub mod host_retrieval;
pub mod host_session;
pub mod host_skills;
mod id;
pub mod learned_memory;
mod migrate;
pub mod quote;
pub mod raw_segment;
pub mod redaction;
mod secret;
mod store;

pub use address::{Address, AddressError};
pub use error::Error;
pub use id::{AgentId, ChannelId, MessageId, UsageId};
pub use quote::{Quote, QuoteRef};
pub use secret::{CredentialStore, FileCredentialStore, Secret};
pub use store::{
    Ack, Agent, AgentLock, AgentSchedule, BillingMode, Channel, ConversationTurn, Group,
    GroupAction, GroupActor, HistorySearchQuery, HistorySort, Incoming, Lag, MAX_RECOVERY_ATTEMPTS,
    Message, NewAgent, NewChannel, NewSchedule, NewUsage, OpenTurn, Origin, OutboundEffect,
    ReceiveOutcome, Store, UsageRecord, UsageTotal, Wake, agent_config_dir, agent_work_dir,
    clean_rag_text, now_ms, rag_content_hash, read_children_roster, read_root_roster, sum_usage,
    tokenize_hybrid_search_tokens, utc_day,
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
