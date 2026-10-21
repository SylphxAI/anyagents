//! Core types for AnyAgents: TypeIDs, send addresses, and the SQLite mailbox.
//!
//! A message is received when its insert commits. That commit assigns the next
//! per-agent sequence. Source time is stored and never used to order delivery.
//! The accepted and processed cursors move only over a contiguous prefix.

mod address;
pub mod boundary;
mod error;
mod id;
mod migrate;
pub mod quote;
mod secret;
mod store;

pub use address::{Address, AddressError};
pub use error::Error;
pub use id::{AgentId, ChannelId, MessageId, UsageId};
pub use quote::{Quote, QuoteRef};
pub use secret::{CredentialStore, FileCredentialStore, Secret};
pub use store::{
    Ack, Agent, AgentLock, BillingMode, Channel, Incoming, Lag, Message, NewAgent, NewChannel, Origin,
    NewUsage, ReceiveOutcome, Store, UsageRecord, UsageTotal, Wake, agent_config_dir,
    agent_work_dir, now_ms, sum_usage, utc_day,
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


