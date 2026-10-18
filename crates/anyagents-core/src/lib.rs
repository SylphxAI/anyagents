//! Core types for AnyAgents: TypeIDs, send addresses, and the SQLite mailbox.
//!
//! A message is received when its insert commits. That commit assigns the next
//! per-agent sequence. Source time is stored and never used to order delivery.
//! The accepted and processed cursors move only over a contiguous prefix.

mod address;
mod error;
mod id;
mod migrate;
mod secret;
mod store;

pub use address::{Address, AddressError};
pub use error::Error;
pub use id::{AgentId, ChannelId, MessageId, UsageId};
pub use secret::{CredentialStore, FileCredentialStore, Secret};
pub use store::{
    Ack, Agent, AgentLock, BillingMode, Channel, Incoming, Lag, Message, NewAgent, NewChannel,
    NewUsage, ReceiveOutcome, Store, UsageRecord, UsageTotal, Wake, agent_config_dir,
    agent_work_dir, now_ms, sum_usage, utc_day,
};
