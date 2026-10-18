//! Core types for AnyAgents: TypeIDs, send addresses, and the SQLite mailbox.
//!
//! A message is received when its insert commits. That commit assigns the next
//! per-agent sequence. Source time is stored and never used to order delivery.
//! The accepted and processed cursors move only over a contiguous prefix.

mod address;
mod error;
mod id;
mod migrate;
mod store;

pub use address::{Address, AddressError};
pub use error::Error;
pub use id::{AgentId, ChannelId, MessageId, UsageId};
pub use store::{
    Ack, Agent, AgentLock, BillingMode, Channel, Incoming, Lag, Message, NewAgent, NewChannel,
    NewUsage, ReceiveOutcome, Store, UsageRecord, now_ms,
};
