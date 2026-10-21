//! JSON shapes shared by the unix-socket server and the MCP tools.

use anyagents_core::Quote;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<String>,
    pub state: String,
    pub process: String,
    pub delivery_mode: String,
    pub steer_support: String,
    pub quota_reset_at_ms: Option<i64>,
    /// Age of the oldest message the harness has not taken yet.
    pub oldest_undelivered_age_ms: Option<i64>,
    /// `installed`, `pending`, or `needed: <why>`: whether this agent's harness
    /// has been installed yet, and what stopped it.
    pub install: String,
}

fn default_harness() -> String {
    "claude-code".to_string()
}

/// Body of `POST /v1/agents`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct CreateAgentRequest {
    pub name: String,
    #[serde(default = "default_harness")]
    pub harness: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub instructions: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentsResponse {
    pub agents: Vec<AgentView>,
    /// The origin a harness points at for provider traffic, when the gateway is
    /// running. A client stores a key against it so the agent reaches the
    /// gateway on its next start.
    #[serde(default)]
    pub gateway: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MessageView {
    pub id: String,
    pub seq: i64,
    pub text: String,
    pub sender: Option<String>,
    pub reply_to: Option<String>,
    pub source_time_ms: Option<i64>,
    pub received_at_ms: i64,
    /// `received` (committed to the mailbox) or `delivered` (the harness took it).
    pub ack: String,
    /// When the harness took it, when it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxResponse {
    pub messages: Vec<MessageView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendRequest {
    /// The principal this message is from. On the identity boundary the server
    /// already knows it from the capability, and this must agree or the call is
    /// refused; the operator names its own principal explicitly.
    pub sender: Option<String>,
    #[serde(default)]
    pub to: Vec<String>,
    pub text: String,
    #[serde(default)]
    pub reply_to: Option<String>,
    /// The message this one quotes, as the sender's channel sent it. Rendered
    /// into the stored body; a harness cannot forge a quoted author.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<Quote>,
    /// Names one send, so a retry after a lost answer is the same delivery
    /// instead of a second one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Delivery {
    pub to: String,
    pub id: String,
    pub seq: i64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendResponse {
    pub deliveries: Vec<Delivery>,
}
