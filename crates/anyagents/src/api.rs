//! JSON shapes shared by the unix-socket server and the MCP tools.

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
    pub oldest_unaccepted_age_ms: Option<i64>,
    pub oldest_unprocessed_age_ms: Option<i64>,
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
    pub ack: String,
    /// Why the last turn on this message failed, when it did. The message stays
    /// accepted and is offered to the next turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxResponse {
    pub messages: Vec<MessageView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendRequest {
    pub sender: Option<String>,
    #[serde(default)]
    pub to: Vec<String>,
    pub text: String,
    #[serde(default)]
    pub reply_to: Option<String>,
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
