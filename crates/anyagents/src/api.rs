//! JSON shapes shared by the unix-socket server and the MCP tools.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    pub harness: String,
    pub state: String,
    pub oldest_unaccepted_age_ms: Option<i64>,
    pub oldest_unprocessed_age_ms: Option<i64>,
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
