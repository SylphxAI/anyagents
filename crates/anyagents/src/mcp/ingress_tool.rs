//! Schema projection only: decode and execute the canonical request on the server.

use agent_ingress::IngressGuardToolRequest;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer};

use super::AgentMcp;

#[derive(JsonSchema)]
#[serde(transparent)]
pub(super) struct Args(#[schemars(with = "RequestSchema")] IngressGuardToolRequest);

impl<'de> Deserialize<'de> for Args {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        IngressGuardToolRequest::deserialize(deserializer).map(Self)
    }
}

#[derive(JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[allow(dead_code)] // Schema for the upstream serde-only wire type.
enum RequestSchema {
    BlockSender {
        target: Target,
        reason: Option<String>,
        expires_at_ms: Option<i64>,
    },
    UnblockSender {
        target: Target,
    },
    ListBlocks,
}

#[derive(JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Target {
    channel: String,
    scope: Scope,
}

#[derive(JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(dead_code)]
enum Scope {
    Sender { sender_id: String },
    Chat { chat_id: String },
}

pub(super) async fn call(agent: &AgentMcp, args: Args) -> Result<serde_json::Value, String> {
    let body = serde_json::to_vec(&args.0).map_err(|err| err.to_string())?;
    agent.client.get("/v1/ingress-blocks", Some(body)).await
}
