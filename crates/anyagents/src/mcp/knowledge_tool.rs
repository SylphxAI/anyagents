//! Route explicit versioned memory operations through the authenticated API.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{AgentMcp, memory_tool::MemoryArgs};

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema)]
pub(super) struct KnowledgeArgs {
    /// Qualified document namespace, e.g. `org:memory` or `domain:engineering`.
    pub(super) namespace: Option<String>,
    pub(super) title: Option<String>,
    pub(super) pinned: Option<bool>,
    /// CAS base version; use 0 for a create.
    pub(super) expected_version: Option<i64>,
    pub(super) reason: Option<String>,
    pub(super) sim_problem: Option<String>,
    pub(super) sim_expected: Option<String>,
    pub(super) sim_observed: Option<String>,
    pub(super) sim_context: Option<String>,
    pub(super) to_version: Option<i64>,
    pub(super) before_version: Option<i64>,
    pub(super) proposal_id: Option<String>,
    pub(super) after: Option<String>,
}

pub(super) fn selected(args: &MemoryArgs) -> bool {
    args.knowledge.namespace.is_some()
        || args.knowledge.expected_version.is_some()
        || args
            .scope
            .as_deref()
            .is_some_and(|scope| !matches!(scope.trim(), "self" | "shared"))
}

fn payload(args: MemoryArgs, self_id: &str) -> Result<Value, String> {
    if args.agent.is_some() {
        return Err(
            "versioned memory uses namespace and authenticated agent identity, not agent".into(),
        );
    }
    let action = args.action.as_deref().unwrap_or_else(|| {
        if args.delete == Some(true) {
            "delete"
        } else if args.content.is_some() {
            "write"
        } else if args.query.is_some() {
            "search"
        } else if args.topic.is_some() {
            "read"
        } else {
            "list"
        }
    });
    if !matches!(
        action,
        "list"
            | "read"
            | "write"
            | "upsert"
            | "delete"
            | "revisions"
            | "rollback"
            | "propose"
            | "proposals"
            | "promote"
            | "reject"
            | "search"
    ) {
        return Err("unsupported versioned memory action".into());
    }
    let scope = args.scope.as_deref().map(str::trim);
    if let Some(scope) = scope {
        anyagents_core::knowledge::Scope::parse(scope).map_err(|err| err.to_string())?;
    }
    let mut fields = args.knowledge;
    if matches!(action, "list" | "search") {
        if fields.namespace.is_some() {
            return Err("use scope for versioned memory list/search".into());
        }
    } else if fields.namespace.is_none() {
        fields.namespace = match scope {
            Some("agent") => Some(format!("agent:{self_id}")),
            Some("org") => Some("org:memory".into()),
            Some(scope) if scope.starts_with("domain:") => Some(scope.into()),
            _ => return Err("document operation requires a namespace".into()),
        };
    }
    let mut payload = serde_json::to_value(fields).map_err(|err| err.to_string())?;
    let fields = payload
        .as_object_mut()
        .expect("knowledge arguments are a struct");
    fields.extend([
        ("action".into(), json!(action)),
        ("scope".into(), json!(scope)),
        ("topic".into(), json!(args.topic)),
        ("description".into(), json!(args.description)),
        ("body".into(), json!(args.content)),
        ("query".into(), json!(args.query)),
        ("limit".into(), json!(args.limit)),
        ("offset".into(), json!(args.offset)),
        ("prefix".into(), json!(args.prefix)),
    ]);
    fields.retain(|_, value| !value.is_null());
    Ok(payload)
}

impl AgentMcp {
    pub(super) async fn execute_knowledge(&self, args: MemoryArgs) -> Result<Value, String> {
        let body = payload(args, self.agent.id.as_str())?;
        self.client
            .get("/v1/knowledge", Some(body.to_string().into_bytes()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_scope_routes_to_versioned_memory_without_overriding_identity() {
        let args = serde_json::from_value(json!({"scope":"agent", "topic":"t", "content":"hello", "expected_version":2, "reason":"update"})).unwrap();
        assert!(selected(&args));
        assert_eq!(
            payload(args, "agt_test").unwrap(),
            json!({"action":"write", "scope":"agent", "namespace":"agent:agt_test", "topic":"t", "body":"hello", "expected_version":2, "reason":"update"})
        );
        let args = serde_json::from_value(json!({"scope":"shared", "action":"list"})).unwrap();
        assert!(!selected(&args));
        let args = serde_json::from_value(json!({"scope":"all", "action":"grant"})).unwrap();
        assert!(payload(args, "agt_test").is_err());
    }
}
