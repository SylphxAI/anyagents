//! Agent-scoped persistence for the canonical ingress block policy.

use agent_ingress::{
    BlockMatch, BlockScope, IngressBlockRule, IngressBlockStore, SenderIdentity, StoreError,
    ValidatedBlockRule,
};
use async_trait::async_trait;

use crate::db::{DbBackend, DbExec as _, DbExecExt as _, DbRow, db_params};

use super::{AgentId, Error, Store};

const RULE_COLS: &str =
    "channel, scope, target_id, rule_id, reason, blocked_by, created_at_ms, expires_at_ms";

/// Persistent ingress block rules for one receiving agent.
///
/// The caller supplies channel keys that include the account namespace. Use
/// the same keys for writes and for sender identities. This adapter uses the
/// synchronous Store database API; evaluation remains in `agent_ingress`.
pub struct IngressBlocks<'a> {
    store: &'a Store,
    agent_id: &'a AgentId,
}

impl<'a> IngressBlocks<'a> {
    pub fn new(store: &'a Store, agent_id: &'a AgentId) -> Self {
        Self { store, agent_id }
    }
}

#[async_trait]
impl IngressBlockStore for IngressBlocks<'_> {
    async fn block(&self, rule: ValidatedBlockRule) -> Result<(), StoreError> {
        let rule = rule.into_rule();
        let (scope, target_id) = scope_key(&rule.target.scope);
        self.store
            .write_tx(|tx| {
                let table = block_table(tx.backend());
                tx.db_execute(
                    &format!(
                        "INSERT INTO {table} (agent_id, {RULE_COLS})
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT (agent_id, channel, scope, target_id) DO UPDATE SET
                         rule_id = excluded.rule_id, reason = excluded.reason,
                         blocked_by = excluded.blocked_by, created_at_ms = excluded.created_at_ms,
                         expires_at_ms = excluded.expires_at_ms"
                    ),
                    &db_params![
                        self.agent_id,
                        rule.target.channel,
                        scope,
                        target_id,
                        rule.rule_id,
                        rule.reason,
                        rule.blocked_by,
                        rule.created_at_ms,
                        rule.expires_at_ms
                    ],
                )?;
                Ok(())
            })
            .map_err(backend_error)
    }

    async fn unblock(&self, target: &BlockMatch) -> Result<bool, StoreError> {
        let (scope, target_id) = scope_key(&target.scope);
        self.store
            .write_tx(|tx| {
                let table = block_table(tx.backend());
                Ok(tx.db_execute(
                    &format!(
                        "DELETE FROM {table}
                     WHERE agent_id = ?1 AND channel = ?2 AND scope = ?3 AND target_id = ?4"
                    ),
                    &db_params![self.agent_id, target.channel, scope, target_id],
                )? != 0)
            })
            .map_err(backend_error)
    }

    /// Return the agent's stored rules with their audit fields, including expired rules.
    async fn list_blocks(&self) -> Result<Vec<IngressBlockRule>, StoreError> {
        let conn = self.store.read_conn();
        let table = block_table(conn.backend());
        conn.db_query_vec(
            &format!("SELECT {RULE_COLS} FROM {table} WHERE agent_id = ?1"),
            &db_params![self.agent_id],
            rule_from_row,
        )
        .map_err(backend_error)
    }

    async fn matching_rules(
        &self,
        sender: &SenderIdentity,
    ) -> Result<Vec<IngressBlockRule>, StoreError> {
        let conn = self.store.read_conn();
        let table = block_table(conn.backend());
        // Two exact primary-key probes in one snapshot. If chat_id is absent,
        // the NULL parameter cannot match a chat row. Keep expired rules here:
        // the canonical policy decides whether each candidate is active.
        conn.db_query_vec(
            &format!(
                "SELECT {RULE_COLS} FROM {table}
                 WHERE agent_id = ?1 AND channel = ?2 AND scope = 'sender' AND target_id = ?3
                 UNION ALL
                 SELECT {RULE_COLS} FROM {table}
                 WHERE agent_id = ?1 AND channel = ?2 AND scope = 'chat' AND target_id = ?4"
            ),
            &db_params![
                self.agent_id,
                sender.channel,
                sender.sender_id,
                sender.chat_id
            ],
            rule_from_row,
        )
        .map_err(backend_error)
    }
}

fn block_table(backend: DbBackend) -> &'static str {
    match backend {
        DbBackend::Sqlite => "main.ingress_blocks",
        DbBackend::Postgres => "public.ingress_blocks",
    }
}

fn scope_key(scope: &BlockScope) -> (&'static str, &str) {
    match scope {
        BlockScope::Sender { sender_id } => ("sender", sender_id),
        BlockScope::Chat { chat_id } => ("chat", chat_id),
    }
}

fn rule_from_row(row: &DbRow<'_>) -> Result<IngressBlockRule, Error> {
    let target_id = row.get(2)?;
    let scope = match row.get::<String>(1)?.as_str() {
        "sender" => BlockScope::Sender {
            sender_id: target_id,
        },
        "chat" => BlockScope::Chat { chat_id: target_id },
        _ => return Err(Error::Message("invalid stored ingress block scope".into())),
    };
    Ok(IngressBlockRule {
        target: BlockMatch {
            channel: row.get(0)?,
            scope,
        },
        rule_id: row.get(3)?,
        reason: row.get(4)?,
        blocked_by: row.get(5)?,
        created_at_ms: row.get(6)?,
        expires_at_ms: row.get(7)?,
    })
}

fn backend_error(error: Error) -> StoreError {
    StoreError::Backend(Box::new(error))
}
