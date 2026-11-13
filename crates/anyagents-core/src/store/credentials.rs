//! Credential bindings (ADR 0004): which credential record or pool serves each
//! slot of an agent. The store holds no credential bytes and resolves no
//! defaults or pools; it records explicit bindings, their audit trail, and
//! launch-time pool observations. Only the binding operations write these
//! tables.

use std::fmt;
use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::{AgentId, Error, Store, now_ms};

fn invalid(what: &str, text: &str) -> Error {
    Error::InvalidCredential(format!("invalid credential {what} `{text}`"))
}

/// A name segment: `[a-z0-9][a-z0-9._-]*`.
fn valid_segment(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
}

/// A channel id segment as minted by the store (letters, digits and `_`).
fn valid_channel_id(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A credential slot of an agent.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CredentialSlot {
    Model(String),
    ToolGithub,
    ToolGitAuthor,
    ToolOp,
    Tool(String),
    Channel(String),
}

impl fmt::Display for CredentialSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialSlot::Model(provider) => write!(f, "model:{provider}"),
            CredentialSlot::ToolGithub => f.write_str("tool:github"),
            CredentialSlot::ToolGitAuthor => f.write_str("tool:git_author"),
            CredentialSlot::ToolOp => f.write_str("tool:op"),
            CredentialSlot::Tool(name) => write!(f, "tool:{name}"),
            CredentialSlot::Channel(id) => write!(f, "channel:{id}"),
        }
    }
}

impl FromStr for CredentialSlot {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        let (class, name) = text.split_once(':').ok_or_else(|| invalid("slot", text))?;
        match (class, name) {
            ("model", provider) if valid_segment(provider) => Ok(Self::Model(provider.into())),
            ("tool", "github") => Ok(Self::ToolGithub),
            ("tool", "git_author") => Ok(Self::ToolGitAuthor),
            ("tool", "op") => Ok(Self::ToolOp),
            ("tool", name) if valid_segment(name) => Ok(Self::Tool(name.into())),
            ("channel", id) if valid_channel_id(id) => Ok(Self::Channel(id.into())),
            _ => Err(invalid("slot", text)),
        }
    }
}

/// The kind of a credential record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CredentialKind {
    OauthSubscription,
    ApiKey,
    GithubUser,
    GithubApp,
    OpServiceAccount,
    ChannelBot,
    BrowserProfile,
    Kubeconfig,
    BuildKey,
}

impl CredentialKind {
    pub const ALL: &'static [CredentialKind] = &[
        Self::OauthSubscription,
        Self::ApiKey,
        Self::GithubUser,
        Self::GithubApp,
        Self::OpServiceAccount,
        Self::ChannelBot,
        Self::BrowserProfile,
        Self::Kubeconfig,
        Self::BuildKey,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OauthSubscription => "oauth_subscription",
            Self::ApiKey => "api_key",
            Self::GithubUser => "github_user",
            Self::GithubApp => "github_app",
            Self::OpServiceAccount => "op_service_account",
            Self::ChannelBot => "channel_bot",
            Self::BrowserProfile => "browser_profile",
            Self::Kubeconfig => "kubeconfig",
            Self::BuildKey => "build_key",
        }
    }
}

impl fmt::Display for CredentialKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CredentialKind {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_str() == text)
            .ok_or_else(|| invalid("kind", text))
    }
}

/// What a slot is bound to: one record, or a named pool.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CredentialTarget {
    Record {
        kind: CredentialKind,
        provider: String,
        name: String,
    },
    Pool(String),
}

impl fmt::Display for CredentialTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialTarget::Record {
                kind,
                provider,
                name,
            } => write!(f, "record:{kind}/{provider}/{name}"),
            CredentialTarget::Pool(name) => write!(f, "pool:{name}"),
        }
    }
}

impl FromStr for CredentialTarget {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        if let Some(name) = text.strip_prefix("pool:") {
            return if valid_segment(name) {
                Ok(Self::Pool(name.into()))
            } else {
                Err(invalid("target", text))
            };
        }
        let record = text
            .strip_prefix("record:")
            .ok_or_else(|| invalid("target", text))?;
        let mut parts = record.split('/');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(kind), Some(provider), Some(name), None)
                if valid_segment(provider) && valid_segment(name) =>
            {
                Ok(Self::Record {
                    kind: kind.parse().map_err(|_| invalid("target", text))?,
                    provider: provider.into(),
                    name: name.into(),
                })
            }
            _ => Err(invalid("target", text)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialBinding {
    pub agent_id: AgentId,
    pub slot: CredentialSlot,
    pub target: CredentialTarget,
    pub set_by: String,
    pub set_at_ms: i64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialBindingAudit {
    pub id: i64,
    pub agent_id: AgentId,
    pub slot: CredentialSlot,
    pub before_target: Option<CredentialTarget>,
    pub after_target: Option<CredentialTarget>,
    pub actor: String,
    pub reason: String,
    pub at_ms: i64,
}

/// The pool member picked for a slot at launch. Never a binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialObservation {
    pub agent_id: AgentId,
    pub slot: CredentialSlot,
    pub record: CredentialTarget,
    pub generation: i64,
    pub reason: String,
    pub at_ms: i64,
}

fn column<T: FromStr<Err = Error>>(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    text.parse().map_err(|err: Error| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn optional_column<T: FromStr<Err = Error>>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    let text: Option<String> = row.get(index)?;
    text.map(|text| {
        text.parse().map_err(|err: Error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    })
    .transpose()
}

fn agent_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<AgentId> {
    let text: String = row.get(index)?;
    AgentId::parse(&text).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

const BINDING_COLS: &str = "agent_id, slot, target, set_by, set_at_ms, reason";

fn binding_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CredentialBinding> {
    Ok(CredentialBinding {
        agent_id: agent_column(row, 0)?,
        slot: column(row, 1)?,
        target: column(row, 2)?,
        set_by: row.get(3)?,
        set_at_ms: row.get(4)?,
        reason: row.get(5)?,
    })
}

fn binding_of(
    conn: &Connection,
    agent_id: &AgentId,
    slot: &CredentialSlot,
) -> Result<Option<CredentialBinding>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {BINDING_COLS} FROM credential_bindings WHERE agent_id = ?1 AND slot = ?2"
            ),
            params![agent_id.as_str(), slot.to_string()],
            binding_from_row,
        )
        .optional()?)
}

fn audit(
    tx: &Transaction<'_>,
    agent_id: &AgentId,
    slot: &str,
    before: Option<&str>,
    after: Option<&str>,
    actor: &str,
    reason: &str,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO credential_binding_audit (agent_id, slot, before_target, after_target, actor, reason, at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![agent_id.as_str(), slot, before, after, actor, reason, now_ms()],
    )?;
    Ok(())
}

fn require_actor(actor: &str, reason: &str) -> Result<(), Error> {
    if actor.trim().is_empty() || reason.trim().is_empty() {
        return Err(Error::InvalidCredential(
            "a binding change needs an actor and a reason".into(),
        ));
    }
    Ok(())
}

/// Remove every binding of a deleted agent, one audit row each, inside the
/// deleting transaction.
pub(super) fn delete_bindings_for_deleted_agent(
    tx: &Transaction<'_>,
    agent_id: &AgentId,
    actor: &str,
) -> Result<(), Error> {
    let rows: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT slot, target FROM credential_bindings WHERE agent_id = ?1 ORDER BY slot",
        )?;
        let rows = stmt.query_map([agent_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<Result<_, _>>()?
    };
    for (slot, target) in rows {
        audit(
            tx,
            agent_id,
            &slot,
            Some(&target),
            None,
            actor,
            "agent deleted",
        )?;
    }
    tx.execute(
        "DELETE FROM credential_bindings WHERE agent_id = ?1",
        [agent_id.as_str()],
    )?;
    tx.execute(
        "DELETE FROM credential_observations WHERE agent_id = ?1",
        [agent_id.as_str()],
    )?;
    Ok(())
}

impl Store {
    fn credential_write<T>(
        &self,
        mut op: impl FnMut(&Transaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let out = op(&tx)?;
            tx.commit()?;
            Ok(out)
        })
    }

    /// Bind `slot` to `target`. Writes the row and one audit line in one
    /// commit; binding the target already bound returns the row unchanged and
    /// writes no audit line. Authorization is the caller's.
    pub fn bind_credential(
        &self,
        agent_id: &AgentId,
        slot: &CredentialSlot,
        target: &CredentialTarget,
        actor: &str,
        reason: &str,
    ) -> Result<CredentialBinding, Error> {
        require_actor(actor, reason)?;
        self.credential_write(|tx| {
            super::agent_by_id(tx, agent_id)?;
            let before = binding_of(tx, agent_id, slot)?;
            if let Some(existing) = before.as_ref().filter(|b| &b.target == target) {
                return Ok(existing.clone());
            }
            let slot_text = slot.to_string();
            let target_text = target.to_string();
            tx.execute(
                "INSERT INTO credential_bindings (agent_id, slot, target, set_by, set_at_ms, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(agent_id, slot) DO UPDATE SET target = ?3, set_by = ?4, set_at_ms = ?5, reason = ?6",
                params![agent_id.as_str(), slot_text, target_text, actor, now_ms(), reason],
            )?;
            let before_text = before.map(|b| b.target.to_string());
            audit(tx, agent_id, &slot_text, before_text.as_deref(), Some(&target_text), actor, reason)?;
            binding_of(tx, agent_id, slot)?
                .ok_or_else(|| Error::Message("credential binding vanished".into()))
        })
    }

    /// Remove a binding, with one audit line. `None` when nothing was bound.
    pub fn unbind_credential(
        &self,
        agent_id: &AgentId,
        slot: &CredentialSlot,
        actor: &str,
        reason: &str,
    ) -> Result<Option<CredentialBinding>, Error> {
        require_actor(actor, reason)?;
        self.credential_write(|tx| {
            let Some(before) = binding_of(tx, agent_id, slot)? else {
                return Ok(None);
            };
            let slot_text = slot.to_string();
            tx.execute(
                "DELETE FROM credential_bindings WHERE agent_id = ?1 AND slot = ?2",
                params![agent_id.as_str(), slot_text],
            )?;
            audit(
                tx,
                agent_id,
                &slot_text,
                Some(&before.target.to_string()),
                None,
                actor,
                reason,
            )?;
            Ok(Some(before))
        })
    }

    pub fn credential_binding(
        &self,
        agent_id: &AgentId,
        slot: &CredentialSlot,
    ) -> Result<Option<CredentialBinding>, Error> {
        binding_of(&self.read_conn(), agent_id, slot)
    }

    /// The agent's bindings, ordered by slot.
    pub fn credential_bindings_of(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<CredentialBinding>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {BINDING_COLS} FROM credential_bindings WHERE agent_id = ?1 ORDER BY slot"
        ))?;
        let rows = stmt.query_map([agent_id.as_str()], binding_from_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Every binding, ordered by agent and slot.
    pub fn list_credential_bindings(&self) -> Result<Vec<CredentialBinding>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {BINDING_COLS} FROM credential_bindings ORDER BY agent_id, slot"
        ))?;
        let rows = stmt.query_map([], binding_from_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The agent's binding audit trail, oldest first.
    pub fn credential_binding_audit(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<CredentialBindingAudit>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(
            "SELECT id, agent_id, slot, before_target, after_target, actor, reason, at_ms
             FROM credential_binding_audit WHERE agent_id = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map([agent_id.as_str()], |row| {
            Ok(CredentialBindingAudit {
                id: row.get(0)?,
                agent_id: agent_column(row, 1)?,
                slot: column(row, 2)?,
                before_target: optional_column(row, 3)?,
                after_target: optional_column(row, 4)?,
                actor: row.get(5)?,
                reason: row.get(6)?,
                at_ms: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Record which record served a pool-bound slot at launch.
    pub fn record_credential_observation(
        &self,
        agent_id: &AgentId,
        slot: &CredentialSlot,
        record: &CredentialTarget,
        generation: i64,
        reason: &str,
    ) -> Result<(), Error> {
        if !matches!(record, CredentialTarget::Record { .. }) {
            return Err(Error::InvalidCredential(format!(
                "an observation names a record, not `{record}`"
            )));
        }
        self.credential_write(|tx| {
            super::agent_by_id(tx, agent_id)?;
            tx.execute(
                "INSERT INTO credential_observations (agent_id, slot, record, generation, reason, at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(agent_id, slot) DO UPDATE SET record = ?3, generation = ?4, reason = ?5, at_ms = ?6",
                params![agent_id.as_str(), slot.to_string(), record.to_string(), generation, reason, now_ms()],
            )?;
            Ok(())
        })
    }

    pub fn credential_observation(
        &self,
        agent_id: &AgentId,
        slot: &CredentialSlot,
    ) -> Result<Option<CredentialObservation>, Error> {
        Ok(self
            .read_conn()
            .query_row(
                "SELECT agent_id, slot, record, generation, reason, at_ms
                 FROM credential_observations WHERE agent_id = ?1 AND slot = ?2",
                params![agent_id.as_str(), slot.to_string()],
                |row| {
                    Ok(CredentialObservation {
                        agent_id: agent_column(row, 0)?,
                        slot: column(row, 1)?,
                        record: column(row, 2)?,
                        generation: row.get(3)?,
                        reason: row.get(4)?,
                        at_ms: row.get(5)?,
                    })
                },
            )
            .optional()?)
    }
}
