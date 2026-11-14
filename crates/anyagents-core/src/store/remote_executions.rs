//! Remote executions: the durable record of an agent that runs as a vendor's
//! remote session (ADR 0010 D6, D7).
//!
//! A remote session has no local process, so nothing on this host holds its
//! state across a server restart except this row. It names the vendor session
//! and the adapter's own handle on it (agent, environment, vault ids), what
//! the kernel already accounted for, and the note the supervisor keeps with a
//! process it hands to the next kernel. The kernel that starts next reads the
//! rows still `attached` and reconnects to each instead of creating a session.

use anyagents_harness::TurnUsage;
use rusqlite::{OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};

use super::{AgentId, Error, Store, agent_exists, now_ms};

/// The values of `remote_executions.status`.
pub mod remote_execution_status {
    /// The vendor is creating the session; no vendor session id yet.
    pub const CREATING: &str = "creating";
    /// A kernel holds the session's connection, or must take it over.
    pub const ATTACHED: &str = "attached";
    /// The agent sleeps: the vendor session lives on, nothing is connected.
    pub const PARKED: &str = "parked";
    /// The vendor session is over; the next start creates a new one.
    pub const ENDED: &str = "ended";
}

/// Usage and finished turns of one vendor session, as totals.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTotals {
    #[serde(flatten)]
    pub usage: TurnUsage,
    #[serde(default)]
    pub turns: u32,
}

/// Where an attach resumes: the last event cursor applied, and the recent
/// mailbox ids and host tool calls the session already holds, so a replayed
/// event is applied once and a replayed call is never run twice.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteResume {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub answered: Vec<String>,
}

/// One agent's remote execution.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteExecution {
    pub agent_id: AgentId,
    /// The adapter that drives the vendor session (`claude-managed`, ...).
    pub adapter: String,
    /// The harness and model the session was created for.
    pub harness: String,
    pub model: Option<String>,
    pub vendor_session: Option<String>,
    /// The adapter's own handle on the session (vendor agent, environment,
    /// vault ids). Never a secret.
    pub adapter_state: serde_json::Value,
    pub status: String,
    /// What the vendor last said about the session (`running`, `idle`,
    /// `budget_reached`, `terminated`).
    pub vendor_status: Option<String>,
    /// The remote session token session (`rs_…`) whose tokens the vendor holds.
    pub token_session: Option<String>,
    /// The spend cap the vendor enforces, in US cents.
    pub budget_cents: Option<i64>,
    /// What the session has cost so far, at list price, in US cents.
    pub cost_cents: Option<i64>,
    /// Usage and cost of the session's finished turns, and how many.
    pub totals: RemoteTotals,
    /// What the turn still running has used so far.
    pub open_usage: TurnUsage,
    /// The totals when the current connection lineage began: what the
    /// supervisor already accounted for before it, under another "process".
    pub base: RemoteTotals,
    /// Where the next attach resumes, and what the session already holds.
    pub resume: RemoteResume,
    pub process_started_at_ms: Option<i64>,
    /// What the last kernel kept with the session for the next one.
    pub note: Option<String>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl RemoteExecution {
    /// Whether a kernel that starts must reconnect to this session.
    pub fn is_attached(&self) -> bool {
        self.status == remote_execution_status::ATTACHED && self.vendor_session.is_some()
    }

    /// Whether the vendor session may still be reconnected to.
    pub fn is_live(&self) -> bool {
        self.vendor_session.is_some()
            && matches!(
                self.status.as_str(),
                remote_execution_status::ATTACHED | remote_execution_status::PARKED
            )
    }
}

/// Fields to change on a row; `None` leaves a field as it is.
#[derive(Clone, Debug, Default)]
pub struct RemoteExecutionPatch {
    pub status: Option<String>,
    pub vendor_session: Option<String>,
    pub adapter_state: Option<serde_json::Value>,
    pub vendor_status: Option<String>,
    pub token_session: Option<String>,
    pub budget_cents: Option<i64>,
    pub cost_cents: Option<i64>,
    pub totals: Option<RemoteTotals>,
    pub open_usage: Option<TurnUsage>,
    pub base: Option<RemoteTotals>,
    pub resume: Option<RemoteResume>,
    pub process_started_at_ms: Option<i64>,
    pub note: Option<String>,
    /// `Some(None)` clears the error.
    pub last_error: Option<Option<String>>,
}

const COLUMNS: &str = "agent_id, adapter, harness, model, vendor_session, adapter_state, status, \
     vendor_status, token_session, budget_cents, cost_cents, totals, base, process_started_at_ms, \
     note, last_error, created_at_ms, updated_at_ms, resume, open_usage";

fn json_or_default<T: for<'de> Deserialize<'de> + Default>(text: &str) -> T {
    serde_json::from_str(text).unwrap_or_default()
}

/// One row as read, before its agent id is checked.
struct Raw(String, RemoteExecution);

fn from_row(row: &Row<'_>) -> rusqlite::Result<Raw> {
    let agent_id: String = row.get(0)?;
    let adapter_state: String = row.get(5)?;
    let totals: String = row.get(11)?;
    let base: String = row.get(12)?;
    let placeholder = AgentId::parse(&agent_id).unwrap_or_else(|_| AgentId::generate());
    Ok(Raw(
        agent_id,
        RemoteExecution {
            agent_id: placeholder,
            adapter: row.get(1)?,
            harness: row.get(2)?,
            model: row.get(3)?,
            vendor_session: row.get(4)?,
            adapter_state: serde_json::from_str(&adapter_state)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            status: row.get(6)?,
            vendor_status: row.get(7)?,
            token_session: row.get(8)?,
            budget_cents: row.get(9)?,
            cost_cents: row.get(10)?,
            totals: json_or_default(&totals),
            base: json_or_default(&base),
            process_started_at_ms: row.get(13)?,
            note: row.get(14)?,
            last_error: row.get(15)?,
            created_at_ms: row.get(16)?,
            updated_at_ms: row.get(17)?,
            resume: json_or_default(&row.get::<_, String>(18)?),
            open_usage: json_or_default(&row.get::<_, String>(19)?),
        },
    ))
}

/// A row whose agent id does not parse is an error, never a made-up agent.
fn finish(raw: Raw) -> Result<RemoteExecution, Error> {
    let Raw(agent_id, mut row) = raw;
    row.agent_id = AgentId::parse(&agent_id)?;
    Ok(row)
}

fn to_json<T: Serialize>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value).map_err(|err| Error::Message(err.to_string()))
}

impl Store {
    /// Start a new record for `agent_id`: a vendor session is about to be
    /// created. Any earlier record of the agent is replaced, so the caller
    /// ends the earlier vendor session first.
    pub fn begin_remote_execution(
        &self,
        agent_id: &AgentId,
        adapter: &str,
        harness: &str,
        model: Option<&str>,
        budget_cents: Option<i64>,
    ) -> Result<RemoteExecution, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.execute(
                "INSERT OR REPLACE INTO remote_executions
                    (agent_id, adapter, harness, model, vendor_session, adapter_state, status,
                     vendor_status, token_session, budget_cents, cost_cents, totals, base,
                     resume, process_started_at_ms, note, last_error, created_at_ms,
                     updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, NULL, '{}', ?5, NULL, NULL, ?6, NULL, '{}', '{}',
                         '{}', NULL, NULL, NULL, ?7, ?7)",
                params![
                    agent_id.as_str(),
                    adapter,
                    harness,
                    model,
                    remote_execution_status::CREATING,
                    budget_cents,
                    now
                ],
            )?;
            Ok(())
        })?;
        self.remote_execution(agent_id)?
            .ok_or_else(|| Error::AgentNotFound(agent_id.to_string()))
    }

    /// The agent's remote execution, when it has one.
    pub fn remote_execution(&self, agent_id: &AgentId) -> Result<Option<RemoteExecution>, Error> {
        let conn = self.read_conn();
        let found = conn
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM remote_executions WHERE agent_id = ?1"
            ))?
            .query_row([agent_id.as_str()], from_row)
            .optional()?;
        found.map(finish).transpose()
    }

    /// Every remote execution, by agent id.
    pub fn remote_executions(&self) -> Result<Vec<RemoteExecution>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLUMNS} FROM remote_executions ORDER BY agent_id"
        ))?;
        let rows = stmt.query_map([], from_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(finish(row?)?);
        }
        Ok(out)
    }

    /// Change the fields `patch` names. Returns whether the agent has a row.
    pub fn update_remote_execution(
        &self,
        agent_id: &AgentId,
        patch: &RemoteExecutionPatch,
    ) -> Result<bool, Error> {
        let mut sets: Vec<String> = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        let mut set = |column: &str, value: rusqlite::types::Value| {
            values.push(value);
            sets.push(format!("{column} = ?{}", values.len()));
        };
        use rusqlite::types::Value as V;
        let text = |s: &str| V::Text(s.to_string());
        if let Some(value) = &patch.status {
            set("status", text(value));
        }
        if let Some(value) = &patch.vendor_session {
            set("vendor_session", text(value));
        }
        if let Some(value) = &patch.adapter_state {
            set("adapter_state", V::Text(to_json(value)?));
        }
        if let Some(value) = &patch.vendor_status {
            set("vendor_status", text(value));
        }
        if let Some(value) = &patch.token_session {
            set("token_session", text(value));
        }
        if let Some(value) = patch.budget_cents {
            set("budget_cents", V::Integer(value));
        }
        if let Some(value) = patch.cost_cents {
            set("cost_cents", V::Integer(value));
        }
        if let Some(value) = &patch.totals {
            set("totals", V::Text(to_json(value)?));
        }
        if let Some(value) = &patch.base {
            set("base", V::Text(to_json(value)?));
        }
        if let Some(value) = &patch.open_usage {
            set("open_usage", V::Text(to_json(value)?));
        }
        if let Some(value) = &patch.resume {
            set("resume", V::Text(to_json(value)?));
        }
        if let Some(value) = patch.process_started_at_ms {
            set("process_started_at_ms", V::Integer(value));
        }
        if let Some(value) = &patch.note {
            set("note", text(value));
        }
        if let Some(value) = &patch.last_error {
            set("last_error", value.as_deref().map_or(V::Null, text));
        }
        set("updated_at_ms", V::Integer(now_ms()));
        values.push(V::Text(agent_id.as_str().to_string()));
        let sql = format!(
            "UPDATE remote_executions SET {} WHERE agent_id = ?{}",
            sets.join(", "),
            values.len()
        );
        self.write_tx(|tx| Ok(tx.execute(&sql, rusqlite::params_from_iter(values.iter()))? > 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    #[test]
    fn a_record_is_created_patched_listed_and_deleted_with_its_agent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("cloud", "claude-cloud"))
            .unwrap();
        assert!(store.remote_execution(&agent.id).unwrap().is_none());

        let row = store
            .begin_remote_execution(
                &agent.id,
                "claude-managed",
                "claude-cloud",
                Some("m"),
                Some(2500),
            )
            .unwrap();
        assert_eq!(row.status, remote_execution_status::CREATING);
        assert!(!row.is_attached() && !row.is_live());
        assert_eq!(row.budget_cents, Some(2500));

        let totals = RemoteTotals {
            usage: TurnUsage {
                input_tokens: 10,
                output_tokens: 5,
                reported_cost_micros: Some(70_000),
                ..TurnUsage::default()
            },
            turns: 2,
        };
        assert!(
            store
                .update_remote_execution(
                    &agent.id,
                    &RemoteExecutionPatch {
                        status: Some(remote_execution_status::ATTACHED.into()),
                        vendor_session: Some("sesn_1".into()),
                        adapter_state: Some(serde_json::json!({"vault_id": "vlt_1"})),
                        totals: Some(totals.clone()),
                        resume: Some(RemoteResume {
                            cursor: Some("sevt_9".into()),
                            held: vec!["msg_1".into()],
                            answered: Vec::new(),
                        }),
                        last_error: Some(Some("boom".into())),
                        ..RemoteExecutionPatch::default()
                    },
                )
                .unwrap()
        );
        let row = store.remote_execution(&agent.id).unwrap().unwrap();
        assert!(row.is_attached() && row.is_live());
        assert_eq!(row.totals, totals);
        assert_eq!(row.base, RemoteTotals::default());
        assert_eq!(row.resume.cursor.as_deref(), Some("sevt_9"));
        assert_eq!(row.resume.held, vec!["msg_1".to_string()]);
        assert_eq!(row.adapter_state["vault_id"], "vlt_1");
        assert_eq!(row.last_error.as_deref(), Some("boom"));
        store
            .update_remote_execution(
                &agent.id,
                &RemoteExecutionPatch {
                    last_error: Some(None),
                    ..RemoteExecutionPatch::default()
                },
            )
            .unwrap();
        assert_eq!(
            store
                .remote_execution(&agent.id)
                .unwrap()
                .unwrap()
                .last_error,
            None
        );
        assert_eq!(store.remote_executions().unwrap().len(), 1);

        store.delete_agent(&agent.id).unwrap();
        assert!(store.remote_executions().unwrap().is_empty());
    }
}
