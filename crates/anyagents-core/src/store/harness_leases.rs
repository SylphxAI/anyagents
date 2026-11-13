//! Managed-harness version leases (ADR 0005): one row per (agent, harness),
//! held by one process from spawn to exit. Garbage collection never removes a
//! version that a lease names, and a failed read is never an empty set.

use std::collections::BTreeSet;

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::runtime::ProcessIdentity;
use super::{AgentId, Error, Store, agent_by_id, now_ms};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessLease {
    pub agent_id: AgentId,
    pub harness: String,
    pub version: String,
    pub holder: ProcessIdentity,
    pub acquired_at_ms: i64,
}

const COLS: &str = "agent_id, harness, version, holder_pid, holder_start_time, acquired_at_ms";

fn lease_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HarnessLease> {
    let agent: String = row.get(0)?;
    Ok(HarnessLease {
        agent_id: AgentId::parse(&agent).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?,
        harness: row.get(1)?,
        version: row.get(2)?,
        holder: ProcessIdentity {
            pid: row.get(3)?,
            start_time: row.get(4)?,
        },
        acquired_at_ms: row.get(5)?,
    })
}

impl Store {
    /// Take (or replace) the agent's lease on `harness` at `version`.
    pub fn acquire_harness_lease(
        &self,
        agent_id: &AgentId,
        harness: &str,
        version: &str,
        holder: ProcessIdentity,
    ) -> Result<HarnessLease, Error> {
        if harness.trim().is_empty() || version.trim().is_empty() {
            return Err(Error::Message(
                "a harness lease needs a harness and a version".into(),
            ));
        }
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            agent_by_id(&tx, agent_id)?;
            tx.execute(
                &format!(
                    "INSERT INTO harness_leases ({COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(agent_id, harness) DO UPDATE SET version = ?3, holder_pid = ?4,
                         holder_start_time = ?5, acquired_at_ms = ?6"
                ),
                params![
                    agent_id.as_str(),
                    harness,
                    version,
                    holder.pid,
                    holder.start_time,
                    now_ms()
                ],
            )?;
            let lease = tx
                .query_row(
                    &format!(
                        "SELECT {COLS} FROM harness_leases WHERE agent_id = ?1 AND harness = ?2"
                    ),
                    params![agent_id.as_str(), harness],
                    lease_from_row,
                )
                .optional()?
                .ok_or_else(|| Error::Message("harness lease vanished".into()))?;
            tx.commit()?;
            Ok(lease)
        })
    }

    /// Release the lease only when `holder` still holds it, so a late release
    /// from an exited process never drops its replacement's lease.
    pub fn release_harness_lease(
        &self,
        agent_id: &AgentId,
        harness: &str,
        holder: &ProcessIdentity,
    ) -> Result<bool, Error> {
        self.write_with_retry(|conn| {
            Ok(conn.execute(
                "DELETE FROM harness_leases WHERE agent_id = ?1 AND harness = ?2
                 AND holder_pid = ?3 AND holder_start_time = ?4",
                params![agent_id.as_str(), harness, holder.pid, holder.start_time],
            )? > 0)
        })
    }

    /// Every lease on `harness`, ordered by agent.
    pub fn harness_leases(&self, harness: &str) -> Result<Vec<HarnessLease>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {COLS} FROM harness_leases WHERE harness = ?1 ORDER BY agent_id"
        ))?;
        let rows = stmt.query_map([harness], lease_from_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The versions of `harness` that some lease holds. An error is returned
    /// as an error, never as an empty set.
    pub fn leased_versions(&self, harness: &str) -> Result<BTreeSet<String>, Error> {
        let conn = self.read_conn();
        let mut stmt =
            conn.prepare("SELECT DISTINCT version FROM harness_leases WHERE harness = ?1")?;
        let rows = stmt.query_map([harness], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Drop every lease whose holder `is_alive` reports gone. Returns the count.
    pub fn reclaim_harness_leases(
        &self,
        is_alive: impl Fn(&ProcessIdentity) -> bool,
    ) -> Result<usize, Error> {
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let leases: Vec<HarnessLease> = {
                let mut stmt = tx.prepare(&format!("SELECT {COLS} FROM harness_leases"))?;
                let rows = stmt.query_map([], lease_from_row)?;
                rows.collect::<Result<_, _>>()?
            };
            let mut dropped = 0;
            for lease in leases.iter().filter(|lease| !is_alive(&lease.holder)) {
                dropped += tx.execute(
                    "DELETE FROM harness_leases WHERE agent_id = ?1 AND harness = ?2
                     AND holder_pid = ?3 AND holder_start_time = ?4",
                    params![
                        lease.agent_id.as_str(),
                        lease.harness,
                        lease.holder.pid,
                        lease.holder.start_time
                    ],
                )?;
            }
            tx.commit()?;
            Ok(dropped)
        })
    }
}
