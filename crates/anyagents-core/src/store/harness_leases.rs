//! Managed-harness version leases (ADR 0005): one row per (agent, harness),
//! held by one process from spawn to exit. Garbage collection never removes a
//! version that a lease names, and a failed read is never an empty set.

use std::collections::BTreeSet;

use super::runtime::ProcessIdentity;
use super::{AgentId, Error, Store, agent_exists, now_ms};
use crate::db::{DbExecExt as _, DbRow, db_params};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessLease {
    pub agent_id: AgentId,
    pub harness: String,
    pub version: String,
    pub holder: ProcessIdentity,
    pub acquired_at_ms: i64,
}

const COLS: &str = "agent_id, harness, version, holder_pid, holder_start_time, acquired_at_ms";

fn lease_from_row(row: &DbRow<'_>) -> Result<HarnessLease, Error> {
    Ok(HarnessLease {
        agent_id: row.get(0)?,
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
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                &format!(
                    "INSERT INTO harness_leases ({COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(agent_id, harness) DO UPDATE SET version = ?3, holder_pid = ?4,
                         holder_start_time = ?5, acquired_at_ms = ?6"
                ),
                &db_params![
                    agent_id,
                    harness,
                    version,
                    holder.pid,
                    holder.start_time,
                    now_ms()
                ],
            )?;
            let lease = tx
                .db_query_opt(
                    &format!(
                        "SELECT {COLS} FROM harness_leases WHERE agent_id = ?1 AND harness = ?2"
                    ),
                    &db_params![agent_id, harness],
                    lease_from_row,
                )?
                .ok_or_else(|| Error::Message("harness lease vanished".into()))?;
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
        self.write_tx(|tx| {
            Ok(tx.db_execute(
                "DELETE FROM harness_leases WHERE agent_id = ?1 AND harness = ?2
                 AND holder_pid = ?3 AND holder_start_time = ?4",
                &db_params![agent_id, harness, holder.pid, holder.start_time],
            )? > 0)
        })
    }

    /// Every lease on `harness`, ordered by agent.
    pub fn harness_leases(&self, harness: &str) -> Result<Vec<HarnessLease>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            &format!("SELECT {COLS} FROM harness_leases WHERE harness = ?1 ORDER BY agent_id"),
            &db_params![harness],
            lease_from_row,
        )
    }

    /// The versions of `harness` that some lease holds. An error is returned
    /// as an error, never as an empty set.
    pub fn leased_versions(&self, harness: &str) -> Result<BTreeSet<String>, Error> {
        let conn = self.read_conn();
        let rows = conn.db_query_vec(
            "SELECT DISTINCT version FROM harness_leases WHERE harness = ?1",
            &db_params![harness],
            |row| row.get::<String>(0),
        )?;
        Ok(rows.into_iter().collect())
    }

    /// Drop every lease whose holder `is_alive` reports gone. Returns the count.
    /// `is_alive` runs while this process's writer is held: it must not use the store.
    pub fn reclaim_harness_leases(
        &self,
        is_alive: impl Fn(&ProcessIdentity) -> bool,
    ) -> Result<usize, Error> {
        self.write_tx(|tx| {
            let leases = tx.db_query_vec(
                &format!("SELECT {COLS} FROM harness_leases"),
                &[],
                lease_from_row,
            )?;
            let mut dropped = 0;
            for lease in leases.iter().filter(|lease| !is_alive(&lease.holder)) {
                dropped += tx.db_execute(
                    "DELETE FROM harness_leases WHERE agent_id = ?1 AND harness = ?2
                     AND holder_pid = ?3 AND holder_start_time = ?4",
                    &db_params![
                        lease.agent_id,
                        lease.harness,
                        lease.holder.pid,
                        lease.holder.start_time
                    ],
                )?;
            }
            Ok(dropped)
        })
    }
}
