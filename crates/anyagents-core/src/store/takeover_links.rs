//! Bounded history for identifying retired viewer links. This is not tunnel custody.

use super::{AgentId, Error, Store, now_ms};
use crate::db::{DbExecExt as _, db_params};

const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

pub struct TakeoverLink {
    pub agent_id: AgentId,
    pub expires_ms: Option<i64>,
    pub ended_ms: Option<i64>,
}

impl Store {
    pub fn remember_takeover_link(
        &self,
        session_id: &str,
        token_hash: &str,
        agent: &AgentId,
        expires_ms: Option<i64>,
    ) -> Result<(), Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM takeover_links WHERE retain_until_ms <= ?1",
                &db_params![now],
            )?;
            tx.db_execute(
                "INSERT INTO takeover_links (session_id, token_hash, agent_id, expires_ms, retain_until_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (session_id, token_hash) DO NOTHING",
                &db_params![session_id, token_hash, agent.as_str(), expires_ms,
                    expires_ms.unwrap_or(now).saturating_add(RETENTION_MS)],
            )?;
            Ok(())
        })
    }

    pub fn end_takeover_link(&self, session_id: &str, token_hash: &str) -> Result<(), Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE takeover_links SET ended_ms = ?3, retain_until_ms = ?4
                 WHERE session_id = ?1 AND token_hash = ?2 AND ended_ms IS NULL",
                &db_params![
                    session_id,
                    token_hash,
                    now,
                    now.saturating_add(RETENTION_MS)
                ],
            )?;
            Ok(())
        })
    }

    pub fn takeover_link(
        &self,
        session_id: &str,
        token_hash: &str,
    ) -> Result<Option<TakeoverLink>, Error> {
        let row: Option<(String, Option<i64>, Option<i64>)> = self.read_conn().db_query_opt(
            "SELECT agent_id, expires_ms, ended_ms FROM takeover_links
             WHERE session_id = ?1 AND token_hash = ?2 AND retain_until_ms > ?3",
            &db_params![session_id, token_hash, now_ms()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        row.map(|(agent, expires_ms, ended_ms)| {
            Ok(TakeoverLink {
                agent_id: AgentId::parse(&agent)?,
                expires_ms,
                ended_ms,
            })
        })
        .transpose()
    }
}
