//! Narrow reads for host history search, on the read pool: the two columns
//! of an agent row that indexing needs, and the redaction receipts.

use super::Store;
use crate::db::{DbExecExt as _, db_params};
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// The current name of each listed agent; an unknown id is skipped. One
    /// primary-key lookup per id, so a caller pays for the agents it may see.
    pub(crate) fn agent_names<'a>(
        &self,
        ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<(AgentId, String)>, Error> {
        let conn = self.read_conn();
        let mut names = Vec::new();
        for id in ids {
            if let Some(name) = conn.db_query_opt(
                "SELECT name FROM agents WHERE id = ?1",
                &db_params![id],
                |row| row.get::<String>(0),
            )? {
                names.push((AgentId::parse(id)?, name));
            }
        }
        Ok(names)
    }

    /// `(agent id, receipt JSON)` of every redaction, or of one agent's.
    pub(crate) fn redaction_receipts(
        &self,
        agent: Option<&str>,
    ) -> Result<Vec<(String, String)>, Error> {
        let conn = self.read_conn();
        let (sql, params) = match agent {
            Some(agent) => (
                "SELECT agent_id, receipt FROM conversation_redactions WHERE agent_id = ?1",
                db_params![agent],
            ),
            None => (
                "SELECT agent_id, receipt FROM conversation_redactions",
                db_params![],
            ),
        };
        conn.db_query_vec(sql, &params, |row| Ok((row.get(0)?, row.get(1)?)))
    }
}
