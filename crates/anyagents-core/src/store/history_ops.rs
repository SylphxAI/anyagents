//! Narrow reads for host history search, on the read pool: the two columns
//! of an agent row that indexing needs, and the redaction receipts.
use rusqlite::OptionalExtension;

use super::Store;
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
        let mut statement = conn.prepare_cached("SELECT name FROM agents WHERE id = ?1")?;
        let mut names = Vec::new();
        for id in ids {
            if let Some(name) = statement
                .query_row([id], |row| row.get::<_, String>(0))
                .optional()?
            {
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
        let mut statement = match agent {
            Some(_) => conn.prepare_cached(
                "SELECT agent_id, receipt FROM conversation_redactions WHERE agent_id = ?1",
            )?,
            None => conn.prepare_cached("SELECT agent_id, receipt FROM conversation_redactions")?,
        };
        let rows = match agent {
            Some(agent) => statement.query([agent])?,
            None => statement.query([])?,
        };
        Ok(rows
            .mapped(|row| Ok((row.get(0)?, row.get(1)?)))
            .collect::<Result<_, _>>()?)
    }
}
