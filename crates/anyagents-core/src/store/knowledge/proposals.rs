use serde::Serialize;

use super::{
    Actor, Edit, Error, Mode, Store, db_params, invalid, key, load, new_id, now_ms, require, save,
};
use crate::db::{DbBackend, DbExec, DbExecExt, DbRow};

const FIELDS: &str = "id, namespace, topic, actor_id, expected_version, edit_json, created_at_ms, status, reviewed_by, reviewed_at_ms";

#[derive(Debug, Serialize)]
pub struct Proposal {
    pub id: String,
    pub namespace: String,
    pub topic: String,
    pub actor_id: String,
    pub expected_version: i64,
    pub edit: Edit,
    pub created_at_ms: i64,
    pub status: String,
    pub reviewed_by: Option<String>,
    pub reviewed_at_ms: Option<i64>,
}

fn row(r: &DbRow<'_>) -> Result<Proposal, Error> {
    let edit: String = r.get(5)?;
    Ok(Proposal {
        id: r.get(0)?,
        namespace: r.get(1)?,
        topic: r.get(2)?,
        actor_id: r.get(3)?,
        expected_version: r.get(4)?,
        edit: serde_json::from_str(&edit).map_err(|e| Error::Message(e.to_string()))?,
        created_at_ms: r.get(6)?,
        status: r.get(7)?,
        reviewed_by: r.get(8)?,
        reviewed_at_ms: r.get(9)?,
    })
}

impl Store {
    pub fn knowledge_propose(
        &self,
        actor: &Actor,
        namespace: &str,
        topic: &str,
        expected_version: i64,
        edit: &Edit,
    ) -> Result<Proposal, Error> {
        key(namespace, topic)?;
        edit.validate()?;
        if expected_version < 0 || (expected_version == 0 && edit.body.is_none()) {
            return Err(invalid(
                "expected_version must be nonnegative and creates require a body",
            ));
        }
        // A proposal alone does not let its author discover whether a doc exists.
        self.write_tx(|tx| {
            require(tx, actor, namespace, Mode::Propose, true)?;
            let id = new_id("kprop");
            let edit_json = serde_json::to_string(edit).map_err(|e| Error::Message(e.to_string()))?;
            tx.db_execute("INSERT INTO knowledge_proposals (id, namespace, topic, actor_id, expected_version, edit_json,
                created_at_ms, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending')",
                &db_params![id, namespace, topic, actor.id(), expected_version, edit_json, now_ms()])?;
            tx.db_query_one(&format!("SELECT {FIELDS} FROM knowledge_proposals WHERE id = ?1"), &db_params![id], row)
        })
    }

    pub fn knowledge_proposals(
        &self,
        actor: &Actor,
        namespace: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Proposal>, Error> {
        super::access::validate_namespace(namespace)?;
        // Lock the matching grant through the payload read, so revoke and review
        // serialize even for a write-only reviewer.
        self.write_tx(|tx| {
            require(tx, actor, namespace, Mode::Write, true)?;
            tx.db_query_vec(
                &format!(
                    "SELECT {FIELDS} FROM knowledge_proposals
                WHERE namespace = ?1 AND status = 'pending' AND id > ?2 ORDER BY id LIMIT ?3"
                ),
                &db_params![namespace, after, limit.clamp(1, 10) as i64],
                row,
            )
        })
    }

    pub fn knowledge_review(
        &self,
        actor: &Actor,
        namespace: &str,
        id: &str,
        expected: Option<i64>,
        promote: bool,
    ) -> Result<(Proposal, Option<super::Doc>), Error> {
        super::access::validate_namespace(namespace)?;
        self.write_tx(|tx| {
            require(tx, actor, namespace, Mode::Write, true)?;
            let lock = if tx.backend() == DbBackend::Postgres { " FOR UPDATE" } else { "" };
            let mut p = tx.db_query_opt(&format!("SELECT {FIELDS} FROM knowledge_proposals WHERE namespace = ?1 AND id = ?2{lock}"),
                &db_params![namespace, id], row)?.ok_or(Error::KnowledgeMissing)?;
            if p.status != "pending" { return Err(Error::KnowledgeReviewed); }
            let doc = if promote {
                // Do not silently retarget an old edit to a newer base, even if
                // the reviewer supplies the newer version.
                if expected != Some(p.expected_version) {
                    return Err(Error::KnowledgeConflict { current: load(tx, namespace, &p.topic)?.map_or(0, |d| d.version) });
                }
                Some(save(tx, actor, namespace, &p.topic, expected, &p.edit, false)?)
            } else { None };
            p.status = if promote { "promoted" } else { "rejected" }.into();
            p.reviewed_at_ms = Some(now_ms());
            p.reviewed_by = Some(actor.id());
            tx.db_execute("UPDATE knowledge_proposals SET status = ?1, reviewed_by = ?2, reviewed_at_ms = ?3 WHERE id = ?4",
                &db_params![p.status, p.reviewed_by, p.reviewed_at_ms, p.id])?;
            Ok((p, doc))
        })
    }
}
