//! Versioned knowledge. Authorization depends only on the authenticated actor
//! and explicit namespace capabilities, never on roster metadata.

use serde::{Deserialize, Serialize};

use super::{Store, now_ms, rag_content_hash};
use crate::db::{DbExec, DbExecExt, DbParam, DbRow, DbTx};
use crate::{AgentId, Error, db_params};

mod access;
mod proposals;
mod search;
pub(super) mod sqlite_index;
mod wake;

pub use access::{Actor, Grant, Mode, Scope};
use access::{require, require_scope, visibility};
pub use proposals::Proposal;
pub use search::SearchHit;
pub(crate) use search::projection_fields;

const DOC: &str = "d.id, d.namespace, d.topic, d.title, d.description, d.body, d.pinned, d.version, d.updated_by, d.created_at_ms, d.updated_at_ms, d.deleted_at_ms, d.content_hash";
const MAX_BODY: usize = 16 * 1024;

#[derive(Debug, Serialize)]
pub struct Doc {
    pub id: String,
    pub namespace: String,
    pub topic: String,
    pub title: String,
    pub description: String,
    pub body: String,
    pub pinned: bool,
    pub version: i64,
    pub updated_by: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub deleted_at_ms: Option<i64>,
    #[serde(skip)]
    pub(crate) content_hash: String,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub namespace: String,
    pub topic: String,
    pub title: String,
    pub description: String,
    pub pinned: bool,
    pub version: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Edit {
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "content")]
    pub body: Option<String>,
    pub pinned: Option<bool>,
    pub reason: String,
    pub sim_problem: Option<String>,
    pub sim_expected: Option<String>,
    pub sim_observed: Option<String>,
    pub sim_context: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Revision {
    pub id: String,
    pub version: i64,
    pub actor_id: String,
    pub body: String,
    pub prior_body: Option<String>,
    pub reason: String,
    pub sim_problem: Option<String>,
    pub sim_expected: Option<String>,
    pub sim_observed: Option<String>,
    pub sim_context: Option<String>,
    pub created_at_ms: i64,
    pub deleted_at_ms: Option<i64>,
}

fn invalid(message: &str) -> Error {
    Error::KnowledgeInvalid(message.into())
}

fn key(namespace: &str, topic: &str) -> Result<(), Error> {
    access::validate_namespace(namespace)?;
    if !access::slug(topic, 120) || topic.contains(':') {
        return Err(invalid("topic must be a slug (maximum 120 bytes)"));
    }
    Ok(())
}

impl Edit {
    fn validate(&self) -> Result<(), Error> {
        if self.reason.trim().is_empty() {
            return Err(invalid("reason is required"));
        }
        for (text, max) in [
            (self.title.as_deref(), 120),
            (self.description.as_deref(), 320),
            (self.body.as_deref(), MAX_BODY),
            (Some(self.reason.as_str()), 512),
            (self.sim_problem.as_deref(), 1024),
            (self.sim_expected.as_deref(), 1024),
            (self.sim_observed.as_deref(), 1024),
            (self.sim_context.as_deref(), 1024),
        ] {
            if text.is_some_and(|s| s.len() > max || s.contains('\0')) {
                return Err(invalid("knowledge field exceeds its limit or contains NUL"));
            }
        }
        Ok(())
    }
}

fn read_doc(r: &DbRow<'_>) -> Result<Doc, Error> {
    Ok(Doc {
        id: r.get(0)?,
        namespace: r.get(1)?,
        topic: r.get(2)?,
        title: r.get(3)?,
        description: r.get(4)?,
        body: r.get(5)?,
        pinned: r.get::<i64>(6)? != 0,
        version: r.get(7)?,
        updated_by: r.get(8)?,
        created_at_ms: r.get(9)?,
        updated_at_ms: r.get(10)?,
        deleted_at_ms: r.get(11)?,
        content_hash: r.get(12)?,
    })
}

fn load(conn: &impl DbExec, namespace: &str, topic: &str) -> Result<Option<Doc>, Error> {
    conn.db_query_opt(
        &format!("SELECT {DOC} FROM knowledge_docs d WHERE d.namespace = ?1 AND d.topic = ?2"),
        &db_params![namespace, topic],
        read_doc,
    )
}

impl Store {
    pub fn knowledge_read(
        &self,
        actor: &Actor,
        namespace: &str,
        topic: &str,
    ) -> Result<Doc, Error> {
        key(namespace, topic)?;
        let conn = self.read_conn();
        let scope = Scope::Namespace(namespace.into());
        require_scope(&conn, actor, &scope)?;
        let mut params = db_params![topic];
        let visible = visibility(actor, &scope, &mut params);
        conn.db_query_opt(
            &format!("SELECT {DOC} FROM knowledge_docs d WHERE d.topic = ?1 AND ({visible})"),
            &params,
            read_doc,
        )?
        .ok_or(Error::KnowledgeMissing)
    }

    pub fn knowledge_list(
        &self,
        actor: &Actor,
        scope: &Scope,
        limit: usize,
        offset: usize,
        prefix: &str,
    ) -> Result<Vec<Summary>, Error> {
        if !prefix.is_empty() && (!access::slug(prefix, 120) || prefix.contains(':')) {
            return Err(invalid("prefix must be a topic slug prefix"));
        }
        let conn = self.read_conn();
        require_scope(&conn, actor, scope)?;
        let mut params = db_params![
            prefix,
            limit.clamp(1, 51) as i64,
            offset.min(i64::MAX as usize) as i64
        ];
        let visible = visibility(actor, scope, &mut params);
        conn.db_query_vec(
            &format!(
                "SELECT d.namespace, d.topic, d.title, d.description, d.pinned, d.version
                FROM knowledge_docs d WHERE d.deleted_at_ms IS NULL AND ({visible})
                AND substr(d.topic, 1, length(?1)) = ?1
                ORDER BY d.pinned DESC, d.namespace, d.topic LIMIT ?2 OFFSET ?3"
            ),
            &params,
            |r| {
                Ok(Summary {
                    namespace: r.get(0)?,
                    topic: r.get(1)?,
                    title: r.get(2)?,
                    description: r.get(3)?,
                    pinned: r.get::<i64>(4)? != 0,
                    version: r.get(5)?,
                })
            },
        )
    }

    pub fn knowledge_instructions(&self, actor: &Actor) -> Result<Vec<Doc>, Error> {
        let conn = self.read_conn();
        let mut params = Vec::new();
        let visible = visibility(
            actor,
            &Scope::Namespace("org:instructions".into()),
            &mut params,
        );
        conn.db_query_vec(
            &format!(
                "SELECT {DOC} FROM knowledge_docs d
            WHERE d.deleted_at_ms IS NULL AND ({visible}) ORDER BY d.pinned DESC, d.topic LIMIT 16"
            ),
            &params,
            read_doc,
        )
    }

    pub fn knowledge_write(
        &self,
        actor: &Actor,
        namespace: &str,
        topic: &str,
        expected: Option<i64>,
        edit: &Edit,
        delete: bool,
    ) -> Result<Doc, Error> {
        key(namespace, topic)?;
        edit.validate()?;
        self.write_tx(|tx| save(tx, actor, namespace, topic, expected, edit, delete))
    }

    pub fn knowledge_rollback(
        &self,
        actor: &Actor,
        namespace: &str,
        topic: &str,
        to_version: i64,
        expected: Option<i64>,
        reason: &str,
    ) -> Result<Doc, Error> {
        key(namespace, topic)?;
        self.write_tx(|tx| {
            // Rolling back discloses an older body. Both capabilities are required.
            require(tx, actor, namespace, Mode::Read, true)?;
            require(tx, actor, namespace, Mode::Write, true)?;
            let doc = load(tx, namespace, topic)?.ok_or(Error::KnowledgeMissing)?;
            let (body, deleted): (String, Option<i64>) = tx.db_query_opt(
                "SELECT body, deleted_at_ms FROM knowledge_revisions WHERE doc_id = ?1 AND version = ?2",
                &db_params![doc.id, to_version], |r| Ok((r.get(0)?, r.get(1)?)),
            )?.ok_or(Error::KnowledgeMissing)?;
            let edit = Edit { body: Some(body), reason: reason.into(), ..Edit::default() };
            edit.validate()?;
            save(tx, actor, namespace, topic, expected.or(Some(doc.version)), &edit, deleted.is_some())
        })
    }

    pub fn knowledge_revisions(
        &self,
        actor: &Actor,
        namespace: &str,
        topic: &str,
        before: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Revision>, Error> {
        key(namespace, topic)?;
        let conn = self.read_conn();
        require(&conn, actor, namespace, Mode::Read, false)?;
        let mut params = db_params![topic, before.unwrap_or(i64::MAX), limit.clamp(1, 25) as i64];
        let visible = visibility(actor, &Scope::Namespace(namespace.into()), &mut params);
        conn.db_query_vec(&format!("SELECT r.id, r.version, r.actor_id, r.body, r.prior_body, r.reason,
            r.sim_problem, r.sim_expected, r.sim_observed, r.sim_context, r.created_at_ms, r.deleted_at_ms
            FROM knowledge_revisions r JOIN knowledge_docs d ON d.id = r.doc_id
            WHERE d.topic = ?1 AND r.version < ?2 AND ({visible}) ORDER BY r.version DESC LIMIT ?3"),
            &params, |r| Ok(Revision { id: r.get(0)?, version: r.get(1)?, actor_id: r.get(2)?, body: r.get(3)?,
                prior_body: r.get(4)?, reason: r.get(5)?, sim_problem: r.get(6)?, sim_expected: r.get(7)?,
                sim_observed: r.get(8)?, sim_context: r.get(9)?, created_at_ms: r.get(10)?, deleted_at_ms: r.get(11)? }))
    }
}

fn save(
    tx: &DbTx<'_>,
    actor: &Actor,
    namespace: &str,
    topic: &str,
    expected: Option<i64>,
    edit: &Edit,
    delete: bool,
) -> Result<Doc, Error> {
    require(tx, actor, namespace, Mode::Write, true)?;
    let prior = load(tx, namespace, topic)?;
    let current = prior.as_ref().map_or(0, |d| d.version);
    if expected.unwrap_or(0) != current {
        return Err(Error::KnowledgeConflict { current });
    }
    if delete && prior.is_none() {
        return Err(Error::KnowledgeMissing);
    }
    let now = now_ms();
    let body = edit
        .body
        .as_deref()
        .or(prior.as_ref().map(|d| d.body.as_str()))
        .ok_or_else(|| invalid("body is required to create a document"))?
        .to_owned();
    let doc = Doc {
        id: prior
            .as_ref()
            .map_or_else(|| new_id("kdoc"), |d| d.id.clone()),
        namespace: namespace.into(),
        topic: topic.into(),
        title: edit
            .title
            .clone()
            .or_else(|| prior.as_ref().map(|d| d.title.clone()))
            .unwrap_or_else(|| topic.into()),
        description: edit
            .description
            .clone()
            .or_else(|| prior.as_ref().map(|d| d.description.clone()))
            .unwrap_or_default(),
        pinned: edit
            .pinned
            .unwrap_or_else(|| prior.as_ref().is_some_and(|d| d.pinned)),
        version: current
            .checked_add(1)
            .ok_or_else(|| invalid("version exhausted"))?,
        updated_by: actor.id(),
        created_at_ms: prior.as_ref().map_or(now, |d| d.created_at_ms),
        updated_at_ms: now,
        deleted_at_ms: delete.then_some(now),
        content_hash: rag_content_hash(&body),
        body,
    };
    let changed = tx.db_execute(
        "INSERT INTO knowledge_docs (id, namespace, topic, title, description, body, pinned, version,
            updated_by, created_at_ms, updated_at_ms, deleted_at_ms, content_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT (namespace, topic) DO UPDATE SET title = excluded.title,
            description = excluded.description, body = excluded.body, pinned = excluded.pinned,
            version = excluded.version, updated_by = excluded.updated_by, updated_at_ms = excluded.updated_at_ms,
            deleted_at_ms = excluded.deleted_at_ms, content_hash = excluded.content_hash
         WHERE knowledge_docs.version = ?14 AND knowledge_docs.id = excluded.id",
        &db_params![doc.id, doc.namespace, doc.topic, doc.title, doc.description, doc.body, i64::from(doc.pinned),
            doc.version, doc.updated_by, doc.created_at_ms, doc.updated_at_ms, doc.deleted_at_ms, doc.content_hash, current],
    )?;
    if changed != 1 {
        return Err(Error::KnowledgeConflict {
            current: load(tx, namespace, topic)?.map_or(0, |d| d.version),
        });
    }
    tx.db_execute("INSERT INTO knowledge_revisions (id, doc_id, namespace, topic, version, actor_id, body,
        prior_body, reason, sim_problem, sim_expected, sim_observed, sim_context, created_at_ms, deleted_at_ms)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        &db_params![new_id("krev"), doc.id, namespace, topic, doc.version, doc.updated_by, doc.body,
            prior.as_ref().map(|d| d.body.as_str()), edit.reason, edit.sim_problem, edit.sim_expected, edit.sim_observed,
            edit.sim_context, now, doc.deleted_at_ms],
    )?;
    if tx.backend() == crate::db::DbBackend::Postgres {
        search::project_postgres(tx, &doc)?;
    }
    Ok(doc)
}

fn new_id(kind: &str) -> String {
    format!("{kind}_{}", uuid::Uuid::new_v4().simple())
}
