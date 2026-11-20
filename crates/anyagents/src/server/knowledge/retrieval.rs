//! Rerank authorized SQL candidates without publishing namespace content to
//! the unscoped host-history index. Return citations from the exact version
//! scored, and keep lexical order when any optional stage is unavailable.
use agent_memory::protocol::Client;
use anyagents_core::knowledge::{Actor, Scope, SearchHit};
use anyagents_core::{Error, Store};
use serde_json::{Value, json};

pub(super) fn search(
    store: &Store,
    actor: &Actor,
    scope: &Scope,
    query: &str,
    limit: usize,
) -> Result<Value, Error> {
    let limit = limit.clamp(1, 50);
    let client = (std::env::var("ANYAGENTS_KNOWLEDGE_RERANK").as_deref() == Ok("1"))
        .then(|| Client::for_data_dir(store.data_dir(), std::time::Duration::from_secs(3)))
        .flatten();
    let depth = super::super::hybrid::RERANK_DEPTH;
    let mut hits = store.knowledge_search(
        actor,
        scope,
        query,
        if client.is_some() {
            limit.max(depth)
        } else {
            limit
        },
    )?;
    let mut reranked = false;
    if let Some(client) = client {
        let head = hits.len().min(depth);
        let passages = hits[..head]
            .iter()
            .map(|hit| {
                let doc = store
                    .knowledge_read(actor, &hit.namespace, &hit.topic)
                    .ok()?;
                if doc.version != hit.version || doc.deleted_at_ms.is_some() {
                    return None;
                }
                let mut end = doc.body.len().min(2048);
                while !doc.body.is_char_boundary(end) {
                    end -= 1;
                }
                Some(format!(
                    "{}\n{}\n{}",
                    doc.title,
                    doc.description,
                    &doc.body[..end]
                ))
            })
            .collect::<Option<Vec<_>>>();
        if let Some(passages) = passages.filter(|p| p.len() > 1)
            && let Ok(scores) = client.rerank(query, &passages)
        {
            reorder(&mut hits[..head], scores);
            reranked = true;
        }
        // Recheck visibility and versions after the service round trip; do not
        // return cached citations for deleted, changed or newly revoked docs.
        let current = store.knowledge_search(actor, scope, query, limit.max(depth))?;
        hits.retain(|hit| {
            current.iter().any(|doc| {
                doc.namespace == hit.namespace
                    && doc.topic == hit.topic
                    && doc.version == hit.version
            })
        });
    }
    hits.truncate(limit);
    Ok(
        json!({"hits":hits, "retrieval":{"stages":if reranked { vec!["lexical", "rerank"] } else { vec!["lexical"] }}}),
    )
}

fn reorder(hits: &mut [SearchHit], scores: Vec<f32>) {
    let mut ranked: Vec<_> = scores.into_iter().enumerate().collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let ordered: Vec<_> = ranked.into_iter().map(|(i, _)| hits[i].clone()).collect();
    for (hit, replacement) in hits.iter_mut().zip(ordered) {
        *hit = replacement;
    }
}
