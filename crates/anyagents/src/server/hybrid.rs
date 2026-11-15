//! Hybrid retrieval for the `conversation` route: BM25 from the host index,
//! dense candidates from the memory service fused by reciprocal rank, and the
//! fused list reordered by the service's cross-encoder. The kernel keeps
//! every authorization, verification and raw read; the service only proposes
//! cited ranges and scores passages. When it does not answer, the route ranks
//! lexically and says which stage did not run.
use anyagents_core::{
    HistorySearchQuery, Store,
    host_index::{IndexHit, MemoryScope},
    host_retrieval::CitedRange,
};
use anyagents_memory::{fusion, protocol::Client};
use std::{path::Path, time::Duration};

use crate::api::{ConversationTurnView, RetrievalView};

/// Dense candidates a search asks for, and fused candidates it reranks.
const DENSE_CANDIDATES: usize = 50;
pub(crate) const RERANK_DEPTH: usize = 20;
/// A slow or stuck service must not hold a search: past these bounds the
/// route answers without it and reports the stage it skipped.
const SEARCH_DEADLINE: Duration = Duration::from_secs(3);
const RERANK_DEADLINE: Duration = Duration::from_secs(8);

pub(crate) struct Hybrid {
    pub hits: Vec<(IndexHit, Vec<u8>)>,
    pub retrieval: RetrievalView,
}

fn source(scope: MemoryScope) -> Option<&'static str> {
    match scope {
        MemoryScope::All => None,
        MemoryScope::Memory => Some("memory"),
        MemoryScope::Transcripts => Some("transcripts"),
    }
}

/// Fuse lexical hits with the service's dense candidates over the same
/// authorized documents. A document found by both keeps its lexical window,
/// which holds the query's terms.
pub(crate) fn search(
    store: &Store,
    query_text: &str,
    owners: &[String],
    scope: MemoryScope,
    request: &HistorySearchQuery,
    lexical: Vec<(IndexHit, Vec<u8>)>,
) -> Hybrid {
    let mut retrieval = RetrievalView {
        mode: "lexical".into(),
        stages: vec!["lexical".into()],
        ..RetrievalView::default()
    };
    // No service is the default, not a fault: the mode says it once.
    let Some(client) = Client::for_data_dir(store.data_dir(), SEARCH_DEADLINE) else {
        return Hybrid {
            hits: lexical,
            retrieval,
        };
    };
    retrieval.mode = "hybrid".into();
    let candidates = match client.search(
        query_text,
        owners,
        source(scope),
        request.since_ms,
        request.until_ms,
        DENSE_CANDIDATES,
    ) {
        Ok((hits, _)) => hits,
        Err(error) => {
            retrieval.degraded.push(format!("dense: {error}"));
            return Hybrid {
                hits: lexical,
                retrieval,
            };
        }
    };
    let cited: Vec<CitedRange> = candidates
        .into_iter()
        .map(|hit| CitedRange {
            document_id: hit.document_id,
            offset: hit.offset,
            length: hit.length,
            sha256: hit.sha256,
        })
        .collect();
    let dense = match store.resolve_host_ranges(owners, scope, request, &cited) {
        Ok(dense) => dense,
        Err(error) => {
            retrieval.degraded.push(format!("dense: {error}"));
            return Hybrid {
                hits: lexical,
                retrieval,
            };
        }
    };
    retrieval.stages.push("dense".into());
    Hybrid {
        hits: fuse(lexical, dense),
        retrieval,
    }
}

/// Reciprocal-rank fusion of two verified hit lists.
pub(crate) fn fuse(
    lexical: Vec<(IndexHit, Vec<u8>)>,
    dense: Vec<(IndexHit, Vec<u8>)>,
) -> Vec<(IndexHit, Vec<u8>)> {
    let order = fusion::reciprocal_rank(
        &[
            lexical.iter().map(|(h, _)| h.id.clone()).collect(),
            dense.iter().map(|(h, _)| h.id.clone()).collect(),
        ],
        fusion::K,
    );
    let mut by_id: std::collections::HashMap<String, (IndexHit, Vec<u8>)> =
        std::collections::HashMap::new();
    for (hit, bytes) in dense.into_iter().chain(lexical) {
        // Lexical entries come last and replace a dense entry for the same id.
        by_id.insert(hit.id.clone(), (hit, bytes));
    }
    order
        .into_iter()
        .filter_map(|(id, _)| by_id.remove(&id))
        .collect()
}

/// Reorder the head of a ranked list by the cross-encoder's score of each
/// turn's cited text. The tail keeps its order.
pub(crate) fn rerank(
    data_dir: &Path,
    query_text: &str,
    ranked: &mut [(f64, ConversationTurnView)],
    retrieval: &mut RetrievalView,
) {
    let Some(client) = Client::for_data_dir(data_dir, RERANK_DEADLINE) else {
        return;
    };
    let depth = ranked.len().min(RERANK_DEPTH);
    if depth < 2 {
        return;
    }
    let passages: Vec<String> = ranked[..depth]
        .iter()
        .map(|(_, t)| t.text.clone())
        .collect();
    match client.rerank(query_text, &passages) {
        Ok(scores) => {
            let mut head: Vec<(f32, usize)> = scores.into_iter().zip(0..).collect();
            head.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            let reordered: Vec<(f64, ConversationTurnView)> =
                head.iter().map(|&(_, i)| ranked[i].clone()).collect();
            for (slot, turn) in ranked.iter_mut().zip(reordered) {
                *slot = turn;
            }
            retrieval.stages.push("rerank".into());
        }
        Err(error) => retrieval.degraded.push(format!("rerank: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{host_index::IndexMetadata, raw_segment::RawCoordinate};

    fn hit(id: &str, window: u64) -> (IndexHit, Vec<u8>) {
        let raw = RawCoordinate {
            segment: "/s".into(),
            offset: 0,
            length: 100,
            sha256: "d".into(),
            seq: 1,
        };
        (
            IndexHit {
                id: id.into(),
                metadata: IndexMetadata {
                    agent_id: "a".into(),
                    source: MemoryScope::Transcripts,
                    timestamp_ms: None,
                    direction: "in".into(),
                    sender: "s".into(),
                    destination: "d".into(),
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: None,
                },
                window: RawCoordinate {
                    offset: window,
                    ..raw.clone()
                },
                raw,
                lexical_score: 0.0,
            },
            id.as_bytes().to_vec(),
        )
    }

    #[test]
    fn fusion_keeps_the_lexical_window_and_ranks_agreement_first() {
        let fused = fuse(
            vec![hit("a", 1), hit("b", 1)],
            vec![hit("b", 2), hit("c", 2)],
        );
        let ids: Vec<(&str, u64)> = fused
            .iter()
            .map(|(h, _)| (h.id.as_str(), h.window.offset))
            .collect();
        assert_eq!(ids, vec![("b", 1), ("a", 1), ("c", 2)]);
    }

    /// With no memory service (nothing configured), search is lexical by
    /// design: the mode says so once, and nothing is reported degraded.
    #[test]
    fn no_service_is_lexical_mode_not_a_degradation() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let hybrid = search(
            &store,
            "q",
            &["a".into()],
            MemoryScope::All,
            &HistorySearchQuery::default(),
            vec![hit("a", 1)],
        );
        assert_eq!(hybrid.hits.len(), 1);
        assert_eq!(hybrid.retrieval.mode, "lexical");
        assert_eq!(hybrid.retrieval.stages, vec!["lexical".to_string()]);
        assert!(hybrid.retrieval.degraded.is_empty());
        let mut ranked = vec![];
        let mut retrieval = hybrid.retrieval;
        rerank(dir.path(), "q", &mut ranked, &mut retrieval);
        assert!(retrieval.degraded.is_empty());
        assert_eq!(retrieval.mode, "lexical");
        // A service that is configured but does not answer is a degradation.
        std::fs::write(
            anyagents_memory::protocol::socket_path(dir.path()),
            b"not a socket",
        )
        .unwrap();
        let down = search(
            &store,
            "q",
            &["a".into()],
            MemoryScope::All,
            &HistorySearchQuery::default(),
            vec![hit("a", 1)],
        );
        assert_eq!(down.retrieval.mode, "hybrid");
        assert!(
            down.retrieval.degraded[0].starts_with("dense:"),
            "{:?}",
            down.retrieval
        );
    }
}
