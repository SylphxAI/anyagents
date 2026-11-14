//! Reciprocal rank fusion: each ranked list contributes `1 / (k + rank)` to an
//! item, so items agreed on by several retrievers rise and no retriever's raw
//! score scale has to be calibrated against another's (Cormack, Clarke and
//! Büttcher, SIGIR 2009).

/// The conventional smoothing constant; it damps the weight of the very top
/// ranks so one retriever cannot dominate.
pub const K: f64 = 60.0;

/// Fuse ranked lists of keys. Ties keep the order of first appearance, so the
/// result is deterministic.
pub fn reciprocal_rank<T: Clone + Eq + std::hash::Hash>(lists: &[Vec<T>], k: f64) -> Vec<(T, f64)> {
    let mut scores: std::collections::HashMap<T, (f64, usize)> = std::collections::HashMap::new();
    let mut order = 0usize;
    for list in lists {
        for (rank, key) in list.iter().enumerate() {
            let entry = scores.entry(key.clone()).or_insert_with(|| {
                order += 1;
                (0.0, order)
            });
            entry.0 += 1.0 / (k + rank as f64 + 1.0);
        }
    }
    let mut fused: Vec<(T, f64, usize)> = scores
        .into_iter()
        .map(|(key, (score, first))| (key, score, first))
        .collect();
    fused.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.2.cmp(&b.2)));
    fused
        .into_iter()
        .map(|(key, score, _)| (key, score))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agreement_beats_a_single_top_rank_and_ties_are_stable() {
        let lexical = vec!["a", "b", "c"];
        let dense = vec!["c", "d", "b"];
        let fused: Vec<&str> = reciprocal_rank(&[lexical, dense], K)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        // b and c appear in both lists; c is ranked 3rd and 1st, b 2nd and 3rd.
        assert_eq!(fused, vec!["c", "b", "a", "d"]);
        let tie: Vec<&str> = reciprocal_rank(&[vec!["x"], vec!["y"]], K)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(tie, vec!["x", "y"]);
    }
}
