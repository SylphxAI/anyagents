//! Offline experiment, NOT shipping retrieval. Run with dense-benchmark feature.
//! Arguments: pinned-model-directory output-directory [labeled-population.json]
//! Population: {"labeling":"...","documents":[{"id":"...","text":"..."}],
//! "queries":[{"id":"...","text":"...","relevant":["document-id"]}]}
//! With no population this tests public synthetic inference, not retrieval recall.
use anyagents_core::{
    HistorySearchQuery,
    host_dense::{DIMENSIONS, E5Model, REVISION, cosine},
    host_index::{HostIndex, IndexMetadata, MemoryScope},
    learned_memory::sha256,
    raw_segment::RawCoordinate,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, time::Instant};
#[derive(Deserialize)]
struct Document {
    id: String,
    text: String,
}
#[derive(Deserialize)]
struct Query {
    id: String,
    text: String,
    relevant: Vec<String>,
}
#[derive(Deserialize)]
struct Population {
    labeling: String,
    documents: Vec<Document>,
    queries: Vec<Query>,
}
#[derive(Serialize, Deserialize)]
struct CandidateCache {
    identity: String,
    embeddings: Vec<Vec<f32>>,
}
fn recall(ranked: &[String], relevant: &[String], k: usize) -> f64 {
    if relevant.is_empty() {
        return 0.0;
    }
    ranked
        .iter()
        .take(k)
        .filter(|id| relevant.contains(id))
        .count() as f64
        / relevant.len() as f64
}
fn rss() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let models = PathBuf::from(args.get(1).ok_or("model directory required")?);
    let output = PathBuf::from(args.get(2).ok_or("output directory required")?);
    fs::create_dir_all(&output)?;
    let load = Instant::now();
    let model = E5Model::load(&models)?;
    println!(
        "{}",
        serde_json::json!({"phase":"load","ms":load.elapsed().as_secs_f64()*1000.0,"revision":REVISION,"rss_kib":rss()})
    );
    // Padding-sensitive public English/Chinese inputs. No claim of Transformers
    // parity: this proves finite, unit-normalized, batch-independent inference.
    let probe = ["how much protein should a female eat", "南瓜的家常做法"];
    let batch = model.embed(&probe, true)?;
    for (text, vector) in probe.iter().zip(&batch) {
        let single = model.embed(&[text], true)?.remove(0);
        let delta = vector
            .iter()
            .zip(&single)
            .map(|(a, b)| (a - b).abs())
            .fold(0.0f32, f32::max);
        assert_eq!(vector.len(), DIMENSIONS);
        assert!(vector.iter().all(|x| x.is_finite()));
        assert!((cosine(vector, vector).unwrap() - 1.0).abs() < 0.0001);
        assert!(delta < 0.0001, "padding changed embedding: {delta}");
    }
    let passages: Vec<String> = (0..32).map(|i|format!("Public synthetic candidate number {i}: a pumpkin recipe with vegetables, protein, and healthy cooking.")).collect();
    let start = Instant::now();
    let passage_refs: Vec<_> = passages.iter().map(String::as_str).collect();
    let content_sha = sha256(passages.join("\n").as_bytes());
    let identity =
        anyagents_core::host_dense::cache_key(&content_sha, "public-synthetic-benchmark-v1");
    let cache_path = output.join("public-candidates.json");
    let (cached, misses) = if cache_path.exists() {
        let cache: CandidateCache = serde_json::from_reader(fs::File::open(&cache_path)?)?;
        if cache.identity != identity
            || cache.embeddings.len() != 32
            || cache
                .embeddings
                .iter()
                .any(|v| cosine(v, v).is_none_or(|x| (x - 1.0).abs() > 0.0001))
        {
            return Err("candidate cache identity/vector mismatch".into());
        }
        (cache.embeddings, 0)
    } else {
        let embeddings = model.embed(&passage_refs, false)?;
        fs::write(
            &cache_path,
            serde_json::to_vec(&CandidateCache {
                identity,
                embeddings: embeddings.clone(),
            })?,
        )?;
        (embeddings, 32)
    };
    println!(
        "{}",
        serde_json::json!({"phase":"candidate-cache-32","ms":start.elapsed().as_secs_f64()*1000.0,"cache_misses":misses,"content_sha256":content_sha,"rss_kib":rss(),"population":"public synthetic; no recall claim"})
    );
    for round in 0..23 {
        let query = format!("How can I cook pumpkin with protein? variant {round}");
        let start = Instant::now();
        let q = model.embed(&[&query], true)?.remove(0);
        let scores: Vec<_> = cached.iter().map(|p| cosine(&q, p).unwrap()).collect();
        println!(
            "{}",
            serde_json::json!({"phase":if round<3 {"warmup"}else{"warm-query"},"round":round,"ms":start.elapsed().as_secs_f64()*1000.0,"candidates":scores.len(),"query_cache":false,"candidate_cache":true,"cold_total_ms":load.elapsed().as_secs_f64()*1000.0,"rss_kib":rss()})
        );
    }
    let Some(population) = args.get(3) else {
        return Ok(());
    };
    let data: Population = serde_json::from_reader(fs::File::open(population)?)?;
    let mut index = HostIndex::open(&output.join("bench-index.db"))?;
    let mut docs = HashMap::new();
    for (i, d) in data.documents.iter().enumerate() {
        let path = output.join(format!("doc-{i}.txt"));
        fs::write(&path, &d.text)?;
        index.index(
            &d.id,
            &IndexMetadata {
                agent_id: "benchmark".into(),
                source: MemoryScope::Transcripts,
                timestamp_ms: i as i64,
                direction: "in".into(),
                sender: "benchmark".into(),
                destination: "benchmark".into(),
                tool_name: None,
                role: None,
            },
            &RawCoordinate {
                segment: path,
                offset: 0,
                length: d.text.len() as u64,
                sha256: sha256(d.text.as_bytes()),
                seq: i as u64 + 1,
            },
        )?;
        docs.insert(d.id.clone(), d.text.as_str());
    }
    let mut vectors = HashMap::new();
    for q in data.queries {
        if q.relevant.is_empty() {
            return Err("relevance judgments required".into());
        }
        let start = Instant::now();
        let hits = index.search(
            &["benchmark".into()],
            MemoryScope::Transcripts,
            &HistorySearchQuery {
                query: Some(q.text.clone()),
                limit: 32,
                ..Default::default()
            },
        )?;
        let lexical_ms = start.elapsed().as_secs_f64() * 1000.0;
        let lexical: Vec<_> = hits.iter().map(|h| h.id.clone()).collect();
        let start = Instant::now();
        let embedding = model.embed(&[&q.text], true)?.remove(0);
        let mut scored = Vec::new();
        let mut misses = 0;
        for h in &hits {
            if !vectors.contains_key(&h.id) {
                vectors.insert(h.id.clone(), model.embed(&[docs[&h.id]], false)?.remove(0));
                misses += 1;
            }
            scored.push((h.id.clone(), cosine(&embedding, &vectors[&h.id]).unwrap()));
        }
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        let dense: Vec<_> = scored.into_iter().map(|(id, _)| id).collect();
        let mut hybrid: Vec<_> = lexical
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let j = dense.iter().position(|d| d == id).unwrap();
                (
                    id.clone(),
                    1.0 / (30.0 + i as f64) + 1.0 / (30.0 + j as f64),
                )
            })
            .collect();
        hybrid.sort_by(|a, b| b.1.total_cmp(&a.1));
        let hybrid: Vec<_> = hybrid.into_iter().map(|(id, _)| id).collect();
        println!(
            "{}",
            serde_json::json!({"phase":"labeled-retrieval","id":q.id,"labeling":data.labeling,"lexical_ms":lexical_ms,"extra_dense_ms":start.elapsed().as_secs_f64()*1000.0,"candidate_cache_misses":misses,"lexical_recall_at5":recall(&lexical,&q.relevant,5),"dense_recall_at5":recall(&dense,&q.relevant,5),"hybrid_recall_at5":recall(&hybrid,&q.relevant,5),"candidate_recall_at32":recall(&lexical,&q.relevant,32),"rss_kib":rss()})
        );
    }
    Ok(())
}
