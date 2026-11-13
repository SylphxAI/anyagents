//! Offline retrieval-quality evaluation on real history.
//!
//! [`build_sample`] copies a few agents' mailboxes out of a `state.db` opened
//! read-only and derives queries from the history itself: when a person
//! replies in Telegram to an earlier message, the reply text (without its
//! quote) is a later message that refers back to an earlier fact, and the
//! quoted message is the one it refers to. [`run`] indexes the sample with the
//! production host index (BM25) and the dense pipeline, then scores BM25 alone,
//! dense alone, their reciprocal-rank fusion, and the fusion reranked by the
//! cross-encoder (plus BM25 reranked, to separate the two effects).
use crate::{
    dense::DenseIndex,
    embed::HostCatalog,
    fusion,
    runtime::{Encoder, Error},
    service, text,
};
use anyagents_core::{
    HistorySearchQuery,
    host_index::{HostIndex, IndexMetadata, MemoryScope},
    raw_segment::{RawCoordinate, read_raw, retained_jsonl_records},
};
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

/// One evaluation query and the documents that answer it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    pub id: String,
    pub agent: String,
    pub query: String,
    /// Only history before the query is searched.
    pub until_ms: i64,
    pub relevant: Vec<String>,
    pub language: String,
}

const CANTONESE: &[char] = &[
    '嘅', '咗', '唔', '喺', '冇', '啲', '嚟', '咁', '乜', '佢', '哋', '嗰', '咩', '睇', '畀', '俾',
];

fn language(text: &str) -> &'static str {
    if text.chars().any(|c| CANTONESE.contains(&c)) {
        "yue"
    } else if text.chars().any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c)) {
        "zh"
    } else {
        "en"
    }
}

/// Text compared for quote matching: no markup, no whitespace, lowercase.
fn normalize(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_whitespace() && !"*_`#>|-".contains(*c))
        .flat_map(char::to_lowercase)
        .collect()
}

/// The quoted text of a reply body, without its author line and quotes.
fn quoted(body: &str) -> Option<String> {
    let (_, tail) = body.split_once(anyagents_core::quote::QUOTE_FENCE)?;
    let tail = tail.split("----- end of quoted message -----").next()?;
    let mut lines = tail.trim().lines();
    let author = lines.next()?;
    if !author.contains("replied to") {
        return None;
    }
    let rest: Vec<&str> = lines.collect();
    let text = rest.join("\n");
    Some(text.trim().trim_matches('"').trim().to_owned())
}

#[derive(Serialize)]
struct Line<'a> {
    id: &'a str,
    timestamp_ms: i64,
    direction: &'a str,
    sender: &'a str,
    destination: &'a str,
    role: &'a str,
    content: &'a str,
}

/// What [`build_sample`] wrote.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SampleStats {
    pub agents: usize,
    pub messages: usize,
    pub queries: usize,
    pub unmatched_quotes: usize,
}

/// Copy the mailboxes of `agents` since `since_ms` into `out` and derive the
/// query set. `state_db` is opened read-only; nothing in it is changed.
pub fn build_sample(
    state_db: &Path,
    agents: &[String],
    since_ms: i64,
    out: &Path,
) -> Result<SampleStats, Error> {
    let uri = format!("file:{}?mode=ro", state_db.display());
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut stats = SampleStats::default();
    let mut queries = Vec::new();
    for agent in agents {
        struct Row {
            id: String,
            ts: i64,
            direction: &'static str,
            sender: String,
            destination: String,
            body: String,
        }
        let mut rows: Vec<Row> = Vec::new();
        let mut inbound = conn.prepare(
            "SELECT id, received_at_ms, COALESCE(sender,''), body FROM messages
             WHERE agent_id=?1 AND received_at_ms>=?2 ORDER BY seq",
        )?;
        for row in inbound.query_map(params![agent, since_ms], |r| {
            Ok(Row {
                id: r.get(0)?,
                ts: r.get(1)?,
                direction: "in",
                sender: r.get(2)?,
                destination: format!("agent:{agent}"),
                body: r.get(3)?,
            })
        })? {
            rows.push(row?);
        }
        let mut outbound = conn.prepare(
            "SELECT id, sent_at_ms, destination, body FROM outbound_messages
             WHERE agent_id=?1 AND sent_at_ms>=?2 ORDER BY sent_at_ms",
        )?;
        for row in outbound.query_map(params![agent, since_ms], |r| {
            Ok(Row {
                id: r.get(0)?,
                ts: r.get(1)?,
                direction: "out",
                sender: format!("agent:{agent}"),
                destination: r.get(2)?,
                body: r.get(3)?,
            })
        })? {
            rows.push(row?);
        }
        rows.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.id.cmp(&b.id)));
        let dir = out.join("agents").join(agent).join("transcripts");
        fs::create_dir_all(&dir)?;
        let mut file = std::io::BufWriter::new(fs::File::create(dir.join("mailbox.jsonl"))?);
        for row in &rows {
            serde_json::to_writer(
                &mut file,
                &Line {
                    id: &row.id,
                    timestamp_ms: row.ts,
                    direction: row.direction,
                    sender: &row.sender,
                    destination: &row.destination,
                    role: if row.direction == "in" {
                        "user"
                    } else {
                        "assistant"
                    },
                    content: &row.body,
                },
            )?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        stats.agents += 1;
        stats.messages += rows.len();
        // Heads only: an earlier reply that quoted the same message does not
        // count as the message itself.
        let heads: Vec<String> = rows
            .iter()
            .map(|r| normalize(anyagents_core::quote::strip_quoted_tail(&r.body)))
            .collect();
        for (i, row) in rows.iter().enumerate() {
            if row.direction != "in" || !row.sender.starts_with("telegram:user:") {
                continue;
            }
            let Some(quote) = quoted(&row.body) else {
                continue;
            };
            let query = anyagents_core::quote::strip_quoted_tail(&row.body)
                .trim()
                .to_owned();
            if query.chars().count() < 4 {
                continue;
            }
            let needle: String = normalize(&quote).chars().take(80).collect();
            if needle.chars().count() < 12 {
                continue;
            }
            let relevant: Vec<String> = (0..i)
                .filter(|&j| rows[j].ts < row.ts && heads[j].contains(&needle))
                .map(|j| format!("eval:{agent}:{}", rows[j].id))
                .collect();
            if relevant.is_empty() {
                stats.unmatched_quotes += 1;
                continue;
            }
            queries.push(Query {
                id: row.id.clone(),
                agent: agent.clone(),
                language: language(&query).into(),
                query,
                until_ms: row.ts - 1,
                relevant,
            });
        }
    }
    let mut file = std::io::BufWriter::new(fs::File::create(out.join("queries.jsonl"))?);
    for query in &queries {
        serde_json::to_writer(&mut file, query)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    stats.queries = queries.len();
    Ok(stats)
}

#[derive(Deserialize)]
struct Event {
    id: String,
    timestamp_ms: i64,
    direction: String,
    sender: String,
    destination: String,
    role: String,
}

/// Index every sample mailbox with the production host index. Document ids
/// carry the message id, so relevance labels name documents directly.
fn index_sample(sample: &Path, work: &Path) -> Result<PathBuf, Error> {
    let path = work.join("host-index.db");
    if path.exists() {
        return Ok(path);
    }
    let building = work.join("host-index.building.db");
    let _ = fs::remove_file(&building);
    let mut index = HostIndex::open(&building)?;
    for entry in fs::read_dir(sample.join("agents"))? {
        let agent_dir = entry?.path();
        let agent = agent_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned();
        let transcripts = agent_dir.join("transcripts");
        for file in fs::read_dir(&transcripts)? {
            let file = file?.path();
            if file.extension().is_none_or(|e| e != "jsonl") {
                continue;
            }
            for coordinate in retained_jsonl_records(&file)? {
                let event: Event = serde_json::from_slice(&read_raw(&coordinate)?)?;
                let metadata = IndexMetadata {
                    agent_id: agent.clone(),
                    source: MemoryScope::Transcripts,
                    timestamp_ms: Some(event.timestamp_ms),
                    direction: event.direction,
                    sender: event.sender,
                    destination: event.destination,
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: Some(event.role),
                };
                index.index(
                    &format!("eval:{agent}:{}", event.id),
                    &metadata,
                    &coordinate,
                )?;
            }
        }
    }
    drop(index);
    fs::rename(&building, &path)?;
    Ok(path)
}

/// Throughput of the dense build, for the rollout estimate.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BuildStats {
    pub documents: usize,
    pub pieces: usize,
    pub reused_pieces: usize,
    pub tokens: usize,
    pub seconds: f64,
    pub raw_bytes: u64,
    pub dense_bytes: u64,
    pub host_index_bytes: u64,
    pub threads: usize,
    /// Summed over workers: reading and cutting text, and model time.
    pub prepare_seconds: f64,
    pub model_seconds: f64,
}

/// Embed the whole sample on `threads` workers, each with its own
/// single-threaded session: small models use cores best one batch per core.
fn embed_sample(
    host_path: &Path,
    dense_path: &Path,
    config: &service::Config,
    environment: &'static ort::environment::Environment,
) -> Result<BuildStats, Error> {
    let host = HostCatalog::open(host_path)?;
    let mut documents = Vec::new();
    let mut cursor = 0;
    loop {
        let batch = host.after(cursor, 4096)?;
        let Some(last) = batch.last() else { break };
        cursor = last.rowid;
        documents.extend(batch);
    }
    let raw_bytes = documents.iter().map(|d| d.raw.length).sum();
    // Create the index (and its model identity) once before the workers.
    drop(DenseIndex::open(
        dense_path,
        config.embedder.name,
        config.embedder.dims,
    )?);
    let queue = std::sync::Mutex::new(documents.chunks(64).collect::<Vec<_>>().into_iter());
    let started = Instant::now();
    let totals = std::sync::Mutex::new(crate::embed::Embedded::default());
    std::thread::scope(|scope| -> Result<(), Error> {
        let workers: Vec<_> = (0..config.index_threads.max(1))
            .map(|_| {
                scope.spawn(|| -> Result<(), Error> {
                    let mut encoder =
                        Encoder::load(environment, &config.models, &config.embedder, 1)?;
                    let mut dense =
                        DenseIndex::open(dense_path, config.embedder.name, config.embedder.dims)?;
                    loop {
                        let next = queue.lock().map_err(|_| "queue poisoned")?.next();
                        let Some(batch) = next else { return Ok(()) };
                        let done =
                            crate::embed::embed_documents(&mut encoder, &mut dense, batch, "eval")?;
                        *totals.lock().map_err(|_| "totals poisoned")? += done;
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().map_err(|_| "embedding worker panicked")??;
        }
        Ok(())
    })?;
    let done = totals.into_inner().map_err(|_| "totals poisoned")?;
    Ok(BuildStats {
        documents: done.documents,
        pieces: done.pieces,
        reused_pieces: done.reused,
        tokens: done.tokens,
        seconds: started.elapsed().as_secs_f64(),
        raw_bytes,
        dense_bytes: fs::metadata(dense_path).map(|m| m.len()).unwrap_or(0),
        host_index_bytes: fs::metadata(host_path).map(|m| m.len()).unwrap_or(0),
        threads: config.index_threads.max(1),
        prepare_seconds: done.prepare_ms as f64 / 1e3,
        model_seconds: done.model_ms as f64 / 1e3,
    })
}

/// Rank metrics of one method.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Metrics {
    pub queries: usize,
    pub recall_at_1: f64,
    pub recall_at_5: f64,
    pub recall_at_10: f64,
    pub recall_at_20: f64,
    pub mrr_at_10: f64,
    pub ndcg_at_10: f64,
    pub mean_ms: f64,
}

#[derive(Default)]
struct Tally {
    n: usize,
    r1: f64,
    r5: f64,
    r10: f64,
    r20: f64,
    mrr: f64,
    ndcg: f64,
    ms: f64,
}

impl Tally {
    fn add(&mut self, ranked: &[String], relevant: &[String], ms: f64) {
        let first = ranked.iter().position(|id| relevant.contains(id));
        let hit = |k: usize| f64::from(first.is_some_and(|p| p < k));
        self.n += 1;
        self.r1 += hit(1);
        self.r5 += hit(5);
        self.r10 += hit(10);
        self.r20 += hit(20);
        self.mrr += first
            .filter(|&p| p < 10)
            .map_or(0.0, |p| 1.0 / (p as f64 + 1.0));
        let dcg: f64 = ranked
            .iter()
            .take(10)
            .enumerate()
            .filter(|(_, id)| relevant.contains(id))
            .map(|(i, _)| 1.0 / (i as f64 + 2.0).log2())
            .sum();
        let ideal: f64 = (0..relevant.len().min(10))
            .map(|i| 1.0 / (i as f64 + 2.0).log2())
            .sum();
        self.ndcg += if ideal > 0.0 { dcg / ideal } else { 0.0 };
        self.ms += ms;
    }
    fn metrics(&self) -> Metrics {
        let n = self.n.max(1) as f64;
        Metrics {
            queries: self.n,
            recall_at_1: self.r1 / n,
            recall_at_5: self.r5 / n,
            recall_at_10: self.r10 / n,
            recall_at_20: self.r20 / n,
            mrr_at_10: self.mrr / n,
            ndcg_at_10: self.ndcg / n,
            mean_ms: self.ms / n,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub embedder: String,
    pub reranker: String,
    pub build: BuildStats,
    /// method -> language bucket (`all`, `yue`, `zh`, `en`) -> metrics.
    pub methods: BTreeMap<String, BTreeMap<String, Metrics>>,
}

/// Candidates a method fuses or reranks.
const CANDIDATES: usize = 50;
/// Candidates the cross-encoder rescores, as the server does.
const RERANK_DEPTH: usize = 20;

pub fn run(
    sample: &Path,
    config: &service::Config,
    max_queries: Option<usize>,
) -> Result<Report, Error> {
    let [embed_pin, rerank_pin] = config.pins();
    crate::model::check(&config.models, &[embed_pin, rerank_pin])?;
    let runtime = crate::model::runtime_pin().ok_or("no pinned ONNX Runtime for this platform")?;
    let environment = crate::runtime::init(&config.models, &runtime)?;
    let mut embedder = Encoder::load(
        environment,
        &config.models,
        &embed_pin,
        config.index_threads,
    )?;
    let mut reranker = Encoder::load(
        environment,
        &config.models,
        &rerank_pin,
        config.query_threads,
    )?;
    let work = sample.join("work");
    fs::create_dir_all(&work)?;
    let host_path = index_sample(sample, &work)?;
    let dense_path = work.join(format!("dense-{}.db", embed_pin.name));
    let build = embed_sample(&host_path, &dense_path, config, environment)?;
    let host = HostIndex::open(&host_path)?;
    let dense = DenseIndex::open(&dense_path, embed_pin.name, embed_pin.dims)?;

    let queries: Vec<Query> = fs::read_to_string(sample.join("queries.jsonl"))?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let mut tallies: BTreeMap<(String, String), Tally> = BTreeMap::new();
    for query in queries.iter().take(max_queries.unwrap_or(usize::MAX)) {
        let owners = [query.agent.clone()];
        let started = Instant::now();
        let lexical = host.search(
            &owners,
            MemoryScope::Transcripts,
            &HistorySearchQuery {
                query: Some(query.query.clone()),
                until_ms: Some(query.until_ms),
                limit: CANDIDATES,
                ..HistorySearchQuery::default()
            },
        )?;
        let bm25_ms = started.elapsed().as_secs_f64() * 1e3;
        let started = Instant::now();
        let vector = embedder.embed_queries(&[query.query.as_str()])?.remove(0);
        let dense_hits = dense.search(
            &vector,
            &owners,
            None,
            None,
            Some(query.until_ms),
            CANDIDATES,
        )?;
        let dense_ms = started.elapsed().as_secs_f64() * 1e3;

        let bm25_ids: Vec<String> = lexical.iter().map(|h| h.id.clone()).collect();
        let dense_ids: Vec<String> = dense_hits.iter().map(|h| h.document_id.clone()).collect();
        let hybrid: Vec<String> =
            fusion::reciprocal_rank(&[bm25_ids.clone(), dense_ids.clone()], fusion::K)
                .into_iter()
                .map(|(id, _)| id)
                .collect();

        // The passage a reranker reads is the cited raw range: the matching
        // window of a lexical hit, the matching piece of a dense hit.
        let mut passages: HashMap<String, String> = HashMap::new();
        for hit in &lexical {
            passages.insert(hit.id.clone(), text::readable(&read_raw(&hit.window)?));
        }
        for hit in &dense_hits {
            if passages.contains_key(&hit.document_id) {
                continue;
            }
            let Some(document) =
                host.document(&owners, MemoryScope::Transcripts, &hit.document_id)?
            else {
                continue;
            };
            let piece = RawCoordinate {
                segment: document.raw.segment.clone(),
                offset: hit.offset,
                length: hit.length,
                sha256: hit.sha256.clone(),
                seq: document.raw.seq,
            };
            passages.insert(hit.document_id.clone(), text::readable(&read_raw(&piece)?));
        }
        let mut rerank = |ids: &[String]| -> Result<(Vec<String>, f64), Error> {
            let started = Instant::now();
            let head: Vec<&String> = ids.iter().take(RERANK_DEPTH).collect();
            let texts: Vec<&str> = head
                .iter()
                .map(|id| passages.get(*id).map_or("", String::as_str))
                .collect();
            let mut scores = Vec::with_capacity(texts.len());
            for batch in texts.chunks(8) {
                scores.extend(reranker.score(&query.query, batch)?);
            }
            let mut order: Vec<(f32, usize)> = scores.into_iter().zip(0..).collect();
            order.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut ranked: Vec<String> = order.into_iter().map(|(_, i)| head[i].clone()).collect();
            ranked.extend(ids.iter().skip(RERANK_DEPTH).cloned());
            Ok((ranked, started.elapsed().as_secs_f64() * 1e3))
        };
        let (hybrid_reranked, hybrid_rerank_ms) = rerank(&hybrid)?;
        let (bm25_reranked, bm25_rerank_ms) = rerank(&bm25_ids)?;

        for (method, ranked, ms) in [
            ("bm25", &bm25_ids, bm25_ms),
            ("dense", &dense_ids, dense_ms),
            ("hybrid", &hybrid, bm25_ms + dense_ms),
            ("bm25+rerank", &bm25_reranked, bm25_ms + bm25_rerank_ms),
            (
                "hybrid+rerank",
                &hybrid_reranked,
                bm25_ms + dense_ms + hybrid_rerank_ms,
            ),
        ] {
            for bucket in ["all", query.language.as_str()] {
                tallies
                    .entry((method.to_owned(), bucket.to_owned()))
                    .or_default()
                    .add(ranked, &query.relevant, ms);
            }
        }
    }
    let mut report = Report {
        embedder: embed_pin.name.into(),
        reranker: rerank_pin.name.into(),
        build,
        methods: BTreeMap::new(),
    };
    for ((method, bucket), tally) in tallies {
        report
            .methods
            .entry(method)
            .or_default()
            .insert(bucket, tally.metrics());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_are_read_and_matched_on_normalised_text() {
        let body = "點解又要再註冊多次嘅？\n\n----- quoted message -----\nThe agent replied to Apollo (@bot):\n\"Google 要一次性收 **US$5** 開發者註冊費\"\n----- end of quoted message -----";
        assert_eq!(
            quoted(body).unwrap(),
            "Google 要一次性收 **US$5** 開發者註冊費"
        );
        assert_eq!(
            anyagents_core::quote::strip_quoted_tail(body),
            "點解又要再註冊多次嘅？"
        );
        assert!(
            normalize("Google 要一次性收 **US$5** 開發者")
                .contains(&normalize("google要一次性收US$5"))
        );
        assert_eq!(language("點解又要再註冊多次嘅？"), "yue");
        assert_eq!(language("已全部補發入庫"), "zh");
        assert_eq!(language("ship it"), "en");
        assert!(quoted("no quote here").is_none());
    }

    #[test]
    fn metrics_count_the_first_relevant_rank() {
        let mut tally = Tally::default();
        let ranked: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        tally.add(&ranked, &["b".into()], 2.0);
        tally.add(&ranked, &["z".into()], 4.0);
        let m = tally.metrics();
        assert_eq!(m.queries, 2);
        assert!((m.recall_at_1 - 0.0).abs() < 1e-9);
        assert!((m.recall_at_5 - 0.5).abs() < 1e-9);
        assert!((m.mrr_at_10 - 0.25).abs() < 1e-9);
        assert!((m.ndcg_at_10 - 0.5 / 3f64.log2()).abs() < 1e-9);
        assert!((m.mean_ms - 3.0).abs() < 1e-9);
    }
}
