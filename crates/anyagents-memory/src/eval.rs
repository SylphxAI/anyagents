//! Offline retrieval-quality evaluation on real history.
//!
//! [`build_sample`] copies a few agents' mailboxes out of a `state.db` opened
//! read-only and derives queries from the history itself: when a person
//! replies in Telegram to an earlier message, the reply text (without its
//! quote) is a later message that refers back to an earlier fact, and the
//! quoted message is the one it refers to. [`run`] indexes the sample with the
//! production host index (BM25) and, for every embedder of a [`Plan`], the
//! dense pipeline; then it scores BM25 alone, BM25 reranked by each reranker
//! (no embeddings at all), and per embedder dense alone, its reciprocal-rank
//! fusion with BM25, and that fusion reranked. Remote models are called
//! through AnyAgents profiles; the run refuses to start above its budget and
//! stops when what the providers report spent reaches it.
use crate::{
    dense::DenseIndex,
    embed::HostCatalog,
    fusion,
    providers::{Embedder, EmbedderConfig, Local, Reranker, RerankerConfig, Usage},
    runtime::Error,
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

/// What one evaluation compares, and what it may spend.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    #[serde(default = "default_embedders")]
    pub embedders: Vec<EmbedderConfig>,
    #[serde(default = "default_rerankers")]
    pub rerankers: Vec<RerankerConfig>,
    /// US$: the run refuses to start when its estimate is higher, and stops
    /// when the spend providers report reaches it.
    #[serde(default = "default_max_usd")]
    pub max_usd: f64,
    /// Characters of a passage a remote reranker reads.
    #[serde(default = "default_rerank_chars")]
    pub rerank_chars: usize,
}
fn default_embedders() -> Vec<EmbedderConfig> {
    vec![EmbedderConfig::default()]
}
fn default_rerankers() -> Vec<RerankerConfig> {
    vec![RerankerConfig::default()]
}
fn default_max_usd() -> f64 {
    5.0
}
fn default_rerank_chars() -> usize {
    1500
}
impl Default for Plan {
    fn default() -> Self {
        Self {
            embedders: default_embedders(),
            rerankers: default_rerankers(),
            max_usd: default_max_usd(),
            rerank_chars: default_rerank_chars(),
        }
    }
}

/// Spend shared by every remote call of a run.
struct Budget {
    max_usd: f64,
    spent: std::sync::Mutex<f64>,
}
impl Budget {
    fn charge(&self, usd: f64) -> Result<(), Error> {
        let mut spent = self.spent.lock().map_err(|_| "budget poisoned")?;
        *spent += usd;
        if *spent > self.max_usd {
            return Err(format!(
                "evaluation stopped: providers report US${:.4} spent, over its US${:.2} budget",
                *spent, self.max_usd
            )
            .into());
        }
        Ok(())
    }
    fn spent(&self) -> f64 {
        self.spent.lock().map_or(0.0, |s| *s)
    }
}

fn all_documents(host: &HostCatalog) -> Result<Vec<crate::embed::HostDocument>, Error> {
    let mut documents = Vec::new();
    let mut cursor = 0;
    loop {
        let batch = host.after(cursor, 4096, crate::embed::Stripe::ALL)?;
        let Some(last) = batch.last() else { break };
        cursor = last.rowid;
        documents.extend(batch);
    }
    Ok(documents)
}

/// Embed the whole sample into `dense_path` on `threads` workers. An index
/// left by an interrupted run is resumed: documents already embedded at
/// their digest are skipped.
fn embed_sample(
    host_path: &Path,
    dense_path: &Path,
    load: &(dyn Fn() -> Result<Embedder, Error> + Sync),
    threads: usize,
    budget: &Budget,
) -> Result<(BuildStats, Usage), Error> {
    let host = HostCatalog::open(host_path)?;
    let documents = all_documents(&host)?;
    let raw_bytes = documents.iter().map(|d| d.raw.length).sum();
    let queue = std::sync::Mutex::new(documents.chunks(64).collect::<Vec<_>>().into_iter());
    // Create the index and its space once, before the workers open it.
    let stamp = load()?.stamp();
    drop(DenseIndex::open(
        dense_path,
        &stamp.key(),
        stamp.dimensions,
    )?);
    let started = Instant::now();
    let totals = std::sync::Mutex::new((crate::embed::Embedded::default(), Usage::default()));
    std::thread::scope(|scope| -> Result<(), Error> {
        let workers: Vec<_> = (0..threads.max(1))
            .map(|_| {
                scope.spawn(|| -> Result<(), Error> {
                    let mut encoder = load()?;
                    let stamp = encoder.stamp();
                    let mut dense = DenseIndex::open(dense_path, &stamp.key(), stamp.dimensions)?;
                    loop {
                        let next = queue.lock().map_err(|_| "queue poisoned")?.next();
                        let Some(batch) = next else { break };
                        let before = encoder.usage();
                        let done =
                            crate::embed::embed_documents(&mut encoder, &mut dense, batch, "eval")?;
                        let spent = encoder.usage().usd - before.usd;
                        totals.lock().map_err(|_| "totals poisoned")?.0 += done;
                        budget.charge(spent)?;
                    }
                    totals.lock().map_err(|_| "totals poisoned")?.1 += encoder.usage();
                    Ok(())
                })
            })
            .collect();
        for worker in workers {
            worker.join().map_err(|_| "embedding worker panicked")??;
        }
        Ok(())
    })?;
    let (done, usage) = totals.into_inner().map_err(|_| "totals poisoned")?;
    Ok((
        BuildStats {
            documents: done.documents,
            pieces: done.pieces,
            reused_pieces: done.reused,
            tokens: done.tokens,
            seconds: started.elapsed().as_secs_f64(),
            raw_bytes,
            dense_bytes: fs::metadata(dense_path).map(|m| m.len()).unwrap_or(0),
            host_index_bytes: fs::metadata(host_path).map(|m| m.len()).unwrap_or(0),
            threads: threads.max(1),
            prepare_seconds: done.prepare_ms as f64 / 1e3,
            model_seconds: done.model_ms as f64 / 1e3,
        },
        usage,
    ))
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

/// One embedder of a run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EmbedderRun {
    pub name: String,
    pub space: String,
    /// Tokens of the sample's pieces, counted before any call.
    pub estimated_tokens: u64,
    pub build: BuildStats,
    /// What building and the queries cost, as providers reported it.
    pub usage: Usage,
}

/// One reranker of a run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RerankerRun {
    pub name: String,
    pub usage: Usage,
    /// Mean seconds to score one query's passages.
    pub mean_seconds: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub embedders: Vec<EmbedderRun>,
    pub rerankers: Vec<RerankerRun>,
    /// US$ the providers reported for the whole run.
    pub spent_usd: f64,
    /// method -> language bucket (`all`, `yue`, `zh`, `en`) -> metrics.
    /// Methods: `bm25`, `bm25+rerank[R]`, `dense[E]`, `hybrid[E]`,
    /// `hybrid[E]+rerank[R]`.
    pub methods: BTreeMap<String, BTreeMap<String, Metrics>>,
}

/// Candidates a method fuses or reranks.
const CANDIDATES: usize = 50;
/// Candidates the cross-encoder rescores, as the server does.
const RERANK_DEPTH: usize = 20;

/// A short name for a model in method names.
fn label(name: &str) -> String {
    name.rsplit('/').next().unwrap_or(name).to_owned()
}

/// Count what a run would embed with each embedder of `plan`, without any
/// remote call: the sample's pieces as each embedder cuts them. Remote
/// embedders are cut with their approximate counter, so no profile is read.
pub fn estimate(
    sample: &Path,
    config: &service::Config,
    plan: &Plan,
) -> Result<Vec<(String, u64)>, Error> {
    let work = sample.join("work");
    fs::create_dir_all(&work)?;
    let host_path = index_sample(sample, &work)?;
    let documents = all_documents(&HostCatalog::open(&host_path)?)?;
    let environment = plan_runtime(config, plan)?;
    let mut out = Vec::new();
    for embedder in &plan.embedders {
        let (name, counter) = match embedder {
            EmbedderConfig::OpenAi(remote) => {
                let mut offline = (**remote).clone();
                offline.dimensions = Some(offline.dimensions.unwrap_or(1));
                let counter = Embedder::remote(offline, crate::providers::Endpoint::offline())?;
                (remote.model.clone(), counter)
            }
            local => {
                let local_runtime = environment.map(|environment| Local {
                    environment,
                    models: &config.models,
                    threads: 1,
                });
                let loaded = Embedder::load(local, &config.data_dir, local_runtime.as_ref())?;
                (loaded.stamp().model, loaded)
            }
        };
        let mut tokens = 0u64;
        for document in &documents {
            if let Ok(raw) = read_raw(&document.raw) {
                tokens += counter
                    .chunks_of(&raw)?
                    .iter()
                    .map(|c| c.tokens as u64)
                    .sum::<u64>();
            }
        }
        out.push((name, tokens));
    }
    Ok(out)
}

/// The ONNX runtime, when the plan runs any local model.
fn plan_runtime(
    config: &service::Config,
    plan: &Plan,
) -> Result<Option<&'static ort::environment::Environment>, Error> {
    let mut pins = Vec::new();
    for embedder in &plan.embedders {
        pins.extend(crate::providers::local_embedder_pin(embedder)?);
    }
    for reranker in &plan.rerankers {
        pins.extend(crate::providers::local_reranker_pin(reranker)?);
    }
    if pins.is_empty() {
        return Ok(None);
    }
    crate::model::check(&config.models, &pins)?;
    let runtime = crate::model::runtime_pin().ok_or("no pinned ONNX Runtime for this platform")?;
    Ok(Some(crate::runtime::init(&config.models, &runtime)?))
}

/// Run `plan` on `sample`. Profiles are read from `config.data_dir`.
pub fn run(
    sample: &Path,
    config: &service::Config,
    plan: &Plan,
    max_queries: Option<usize>,
) -> Result<Report, Error> {
    let budget = Budget {
        max_usd: plan.max_usd,
        spent: std::sync::Mutex::new(0.0),
    };
    let environment = plan_runtime(config, plan)?;
    let local = |threads: usize| {
        environment.map(|environment| Local {
            environment,
            models: &config.models,
            threads,
        })
    };
    let work = sample.join("work");
    fs::create_dir_all(&work)?;
    let host_path = index_sample(sample, &work)?;
    let queries: Vec<Query> = fs::read_to_string(sample.join("queries.jsonl"))?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let queries: Vec<Query> = queries
        .into_iter()
        .take(max_queries.unwrap_or(usize::MAX))
        .collect();
    let estimates = estimate(sample, config, plan)?;

    // Build every embedder's index, then embed every query with it.
    struct Space {
        name: String,
        dense: DenseIndex,
        vectors: Vec<Vec<f32>>,
        query_ms: f64,
    }
    let mut report = Report::default();
    let mut spaces: Vec<Space> = Vec::new();
    for (embedder_config, (_, estimated_tokens)) in plan.embedders.iter().zip(&estimates) {
        let threads = match embedder_config {
            EmbedderConfig::Local { .. } => config.index_threads,
            EmbedderConfig::OpenAi(_) => 2,
        };
        let load = || Embedder::load(embedder_config, &config.data_dir, local(1).as_ref());
        let mut query_embedder = Embedder::load(
            embedder_config,
            &config.data_dir,
            local(config.query_threads).as_ref(),
        )?;
        let stamp = query_embedder.stamp();
        let dense_path = work.join(format!("dense-{}.db", stamp.digest()));
        let (build, mut usage) = embed_sample(&host_path, &dense_path, &load, threads, &budget)?;
        let started = Instant::now();
        let texts: Vec<&str> = queries.iter().map(|q| q.query.as_str()).collect();
        let mut vectors = Vec::with_capacity(texts.len());
        for batch in texts.chunks(64) {
            let before = query_embedder.usage().usd;
            vectors.extend(query_embedder.embed_queries(batch)?);
            budget.charge(query_embedder.usage().usd - before)?;
        }
        let query_ms = started.elapsed().as_secs_f64() * 1e3 / texts.len().max(1) as f64;
        usage += query_embedder.usage();
        // Two candidates of one model (another width or prefix) are two
        // spaces: never merge their tallies.
        let mut name = label(&stamp.model);
        if spaces.iter().any(|s| s.name == name) {
            name = format!("{name}~{}", &stamp.digest()[..6]);
        }
        report.embedders.push(EmbedderRun {
            name: stamp.model.clone(),
            space: stamp.key(),
            estimated_tokens: *estimated_tokens,
            build,
            usage,
        });
        spaces.push(Space {
            name,
            dense: DenseIndex::open(&dense_path, &stamp.key(), stamp.dimensions)?,
            vectors,
            query_ms,
        });
    }
    let mut rerankers: Vec<(String, Reranker, bool, f64)> = Vec::new();
    for reranker_config in &plan.rerankers {
        let reranker = Reranker::load(
            reranker_config,
            &config.data_dir,
            local(config.query_threads).as_ref(),
        )?;
        let remote = matches!(reranker_config, RerankerConfig::Rerank(_));
        let mut name = match reranker_config {
            RerankerConfig::Rerank(r) => label(&r.model),
            _ => label(&reranker.name()),
        };
        if rerankers.iter().any(|(n, ..)| *n == name) {
            name = format!("{name}#{}", rerankers.len() + 1);
        }
        rerankers.push((name, reranker, remote, 0.0));
    }

    let host = HostIndex::open(&host_path)?;
    let mut tallies: BTreeMap<(String, String), Tally> = BTreeMap::new();
    let mut tally = |method: String, query: &Query, ranked: &[String], ms: f64| {
        for bucket in ["all", query.language.as_str()] {
            tallies
                .entry((method.clone(), bucket.to_owned()))
                .or_default()
                .add(ranked, &query.relevant, ms);
        }
    };
    for (q, query) in queries.iter().enumerate() {
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
        let bm25_ids: Vec<String> = lexical.iter().map(|h| h.id.clone()).collect();
        // The passage a reranker reads is the cited raw range: the matching
        // window of a lexical hit, the matching piece of a dense hit.
        let mut passages: HashMap<String, String> = HashMap::new();
        for hit in &lexical {
            passages.insert(hit.id.clone(), text::readable(&read_raw(&hit.window)?));
        }
        let mut lists: Vec<(String, Vec<String>, f64)> = Vec::new();
        for space in &spaces {
            let started = Instant::now();
            let dense_hits = space.dense.search(
                &space.vectors[q],
                &owners,
                None,
                None,
                Some(query.until_ms),
                CANDIDATES,
            )?;
            let dense_ms = started.elapsed().as_secs_f64() * 1e3 + space.query_ms;
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
            let dense_ids: Vec<String> = dense_hits.iter().map(|h| h.document_id.clone()).collect();
            let hybrid: Vec<String> =
                fusion::reciprocal_rank(&[bm25_ids.clone(), dense_ids.clone()], fusion::K)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();
            tally(
                format!("dense[{}]", space.name),
                query,
                &dense_ids,
                dense_ms,
            );
            tally(
                format!("hybrid[{}]", space.name),
                query,
                &hybrid,
                bm25_ms + dense_ms,
            );
            lists.push((space.name.clone(), hybrid, bm25_ms + dense_ms));
        }
        tally("bm25".into(), query, &bm25_ids, bm25_ms);
        // A cross-encoder scores each (query, passage) pair on its own, so
        // every list's head is scored in one pass over their union.
        let mut union: Vec<String> = Vec::new();
        for id in bm25_ids.iter().take(RERANK_DEPTH).chain(
            lists
                .iter()
                .flat_map(|(_, l, _)| l.iter().take(RERANK_DEPTH)),
        ) {
            if !union.contains(id) {
                union.push(id.clone());
            }
        }
        for (name, reranker, remote, seconds) in &mut rerankers {
            let texts: Vec<String> = union
                .iter()
                .map(|id| {
                    let text = passages.get(id).map_or("", String::as_str);
                    if *remote {
                        text.chars().take(plan.rerank_chars).collect()
                    } else {
                        text.to_owned()
                    }
                })
                .collect();
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            let started = Instant::now();
            let before = reranker.usage().usd;
            let scores = reranker.score(&query.query, &refs)?;
            budget.charge(reranker.usage().usd - before)?;
            let rerank_ms = started.elapsed().as_secs_f64() * 1e3;
            *seconds += rerank_ms / 1e3;
            let score_of: HashMap<&String, f32> = union.iter().zip(scores).collect();
            let rerank = |ids: &[String]| -> Vec<String> {
                let mut head: Vec<(usize, &String)> =
                    ids.iter().take(RERANK_DEPTH).enumerate().collect();
                head.sort_by(|a, b| {
                    let sa = score_of.get(a.1).copied().unwrap_or(f32::MIN);
                    let sb = score_of.get(b.1).copied().unwrap_or(f32::MIN);
                    sb.total_cmp(&sa).then(a.0.cmp(&b.0))
                });
                let mut ranked: Vec<String> = head.into_iter().map(|(_, id)| id.clone()).collect();
                ranked.extend(ids.iter().skip(RERANK_DEPTH).cloned());
                ranked
            };
            tally(
                format!("bm25+rerank[{name}]"),
                query,
                &rerank(&bm25_ids),
                bm25_ms + rerank_ms,
            );
            for (space, hybrid, ms) in &lists {
                tally(
                    format!("hybrid[{space}]+rerank[{name}]"),
                    query,
                    &rerank(hybrid),
                    ms + rerank_ms,
                );
            }
        }
    }
    for (name, reranker, _, seconds) in &rerankers {
        report.rerankers.push(RerankerRun {
            name: name.clone(),
            usage: reranker.usage(),
            mean_seconds: seconds / queries.len().max(1) as f64,
        });
    }
    report.spent_usd = budget.spent();
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
