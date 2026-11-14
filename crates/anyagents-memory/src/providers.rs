//! Where vectors and rerank scores come from: the pinned local ONNX models
//! (the default, offline), an OpenAI-compatible `/embeddings` endpoint, or a
//! `/rerank` endpoint in the Cohere, Voyage, Jina and OpenRouter shape. A
//! remote provider is bound to one AnyAgents profile
//! (`<data>/profiles/<provider>/<profile>`), whose API key it reads, and to a
//! model id; nothing else chooses it.
//!
//! Every embedder names what its vectors are: provider, model, dimension and
//! how text was cut into pieces ([`Stamp`]). Vectors with another stamp live
//! in another space and in another dense index file.
use crate::{
    embed::{Chunk, MAX_PIECES, chunk},
    model::ModelPin,
    runtime::{Encoder, Error},
    text::project,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Tokens kept free in a piece for special tokens and a passage prefix.
const RESERVED_TOKENS: usize = 16;
/// Approximate tokens per piece for a remote embedder, unless configured.
const REMOTE_PIECE_TOKENS: usize = 512;
/// Inputs per remote embedding request.
const REMOTE_BATCH: usize = 64;
/// Characters of each passage a remote reranker reads, unless configured:
/// what the desk's models were measured with (2026-11-14), and a bound on
/// what one search costs.
pub const REMOTE_RERANK_CHARS: usize = 1000;
/// Attempts for one remote request that is throttled or fails upstream, and
/// the longest wait between two of them.
const ATTEMPTS: u32 = 6;
const MAX_WAIT: Duration = Duration::from_secs(60);

/// A remote model bound to a profile.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Remote {
    /// `<provider>/<profile>`, a directory under `<data>/profiles`.
    pub profile: String,
    /// The model id the endpoint expects.
    pub model: String,
    /// The API root (`…/v1`); the provider's documented root by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Embedder: ask for vectors of this width (Matryoshka truncation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<usize>,
    /// Embedder: approximate tokens per embedded piece.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub piece_tokens: Option<usize>,
    /// Price for estimates when the endpoint does not report cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_per_million_tokens: Option<f64>,
    /// Reranker: price per search, for providers that bill searches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_per_search: Option<f64>,
    /// Reranker: characters of each passage sent
    /// ([`REMOTE_RERANK_CHARS`] when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passage_chars: Option<usize>,
    /// Text put before each query or passage (an instruction some models
    /// expect).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query_prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub passage_prefix: String,
    /// Fields added to every request body (routing options). An OpenRouter
    /// request always carries `provider.data_collection = "deny"` whatever
    /// this says.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub body: Map<String, Value>,
    /// Fields added to query or passage requests only (`input_type`).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub query_body: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub passage_body: Map<String, Value>,
}

/// Which embedder runs. Nothing configured is the pinned local default.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "provider", rename_all = "kebab-case")]
pub enum EmbedderConfig {
    /// A pinned ONNX model (`pins::EMBEDDERS`), run on this machine.
    Local {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    /// `POST {base_url}/embeddings`, OpenAI's request and response.
    #[serde(rename = "openai")]
    OpenAi(Box<Remote>),
}

/// Which reranker runs. Nothing configured is the pinned local default.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "provider", rename_all = "kebab-case")]
pub enum RerankerConfig {
    Local {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    /// `POST {base_url}/rerank`: `model`, `query`, `documents`, `top_n`;
    /// scores from `results[]` or `data[]` (`index`, `relevance_score`).
    Rerank(Box<Remote>),
}

impl Default for EmbedderConfig {
    fn default() -> Self {
        Self::Local { model: None }
    }
}
impl Default for RerankerConfig {
    fn default() -> Self {
        Self::Local { model: None }
    }
}

/// `<data>/memory-service/config.json`. Absent, the service runs the pinned
/// local pair offline.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    #[serde(default)]
    pub embedder: EmbedderConfig,
    #[serde(default)]
    pub reranker: RerankerConfig,
    /// The most a rebuild for a new embedding model may cost before it waits
    /// for the operator (US$; [`DEFAULT_MAX_REBUILD_USD`] when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rebuild_usd: Option<f64>,
    /// The most remote models may cost in one UTC day outside a rebuild:
    /// query embeddings, rerank, and embedding new history once the space is
    /// built (US$; [`DEFAULT_MAX_DAILY_USD`] when absent). Persisted, so a
    /// restart does not reset it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_daily_usd: Option<f64>,
}

pub const DEFAULT_MAX_REBUILD_USD: f64 = 10.0;
pub const DEFAULT_MAX_DAILY_USD: f64 = 1.0;

impl ProviderConfig {
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("memory-service").join("config.json")
    }
    /// The file at `path`, or the local default when there is none: for
    /// the default path only, where no file means nothing is configured.
    pub fn load(path: &Path) -> Result<Self, Error> {
        match std::fs::read(path) {
            Ok(_) => Self::load_required(path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(format!("{}: {e}", path.display()).into()),
        }
    }

    /// The file at `path`, which must exist: a path the operator named is
    /// never replaced by the local default.
    pub fn load_required(path: &Path) -> Result<Self, Error> {
        let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))?)
    }
}

/// The identity of a vector space: equal stamps, comparable vectors.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stamp {
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    /// How raw history became the embedded text: projection, piece budget,
    /// token counter and passage prefix.
    pub preprocessing: String,
}

impl Stamp {
    pub fn key(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
    /// A short digest naming the stamp's dense index file.
    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(self.key().as_bytes()))[..16].to_owned()
    }
}

/// Tokens and money one model has used in this process.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Usage {
    pub requests: u64,
    pub tokens: u64,
    /// As the endpoint reported it, else from the configured price.
    pub usd: f64,
    /// Requests whose cost was neither reported nor priced.
    pub unpriced_requests: u64,
}

impl std::ops::AddAssign for Usage {
    fn add_assign(&mut self, other: Self) {
        self.requests += other.requests;
        self.tokens += other.tokens;
        self.usd += other.usd;
        self.unpriced_requests += other.unpriced_requests;
    }
}

// ---------------------------------------------------------------- profiles

/// The provider's documented API root, for a profile that names none.
fn default_base_url(provider: &str) -> Option<&'static str> {
    match provider {
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        "google-gemini" => Some("https://generativelanguage.googleapis.com/v1beta/openai"),
        "sylphx" => Some("https://api.sylphx.com/v1"),
        "openai" => Some("https://api.openai.com/v1"),
        "voyage" | "voyageai" => Some("https://api.voyageai.com/v1"),
        "cohere" => Some("https://api.cohere.com/v2"),
        "jina" => Some("https://api.jina.ai/v1"),
        _ => None,
    }
}

fn first_file(dir: &Path, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        let text = std::fs::read_to_string(dir.join(name)).ok()?;
        let key = text.trim();
        (!key.is_empty()).then(|| key.to_owned())
    })
}

fn yaml_ref(dir: &Path, name: &str) -> Option<String> {
    yaml_ref_in(
        &std::fs::read_to_string(dir.join(".dsh/.credentials.yaml")).ok()?,
        name,
    )
}

/// A `refs:` entry of a dsh credentials file.
fn yaml_ref_in(text: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}:");
    text.lines().find_map(|line| {
        let value = line.trim().strip_prefix(&prefix)?;
        let value = value.trim().trim_matches('"').trim_matches('\'');
        (!value.is_empty()).then(|| value.to_owned())
    })
}

/// The API key of an API-key profile, from where AnyAgents' profile pool keeps
/// it for that provider (the same files the harness configuration reads).
pub fn profile_api_key(profile_dir: &Path, provider: &str) -> Option<String> {
    let opencode = format!(".config/opencode/{provider}.key");
    let spiron = format!(".spiron/{provider}.key");
    let named = format!("{provider}-api-key");
    let found = match provider {
        "openrouter" => first_file(profile_dir, &[".config/opencode/openrouter.key"])
            .or_else(|| yaml_ref(profile_dir, "OPENROUTER_API_KEY")),
        "google-gemini" => first_file(
            profile_dir,
            &["gemini-api-key", ".config/opencode/gemini.key"],
        )
        .or_else(|| yaml_ref(profile_dir, "GEMINI_API_KEY")),
        "sylphx" => first_file(
            profile_dir,
            &[".config/sylphx/key", ".config/opencode/sylphx.key"],
        ),
        _ => None,
    };
    found.or_else(|| first_file(profile_dir, &["api-key", &named, &opencode, &spiron]))
}

/// Where a remote model is called, and with which key.
#[derive(Clone)]
pub struct Endpoint {
    pub base_url: String,
    key: String,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl Endpoint {
    /// Resolve `remote.profile` under `<data_dir>/profiles`.
    pub fn resolve(data_dir: &Path, remote: &Remote) -> Result<Self, Error> {
        let (provider, profile) = remote
            .profile
            .split_once('/')
            .filter(|(p, n)| !p.is_empty() && !n.is_empty() && !n.contains('/'))
            .ok_or_else(|| format!("profile {:?} is not <provider>/<profile>", remote.profile))?;
        if [provider, profile]
            .iter()
            .any(|part| part.starts_with('.') || part.contains(['\\', '\0']))
        {
            return Err(format!("profile {:?} is not a profile name", remote.profile).into());
        }
        let dir = data_dir.join("profiles").join(provider).join(profile);
        let key = profile_api_key(&dir, provider)
            .ok_or_else(|| format!("profile {} holds no API key", remote.profile))?;
        let base_url = remote
            .base_url
            .clone()
            .or_else(|| default_base_url(provider).map(str::to_owned))
            .ok_or_else(|| {
                format!(
                    "profile {} has no known API root; set base_url",
                    remote.profile
                )
            })?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key,
        })
    }

    /// An endpoint that is never called: for counting what a remote model
    /// would read without reading its profile.
    pub fn offline() -> Self {
        Self {
            base_url: "http://offline.invalid".into(),
            key: String::new(),
        }
    }

    #[cfg(test)]
    pub fn for_test(base_url: &str, key: &str) -> Self {
        Self {
            base_url: base_url.to_owned(),
            key: key.to_owned(),
        }
    }
}

// -------------------------------------------------------------------- HTTP

/// One JSON client for remote models: bounded time per request, retries on
/// throttling and upstream failures, never logs a key or a body.
#[derive(Clone)]
pub struct Http {
    agent: ureq::Agent,
}

impl Default for Http {
    fn default() -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(20)))
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .user_agent("anyagents-memory")
            .build();
        Self {
            agent: config.into(),
        }
    }
}

impl Http {
    pub fn post(&self, endpoint: &Endpoint, path: &str, body: &Value) -> Result<Value, Error> {
        let url = format!("{}{path}", endpoint.base_url);
        let payload = serde_json::to_vec(body)?;
        let mut wait = Duration::from_secs(2);
        for attempt in 1..=ATTEMPTS {
            let response = self
                .agent
                .post(&url)
                .header("Authorization", &format!("Bearer {}", endpoint.key))
                .header("Content-Type", "application/json")
                .send(&payload[..]);
            let mut response = match response {
                Ok(response) => response,
                Err(error) if attempt < ATTEMPTS => {
                    eprintln!("memory-service: {url}: {error}; retrying");
                    std::thread::sleep(wait);
                    wait = (wait * 2).min(MAX_WAIT);
                    continue;
                }
                Err(error) => return Err(format!("{url}: {error}").into()),
            };
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|s| Duration::from_secs(s.min(60)));
            let text = response
                .body_mut()
                .with_config()
                .limit(256 * 1024 * 1024)
                .read_to_string()
                .map_err(|e| format!("{url}: {e}"))?;
            if status == 200 {
                return Ok(serde_json::from_str(&text)
                    .map_err(|e| format!("{url}: unreadable answer: {e}"))?);
            }
            let retryable = matches!(status, 408 | 409 | 425 | 429 | 500 | 502 | 503 | 504 | 529);
            if retryable && attempt < ATTEMPTS {
                std::thread::sleep(retry_after.unwrap_or(wait));
                wait = (wait * 2).min(MAX_WAIT);
                continue;
            }
            let summary: String = text.chars().take(300).collect();
            return Err(format!("{url}: HTTP {status}: {summary}").into());
        }
        Err(format!("{url}: no answer").into())
    }
}

fn merged(base: Value, layers: &[&Map<String, Value>]) -> Value {
    let mut base = base;
    if let Value::Object(object) = &mut base {
        for layer in layers {
            for (k, v) in layer.iter() {
                object.insert(k.clone(), v.clone());
            }
        }
    }
    base
}

/// Is this model reached through OpenRouter, by its profile or its API root?
fn via_openrouter(config: &Remote, endpoint: &Endpoint) -> bool {
    config.profile.split('/').next() == Some("openrouter")
        || endpoint
            .base_url
            .split("://")
            .nth(1)
            .and_then(|rest| rest.split(['/', ':']).next())
            .is_some_and(|host| host == "openrouter.ai" || host.ends_with(".openrouter.ai"))
}

/// OpenRouter routes a request only to upstream providers that do not keep
/// or train on it: `provider.data_collection = "deny"` on every request,
/// merged into any routing options the configuration sets, never left to
/// configuration.
fn deny_data_collection(body: &mut Value) {
    let Value::Object(object) = body else {
        return;
    };
    let provider = object
        .entry("provider")
        .or_insert_with(|| Value::Object(Map::new()));
    if !provider.is_object() {
        *provider = Value::Object(Map::new());
    }
    if let Value::Object(provider) = provider {
        provider.insert("data_collection".into(), json!("deny"));
    }
}

/// The cost an answer reports (`usage.cost`, as OpenRouter does), if any.
fn reported_cost(answer: &Value) -> Option<f64> {
    answer.pointer("/usage/cost").and_then(Value::as_f64)
}

fn normalized(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

// --------------------------------------------------------------- counting

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3040..=0x30ff | 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xac00..=0xd7af
        | 0xf900..=0xfaff | 0xff00..=0xffef | 0x20000..=0x2ffff)
}

/// Approximate token boundaries for a model whose tokenizer is not local:
/// each CJK character, each punctuation mark, and each run of up to four
/// letters or digits is one token. It decides where pieces are cut and
/// estimates what a rebuild reads; the provider counts what it bills.
pub fn approximate_tokens(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut run: Option<(usize, usize, usize)> = None; // start, end, chars
    for (at, c) in text.char_indices() {
        let end = at + c.len_utf8();
        if c.is_alphanumeric() && !is_cjk(c) {
            match &mut run {
                Some((_, e, n)) if *n < 4 => {
                    *e = end;
                    *n += 1;
                }
                _ => {
                    if let Some((s, e, _)) = run.take() {
                        out.push((s, e));
                    }
                    run = Some((at, end, 1));
                }
            }
            continue;
        }
        if let Some((s, e, _)) = run.take() {
            out.push((s, e));
        }
        if !c.is_whitespace() {
            out.push((at, end));
        }
    }
    if let Some((s, e, _)) = run {
        out.push((s, e));
    }
    out
}

// ---------------------------------------------------------------- embedder

/// What a remote model is called with.
pub struct RemoteModel {
    pub config: Remote,
    pub endpoint: Endpoint,
    http: Http,
    pub usage: Usage,
    last_request: Option<Instant>,
}

impl RemoteModel {
    pub fn new(config: Remote, endpoint: Endpoint) -> Self {
        Self {
            config,
            endpoint,
            http: Http::default(),
            usage: Usage::default(),
            last_request: None,
        }
    }

    /// At most a few requests a second from one worker: a rebuild is
    /// background work and must not crowd out queries.
    fn pace(&mut self, gap: Duration) {
        if let Some(last) = self.last_request {
            let since = last.elapsed();
            if since < gap {
                std::thread::sleep(gap - since);
            }
        }
        self.last_request = Some(Instant::now());
    }

    /// The body of one request: the configured fields over the base, and the
    /// data policy a provider must honour.
    fn body(&self, base: Value, extra: &Map<String, Value>) -> Value {
        let mut body = merged(base, &[&self.config.body, extra]);
        if via_openrouter(&self.config, &self.endpoint) {
            deny_data_collection(&mut body);
        }
        body
    }

    fn account(&mut self, answer: &Value, tokens: u64, searches: u64) {
        self.usage.requests += 1;
        self.usage.tokens += tokens;
        match reported_cost(answer) {
            Some(usd) => self.usage.usd += usd,
            None => match (
                self.config.usd_per_search,
                self.config.usd_per_million_tokens,
            ) {
                (Some(per), _) if searches > 0 => self.usage.usd += per * searches as f64,
                (_, Some(per)) => self.usage.usd += per * tokens as f64 / 1e6,
                _ => self.usage.unpriced_requests += 1,
            },
        }
    }

    fn embed(
        &mut self,
        texts: &[String],
        extra: &Map<String, Value>,
    ) -> Result<Vec<Vec<f32>>, Error> {
        let mut out = Vec::with_capacity(texts.len());
        for batch in texts.chunks(REMOTE_BATCH) {
            self.pace(Duration::from_millis(200));
            let mut body = json!({
                "model": self.config.model,
                "input": batch,
                "encoding_format": "float",
            });
            if let Some(d) = self.config.dimensions {
                body["dimensions"] = json!(d);
            }
            let body = self.body(body, extra);
            let answer = self.http.post(&self.endpoint, "/embeddings", &body)?;
            let mut rows: Vec<(usize, Vec<f32>)> = answer
                .get("data")
                .and_then(Value::as_array)
                .ok_or("embeddings answer has no data")?
                .iter()
                .enumerate()
                .map(|(i, row)| {
                    let index = row
                        .get("index")
                        .and_then(Value::as_u64)
                        .map_or(i, |n| n as usize);
                    let vector: Vec<f32> = row
                        .get("embedding")
                        .and_then(Value::as_array)
                        .map(|v| {
                            v.iter()
                                .filter_map(Value::as_f64)
                                .map(|x| x as f32)
                                .collect()
                        })
                        .unwrap_or_default();
                    (index, vector)
                })
                .collect();
            rows.sort_by_key(|(i, _)| *i);
            if rows.len() != batch.len() || rows.iter().any(|(_, v)| v.is_empty()) {
                return Err(format!(
                    "embeddings answer has {} vectors for {} inputs",
                    rows.len(),
                    batch.len()
                )
                .into());
            }
            let tokens = answer
                .pointer("/usage/prompt_tokens")
                .or_else(|| answer.pointer("/usage/total_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or_else(|| {
                    batch
                        .iter()
                        .map(|t| approximate_tokens(t).len() as u64)
                        .sum()
                });
            self.account(&answer, tokens, 0);
            out.extend(rows.into_iter().map(|(_, v)| normalized(v)));
        }
        Ok(out)
    }

    /// Relevance of each passage to `query`, in input order.
    fn rerank(&mut self, query: &str, passages: &[&str]) -> Result<Vec<f32>, Error> {
        if passages.is_empty() {
            return Ok(Vec::new());
        }
        self.pace(Duration::from_millis(100));
        let cut = self.config.passage_chars.unwrap_or(REMOTE_RERANK_CHARS);
        let documents: Vec<String> = passages
            .iter()
            .map(|p| {
                let passage: String = p.chars().take(cut).collect();
                format!("{}{passage}", self.config.passage_prefix)
            })
            .collect();
        let body = json!({
            "model": self.config.model,
            "query": format!("{}{query}", self.config.query_prefix),
            "documents": documents,
            "top_n": documents.len(),
        });
        let body = self.body(body, &Map::new());
        let answer = self.http.post(&self.endpoint, "/rerank", &body)?;
        let rows = answer
            .get("results")
            .or_else(|| answer.get("data"))
            .and_then(Value::as_array)
            .ok_or("rerank answer has no results")?;
        let mut scores = vec![f32::MIN; passages.len()];
        for row in rows {
            let index = row
                .get("index")
                .and_then(Value::as_u64)
                .ok_or("rerank result without index")? as usize;
            let score = row
                .get("relevance_score")
                .or_else(|| row.get("score"))
                .and_then(Value::as_f64)
                .ok_or("rerank result without relevance_score")?;
            if let Some(slot) = scores.get_mut(index) {
                *slot = score as f32;
            }
        }
        let tokens = answer
            .pointer("/usage/total_tokens")
            .or_else(|| answer.pointer("/meta/billed_units/input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_else(|| {
                documents
                    .iter()
                    .map(|p| approximate_tokens(p).len() as u64)
                    .sum::<u64>()
                    + approximate_tokens(query).len() as u64
            });
        let searches = answer
            .pointer("/usage/search_units")
            .or_else(|| answer.pointer("/meta/billed_units/search_units"))
            .and_then(Value::as_u64)
            .unwrap_or(1);
        self.account(&answer, tokens, searches);
        Ok(scores)
    }
}

/// Where a loaded local model's files and runtime come from.
pub struct Local<'a> {
    pub environment: &'static ort::environment::Environment,
    pub models: &'a Path,
    pub threads: usize,
}

/// A loaded embedder.
pub enum Embedder {
    Local(Box<Encoder>),
    Remote {
        model: Box<RemoteModel>,
        dims: usize,
    },
}

/// The pinned local embedder a configuration names.
pub fn local_embedder_pin(config: &EmbedderConfig) -> Result<Option<ModelPin>, Error> {
    Ok(match config {
        EmbedderConfig::Local { model: None } => Some(crate::pins::EMBEDDER),
        EmbedderConfig::Local { model: Some(name) } => Some(
            crate::pins::by_name(name)
                .filter(|p| p.head != crate::model::Head::Logit)
                .ok_or_else(|| format!("no pinned embedder named {name}"))?,
        ),
        EmbedderConfig::OpenAi(_) => None,
    })
}

/// The pinned local reranker a configuration names.
pub fn local_reranker_pin(config: &RerankerConfig) -> Result<Option<ModelPin>, Error> {
    Ok(match config {
        RerankerConfig::Local { model: None } => Some(crate::pins::RERANKER),
        RerankerConfig::Local { model: Some(name) } => Some(
            crate::pins::by_name(name)
                .filter(|p| p.head == crate::model::Head::Logit)
                .ok_or_else(|| format!("no pinned reranker named {name}"))?,
        ),
        RerankerConfig::Rerank(_) => None,
    })
}

impl Embedder {
    /// Load the configured embedder. A local one needs `local`; a remote one
    /// resolves its profile under `data_dir` and, when no width is
    /// configured, learns it from one short request.
    pub fn load(
        config: &EmbedderConfig,
        data_dir: &Path,
        local: Option<&Local<'_>>,
    ) -> Result<Self, Error> {
        match config {
            EmbedderConfig::Local { .. } => {
                let pin = local_embedder_pin(config)?.ok_or("no local embedder")?;
                let local = local.ok_or("a local embedder needs the ONNX runtime")?;
                Ok(Self::Local(Box::new(Encoder::load(
                    local.environment,
                    local.models,
                    &pin,
                    local.threads,
                )?)))
            }
            EmbedderConfig::OpenAi(remote) => {
                let endpoint = Endpoint::resolve(data_dir, remote)?;
                Self::remote((**remote).clone(), endpoint)
            }
        }
    }

    pub fn remote(remote: Remote, endpoint: Endpoint) -> Result<Self, Error> {
        let mut model = Box::new(RemoteModel::new(remote, endpoint));
        let dims = match model.config.dimensions {
            Some(d) => d,
            None => model
                .embed(&["dimension probe".to_owned()], &Map::new())?
                .remove(0)
                .len(),
        };
        Ok(Self::Remote { model, dims })
    }

    pub fn dims(&self) -> usize {
        match self {
            Self::Local(encoder) => encoder.pin().dims,
            Self::Remote { dims, .. } => *dims,
        }
    }

    fn budget(&self) -> usize {
        match self {
            Self::Local(encoder) => encoder
                .pin()
                .max_tokens
                .saturating_sub(RESERVED_TOKENS)
                .max(16),
            Self::Remote { model, .. } => model
                .config
                .piece_tokens
                .unwrap_or(REMOTE_PIECE_TOKENS)
                .saturating_sub(RESERVED_TOKENS)
                .max(16),
        }
    }

    pub fn stamp(&self) -> Stamp {
        let (provider, model, counter, prefix) = match self {
            Self::Local(encoder) => (
                "local".to_owned(),
                encoder.pin().name.to_owned(),
                format!("tokenizer:{}", encoder.pin().name),
                encoder.pin().passage_prefix.to_owned(),
            ),
            Self::Remote { model, .. } => (
                format!(
                    "openai:{}",
                    model.config.profile.split('/').next().unwrap_or_default()
                ),
                model.config.model.clone(),
                "approximate-v1".to_owned(),
                format!(
                    "{}{}",
                    model.config.passage_prefix,
                    serde_json::to_string(&model.config.passage_body).unwrap_or_default()
                ),
            ),
        };
        use sha2::{Digest, Sha256};
        let prefix = format!("{:x}", Sha256::digest(prefix.as_bytes()));
        Stamp {
            provider,
            model,
            dimensions: self.dims(),
            preprocessing: format!(
                "readable-v1;pieces={MAX_PIECES};budget={};counter={counter};prefix={}",
                self.budget(),
                &prefix[..12]
            ),
        }
    }

    /// The pieces of one raw event as this embedder reads them.
    pub fn chunks_of(&self, raw: &[u8]) -> Result<Vec<Chunk>, Error> {
        let spans = project(raw);
        let budget = self.budget();
        match self {
            Self::Local(encoder) => {
                let tokenizer = encoder.counter();
                let mut count = |text: &str| -> Result<Vec<(usize, usize)>, Error> {
                    Ok(tokenizer
                        .encode(text, false)
                        .map_err(|e| e.to_string())?
                        .get_offsets()
                        .to_vec())
                };
                chunk(&spans, &mut count, budget, MAX_PIECES)
            }
            Self::Remote { .. } => {
                let mut count = |text: &str| -> Result<Vec<(usize, usize)>, Error> {
                    Ok(approximate_tokens(text))
                };
                chunk(&spans, &mut count, budget, MAX_PIECES)
            }
        }
    }

    /// Inputs one call should carry.
    pub fn batch(&self) -> usize {
        match self {
            Self::Local(_) => 16,
            Self::Remote { .. } => REMOTE_BATCH,
        }
    }

    pub fn embed_queries(&mut self, queries: &[&str]) -> Result<Vec<Vec<f32>>, Error> {
        match self {
            Self::Local(encoder) => encoder.embed_queries(queries),
            Self::Remote { model, .. } => {
                let texts: Vec<String> = queries
                    .iter()
                    .map(|q| format!("{}{q}", model.config.query_prefix))
                    .collect();
                let extra = model.config.query_body.clone();
                model.embed(&texts, &extra)
            }
        }
    }

    pub fn embed_passages(&mut self, passages: &[&str]) -> Result<Vec<Vec<f32>>, Error> {
        match self {
            Self::Local(encoder) => encoder.embed_passages(passages),
            Self::Remote { model, .. } => {
                let texts: Vec<String> = passages
                    .iter()
                    .map(|p| format!("{}{p}", model.config.passage_prefix))
                    .collect();
                let extra = model.config.passage_body.clone();
                model.embed(&texts, &extra)
            }
        }
    }

    /// What this embedder has spent; nothing for a local model.
    pub fn usage(&self) -> Usage {
        match self {
            Self::Local(_) => Usage::default(),
            Self::Remote { model, .. } => model.usage,
        }
    }

    /// US$ per million tokens, if known.
    pub fn price(&self) -> Option<f64> {
        match self {
            Self::Local(_) => Some(0.0),
            Self::Remote { model, .. } => model.config.usd_per_million_tokens,
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

// ---------------------------------------------------------------- reranker

/// A loaded reranker.
pub enum Reranker {
    Local(Box<Encoder>),
    Remote(Box<RemoteModel>),
}

impl Reranker {
    pub fn load(
        config: &RerankerConfig,
        data_dir: &Path,
        local: Option<&Local<'_>>,
    ) -> Result<Self, Error> {
        match config {
            RerankerConfig::Local { .. } => {
                let pin = local_reranker_pin(config)?.ok_or("no local reranker")?;
                let local = local.ok_or("a local reranker needs the ONNX runtime")?;
                Ok(Self::Local(Box::new(Encoder::load(
                    local.environment,
                    local.models,
                    &pin,
                    local.threads,
                )?)))
            }
            RerankerConfig::Rerank(remote) => Ok(Self::Remote(Box::new(RemoteModel::new(
                (**remote).clone(),
                Endpoint::resolve(data_dir, remote)?,
            )))),
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Local(encoder) => encoder.pin().name.to_owned(),
            Self::Remote(model) => format!("{} ({})", model.config.model, model.config.profile),
        }
    }

    /// Relevance of each passage to `query`, in input order. A remote
    /// reranker reads all passages in one request.
    pub fn score(&mut self, query: &str, passages: &[&str]) -> Result<Vec<f32>, Error> {
        match self {
            Self::Local(encoder) => {
                let mut scores = Vec::with_capacity(passages.len());
                for batch in passages.chunks(8) {
                    scores.extend(encoder.score(query, batch)?);
                }
                Ok(scores)
            }
            Self::Remote(model) => model.rerank(query, passages),
        }
    }

    pub fn usage(&self) -> Usage {
        match self {
            Self::Local(_) => Usage::default(),
            Self::Remote(model) => model.usage,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// A one-thread HTTP server answering each request with `answer(path,
    /// body)`, recording what it was sent.
    #[allow(clippy::type_complexity)]
    pub(crate) fn serve(
        answer: impl Fn(&str, &Value) -> (u16, Value) + Send + 'static,
    ) -> (String, Arc<Mutex<Vec<(String, String, Value)>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
                let mut length = 0;
                let mut auth = String::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let lower = header.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if lower.starts_with("authorization:") {
                        auth = header["authorization:".len()..].trim().to_owned();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let body: Value = serde_json::from_slice(&body).unwrap();
                let (status, reply) = answer(&path, &body);
                record.lock().unwrap().push((path, auth, body));
                let text = reply.to_string();
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                    text.len()
                )
                .unwrap();
            }
        });
        (url, seen)
    }

    #[test]
    fn a_profile_key_is_read_where_the_profile_pool_keeps_it() {
        let dir = crate::test_tempdir();
        let profiles = dir.path().join("profiles");
        assert_eq!(
            yaml_ref_in(
                "version: 1\nrefs:\n  OPENROUTER_API_KEY: \"sk-or-test\"\n",
                "OPENROUTER_API_KEY"
            )
            .as_deref(),
            Some("sk-or-test")
        );
        // (Test sandboxes refuse dot-directories such as `.config` or `.dsh`,
        // so the files a harness writes are covered by the parser above and
        // the plain fallback here.)
        let or = profiles.join("openrouter/default");
        std::fs::create_dir_all(&or).unwrap();
        std::fs::write(or.join("api-key"), "sk-or-test\n").unwrap();
        let gem = profiles.join("google-gemini/api");
        std::fs::create_dir_all(&gem).unwrap();
        std::fs::write(gem.join("gemini-api-key"), "g-key\n").unwrap();
        let remote = |profile: &str| Remote {
            profile: profile.into(),
            model: "m".into(),
            ..Remote::default()
        };
        let e = Endpoint::resolve(dir.path(), &remote("openrouter/default")).unwrap();
        assert_eq!(e.base_url, "https://openrouter.ai/api/v1");
        assert_eq!(e.key, "sk-or-test");
        let e = Endpoint::resolve(dir.path(), &remote("google-gemini/api")).unwrap();
        assert_eq!(e.key, "g-key");
        assert!(e.base_url.ends_with("/v1beta/openai"));
        // No key, no traversal, no unknown root without base_url.
        assert!(Endpoint::resolve(dir.path(), &remote("openrouter/none")).is_err());
        assert!(Endpoint::resolve(dir.path(), &remote("../x")).is_err());
        assert!(Endpoint::resolve(dir.path(), &remote("openrouter/..")).is_err());
        let other = profiles.join("acme/k");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("api-key"), "a").unwrap();
        assert!(Endpoint::resolve(dir.path(), &remote("acme/k")).is_err());
        let mut with_root = remote("acme/k");
        with_root.base_url = Some("https://example.test/v1/".into());
        assert_eq!(
            Endpoint::resolve(dir.path(), &with_root).unwrap().base_url,
            "https://example.test/v1"
        );
        // The key never appears in a debug print.
        let e = Endpoint::resolve(dir.path(), &remote("openrouter/default")).unwrap();
        assert!(!format!("{e:?}").contains("sk-or-test"));
    }

    #[test]
    fn configuration_defaults_to_the_local_pair_and_reads_remote_providers() {
        assert_eq!(
            ProviderConfig::load(Path::new("/nonexistent/config.json")).unwrap(),
            ProviderConfig::default()
        );
        // A path the operator named must exist: a typo is never the local
        // default.
        let missing = ProviderConfig::load_required(Path::new("/nonexistent/config.json"));
        assert!(
            missing
                .unwrap_err()
                .to_string()
                .contains("/nonexistent/config.json")
        );
        let config: ProviderConfig = serde_json::from_value(json!({
            "embedder": {"provider": "openai", "profile": "openrouter/default",
                         "model": "qwen/qwen3-embedding-8b", "dimensions": 1024,
                         "usd_per_million_tokens": 0.01},
            "reranker": {"provider": "rerank", "profile": "openrouter/default",
                         "model": "cohere/rerank-5-pro"},
            "max_rebuild_usd": 3.0
        }))
        .unwrap();
        let EmbedderConfig::OpenAi(remote) = &config.embedder else {
            panic!("{config:?}")
        };
        assert_eq!(remote.dimensions, Some(1024));
        assert!(matches!(config.reranker, RerankerConfig::Rerank(_)));
        assert!(
            serde_json::from_value::<ProviderConfig>(json!({"embedder": {"provider": "openai",
                "profile": "a/b", "model": "m", "typo": 1}}))
            .is_err()
        );
        assert_eq!(
            local_embedder_pin(&EmbedderConfig::default())
                .unwrap()
                .unwrap()
                .name,
            crate::pins::EMBEDDER.name
        );
        assert!(
            local_embedder_pin(&EmbedderConfig::Local {
                model: Some(crate::pins::RERANKER.name.into())
            })
            .is_err()
        );
    }

    #[test]
    fn approximate_tokens_count_cjk_by_character_and_words_by_four_letters() {
        let text = "點解 ship it, 部署v2";
        let tokens: Vec<&str> = approximate_tokens(text)
            .into_iter()
            .map(|(s, e)| &text[s..e])
            .collect();
        assert_eq!(
            tokens,
            vec!["點", "解", "ship", "it", ",", "部", "署", "v2"]
        );
        assert_eq!(approximate_tokens("abcdefghij").len(), 3);
    }

    #[test]
    fn a_remote_embedder_sends_the_openai_request_and_stamps_its_space() {
        let (url, seen) = serve(|path, body| {
            assert_eq!(path, "/v1/embeddings");
            let n = body["input"].as_array().unwrap().len();
            let data: Vec<Value> = (0..n)
                .rev()
                .map(|i| json!({"index": i, "embedding": [3.0, 4.0 + i as f64]}))
                .collect();
            (
                200,
                json!({"data": data, "usage": {"prompt_tokens": 7 * n, "cost": 0.001}}),
            )
        });
        let remote = Remote {
            profile: "openrouter/default".into(),
            model: "qwen/qwen3-embedding-8b".into(),
            query_prefix: "Q: ".into(),
            query_body: [("input_type".to_owned(), json!("query"))]
                .into_iter()
                .collect(),
            body: [("provider".to_owned(), json!({"data_collection": "deny"}))]
                .into_iter()
                .collect(),
            ..Remote::default()
        };
        let mut embedder = Embedder::remote(
            remote.clone(),
            Endpoint::for_test(&format!("{url}/v1"), "k1"),
        )
        .unwrap();
        assert_eq!(embedder.dims(), 2, "learned from the probe");
        let vectors = embedder.embed_queries(&["a", "b"]).unwrap();
        assert_eq!(vectors.len(), 2);
        assert!(
            (vectors[0][0] - 0.6).abs() < 1e-6,
            "normalized, in input order"
        );
        let requests = seen.lock().unwrap();
        let (_, auth, body) = requests.last().unwrap();
        assert_eq!(auth, "Bearer k1");
        assert_eq!(body["input"], json!(["Q: a", "Q: b"]));
        assert_eq!(body["input_type"], json!("query"));
        assert_eq!(body["provider"]["data_collection"], json!("deny"));
        assert_eq!(body["model"], json!("qwen/qwen3-embedding-8b"));
        drop(requests);
        let usage = embedder.usage();
        assert_eq!(usage.requests, 2);
        assert!((usage.usd - 0.002).abs() < 1e-9, "reported cost is summed");
        let stamp = embedder.stamp();
        assert_eq!(stamp.provider, "openai:openrouter");
        assert_eq!(stamp.model, "qwen/qwen3-embedding-8b");
        assert_eq!(stamp.dimensions, 2);
        // Another model, width or cut is another space.
        let mut wider = remote.clone();
        wider.dimensions = Some(3);
        let other =
            Embedder::remote(wider, Endpoint::for_test(&format!("{url}/v1"), "k1")).unwrap();
        assert_ne!(other.stamp().digest(), stamp.digest());
        let mut cut = remote;
        cut.piece_tokens = Some(256);
        let other = Embedder::remote(cut, Endpoint::for_test(&format!("{url}/v1"), "k1")).unwrap();
        assert_ne!(other.stamp().digest(), stamp.digest());
    }

    #[test]
    fn a_remote_reranker_reads_cohere_and_voyage_answers_and_retries_throttling() {
        let calls = Arc::new(Mutex::new(0));
        let counter = Arc::clone(&calls);
        let (url, seen) = serve(move |path, body| {
            assert_eq!(path, "/v1/rerank");
            let mut n = counter.lock().unwrap();
            *n += 1;
            if *n == 1 {
                return (429, json!({"error": "slow down"}));
            }
            let docs = body["documents"].as_array().unwrap().len();
            if body["model"] == "voyage" {
                (
                    200,
                    json!({"data": (0..docs).map(|i| json!({"index": i, "relevance_score": i as f64})).collect::<Vec<_>>(),
                             "usage": {"total_tokens": 40}}),
                )
            } else {
                (
                    200,
                    json!({"results": [{"index": 1, "relevance_score": 0.9}, {"index": 0, "relevance_score": 0.1}],
                             "meta": {"billed_units": {"search_units": 1}}}),
                )
            }
        });
        let mut cohere = Reranker::Remote(Box::new(RemoteModel::new(
            Remote {
                profile: "openrouter/default".into(),
                model: "cohere".into(),
                usd_per_search: Some(0.002),
                ..Remote::default()
            },
            Endpoint::for_test(&format!("{url}/v1"), "k"),
        )));
        let scores = cohere.score("q", &["a", "b"]).unwrap();
        assert_eq!(scores, vec![0.1, 0.9]);
        assert_eq!(seen.lock().unwrap()[1].2["top_n"], json!(2));
        assert!((cohere.usage().usd - 0.002).abs() < 1e-12);
        let mut voyage = Reranker::Remote(Box::new(RemoteModel::new(
            Remote {
                profile: "openrouter/default".into(),
                model: "voyage".into(),
                usd_per_million_tokens: Some(50.0),
                ..Remote::default()
            },
            Endpoint::for_test(&format!("{url}/v1"), "k"),
        )));
        assert_eq!(
            voyage.score("q", &["a", "b", "c"]).unwrap(),
            vec![0.0, 1.0, 2.0]
        );
        assert!((voyage.usage().usd - 0.002).abs() < 1e-12);
        assert_eq!(*calls.lock().unwrap(), 3, "one retry after 429");
    }

    /// The owner's data policy is code, not configuration: every request to
    /// OpenRouter, the dimension probe and rerank included, asks for upstream
    /// providers that keep nothing, alongside any routing the configuration
    /// sets. A remote reranker reads a bounded passage.
    #[test]
    fn every_openrouter_request_denies_data_collection_and_rerank_passages_are_bounded() {
        let (url, seen) = serve(|path, body| match path {
            "/v1/embeddings" => {
                let n = body["input"].as_array().unwrap().len();
                let data: Vec<Value> = (0..n)
                    .map(|i| json!({"index": i, "embedding": [1.0, 0.0]}))
                    .collect();
                (200, json!({"data": data}))
            }
            _ => {
                let n = body["documents"].as_array().unwrap().len();
                let results: Vec<Value> = (0..n)
                    .map(|i| json!({"index": i, "relevance_score": 0.5}))
                    .collect();
                (200, json!({"results": results}))
            }
        });
        let endpoint = || Endpoint::for_test(&format!("{url}/v1"), "k");
        let remote = |profile: &str, body: Value| Remote {
            profile: profile.into(),
            model: "m".into(),
            body: body.as_object().cloned().unwrap_or_default(),
            ..Remote::default()
        };
        // No dimensions: the first request is the probe.
        let mut plain =
            Embedder::remote(remote("openrouter/default", json!({})), endpoint()).unwrap();
        plain.embed_passages(&["p"]).unwrap();
        let mut routed = Embedder::remote(
            remote(
                "openrouter/default",
                json!({"provider": {"sort": "price", "data_collection": "allow"}}),
            ),
            endpoint(),
        )
        .unwrap();
        routed.embed_queries(&["q"]).unwrap();
        let mut odd = Embedder::remote(
            remote("openrouter/default", json!({"provider": "anything"})),
            endpoint(),
        )
        .unwrap();
        odd.embed_queries(&["q"]).unwrap();
        let mut reranker = Reranker::Remote(Box::new(RemoteModel::new(
            remote("openrouter/default", json!({})),
            endpoint(),
        )));
        let long = "長".repeat(3 * REMOTE_RERANK_CHARS);
        reranker.score("q", &[long.as_str(), "short"]).unwrap();
        let mut narrow = remote("openrouter/default", json!({}));
        narrow.passage_chars = Some(5);
        let mut narrow = Reranker::Remote(Box::new(RemoteModel::new(narrow, endpoint())));
        narrow.score("q", &["abcdefghij"]).unwrap();
        // Another provider's request carries no OpenRouter field.
        let mut voyage = Embedder::remote(remote("voyage/api", json!({})), endpoint()).unwrap();
        voyage.embed_queries(&["q"]).unwrap();

        let requests = seen.lock().unwrap();
        let (openrouter, other) = requests.split_at(requests.len() - 2);
        assert_eq!(
            openrouter.len(),
            8,
            "probe and call per embedder, two reranks"
        );
        for (path, _, body) in openrouter {
            assert_eq!(
                body["provider"]["data_collection"],
                json!("deny"),
                "{path}: {body}"
            );
        }
        assert_eq!(
            openrouter[3].2["provider"]["sort"],
            json!("price"),
            "routing is kept"
        );
        let documents = |i: usize| -> Vec<usize> {
            openrouter[i].2["documents"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d.as_str().unwrap().chars().count())
                .collect()
        };
        assert_eq!(documents(6), vec![REMOTE_RERANK_CHARS, 5]);
        assert_eq!(documents(7), vec![5]);
        for (_, _, body) in other {
            assert!(body.get("provider").is_none(), "{body}");
        }
        // An OpenRouter root under another profile name is OpenRouter too.
        let custom = remote("custom/x", json!({}));
        assert!(via_openrouter(
            &custom,
            &Endpoint::for_test("https://openrouter.ai/api/v1", "k")
        ));
        assert!(!via_openrouter(
            &custom,
            &Endpoint::for_test("https://api.voyageai.com/v1", "k")
        ));
        assert!(!via_openrouter(
            &custom,
            &Endpoint::for_test("https://openrouter.ai.example.com/v1", "k")
        ));
    }

    /// The desk's configuration, exactly as `docs/host-memory.md` prints it,
    /// is what this build reads, and names the pair the owner chose.
    #[test]
    fn the_documented_desk_configuration_parses_as_the_chosen_pair() {
        let doc = include_str!("../../../docs/host-memory.md");
        let section = doc.split("### Desk configuration").nth(1).unwrap();
        let block = section.split("```json\n").nth(1).unwrap();
        let text = block.split("\n```").next().unwrap();
        let config: ProviderConfig = serde_json::from_str(text).unwrap();
        let EmbedderConfig::OpenAi(embedder) = &config.embedder else {
            panic!("{config:?}");
        };
        assert_eq!(
            (
                embedder.profile.as_str(),
                embedder.model.as_str(),
                embedder.dimensions
            ),
            ("openrouter/default", "qwen/qwen3-embedding-8b", Some(1024))
        );
        assert!(embedder.usd_per_million_tokens.is_some());
        let RerankerConfig::Rerank(reranker) = &config.reranker else {
            panic!("{config:?}");
        };
        assert_eq!(
            (reranker.profile.as_str(), reranker.model.as_str()),
            ("openrouter/default", "mongodb/rerank-3")
        );
        assert_eq!(config.max_rebuild_usd, Some(10.0));
        assert!(config.max_daily_usd.is_some());
    }
}
