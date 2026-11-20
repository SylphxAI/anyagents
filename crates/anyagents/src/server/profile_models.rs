//! Dynamic multi-harness model catalog builder for `GET /v1/profiles/models` (`agents(models)`).
//!
//! AnyAgents is a pure orchestrator and never hardcodes static model lists or
//! whitelists. Catalog entries come exclusively from live observed sources:
//! 1. Live provider/CLI probes (`probe_profile_models_catalog`, hydrated provider profiles).
//! 2. Live OpenAI-compatible remote `/v1/models` discovery when configured via pure-OSS
//!    environment variables (`ANYAGENTS_MODELS_URL`, `ANYAGENTS_MODELS_BASE_URL`,
//!    `ANYAGENTS_GATEWAY_BASE_URL`, `OPENAI_BASE_URL`, or per-harness
//!    `ANYAGENTS_<HARNESS>_MODELS_URL` / `ANYAGENTS_<HARNESS>_BASE_URL`), with a short
//!    2s timeout, bounded 5m TTL cache, and strict fail-open behavior returning `[]`.
//! 3. Models already observed on live configured agents (`source: "configured_agent"`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use anyagents_harness::{decompose_harness_selector, decompose_legacy_selector};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

/// Canonical harness buckets reported in `harnesses` (strict alphabetical order,
/// always initialized empty unless populated by live observed sources).
const KNOWN_HARNESSES: &[&str] = &[
    "agy",
    "claude-code",
    "codex",
    "gemini",
    "omp",
    "opencode",
    "spiron",
];

const REMOTE_MODELS_TIMEOUT: Duration = Duration::from_secs(2);
const REMOTE_MODELS_CACHE_TTL: Duration = Duration::from_secs(300);
const REMOTE_MODELS_CACHE_MAX_ENTRIES: usize = 64;
const REMOTE_MODELS_MAX_BODY_BYTES: u64 = 2 * 1024 * 1024;

const MODEL_SELECTION_NOTE: &str = "AnyAgents is a pure orchestrator and does not maintain a static model whitelist. Any opaque model ID string supported by the target harness, provider, or gateway is accepted and forwarded untouched.";

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct RemoteCacheKey {
    url: String,
    auth_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Debug)]
struct CachedRemoteModels {
    fetched_at: Instant,
    models: Vec<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteEndpoint {
    url: String,
    api_key: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteDiscoveryConfig {
    global: Option<RemoteEndpoint>,
    per_harness: BTreeMap<String, RemoteEndpoint>,
}

fn remote_models_cache() -> &'static Mutex<HashMap<RemoteCacheKey, CachedRemoteModels>> {
    static CACHE: OnceLock<Mutex<HashMap<RemoteCacheKey, CachedRemoteModels>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn is_http_url(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    (lower.starts_with("http://") || lower.starts_with("https://")) && raw.len() > "http://".len()
}

fn normalize_explicit_models_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    is_http_url(trimmed).then(|| trimmed.to_string())
}

fn models_url_from_base(base: &str) -> Option<String> {
    let trimmed = base.trim().trim_end_matches('/');
    if !is_http_url(trimmed) {
        return None;
    }
    if trimmed.ends_with("/models") {
        Some(trimmed.to_string())
    } else if trimmed.ends_with("/v1") {
        Some(format!("{trimmed}/models"))
    } else {
        Some(format!("{trimmed}/v1/models"))
    }
}

fn non_empty_trimmed(val: Option<String>) -> Option<String> {
    val.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

impl RemoteDiscoveryConfig {
    fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let shared_key = non_empty_trimmed(lookup("ANYAGENTS_MODELS_API_KEY"))
            .or_else(|| non_empty_trimmed(lookup("ANYAGENTS_MODELS_TOKEN")));
        let openai_key = non_empty_trimmed(lookup("OPENAI_API_KEY"));

        let global = non_empty_trimmed(lookup("ANYAGENTS_MODELS_URL"))
            .and_then(|u| normalize_explicit_models_url(&u))
            .map(|url| RemoteEndpoint {
                url,
                api_key: shared_key.clone().or_else(|| openai_key.clone()),
            })
            .or_else(|| {
                non_empty_trimmed(lookup("ANYAGENTS_MODELS_BASE_URL"))
                    .or_else(|| non_empty_trimmed(lookup("ANYAGENTS_GATEWAY_BASE_URL")))
                    .and_then(|base| models_url_from_base(&base))
                    .map(|url| RemoteEndpoint {
                        url,
                        api_key: shared_key.clone().or_else(|| openai_key.clone()),
                    })
            })
            .or_else(|| {
                non_empty_trimmed(lookup("OPENAI_BASE_URL"))
                    .and_then(|base| models_url_from_base(&base))
                    .map(|url| RemoteEndpoint {
                        url,
                        api_key: openai_key.clone().or_else(|| shared_key.clone()),
                    })
            });

        let mut per_harness = BTreeMap::new();
        for &harness in KNOWN_HARNESSES {
            let env_prefix = harness.to_ascii_uppercase().replace('-', "_");
            let url = non_empty_trimmed(lookup(&format!("ANYAGENTS_{env_prefix}_MODELS_URL")))
                .and_then(|u| normalize_explicit_models_url(&u))
                .or_else(|| {
                    non_empty_trimmed(lookup(&format!("ANYAGENTS_{env_prefix}_BASE_URL")))
                        .and_then(|base| models_url_from_base(&base))
                });
            if let Some(url) = url {
                let api_key = non_empty_trimmed(lookup(&format!("ANYAGENTS_{env_prefix}_API_KEY")))
                    .or_else(|| shared_key.clone());
                per_harness.insert(harness.to_string(), RemoteEndpoint { url, api_key });
            }
        }

        Self {
            global,
            per_harness,
        }
    }
}

fn auth_fingerprint(api_key: Option<&str>) -> Option<[u8; 32]> {
    api_key.map(str::trim).filter(|s| !s.is_empty()).map(|key| {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hasher.finalize().into()
    })
}

fn parse_openai_models_payload(body: &str) -> Option<Vec<Value>> {
    let val: Value = serde_json::from_str(body).ok()?;
    let items = val
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| val.get("models").and_then(Value::as_array))
        .or_else(|| val.as_array())?;

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        let (id, label, owned_by) = match item {
            Value::String(s) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    continue;
                }
                (trimmed, trimmed, None)
            }
            Value::Object(obj) => {
                let Some(id) = obj
                    .get("id")
                    .or_else(|| obj.get("model"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    continue;
                };
                let label = obj
                    .get("label")
                    .or_else(|| obj.get("name"))
                    .or_else(|| obj.get("display_name"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(id);
                let owned_by = obj
                    .get("owned_by")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                (id, label, owned_by)
            }
            _ => continue,
        };
        if !seen.insert(id.to_string()) {
            continue;
        }
        let mut entry = Map::new();
        entry.insert("id".to_string(), Value::String(id.to_string()));
        entry.insert("label".to_string(), Value::String(label.to_string()));
        entry.insert("default".to_string(), Value::Bool(false));
        entry.insert(
            "source".to_string(),
            Value::String("remote_catalog".to_string()),
        );
        if let Some(owner) = owned_by {
            entry.insert("owned_by".to_string(), Value::String(owner.to_string()));
        }
        out.push(Value::Object(entry));
    }
    Some(out)
}

fn fetch_remote_models_cached(
    endpoint: &RemoteEndpoint,
    timeout: Duration,
    ttl: Duration,
) -> Vec<Value> {
    if !is_http_url(&endpoint.url) {
        return Vec::new();
    }
    let key = RemoteCacheKey {
        url: endpoint.url.clone(),
        auth_sha256: auth_fingerprint(endpoint.api_key.as_deref()),
    };
    if !ttl.is_zero()
        && let Ok(cache) = remote_models_cache().lock()
        && let Some(cached) = cache.get(&key)
        && cached.fetched_at.elapsed() < ttl
    {
        return cached.models.clone();
    }

    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .build()
        .new_agent();
    let mut req = agent
        .get(&endpoint.url)
        .header("accept", "application/json");
    if let Some(token) = endpoint
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        req = req.header("authorization", &format!("Bearer {token}"));
    }

    let Ok(resp) = req.call() else {
        return Vec::new();
    };
    let Ok(body) = resp
        .into_body()
        .with_config()
        .limit(REMOTE_MODELS_MAX_BODY_BYTES)
        .read_to_string()
    else {
        return Vec::new();
    };
    let Some(models) = parse_openai_models_payload(&body) else {
        return Vec::new();
    };

    if !ttl.is_zero()
        && let Ok(mut cache) = remote_models_cache().lock()
    {
        let now = Instant::now();
        cache.retain(|_, v| now.saturating_duration_since(v.fetched_at) < ttl);
        if cache.len() >= REMOTE_MODELS_CACHE_MAX_ENTRIES
            && !cache.contains_key(&key)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, v)| v.fetched_at)
                .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            key,
            CachedRemoteModels {
                fetched_at: now,
                models: models.clone(),
            },
        );
    }

    models
}

/// Resolve a harness selector (including `harness@runtime` and legacy suffixes such as
/// `spiron@cloud`, `spiron@sandbox`, `spiron-cloud`, `claude-cloud`, `openai-cloud`)
/// to its canonical lowercase harness key.
fn canonical_harness_key(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    if let Ok((canonical, _)) = decompose_harness_selector(&lower) {
        return canonical;
    }
    let base = lower.split_once('@').map_or(lower.as_str(), |(h, _)| h);
    decompose_legacy_selector(base).0
}

fn provider_matches_filter(provider: &str, profile: &str, filter: Option<&str>) -> bool {
    let Some(f) = filter else {
        return true;
    };
    let full = format!("{provider}/{profile}");
    let canon = canonical_harness_key(f);
    f.eq_ignore_ascii_case(provider)
        || f.eq_ignore_ascii_case(profile)
        || f.eq_ignore_ascii_case(&full)
        || canon.eq_ignore_ascii_case(provider)
        || (canon == "agy" && provider == "google-antigravity")
        || (canon == "claude-code" && provider == "anthropic")
        || (canon == "codex" && provider == "openai")
        || (canon == "gemini" && provider == "google")
}

fn harness_matches_filter(harness: &str, filter: Option<&str>) -> bool {
    let Some(f) = filter else {
        return true;
    };
    let norm = canonical_harness_key(harness);
    let f_norm = canonical_harness_key(f);
    norm == f_norm
        || (norm == "agy" && f_norm == "google-antigravity")
        || (norm == "claude-code" && f_norm == "anthropic")
        || (norm == "codex" && f_norm == "openai")
        || (norm == "gemini" && f_norm == "google")
}

/// Hydrate profiles from PostgreSQL `deployment_blobs(namespace='profiles')` when
/// running in Postgres mode, probe live provider catalogs, query any configured
/// remote OpenAI-compatible `/v1/models` endpoints, and merge models observed on
/// live configured agents. Never fabricates static/hardcoded model lists.
pub(crate) fn catalog_for_store(store: &Store, target: Option<&str>) -> Value {
    let remote = RemoteDiscoveryConfig::from_env();
    catalog_for_store_with_remote(
        store,
        target,
        &remote,
        REMOTE_MODELS_TIMEOUT,
        REMOTE_MODELS_CACHE_TTL,
    )
}

fn catalog_for_store_with_remote(
    store: &Store,
    target: Option<&str>,
    remote: &RemoteDiscoveryConfig,
    remote_timeout: Duration,
    remote_ttl: Duration,
) -> Value {
    let data = store.data_dir();
    crate::create::hydrate_profiles_from_store_if_postgres(store, data);

    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("models") && !s.eq_ignore_ascii_case("all")
    });
    let probe_filter_buf: Option<String> = filter.and_then(|f| {
        if f.contains('/') {
            return None;
        }
        let canon = canonical_harness_key(f);
        (!canon.eq_ignore_ascii_case(f)).then_some(canon)
    });
    let probe_filter = probe_filter_buf.as_deref().or(filter);

    let mut base = crate::create::probe_profile_models_catalog(data, probe_filter);
    let agy_cli_models = base
        .get("agy_cli_models")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut profiles = base
        .get("profiles")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut seen_profiles: HashSet<String> = profiles
        .iter()
        .filter_map(|p| p.get("profile").and_then(Value::as_str).map(str::to_string))
        .collect();

    // Initialize empty harness buckets for known harnesses matching the filter.
    // Never populate static/hardcoded fallback models.
    let mut harness_models: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for &h in KNOWN_HARNESSES {
        if harness_matches_filter(h, filter) {
            harness_models.entry(h.to_string()).or_default();
        }
    }

    // 1. Live `agy` CLI / OAuth probe models.
    if !agy_cli_models.is_empty() && harness_matches_filter("agy", filter) {
        let agy_map = harness_models.entry("agy".to_string()).or_default();
        for item in &agy_cli_models {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                agy_map.insert(id.to_string(), item.clone());
            }
        }
    }

    // 2. Live dynamic OpenAI-compatible remote `/v1/models` discovery (strictly fail-open).
    let remote_models = remote
        .global
        .as_ref()
        .map(|ep| fetch_remote_models_cached(ep, remote_timeout, remote_ttl))
        .unwrap_or_default();

    if !remote_models.is_empty() {
        for map in harness_models.values_mut() {
            for item in &remote_models {
                if let Some(id) = item.get("id").and_then(Value::as_str) {
                    map.entry(id.to_string()).or_insert_with(|| item.clone());
                }
            }
        }
    }

    for (raw_harness, endpoint) in &remote.per_harness {
        let canon = canonical_harness_key(raw_harness);
        if !harness_matches_filter(&canon, filter) {
            continue;
        }
        let discovered = fetch_remote_models_cached(endpoint, remote_timeout, remote_ttl);
        let map = harness_models.entry(canon).or_default();
        for item in discovered {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                map.insert(id.to_string(), item);
            }
        }
    }

    // 3. Models already observed on live configured agents.
    if let Ok(live) = store.list_live_agents() {
        for agent in live {
            let norm = canonical_harness_key(&agent.harness);
            if !harness_matches_filter(&norm, filter) {
                continue;
            }
            if let Some(m) = agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                harness_models
                    .entry(norm)
                    .or_default()
                    .entry(m.to_string())
                    .or_insert_with(|| {
                        json!({
                            "id": m,
                            "label": m,
                            "default": false,
                            "source": "configured_agent",
                        })
                    });
            }
        }
    }

    // Enumerate all hydrated/local provider profiles (not only `google-antigravity`).
    let profiles_root = data.join("profiles");
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        let mut prov_list: Vec<_> = providers.flatten().collect();
        prov_list.sort_by_key(|e| e.file_name());
        for prov_entry in prov_list {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            let mut prof_list: Vec<_> = prof_dirs.flatten().collect();
            prof_list.sort_by_key(|e| e.file_name());
            for prof_entry in prof_list {
                let prof_dir = prof_entry.path();
                if !prof_dir.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                if profile.starts_with('.') {
                    continue;
                }
                if !provider_matches_filter(&provider, &profile, filter) {
                    continue;
                }
                let full_name = format!("{provider}/{profile}");
                if !seen_profiles.insert(full_name.clone()) {
                    continue;
                }
                let compatible = crate::create::compatible_harnesses_for(&provider);
                let mut models = Vec::new();
                for &h in compatible {
                    let canon = canonical_harness_key(h);
                    if let Some(map) = harness_models.get(&canon) {
                        for m in map.values() {
                            if !models
                                .iter()
                                .any(|existing: &Value| existing["id"] == m["id"])
                            {
                                models.push(m.clone());
                            }
                        }
                    }
                }
                profiles.push(json!({
                    "profile": full_name,
                    "provider": provider,
                    "compatible_harnesses": compatible,
                    "models": models,
                }));
            }
        }
    }

    let harnesses: Value = Value::Object(
        harness_models
            .into_iter()
            .map(|(h, map)| (h, Value::Array(map.into_values().collect())))
            .collect(),
    );

    if let Some(obj) = base.as_object_mut() {
        obj.insert("agy_cli_models".to_string(), Value::Array(agy_cli_models));
        obj.insert("remote_models".to_string(), Value::Array(remote_models));
        obj.insert("profiles".to_string(), Value::Array(profiles));
        obj.insert("harnesses".to_string(), harnesses);
        obj.insert("opaque_models_accepted".to_string(), Value::Bool(true));
        obj.insert(
            "model_selection_note".to_string(),
            Value::String(MODEL_SELECTION_NOTE.to_string()),
        );
        base
    } else {
        json!({
            "agy_cli_models": agy_cli_models,
            "remote_models": remote_models,
            "profiles": profiles,
            "harnesses": harnesses,
            "opaque_models_accepted": true,
            "model_selection_note": MODEL_SELECTION_NOTE,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyagents_core::NewAgent;

    use super::*;

    fn spawn_mock_http_server(
        status: &'static str,
        body: &'static str,
        delay: Duration,
        expected_auth: Option<&'static str>,
        max_requests: usize,
    ) -> (String, Arc<AtomicUsize>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_bg = Arc::clone(&hits);
        let handle = std::thread::spawn(move || {
            for _ in 0..max_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                hits_bg.fetch_add(1, Ordering::SeqCst);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req_text = String::from_utf8_lossy(&buf[..n]);
                if let Some(auth) = expected_auth {
                    let expected_header = format!("authorization: Bearer {auth}");
                    assert!(
                        req_text
                            .to_ascii_lowercase()
                            .contains(&expected_header.to_ascii_lowercase()),
                        "expected bearer header `{expected_header}`, got:\n{req_text}"
                    );
                }
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}/v1/models"), hits, handle)
    }

    #[test]
    fn catalog_fabricates_zero_hardcoded_models_when_no_live_sources_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let mut ag = NewAgent::new("director-a", "claude-code");
        ag.model = Some("provider/custom-live-model".into());
        let created = store.create_agent(ag).unwrap();
        store.set_agent_running(&created.id, "/work").unwrap();

        std::fs::create_dir_all(data.join("profiles/anthropic/work")).unwrap();
        std::fs::create_dir_all(data.join("profiles/openai/default")).unwrap();

        let cat = catalog_for_store_with_remote(
            &store,
            None,
            &RemoteDiscoveryConfig::default(),
            REMOTE_MODELS_TIMEOUT,
            REMOTE_MODELS_CACHE_TTL,
        );

        assert_eq!(cat["opaque_models_accepted"], Value::Bool(true));
        assert!(
            cat["model_selection_note"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "expected non-empty model_selection_note"
        );
        assert!(
            cat["remote_models"].as_array().unwrap().is_empty(),
            "remote_models must be empty when no remote URL is configured"
        );

        // No hardcoded/fabricated baseline models for unconfigured harnesses.
        for empty_harness in ["spiron", "codex", "opencode", "gemini", "omp"] {
            assert_eq!(
                cat["harnesses"][empty_harness].as_array().unwrap(),
                &Vec::<Value>::new(),
                "harness `{empty_harness}` must not contain hardcoded fallback models"
            );
        }

        // Only live observed agent model appears under `claude-code`.
        let claude_models = cat["harnesses"]["claude-code"].as_array().unwrap();
        assert_eq!(claude_models.len(), 1, "expected only live agent model");
        assert_eq!(claude_models[0]["id"], "provider/custom-live-model");
        assert_eq!(claude_models[0]["source"], "configured_agent");

        // Hydrated profiles still enumerate cleanly.
        let profiles = cat["profiles"].as_array().unwrap();
        assert!(
            profiles.iter().any(|p| p["profile"] == "anthropic/work"),
            "expected anthropic/work profile in catalog: {profiles:?}"
        );
        assert!(
            profiles.iter().any(|p| p["profile"] == "openai/default"),
            "expected openai/default profile in catalog: {profiles:?}"
        );
        let openai_profile = profiles
            .iter()
            .find(|p| p["profile"] == "openai/default")
            .unwrap();
        assert!(
            openai_profile["models"].as_array().unwrap().is_empty(),
            "openai/default profile must not fabricate static models"
        );

        let filtered = catalog_for_store_with_remote(
            &store,
            Some("codex"),
            &RemoteDiscoveryConfig::default(),
            REMOTE_MODELS_TIMEOUT,
            REMOTE_MODELS_CACHE_TTL,
        );
        assert_eq!(
            filtered["harnesses"]["codex"].as_array().unwrap(),
            &Vec::<Value>::new()
        );
        assert!(filtered["harnesses"].get("agy").is_none());
        assert!(filtered["harnesses"].get("spiron").is_none());
    }

    #[test]
    fn remote_v1_models_discovery_populates_dynamically_caches_within_ttl_and_fails_open() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path().join("state.db")).unwrap();

        // 1. Live mock `/v1/models` with bearer auth -> populates dynamically and caches within TTL.
        let (url, hits, handle) = spawn_mock_http_server(
            "200 OK",
            r#"{"object":"list","data":[{"id":"acme/dynamic-model-v2","name":"Dynamic Model V2","owned_by":"acme"},{"id":"acme/reasoning-pro#xhigh"}]}"#,
            Duration::ZERO,
            Some("test-bearer-token"),
            1,
        );
        let remote = RemoteDiscoveryConfig {
            global: Some(RemoteEndpoint {
                url: url.clone(),
                api_key: Some("test-bearer-token".to_string()),
            }),
            per_harness: BTreeMap::new(),
        };

        let first = catalog_for_store_with_remote(
            &store,
            Some("spiron@cloud"),
            &remote,
            Duration::from_secs(2),
            Duration::from_secs(300),
        );
        let second = catalog_for_store_with_remote(
            &store,
            Some("spiron-cloud"),
            &remote,
            Duration::from_secs(2),
            Duration::from_secs(300),
        );
        handle.join().unwrap();

        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second call within TTL must hit cache without re-fetching upstream"
        );
        for cat in [&first, &second] {
            let remote_list = cat["remote_models"].as_array().unwrap();
            assert_eq!(remote_list.len(), 2);
            assert_eq!(remote_list[0]["id"], "acme/dynamic-model-v2");
            assert_eq!(remote_list[0]["label"], "Dynamic Model V2");
            assert_eq!(remote_list[0]["source"], "remote_catalog");
            assert_eq!(remote_list[0]["owned_by"], "acme");
            assert_eq!(remote_list[1]["id"], "acme/reasoning-pro#xhigh");

            let spiron_models = cat["harnesses"]["spiron"].as_array().unwrap();
            assert_eq!(spiron_models.len(), 2);
            assert!(
                spiron_models
                    .iter()
                    .any(|m| m["id"] == "acme/dynamic-model-v2")
            );
            assert!(
                spiron_models
                    .iter()
                    .any(|m| m["id"] == "acme/reasoning-pro#xhigh")
            );
        }

        // 2. HTTP 500 error -> fails open returning `[]`, never fabricating models.
        let (err_url, err_hits, err_handle) = spawn_mock_http_server(
            "500 Internal Server Error",
            r#"{"error":"upstream unavailable"}"#,
            Duration::ZERO,
            None,
            1,
        );
        let err_remote = RemoteDiscoveryConfig {
            global: Some(RemoteEndpoint {
                url: err_url,
                api_key: None,
            }),
            per_harness: BTreeMap::new(),
        };
        let err_cat = catalog_for_store_with_remote(
            &store,
            Some("spiron"),
            &err_remote,
            Duration::from_secs(2),
            Duration::from_secs(300),
        );
        err_handle.join().unwrap();
        assert_eq!(err_hits.load(Ordering::SeqCst), 1);
        assert!(err_cat["remote_models"].as_array().unwrap().is_empty());
        assert!(
            err_cat["harnesses"]["spiron"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        // 3. Upstream timeout -> fails open returning `[]`.
        let (slow_url, slow_hits, slow_handle) = spawn_mock_http_server(
            "200 OK",
            r#"{"data":[{"id":"too-slow/model"}]}"#,
            Duration::from_millis(400),
            None,
            1,
        );
        let slow_remote = RemoteDiscoveryConfig {
            global: Some(RemoteEndpoint {
                url: slow_url,
                api_key: None,
            }),
            per_harness: BTreeMap::new(),
        };
        let slow_cat = catalog_for_store_with_remote(
            &store,
            Some("spiron"),
            &slow_remote,
            Duration::from_millis(80),
            Duration::from_secs(300),
        );
        slow_handle.join().unwrap();
        assert_eq!(slow_hits.load(Ordering::SeqCst), 1);
        assert!(slow_cat["remote_models"].as_array().unwrap().is_empty());
        assert!(
            slow_cat["harnesses"]["spiron"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn remote_discovery_config_parses_pure_oss_env_vars_and_normalizes_base_urls() {
        let mut env = HashMap::new();
        env.insert(
            "ANYAGENTS_MODELS_BASE_URL".to_string(),
            "https://gateway.example.test/v1/".to_string(),
        );
        env.insert(
            "ANYAGENTS_MODELS_API_KEY".to_string(),
            "shared-secret".to_string(),
        );
        env.insert(
            "ANYAGENTS_SPIRON_BASE_URL".to_string(),
            "https://spiron-gw.example.test".to_string(),
        );
        env.insert(
            "ANYAGENTS_SPIRON_API_KEY".to_string(),
            "spiron-secret".to_string(),
        );

        let cfg = RemoteDiscoveryConfig::from_lookup(|k| env.get(k).cloned());
        assert_eq!(
            cfg.global,
            Some(RemoteEndpoint {
                url: "https://gateway.example.test/v1/models".to_string(),
                api_key: Some("shared-secret".to_string()),
            })
        );
        assert_eq!(
            cfg.per_harness.get("spiron"),
            Some(&RemoteEndpoint {
                url: "https://spiron-gw.example.test/v1/models".to_string(),
                api_key: Some("spiron-secret".to_string()),
            })
        );

        // Explicit ANYAGENTS_MODELS_URL takes precedence verbatim over base URLs.
        env.insert(
            "ANYAGENTS_MODELS_URL".to_string(),
            "https://catalog.example.test/custom/models".to_string(),
        );
        let cfg2 = RemoteDiscoveryConfig::from_lookup(|k| env.get(k).cloned());
        assert_eq!(
            cfg2.global.as_ref().map(|e| e.url.as_str()),
            Some("https://catalog.example.test/custom/models")
        );

        // OPENAI_BASE_URL fallback appends `/v1/models` or `/models` cleanly.
        let mut oai = HashMap::new();
        oai.insert(
            "OPENAI_BASE_URL".to_string(),
            "https://api.example.test/v1".to_string(),
        );
        oai.insert("OPENAI_API_KEY".to_string(), "sk-test".to_string());
        let cfg3 = RemoteDiscoveryConfig::from_lookup(|k| oai.get(k).cloned());
        assert_eq!(
            cfg3.global,
            Some(RemoteEndpoint {
                url: "https://api.example.test/v1/models".to_string(),
                api_key: Some("sk-test".to_string()),
            })
        );
    }

    #[test]
    fn harness_matches_filter_resolves_runtime_and_legacy_suffixes() {
        for filter in [
            "spiron",
            "spiron@cloud",
            "spiron@sandbox",
            "spiron@sandbox:docker",
            "spiron@local",
            "spiron-cloud",
            "spiron_cloud",
            "spiron-local",
            "sylphx",
            "sylphx-cloud",
            "SPIRON@CLOUD",
        ] {
            assert!(
                harness_matches_filter("spiron", Some(filter)),
                "spiron should match filter `{filter}`"
            );
            assert!(
                !harness_matches_filter("codex", Some(filter)),
                "codex must not match filter `{filter}`"
            );
        }

        assert!(harness_matches_filter("claude-code", Some("claude@cloud")));
        assert!(harness_matches_filter("claude-code", Some("claude-cloud")));
        assert!(harness_matches_filter("claude-code", Some("anthropic")));
        assert!(harness_matches_filter("codex", Some("openai-cloud")));
        assert!(harness_matches_filter("codex", Some("codex@sandbox")));
        assert!(harness_matches_filter("agy", Some("agy@sandbox")));
        assert!(harness_matches_filter("agy", Some("google-antigravity")));
        assert!(harness_matches_filter("gemini", Some("google")));
        assert!(provider_matches_filter(
            "spiron",
            "default",
            Some("spiron@cloud")
        ));
        assert!(provider_matches_filter(
            "anthropic",
            "work",
            Some("claude@cloud")
        ));
    }

    #[test]
    fn opaque_model_ids_are_accepted_untouched_without_local_gating() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let opaque = "provider/custom-model-2026#xhigh";

        // 1. Subagent default resolution forwards opaque model strings untouched.
        let (harness, resolved_model, _) = crate::create::resolve_subagent_defaults(
            data,
            None,
            "spiron",
            Some(opaque.to_string()),
            None,
        );
        assert_eq!(harness, "spiron");
        assert_eq!(resolved_model.as_deref(), Some(opaque));

        // 2. Spiron model normalization leaves opaque non-prefixed model strings untouched.
        assert_eq!(crate::spiron::normalize_model(opaque), opaque);

        // 3. Agent creation accepts arbitrary opaque model IDs with zero local whitelist gating.
        let created = crate::create::create(
            &store,
            data,
            crate::create::CreateRequest {
                name: "opaque-worker".to_string(),
                harness: "spiron".to_string(),
                model: Some(opaque.to_string()),
                ..Default::default()
            },
        )
        .expect("opaque model ID must be accepted on create without local gating");
        assert_eq!(created.model.as_deref(), Some(opaque));

        // 4. Runtime model reconfiguration accepts another arbitrary opaque model string untouched.
        let opaque_v2 = "vendor-next/experimental-2027-alpha:reasoning=max";
        let _ = crate::create::configure_agent_runtime(
            &store,
            data,
            "operator",
            "opaque-worker",
            None,
            Some(opaque_v2),
            None,
        )
        .expect("opaque model ID must be accepted on runtime update without local gating");
        let reloaded = store.agent(&created.id).unwrap();
        assert_eq!(reloaded.model.as_deref(), Some(opaque_v2));

        // 5. Running agent's opaque model surfaces dynamically in catalog as `configured_agent`.
        store.set_agent_running(&created.id, "/work").unwrap();
        let cat = catalog_for_store_with_remote(
            &store,
            Some("spiron@cloud"),
            &RemoteDiscoveryConfig::default(),
            REMOTE_MODELS_TIMEOUT,
            REMOTE_MODELS_CACHE_TTL,
        );
        assert_eq!(cat["opaque_models_accepted"], Value::Bool(true));
        let spiron_models = cat["harnesses"]["spiron"].as_array().unwrap();
        assert_eq!(spiron_models.len(), 1);
        assert_eq!(spiron_models[0]["id"], opaque_v2);
        assert_eq!(spiron_models[0]["source"], "configured_agent");
    }
}
