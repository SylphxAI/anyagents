//! Per-profile model catalogue: which models a provider profile's own endpoint serves.
//!
//! # Sources, in order
//!
//! 1. **Caller override.** A `models` array in the profile's `profile.json` is
//!    authoritative when present. A malformed override is a refusal.
//! 2. **Fresh snapshot.** A snapshot younger than [`SNAPSHOT_FRESH_MS`] is used
//!    without touching the network.
//! 3. **Live read.** The profile's own provider model list, read with the
//!    profile's own credentials and endpoint (never a harness cache, which
//!    profile provisioning copies between profiles). A non-empty result is
//!    written to [`SNAPSHOT_FILE`] in the profile directory.
//! 4. **Stale snapshot.** When the live read fails, a snapshot younger than
//!    [`SNAPSHOT_TTL_MS`] is used.
//!
//! With none of these the catalogue is unknown and callers refuse (fail closed).
//!
//! # Matching
//!
//! Ids are compared on whole `/`-separated segments after dropping the
//! `#effort` suffix: a model matches an entry when one id's segment list ends
//! with the other's (so a routing prefix such as `<provider>/` or a vendor
//! namespace on either side does not matter). An entry may also be a dated
//! snapshot of the model id (`<model>-YYYYMMDD`). Nothing else matches.

use std::path::Path;
use std::time::Duration;

use anyagents_core::Error;

pub(crate) const SNAPSHOT_FILE: &str = ".anyagents-model-catalogue.json";
/// Snapshot age under which no live read is attempted.
pub(crate) const SNAPSHOT_FRESH_MS: i64 = 10 * 60 * 1000;
/// Oldest snapshot accepted when the live read fails.
pub(crate) const SNAPSHOT_TTL_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// `models` in `profile.json`.
    Declared,
    /// Read from the provider just now with the profile's credentials.
    Live,
    /// A snapshot of an earlier live read.
    Snapshot { age_ms: i64 },
}

impl Source {
    /// True when the catalogue was obtained by authenticating as the profile.
    pub(crate) fn authenticated(&self) -> bool {
        !matches!(self, Source::Declared)
    }

    pub(crate) fn label(&self) -> String {
        match self {
            Source::Declared => "declared".to_string(),
            Source::Live => "live".to_string(),
            Source::Snapshot { age_ms } => format!("snapshot ({}s old)", age_ms / 1000),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Catalogue {
    pub models: Vec<String>,
    pub source: Source,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Snapshot {
    fetched_at_ms: i64,
    models: Vec<String>,
}

/// `<model>#<effort>` without the effort suffix.
pub(crate) fn base_model_id(model: &str) -> &str {
    let model = model.trim();
    model.split_once('#').map_or(model, |(base, _)| base).trim()
}

fn segments(id: &str) -> Vec<&str> {
    id.split('/').filter(|s| !s.is_empty()).collect()
}

fn ends_with_segments(long: &[&str], short: &[&str]) -> bool {
    !short.is_empty() && long.len() >= short.len() && long[long.len() - short.len()..] == *short
}

fn is_dated_snapshot_of(entry: &str, model_last: &str) -> bool {
    entry
        .strip_prefix(model_last)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|date| date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()))
}

/// True when `model` is served according to `catalogue` (see module docs).
pub(crate) fn serves(catalogue: &[String], model: &str) -> bool {
    let model = base_model_id(model);
    let model_segs = segments(model);
    let Some(model_last) = model_segs.last() else {
        return false;
    };
    catalogue.iter().any(|entry| {
        let entry = base_model_id(entry);
        let entry_segs = segments(entry);
        if ends_with_segments(&model_segs, &entry_segs)
            || ends_with_segments(&entry_segs, &model_segs)
        {
            return true;
        }
        entry_segs
            .last()
            .is_some_and(|last| is_dated_snapshot_of(last, model_last))
    })
}

fn read_declared(dir: &Path) -> Result<Option<Vec<String>>, Error> {
    let path = dir.join("profile.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(Error::Message(format!(
                "cannot read {} ({err}); refusing",
                path.display()
            )));
        }
    };
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|err| {
        Error::Message(format!(
            "{} is not valid JSON ({err}); refusing",
            path.display()
        ))
    })?;
    let Some(models) = value.get("models") else {
        return Ok(None);
    };
    let list = models.as_array().ok_or_else(|| {
        Error::Message(format!(
            "`models` in {} must be an array of model ids; refusing",
            path.display()
        ))
    })?;
    list.iter()
        .map(|entry| {
            entry
                .as_str()
                .map(base_model_id)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    Error::Message(format!(
                        "`models` in {} has a non-string or empty entry; refusing",
                        path.display()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn read_snapshot(dir: &Path) -> Option<Snapshot> {
    let text = std::fs::read_to_string(dir.join(SNAPSHOT_FILE)).ok()?;
    serde_json::from_str::<Snapshot>(&text)
        .ok()
        .filter(|s| !s.models.is_empty())
}

fn write_snapshot(dir: &Path, models: &[String], now_ms: i64) {
    let snap = Snapshot {
        fetched_at_ms: now_ms,
        models: models.to_vec(),
    };
    if let Ok(bytes) = serde_json::to_vec_pretty(&snap) {
        let _ =
            anyagents_harness::atomic_write_nonempty_file(&dir.join(SNAPSHOT_FILE), &bytes, 0o644);
    }
}

/// Reads a provider model list for `(profile_dir, provider)`.
pub(crate) type LiveReader<'a> = &'a dyn Fn(&Path, &str) -> Result<Vec<String>, String>;

/// Resolve with an injected live reader (tests) and clock.
pub(crate) fn resolve_with(
    dir: &Path,
    provider: &str,
    now_ms: i64,
    read_live: LiveReader<'_>,
) -> Result<Catalogue, Error> {
    if let Some(models) = read_declared(dir)? {
        return Ok(Catalogue {
            models,
            source: Source::Declared,
        });
    }
    let snapshot = read_snapshot(dir);
    let snapshot_age = snapshot
        .as_ref()
        .map(|s| now_ms.saturating_sub(s.fetched_at_ms));
    if let (Some(snap), Some(age)) = (&snapshot, snapshot_age)
        && (0..SNAPSHOT_FRESH_MS).contains(&age)
    {
        return Ok(Catalogue {
            models: snap.models.clone(),
            source: Source::Snapshot { age_ms: age },
        });
    }
    let live_err = match read_live(dir, provider) {
        Ok(models) if !models.is_empty() => {
            write_snapshot(dir, &models, now_ms);
            return Ok(Catalogue {
                models,
                source: Source::Live,
            });
        }
        Ok(_) => "the provider returned an empty model list".to_string(),
        Err(err) => err,
    };
    if let (Some(snap), Some(age)) = (snapshot, snapshot_age)
        && (0..SNAPSHOT_TTL_MS).contains(&age)
    {
        return Ok(Catalogue {
            models: snap.models,
            source: Source::Snapshot { age_ms: age },
        });
    }
    Err(Error::Message(format!(
        "model catalogue of profile `{provider}/{}` is unknown: live read failed ({live_err}) and no snapshot younger than {}h exists; refusing",
        dir.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        SNAPSHOT_TTL_MS / 3_600_000
    )))
}

/// Resolve a profile's catalogue, reading the provider live when needed.
pub(crate) fn resolve(data: &Path, provider: &str, profile: &str) -> Result<Catalogue, Error> {
    let dir = data.join("profiles").join(provider).join(profile);
    let catalogue = resolve_with(
        &dir,
        provider,
        anyagents_core::now_ms(),
        &|dir, provider| read_live(dir, provider),
    );
    if catalogue.is_err() && matches!(provider, "anthropic" | "google-antigravity") {
        // An expired OAuth access token is refreshed by the usage probe; retry once.
        let _ = crate::create::probe_one_profile_usage(data, provider, profile);
        return resolve_with(
            &dir,
            provider,
            anyagents_core::now_ms(),
            &|dir, provider| read_live(dir, provider),
        );
    }
    catalogue
}

fn http() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(8)))
        .build()
        .new_agent()
}

fn env_url(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn read_json(dir: &Path, rel: &str) -> Option<serde_json::Value> {
    serde_json::from_str(&std::fs::read_to_string(dir.join(rel)).ok()?).ok()
}

fn read_trimmed(dir: &Path, rel: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(rel))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn get_body(req: ureq::RequestBuilder<ureq::typestate::WithoutBody>) -> Result<String, String> {
    match req.call() {
        Ok(resp) => resp
            .into_body()
            .read_to_string()
            .map_err(|err| format!("unreadable response: {err}")),
        Err(ureq::Error::StatusCode(code)) => Err(format!("HTTP {code}")),
        Err(err) => Err(err.to_string()),
    }
}

/// `{"data": [{"id": ...}, ...]}` (OpenAI-compatible and Anthropic shapes).
pub(crate) fn parse_data_ids(body: &str) -> Result<Vec<String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|err| format!("model list is not JSON: {err}"))?;
    let data = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "model list has no `data` array".to_string())?;
    Ok(data
        .iter()
        .filter_map(|m| m.get("id").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// `{"models": {"<id>": {...}, ...}}` (Antigravity backend shape).
pub(crate) fn parse_model_keys(body: &str) -> Result<Vec<String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|err| format!("model list is not JSON: {err}"))?;
    let models = value
        .get("models")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "model list has no `models` object".to_string())?;
    Ok(models.keys().cloned().collect())
}

fn read_live(dir: &Path, provider: &str) -> Result<Vec<String>, String> {
    match provider {
        "google-antigravity" => read_antigravity(dir),
        "anthropic" => read_anthropic(dir),
        _ => read_openai_compatible(dir, provider),
    }
}

fn read_antigravity(dir: &Path) -> Result<Vec<String>, String> {
    let tok = read_json(dir, ".gemini/antigravity-cli/antigravity-oauth-token")
        .and_then(|v| {
            v.pointer("/token/access_token")
                .or_else(|| v.get("access_token"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .ok_or_else(|| "profile holds no access token".to_string())?;
    let url = env_url(
        "ANYAGENTS_ANTIGRAVITY_MODELS_URL",
        "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
    );
    let body = match http()
        .post(&url)
        .header("authorization", &format!("Bearer {tok}"))
        .header("content-type", "application/json")
        .header("user-agent", "antigravity")
        .send(b"{}")
    {
        Ok(resp) => resp
            .into_body()
            .read_to_string()
            .map_err(|err| format!("unreadable response: {err}"))?,
        Err(ureq::Error::StatusCode(code)) => return Err(format!("HTTP {code}")),
        Err(err) => return Err(err.to_string()),
    };
    parse_model_keys(&body)
}

fn read_anthropic(dir: &Path) -> Result<Vec<String>, String> {
    let base = env_url("ANYAGENTS_ANTHROPIC_API_URL", "https://api.anthropic.com");
    let url = format!("{}/v1/models?limit=1000", base.trim_end_matches('/'));
    let oauth = [".credentials.json", ".claude/.credentials.json"]
        .into_iter()
        .find_map(|rel| {
            read_json(dir, rel)?
                .pointer("/claudeAiOauth/accessToken")?
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });
    let req = http().get(&url).header("anthropic-version", "2023-06-01");
    let req = if let Some(tok) = oauth {
        req.header("authorization", &format!("Bearer {tok}"))
            .header("anthropic-beta", "oauth-2025-04-20")
    } else if let Some(key) = read_trimmed(dir, ".config/opencode/anthropic.key") {
        req.header("x-api-key", &key)
    } else {
        return Err("profile holds no credential".to_string());
    };
    parse_data_ids(&get_body(req)?)
}

/// The profile's own endpoint: an explicit base URL in the profile's harness
/// configuration, else the provider's published default.
fn openai_compatible_base(dir: &Path, provider: &str) -> Option<String> {
    let probe_model = format!("{provider}/_");
    crate::spiron::discover_seat_gateway_url_for_model(dir, Some(&probe_model))
        .or_else(|| config_toml_gateway_url(dir))
        .or_else(|| match provider {
            "sylphx" => Some("https://api.sylphx.com/v1".to_string()),
            "openrouter" => Some("https://openrouter.ai/api/v1".to_string()),
            "openai" => Some("https://api.openai.com/v1".to_string()),
            "deepseek" => Some("https://api.deepseek.com/v1".to_string()),
            "xai" => Some("https://api.x.ai/v1".to_string()),
            _ => None,
        })
}

/// `sylphx_ai_url = "<url>"` in the profile's harness `config.toml`.
fn config_toml_gateway_url(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join("config.toml")).ok()?;
    text.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "sylphx_ai_url")
            .then(|| serde_json::from_str::<String>(value.trim()).ok())
            .flatten()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

fn openai_compatible_key(dir: &Path, provider: &str) -> Option<String> {
    let mut rels = vec![
        format!(".spiron/{provider}.key"),
        format!(".config/opencode/{provider}.key"),
    ];
    if provider == "qwen-token-plan" {
        rels.push(".config/opencode/qwen-coding-plan.key".to_string());
    }
    rels.extend(
        [".spiron/key", ".config/sylphx/key"]
            .into_iter()
            .map(str::to_string),
    );
    if let Some(key) = rels.iter().find_map(|rel| read_trimmed(dir, rel)) {
        return Some(key);
    }
    for rel in [
        ".spiron/credentials.json",
        ".config/sylphx/credentials.json",
        "credentials.json",
    ] {
        if let Some(val) = read_json(dir, rel)
            && let Some(key) = crate::spiron::extract_project_bound_key(dir, &val)
        {
            return Some(key);
        }
    }
    for rel in [
        "auth.json",
        ".spiron/auth.json",
        ".config/sylphx/auth.json",
        "credentials.json",
    ] {
        if let Some(key) = read_json(dir, rel).and_then(|v| {
            v.get("access_token")
                .or_else(|| v.get("api_key"))
                .or_else(|| v.get("OPENAI_API_KEY"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        }) {
            return Some(key);
        }
    }
    for rel in ["opencode.json", ".config/opencode/opencode.json"] {
        if let Some(key) = read_json(dir, rel).and_then(|v| {
            v.pointer(&format!("/provider/{provider}/options/apiKey"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        }) {
            return Some(key);
        }
    }
    None
}

fn read_openai_compatible(dir: &Path, provider: &str) -> Result<Vec<String>, String> {
    let base = openai_compatible_base(dir, provider)
        .ok_or_else(|| format!("no model list endpoint is known for provider `{provider}`"))?;
    let key = openai_compatible_key(dir, provider)
        .ok_or_else(|| "profile holds no API key".to_string())?;
    let url = format!("{}/models", base.trim_end_matches('/'));
    let req = http()
        .get(&url)
        .header("authorization", &format!("Bearer {key}"));
    parse_data_ids(&get_body(req)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn serves_matches_whole_segment_suffixes_and_dated_snapshots() {
        let cat = list(&["lab/model-a", "model-b", "model-c-20260101"]);
        for model in [
            "vendor/lab/model-a#xhigh",
            "lab/model-a",
            "model-a",
            "vendor/other/model-b",
            "model-c",
        ] {
            assert!(serves(&cat, model), "{model}");
        }
    }

    #[test]
    fn serves_refuses_partial_segments_and_other_models() {
        let cat = list(&["lab/model-a", "model-c-20260101"]);
        for model in [
            "lab/model-a-pro",
            "lab/model",
            "odel-a",
            "model-c-2026",
            "lab/model-b",
            "",
            "#xhigh",
        ] {
            assert!(!serves(&cat, model), "{model}");
        }
    }

    #[test]
    fn parsers_read_provider_shapes_and_reject_others() {
        assert_eq!(
            parse_data_ids(r#"{"data":[{"id":"a"},{"id":" b "},{"x":1}]}"#).unwrap(),
            list(&["a", "b"])
        );
        assert!(parse_data_ids(r#"{"models":[]}"#).is_err());
        assert!(parse_data_ids("<html>").is_err());
        let mut keys = parse_model_keys(r#"{"models":{"m1":{},"m2":{}}}"#).unwrap();
        keys.sort();
        assert_eq!(keys, list(&["m1", "m2"]));
        assert!(parse_model_keys(r#"{"data":[]}"#).is_err());
    }

    fn dir_with(meta: Option<&str>) -> tempfile::TempDir {
        let dir = crate::test_tempdir();
        if let Some(meta) = meta {
            std::fs::write(dir.path().join("profile.json"), meta).unwrap();
        }
        dir
    }

    #[test]
    fn declared_override_wins_and_malformed_override_refuses() {
        let dir = dir_with(Some(r#"{"models":["lab/a"]}"#));
        let live = |_: &Path, _: &str| -> Result<Vec<String>, String> { panic!("no live read") };
        let cat = resolve_with(dir.path(), "p", 1, &live).unwrap();
        assert_eq!(cat.source, Source::Declared);
        assert_eq!(cat.models, list(&["lab/a"]));
        for bad in [r#"{"models":"lab/a"}"#, r#"{"models":[1]}"#, "{nope"] {
            let dir = dir_with(Some(bad));
            assert!(resolve_with(dir.path(), "p", 1, &live).is_err(), "{bad}");
        }
    }

    #[test]
    fn live_read_is_snapshotted_and_fresh_snapshot_skips_the_network() {
        let dir = dir_with(Some(r#"{"provider":"p"}"#));
        let live = |_: &Path, _: &str| Ok(list(&["lab/a"]));
        let cat = resolve_with(dir.path(), "p", 1_000, &live).unwrap();
        assert_eq!(cat.source, Source::Live);
        assert!(dir.path().join(SNAPSHOT_FILE).is_file());
        let no_network = |_: &Path, _: &str| -> Result<Vec<String>, String> { panic!("network") };
        let cat =
            resolve_with(dir.path(), "p", 1_000 + SNAPSHOT_FRESH_MS - 1, &no_network).unwrap();
        assert_eq!(
            cat.source,
            Source::Snapshot {
                age_ms: SNAPSHOT_FRESH_MS - 1
            }
        );
    }

    #[test]
    fn stale_snapshot_covers_a_failed_live_read_until_the_ttl() {
        let dir = dir_with(None);
        let live_ok = |_: &Path, _: &str| Ok(list(&["lab/a"]));
        resolve_with(dir.path(), "p", 0, &live_ok).unwrap();
        let live_down =
            |_: &Path, _: &str| -> Result<Vec<String>, String> { Err("HTTP 503".into()) };
        let cat = resolve_with(dir.path(), "p", SNAPSHOT_TTL_MS - 1, &live_down).unwrap();
        assert!(matches!(cat.source, Source::Snapshot { .. }));
        let err = resolve_with(dir.path(), "p", SNAPSHOT_TTL_MS, &live_down)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("HTTP 503") && err.contains("no snapshot"),
            "{err}"
        );
    }

    #[test]
    fn no_override_no_snapshot_and_failed_or_empty_live_read_refuses() {
        let dir = dir_with(None);
        let down = |_: &Path, _: &str| -> Result<Vec<String>, String> { Err("HTTP 401".into()) };
        assert!(resolve_with(dir.path(), "p", 5, &down).is_err());
        let empty = |_: &Path, _: &str| Ok(Vec::new());
        let err = resolve_with(dir.path(), "p", 5, &empty)
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty model list"), "{err}");
        assert!(!dir.path().join(SNAPSHOT_FILE).exists());
    }

    #[test]
    fn openai_compatible_reader_uses_the_profiles_own_endpoint_and_key() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let n = sock.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = r#"{"data":[{"id":"lab/served"}]}"#;
            write!(
                sock,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            req
        });
        let dir = dir_with(None);
        std::fs::write(
            dir.path().join("opencode.json"),
            serde_json::json!({"provider": {"vendor": {"options": {
                "baseURL": format!("http://{addr}/v1")
            }}}})
            .to_string(),
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join(".config/opencode")).unwrap();
        std::fs::write(dir.path().join(".config/opencode/vendor.key"), "k-1\n").unwrap();
        let models = read_live(dir.path(), "vendor").unwrap();
        assert_eq!(models, list(&["lab/served"]));
        let req = server.join().unwrap();
        assert!(req.starts_with("GET /v1/models "), "{req}");
        assert!(
            req.to_ascii_lowercase()
                .contains("authorization: bearer k-1"),
            "{req}"
        );
    }

    #[test]
    fn openai_compatible_reader_refuses_without_endpoint_or_key() {
        let dir = dir_with(None);
        let err = read_live(dir.path(), "unknown-provider").unwrap_err();
        assert!(err.contains("no model list endpoint"), "{err}");
        std::fs::write(
            dir.path().join("opencode.json"),
            r#"{"provider":{"vendor":{"options":{"baseURL":"http://127.0.0.1:9/v1"}}}}"#,
        )
        .unwrap();
        let err = read_live(dir.path(), "vendor").unwrap_err();
        assert!(err.contains("no API key"), "{err}");
    }
}
