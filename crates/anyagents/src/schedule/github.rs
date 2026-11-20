//! Pure-OSS GitHub REST API watch parser and poller (`github:pr:*`, `github:check:*`).
//!
//! Uses `ureq` against `https://api.github.com` (`ANYAGENTS_GITHUB_API_BASE` override)
//! with zero `gh` CLI dependency, ETag caching, rate-limit backoff, and per-agent secrets.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use anyagents_core::{AgentId, AgentSchedule, Store};
use serde_json::Value;

use super::{WatchEval, WatchTarget};

const MIN_POLL_MS: i64 = 30_000;
const MAX_POLL_MS: i64 = 180_000;

pub(super) fn is_github_watch_scheme(v: &str) -> bool {
    v.starts_with("github:")
        || v.starts_with("gh:")
        || v.starts_with("pr:")
        || v.starts_with("check:")
        || v.starts_with("checks:")
        || v.starts_with("https://github.com/")
        || v.starts_with("http://github.com/")
}

pub(super) fn parse_github_watch_spec(raw: &str) -> Option<Result<WatchTarget, String>> {
    let s = raw.trim();
    if let Some(r) = s
        .strip_prefix("https://github.com/")
        .or_else(|| s.strip_prefix("http://github.com/"))
    {
        return Some(parse_pr(r));
    }
    let (had_gh, body) = match s.strip_prefix("github:").or_else(|| s.strip_prefix("gh:")) {
        Some(rest) => (true, rest.trim()),
        None => (false, s),
    };
    if let Some(spec) = body
        .strip_prefix("checks:")
        .or_else(|| body.strip_prefix("check:"))
    {
        return Some(parse_check(spec.trim()));
    }
    let spec = body
        .strip_prefix("pr:")
        .map(str::trim)
        .or(had_gh.then_some(body))?;
    if let Some(pr) = spec
        .strip_suffix(":checks")
        .or_else(|| spec.strip_suffix(":check"))
    {
        return Some(parse_pr(pr.trim()).map(|t| match t {
            WatchTarget::GitHubPr {
                owner,
                repo,
                number,
            } => WatchTarget::GitHubCheck {
                owner,
                repo,
                reference: number.to_string(),
                check_name: None,
            },
            other => other,
        }));
    }
    Some(if spec.contains('@') {
        parse_check(spec)
    } else {
        parse_pr(spec)
    })
}

fn valid_seg(seg: &str) -> bool {
    let s = seg.trim();
    !s.is_empty()
        && s.len() <= 100
        && !matches!(s, "." | "..")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

fn parse_pr(spec: &str) -> Result<WatchTarget, String> {
    let coords = spec
        .split_once('#')
        .and_then(|(rp, n)| {
            rp.split_once('/')
                .map(|(o, r)| (o.trim(), r.trim(), n.trim()))
        })
        .or_else(|| {
            match spec
                .trim_matches('/')
                .split('/')
                .collect::<Vec<_>>()
                .as_slice()
            {
                [o, r, n] | [o, r, "pull" | "pulls", n, ..] => Some((*o, *r, *n)),
                _ => None,
            }
        });
    if let Some((o, r, n)) = coords
        && let Ok(number) = n.parse::<u64>()
        && number > 0
        && valid_seg(o)
        && valid_seg(r)
    {
        return Ok(WatchTarget::GitHubPr {
            owner: o.into(),
            repo: r.into(),
            number,
        });
    }
    Err(format!("invalid GitHub PR watch spec `{spec}`"))
}

fn parse_check(spec: &str) -> Result<WatchTarget, String> {
    let err = || format!("invalid GitHub check watch spec `{spec}`");
    let (repo, tail) = spec
        .split_once('@')
        .or_else(|| spec.split_once('#'))
        .ok_or_else(err)?;
    let (o, r) = repo
        .split_once('/')
        .filter(|(o, r)| valid_seg(o) && valid_seg(r))
        .ok_or_else(err)?;
    let (rf, name) = match tail.split_once('#') {
        Some((rf, n)) if !n.trim().is_empty() => (rf.trim(), Some(n.trim().to_string())),
        Some(_) => return Err("GitHub check name cannot be empty".into()),
        None => (tail.trim(), None),
    };
    let zero_num = rf.chars().all(|c| c.is_ascii_digit()) && rf.parse::<u64>().ok() == Some(0);
    if rf.is_empty() || zero_num || rf.bytes().any(|b| b <= b' ' || b == 0x7f) {
        return Err(err());
    }
    Ok(WatchTarget::GitHubCheck {
        owner: o.trim().into(),
        repo: r.trim().into(),
        reference: rf.into(),
        check_name: name,
    })
}

fn resolve_token(store: &Store, agent_id: &AgentId) -> Option<String> {
    let clean = |s: &str| {
        let t = s.trim();
        (!t.is_empty() && !t.bytes().any(|b| b <= b' ' || b == 0x7f)).then(|| t.to_string())
    };
    let keys = ["GITHUB_TOKEN", "GH_TOKEN"];
    if let Ok(env) = store.resolve_agent_secret_env(agent_id)
        && let Some(tok) = keys
            .into_iter()
            .find_map(|k| env.get(k).and_then(|v| clean(v)))
    {
        return Some(tok);
    }
    keys.into_iter()
        .find_map(|k| std::env::var(k).ok().and_then(|v| clean(&v)))
}

fn api_base() -> String {
    std::env::var("ANYAGENTS_GITHUB_API_BASE")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://api.github.com".into())
}

static HTTP: LazyLock<ureq::Agent> = LazyLock::new(|| {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent()
});

type Obs = (bool, String, Option<String>);

#[derive(Default)]
struct TargetCache {
    etags: HashMap<String, (Option<String>, Value)>,
    last: Option<Obs>,
    next_at: i64,
    interval: i64,
    settled: bool,
}

#[derive(Default)]
struct ProbeCache {
    targets: HashMap<String, TargetCache>,
    limited_until: i64,
}

enum FetchErr {
    RateLimited(i64),
    Auth,
    Other(String),
}

static PROBES: LazyLock<Mutex<ProbeCache>> = LazyLock::new(|| Mutex::new(ProbeCache::default()));

fn target_key(base: &str, t: &WatchTarget) -> Option<String> {
    match t {
        WatchTarget::GitHubPr {
            owner,
            repo,
            number,
        } => Some(format!(
            "{base}:{}/{}:pr:{number}",
            owner.to_ascii_lowercase(),
            repo.to_ascii_lowercase()
        )),
        WatchTarget::GitHubCheck {
            owner,
            repo,
            reference,
            check_name,
        } => Some(format!(
            "{base}:{}/{}:chk:{reference}:{}",
            owner.to_ascii_lowercase(),
            repo.to_ascii_lowercase(),
            check_name.as_deref().unwrap_or("")
        )),
        _ => None,
    }
}

pub(super) fn next_probe_at(target: &WatchTarget, now: i64) -> i64 {
    let Some(key) = target_key(&api_base(), target) else {
        return i64::MAX;
    };
    let Ok(cache) = PROBES.lock() else {
        return now.saturating_add(MIN_POLL_MS);
    };
    let Some(e) = cache.targets.get(&key) else {
        return now.saturating_add(MIN_POLL_MS);
    };
    if e.settled {
        i64::MAX
    } else {
        e.next_at
            .max(cache.limited_until)
            .max(now.saturating_add(1_000))
    }
}

pub(super) fn evaluate_github_watch(
    store: &Store,
    sched: &AgentSchedule,
    target: &WatchTarget,
    now: i64,
) -> WatchEval {
    let base = api_base();
    let tok = resolve_token(store, &sched.agent_id);
    let (terminal, state, head_sha) = probe(&base, tok.as_deref(), target, now);
    match target {
        WatchTarget::GitHubPr { .. } => evaluate_pr_observation(sched, state, head_sha),
        WatchTarget::GitHubCheck { .. } => evaluate_check_observation(sched, terminal, state),
        _ => WatchEval::pending(sched.watch_baseline.clone(), None),
    }
}

fn probe(base: &str, token: Option<&str>, target: &WatchTarget, now: i64) -> Obs {
    let Some(key) = target_key(base, target) else {
        return (false, "invalid_spec".into(), None);
    };
    let Ok(mut cache) = PROBES.lock() else {
        return (false, "probe_error:cache".into(), None);
    };
    if cache.targets.len() > 512 {
        cache.targets.clear();
    }
    if now < cache.limited_until {
        return (false, "probe_error:rate_limited".into(), None);
    }
    let entry = cache.targets.entry(key).or_default();
    if (entry.settled || now < entry.next_at)
        && let Some(obs) = &entry.last
    {
        return obs.clone();
    }
    let mut pause = 0i64;
    let res = read_target(entry, base, token, target, now, &mut pause);
    cache.limited_until = cache.limited_until.max(pause);
    match res {
        Ok(obs) => {
            let iv = if entry.last.as_ref() == Some(&obs) {
                (entry.interval.max(MIN_POLL_MS) * 2).clamp(MIN_POLL_MS, MAX_POLL_MS)
            } else {
                MIN_POLL_MS
            };
            entry.interval = iv;
            let term = matches!(obs.1.as_str(), "merged" | "closed");
            entry.settled = match target {
                WatchTarget::GitHubPr { .. } => term,
                WatchTarget::GitHubCheck { reference, .. } => {
                    obs.0
                        && (term
                            || (reference.len() == 40
                                && reference.bytes().all(|b| b.is_ascii_hexdigit())))
                }
                _ => false,
            };
            entry.last = Some(obs.clone());
            entry.next_at = now.saturating_add(iv);
            obs
        }
        Err(FetchErr::RateLimited(until)) => {
            cache.limited_until = cache
                .limited_until
                .max(until)
                .max(now.saturating_add(MIN_POLL_MS));
            (false, "probe_error:rate_limited".into(), None)
        }
        Err(FetchErr::Auth) => {
            entry.next_at = now.saturating_add(MIN_POLL_MS * 2);
            (false, "probe_error:auth".into(), None)
        }
        Err(FetchErr::Other(desc)) => {
            entry.next_at = now.saturating_add(MIN_POLL_MS);
            (false, desc, None)
        }
    }
}

fn read_target(
    entry: &mut TargetCache,
    base: &str,
    token: Option<&str>,
    target: &WatchTarget,
    now: i64,
    pause: &mut i64,
) -> Result<Obs, FetchErr> {
    let bad = || FetchErr::Other("probe_error:payload".into());
    let (owner, repo, is_pr, reference, check_name) = match target {
        WatchTarget::GitHubPr {
            owner,
            repo,
            number,
        } => (owner, repo, true, number.to_string(), None),
        WatchTarget::GitHubCheck {
            owner,
            repo,
            reference,
            check_name,
        } => {
            let c = reference.trim_start_matches('#').to_string();
            (
                owner,
                repo,
                c.parse::<u64>().is_ok(),
                c,
                check_name.as_deref(),
            )
        }
        _ => return Err(bad()),
    };
    let root = format!("repos/{owner}/{repo}");
    let mut fetch = |p: &str| get_json(entry, base, token, p, now, pause);
    let head = if is_pr {
        let j = fetch(&format!("{root}/pulls/{reference}"))?;
        let merged = j["merged"].as_bool().unwrap_or(false) || !j["merged_at"].is_null();
        let st = if merged {
            "merged".into()
        } else {
            j["state"].as_str().ok_or_else(bad)?.to_ascii_lowercase()
        };
        let sha = j["head"]["sha"].as_str().unwrap_or_default().to_string();
        if st == "merged" || st == "closed" {
            return Ok((true, st, (!sha.is_empty()).then_some(sha)));
        }
        if sha.is_empty() {
            return Err(bad());
        }
        sha
    } else {
        reference
    };
    let eh = url_encode(&head);
    let mut runs: Vec<(String, bool, bool)> = Vec::new();
    for page in 1..=10 {
        let j = fetch(&format!(
            "{root}/commits/{eh}/check-runs?per_page=100&page={page}"
        ))?;
        let arr = j["check_runs"].as_array().ok_or_else(bad)?;
        for r in arr {
            let done = r["status"].as_str() == Some("completed");
            let c = r["conclusion"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase();
            let fail = done
                && matches!(
                    c.as_str(),
                    "failure"
                        | "error"
                        | "timed_out"
                        | "cancelled"
                        | "action_required"
                        | "startup_failure"
                );
            runs.push((r["name"].as_str().unwrap_or_default().into(), done, fail));
        }
        let total = j["total_count"].as_u64().unwrap_or(runs.len() as u64);
        if arr.len() < 100 || (runs.len() as u64) >= total {
            break;
        }
    }
    if check_name.is_none() {
        let status_json = fetch(&format!("{root}/commits/{eh}/status"))?;
        if let Some(sts) = status_json["statuses"].as_array() {
            for st in sts {
                let s = st["state"].as_str().unwrap_or_default();
                let fail = s.eq_ignore_ascii_case("failure") || s.eq_ignore_ascii_case("error");
                runs.push((
                    st["context"].as_str().unwrap_or_default().into(),
                    !s.eq_ignore_ascii_case("pending"),
                    fail,
                ));
            }
        }
    }
    let f: Vec<_> = runs
        .iter()
        .filter(|(n, _, _)| check_name.is_none_or(|w| n == w))
        .collect();
    let (pend, fail, rev) = (
        f.iter().any(|(_, d, _)| !d),
        f.iter().any(|(_, _, fl)| *fl),
        f.iter().any(|(n, _, fl)| *fl && n == "review"),
    );
    Ok(if matches!(target, WatchTarget::GitHubPr { .. }) {
        let s = match (f.is_empty(), pend, rev, fail) {
            (true, _, _, _) => "open",
            (_, true, _, _) => "checks_in_progress",
            (_, _, true, _) => "review_failed",
            (_, _, _, true) => "checks_failed",
            _ => "checks_completed",
        };
        (false, s.into(), Some(head))
    } else {
        let (t, s) = match (f.is_empty(), pend, fail) {
            (true, _, _) => (false, "no_checks_yet"),
            (_, true, _) => (false, "pending"),
            (_, _, true) => (true, "failure"),
            _ => (true, "success"),
        };
        (t, s.into(), Some(head))
    })
}

fn get_json(
    entry: &mut TargetCache,
    base: &str,
    token: Option<&str>,
    path: &str,
    now: i64,
    pause: &mut i64,
) -> Result<Value, FetchErr> {
    if *pause > now {
        return Err(FetchErr::RateLimited(*pause));
    }
    let cached = entry.etags.get(path).cloned();
    let url = format!("{}/{path}", base.trim_end_matches('/'));
    let mut req = HTTP
        .get(&url)
        .header("accept", "application/vnd.github+json")
        .header("user-agent", "anyagents-github-watch/1.0")
        .header("x-github-api-version", "2022-11-28");
    if let Some(tok) = token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    if let Some((Some(etag), _)) = &cached {
        req = req.header("if-none-match", etag);
    }
    fn tx_err<E>(_: E) -> FetchErr {
        FetchErr::Other("probe_error:transport".into())
    }
    let mut resp = req.call().map_err(tx_err)?;
    let status = resp.status().as_u16();
    let hdr = |k: &str| {
        resp.headers()
            .get(k)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let (etag_h, ra_h) = (hdr("etag"), hdr("retry-after"));
    let rem_h = hdr("x-ratelimit-remaining").and_then(|v| v.parse::<u64>().ok());
    let reset_h = hdr("x-ratelimit-reset").and_then(|v| v.parse::<i64>().ok());
    let body = resp.body_mut().read_to_string().map_err(tx_err)?;
    let until = || rate_limit_until(ra_h.as_deref(), reset_h, now);
    if rem_h.is_some_and(|r| r < 100) {
        *pause = (*pause).max(until());
    }
    if status == 304
        && let Some((_, val)) = cached
    {
        return Ok(val);
    }
    let rl = ra_h.is_some() || rem_h == Some(0) || body.to_ascii_lowercase().contains("rate limit");
    if status == 429 || (status == 403 && rl) {
        return Err(FetchErr::RateLimited(until()));
    }
    if status == 401 || status == 403 {
        return Err(FetchErr::Auth);
    }
    if !(200..300).contains(&status) {
        return Err(FetchErr::Other(format!("probe_error:http:{status}")));
    }
    let val: Value =
        serde_json::from_str(&body).map_err(|_| FetchErr::Other("probe_error:payload".into()))?;
    entry.etags.insert(path.into(), (etag_h, val.clone()));
    Ok(val)
}

fn rate_limit_until(retry_after: Option<&str>, reset_secs: Option<i64>, now: i64) -> i64 {
    if let Some(ra) = retry_after {
        if let Ok(s) = ra.trim().parse::<i64>() {
            return now.saturating_add(s.max(0).saturating_mul(1_000));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(ra.trim()) {
            return dt.timestamp_millis();
        }
    }
    reset_secs
        .map(|s| s.saturating_mul(1_000))
        .filter(|&ms| ms > now)
        .unwrap_or_else(|| now.saturating_add(MIN_POLL_MS))
}

fn url_encode(v: &str) -> String {
    v.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn fired(next_state: &'static str, state: String, baseline: Option<String>) -> WatchEval {
    WatchEval {
        triggered: true,
        next_state,
        transition: Some(state.clone()),
        observed: Some(state),
        baseline,
    }
}

pub(super) fn evaluate_pr_observation(
    sched: &AgentSchedule,
    state: String,
    head_sha: Option<String>,
) -> WatchEval {
    if state.starts_with("probe_error:") {
        return WatchEval::pending(sched.watch_baseline.clone(), Some(state));
    }
    if state == "merged" || state == "closed" {
        return fired("fired", state.clone(), Some(state));
    }
    let (raw_prev, prev_head) = match sched.watch_baseline.as_deref() {
        Some(raw) => match raw.split_once('@') {
            Some((s, h)) if !h.is_empty() => (s, Some(h)),
            _ => (raw, None),
        },
        None => ("pending", None),
    };
    let next_base = match head_sha.as_deref().filter(|h| !h.is_empty()).or(prev_head) {
        Some(h) => format!("{state}@{h}"),
        None => state.clone(),
    };
    let changed = prev_head
        .zip(head_sha.as_deref())
        .is_some_and(|(a, b)| a != b);
    let prev = if changed { "pending" } else { raw_prev };
    if matches!(
        state.as_str(),
        "review_failed" | "checks_failed" | "checks_completed"
    ) && prev != state
    {
        return fired("active", state, Some(next_base));
    }
    WatchEval::pending(Some(next_base), Some(state))
}

pub(super) fn evaluate_check_observation(
    sched: &AgentSchedule,
    is_terminal: bool,
    state: String,
) -> WatchEval {
    if is_terminal {
        fired("fired", state, sched.watch_baseline.clone())
    } else {
        WatchEval::pending(sched.watch_baseline.clone(), Some(state))
    }
}

#[cfg(test)]
pub(crate) fn reset_probes_for_test() {
    if let Ok(mut cache) = PROBES.lock() {
        *cache = ProbeCache::default();
    }
}

#[cfg(test)]
pub(crate) struct RestoreEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);
#[cfg(test)]
impl RestoreEnv {
    pub(crate) fn capture(keys: &[&'static str]) -> Self {
        Self(keys.iter().map(|&k| (k, std::env::var_os(k))).collect())
    }
}
#[cfg(test)]
impl Drop for RestoreEnv {
    fn drop(&mut self) {
        for (k, v) in &self.0 {
            // SAFETY: held under `crate::server::tests::env_lock()`.
            unsafe {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) type MockGitHubHandler = std::sync::Arc<
    dyn Fn(&str, &HashMap<String, String>) -> (u16, Option<(&'static str, &'static str)>, String)
        + Send
        + Sync,
>;

#[cfg(test)]
pub(crate) fn spawn_mock_github(
    handler: MockGitHubHandler,
) -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        for mut s in listener.incoming().flatten() {
            let mut buf = [0u8; 8192];
            let Ok(n) = s.read(&mut buf) else { break };
            if n == 0 {
                continue;
            }
            let raw = String::from_utf8_lossy(&buf[..n]);
            let mut ls = raw.lines();
            let path = ls
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            if path == "/__shutdown" {
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                break;
            }
            let hdrs = ls
                .take_while(|l| !l.is_empty())
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().into()))
                .collect();
            let (st, rh, body) = handler(path, &hdrs);
            let ex = rh.map(|(k, v)| format!("{k}: {v}\r\n")).unwrap_or_default();
            let out = format!(
                "HTTP/1.1 {st} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{ex}\r\n{body}",
                body.len()
            );
            let _ = s.write_all(out.as_bytes());
        }
    });
    (format!("http://{addr}"), handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy(spec: &str) -> AgentSchedule {
        AgentSchedule {
            id: "sch_gh".into(),
            agent_id: AgentId::generate(),
            kind: "watch".into(),
            spec: spec.into(),
            prompt: "check".into(),
            interval_ms: None,
            next_fire_at_ms: None,
            last_fired_at_ms: None,
            watch_baseline: Some("pending".into()),
            last_observed: Some("pending".into()),
            state: "active".into(),
            created_at_ms: 1_000,
        }
    }

    #[test]
    fn parses_github_specs_and_evaluates_transitions() {
        let p = |s| parse_github_watch_spec(s).unwrap().unwrap();
        let pr = |o: &str, r: &str, n| WatchTarget::GitHubPr {
            owner: o.into(),
            repo: r.into(),
            number: n,
        };
        let ck = |o: &str, r: &str, rf: &str, cn: Option<&str>| WatchTarget::GitHubCheck {
            owner: o.into(),
            repo: r.into(),
            reference: rf.into(),
            check_name: cn.map(str::to_string),
        };
        assert_eq!(
            p("github:pr:SylphxAI/anyagents#408"),
            pr("SylphxAI", "anyagents", 408)
        );
        assert_eq!(
            p("https://github.com/octocat/hello/pull/12"),
            pr("octocat", "hello", 12)
        );
        assert_eq!(
            p("github:pr:octocat/hello#12:checks"),
            ck("octocat", "hello", "12", None)
        );
        assert_eq!(
            p("github:checks:octocat/hello@main#ci"),
            ck("octocat", "hello", "main", Some("ci"))
        );
        for b in [
            "github:pr:a/b#0",
            "github:pr:../b#1",
            "github:check:a/b@",
            "github:check:a/b@m#",
        ] {
            assert!(parse_github_watch_spec(b).unwrap().is_err(), "{b}");
        }
        assert!(parse_github_watch_spec("webhook:deploy").is_none());

        let mut s = dummy("github:pr:o/r#7");
        let e = evaluate_pr_observation(&s, "open".into(), Some("s1".into()));
        assert!(!e.triggered && e.baseline.as_deref() == Some("open@s1"));
        s.watch_baseline = e.baseline;
        let e = evaluate_pr_observation(&s, "checks_completed".into(), Some("s1".into()));
        assert!(e.triggered && e.next_state == "active");
        s.watch_baseline = e.baseline;
        assert!(
            !evaluate_pr_observation(&s, "checks_completed".into(), Some("s1".into())).triggered
        );
        assert!(evaluate_pr_observation(&s, "checks_failed".into(), Some("s2".into())).triggered);
        for t in ["merged", "closed"] {
            assert_eq!(
                evaluate_pr_observation(&s, t.into(), Some("s2".into())).next_state,
                "fired"
            );
        }
        let c = dummy("github:check:o/r@s1");
        assert!(!evaluate_check_observation(&c, false, "pending".into()).triggered);
        for t in ["success", "failure"] {
            assert_eq!(
                evaluate_check_observation(&c, true, t.into()).next_state,
                "fired"
            );
        }
    }
}
