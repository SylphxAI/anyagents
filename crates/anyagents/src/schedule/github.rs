//! REST-only, process-wide GitHub watch observations. The mutex is an intentional
//! single-flight/serial request gate (not a database lock): GitHub recommends
//! serial requests to avoid secondary limits. Cached pages retain summaries, not
//! thousands of full Check Run objects.
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use serde_json::Value;

const MIN_INTERVAL: i64 = 60_000;
const MAX_INTERVAL: i64 = 300_000;
const MAX_PAGES: usize = 1_000;

type Observation = (bool, String);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Checks {
    count: usize,
    pending: bool,
    review_failed: bool,
}

impl Checks {
    fn add(&mut self, other: &Self) {
        self.count += other.count;
        self.pending |= other.pending;
        self.review_failed |= other.review_failed;
    }

    fn observation(&self, pr: bool) -> Observation {
        let state = if pr && self.review_failed {
            "review_failed"
        } else if self.count == 0 {
            if pr { "open" } else { "no_checks_yet" }
        } else if self.pending {
            "checks_in_progress"
        } else {
            "checks_completed"
        };
        (state == "checks_completed", state.to_string())
    }
}

#[derive(Clone)]
enum Summary {
    Pull { state: String, head: String },
    Checks(Checks),
}

#[derive(Clone)]
struct Page {
    etag: Option<String>,
    next: bool,
    summary: Summary,
    used_at: i64,
}

#[derive(Default)]
struct Target {
    next_at: i64,
    interval: i64,
    result: Option<Observation>,
    // Last successful observation survives transport/rate errors.
    successful: Option<Observation>,
    pages: HashMap<String, Page>,
}

#[derive(Default)]
struct Probes {
    targets: HashMap<String, Target>,
    limited_until: i64,
    limit_interval: i64,
}

static PROBES: LazyLock<Mutex<Probes>> = LazyLock::new(|| Mutex::new(Probes::default()));

struct Response {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

#[derive(Debug)]
enum Error {
    RateLimited(i64),
    Transport,
    Http(u16),
    Payload,
}

impl Error {
    fn observation(&self) -> Observation {
        let reason = match self {
            Self::RateLimited(_) => "rate_limited".to_string(),
            Self::Transport => "transport".to_string(),
            Self::Http(code) => format!("http:{code}"),
            Self::Payload => "payload".to_string(),
        };
        (false, format!("probe_error:{reason}"))
    }
}

pub(super) fn probe(
    owner: &str,
    repo: &str,
    reference: &str,
    pr: bool,
    agent_id: &str,
    now: i64,
) -> Option<Observation> {
    let base = std::env::var("ANYAGENTS_GITHUB_API_BASE").unwrap_or_default();
    let key = format!(
        "{base}:{}:{}/{}:{reference}",
        if pr { "pr" } else { "check" },
        owner.to_ascii_lowercase(),
        repo.to_ascii_lowercase()
    );
    let mut probes = PROBES.lock().ok()?;
    Some(
        probes.probe(&key, owner, repo, reference, pr, now, |path, etag| {
            request(&base, path, etag, agent_id)
        }),
    )
}

impl Probes {
    #[allow(clippy::too_many_arguments)]
    fn probe(
        &mut self,
        key: &str,
        owner: &str,
        repo: &str,
        reference: &str,
        pr: bool,
        now: i64,
        mut request: impl FnMut(&str, Option<&str>) -> Result<Response, Error>,
    ) -> Observation {
        if now < self.limited_until {
            return Error::RateLimited(self.limited_until).observation();
        }
        let target = self.targets.entry(key.to_string()).or_default();
        // A cooldown overrides the normal interval. Always re-read on recovery,
        // even when the last ordinary observation had a five-minute TTL.
        let recovering = self.limited_until != 0 && target.next_at <= self.limited_until;
        if !recovering
            && now < target.next_at
            && let Some(result) = &target.result
        {
            return result.clone();
        }
        let root = format!("repos/{}/{}", encode(owner), encode(repo));
        let result = read_target(target, &root, reference, pr, now, &mut request);
        match result {
            Ok(observation) => {
                target.pages.retain(|_, page| page.used_at == now);
                target.interval = if target.successful.as_ref() == Some(&observation) {
                    (target.interval * 2).clamp(MIN_INTERVAL, MAX_INTERVAL)
                } else {
                    MIN_INTERVAL
                };
                target.successful = Some(observation.clone());
                target.result = Some(observation.clone());
                target.next_at = now.saturating_add(target.interval);
                self.limit_interval = 0;
                observation
            }
            Err(error) => {
                if let Error::RateLimited(until) = error {
                    self.limit_interval =
                        (self.limit_interval * 2).clamp(MIN_INTERVAL, MAX_INTERVAL);
                    self.limited_until = until.max(now.saturating_add(self.limit_interval));
                    // All retained targets must re-read after the cooldown.
                    for target in self.targets.values_mut() {
                        target.next_at = self.limited_until;
                    }
                } else {
                    target.next_at = now.saturating_add(MIN_INTERVAL);
                    target.result = Some(error.observation());
                }
                error.observation()
            }
        }
    }
}

fn read_target(
    target: &mut Target,
    root: &str,
    reference: &str,
    pr: bool,
    now: i64,
    request: &mut impl FnMut(&str, Option<&str>) -> Result<Response, Error>,
) -> Result<Observation, Error> {
    let head = if pr {
        let path = format!("{root}/pulls/{reference}");
        let page = read_page(target, &path, now, request)?;
        let Summary::Pull { state, head } = page.summary else {
            return Err(Error::Payload);
        };
        if state == "merged" || state == "closed" {
            return Ok((true, state));
        }
        if head.is_empty() {
            return Err(Error::Payload);
        }
        head
    } else {
        reference.to_string()
    };
    let mut checks = read_checks(
        target,
        &format!("{root}/commits/{}/check-runs", encode(&head)),
        now,
        request,
    )?;
    if pr {
        checks.add(&read_checks(
            target,
            &format!("{root}/commits/{}/status", encode(&head)),
            now,
            request,
        )?);
    }
    Ok(checks.observation(pr))
}

fn read_checks(
    target: &mut Target,
    endpoint: &str,
    now: i64,
    request: &mut impl FnMut(&str, Option<&str>) -> Result<Response, Error>,
) -> Result<Checks, Error> {
    let mut checks = Checks::default();
    for n in 1..=MAX_PAGES {
        let path = format!("{endpoint}?per_page=100&page={n}");
        let page = read_page(target, &path, now, request)?;
        let Summary::Checks(summary) = page.summary else {
            return Err(Error::Payload);
        };
        checks.add(&summary);
        if !page.next {
            return Ok(checks);
        }
    }
    Err(Error::Payload) // Never report completion from a truncated population.
}

fn read_page(
    target: &mut Target,
    path: &str,
    now: i64,
    request: &mut impl FnMut(&str, Option<&str>) -> Result<Response, Error>,
) -> Result<Page, Error> {
    let cached = target.pages.get(path);
    let response = request(path, cached.and_then(|page| page.etag.as_deref()))?;
    if response.status == 304 {
        let mut page = cached.cloned().ok_or(Error::Payload)?;
        page.used_at = now;
        target.pages.insert(path.to_string(), page.clone());
        return Ok(page);
    }
    if response.status == 403 || response.status == 429 {
        let secondary = response.body.to_ascii_lowercase();
        let retry = response.headers.get("retry-after");
        if retry.is_some()
            || response.status == 429
            || secondary.contains("secondary rate limit")
            || secondary.contains("abuse detection")
            || response
                .headers
                .get("x-ratelimit-remaining")
                .is_some_and(|v| v == "0")
        {
            let until = retry
                .and_then(|v| v.parse::<i64>().ok())
                .map(|s| now.saturating_add(s.saturating_mul(1_000)))
                .or_else(|| {
                    response
                        .headers
                        .get("x-ratelimit-reset")
                        .and_then(|v| v.parse::<i64>().ok())
                        .map(|s| s.saturating_mul(1_000))
                })
                .unwrap_or_else(|| now.saturating_add(MIN_INTERVAL));
            return Err(Error::RateLimited(until));
        }
    }
    if response.status != 200 {
        return Err(Error::Http(response.status));
    }
    let json: Value = serde_json::from_str(&response.body).map_err(|_| Error::Payload)?;
    let summary = if path.contains("/pulls/") {
        let state = json
            .get("state")
            .and_then(Value::as_str)
            .ok_or(Error::Payload)?;
        Summary::Pull {
            state: if json.get("merged").and_then(Value::as_bool) == Some(true)
                || json.get("merged_at").is_some_and(|v| !v.is_null())
            {
                "merged".to_string()
            } else {
                state.to_ascii_lowercase()
            },
            head: json
                .pointer("/head/sha")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        }
    } else {
        let runs = json
            .get("check_runs")
            .or_else(|| json.get("statuses"))
            .and_then(Value::as_array)
            .ok_or(Error::Payload)?;
        let mut checks = Checks {
            count: runs.len(),
            ..Checks::default()
        };
        for item in runs {
            checks.pending |= !super::check_item_is_terminal(item);
            let name = item
                .get("name")
                .or_else(|| item.get("context"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let conclusion = item
                .get("conclusion")
                .or_else(|| item.get("state"))
                .and_then(Value::as_str)
                .unwrap_or("");
            checks.review_failed |= name.to_ascii_lowercase().contains("review")
                && matches!(
                    conclusion.to_ascii_uppercase().as_str(),
                    "FAILURE" | "ERROR" | "TIMED_OUT" | "ACTION_REQUIRED"
                );
        }
        Summary::Checks(checks)
    };
    let page = Page {
        etag: response.headers.get("etag").cloned(),
        next: response
            .headers
            .get("link")
            .is_some_and(|v| v.split(',').any(|link| link.contains("rel=\"next\""))),
        summary,
        used_at: now,
    };
    target.pages.insert(path.to_string(), page.clone());
    Ok(page)
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn request(base: &str, path: &str, etag: Option<&str>, agent_id: &str) -> Result<Response, Error> {
    if !base.is_empty() {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .max_redirects(0)
            .http_status_as_error(false)
            .build()
            .new_agent();
        let mut req = agent
            .get(format!("{}/{path}", base.trim_end_matches('/')))
            .header("accept", "application/vnd.github+json");
        if let Some(etag) = etag {
            req = req.header("if-none-match", etag);
        }
        let mut response = req.call().map_err(|_| Error::Transport)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                Some((
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().ok()?.to_string(),
                ))
            })
            .collect();
        let body = response
            .body_mut()
            .read_to_string()
            .map_err(|_| Error::Transport)?;
        return Ok(Response {
            status,
            headers,
            body,
        });
    }
    // Keep authentication in gh's authorized credential adapter; never read a
    // token into arguments, retained output, or the watch cache. No GraphQL.
    let mut command = std::process::Command::new("timeout");
    command.env("ANYAGENTS_AGENT_ID", agent_id).args([
        "15s",
        "gh",
        "api",
        "--include",
        "--method",
        "GET",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "X-GitHub-Api-Version: 2022-11-28",
    ]);
    if let Some(etag) = etag {
        command.args(["-H", &format!("If-None-Match: {etag}")]);
    }
    let output = command.arg(path).output().map_err(|_| Error::Transport)?;
    // gh exits nonzero for 403/429; stdout still carries HTTP headers + body.
    parse_response(&output.stdout)
}

fn parse_response(bytes: &[u8]) -> Result<Response, Error> {
    let text = std::str::from_utf8(bytes).map_err(|_| Error::Payload)?;
    let (headers, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or(Error::Transport)?;
    let mut lines = headers.lines();
    let status = lines
        .next()
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Transport)?;
    let headers = lines
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Ok(Response {
        status,
        headers,
        body: body.to_string(),
    })
}
