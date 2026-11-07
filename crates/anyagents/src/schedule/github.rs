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
    head: Option<String>,
    accessed_at: i64,
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
        self.targets
            .retain(|_, target| now.saturating_sub(target.accessed_at) < 3_600_000);
        let target = self.targets.entry(key.to_string()).or_default();
        target.accessed_at = now;
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
    if target.head.as_deref() != Some(&head) {
        target.pages.retain(|path, _| path.contains("/pulls/"));
        target.head = Some(head.clone());
    }
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
                .and_then(|v| retry_after(v, now))
                .or_else(|| {
                    // Secondary limits have an independent primary reset
                    // header, often an hour away. Use it only for primary
                    // exhaustion, never as the secondary cooldown.
                    if response
                        .headers
                        .get("x-ratelimit-remaining")
                        .is_none_or(|v| v != "0")
                    {
                        return None;
                    }
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

fn retry_after(raw: &str, now: i64) -> Option<i64> {
    if let Ok(seconds) = raw.parse::<i64>() {
        return Some(now.saturating_add(seconds.max(0).saturating_mul(1_000)));
    }
    // HTTP-date is permitted by Retry-After as well as delta-seconds.
    let parts: Vec<_> = raw.split_whitespace().collect();
    if parts.len() != 6 || parts[5] != "GMT" {
        return None;
    }
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|month| *month == parts[2])?
        + 1;
    super::parse_rfc3339_ms(&format!(
        "{}-{month:02}-{}T{}Z",
        parts[3], parts[1], parts[4]
    ))
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
    gh_command(path, etag, agent_id)
        .output()
        .map_err(|_| Error::Transport)
        .and_then(|output| parse_response(&output.stdout))
}

fn gh_command(path: &str, etag: Option<&str>, agent_id: &str) -> std::process::Command {
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
        // Own revalidation at this boundary, including cold starts. An outer
        // gh cache may otherwise restore a body under 304 headers without our
        // pagination metadata. Custom headers keep gh adapters in passthrough.
        "-H",
        "Cache-Control: no-cache",
    ]);
    if let Some(etag) = etag {
        command.args(["-H", &format!("If-None-Match: {etag}")]);
    }
    command.arg(path);
    // gh exits nonzero for 403/429; stdout still carries HTTP headers + body.
    command
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn response(status: u16, body: Value, headers: &[(&str, &str)]) -> Response {
        Response {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.to_string(),
        }
    }

    fn completed() -> Value {
        serde_json::from_str(include_str!("fixtures/completed-check-runs.json")).unwrap()
    }

    fn pending() -> Value {
        serde_json::from_str(include_str!("fixtures/pending-check-runs.json")).unwrap()
    }

    fn pull(state: &str, merged: bool, sha: &str) -> Value {
        json!({"state": state, "merged": merged, "merged_at": null, "head": {"sha": sha}})
    }

    #[test]
    fn conditional_pages_adapt_and_recover_after_changes() {
        let mut probes = Probes::default();
        let mut calls = 0;
        let expected = [
            (0, 60_000),
            (60_000, 120_000),
            (180_000, 240_000),
            (420_000, 300_000),
            (720_000, 300_000),
        ];
        for (now, interval) in expected {
            let result = probes.probe("target", "o", "r", "sha", false, now, |path, etag| {
                assert_eq!(path, "repos/o/r/commits/sha/check-runs?per_page=100&page=1");
                calls += 1;
                if now == 0 {
                    assert_eq!(etag, None);
                    Ok(response(200, pending(), &[("etag", "\"pending\"")]))
                } else {
                    assert_eq!(etag, Some("\"pending\""));
                    Ok(response(304, Value::Null, &[]))
                }
            });
            assert_eq!(result, (false, "checks_in_progress".to_string()));
            assert_eq!(probes.targets["target"].interval, interval);
            for offset in [0, 1, interval - 1] {
                assert_eq!(
                    probes.probe(
                        "target",
                        "o",
                        "r",
                        "sha",
                        false,
                        now + offset,
                        |_, _| panic!("duplicate request")
                    ),
                    result
                );
            }
        }
        assert_eq!(calls, 5);
        assert_eq!(
            probes.probe("target", "o", "r", "sha", false, 1_020_000, |_, etag| {
                assert_eq!(etag, Some("\"pending\""));
                Ok(response(200, completed(), &[("etag", "\"done\"")]))
            }),
            (true, "checks_completed".to_string())
        );
        assert_eq!(probes.targets["target"].interval, MIN_INTERVAL);
    }

    #[test]
    fn three_thousand_checks_revalidate_every_page_and_never_complete_early() {
        let mut probes = Probes::default();
        let template = completed()["check_runs"][0].clone();
        for (now, done) in [(0, false), (60_000, false), (180_000, true)] {
            let mut pages = 0;
            let result = probes.probe("pr", "o", "r", "42", true, now, |path, etag| {
                pages += 1;
                if path.ends_with("/pulls/42") {
                    return Ok(if now == 0 {
                        response(200, pull("open", false, "head"), &[("etag", "pull")])
                    } else {
                        assert_eq!(etag, Some("pull"));
                        response(304, Value::Null, &[])
                    });
                }
                if path.contains("/status?") {
                    return Ok(response(200, json!({"statuses":[]}), &[]));
                }
                let n: usize = path.rsplit_once("page=").unwrap().1.parse().unwrap();
                assert!((1..=30).contains(&n));
                if now != 0 {
                    assert_eq!(etag, Some(format!("page{n}").as_str()));
                }
                if now != 0 && !(done && n == 30) {
                    return Ok(response(304, Value::Null, &[]));
                }
                let mut runs = vec![template.clone(); 100];
                for (i, run) in runs.iter_mut().enumerate() {
                    run["id"] = json!((n - 1) * 100 + i);
                }
                if n == 30 && !done {
                    runs[99]["status"] = json!("in_progress");
                    runs[99]["conclusion"] = Value::Null;
                }
                let mut headers = vec![("etag".to_string(), format!("page{n}"))];
                if n < 30 {
                    headers.push((
                        "link".to_string(),
                        format!(
                            "<https://api.github.com/ignored?page={}>; rel=\"next\"",
                            n + 1
                        ),
                    ));
                }
                Ok(Response {
                    status: 200,
                    headers: headers.into_iter().collect(),
                    body: json!({"total_count":3000,"check_runs":runs}).to_string(),
                })
            });
            assert_eq!(pages, 32); // pull + 30 check pages + legacy commit statuses
            assert_eq!(
                result,
                (
                    done,
                    if done {
                        "checks_completed"
                    } else {
                        "checks_in_progress"
                    }
                    .to_string()
                )
            );
            assert_eq!(probes.targets["pr"].pages.len(), 32);
        }
    }

    #[test]
    fn rate_limit_is_global_and_recovery_rereads_without_losing_the_baseline() {
        let mut probes = Probes::default();
        let first = probes.probe("pr", "o", "r", "42", true, 0, |path, _| {
            Ok(response(
                200,
                if path.contains("/pulls/") {
                    pull("open", false, "head")
                } else if path.contains("/status?") {
                    json!({"statuses":[]})
                } else {
                    pending()
                },
                &[("etag", "before")],
            ))
        });
        assert_eq!(first.1, "checks_in_progress");
        assert_eq!(
            probes
                .probe("other", "o", "r", "sha", false, 1_000, |_, _| {
                    Ok(response(
                        403,
                        json!({"message":"You have exceeded a secondary rate limit."}),
                        &[("retry-after", "180")],
                    ))
                })
                .1,
            "probe_error:rate_limited"
        );
        assert_eq!(probes.limited_until, 181_000);
        for now in [1_001, 60_000, 180_999] {
            for key in ["pr", "other", "new-target"] {
                assert_eq!(
                    probes
                        .probe(key, "o", "r", "42", true, now, |_, _| panic!(
                            "global cooldown bypassed"
                        ))
                        .1,
                    "probe_error:rate_limited"
                );
            }
        }
        assert_eq!(
            probes.targets["pr"].successful.as_ref().unwrap().1,
            "checks_in_progress"
        );
        assert_eq!(
            probes.probe("pr", "o", "r", "42", true, 181_000, |path, etag| {
                assert!(path.ends_with("/pulls/42"));
                assert_eq!(etag, Some("before"));
                Ok(response(
                    200,
                    pull("closed", true, "head"),
                    &[("etag", "after")],
                ))
            }),
            (true, "merged".to_string())
        );
        assert_eq!(probes.targets["pr"].pages.len(), 1);
        assert_eq!(probes.targets["pr"].interval, MIN_INTERVAL);
    }

    #[test]
    fn repeated_secondary_limits_back_off_and_permission_errors_do_not() {
        let mut probes = Probes::default();
        let mut now = 0;
        for delay in [60_000, 120_000, 240_000, 300_000, 300_000] {
            assert_eq!(
                probes
                    .probe("a", "o", "r", "sha", false, now, |_, _| {
                        Ok(response(
                            429,
                            json!({"message":"secondary rate limit"}),
                            &[],
                        ))
                    })
                    .1,
                "probe_error:rate_limited"
            );
            assert_eq!(probes.limited_until, now + delay);
            now += delay;
        }
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 0, |_, _| {
                    Ok(response(
                        403,
                        json!({"message":"Resource not accessible by integration"}),
                        &[],
                    ))
                })
                .1,
            "probe_error:http:403"
        );
        assert_eq!(probes.limited_until, 0);
        assert_eq!(
            probes
                .probe("b", "o", "r", "sha", false, 0, |_, _| Ok(response(
                    200,
                    completed(),
                    &[]
                )))
                .1,
            "checks_completed"
        );
    }

    #[test]
    fn status_contexts_and_review_failures_preserve_pr_semantics() {
        for (run, statuses, expected) in [
            (
                completed(),
                json!([{"context":"deploy","state":"pending"}]),
                "checks_in_progress",
            ),
            (
                completed(),
                json!([{"context":"review","state":"failure"}]),
                "review_failed",
            ),
            (
                json!({"check_runs":[{"name":"review","status":"completed","conclusion":"timed_out"}]}),
                json!([]),
                "review_failed",
            ),
            (
                json!({"check_runs":[]}),
                json!([{"context":"ci","state":"error"}]),
                "checks_completed",
            ),
            (json!({"check_runs":[]}), json!([]), "open"),
        ] {
            let mut probes = Probes::default();
            assert_eq!(
                probes
                    .probe("pr", "o", "r", "42", true, 0, |path, _| Ok(response(
                        200,
                        if path.contains("/pulls/") {
                            pull("open", false, "head")
                        } else if path.contains("/status?") {
                            json!({"statuses":statuses})
                        } else {
                            run.clone()
                        },
                        &[]
                    )))
                    .1,
                expected
            );
        }
        for (state, merged, expected) in [("closed", false, "closed"), ("closed", true, "merged")] {
            let mut probes = Probes::default();
            let mut calls = 0;
            assert_eq!(
                probes.probe("pr", "o", "r", "42", true, 0, |path, _| {
                    calls += 1;
                    assert!(path.contains("/pulls/"));
                    Ok(response(200, pull(state, merged, "head"), &[]))
                }),
                (true, expected.to_string())
            );
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn malformed_or_partial_pages_never_report_completion() {
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 0, |_, _| Ok(response(
                    304,
                    Value::Null,
                    &[]
                )))
                .1,
            "probe_error:payload"
        );
        assert_eq!(
            probes
                .probe("b", "o", "r", "sha", false, 0, |path, _| {
                    Ok(if path.ends_with("page=1") {
                        response(200, completed(), &[("link", "<x>; rel=\"next\"")])
                    } else {
                        response(200, json!({"message":"bad payload"}), &[])
                    })
                })
                .1,
            "probe_error:payload"
        );
    }

    #[test]
    fn secondary_limits_do_not_wait_for_an_unexhausted_primary_window() {
        let mut probes = Probes::default();
        for (now, until) in [(0, 60_000), (60_000, 180_000)] {
            assert_eq!(
                probes
                    .probe("a", "o", "r", "sha", false, now, |_, _| {
                        Ok(response(
                            403,
                            json!({"message":"You have exceeded a secondary rate limit."}),
                            &[
                                ("x-ratelimit-remaining", "4990"),
                                ("x-ratelimit-reset", "3600"),
                            ],
                        ))
                    })
                    .1,
                "probe_error:rate_limited"
            );
            assert_eq!(probes.limited_until, until);
        }
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 0, |_, _| {
                    Ok(response(
                        403,
                        json!({"message":"API rate limit exceeded"}),
                        &[
                            ("x-ratelimit-remaining", "0"),
                            ("x-ratelimit-reset", "3600"),
                        ],
                    ))
                })
                .1,
            "probe_error:rate_limited"
        );
        assert_eq!(probes.limited_until, 3_600_000);
    }

    #[test]
    fn retry_after_accepts_delta_seconds_and_http_date() {
        assert_eq!(retry_after("120", 1_000), Some(121_000));
        assert_eq!(
            retry_after("Sat, 07 Nov 2026 03:00:00 GMT", 0),
            super::super::parse_rfc3339_ms("2026-11-07T03:00:00Z")
        );
        assert_eq!(retry_after("invalid", 0), None);
    }

    #[test]
    fn gh_adapter_owns_cold_and_warm_conditional_requests() {
        for etag in [None, Some("W/\"v1\"")] {
            let command = gh_command("repos/o/r/pulls/42", etag, "agent");
            let args: Vec<_> = command.get_args().map(|v| v.to_str().unwrap()).collect();
            assert!(
                args.windows(2)
                    .any(|pair| pair == ["-H", "Cache-Control: no-cache"])
            );
            assert!(args.windows(2).any(|pair| pair == ["--method", "GET"]));
            assert!(args.contains(&"--include"));
            assert_eq!(
                args.iter().any(|arg| arg.starts_with("If-None-Match:")),
                etag.is_some()
            );
            assert_eq!(args.last(), Some(&"repos/o/r/pulls/42"));
        }
    }

    #[test]
    fn gh_included_headers_keep_error_responses_and_etags() {
        let response = parse_response(b"HTTP/2.0 403 Forbidden\r\nRetry-After: 120\r\nETag: W/\"a\"\r\n\r\n{\"message\":\"secondary rate limit\"}\n").unwrap();
        assert_eq!(response.status, 403);
        assert_eq!(response.headers["retry-after"], "120");
        assert_eq!(response.headers["etag"], "W/\"a\"");
        assert!(response.body.contains("secondary rate limit"));
        assert_eq!(
            parse_response(b"HTTP/2.0 304 Not Modified\nEtag: x\n\n")
                .unwrap()
                .status,
            304
        );
        assert_eq!(encode("feature/path #?"), "feature%2Fpath%20%23%3F");
    }
}
