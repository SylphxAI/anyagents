//! REST-only, process-wide GitHub watch observations. The mutex is an intentional
//! single-flight/serial request gate (not a database lock): GitHub recommends
//! serial requests to avoid secondary limits. Cached pages retain summaries, not
//! full Check Run objects; IDs are retained to validate distinct populations.
use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use serde_json::Value;

const MIN_INTERVAL: i64 = 60_000;
const MAX_INTERVAL: i64 = 300_000;
const MAX_PAGES: usize = 1_000;

type Observation = (bool, String);

/// Exact configured check identity; a preview or unrelated audit is not review.
fn review_check_name() -> String {
    std::env::var("ANYAGENTS_GITHUB_REVIEW_CHECK").unwrap_or_else(|_| "review".into())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Checks {
    count: usize,
    // IDs survive page caching/304s; raw row counts cannot prove completeness.
    ids: HashSet<u64>,
    pending: bool,
    review_failed: bool,
}

impl Checks {
    fn add(&mut self, other: &Self) {
        self.count += other.count;
        self.ids.extend(&other.ids);
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

#[derive(Clone, PartialEq, Eq)]
enum Summary {
    Pull { state: String, head: String },
    Checks(Checks),
}

#[derive(Clone)]
struct Page {
    etag: Option<String>,
    last_modified: Option<String>,
    next: bool,
    total_count: Option<u64>,
    summary: Summary,
    used_at: i64,
}

#[derive(Default)]
struct Target {
    watchers: HashSet<String>,
    settled: bool,
    changed: bool,
    next_at: i64,
    interval: i64,
    resume_epoch: i64,
    resume_at: i64,
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
    auth_until: HashMap<String, i64>,
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
    Auth,
    Transport,
    Http(u16),
    Payload,
}

impl Error {
    fn observation(&self) -> Observation {
        let reason = match self {
            Self::RateLimited(_) => "rate_limited".to_string(),
            Self::Auth => "auth".to_string(),
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
    let key = target_key(&base, owner, repo, reference, pr);
    let mut probes = PROBES.lock().ok()?;
    Some(probes.probe_with_clock(
        &key,
        owner,
        repo,
        reference,
        pr,
        now,
        |path, etag, modified| request(&base, path, etag, modified, agent_id, owner),
        completed_now,
    ))
}

#[cfg(test)]
std::thread_local! {
    static COMPLETION_CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) fn with_completion_clock<T>(now: i64, call: impl FnOnce() -> T) -> T {
    struct Reset(Option<i64>);
    impl Drop for Reset {
        fn drop(&mut self) {
            COMPLETION_CLOCK.set(self.0);
        }
    }
    let _reset = Reset(COMPLETION_CLOCK.replace(Some(now)));
    call()
}

fn completed_now() -> i64 {
    #[cfg(test)]
    if let Some(now) = COMPLETION_CLOCK.get() {
        return now;
    }
    anyagents_core::now_ms()
}

fn target_key(base: &str, owner: &str, repo: &str, reference: &str, pr: bool) -> String {
    format!(
        "{base}:{}:{}/{}:{reference}",
        if pr { "pr" } else { "check" },
        owner.to_ascii_lowercase(),
        repo.to_ascii_lowercase()
    )
}

/// The probe target of a GitHub watch: a numeric check reference is its PR.
fn target_of(base: &str, target: &super::WatchTarget) -> Option<String> {
    let (owner, repo, reference, pr) = match target {
        super::WatchTarget::GitHubPr {
            owner,
            repo,
            number,
        } => (owner, repo, number.to_string(), true),
        super::WatchTarget::GitHubCheck {
            owner,
            repo,
            reference,
        } => {
            let clean = reference.strip_prefix('#').unwrap_or(reference);
            match clean.parse::<u64>() {
                Ok(number) => (owner, repo, number.to_string(), true),
                Err(_) => (owner, repo, clean.to_string(), false),
            }
        }
        _ => return None,
    };
    Some(target_key(base, owner, repo, &reference, pr))
}

/// Register this cycle's watchers, `(schedule id, target)`, of each target.
pub(super) fn begin_cycle<'a>(
    watches: impl IntoIterator<Item = (&'a str, &'a super::WatchTarget)>,
    now: i64,
) {
    let base = std::env::var("ANYAGENTS_GITHUB_API_BASE").unwrap_or_default();
    let mut watchers: HashMap<String, HashSet<String>> = HashMap::new();
    for (id, target) in watches {
        if let Some(key) = target_of(&base, target) {
            watchers.entry(key).or_default().insert(id.to_string());
        }
    }
    if let Ok(mut probes) = PROBES.lock() {
        probes.register(watchers, now);
    }
}

/// The earliest time a probe of this watch's target can reach the network:
/// before it, [`probe`] answers from its cache or its rate-limit hold. `None`
/// for a target that is not GitHub, `Some(i64::MAX)` for a settled one.
pub(super) fn next_probe_at(target: &super::WatchTarget) -> Option<i64> {
    let base = std::env::var("ANYAGENTS_GITHUB_API_BASE").unwrap_or_default();
    let key = target_of(&base, target)?;
    let probes = PROBES.lock().ok()?;
    let Some(state) = probes.targets.get(&key) else {
        return Some(0);
    };
    if state.settled && state.successful.is_some() {
        return Some(i64::MAX);
    }
    let owner = match target {
        super::WatchTarget::GitHubPr { owner, .. }
        | super::WatchTarget::GitHubCheck { owner, .. } => owner.to_ascii_lowercase(),
        _ => return None,
    };
    let cached_until = if state.result.is_some() {
        state.next_at
    } else {
        0
    };
    Some(
        cached_until
            .max(probes.limited_until)
            .max(state.resume_at)
            .max(probes.auth_until.get(&owner).copied().unwrap_or(0)),
    )
}

impl Probes {
    fn register(&mut self, watchers: HashMap<String, HashSet<String>>, now: i64) {
        for (key, current) in watchers {
            let target = self.targets.entry(key).or_default();
            if !current.is_subset(&target.watchers) {
                target.next_at = 0;
                target.interval = MIN_INTERVAL;
                target.settled = false;
                target.successful = None;
            }
            target.watchers = current;
            target.accessed_at = now;
        }
    }

    fn spread_recovery(&mut self) {
        // Stratify the window, shuffling target order per reset. This avoids
        // probabilistic collisions/clumps while keeping each target's ordinary
        // next_at and adaptive interval intact.
        let until = self.limited_until;
        let mut keys: Vec<_> = self.targets.keys().cloned().collect();
        keys.sort_by_cached_key(|key| (recovery_hash(key, until), key.clone()));
        let count = keys.len().max(1) as i64;
        let span = MAX_INTERVAL - MIN_INTERVAL;
        for (rank, key) in keys.into_iter().enumerate() {
            let start = MIN_INTERVAL + rank as i64 * span / count;
            let width = (((rank + 1) as i64 * span / count) - (start - MIN_INTERVAL)).max(1);
            let target = self.targets.get_mut(&key).unwrap();
            target.resume_epoch = until;
            target.resume_at =
                until.saturating_add(start + (recovery_hash(&key, until) % width as u64) as i64);
        }
    }

    fn ensure_recovery(&mut self, key: &str) {
        let until = self.limited_until;
        if until == 0
            || self
                .targets
                .get(key)
                .is_some_and(|t| t.resume_epoch == until)
        {
            return;
        }
        // A target first seen during/after a hold gets its own gate too. Do not
        // move already assigned gates when a watcher arrives or is cancelled.
        let span = MAX_INTERVAL - MIN_INTERVAL;
        let seed = (recovery_hash(key, until) % span as u64) as i64;
        let occupied: HashSet<_> = self
            .targets
            .values()
            .filter(|t| t.resume_epoch == until)
            .map(|t| t.resume_at)
            .collect();
        let offset = (0..span)
            .map(|step| (seed + step) % span)
            .find(|offset| !occupied.contains(&until.saturating_add(MIN_INTERVAL + offset)))
            .unwrap_or(seed);
        let target = self.targets.entry(key.to_string()).or_default();
        target.resume_epoch = until;
        target.resume_at = until.saturating_add(MIN_INTERVAL + offset);
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn probe(
        &mut self,
        key: &str,
        owner: &str,
        repo: &str,
        reference: &str,
        pr: bool,
        now: i64,
        request: impl FnMut(&str, Option<&str>, Option<&str>) -> Result<Response, Error>,
    ) -> Observation {
        self.probe_with_clock(key, owner, repo, reference, pr, now, request, || now)
    }

    #[allow(clippy::too_many_arguments)]
    fn probe_with_clock(
        &mut self,
        key: &str,
        owner: &str,
        repo: &str,
        reference: &str,
        pr: bool,
        now: i64,
        mut request: impl FnMut(&str, Option<&str>, Option<&str>) -> Result<Response, Error>,
        completed_at: impl FnOnce() -> i64,
    ) -> Observation {
        let auth_owner = owner.to_ascii_lowercase();
        if now < self.auth_until.get(&auth_owner).copied().unwrap_or(0) {
            return Error::Auth.observation();
        }
        self.ensure_recovery(key);
        self.targets.retain(|_, target| {
            now.saturating_sub(target.accessed_at) < 3_600_000 || now <= target.resume_at
        });
        let target = self.targets.entry(key.to_string()).or_default();
        target.accessed_at = now;
        if now < self.limited_until.max(target.resume_at) {
            return Error::RateLimited(self.limited_until.max(target.resume_at)).observation();
        }
        if target.settled
            && let Some(result) = &target.successful
        {
            return result.clone();
        }
        target.changed = false;
        if now < target.next_at
            && let Some(result) = &target.result
        {
            return result.clone();
        }
        let root = format!("repos/{}/{}", encode(owner), encode(repo));
        let mut pause_until = 0;
        let floor = std::env::var("ANYAGENTS_GITHUB_RATE_LIMIT_FLOOR")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1500);
        let result = read_target(
            target,
            &root,
            reference,
            pr,
            now,
            &mut |path, etag, modified| {
                if pause_until > now {
                    return Err(Error::RateLimited(pause_until));
                }
                let response = request(path, etag, modified)?;
                pause_until = reserve_pause(&response, floor, now);
                Ok(response)
            },
        );
        // Network/subprocess time belongs to this probe, not its next interval.
        let completed_at = completed_at();
        // Consume the response that crossed the floor, but admit no next page or
        // target until reset. A terminal final page can still be delivered.
        let previous_until = self.limited_until;
        self.limited_until = self.limited_until.max(pause_until);
        let observation = match result {
            Ok(observation) => {
                target.pages.retain(|_, page| page.used_at == now);
                target.interval =
                    if !target.changed && target.successful.as_ref() == Some(&observation) {
                        (target.interval * 2).clamp(MIN_INTERVAL, MAX_INTERVAL)
                    } else {
                        MIN_INTERVAL
                    };
                target.settled = if pr {
                    matches!(observation.1.as_str(), "merged" | "closed")
                } else {
                    observation.0
                        && reference.len() == 40
                        && reference.bytes().all(|b| b.is_ascii_hexdigit())
                };
                target.successful = Some(observation.clone());
                target.result = Some(observation.clone());
                target.next_at = completed_at.saturating_add(target.interval);
                self.limit_interval = 0;
                observation
            }
            Err(error) => {
                if let Error::RateLimited(until) = error {
                    self.limit_interval =
                        (self.limit_interval * 2).clamp(MIN_INTERVAL, MAX_INTERVAL);
                    self.limited_until = self
                        .limited_until
                        .max(until)
                        .max(now.saturating_add(self.limit_interval));
                } else {
                    if matches!(error, Error::Auth) {
                        self.auth_until
                            .insert(auth_owner, now.saturating_add(MIN_INTERVAL));
                    }
                    target.next_at = completed_at.saturating_add(MIN_INTERVAL);
                    target.result = Some(error.observation());
                }
                error.observation()
            }
        };
        if self.limited_until > previous_until {
            self.spread_recovery();
        }
        observation
    }
}

fn recovery_hash(key: &str, until: i64) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hash);
    until.hash(&mut hash);
    hash.finish()
}

fn read_target(
    target: &mut Target,
    root: &str,
    reference: &str,
    pr: bool,
    now: i64,
    request: &mut impl FnMut(&str, Option<&str>, Option<&str>) -> Result<Response, Error>,
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
        // A new revision is a transition even when its aggregate check state
        // matches the old head. Restart adaptive polling without altering the
        // schedule's persisted transition baseline.
        target.successful = None;
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
    request: &mut impl FnMut(&str, Option<&str>, Option<&str>) -> Result<Response, Error>,
) -> Result<Checks, Error> {
    let mut checks = Checks::default();
    let check_runs = endpoint.ends_with("/check-runs");
    let mut total_count = None;
    for n in 1..=MAX_PAGES {
        let path = format!("{endpoint}?per_page=100&page={n}");
        let page = read_page(target, &path, now, request)?;
        let Summary::Checks(summary) = page.summary else {
            return Err(Error::Payload);
        };
        if check_runs {
            let page_total = page.total_count.ok_or(Error::Payload)?;
            // Offset pages are not an atomic snapshot. A changed total proves
            // a torn read even if the fetched completed rows reach that total.
            if total_count.is_some_and(|previous| previous != page_total) {
                return Err(Error::Payload);
            }
            total_count = Some(page_total);
        } else {
            total_count = Some(total_count.unwrap_or(0).max(page.total_count.unwrap_or(0)));
        }
        checks.add(&summary);
        let observed_count = if check_runs {
            checks.ids.len() as u64
        } else {
            checks.count as u64
        };
        let total = total_count.unwrap_or(0);
        if check_runs && observed_count > total {
            return Err(Error::Payload);
        }
        // A cold 304 can restore a body without the original Link header.
        // Only distinct IDs from a stable declared population can satisfy it.
        let next = page.next || observed_count < total;
        if !next {
            return Ok(checks);
        }
        if summary.count == 0 {
            return Err(Error::Payload); // No progress toward a known next page.
        }
    }
    Err(Error::Payload) // Never report completion from a truncated population.
}

fn read_page(
    target: &mut Target,
    path: &str,
    now: i64,
    request: &mut impl FnMut(&str, Option<&str>, Option<&str>) -> Result<Response, Error>,
) -> Result<Page, Error> {
    let cached = target.pages.get(path);
    let response = request(
        path,
        cached.and_then(|page| page.etag.as_deref()),
        cached.and_then(|page| page.last_modified.as_deref()),
    )?;
    if response.status == 401 {
        return Err(Error::Auth);
    }
    if response.status == 304 {
        if let Some(mut page) = cached.cloned() {
            if let Some(etag) = response.headers.get("etag") {
                page.etag = Some(etag.clone());
            }
            page.used_at = now;
            target.pages.insert(path.to_string(), page.clone());
            return Ok(page);
        }
        if response.body.trim().is_empty() {
            return Err(Error::Payload);
        }
        // The gh wrapper can restore a disk-cached body under 304 headers
        // before this process has an in-memory page to reuse.
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
            let primary_reset = if response
                .headers
                .get("x-ratelimit-remaining")
                .is_some_and(|v| v == "0")
            {
                response
                    .headers
                    .get("x-ratelimit-reset")
                    .and_then(|v| v.parse::<i64>().ok())
                    .map(|s| s.saturating_mul(1_000))
            } else {
                None
            };
            let until = retry
                .and_then(|v| retry_after(v, now))
                .into_iter()
                .chain(primary_reset)
                .max()
                .unwrap_or_else(|| now.saturating_add(MIN_INTERVAL));
            return Err(Error::RateLimited(until));
        }
    }
    if response.status != 200 && response.status != 304 {
        return Err(Error::Http(response.status));
    }
    let json: Value = serde_json::from_str(&response.body).map_err(|_| Error::Payload)?;
    let mut total_count = None;
    let mut next = response
        .headers
        .get("link")
        .is_some_and(|v| v.split(',').any(|link| link.contains("rel=\"next\"")));
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
        let check_runs = path.contains("/check-runs?");
        let runs = json
            .get(if check_runs { "check_runs" } else { "statuses" })
            .and_then(Value::as_array)
            .ok_or(Error::Payload)?;
        total_count = json
            .get("total_count")
            .map(|value| value.as_u64().ok_or(Error::Payload))
            .transpose()?;
        // Legacy status envelopes need not declare a total. A full page can
        // never prove exhaustion when an adapter omitted pagination headers.
        next |= !check_runs && runs.len() == 100;
        let mut checks = Checks {
            count: runs.len(),
            ..Checks::default()
        };
        for item in runs {
            if check_runs {
                checks.ids.insert(
                    item.get("id")
                        .and_then(Value::as_u64)
                        .ok_or(Error::Payload)?,
                );
                checks.pending |= item.get("status").and_then(Value::as_str) != Some("completed");
            } else {
                checks.pending |= !super::check_item_is_terminal(item);
            }
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
            checks.review_failed |= name == review_check_name()
                && matches!(
                    conclusion.to_ascii_uppercase().as_str(),
                    "FAILURE" | "ERROR" | "TIMED_OUT" | "ACTION_REQUIRED"
                );
        }
        Summary::Checks(checks)
    };
    let page = Page {
        etag: response.headers.get("etag").cloned(),
        last_modified: response.headers.get("last-modified").cloned(),
        next,
        total_count,
        summary,
        used_at: now,
    };
    target.changed |= cached.is_none_or(|old| old.etag != page.etag || old.summary != page.summary);
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

fn reserve_pause(response: &Response, floor: u64, now: i64) -> i64 {
    if response
        .headers
        .get("x-ratelimit-remaining")
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|remaining| remaining < floor)
    {
        return response
            .headers
            .get("x-ratelimit-reset")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|s| s.saturating_mul(1000))
            .filter(|reset| *reset > now)
            .unwrap_or_else(|| now.saturating_add(MIN_INTERVAL));
    }
    0
}

fn token_for_owner(dir: &std::path::Path, owner: &str) -> Result<String, Error> {
    let expected = format!("{owner}.token");
    let mut found = None;
    for entry in std::fs::read_dir(dir).map_err(|_| Error::Auth)? {
        let entry = entry.map_err(|_| Error::Auth)?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case(&expected))
        {
            if found.is_some() {
                return Err(Error::Auth);
            }
            found = Some(entry.path());
        }
    }
    token_from_file(&found.ok_or(Error::Auth)?)
}

fn token_from_file(path: &std::path::Path) -> Result<String, Error> {
    use std::io::Read;
    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    // Reject special files via the opened descriptor without blocking first
    // on a FIFO's writer while the process-wide probe gate is held.
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).map_err(|_| Error::Auth)?;
    let metadata = file.metadata().map_err(|_| Error::Auth)?;
    if !metadata.is_file() {
        return Err(Error::Auth);
    }
    #[cfg(unix)]
    if metadata.mode() & 0o7777 != 0o600 || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(Error::Auth);
    }
    let mut raw = String::new();
    file.take(16_385)
        .read_to_string(&mut raw)
        .map_err(|_| Error::Auth)?;
    let token = raw.trim();
    if raw.len() > 16_384
        || token.is_empty()
        || token
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(Error::Auth);
    }
    Ok(token.to_string())
}

static HTTP: LazyLock<ureq::Agent> = LazyLock::new(|| {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent()
});

fn request(
    base: &str,
    path: &str,
    etag: Option<&str>,
    modified: Option<&str>,
    agent_id: &str,
    owner: &str,
) -> Result<Response, Error> {
    let token_dir = std::env::var_os("ANYAGENTS_GITHUB_TOKEN_DIR");
    request_with_token_dir(
        base,
        path,
        etag,
        modified,
        agent_id,
        owner,
        token_dir.as_deref().map(std::path::Path::new),
    )
}

fn request_with_token_dir(
    base: &str,
    path: &str,
    etag: Option<&str>,
    modified: Option<&str>,
    agent_id: &str,
    owner: &str,
    token_dir: Option<&std::path::Path>,
) -> Result<Response, Error> {
    // Presence, not readability, selects dedicated mode. Rotation is an atomic
    // file replacement; re-read for every wire request and never fall back.
    let token = token_dir
        .map(|dir| token_for_owner(dir, owner))
        .transpose()?;
    if !base.is_empty() || token.is_some() {
        let base = if base.is_empty() {
            "https://api.github.com"
        } else {
            base
        };
        let response = http_request(base, path, etag, modified, token.as_deref())?;
        record_response(path, &response);
        return Ok(response);
    }
    gh_command(path, etag, modified, agent_id)
        .output()
        .map_err(|_| Error::Transport)
        .and_then(|output| parse_response(&output.stdout))
        .inspect(|response| record_response(path, response))
}

fn record_response(path: &str, response: &Response) {
    let kind = if path.contains("/pulls/") {
        "pull"
    } else if path.contains("/check-runs") {
        "check_runs"
    } else {
        "commit_status"
    };
    // Status/type only: no body, credential, validator or response headers.
    eprintln!("github_watch_probe kind={kind} status={}", response.status);
}

fn http_request(
    base: &str,
    path: &str,
    etag: Option<&str>,
    modified: Option<&str>,
    token: Option<&str>,
) -> Result<Response, Error> {
    let mut req = HTTP
        .get(format!("{}/{path}", base.trim_end_matches('/')))
        .header("accept", "application/vnd.github+json")
        .header("user-agent", "anyagents-github-watch")
        .header("x-github-api-version", "2022-11-28");
    if let Some(token) = token {
        req = req.header("authorization", &format!("Bearer {token}"));
    }
    if let Some(etag) = etag {
        req = req.header("if-none-match", etag);
    } else if let Some(modified) = modified {
        req = req.header("if-modified-since", modified);
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
    Ok(Response {
        status,
        headers,
        body,
    })
}

fn gh_command(
    path: &str,
    etag: Option<&str>,
    modified: Option<&str>,
    agent_id: &str,
) -> std::process::Command {
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
    } else if let Some(modified) = modified {
        command.args(["-H", &format!("If-Modified-Since: {modified}")]);
    }
    command.arg(path);
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
        response_body(status, body.to_string(), headers)
    }

    fn response_body(status: u16, body: impl Into<String>, headers: &[(&str, &str)]) -> Response {
        Response {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.into(),
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

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const COLD_PAGE1: &str = include_str!("fixtures/cold-304-check-runs-page1.http");

    fn pending_page2() -> Response {
        response(
            200,
            json!({"total_count":101,"check_runs":[{"id":101,"name":"ci-last","status":"in_progress","conclusion":null}]}),
            &[("etag", "\"page2-v1\"")],
        )
    }

    #[test]
    fn probe_backoff_starts_after_successful_or_failed_request_completion() {
        for fails in [false, true] {
            let mut probes = Probes::default();
            let result = probes.probe_with_clock(
                "slow",
                "o",
                "r",
                "1",
                true,
                1000,
                |path, _, _| {
                    if fails {
                        return Err(Error::Transport);
                    }
                    Ok(response(
                        200,
                        if path.contains("/pulls/") {
                            pull("open", false, SHA)
                        } else if path.contains("/check-runs") {
                            pending()
                        } else {
                            json!({"statuses":[]})
                        },
                        &[],
                    ))
                },
                || 9000,
            );
            let next = 9000 + MIN_INTERVAL;
            assert_eq!(probes.targets["slow"].next_at, next);
            let cached = probes.probe_with_clock(
                "slow",
                "o",
                "r",
                "1",
                true,
                next - 1,
                |_, _, _| panic!("slow probe must retain a complete post-request interval"),
                || next - 1,
            );
            assert_eq!(cached, result);
            let mut calls = 0;
            probes.probe_with_clock(
                "slow",
                "o",
                "r",
                "1",
                true,
                next,
                |_, _, _| {
                    calls += 1;
                    Err(Error::Transport)
                },
                || next + 2000,
            );
            assert_eq!(calls, 1, "an expired interval must admit another request");
            assert_eq!(probes.targets["slow"].next_at, next + 2000 + MIN_INTERVAL);
        }
    }

    // Acceptance assertion, deliberately not should_panic: an unsafe terminal is a failure.
    #[test]
    fn cold_304_missing_link_must_not_complete_partial_population() {
        let mut probes = Probes::default();
        let mut seen = vec![];
        let result = probes.probe("cold", "o", "r", SHA, false, 0, |path, etag, _| {
            seen.push(path.to_string());
            assert_eq!(etag, None);
            if path.ends_with("page=1") {
                parse_response(COLD_PAGE1.as_bytes())
            } else {
                assert!(path.ends_with("page=2"));
                Ok(pending_page2())
            }
        });
        eprintln!(
            "cold result={result:?}; requested={seen:?}; settled={}",
            probes.targets["cold"].settled
        );
        // The full known population contains pending page 2; failing closed is also safe.
        assert!(
            !result.0,
            "completed page 1 is not the whole population of 101 checks"
        );
    }

    #[test]
    fn missing_links_use_cumulative_totals_on_cold_and_warm_pages() {
        for cold_status in [200, 304] {
            let mut probes = Probes::default();
            for now in [0, 60_000] {
                let mut seen = vec![];
                let result = probes.probe("pages", "o", "r", "sha", false, now, |path, etag, _| {
                    let n: usize = path.rsplit_once("page=").unwrap().1.parse().unwrap();
                    assert!((1..=3).contains(&n), "all 201 checks were already observed");
                    seen.push(n);
                    if now != 0 {
                        assert_eq!(etag, Some("page"));
                        return Ok(response_body(304, "", &[]));
                    }
                    assert_eq!(etag, None);
                    let count = if n == 3 { 1 } else { 100 };
                    Ok(response(
                        cold_status,
                        json!({
                            "total_count": 201,
                            "check_runs": (0..count)
                                .map(|id| json!({"id": (n - 1) * 100 + id, "status": "completed"}))
                                .collect::<Vec<_>>()
                        }),
                        &[("etag", "page")],
                    ))
                });
                assert_eq!(result, (true, "checks_completed".to_string()));
                assert_eq!(seen, [1, 2, 3]);
            }
        }
    }

    #[test]
    fn added_pending_check_with_rising_total_must_not_complete_duplicate_population() {
        let mut probes = Probes::default();
        for now in [0, MIN_INTERVAL] {
            let mut pages = vec![];
            let result = probes.probe("offset-race", "o", "r", SHA, false, now, |path, etag, _| {
                let page: usize = path.rsplit_once("page=").unwrap().1.parse().unwrap();
                pages.push(page);
                match page {
                    // The cold 304 restores the original first 100 IDs and no
                    // Link header. The second cycle reuses that warm summary.
                    1 if now == 0 => parse_response(COLD_PAGE1.as_bytes()),
                    1 => {
                        assert_eq!(etag, Some("\"page1-v1\""));
                        Ok(response_body(304, "", &[]))
                    }
                    // Pending id 102 was inserted at the front, shifting the
                    // offset: id 99 repeats, id 100 is new, and both fetched
                    // rows are completed. The pending run is on old page 1.
                    2 => Ok(response(
                        200,
                        json!({
                            "total_count": 102,
                            "check_runs": [
                                {"id": 99, "status": "completed"},
                                {"id": 100, "status": "completed"}
                            ]
                        }),
                        &[],
                    )),
                    _ => panic!("offset race requested unexpected page {page}"),
                }
            });
            assert_eq!(pages, [1, 2]);
            assert_eq!(result, (false, "probe_error:payload".to_string()));
            assert!(!probes.targets["offset-race"].settled);
        }
    }

    #[test]
    fn duplicate_completed_ids_cannot_satisfy_an_unchanged_total() {
        let mut probes = Probes::default();
        let mut pages = vec![];
        let result = probes.probe("duplicate", "o", "r", SHA, false, 0, |path, _, _| {
            let page: usize = path.rsplit_once("page=").unwrap().1.parse().unwrap();
            pages.push(page);
            let runs = match page {
                1 => json!([{"id":1,"status":"completed"}]),
                2 => json!([{"id":1,"status":"completed"}, {"id":2,"status":"completed"}]),
                3 => json!([]),
                _ => panic!("incomplete population did not stop on an empty page"),
            };
            Ok(response(
                200,
                json!({"total_count":3,"check_runs":runs}),
                &[],
            ))
        });
        assert_eq!(pages, [1, 2, 3]);
        assert_eq!(result, (false, "probe_error:payload".to_string()));
        assert!(!probes.targets["duplicate"].settled);
    }

    #[test]
    fn changed_total_is_inconsistent_even_without_duplicate_ids() {
        for later_total in [1, 3] {
            let mut probes = Probes::default();
            let result = probes.probe("changed-total", "o", "r", SHA, false, 0, |path, _, _| {
                let (total, id) = if path.ends_with("page=1") {
                    (2, 1)
                } else {
                    (later_total, 2)
                };
                Ok(response(
                    200,
                    json!({"total_count":total,"check_runs":[{"id":id,"status":"completed"}]}),
                    &[],
                ))
            });
            assert_eq!(result, (false, "probe_error:payload".to_string()));
            assert!(!probes.targets["changed-total"].settled);
        }
    }

    #[test]
    fn check_run_completion_requires_valid_ids_totals_and_completed_status() {
        for body in [
            json!({"check_runs":[{"id":1,"status":"completed"}]}),
            json!({"total_count":1,"check_runs":[{"status":"completed"}]}),
            json!({"total_count":1,"check_runs":[{"id":null,"status":"completed"}]}),
            json!({"total_count":1,"check_runs":[{"id":"1","status":"completed"}]}),
            json!({"total_count":0,"check_runs":[{"id":1,"status":"completed"}]}),
        ] {
            let mut probes = Probes::default();
            let result = probes.probe("invalid", "o", "r", SHA, false, 0, |_, _, _| {
                Ok(response(200, body.clone(), &[]))
            });
            assert_eq!(result, (false, "probe_error:payload".to_string()), "{body}");
            assert!(!probes.targets["invalid"].settled);
        }
        // A legacy terminal state cannot substitute for a check-run status.
        let mut probes = Probes::default();
        let result = probes.probe("missing-status", "o", "r", SHA, false, 0, |_, _, _| {
            Ok(response(
                200,
                json!({"total_count":1,"check_runs":[{"id":1,"state":"success"}]}),
                &[],
            ))
        });
        assert_eq!(result, (false, "checks_in_progress".to_string()));
        assert!(!probes.targets["missing-status"].settled);
    }

    #[test]
    fn incomplete_total_fails_closed_and_can_recover_after_an_empty_page() {
        for later_total in [None, Some(0), Some(101)] {
            let mut probes = Probes::default();
            for now in [0, 60_000] {
                let mut calls = 0;
                let result = probes.probe("partial", "o", "r", SHA, false, now, |path, etag, _| {
                    calls += 1;
                    if path.ends_with("page=1") {
                        if now == 0 {
                            assert_eq!(etag, None);
                            return parse_response(COLD_PAGE1.as_bytes());
                        }
                        assert_eq!(etag, Some("\"page1-v1\""));
                        return Ok(response_body(304, "", &[]));
                    }
                    assert!(path.ends_with("page=2"));
                    if now == 0 {
                        let mut body = json!({"check_runs":[]});
                        if let Some(total) = later_total {
                            body["total_count"] = json!(total);
                        }
                        Ok(response(200, body, &[]))
                    } else {
                        Ok(response(
                            200,
                            json!({"total_count":101,"check_runs":[{"id":101,"status":"completed"}]}),
                            &[],
                        ))
                    }
                });
                assert_eq!(calls, 2);
                if now == 0 {
                    assert_eq!(result, (false, "probe_error:payload".to_string()));
                    assert!(!probes.targets["partial"].settled);
                } else {
                    assert_eq!(result, (true, "checks_completed".to_string()));
                    assert!(probes.targets["partial"].settled);
                }
            }
        }
    }

    #[test]
    fn full_legacy_status_page_without_link_reads_the_next_page() {
        for cold_status in [200, 304] {
            for pending in [true, false] {
                let mut probes = Probes::default();
                let mut status_pages = vec![];
                let result = probes.probe("pr", "o", "r", "42", true, 0, |path, etag, _| {
                    assert_eq!(etag, None);
                    let body = if path.ends_with("/pulls/42") {
                        pull("open", false, SHA)
                    } else if path.contains("/check-runs?") {
                        json!({"total_count":0,"check_runs":[]})
                    } else {
                        let n: usize = path.rsplit_once("page=").unwrap().1.parse().unwrap();
                        status_pages.push(n);
                        match n {
                            1 | 2 => json!({"statuses":vec![json!({"state":"success"});100]}),
                            3 if pending => json!({"statuses":[{"state":"pending"}]}),
                            3 => json!({"statuses":[]}),
                            _ => panic!("legacy status pagination did not stop"),
                        }
                    };
                    Ok(response(cold_status, body, &[]))
                });
                assert_eq!(status_pages, [1, 2, 3]);
                assert_eq!(
                    result,
                    (
                        !pending,
                        if pending {
                            "checks_in_progress"
                        } else {
                            "checks_completed"
                        }
                        .to_string()
                    )
                );
            }
        }
    }

    #[test]
    fn malformed_total_count_is_a_payload_error() {
        for status in [200, 304] {
            for total in [json!(null), json!(-1), json!(1.5), json!("101")] {
                let mut probes = Probes::default();
                let result = probes.probe("bad-count", "o", "r", SHA, false, 0, |_, _, _| {
                    Ok(response(
                        status,
                        json!({"total_count":total,"check_runs":[{"id":1,"status":"completed"}]}),
                        &[],
                    ))
                });
                assert_eq!(result, (false, "probe_error:payload".to_string()));
                assert!(!probes.targets["bad-count"].settled);
            }
        }
    }

    #[test]
    fn next_link_is_honored_even_when_declared_total_is_already_observed() {
        let mut probes = Probes::default();
        let mut calls = 0;
        let result = probes.probe("link", "o", "r", SHA, false, 0, |path, _, _| {
            calls += 1;
            Ok(if path.ends_with("page=1") {
                response(
                    304,
                    json!({"total_count":2,"check_runs":[{"id":1,"status":"completed"}]}),
                    &[("link", "<page2>; rel=\"next\"")],
                )
            } else {
                assert!(path.ends_with("page=2"));
                response(
                    200,
                    json!({"total_count":2,"check_runs":[{"id":2,"status":"in_progress"}]}),
                    &[],
                )
            })
        });
        assert_eq!(result, (false, "checks_in_progress".to_string()));
        assert_eq!(calls, 2);
    }

    #[test]
    fn unsatisfied_total_count_is_bounded_by_the_page_limit() {
        let mut probes = Probes::default();
        let mut calls = 0;
        let result = probes.probe("bounded", "o", "r", SHA, false, 0, |_, _, _| {
            calls += 1;
            Ok(response(
                304,
                json!({"total_count":u64::MAX,"check_runs":[{"id":1,"status":"completed"}]}),
                &[],
            ))
        });
        assert_eq!(calls, MAX_PAGES);
        assert_eq!(result, (false, "probe_error:payload".to_string()));
        assert!(!probes.targets["bounded"].settled);
    }

    #[test]
    fn not_modified_with_cached_page_uses_cache_when_body_is_empty() {
        let path = "repos/o/r/commits/sha/check-runs?per_page=100&page=1";
        let mut target = Target::default();
        target.pages.insert(
            path.to_string(),
            Page {
                etag: Some("old".to_string()),
                last_modified: None,
                next: false,
                total_count: None,
                summary: Summary::Checks(Checks {
                    count: 1,
                    ids: HashSet::from([1]),
                    pending: true,
                    review_failed: false,
                }),
                used_at: 0,
            },
        );

        for (now, headers, expected_etag) in
            [(123, vec![], "old"), (456, vec![("etag", "new")], "new")]
        {
            let page = read_page(&mut target, path, now, &mut |_, etag, _| {
                assert_eq!(etag, Some("old"));
                Ok(response_body(304, "", &headers))
            })
            .unwrap();

            assert_eq!(page.etag.as_deref(), Some(expected_etag));
            assert_eq!(page.used_at, now);
            assert!(matches!(
                page.summary,
                Summary::Checks(Checks { count: 1, .. })
            ));
            assert_eq!(target.pages[path].etag.as_deref(), Some(expected_etag));
            assert_eq!(target.pages[path].used_at, now);
        }
    }

    #[test]
    fn not_modified_with_disk_cached_body_populates_cold_check_probe_page() {
        let mut probes = Probes::default();
        let body = completed();
        let expected_count = body["check_runs"].as_array().unwrap().len();
        let result = probes.probe("check", "o", "r", "sha", false, 0, |path, etag, _| {
            assert_eq!(path, "repos/o/r/commits/sha/check-runs?per_page=100&page=1");
            assert_eq!(etag, None);
            parse_response(
                format!("HTTP/2.0 304 Not Modified\nEtag: \"disk\"\n\n{body}").as_bytes(),
            )
        });

        assert_eq!(result, (true, "checks_completed".to_string()));
        let page = probes.targets["check"]
            .pages
            .get("repos/o/r/commits/sha/check-runs?per_page=100&page=1");
        let page = page.expect("304 body should populate the in-memory page cache");
        assert_eq!(page.etag.as_deref(), Some("\"disk\""));
        assert_eq!(page.used_at, 0);
        assert!(matches!(
            page.summary,
            Summary::Checks(Checks { count, .. }) if count == expected_count
        ));
    }

    #[test]
    fn not_modified_without_cached_page_or_body_is_a_payload_error() {
        let path = "repos/o/r/commits/sha/check-runs?per_page=100&page=1";
        for body in ["", " \t\r\n", "invalid json", "null"] {
            let mut target = Target::default();
            let result = read_page(&mut target, path, 123, &mut |_, etag, _| {
                assert_eq!(etag, None);
                Ok(response_body(304, body, &[]))
            });

            assert!(matches!(result, Err(Error::Payload)), "body: {body:?}");
            assert!(target.pages.is_empty());
        }
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
            let result = probes.probe("target", "o", "r", "sha", false, now, |path, etag, _| {
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
                        |_, _, _| panic!("duplicate request")
                    ),
                    result
                );
            }
        }
        assert_eq!(calls, 5);
        assert_eq!(
            probes.probe("target", "o", "r", "sha", false, 1_020_000, |_, etag, _| {
                assert_eq!(etag, Some("\"pending\""));
                Ok(response(200, completed(), &[("etag", "\"done\"")]))
            }),
            (true, "checks_completed".to_string())
        );
        assert_eq!(probes.targets["target"].interval, MIN_INTERVAL);
    }

    #[test]
    fn head_replacement_revalidates_checks_and_resets_the_interval() {
        let mut probes = Probes::default();
        for (now, head, interval) in [
            (0, "old", 60_000),
            (60_000, "old", 120_000),
            (180_000, "old", 240_000),
            (420_000, "old", 300_000),
            (720_000, "new", 60_000),
            (780_000, "new", 120_000),
        ] {
            let changed = now == 0 || now == 720_000;
            let mut calls = 0;
            assert_eq!(
                probes.probe("pr", "o", "r", "42", true, now, |path, etag, _| {
                    calls += 1;
                    if path.contains("/pulls/") {
                        return Ok(response(200, pull("open", false, head), &[("etag", head)]));
                    }
                    assert!(path.contains(&format!("/commits/{head}/")));
                    assert_eq!(etag, if changed { None } else { Some(head) });
                    Ok(if changed {
                        response(
                            200,
                            if path.contains("/status?") {
                                json!({"statuses":[]})
                            } else {
                                pending()
                            },
                            &[("etag", head)],
                        )
                    } else {
                        response(304, Value::Null, &[])
                    })
                }),
                (false, "checks_in_progress".to_string())
            );
            assert_eq!(calls, 3);
            assert_eq!(probes.targets["pr"].interval, interval);
            assert_eq!(probes.targets["pr"].next_at, now + interval);
            assert!(probes.targets["pr"].pages.keys().all(|path| {
                path.contains("/pulls/") || path.contains(&format!("/commits/{head}/"))
            }));
        }
    }

    #[test]
    fn three_thousand_checks_revalidate_every_page_and_never_complete_early() {
        let mut probes = Probes::default();
        let template = completed()["check_runs"][0].clone();
        for (now, done) in [(0, false), (60_000, false), (180_000, true)] {
            let mut pages = 0;
            let result = probes.probe("pr", "o", "r", "42", true, now, |path, etag, _| {
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
        let first = probes.probe("pr", "o", "r", "42", true, 0, |path, _, _| {
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
                .probe("other", "o", "r", "sha", false, 1_000, |_, _, _| {
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
                        .probe(key, "o", "r", "42", true, now, |_, _, _| panic!(
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
            probes.probe(
                "pr",
                "o",
                "r",
                "42",
                true,
                probes.targets["pr"].resume_at,
                |path, etag, _| {
                    assert!(path.ends_with("/pulls/42"));
                    assert_eq!(etag, Some("before"));
                    Ok(response(
                        200,
                        pull("closed", true, "head"),
                        &[("etag", "after")],
                    ))
                }
            ),
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
                    .probe("a", "o", "r", "sha", false, now, |_, _, _| {
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
            now = probes.targets["a"].resume_at;
        }
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 0, |_, _, _| {
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
                .probe("b", "o", "r", "sha", false, 0, |_, _, _| Ok(response(
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
                json!({"total_count":1,"check_runs":[{"id":1,"name":"review","status":"completed","conclusion":"timed_out"}]}),
                json!([]),
                "review_failed",
            ),
            (
                json!({"total_count":0,"check_runs":[]}),
                json!([{"context":"ci","state":"error"}]),
                "checks_completed",
            ),
            (json!({"total_count":0,"check_runs":[]}), json!([]), "open"),
            (
                json!({"total_count":1,"check_runs":[{"id":1,"name":"preview-build","status":"completed","conclusion":"failure"}]}),
                json!([{ "context":"deploy", "state":"pending"}]),
                "checks_in_progress",
            ),
            (
                json!({"total_count":1,"check_runs":[{"id":1,"name":"review-extra","status":"completed","conclusion":"failure"}]}),
                json!([]),
                "checks_completed",
            ),
            (
                json!({"total_count":1,"check_runs":[{"id":1,"name":"Review","status":"completed","conclusion":"failure"}]}),
                json!([]),
                "checks_completed",
            ),
        ] {
            let mut probes = Probes::default();
            assert_eq!(
                probes
                    .probe("pr", "o", "r", "42", true, 0, |path, _, _| Ok(response(
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
                probes.probe("pr", "o", "r", "42", true, 0, |path, _, _| {
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
                .probe("a", "o", "r", "sha", false, 0, |_, _, _| Ok(response(
                    304,
                    Value::Null,
                    &[]
                )))
                .1,
            "probe_error:payload"
        );
        assert_eq!(
            probes
                .probe("b", "o", "r", "sha", false, 0, |path, _, _| {
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
        let mut now = 0;
        for delay in [60_000, 120_000] {
            assert_eq!(
                probes
                    .probe("a", "o", "r", "sha", false, now, |_, _, _| {
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
            assert_eq!(probes.limited_until, now + delay);
            now = probes.targets["a"].resume_at;
        }
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 0, |_, _, _| {
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
    fn fifty_watches_share_three_requests_per_cycle_and_new_watch_resets_once() {
        let mut probes = Probes::default();
        let population = |extra: bool| {
            let mut watchers: HashMap<String, HashSet<String>> = HashMap::new();
            for i in 0..50 {
                watchers
                    .entry(format!("target{}", i % 3))
                    .or_default()
                    .insert(format!("watch{i}"));
            }
            if extra {
                watchers
                    .get_mut("target0")
                    .unwrap()
                    .insert("new".to_string());
            }
            watchers
        };
        for now in [0, 60_000, 180_000] {
            probes.register(population(false), now);
            let mut count = 0;
            for i in 0..50 {
                let result = probes.probe(
                    &format!("target{}", i % 3),
                    "o",
                    "r",
                    "sha",
                    false,
                    now,
                    |_, etag, _| {
                        count += 1;
                        Ok(if etag.is_some() {
                            response(304, Value::Null, &[])
                        } else {
                            response(200, pending(), &[("etag", "pending")])
                        })
                    },
                );
                assert_eq!(result.1, "checks_in_progress");
            }
            assert_eq!(count, 3);
        }
        probes.register(population(true), 180_001);
        assert_eq!(probes.targets["target0"].next_at, 0);
        assert_eq!(probes.targets["target1"].next_at, 420_000);
        let mut count = 0;
        for i in 0..51 {
            probes.probe(
                &format!("target{}", i % 3),
                "o",
                "r",
                "sha",
                false,
                180_001,
                |_, _, _| {
                    count += 1;
                    Ok(response(304, Value::Null, &[]))
                },
            );
        }
        assert_eq!(count, 1);
        assert_eq!(probes.targets["target0"].interval, MIN_INTERVAL);
        probes.register(population(true), 180_002);
        assert!(probes.targets["target0"].next_at > 180_002);
    }

    #[test]
    fn terminal_pr_and_pinned_sha_stop_requests_while_delivery_is_deferred() {
        for (pr, reference) in [
            (true, "42"),
            (false, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ] {
            let mut probes = Probes::default();
            let expected = probes.probe("target", "o", "r", reference, pr, 0, |_, _, _| {
                Ok(response(
                    200,
                    if pr {
                        pull("closed", true, "head")
                    } else {
                        completed()
                    },
                    &[],
                ))
            });
            for now in [60_000, 600_000, 3_500_000] {
                assert_eq!(
                    probes.probe("target", "o", "r", reference, pr, now, |_, _, _| panic!(
                        "settled target polled"
                    )),
                    expected
                );
            }
        }
    }

    #[test]
    fn primary_reserve_stops_all_targets_until_reset_including_after_304() {
        for status in [200, 304] {
            let mut probes = Probes::default();
            probes.probe("a", "o", "r", "sha", false, 0, |_, _, _| {
                Ok(response(200, pending(), &[("etag", "a")]))
            });
            probes.probe("a", "o", "r", "sha", false, 60_000, |_, _, _| {
                Ok(response(
                    status,
                    pending(),
                    &[
                        ("x-ratelimit-remaining", "1499"),
                        ("x-ratelimit-reset", "3600"),
                    ],
                ))
            });
            assert_eq!(probes.limited_until, 3_600_000);
            for key in ["a", "b"] {
                assert_eq!(
                    probes
                        .probe(key, "o", "r", "sha", false, 3_599_999, |_, _, _| panic!(
                            "reserve bypassed"
                        ))
                        .1,
                    "probe_error:rate_limited"
                );
            }
            assert_eq!(
                probes
                    .probe(
                        "a",
                        "o",
                        "r",
                        "sha",
                        false,
                        probes.targets["a"].resume_at,
                        |_, _, _| Ok(response(200, completed(), &[]))
                    )
                    .1,
                "checks_completed"
            );
        }
    }

    #[test]
    fn secondary_retry_never_shortens_the_reserve_hold_from_the_same_response() {
        let mut probes = Probes::default();
        let mut calls = 0;
        assert_eq!(
            probes
                .probe("floor", "o", "r", "sha", false, 0, |_, _, _| {
                    calls += 1;
                    Ok(response(
                        403,
                        json!({"message":"You have exceeded a secondary rate limit."}),
                        &[
                            ("retry-after", "60"),
                            ("x-ratelimit-remaining", "1499"),
                            ("x-ratelimit-reset", "3600"),
                        ],
                    ))
                })
                .1,
            "probe_error:rate_limited"
        );
        assert_eq!(probes.limited_until, 3_600_000);
        for now in [60_000, 359_999, 3_599_999, 3_600_000, 3_601_000] {
            assert_eq!(
                probes
                    .probe("other", "other-owner", "r", "sha", false, now, |_, _, _| {
                        calls += 1;
                        Ok(response(200, pending(), &[]))
                    })
                    .1,
                "probe_error:rate_limited"
            );
        }
        assert_eq!(calls, 1);
        let resume = probes.targets["other"].resume_at;
        assert!(resume >= 3_660_000);
        assert_eq!(
            probes
                .probe(
                    "other",
                    "other-owner",
                    "r",
                    "sha",
                    false,
                    resume,
                    |_, _, _| {
                        calls += 1;
                        Ok(response(200, pending(), &[]))
                    }
                )
                .1,
            "checks_in_progress"
        );
        assert_eq!(calls, 2);
    }

    #[test]
    fn recovery_spread_admits_no_request_in_the_old_secondary_only_jitter_window() {
        let mut probes = Probes::default();
        let mut calls = 0;
        probes.probe("a", "o", "r", "sha", false, 0, |_, _, _| {
            calls += 1;
            Ok(response(
                403,
                json!({"message":"secondary rate limit"}),
                &[
                    ("retry-after", "60"),
                    ("x-ratelimit-remaining", "1499"),
                    ("x-ratelimit-reset", "3600"),
                ],
            ))
        });
        // Before the repair this was assigned from the shorter secondary hold.
        // Check that entire old window, not an internal gate that is correctly
        // beyond the hourly reset after the fix.
        for now in 120_000..360_000 {
            let result = probes.probe("a", "o", "r", "sha", false, now, |_, _, _| {
                calls += 1;
                Ok(response(
                    200,
                    pending(),
                    &[
                        ("x-ratelimit-remaining", "1498"),
                        ("x-ratelimit-reset", "3600"),
                    ],
                ))
            });
            assert_eq!(result.1, "probe_error:rate_limited");
        }
        assert_eq!(
            calls, 1,
            "recovery jitter admitted a request below reserve before the one-hour reset"
        );
        assert!((3_660_000..3_900_000).contains(&probes.targets["a"].resume_at));
    }

    #[test]
    fn paused_targets_resume_across_the_window_without_a_reset_herd() {
        const N: usize = 96;
        const RESET: i64 = 600_000;
        for status in [200, 304, 403] {
            let mut probes = Probes::default();
            for i in 0..N {
                let key = format!("target-{i}");
                for now in [0, 60_000] {
                    probes.probe(&key, "o", "r", "sha", false, now, |_, _, _| {
                        Ok(response(200, pending(), &[("etag", "pending")]))
                    });
                }
            }
            let before: HashMap<_, _> = probes
                .targets
                .iter()
                .map(|(key, target)| (key.clone(), (target.next_at, target.interval)))
                .collect();
            probes.probe("target-0", "o", "r", "sha", false, 180_000, |_, _, _| {
                Ok(response(
                    status,
                    if status == 403 {
                        json!({"message":"API rate limit exceeded"})
                    } else {
                        pending()
                    },
                    &[
                        (
                            "x-ratelimit-remaining",
                            if status == 403 { "0" } else { "1499" },
                        ),
                        ("x-ratelimit-reset", "600"),
                        ("etag", "pending"),
                    ],
                ))
            });
            assert_eq!(probes.limited_until, RESET);
            let mut gates: Vec<_> = probes
                .targets
                .iter()
                .map(|(key, target)| {
                    assert_eq!(
                        target.interval,
                        if key == "target-0" && status != 403 {
                            before[key].1 * 2
                        } else {
                            before[key].1
                        }
                    );
                    // The crossing 200/304 is a successful poll, so it may advance
                    // its own ordinary due time. Other targets retain theirs.
                    if key != "target-0" || status == 403 {
                        assert_eq!(target.next_at, before[key].0);
                    }
                    assert!(
                        (RESET + MIN_INTERVAL..RESET + MAX_INTERVAL).contains(&target.resume_at)
                    );
                    (target.resume_at, key.clone())
                })
                .collect();
            gates.sort();
            assert_eq!(
                gates.iter().map(|(at, _)| at).collect::<HashSet<_>>().len(),
                N
            );
            // Stratification covers every quarter of the recovery window.
            for quarter in 0..4 {
                let start = RESET + MIN_INTERVAL + quarter * 60_000;
                assert_eq!(
                    gates
                        .iter()
                        .filter(|(at, _)| (start..start + 60_000).contains(at))
                        .count(),
                    N / 4
                );
            }
            for now in [RESET - 1, RESET, RESET + 1, RESET + 1_000] {
                for (_, key) in &gates {
                    assert_eq!(
                        probes
                            .probe(key, "o", "r", "sha", false, now, |_, _, _| panic!(
                                "reset herd"
                            ))
                            .1,
                        "probe_error:rate_limited"
                    );
                }
            }
            let mut wire_times = Vec::new();
            for (at, key) in gates {
                let previous_interval = probes.targets[&key].interval;
                assert_eq!(
                    probes
                        .probe(&key, "o", "r", "sha", false, at - 1, |_, _, _| panic!(
                            "early recovery"
                        ))
                        .1,
                    "probe_error:rate_limited"
                );
                assert_eq!(
                    probes
                        .probe(&key, "o", "r", "sha", false, at, |_, _, _| {
                            wire_times.push(at);
                            Ok(response(200, pending(), &[("etag", "pending")]))
                        })
                        .1,
                    "checks_in_progress"
                );
                assert_eq!(
                    probes.targets[&key].interval,
                    (previous_interval * 2).clamp(MIN_INTERVAL, MAX_INTERVAL)
                );
            }
            assert_eq!(wire_times.len(), N);
            assert_eq!(wire_times.iter().collect::<HashSet<_>>().len(), N);
        }
    }

    #[test]
    fn new_watches_cannot_skip_recovery_gates_or_advance_a_future_due_time() {
        let mut probes = Probes::default();
        probes.probe("existing", "o", "r", "sha", false, 0, |_, _, _| {
            Ok(response(200, pending(), &[]))
        });
        probes.targets.get_mut("existing").unwrap().next_at = 900_000;
        probes.probe("limited", "o", "r", "sha", false, 0, |_, _, _| {
            Ok(response(429, Value::Null, &[("retry-after", "60")]))
        });
        let existing_gate = probes.targets["existing"].resume_at;
        assert_eq!(probes.targets["existing"].next_at, 900_000);
        assert_eq!(
            probes
                .probe("late", "o", "r", "sha", false, 60_000, |_, _, _| panic!(
                    "new target bypassed gate"
                ))
                .1,
            "probe_error:rate_limited"
        );
        let late_gate = probes.targets["late"].resume_at;
        probes.register(
            HashMap::from([("late".into(), HashSet::from(["new-watch".into()]))]),
            60_001,
        );
        assert_eq!(probes.targets["late"].resume_at, late_gate);
        assert_eq!(probes.targets["existing"].resume_at, existing_gate);
        assert_ne!(late_gate, existing_gate);
        assert_eq!(
            probes
                .probe(
                    "late",
                    "o",
                    "r",
                    "sha",
                    false,
                    late_gate - 1,
                    |_, _, _| panic!("new watch bypassed gate")
                )
                .1,
            "probe_error:rate_limited"
        );
        assert_eq!(
            probes
                .probe("late", "o", "r", "sha", false, late_gate, |_, _, _| Ok(
                    response(200, pending(), &[])
                ))
                .1,
            "checks_in_progress"
        );
        assert_eq!(
            probes
                .probe(
                    "existing",
                    "o",
                    "r",
                    "sha",
                    false,
                    existing_gate,
                    |_, _, _| panic!("ordinary due time advanced")
                )
                .1,
            "checks_in_progress"
        );
    }

    #[test]
    fn auth_failure_pauses_owner_and_recovers_without_losing_validators() {
        let mut probes = Probes::default();
        probes.probe("a", "o", "r", "sha", false, 0, |_, _, _| {
            Ok(response(
                200,
                pending(),
                &[("etag", "a"), ("last-modified", "date")],
            ))
        });
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 60_000, |_, etag, modified| {
                    assert_eq!(etag, Some("a"));
                    assert_eq!(modified, Some("date"));
                    Ok(response(401, Value::Null, &[]))
                })
                .1,
            "probe_error:auth"
        );
        assert_eq!(
            probes
                .probe("b", "o", "r", "sha", false, 119_999, |_, _, _| panic!(
                    "auth cooldown bypassed"
                ))
                .1,
            "probe_error:auth"
        );
        assert_eq!(
            probes
                .probe("a", "o", "r", "sha", false, 120_000, |_, etag, modified| {
                    assert_eq!(etag, Some("a"));
                    assert_eq!(modified, Some("date"));
                    Ok(response(200, completed(), &[]))
                })
                .1,
            "checks_completed"
        );
    }

    #[test]
    fn owner_tokens_are_case_insensitive_private_and_auth_holds_are_isolated() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        for (owner, token) in [
            ("AnotherOrg", "fixture-another"),
            ("ExampleOrg", "fixture-exampleorg"),
        ] {
            let path = dir.path().join(format!("{owner}.token"));
            std::fs::write(&path, token).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert_eq!(
                token_for_owner(dir.path(), &owner.to_ascii_lowercase()).unwrap(),
                token
            );
        }
        assert!(matches!(
            token_for_owner(dir.path(), "missing"),
            Err(Error::Auth)
        ));
        let path = dir.path().join("ExampleOrg.token");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            token_for_owner(dir.path(), "ExampleOrg"),
            Err(Error::Auth)
        ));
        assert!(token_for_owner(dir.path(), "AnotherOrg").is_ok());
        let mut probes = Probes::default();
        assert_eq!(
            probes
                .probe("bad", "ExampleOrg", "r", "sha", false, 0, |_, _, _| Err(
                    Error::Auth
                ))
                .1,
            "probe_error:auth"
        );
        assert_eq!(
            probes
                .probe(
                    "same-owner",
                    "exampleorg",
                    "r",
                    "sha",
                    false,
                    1,
                    |_, _, _| panic!("auth hold bypassed")
                )
                .1,
            "probe_error:auth"
        );
        assert_eq!(
            probes
                .probe("good", "AnotherOrg", "r", "sha", false, 1, |_, _, _| Ok(
                    response(200, pending(), &[])
                ))
                .1,
            "checks_in_progress"
        );
    }

    #[test]
    fn token_file_is_reread_and_missing_or_empty_never_becomes_anonymous() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        assert!(matches!(token_from_file(&path), Err(Error::Auth)));
        for token in ["fixture-first", "fixture-rotated"] {
            std::fs::write(&path, format!("{token}\n")).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            assert_eq!(token_from_file(&path).unwrap(), token);
        }
        for token in ["", " \n", "bad\nheader", "bad token"] {
            std::fs::write(&path, token).unwrap();
            assert!(matches!(token_from_file(&path), Err(Error::Auth)));
        }
    }

    // A real socket boundary checks routing, credentials and prepared headers;
    // injected responses alone cannot establish which identity went on the wire.
    fn credential_server(
        expected: Vec<(String, String, u16, Option<String>)>,
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let thread = std::thread::spawn(move || {
            for (path, token, status, location) in expected {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            break stream;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(std::time::Instant::now() < deadline, "missing HTTP request");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("fixture accept: {error}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert_eq!(line, format!("GET /{path} HTTP/1.1\r\n"));
                let mut headers = HashMap::new();
                loop {
                    line.clear();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (key, value) = line.trim().split_once(':').unwrap();
                    headers.insert(key.to_ascii_lowercase(), value.trim().to_string());
                }
                assert_eq!(headers["authorization"], format!("Bearer {token}"));
                assert_eq!(headers["accept"], "application/vnd.github+json");
                assert_eq!(headers["x-github-api-version"], "2022-11-28");
                let body = pending().to_string();
                let location = location
                    .map(|v| format!("Location: {v}\r\n"))
                    .unwrap_or_default();
                write!(stream, "HTTP/1.1 {status} Fixture\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        (base, thread)
    }

    #[test]
    fn dedicated_http_routes_and_rotates_owner_tokens_and_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let write_token = |owner: &str, value: &str| {
            // Match the publisher's atomic replacement rather than mutating an
            // inode that a concurrent reader may already have open.
            let staged = dir.path().join("staged");
            std::fs::write(&staged, value).unwrap();
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::rename(staged, dir.path().join(format!("{owner}.token"))).unwrap();
        };
        write_token("AnotherOrg", "fixture-first");
        write_token("ExampleOrg", "fixture-exampleorg");
        let path =
            |owner: &str| format!("repos/{owner}/r/commits/sha/check-runs?per_page=100&page=1");
        let expected = [
            ("anotherorg", "fixture-first", 200),
            ("exampleorg", "fixture-exampleorg", 200),
            ("anotherorg", "fixture-rotated", 200),
            ("exampleorg", "fixture-exampleorg", 401),
            ("anotherorg", "fixture-rotated", 200),
            ("exampleorg", "fixture-recovered", 200),
            ("anotherorg", "fixture-rotated", 200),
        ]
        .into_iter()
        .map(|(owner, token, status)| (path(owner), token.to_string(), status, None))
        .collect();
        let (base, server) = credential_server(expected);
        // A configured missing/bad file must never reach either HTTP or gh.
        assert!(matches!(
            request_with_token_dir(
                &base,
                &path("missing"),
                None,
                None,
                "agent",
                "missing",
                Some(dir.path())
            ),
            Err(Error::Auth)
        ));
        let exampleorg = dir.path().join("ExampleOrg.token");
        std::fs::set_permissions(&exampleorg, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            request_with_token_dir(
                &base,
                &path("exampleorg"),
                None,
                None,
                "agent",
                "exampleorg",
                Some(dir.path())
            ),
            Err(Error::Auth)
        ));
        std::fs::set_permissions(&exampleorg, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut probes = Probes::default();
        let poll = |probes: &mut Probes, key: &str, owner: &str, now: i64| {
            probes
                .probe(
                    key,
                    owner,
                    "r",
                    "sha",
                    false,
                    now,
                    |path, etag, modified| {
                        request_with_token_dir(
                            &base,
                            path,
                            etag,
                            modified,
                            "agent",
                            owner,
                            Some(dir.path()),
                        )
                    },
                )
                .1
        };
        assert_eq!(
            poll(&mut probes, "s", "anotherorg", 0),
            "checks_in_progress"
        );
        assert_eq!(
            poll(&mut probes, "c", "exampleorg", 0),
            "checks_in_progress"
        );
        write_token("AnotherOrg", "fixture-rotated");
        assert_eq!(
            poll(&mut probes, "s", "anotherorg", 60_000),
            "checks_in_progress"
        );
        assert_eq!(
            poll(&mut probes, "c", "exampleorg", 60_000),
            "probe_error:auth"
        );
        // The owner's 401 holds all its targets, but not the other owner.
        assert_eq!(
            poll(&mut probes, "c2", "ExampleOrg", 119_999),
            "probe_error:auth"
        );
        assert_eq!(
            poll(&mut probes, "s2", "anotherorg", 119_999),
            "checks_in_progress"
        );
        write_token("ExampleOrg", "fixture-recovered");
        assert_eq!(
            poll(&mut probes, "c", "exampleorg", 120_000),
            "checks_in_progress"
        );
        assert_eq!(
            poll(&mut probes, "s", "anotherorg", 180_000),
            "checks_in_progress"
        );
        server.join().unwrap();
    }

    #[test]
    fn fifo_credential_is_refused_without_stalling_other_owners() {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("bad.token");
        let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let good = dir.path().join("good.token");
        std::fs::write(&good, "fixture-other-owner").unwrap();
        std::fs::set_permissions(good, std::fs::Permissions::from_mode(0o600)).unwrap();
        let (base, server) = credential_server(vec![(
            "repos/good/r/commits/sha/check-runs?per_page=100&page=1".to_string(),
            "fixture-other-owner".to_string(),
            200,
            None,
        )]);
        let token_dir = dir.path().to_path_buf();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            // Match the process-wide entrypoint's mutex lifetime: credential
            // selection runs while the probe lock is held, then another owner
            // must be able to use that same gate.
            let probes = Mutex::new(Probes::default());
            for owner in ["bad", "good"] {
                let observation = probes.lock().unwrap().probe(
                    owner,
                    owner,
                    "r",
                    "sha",
                    false,
                    0,
                    |path, etag, modified| {
                        request_with_token_dir(
                            &base,
                            path,
                            etag,
                            modified,
                            "agent",
                            owner,
                            Some(&token_dir),
                        )
                    },
                );
                tx.send(observation.1).unwrap();
            }
        });
        let first = rx.recv_timeout(Duration::from_secs(2));
        let returned_without_writer = first.is_ok();
        // If the regression returns, unblock its old blocking-open behavior so
        // the test can report a bounded failure instead of hanging the suite.
        let _rescue = if returned_without_writer {
            None
        } else {
            Some(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&fifo)
                    .unwrap(),
            )
        };
        let first = first.unwrap_or_else(|_| rx.recv_timeout(Duration::from_secs(2)).unwrap());
        assert_eq!(first, "probe_error:auth");
        let second = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        worker.join().unwrap();
        server.join().unwrap();
        assert_eq!(second, "checks_in_progress");
        assert!(
            returned_without_writer,
            "FIFO open stalled the shared probe gate"
        );
    }

    #[test]
    fn dedicated_http_never_forwards_credentials_on_redirect() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("o.token");
        std::fs::write(&token, "fixture-origin-only").unwrap();
        std::fs::set_permissions(token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let destination = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        destination.set_nonblocking(true).unwrap();
        let (base, server) = credential_server(vec![(
            "repos/o/r/pulls/1".to_string(),
            "fixture-origin-only".to_string(),
            302,
            Some(format!(
                "http://{}/redirected",
                destination.local_addr().unwrap()
            )),
        )]);
        let mut probes = Probes::default();
        let result = probes.probe(
            "redirect",
            "o",
            "r",
            "1",
            true,
            0,
            |path, etag, modified| {
                request_with_token_dir(&base, path, etag, modified, "agent", "o", Some(dir.path()))
            },
        );
        assert_eq!(result.1, "probe_error:http:302");
        server.join().unwrap();
        assert!(
            matches!(destination.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
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
            let command = gh_command("repos/o/r/pulls/42", etag, None, "agent");
            let args: Vec<_> = command.get_args().map(|v| v.to_str().unwrap()).collect();
            assert!(!args.iter().any(|arg| arg.starts_with("Cache-Control:")));
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

    #[test]
    fn reviewer_secondary_limit_must_not_shorten_primary_reserve_hold() {
        let mut probes = Probes::default();
        let result = probes.probe("a", "o", "r", "sha", false, 0, |_, _, _| {
            Ok(response(
                403,
                json!({"message":"You have exceeded a secondary rate limit."}),
                &[
                    ("retry-after", "60"),
                    ("x-ratelimit-remaining", "1499"),
                    ("x-ratelimit-reset", "3600"),
                ],
            ))
        });
        assert_eq!(result.1, "probe_error:rate_limited");
        assert_eq!(
            probes.limited_until, 3_600_000,
            "secondary Retry-After must not overwrite a longer primary reserve hold"
        );
    }

    #[test]
    fn reviewer_below_reserve_secondary_limit_must_admit_no_pre_reset_request() {
        let mut probes = Probes::default();
        let mut calls = 0;
        probes.probe("a", "o", "r", "sha", false, 0, |_, _, _| {
            calls += 1;
            Ok(response(
                403,
                json!({"message":"secondary rate limit"}),
                &[
                    ("retry-after", "60"),
                    ("x-ratelimit-remaining", "1499"),
                    ("x-ratelimit-reset", "3600"),
                ],
            ))
        });
        let result = probes.probe("b", "o", "r", "sha", false, 60_000, |_, _, _| {
            calls += 1;
            Ok(response(
                200,
                pending(),
                &[
                    ("x-ratelimit-remaining", "1498"),
                    ("x-ratelimit-reset", "3600"),
                ],
            ))
        });
        assert_eq!(calls, 1, "request admitted below reserve before UTC reset");
        assert_eq!(result.1, "probe_error:rate_limited");
    }

    #[test]
    fn reviewer_validator_changes_do_not_hide_terminal_payloads() {
        for (old_etag, new_etag, old_modified, new_modified) in [
            ("same", "same", "old", "new"),
            ("old", "new", "same", "same"),
        ] {
            let mut probes = Probes::default();
            probes.probe("v", "o", "r", "sha", false, 0, |_, _, _| {
                Ok(response(
                    200,
                    pending(),
                    &[("etag", old_etag), ("last-modified", old_modified)],
                ))
            });
            let terminal =
                probes.probe("v", "o", "r", "sha", false, 60_000, |_, etag, modified| {
                    assert_eq!(etag, Some(old_etag));
                    assert_eq!(modified, Some(old_modified));
                    Ok(response(
                        200,
                        completed(),
                        &[("etag", new_etag), ("last-modified", new_modified)],
                    ))
                });
            assert_eq!(terminal.1, "checks_completed");
            assert!(terminal.0);
        }
        let mut probes = Probes::default();
        probes.probe("no-etag", "o", "r", "sha", false, 0, |_, _, _| {
            Ok(response(200, pending(), &[("last-modified", "old")]))
        });
        assert!(
            probes
                .probe(
                    "no-etag",
                    "o",
                    "r",
                    "sha",
                    false,
                    60_000,
                    |_, etag, modified| {
                        assert_eq!(etag, None);
                        assert_eq!(modified, Some("old"));
                        Ok(response(200, completed(), &[("last-modified", "new")]))
                    }
                )
                .0
        );
    }

    #[test]
    fn reviewer_unsafe_token_files_fail_closed() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let safe = dir.path().join("safe");
        std::fs::write(&safe, "fixture-only").unwrap();
        std::fs::set_permissions(&safe, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("o.token");
        symlink(&safe, &link).unwrap();
        assert!(matches!(token_for_owner(dir.path(), "o"), Err(Error::Auth)));
        std::fs::remove_file(&link).unwrap();
        std::fs::create_dir(&link).unwrap();
        assert!(matches!(token_for_owner(dir.path(), "o"), Err(Error::Auth)));
        std::fs::remove_dir(&link).unwrap();
        std::fs::write(&link, "fixture-only").unwrap();
        std::fs::set_permissions(&link, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(token_for_owner(dir.path(), "o"), Err(Error::Auth)));
        std::fs::set_permissions(&link, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(token_for_owner(dir.path(), "o").is_ok());
        let duplicate = dir.path().join("O.token");
        std::fs::write(&duplicate, "fixture-only").unwrap();
        std::fs::set_permissions(&duplicate, std::fs::Permissions::from_mode(0o600)).unwrap();
        // A case-insensitive filesystem aliases these paths instead of creating
        // an ambiguous pair. Verify the actual fixture population first.
        let matching_files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case("o.token"))
            })
            .count();
        if matching_files == 2 {
            assert!(matches!(token_for_owner(dir.path(), "o"), Err(Error::Auth)));
        } else {
            assert_eq!(matching_files, 1);
            assert_eq!(token_for_owner(dir.path(), "o").unwrap(), "fixture-only");
        }
    }

    #[test]
    fn reviewer_auth_retries_are_unbounded_but_one_per_owner_per_minute() {
        let mut probes = Probes::default();
        let mut requests = 0;
        for minute in 0..180 {
            let now = minute * 60_000;
            assert_eq!(
                probes
                    .probe("bad", "o", "r", "sha", false, now, |_, _, _| {
                        requests += 1;
                        Ok(response(401, Value::Null, &[]))
                    })
                    .1,
                "probe_error:auth"
            );
            assert_eq!(
                probes
                    .probe(
                        "second",
                        "O",
                        "r2",
                        "sha",
                        false,
                        now + 59_999,
                        |_, _, _| { panic!("owner pause bypassed") }
                    )
                    .1,
                "probe_error:auth"
            );
        }
        assert_eq!(requests, 180);
    }

    #[test]
    fn reviewer_http_conditional_validators_are_sent_with_etag_precedence() {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for (expected_etag, expected_modified, status) in
                [(Some("v1"), None, 304), (None, Some("date1"), 304)]
            {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut headers = HashMap::new();
                loop {
                    line.clear();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (key, value) = line.trim().split_once(':').unwrap();
                    headers.insert(key.to_ascii_lowercase(), value.trim().to_string());
                }
                assert_eq!(
                    headers.get("if-none-match").map(String::as_str),
                    expected_etag
                );
                assert_eq!(
                    headers.get("if-modified-since").map(String::as_str),
                    expected_modified
                );
                write!(stream, "HTTP/1.1 {status} Not Modified\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
        });
        assert_eq!(
            http_request(&base, "repos/o/r/pulls/1", Some("v1"), Some("date1"), None)
                .unwrap()
                .status,
            304
        );
        assert_eq!(
            http_request(&base, "repos/o/r/pulls/1", None, Some("date1"), None)
                .unwrap()
                .status,
            304
        );
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn reviewer_fifo_token_must_be_refused_without_blocking_all_owners() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("o.token");
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            tx.send(matches!(token_from_file(&path), Err(Error::Auth)))
                .unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)),
            Ok(true),
            "unsafe FIFO must be refused instead of blocking before descriptor validation"
        );
    }
}
