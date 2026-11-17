//! Authenticated deliveries update the same target cache used by REST. Incomplete
//! populations never prove aggregate completion; reconciliation is the fallback.
use super::*;

const MAX_DELIVERIES: usize = 4096;
const MAX_VERSIONS: usize = 4096;

pub(super) fn apply(
    probes: &mut Probes,
    event: &str,
    payload: &Value,
    delivery: &str,
    now: i64,
) -> Result<bool, String> {
    if !matches!(
        event,
        "pull_request" | "check_run" | "check_suite" | "workflow_run"
    ) {
        return Ok(false);
    }
    let full_name = payload["repository"]["full_name"]
        .as_str()
        .ok_or("missing repository.full_name")?;
    let (owner, repo) = full_name
        .split_once('/')
        .filter(|(o, r)| !o.is_empty() && !r.is_empty() && !r.contains('/'))
        .ok_or("invalid repository.full_name")?;
    let object = payload
        .get(event)
        .filter(|v| v.is_object())
        .ok_or("missing event object")?;
    let sha = if event == "pull_request" {
        object["head"]["sha"].as_str()
    } else {
        object["head_sha"].as_str()
    }
    .filter(|s| !s.is_empty())
    .ok_or("missing head sha")?;
    if event == "check_run"
        && (object["id"].as_u64().is_none()
            || object["name"].as_str().is_none()
            || !matches!(
                object["status"].as_str(),
                Some("queued" | "in_progress" | "completed" | "waiting" | "pending" | "requested")
            ))
    {
        return Err("invalid check run".into());
    }
    if event == "pull_request"
        && (object["number"]
            .as_u64()
            .or(payload["number"].as_u64())
            .is_none()
            || !matches!(object["state"].as_str(), Some("open" | "closed")))
    {
        return Err("invalid pull request".into());
    }
    probes
        .deliveries
        .retain(|_, at| now.saturating_sub(*at) < 86_400_000);
    if probes.deliveries.contains_key(delivery) {
        return Ok(false);
    }
    if probes.deliveries.len() >= MAX_DELIVERIES
        && let Some(oldest) = probes
            .deliveries
            .iter()
            .min_by_key(|(_, at)| **at)
            .map(|(id, _)| id.clone())
    {
        probes.deliveries.remove(&oldest);
    }
    probes.deliveries.insert(delivery.to_string(), now);
    let mut changed = false;
    for target in probes.targets.values_mut() {
        let Some(spec) = target.spec.clone() else {
            continue;
        };
        if !spec.owner.eq_ignore_ascii_case(owner) || !spec.repo.eq_ignore_ascii_case(repo) {
            continue;
        }
        let number = object["number"].as_u64().or(payload["number"].as_u64());
        let matches_pr = spec.pr
            && if event == "pull_request" {
                number.map(|n| n.to_string()) == Some(spec.reference.clone())
            } else {
                object["pull_requests"].as_array().is_some_and(|prs| {
                    prs.iter().any(|pr| {
                        pr["number"].as_u64().map(|n| n.to_string()) == Some(spec.reference.clone())
                    })
                }) || target.head.as_deref() == Some(sha)
            };
        let branch = object["head_branch"]
            .as_str()
            .or(object["check_suite"]["head_branch"].as_str())
            .or(object["head"]["ref"].as_str());
        let matches_ref = !spec.pr
            && (spec.reference == sha
                || branch == Some(spec.reference.as_str())
                || target.head_sha.as_deref() == Some(sha));
        if !matches_pr && !matches_ref {
            continue;
        }
        // Only a PR event can change the known PR head. Empty check_run PR lists
        // are common; a known current head still identifies the right target.
        if spec.pr
            && event != "pull_request"
            && target.head.as_deref().is_some_and(|head| head != sha)
        {
            continue;
        }
        if spec.pr && target.settled && event != "pull_request" {
            continue;
        }
        if event == "check_run"
            && spec
                .check_name
                .as_deref()
                .is_some_and(|name| object["name"].as_str() != Some(name))
        {
            continue;
        }
        let version_key = format!("{event}:{}", object["id"]);
        let version = object["updated_at"]
            .as_str()
            .or(object["completed_at"].as_str())
            .or(object["started_at"].as_str())
            .unwrap_or("");
        if target
            .event_versions
            .get(&version_key)
            .is_some_and(|previous| version < previous.as_str())
        {
            continue;
        }
        // For mutable branch refs, never let an older head replace a newer one.
        let new_head = target.head_sha.as_deref() != Some(sha);
        if new_head
            && target.head_sha.is_some()
            && !spec.pr
            && spec.reference != sha
            && target
                .event_versions
                .get("head")
                .is_some_and(|previous| version <= previous.as_str())
        {
            continue;
        }
        if !target.event_versions.contains_key(&version_key)
            && target.event_versions.len() >= MAX_VERSIONS
        {
            continue;
        }
        if event == "check_run"
            && !new_head
            && let Some(snapshot) = &target.snapshot
        {
            let id = object["id"].as_u64().unwrap();
            let same_check = |run: &Value| {
                run["name"] == object["name"] && run["app"]["id"] == object["app"]["id"]
            };
            if snapshot
                .runs
                .get(&id)
                .is_some_and(|old| old["status"] == "completed" && object["status"] != "completed")
                || snapshot
                    .runs
                    .iter()
                    .any(|(old_id, run)| *old_id > id && same_check(run))
            {
                continue;
            }
        }
        let before = (
            target.result.clone(),
            target.snapshot.clone(),
            target.head_sha.clone(),
            target.settled,
            target.snapshot_complete,
        );
        if new_head {
            target.snapshot = None;
            target.snapshot_complete = false;
            target.pages.clear();
            target.legacy_pending = false;
            target.legacy_review_failed = false;
            target.settled = false;
            target.result = Some((false, "checks_in_progress".into()));
        }
        if event == "pull_request" && spec.pr {
            target.head = Some(sha.into());
            let state = object["state"].as_str().unwrap();
            target.result = Some(if object["merged"].as_bool() == Some(true) {
                (true, "merged".into())
            } else if state == "closed" {
                (true, "closed".into())
            } else if new_head
                || target
                    .result
                    .as_ref()
                    .is_some_and(|(_, state)| matches!(state.as_str(), "closed" | "merged"))
            {
                (false, "open".into())
            } else {
                target.result.clone().unwrap_or((false, "open".into()))
            });
            target.settled = state == "closed";
        } else if event == "check_run" {
            let id = object["id"].as_u64().unwrap();
            let snapshot = target.snapshot.get_or_insert_with(Checks::default);
            let known_slot = snapshot.runs.values().any(|run| {
                run["name"] == object["name"] && run["app"]["id"] == object["app"]["id"]
            });
            if spec.check_name.is_none() && target.snapshot_complete && !known_slot {
                target.snapshot_complete = false;
            }
            // REST's default filter is latest per name/app, not all reruns.
            snapshot.runs.retain(|old_id, run| {
                *old_id == id
                    || run["name"] != object["name"]
                    || run["app"]["id"] != object["app"]["id"]
            });
            snapshot.runs.insert(id, object.clone());
            let review = spec.pr
                && (target.legacy_review_failed
                    || snapshot.runs.values().any(|run| {
                        run["name"].as_str() == Some(review_check_name().as_str())
                            && matches!(
                                run["conclusion"].as_str(),
                                Some("failure" | "timed_out" | "action_required" | "error")
                            )
                    }));
            let result = if let Some(name) = spec.check_name.as_deref() {
                snapshot.selected(name)
            } else if review {
                (false, "review_failed".into())
            } else if target.snapshot_complete {
                let pending = target.legacy_pending
                    || snapshot
                        .runs
                        .values()
                        .any(|run| run["status"] != "completed");
                (
                    !pending,
                    if pending {
                        "checks_in_progress"
                    } else {
                        "checks_completed"
                    }
                    .into(),
                )
            } else {
                (false, "checks_in_progress".into())
            };
            target.result = Some(result);
            if spec.pr {
                target.head = Some(sha.into());
            }
        } else {
            // Suite/workflow completion alone cannot prove a check population.
            target
                .result
                .get_or_insert((false, "checks_in_progress".into()));
        }
        target.head_sha = Some(sha.into());
        if !version.is_empty() {
            target.event_versions.insert(version_key, version.into());
            if new_head {
                target.event_versions.insert("head".into(), version.into());
            }
        }
        let reconcile_rest = event != "pull_request"
            && spec.check_name.is_none()
            && !target.snapshot_complete
            && !target.settled
            && target
                .result
                .as_ref()
                .is_none_or(|(_, state)| state != "review_failed")
            && object["status"].as_str() == Some("completed")
            && target
                .snapshot
                .as_ref()
                .is_none_or(|s| s.runs.values().all(|r| r["status"] == "completed"));
        let rest_deferred = target.event_observation || target.next_at > now;
        if before
            == (
                target.result.clone(),
                target.snapshot.clone(),
                target.head_sha.clone(),
                target.settled,
                target.snapshot_complete,
            )
            && !(reconcile_rest && rest_deferred)
        {
            continue;
        }
        target.successful = target.result.clone();
        target.event_ready = true;
        if reconcile_rest {
            target.event_observation = false;
            target.next_at = 0;
        } else {
            target.event_observation = true;
            target.next_at = now.saturating_add(DISCONNECTED_INTERVAL);
        }
        target.accessed_at = now;
        changed = true;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn setup(pr: bool, name: Option<&str>) -> (Probes, String) {
        let mut probes = Probes::default();
        let spec = Spec {
            owner: "org".into(),
            repo: "repo".into(),
            reference: if pr { "7" } else { "main" }.into(),
            pr,
            check_name: name.map(str::to_string),
        };
        let key = spec.key("");
        probes.targets.entry(key.clone()).or_default().spec = Some(spec);
        (probes, key)
    }
    fn run(id: u64, name: &str, status: &str, sha: &str) -> Value {
        json!({"repository":{"full_name":"ORG/repo"}, "check_run":{"id":id,"name":name,"app":{"id":1},"status":status,"conclusion":"success","head_sha":sha,"check_suite":{"head_branch":"main"},"pull_requests":[{"number":7}],"updated_at":"2026-11-12T01:00:00Z"}})
    }
    #[test]
    fn named_event_is_independent_deduplicated_and_serves_all_consumers_under_hold() {
        let (mut p, key) = setup(true, Some("build"));
        assert!(
            !apply(
                &mut p,
                "check_run",
                &run(1, "unrelated", "in_progress", "a"),
                "x",
                10
            )
            .unwrap()
        );
        let done = run(2, "build", "completed", "a");
        assert!(apply(&mut p, "check_run", &done, "y", 10).unwrap());
        assert!(!apply(&mut p, "check_run", &done, "y", 11).unwrap());
        assert!(!apply(&mut p, "check_run", &done, "other-id", 12).unwrap());
        p.limited_until = 100_000;
        p.auth_until.insert("org".into(), 100_000);
        for _ in 0..2 {
            let result = p.probe_with_clock(
                &key,
                "org",
                "repo",
                "7",
                true,
                20,
                |_, _, _| panic!("webhook must not request REST"),
                || 20,
            );
            assert_eq!(result, (true, "checks_completed".into()));
        }
    }
    #[test]
    fn rest_pr_head_preserves_complete_population_for_same_head_event() {
        let (mut p, key) = setup(true, None);
        let observed = p.probe_with_clock(
            &key,
            "org",
            "repo",
            "7",
            true,
            10,
            |path, _, _| {
                let body = if path.contains("/pulls/") {
                    json!({"state":"open", "head":{"sha":"a"}})
                } else if path.contains("/status?") {
                    json!({"statuses":[]})
                } else {
                    // The pull's head remains authoritative even if a check
                    // payload lacks head_sha (or the population is empty).
                    json!({"total_count":1,"check_runs":[{
                        "id":1,"name":"build","app":{"id":1},"status":"in_progress"
                    }]})
                };
                Ok(Response {
                    status: 200,
                    headers: HashMap::new(),
                    body: body.to_string(),
                })
            },
            || 10,
        );
        assert_eq!(observed, (false, "checks_in_progress".into()));
        assert!(p.targets[&key].snapshot_complete);
        assert_eq!(p.targets[&key].head_sha.as_deref(), Some("a"));
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(1, "build", "completed", "a"),
                "done",
                11
            )
            .unwrap()
        );
        assert!(p.targets[&key].snapshot_complete);
        assert_eq!(
            p.targets[&key].result,
            Some((true, "checks_completed".into()))
        );
    }

    #[test]
    fn complete_snapshot_required_and_old_head_cannot_complete() {
        let (mut p, key) = setup(true, None);
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(1, "build", "completed", "a"),
                "1",
                10
            )
            .unwrap()
        );
        assert!(!p.targets[&key].result.as_ref().unwrap().0);
        p.targets.get_mut(&key).unwrap().snapshot_complete = true;
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(2, "build", "completed", "a"),
                "2",
                11
            )
            .unwrap()
        );
        assert!(p.targets[&key].result.as_ref().unwrap().0);
        let sync = json!({"repository":{"full_name":"org/repo"},"pull_request":{"id":7,"number":7,"state":"open","head":{"sha":"b","ref":"main"},"updated_at":"2026-11-12T02:00:00Z"}});
        assert!(apply(&mut p, "pull_request", &sync, "3", 12).unwrap());
        assert!(!p.targets[&key].snapshot_complete);
        assert!(
            !apply(
                &mut p,
                "check_run",
                &run(3, "build", "completed", "a"),
                "4",
                13
            )
            .unwrap()
        );
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(4, "build", "completed", "b"),
                "5",
                14
            )
            .unwrap()
        );
        assert!(!p.targets[&key].result.as_ref().unwrap().0);
    }
    #[test]
    fn reruns_and_stale_pr_events_do_not_regress_state() {
        let (mut p, key) = setup(true, Some("build"));
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(2, "build", "completed", "a"),
                "1",
                1
            )
            .unwrap()
        );
        assert!(
            !apply(
                &mut p,
                "check_run",
                &run(1, "build", "in_progress", "a"),
                "2",
                2
            )
            .unwrap()
        );
        assert!(
            !apply(
                &mut p,
                "check_run",
                &run(2, "build", "in_progress", "a"),
                "3",
                3
            )
            .unwrap()
        );
        assert!(
            apply(
                &mut p,
                "check_run",
                &run(3, "build", "in_progress", "a"),
                "4",
                4
            )
            .unwrap()
        );
        assert!(!p.targets[&key].result.as_ref().unwrap().0);
        let closed = json!({"repository":{"full_name":"org/repo"},"pull_request":{"id":7,"number":7,"state":"closed","merged":true,"head":{"sha":"a"},"updated_at":"2026-11-12T02:00:00Z"}});
        assert!(apply(&mut p, "pull_request", &closed, "5", 5).unwrap());
        let mut stale = closed;
        stale["pull_request"]["state"] = json!("open");
        stale["pull_request"]["updated_at"] = json!("2026-11-12T01:00:00Z");
        stale["pull_request"]["head"]["sha"] = json!("old");
        assert!(!apply(&mut p, "pull_request", &stale, "6", 6).unwrap());
        assert_eq!(p.targets[&key].result, Some((true, "merged".into())));
    }
    #[test]
    fn suite_workflow_and_wrong_repository_never_invent_completion() {
        let (mut p, key) = setup(false, None);
        for (i, event) in ["check_suite", "workflow_run"].into_iter().enumerate() {
            let payload = json!({"repository":{"full_name":"org/repo"},event:{"id":i,"head_sha":"a","head_branch":"main","status":"completed","conclusion":"success"}});
            apply(&mut p, event, &payload, &i.to_string(), 10).unwrap();
            assert!(!p.targets[&key].result.as_ref().unwrap().0);
        }
        let mut wrong = run(3, "build", "completed", "a");
        wrong["repository"]["full_name"] = json!("elsewhere/repo");
        assert!(!apply(&mut p, "check_run", &wrong, "wrong", 11).unwrap());
    }
}
