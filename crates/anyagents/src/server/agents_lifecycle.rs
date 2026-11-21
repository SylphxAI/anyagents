//! Agent archive, pause, wake, deletion, and ephemeral worktree reaping.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyagents_core::{Agent, Error, Store, now_ms};
use anyagents_supervisor::HealthBoard;
use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use serde::Deserialize;

use crate::api::AgentView;
use crate::auth::Principal;
use crate::create;

use super::agents_roster::{
    Markers, PROFILE_ROTATED, agent_view, compute_block_reason, persisted_fault,
};
use super::messaging::{Actor, can_actor_message_agent};
use super::types::{
    ApiError, AppState, is_self_or_ancestor, require_operator_audited,
    resolve_remote_sandbox_workdir, runs_off_host, spawn_store, via_http_mcp,
};
use super::{file_cache, in_flight, reap_queue, serverless, serverless_failure};

pub(crate) fn has_explicit_archive_marker(
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    anyagents_core::agent_config_dir(data_dir, agent_id)
        .join(".anyagents-archived")
        .exists()
}

pub(crate) fn is_explicitly_archived(data_dir: &Path, agent: &Agent) -> bool {
    agent.state == "archived" || has_explicit_archive_marker(data_dir, &agent.id)
}

/// The caller holds this agent's lifecycle lock; descendant locks follow only
/// downward, so a cascading archive cannot deadlock a direct child wake.
pub(crate) fn mark_explicitly_archived_locked(
    store: &Store,
    data_dir: &Path,
    agent: &Agent,
) -> Result<Vec<anyagents_core::AgentId>, anyagents_core::Error> {
    let mut archived = vec![agent.id.clone()];
    mark_archive_generation_locked(store, data_dir, agent);
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(agent.id.clone());
    let mut seen = std::collections::HashSet::new();
    seen.insert(agent.id.clone());
    while let Some(pid) = queue.pop_front() {
        let Ok(mut children) = store.children_of(&pid) else {
            continue;
        };
        children.sort_by(|a, b| a.name.cmp(&b.name));
        for child in &children {
            if seen.insert(child.id.clone()) {
                queue.push_back(child.id.clone());
                let Ok(_child_lifecycle) = store.try_lock_agent_lifecycle(&child.id) else {
                    continue;
                };
                let Ok(child) = store.set_agent_state(&child.id, "archived") else {
                    continue;
                };
                mark_archive_generation_locked(store, data_dir, &child);
                archived.push(child.id.clone());
            }
        }
    }
    Ok(archived)
}

/// Mark only this row while its lifecycle lock is held; descendant cleanup is
/// handled separately by maintenance.
pub(super) fn mark_archive_generation_locked(store: &Store, data_dir: &Path, agent: &Agent) {
    let cfg = anyagents_core::agent_config_dir(data_dir, &agent.id);
    let _ = std::fs::create_dir_all(&cfg);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let archived_marker = cfg.join(".anyagents-archived");
    if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
        &archived_marker,
        format!("{}:{}\n", now_ms(), anyagents_core::AgentId::generate()).as_bytes(),
        0o644,
    ) {
        eprintln!("cannot persist {}: {err}", archived_marker.display());
    }
    if agent.parent_id.is_some() {
        reap_queue::enqueue(data_dir, &agent.id);
    }
    if let Ok(schedules) = store.list_schedules(Some(&agent.id), true) {
        for sched in schedules {
            let _ = store.cancel_schedule(Some(&agent.id), &sched.id);
        }
    }
    let _ = super::serverless_watches::cancel_watches_targeting_agent(store, agent);
    serverless_failure::cancel_active_remote_turns_for_archived_agent(store, &agent.id);
    crate::schedule::schedules_changed(store.data_dir());
    crate::telegram::detach_archived_agent_from_telegram_groups(store, data_dir, agent, true);
}

/// The caller holds the lifecycle lock. The marker identifies this archive,
/// not directory existence: retained dirty worktrees are not pending cleanup.
pub(crate) fn reap_archived_agent_locked(
    store: &Store,
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) {
    // Health is a projection; only custody excludes a worker still exiting.
    let Ok(process) = store.try_lock_agent(agent_id) else {
        reap_queue::note_failure(data_dir, agent_id, now_ms());
        return;
    };
    reap_archived_agent_with_process_locked(store, data_dir, agent_id, &process);
}

/// Caller holds the lifecycle lock and retains actual process custody.
fn reap_archived_agent_with_process_locked(
    store: &Store,
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
    _process: &anyagents_core::AgentLock,
) {
    let agent = match store.agent(agent_id) {
        Ok(agent) => agent,
        Err(Error::AgentNotFound(_)) => return reap_queue::dequeue(data_dir, agent_id),
        Err(_) => return reap_queue::note_failure(data_dir, agent_id, now_ms()),
    };
    if agent.state != "archived" || agent.parent_id.is_none() {
        return reap_queue::dequeue(data_dir, agent_id);
    }
    let cfg = anyagents_core::agent_config_dir(data_dir, agent_id);
    let archived = cfg.join(".anyagents-archived");
    if std::fs::read(&archived).is_ok_and(|generation| generation.is_empty())
        || std::fs::symlink_metadata(&archived)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        // Upgrade legacy or interrupted empty archive markers under the wake lock.
        mark_archive_generation_locked(store, data_dir, &agent);
    }
    let generation = std::fs::read(&archived).unwrap_or_default();
    if generation.is_empty() {
        return reap_queue::note_failure(data_dir, agent_id, now_ms());
    }
    let reaped = cfg.join(".anyagents-reaped");
    if std::fs::read(&reaped).is_ok_and(|done| done == generation) {
        return reap_queue::dequeue(data_dir, agent_id);
    }
    if !reap_subagent_ephemeral_dirs_in(store, data_dir, agent_id) {
        return reap_queue::note_failure(data_dir, agent_id, now_ms());
    }
    match anyagents_harness::atomic_write_nonempty_file(&reaped, &generation, 0o644) {
        Ok(()) => reap_queue::dequeue(data_dir, agent_id),
        Err(err) => {
            eprintln!("cannot persist {}: {err}", reaped.display());
            reap_queue::note_failure(data_dir, agent_id, now_ms());
        }
    }
}

/// The maintenance worker runs this at startup and each minute without waiting
/// for an agent lock. It visits only archives whose cleanup is owed and due.
pub(crate) fn reap_archived_agents(store: &Store, data_dir: &Path, health: &HealthBoard) {
    reap_archived_agents_at(store, data_dir, health, now_ms());
}

pub(super) fn reap_archived_agents_at(
    store: &Store,
    data_dir: &Path,
    health: &HealthBoard,
    now: i64,
) {
    reap_queue::backfill(store, data_dir);
    for agent_id in reap_queue::due(data_dir, now, 32) {
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent_id) else {
            reap_queue::note_failure(data_dir, &agent_id, now);
            continue;
        };
        // A wake followed by a new self-archive may have started a process
        // since the archive. Check current health only after taking the lock.
        if health.get(&agent_id).is_some_and(|live| live.process_alive) {
            reap_queue::note_failure(data_dir, &agent_id, now);
            continue;
        }
        // Re-read under the lock: the queue may predate a direct wake.
        reap_archived_agent_locked(store, data_dir, &agent_id);
    }
}

pub(crate) fn clear_explicitly_archived(data_dir: &Path, agent_id: &anyagents_core::AgentId) {
    let cfg = anyagents_core::agent_config_dir(data_dir, agent_id);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let _ = std::fs::remove_file(cfg.join(".anyagents-archived"));
    reap_queue::dequeue(data_dir, agent_id);
}

pub(super) async fn archive_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let clean_name = name.trim().strip_prefix("agent:").unwrap_or(name.trim());
        let found = if clean_name.eq_ignore_ascii_case("self")
            && let Principal::Agent { ref agent_id, .. } = principal
        {
            state.store.agent(agent_id).map_err(ApiError::from_store)?
        } else {
            state
                .store
                .agent_by_name(clean_name)
                .map_err(ApiError::from_store)?
        };
        let is_self =
            matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id);
        if found.parent_id.is_none() && !is_self {
            require_operator_audited(&state.store, &principal, "agents.archive_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_operator_audited(&state.store, &principal, "agents.archive", &found.name)?;
        }
        let _lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let prev_schedules = state
            .store
            .list_schedules(Some(&found.id), true)
            .unwrap_or_default();
        let prev_turns = state
            .store
            .remote_turn_store()
            .list_active_for_agent(&found.id)
            .unwrap_or_default();
        let agent = state
            .store
            .set_agent_state(&found.id, "archived")
            .map_err(ApiError::from_store)?;
        for sched in &prev_schedules {
            crate::timer::cancel_schedule_timer(state.timer_backend.as_ref(), sched);
        }
        super::serverless_maintenance::cancel_all_agent_system_timers(
            &state.store,
            state.timer_backend.as_ref(),
            &found.id,
            &prev_turns,
        );
        finish_agent_archive_locked(&state, &agent, is_self)?;
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VerdictRequest {
    pub(super) verdict: String,
    #[serde(default, alias = "note")]
    pub(super) reason: Option<String>,
}

/// `POST /v1/agents/{name}/verdict`: record a supervision verdict (strict ancestor, root admin, or operator).
pub(super) async fn verdict_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<VerdictRequest>,
) -> Result<Json<anyagents_core::Outcome>, ApiError> {
    let dispatch_store = Arc::clone(&state.store);
    let dispatch_timer = Arc::clone(&state.timer_backend);
    let (outcome, rework_id) = spawn_store(move || {
        let kind = anyagents_core::OutcomeKind::parse_verdict(&body.verdict).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "verdict must be accepted, rework, rejected or abandoned",
            )
        })?;
        let s = &state.store;
        let clean = name.trim().strip_prefix("agent:").unwrap_or(name.trim());
        let target = s.agent_by_name(clean).map_err(ApiError::from_store)?;
        let _lock = s
            .lock_agent_lifecycle(&target.id)
            .map_err(ApiError::from_store)?;
        let cfg = anyagents_core::agent_config_dir(s.data_dir(), &target.id);
        let actor = match &principal {
            Principal::Operator => None,
            Principal::Agent { agent_id, .. } => Some(agent_id),
        };
        let prev_schedules = s.list_schedules(Some(&target.id), true).unwrap_or_default();
        let prev_turns = s
            .remote_turn_store()
            .list_active_for_agent(&target.id)
            .unwrap_or_default();
        let prof = create::bound_profile_name(&cfg);
        let (outcome, changed) = s
            .apply_verdict(actor, &target.id, kind, body.reason, prof)
            .map_err(ApiError::from_store)?;
        let mut rework_id = None;
        if let Some(agent) = changed {
            if agent.state == "archived" {
                for sched in &prev_schedules {
                    crate::timer::cancel_schedule_timer(state.timer_backend.as_ref(), sched);
                }
                super::serverless_maintenance::cancel_all_agent_system_timers(
                    &state.store,
                    state.timer_backend.as_ref(),
                    &target.id,
                    &prev_turns,
                );
                finish_agent_archive_locked(&state, &agent, false)?;
            } else {
                clear_explicitly_archived(s.data_dir(), &agent.id);
                rework_id = Some(agent.id);
            }
        }
        Ok((outcome, rework_id))
    })
    .await?;
    if serverless::is_serverless_mode()
        && let Some(id) = rework_id
    {
        let call = in_flight::carry();
        tokio::spawn(async move {
            let _call = call;
            if let Err(err) = serverless::dispatch_next_mailbox_message_with_timer(
                dispatch_store,
                &id,
                Some(dispatch_timer),
            )
            .await
            {
                eprintln!("anyagents-serverless: verdict rework dispatch failed for `{id}`: {err}");
            }
        });
    }
    Ok(Json(outcome))
}

fn finish_agent_archive_locked(
    state: &AppState,
    agent: &Agent,
    is_self: bool,
) -> Result<(), ApiError> {
    mark_explicitly_archived_locked(&state.store, state.store.data_dir(), agent)
        .map_err(ApiError::from_store)?;
    let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id);
    if let Err(err) = crate::sandbox_exec::release_sandbox_for_agent(&state.store, &agent.id, &cfg)
    {
        eprintln!("anyagents-sandbox: archive release failed: {err}");
    }
    let mut process_guard = None;
    if !is_self {
        let deadline = std::time::Instant::now() + anyagents_harness::STOP_DEADLINE;
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&agent.id)
                .is_none_or(|live| !live.process_alive)
                && let Ok(guard) = state.store.try_lock_agent(&agent.id)
            {
                process_guard = Some(guard);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        if process_guard.is_none()
            && state
                .health
                .get(&agent.id)
                .is_none_or(|live| !live.process_alive)
        {
            process_guard = state.store.try_lock_agent(&agent.id).ok();
        }
    }
    if let Some(process) = process_guard.as_ref() {
        reap_archived_agent_with_process_locked(
            &state.store,
            state.store.data_dir(),
            &agent.id,
            process,
        );
    }
    drop(process_guard);
    Ok(())
}

#[cfg(test)]
pub(crate) fn reap_subagent_ephemeral_dirs(
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    let Ok(store) = Store::open(data_dir.join("state.db")) else {
        return false;
    };
    reap_subagent_ephemeral_dirs_in(&store, data_dir, agent_id)
}

pub(super) fn reap_subagent_ephemeral_dirs_in(
    store: &Store,
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    // Archive reaping calls this with the row archived, and delete before it
    // removes the row; all surviving references still protect their
    // workdirs. A failed read must never mean no shared workdirs.
    let agent = match store.agent(agent_id) {
        Ok(agent) => Some(agent),
        Err(Error::AgentNotFound(_)) => None,
        Err(_) => return false,
    };
    let parent = match agent.as_ref().and_then(|agent| agent.parent_id.as_ref()) {
        Some(parent_id) => match store.agent(parent_id) {
            Ok(parent) => Some(parent),
            Err(Error::AgentNotFound(_)) => None,
            Err(_) => return false,
        },
        None => None,
    };
    let Ok(unarchived) = store.unarchived_workdirs() else {
        return false;
    };
    let agent = agent.as_ref();
    // Intentionally retained unknown work is a terminal preservation decision;
    // only failed cleanup operations need a retry.
    let mut complete = true;
    let agent_root = data_dir.join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let configured_work = anyagents_core::agent_work_dir(data_dir, agent_id);
    let legacy_work = anyagents_core::legacy_agent_work_dir(data_dir, agent_id);
    let seat_ids = detect_seat_ids(&config_dir)
        .or_else(|| detect_seat_ids(&configured_work))
        .or_else(|| detect_seat_ids(&legacy_work));
    // Native harness homes can contain the only session or learned-memory
    // copy. Remove only runtime-designated cache and temporary directories.
    for rel in ["cache-home", ".cache", "tmp"] {
        let target = config_dir.join(rel);
        complete &= remove_path_with_seat_under(data_dir, &target, seat_ids);
    }
    complete &= remove_path_with_seat_under(data_dir, &agent_root.join("tmp"), seat_ids);
    if let Some(scratch_dir) = anyagents_core::configured_scratch_dir() {
        complete &= remove_path_with_seat_under(
            &scratch_dir,
            &anyagents_core::agent_scratch_build_dir(&scratch_dir, agent_id),
            seat_ids,
        );
        complete &= remove_path_with_seat_under(
            &scratch_dir,
            &anyagents_core::agent_scratch_tmp_dir(&scratch_dir, agent_id),
            seat_ids,
        );
    }
    if agent.is_none() {
        let _ = std::fs::remove_file(
            data_dir
                .join("locks")
                .join(format!("{}.lock", agent_id.as_str())),
        );
    }
    let mut shared_workdirs = std::collections::HashSet::new();
    if let Some(agent) = agent {
        complete &= remove_path_with_seat_under(
            data_dir,
            &data_dir.join("browser-profiles").join(&agent.name),
            seat_ids,
        );
    }
    // Every other agent that is not archived, and the parent even when it is,
    // keeps its workdir.
    let protected = unarchived
        .iter()
        .filter(|(id, _)| id != agent_id)
        .map(|(_, workdir)| workdir.as_str())
        .chain(
            parent
                .as_ref()
                .filter(|parent| &parent.id != agent_id)
                .and_then(|parent| parent.workdir.as_deref()),
        );
    for wd in protected {
        let wd_path = PathBuf::from(wd);
        if let Ok(abs) = std::path::absolute(&wd_path) {
            shared_workdirs.insert(abs);
        }
        shared_workdirs.insert(wd_path);
    }
    let marker_path = config_dir.join(".anyagents-managed-worktree.json");
    let configured_work_base =
        anyagents_core::configured_workspaces_dir().unwrap_or_else(|| data_dir.to_path_buf());
    let configured_abs =
        std::path::absolute(&configured_work).unwrap_or_else(|_| configured_work.clone());
    let mut managed_work_dirs = vec![(configured_work_base, configured_abs)];
    let legacy_abs = std::path::absolute(&legacy_work).unwrap_or_else(|_| legacy_work.clone());
    if !managed_work_dirs.iter().any(|(_, dir)| dir == &legacy_abs) && legacy_abs.exists() {
        managed_work_dirs.push((data_dir.to_path_buf(), legacy_abs));
    }
    // Never reap an external `explicit-worktree` (`wt_path != &work_dir`) such as `/scratch/wt/...`
    // that a parent or operator created and lent to a child agent. Only reap the agent's own
    // managed workspace directory when not shared with another agent.
    let seat = seat_ids.map(|(uid, gid, _)| (uid, gid));
    for (work_base, work_dir) in managed_work_dirs {
        // A live agent may work inside this directory, or this one inside its.
        if shared_workdirs
            .iter()
            .any(|live| workdirs_overlap(live, &work_dir))
        {
            continue;
        }
        if !real_directory_path_under(&work_base, &work_dir) {
            continue;
        }
        let dotgit = work_dir.join(".git");
        if std::fs::symlink_metadata(&dotgit).is_ok_and(|meta| meta.is_file()) {
            // A linked worktree from before per-agent repositories: convert it
            // first, so the shared store forgets it and it is judged as one.
            if let crate::repo::Adoption::Declined(reason) = crate::repo::adopt(&work_dir, seat) {
                eprintln!(
                    "agent {agent_id}: keeping checkout {}: not converted: {reason}",
                    work_dir.display()
                );
                continue;
            }
        }
        if dotgit.symlink_metadata().is_ok() && is_git_worktree_safe_to_reap(&work_dir, seat) {
            // Keep pool recovery refs: removing them before checkout
            // deletion succeeds would discard recovery on an I/O error.
            if remove_path_with_seat_under(&work_base, &work_dir, seat_ids) {
                let _ = std::fs::remove_file(&marker_path);
            } else {
                complete = false;
            }
        }
        // Missing/unreadable Git metadata and non-Git work are unknown, not
        // disposable. A build-like name is not a recovery proof either.
    }
    complete
}

/// Reject symlinks and unreadable ancestors beneath a trusted root `base`
/// while tolerating host/system mount symlinks above `base` (such as macOS
/// `/var -> private/var` or `/tmp -> private/tmp`).
fn real_directory_path_under(base: &Path, path: &Path) -> bool {
    let (Ok(base_abs), Ok(path_abs)) = (std::path::absolute(base), std::path::absolute(path))
    else {
        return false;
    };
    if !std::fs::metadata(&base_abs).is_ok_and(|meta| meta.is_dir()) {
        return false;
    }
    let Ok(rel) = path_abs.strip_prefix(&base_abs) else {
        return false;
    };
    let mut cur = base_abs;
    for comp in rel.components() {
        match comp {
            std::path::Component::CurDir => {}
            std::path::Component::Normal(part) => {
                cur.push(part);
                if !std::fs::symlink_metadata(&cur).is_ok_and(|meta| meta.is_dir()) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Either directory contains the other, including aliases.
pub(super) fn workdirs_overlap(a: &Path, b: &Path) -> bool {
    let spellings = |path: &Path| -> Vec<PathBuf> {
        if path.as_os_str().is_empty() {
            return Vec::new();
        }
        let mut forms = vec![path.to_path_buf()];
        forms.extend(std::path::absolute(path).ok());
        forms.extend(std::fs::canonicalize(path).ok());
        forms
    };
    let (a, b) = (spellings(a), spellings(b));
    a.iter()
        .any(|x| b.iter().any(|y| x.starts_with(y) || y.starts_with(x)))
}

/// A checkout may go only when it is the agent's own repository and nothing
/// in it is the only copy of anything:
/// - no tracked change, untracked file, or ignored content;
/// - a born HEAD, and no submodule with a Git directory of its own;
/// - the remote holds everything else: no stash, no tag or note the remote
///   lacks, and no commit of any ref (every namespace, `refs/original` and
///   the like included) but remote-tracking ones that no advertised ref
///   reaches.
///
/// The proof is the remote's own answer, asked by the server; a checkout it
/// cannot ask about stays. Git reads the checkout as the agent's seat.
pub(crate) fn is_git_worktree_safe_to_reap(wt_path: &Path, seat: Option<(u32, u32)>) -> bool {
    // Only a `.git` directory: a pointer could name any other repository.
    let git_dir = wt_path.join(".git");
    if !std::fs::symlink_metadata(&git_dir).is_ok_and(|meta| meta.is_dir()) {
        return false;
    }
    // In-progress operations and locks can hold unique recovery state.
    for name in [
        "index.lock",
        "HEAD.lock",
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "rebase-apply",
        "rebase-merge",
        "sequencer",
        "BISECT_START",
        "AUTO_MERGE",
    ] {
        match std::fs::symlink_metadata(git_dir.join(name)) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            _ => return false,
        }
    }
    let git = || crate::repo::inspect_command(seat, &git_dir, Some(wt_path));
    let output = |args: &[&str]| {
        git().and_then(|mut cmd| cmd.args(args).stdin(std::process::Stdio::null()).output())
    };
    let Ok(status_out) = output(&[
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=normal",
        "--ignored=matching",
    ]) else {
        return false;
    };
    if !status_out.status.success() {
        return false;
    }
    if !status_out.stdout.is_empty() {
        return false;
    }
    // An unborn HEAD (`switch --orphan`) says nothing about the branches beside it.
    if !output(&["rev-parse", "--verify", "--quiet", "HEAD"]).is_ok_and(|out| out.status.success())
    {
        return false;
    }
    // A submodule with its own Git directory holds history no check here reads.
    if git_dir.join("modules").exists() {
        return false;
    }
    let Ok(stage) = output(&["ls-files", "--stage", "-z"]) else {
        return false;
    };
    if !stage.status.success() {
        return false;
    }
    let populated_gitlink = String::from_utf8_lossy(&stage.stdout)
        .split('\0')
        .filter_map(|entry| entry.strip_prefix("160000 "))
        .filter_map(|entry| entry.split_once('\t').map(|(_, path)| path))
        .any(|path| std::fs::symlink_metadata(wt_path.join(path).join(".git")).is_ok());
    if populated_gitlink {
        return false;
    }
    let Ok(local) = output(&[
        "for-each-ref",
        "--format=%(objectname) %(refname)",
        "refs/stash",
        "refs/tags",
        "refs/notes",
    ]) else {
        return false;
    };
    if !local.status.success() {
        return false;
    }
    let Some(advertised) = crate::repo::published(wt_path, seat) else {
        return false;
    };
    for line in String::from_utf8_lossy(&local.stdout).lines() {
        let Some((oid, name)) = line.split_once(' ') else {
            return false;
        };
        if name == "refs/stash"
            || !advertised
                .iter()
                .any(|(remote_oid, remote_name)| remote_oid == oid && remote_name == name)
        {
            return false;
        }
    }
    let covered: String = advertised
        .iter()
        .map(|(oid, _)| format!("^{oid}\n"))
        .collect();
    let Ok(mut unpublished) = git() else {
        return false;
    };
    unpublished.args([
        "rev-list",
        "--count",
        "--exclude=refs/remotes/*",
        "--all",
        "HEAD",
        "--reflog",
        "--ignore-missing",
        "--stdin",
    ]);
    crate::repo::output_with_input(&mut unpublished, &covered)
        .is_ok_and(|out| out.status.success() && out.stdout == b"0\n")
}

/// Determine the seat owning a runtime directory for permission-aware cleanup.
#[cfg(unix)]
fn detect_seat_ids(config_dir: &Path) -> Option<(u32, u32, u32)> {
    use std::os::unix::fs::MetadataExt as _;
    let (me, my_gid) = std::fs::metadata("/proc/self")
        .map(|m| (m.uid(), m.gid()))
        .unwrap_or((0, 0));
    let cfg_meta = std::fs::metadata(config_dir).ok()?;
    let gid = cfg_meta.gid();
    let sock_gid = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.gid())
        .unwrap_or(gid);
    if cfg_meta.uid() != me && cfg_meta.uid() != 0 {
        return Some((cfg_meta.uid(), gid, sock_gid));
    }
    if let Ok(entries) = std::fs::read_dir(config_dir) {
        for entry in entries.flatten() {
            if let Ok(m) = entry.metadata() {
                if m.uid() != me && m.uid() != 0 {
                    return Some((m.uid(), gid, sock_gid));
                }
                if m.is_dir()
                    && let Ok(sub) = std::fs::read_dir(entry.path())
                {
                    for child in sub.flatten() {
                        if let Ok(cm) = child.metadata()
                            && cm.uid() != me
                            && cm.uid() != 0
                        {
                            return Some((cm.uid(), gid, sock_gid));
                        }
                    }
                }
            }
        }
    }
    if gid != my_gid && gid != 0 {
        return Some((gid, gid, sock_gid));
    }
    None
}

#[cfg(not(unix))]
fn detect_seat_ids(_config_dir: &Path) -> Option<(u32, u32, u32)> {
    None
}

fn remove_path_with_seat_under(
    base: &Path,
    target: &Path,
    seat_ids: Option<(u32, u32, u32)>,
) -> bool {
    let meta = match std::fs::symlink_metadata(target) {
        Ok(meta) => meta,
        Err(error) => return error.kind() == std::io::ErrorKind::NotFound,
    };
    if !target
        .parent()
        .is_some_and(|parent| real_directory_path_under(base, parent))
    {
        return false;
    }
    if meta.file_type().is_symlink() || meta.is_file() {
        if std::fs::remove_file(target).is_ok() {
            return true;
        }
    } else if meta.is_dir() && std::fs::remove_dir_all(target).is_ok() {
        return true;
    }
    if !Path::new("/usr/local/bin/anyagents-setpriv").is_file() {
        return false;
    }
    let mut candidate_seats = Vec::new();
    if let Some(ids) = seat_ids {
        candidate_seats.push(ids);
    }
    #[cfg(unix)]
    if let Some(target_ids) = detect_seat_ids(target)
        && !candidate_seats.contains(&target_ids)
    {
        candidate_seats.push(target_ids);
    }
    for (uid, gid, sock_gid) in candidate_seats {
        let uid_s = uid.to_string();
        let gid_s = gid.to_string();
        let groups_s = if sock_gid != gid && sock_gid != 0 {
            format!("{gid},{sock_gid}")
        } else {
            gid_s.clone()
        };
        let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
            .args([
                "--reuid",
                &uid_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_s,
                "--no-new-privs",
                "--",
                "/bin/rm",
                "-rf",
            ])
            .arg(target)
            .stderr(std::process::Stdio::null())
            .status();
        if std::fs::remove_dir_all(target).is_ok()
            || std::fs::symlink_metadata(target)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return true;
        }
        let _ = std::fs::remove_file(target);
    }
    std::fs::symlink_metadata(target)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

pub(super) async fn pause_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot pause itself; pass a target agent name or id in `agent`",
                ));
            }
            require_operator_audited(&state.store, &principal, "agents.pause_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_operator_audited(&state.store, &principal, "agents.pause", &found.name)?;
        }
        // Serialized with this agent's harness, profile, wake and archive
        // transitions, as the command-line pause is.
        let lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        // Read again under the lock: an archive may have completed meanwhile,
        // and a pause must not bring an archived agent back as paused.
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        if found.state == "archived" {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                format!("agent {} is archived; wake it to resume", found.name),
            ));
        }
        let agent = state
            .store
            .set_agent_state(&found.id, "paused")
            .map_err(ApiError::from_store)?;
        super::serverless_maintenance::clear_dispatch_retry_state(
            &state.store,
            Some(state.timer_backend.as_ref()),
            &found.id,
        );
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &found.id);
        if let Err(err) =
            crate::sandbox_exec::release_sandbox_for_agent(&state.store, &found.id, &cfg)
        {
            eprintln!("anyagents-sandbox: pause release failed: {err}");
        }
        drop(lifecycle);
        let deadline = std::time::Instant::now() + anyagents_harness::STOP_DEADLINE;
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
                && state.store.try_lock_agent(&found.id).is_ok()
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

pub(super) async fn delete_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let _lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot delete itself",
                ));
            }
            require_operator_audited(&state.store, &principal, "agents.delete_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_operator_audited(&state.store, &principal, "agents.delete", &found.name)?;
        }
        let prev_schedules = state
            .store
            .list_schedules(Some(&found.id), true)
            .unwrap_or_default();
        let prev_turns = state
            .store
            .remote_turn_store()
            .list_active_for_agent(&found.id)
            .unwrap_or_default();
        state
            .store
            .set_agent_state(&found.id, "archived")
            .map_err(ApiError::from_store)?;
        for sched in &prev_schedules {
            crate::timer::cancel_schedule_timer(state.timer_backend.as_ref(), sched);
        }
        super::serverless_maintenance::cancel_all_agent_system_timers(
            &state.store,
            state.timer_backend.as_ref(),
            &found.id,
            &prev_turns,
        );
        let deadline = std::time::Instant::now() + anyagents_harness::STOP_DEADLINE;
        // Health is a projection: the supervisor may publish stopped before
        // its worker exits. Allow that normal shutdown to release actual
        // process custody within the same deadline, retaining the guard to commit.
        let _process = loop {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
            {
                match state.store.try_lock_agent(&found.id) {
                    Ok(process) => break process,
                    Err(anyagents_core::Error::AgentBusy) => {}
                    Err(error) => return Err(ApiError::from_store(error)),
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "agent is archived but its process has not stopped; identity and resources are retained",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        // Everything the agent's name or identity keys on disk goes before its
        // row: a stop between the two then leaves an archived row that holds
        // the name and a delete that can be run again, never a free name
        // whose next agent inherits the old browser profile or config.
        let agent_dir = state
            .store
            .data_dir()
            .join("agents")
            .join(found.id.as_str());
        let data_dir = state.store.data_dir();
        let config = agent_dir.join("config");
        let seat_ids = detect_seat_ids(&config);
        crate::sandbox_exec::destroy_sandbox_for_agent(&state.store, &found.id, &config)
            .map_err(|err| ApiError::new(StatusCode::CONFLICT, err))?;

        reap_subagent_ephemeral_dirs_in(&state.store, data_dir, &found.id);
        remove_agent_identity_files(data_dir, &found, &agent_dir, seat_ids);
        let deleted = state
            .store
            .delete_agent(&found.id)
            .map_err(ApiError::from_store)?;
        let _ = std::fs::remove_file(
            data_dir
                .join("locks")
                .join(format!("{}.lock", deleted.id.as_str())),

        );
        if !agent_dir.join("work").exists() {
            remove_path_with_seat_under(data_dir, &agent_dir, seat_ids);
            let _ = std::fs::remove_dir_all(&agent_dir);
        }
        Ok(Json(serde_json::json!({
            "id": deleted.id.as_str(),
            "name": deleted.name,
            "deleted": true,
        })))
    })
    .await
}

/// Remove what an agent being deleted keeps under its identity: its browser
/// profile (keyed by name, so a later agent of the same name would inherit
/// it) and its private config, memory and skills trees. Idempotent: a delete
/// stopped part way runs it again.
pub(super) fn remove_agent_identity_files(
    data_dir: &Path,
    agent: &Agent,
    agent_dir: &Path,
    seat_ids: Option<(u32, u32, u32)>,
) {
    remove_path_with_seat_under(
        data_dir,
        &data_dir.join("browser-profiles").join(&agent.name),
        seat_ids,
    );
    for private_tree in ["config", "memory", "skills"] {
        let tree_path = agent_dir.join(private_tree);
        remove_path_with_seat_under(data_dir, &tree_path, seat_ids);
        let _ = std::fs::remove_dir_all(&tree_path);
    }
}

struct CancelWakeOnDrop(Arc<std::sync::atomic::AtomicBool>);

impl Drop for CancelWakeOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

const WAKE_RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) async fn wake_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<AgentView>, ApiError> {
    let via_http = via_http_mcp(&headers);
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_guard = CancelWakeOnDrop(Arc::clone(&cancelled));
    let dispatch_store = Arc::clone(&state.store);
    let dispatch_timer = Arc::clone(&state.timer_backend);
    let out: Result<(Json<AgentView>, anyagents_core::AgentId), ApiError> =
        spawn_store(move || {
            use std::sync::atomic::Ordering;
            let found = state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?;
            if via_http && !runs_off_host(&state.store, &found) {
                return Err(ApiError::forbidden(format!(
                    "`{}` runs on this host; a call over HTTP MCP cannot wake it",
                    found.name
                )));
            }
            if let Principal::Agent {
                agent_id: ref caller_id,
                name: ref caller_name,
            } = principal
            {
                let actor = Actor::Agent(caller_name.clone());
                if !is_self_or_ancestor(&state.store, caller_id, &found)
                    && can_actor_message_agent(&state.store, &actor, &found).is_err()
                {
                    require_operator_audited(&state.store, &principal, "agents.wake", &found.name)?;
                }
            }
            let _lifecycle = state
                .store
                .lock_agent_lifecycle(&found.id)
                .map_err(ApiError::from_store)?;
            if cancelled.load(Ordering::SeqCst) {
                return Err(ApiError::conflict("wake recovery cancelled"));
            }
            let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
            let now = now_ms();
            let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &found.id);
            let files = file_cache::cache();
            files.catch_up();
            let markers = Markers { files, cfg: &cfg };
            let rotated_ms = markers.ms(PROFILE_ROTATED);
            let (session_quota, session_error) =
                persisted_fault(&found.harness, &markers, rotated_ms, now);
            let live = state.health.get(&found.id);
            let live_quota_active = live
                .as_ref()
                .and_then(|l| l.quota_reset_at_ms)
                .is_some_and(|ms| ms > now);
            let persisted_quota_active = session_quota.is_some_and(|ms| ms > now);
            let live_err = live.as_ref().and_then(|l| l.last_error.clone());
            let live_err_kind = live_err
                .as_deref()
                .and_then(|err| compute_block_reason(None, Some(err), now));
            let persisted_err_kind = session_error
                .as_deref()
                .and_then(|err| compute_block_reason(None, Some(err), now));
            let live_delivery_err = if live_err_kind.as_deref() == Some("delivery_error") {
                live_err.clone()
            } else {
                None
            };
            let persisted_delivery_err = persisted_err_kind.as_deref() == Some("delivery_error");
            let has_unrelated_fault = live_quota_active
                || persisted_quota_active
                || matches!(
                    live_err_kind.as_deref(),
                    Some("sign_in_required" | "quota_exhausted" | "memory_migration_wait")
                )
                || matches!(
                    persisted_err_kind.as_deref(),
                    Some("sign_in_required" | "quota_exhausted" | "memory_migration_wait")
                );

            if live_delivery_err.is_some() || persisted_delivery_err {
                if let Some(ref expected) = live_delivery_err {
                    state
                        .health
                        .stop_for_wake_cancellable(
                            &found.id,
                            anyagents_supervisor::WakeFault::Error(expected),
                            WAKE_RECOVERY_TIMEOUT,
                            &cancelled,
                        )
                        .map_err(ApiError::conflict)?;
                }
                if cancelled.load(Ordering::SeqCst) {
                    return Err(ApiError::conflict("wake recovery cancelled"));
                }
                let custody = match state.store.try_lock_agent(&found.id) {
                    Ok(custody) => custody,
                    // This server's own worker keeps the lock through a restart
                    // backoff or quota hold after its process died. Retire that
                    // exact generation with checked custody, as for a live fault;
                    // a lock held by anything else (a live process, a keeper or
                    // another server) is still refused.
                    Err(anyagents_core::Error::AgentBusy)
                        if live_delivery_err.is_none()
                            && live.as_ref().is_some_and(|l| !l.process_alive) =>
                    {
                        state
                            .health
                            .stop_for_wake_cancellable(
                                &found.id,
                                anyagents_supervisor::WakeFault::ProcessDown(live_err.as_deref()),
                                WAKE_RECOVERY_TIMEOUT,
                                &cancelled,
                            )
                            .map_err(ApiError::conflict)?;
                        if cancelled.load(Ordering::SeqCst) {
                            return Err(ApiError::conflict("wake recovery cancelled"));
                        }
                        state.store.try_lock_agent(&found.id).map_err(|_| {
                            ApiError::conflict(
                                "agent process lock is still held; refusing wake recovery",
                            )
                        })?
                    }
                    Err(_) => {
                        return Err(ApiError::conflict(
                            "agent process lock is still held; refusing wake recovery",
                        ));
                    }
                };
                let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
                if let Some(ref expected) = live_delivery_err {
                    state
                        .health
                        .clear_wake_fault(&found.id, expected)
                        .map_err(ApiError::conflict)?;
                }
                prepare_agent_wake_with_options(
                    &state.store,
                    &found,
                    persisted_delivery_err || session_error.is_none(),
                    false,
                    !has_unrelated_fault,
                )?;
                drop(custody);
                let agent = state
                    .store
                    .set_agent_state(&found.id, "running")
                    .map_err(ApiError::from_store)?;
                create::poke_agent_wake_socket(state.store.data_dir(), &agent.id);
                return Ok((
                    Json(agent_view(&state.store, &state.health, &agent)?),
                    agent.id,
                ));
            }

            if found.state == "running" {
                super::serverless_maintenance::clear_dispatch_retry_state(
                    &state.store,
                    Some(state.timer_backend.as_ref()),
                    &found.id,
                );
                create::poke_agent_wake_socket(state.store.data_dir(), &found.id);
                return Ok((
                    Json(agent_view(&state.store, &state.health, &found)?),
                    found.id,
                ));
            }

            super::serverless_maintenance::clear_dispatch_retry_state(
                &state.store,
                Some(state.timer_backend.as_ref()),
                &found.id,
            );
            prepare_agent_wake_with_options(
                &state.store,
                &found,
                !has_unrelated_fault,
                !has_unrelated_fault,
                !has_unrelated_fault,
            )?;
            let agent = state
                .store
                .set_agent_state(&found.id, "running")
                .map_err(ApiError::from_store)?;
            create::poke_agent_wake_socket(state.store.data_dir(), &agent.id);
            Ok((
                Json(agent_view(&state.store, &state.health, &agent)?),
                agent.id,
            ))
        })
        .await;
    drop(cancel_guard);
    let (view, agent_id) = out?;
    if serverless::is_serverless_mode() {
        let call = in_flight::carry();
        tokio::spawn(async move {
            let _call = call;
            if let Err(err) = serverless::dispatch_next_mailbox_message_with_timer(
                dispatch_store,
                &agent_id,
                Some(dispatch_timer),
            )
            .await
            {
                eprintln!(
                    "anyagents-serverless: wake dispatch failed for `{}`: {err}",
                    agent_id.as_str()
                );
            }
        });
    }
    Ok(view)
}

/// Restore runtime and archive-marker state for an explicit wake. The caller
/// commits `running` only after this succeeds.
fn prepare_agent_wake_with_options(
    store: &Store,
    found: &Agent,
    clear_delivery_fault: bool,
    clear_quota: bool,
    rotate_profile: bool,
) -> Result<(), ApiError> {
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &found.id);
    if !crate::remote::is_agent_cloud(found, Some(store.data_dir()), Some(&cfg)) {
        let remote_workdir = resolve_remote_sandbox_workdir(store, found);
        match crate::sandbox_exec::ensure_sandbox_for_agent(
            store,
            found,
            &cfg,
            Some(remote_workdir.as_str()),
            &std::collections::BTreeMap::new(),
        ) {
            Ok(Some(_)) => {}
            Ok(None) => {
                // Only dispatch legacy HTTP resume when no leased/registry provider handled wake.
                if let Some(sbx) = crate::sandbox_providers::sandbox_config_from_config_dir(&cfg) {
                    let _ = agent_tools::SandboxProvider::resume(&sbx);
                }
            }
            Err(err) => {
                if crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
                    || !matches!(store.sandbox_credential_ref(&found.id), Ok(None))
                {
                    return Err(ApiError::new(StatusCode::BAD_GATEWAY, err));
                }
                eprintln!("anyagents-sandbox: wake lease ensure failed: {err}");
            }
        }
    }
    let work_dir = found
        .workdir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| anyagents_core::agent_work_dir(store.data_dir(), &found.id));
    if !work_dir.exists() {
        let _ = std::fs::create_dir_all(&work_dir);
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        || std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some()
    {
        create::reconcile_agent_runtime_config(store, store.data_dir(), found, &cfg, &work_dir)
            .map_err(|err| ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, err.to_string()))?;
    }
    if clear_delivery_fault {
        let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        let _ = std::fs::remove_file(cfg.join(".anyagents-notified-parent-error.json"));
    }
    let now = now_ms();
    if clear_quota {
        create::write_profile_marker(&cfg, ".anyagents-quota-cleared-ms", &format!("{now}\n"))
            .map_err(ApiError::from_store)?;
    }
    if rotate_profile && !store.has_open_turn(&found.id).unwrap_or(false) {
        create::write_profile_marker(&cfg, ".anyagents-profile-rotated-ms", &format!("{now}\n"))
            .map_err(ApiError::from_store)?;
    }
    clear_explicitly_archived(store.data_dir(), &found.id);
    // The gateway starts with routes for the live fleet only.
    if found.state == "archived"
        && let Err(err) = crate::gateway::add_route(store.data_dir(), found)
    {
        eprintln!("agent {}: gateway route not restored: {err}", found.id);
    }
    Ok(())
}
