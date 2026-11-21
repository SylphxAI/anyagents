use super::super::agents_lifecycle::{
    archive_agent_route, delete_agent_route, mark_archive_generation_locked,
    reap_archived_agents_at, reap_subagent_ephemeral_dirs_in, remove_agent_identity_files,
    wake_agent_route,
};
use super::super::*;
use super::helpers::{
    archetype_validation_state, authored_send, env_lock, mark_explicitly_archived,
    single_test_worker,
};
use anyagents_core::{Agent, Error, now_ms};
use axum::Json;
use std::path::PathBuf;

pub(super) fn archived_reap_fixture(store: &Store, name: &str, parent: &Agent) -> Agent {
    let mut new = anyagents_core::NewAgent::new(name, "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let _lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
    let child = store.set_agent_state(&child.id, "archived").unwrap();
    mark_archive_generation_locked(store, store.data_dir(), &child);
    child
}

#[test]
fn archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive() {
    if single_test_worker(
        module_path!(),
        "archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive",
    ) {
        return;
    }
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let state = archetype_validation_state(dir.path());
    let store = state.store.as_ref();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap();
    std::fs::write(cfg.join(".anyagents-reaped"), b"foreign generation").unwrap();
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    reap_archived_agents(store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        generation
    );
    // A fresh process must use durable completion, not rescan retained dirs.
    std::fs::create_dir_all(cfg.join("cache-home")).unwrap();
    let restarted = Store::open(&db).unwrap();
    reap_archived_agents(&restarted, dir.path(), &HealthBoard::default());
    assert!(cfg.join("cache-home").exists());
    let stale_child = restarted.agent(&child.id).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let Json(woken) = rt
        .block_on(wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(child.name.clone()),
            axum::http::HeaderMap::new(),
        ))
        .unwrap_or_else(|e| panic!("{}", e.message));
    assert_eq!(woken.state, "running");
    assert!(!cfg.join(".anyagents-reaped").exists());
    assert!(!cfg.join(".anyagents-archived").exists());
    {
        let _lifecycle = restarted.lock_agent_lifecycle(&stale_child.id).unwrap();
        reap_archived_agent_locked(&restarted, dir.path(), &stale_child.id);
    }
    assert!(
        cfg.join("cache-home").exists(),
        "stale cleanup deleted running config"
    );
    assert_eq!(store.agent(&child.id).unwrap().state, "running");
    mark_explicitly_archived(store, dir.path(), &child);
    assert_ne!(
        std::fs::read(cfg.join(".anyagents-archived")).unwrap(),
        generation
    );
    assert!(!cfg.join(".anyagents-reaped").exists());
    reap_archived_agents(store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join("cache-home").exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(cfg.join(".anyagents-archived")).unwrap()
    );
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archive_reap_repairs_empty_legacy_marker_and_retries_partial_failure() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let other = archived_reap_fixture(&store, "other", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let other_cfg = anyagents_core::agent_config_dir(dir.path(), &other.id);
    std::fs::write(cfg.join(".anyagents-archived"), b"").unwrap();
    let blocked = cfg.join(".cache");
    std::fs::create_dir_all(&blocked).unwrap();
    std::fs::write(blocked.join("keep"), b"partial cleanup").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let failed_at = now_ms();
    reap_archived_agents_at(&store, dir.path(), &HealthBoard::default(), failed_at);
    // Restore access before asserting so failed tests don't leak fixtures.
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !std::fs::read(cfg.join(".anyagents-archived"))
            .unwrap()
            .is_empty()
    );
    assert!(
        !cfg.join(".anyagents-reaped").exists(),
        "failed cleanup was marked complete"
    );
    assert!(
        other_cfg.join(".anyagents-reaped").exists(),
        "failure starved an independent archive"
    );
    // A failed cleanup waits out its backoff instead of the next minute.
    reap_archived_agents_at(
        &store,
        dir.path(),
        &HealthBoard::default(),
        failed_at + 30_000,
    );
    assert!(blocked.exists(), "retried before its backoff");
    assert!(!cfg.join(".anyagents-reaped").exists());
    reap_archived_agents_at(
        &store,
        dir.path(),
        &HealthBoard::default(),
        now_ms() + 60_000,
    );
    assert!(!blocked.exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(cfg.join(".anyagents-archived")).unwrap()
    );
}

#[test]
fn reap_queue_backfills_once_and_idle_maintenance_visits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    // Finished under an earlier binary: its marker matches its archive.
    let finished = archived_reap_fixture(&store, "finished", &parent);
    let finished_cfg = anyagents_core::agent_config_dir(dir.path(), &finished.id);
    std::fs::copy(
        finished_cfg.join(".anyagents-archived"),
        finished_cfg.join(".anyagents-reaped"),
    )
    .unwrap();
    // Archived by an earlier binary that wrote no marker at all.
    let mut new = anyagents_core::NewAgent::new("legacy", "opencode");
    new.parent_id = Some(parent.id.clone());
    let legacy = store.create_agent(new).unwrap();
    store.set_agent_state(&legacy.id, "archived").unwrap();
    let legacy_cfg = anyagents_core::agent_config_dir(dir.path(), &legacy.id);
    // Forget what this binary queued, as if the archives predated the queue.
    std::fs::remove_dir_all(dir.path().join("reap-pending")).unwrap();
    for cfg in [&finished_cfg, &legacy_cfg] {
        std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    }

    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(
        finished_cfg.join(".cache").exists(),
        "finished archive revisited"
    );
    assert!(
        !legacy_cfg.join(".cache").exists(),
        "legacy archive not backfilled"
    );
    assert_eq!(
        std::fs::read(legacy_cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(legacy_cfg.join(".anyagents-archived")).unwrap()
    );

    // Nothing is owed now, so maintenance visits no agent at all.
    std::fs::create_dir_all(legacy_cfg.join(".cache")).unwrap();
    std::fs::remove_file(finished_cfg.join(".anyagents-reaped")).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(legacy_cfg.join(".cache").exists());
    assert!(finished_cfg.join(".cache").exists());
    assert!(reap_queue::due(dir.path(), i64::MAX, usize::MAX).is_empty());
}

#[test]
fn roots_and_role_bearing_children_can_self_archive_without_losing_browser_profiles() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (name, parent_id, role) in [
            ("root", None, Some("Researcher")),
            (
                "labelled-child",
                Some(parent.id.clone()),
                Some("Researcher"),
            ),
            ("plain-child", Some(parent.id.clone()), None),
        ] {
            let mut new = anyagents_core::NewAgent::new(name, "codex");
            new.parent_id = parent_id;
            new.role = role.map(str::to_string);
            let agent = store.create_agent(new).unwrap();
            let profile = dir.path().join("browser-profiles").join(name);
            std::fs::create_dir_all(&profile).unwrap();
            std::fs::write(profile.join("login"), b"preserved session").unwrap();
            let caller = Principal::Agent {
                agent_id: agent.id.clone(),
                name: name.into(),
            };
            let Json(archived) =
                archive_agent_route(caller, State(state.clone()), AxumPath("self".into()))
                    .await
                    .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(archived.agent.state, "archived");
            assert_eq!(store.agent(&agent.id).unwrap().role, agent.role);
            assert_eq!(
                std::fs::read(profile.join("login")).unwrap(),
                b"preserved session"
            );
            let _ = delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(name.into()),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert!(!profile.exists(), "delete must remove the profile");
        }
    });
}

#[test]
fn archive_reap_waits_for_self_archived_process_to_stop() {
    if single_test_worker(
        module_path!(),
        "archive_reap_waits_for_self_archived_process_to_stop",
    ) {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    let health = HealthBoard::default();
    let mut live = anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: anyagents_harness::TurnState::Idle,
        delivery: Some(crate::vendor::declared_delivery(&child.harness)),
        quota_reset_at_ms: None,
        restart_at_ms: None,
        pid: None,
        live_harness: Some(child.harness.clone()),
        live_model: None,
        process_started_at_ms: None,
        last_error: None,
        retry_at_ms: None,
    };
    health.publish(&child.id, live.clone());
    reap_archived_agents(&store, dir.path(), &health);
    assert!(cfg.join(".cache").exists());
    assert!(!cfg.join(".anyagents-reaped").exists());
    live.process_alive = false;
    health.publish(&child.id, live);
    reap_queue::enqueue(dir.path(), &child.id);
    reap_archived_agents(&store, dir.path(), &health);
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_failed_protection_read_and_delete_preserve_unknown_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("keep"), b"protected").unwrap();
    // Simulate an unreadable/corrupt protection database, not an empty list.
    let failed = tempfile::tempdir().unwrap();
    let failed_work = anyagents_core::agent_work_dir(failed.path(), &child.id);
    std::fs::create_dir_all(&failed_work).unwrap();
    std::fs::write(failed_work.join("keep"), b"unknown references").unwrap();
    std::fs::write(failed.path().join("state.db"), b"not a database").unwrap();
    assert!(!reap_subagent_ephemeral_dirs(failed.path(), &child.id));
    assert!(failed_work.join("keep").exists());
    // A live sibling sharing this workdir protects it. Deleting the target
    // row still invokes raw cleanup, but must retain surviving references.
    let sibling = store
        .create_agent(anyagents_core::NewAgent::new("sibling", "opencode"))
        .unwrap();
    store
        .set_agent_running(&sibling.id, work.to_str().unwrap())
        .unwrap();
    store.delete_agent(&child.id).unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(work.join("keep").exists());
    store.delete_agent(&sibling.id).unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(std::fs::read(work.join("keep")).unwrap(), b"protected");
}

#[test]
fn archive_reap_requires_process_custody_even_when_health_is_absent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    let custody = store.try_lock_agent(&child.id).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(cfg.join(".cache").exists());
    assert!(!cfg.join(".anyagents-reaped").exists());
    drop(custody);
    reap_queue::enqueue(dir.path(), &child.id);
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_preserves_native_homes_memory_sessions_and_unknown_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let files: Vec<_> = [
        ".claude/projects/session.json",
        ".codex/history.jsonl",
        "state-home/learned.md",
        ".config/settings",
        "data-home/sessions/a",
    ]
    .map(|p| cfg.join(p))
    .into_iter()
    .chain(
        [
            "memory/MEMORY.md",
            "sessions/transcript.json",
            "target/report",
        ]
        .map(|p| work.join(p)),
    )
    .collect();
    for file in &files {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, b"only copy").unwrap();
    }
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    for file in &files {
        assert_eq!(std::fs::read(file).unwrap(), b"only copy");
    }
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_drains_at_most_32_per_pass() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let children: Vec<_> = (0..65)
        .map(|i| archived_reap_fixture(&store, &format!("worker-{i}"), &parent))
        .collect();
    for remaining in [33, 1, 0] {
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert_eq!(
            reap_queue::due(dir.path(), i64::MAX, usize::MAX).len(),
            remaining
        );
        let done = children
            .iter()
            .filter(|a| {
                anyagents_core::agent_config_dir(dir.path(), &a.id)
                    .join(".anyagents-reaped")
                    .exists()
            })
            .count();
        assert_eq!(done, 65 - remaining);
    }
}

#[test]
fn archive_reap_blocked_in_git_does_not_delay_other_mail() {
    if single_test_worker(
        module_path!(),
        "archive_reap_blocked_in_git_does_not_delay_other_mail",
    ) {
        return;
    }
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Store::open(&db).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "slow-worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let recipient = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
        .unwrap();
    store
        .receive(&recipient.id, anyagents_core::Incoming::new("pending mail"))
        .unwrap();
    store.set_agent_state(&recipient.id, "running").unwrap();
    let entered = dir.path().join("git-entered");
    let release = dir.path().join("git-release");
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    // Delegate to the same Git that prepared the fixture (Homebrew on
    // macOS), preserving all arguments and stdin for validation probes.
    let real_git = std::process::Command::new("/bin/sh")
        .args(["-c", "command -v git"])
        .output()
        .unwrap();
    assert!(real_git.status.success());
    let real_git = String::from_utf8(real_git.stdout).unwrap();
    let real_git = real_git.trim();
    assert!(Path::new(real_git).is_absolute());
    let git = bin.join("git");
    crate::repo::tests::write_executable(
        &git,
        &format!(
            r#"#!/bin/sh
# Parse in a subshell so non-barrier calls retain the original argv.
if (
while [ "$#" -gt 0 ]; do
    case "$1" in
        -c|--git-dir) shift 2 ;;
        --no-optional-locks) shift ;;
        --work-tree|-C) cwd="$2"; shift 2 ;;
        *) break ;;
    esac
done
[ "$cwd" = '{}' ] && [ "$1" = status ]
); then
touch '{}'
n=0
while [ ! -e '{}' ] && [ "$n" -lt 1000 ]; do sleep 0.01; n=$((n+1)); done
exit 1
fi
exec '{}' "$@"
"#,
            work.display(),
            entered.display(),
            release.display(),
            real_git
        ),
    );
    let previous_path = std::env::var_os("PATH");
    let path =
        std::env::join_paths([bin, PathBuf::from("/usr/bin"), PathBuf::from("/bin")]).unwrap();
    unsafe {
        std::env::set_var("PATH", path);
    }
    let worker_store = Store::open(&db).unwrap();
    let worker_data = dir.path().to_path_buf();
    let worker = std::thread::spawn(move || {
        reap_archived_agents(&worker_store, &worker_data, &HealthBoard::default())
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !entered.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let was_entered = entered.exists();
    let owns_lifecycle = matches!(
        store.try_lock_agent_lifecycle(&child.id),
        Err(Error::AgentBusy)
    );
    let rec_store = Store::open(&db).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let scanner = std::thread::spawn(move || {
        authored_send(
            &rec_store,
            &Actor::Operator("operator".into()),
            "recipient",
            "unrelated mail",
            "during-reap",
        );
        done_tx.send(()).unwrap();
    });
    let progressed_while_blocked = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .is_ok();
    let recipient_running = store.agent(&recipient.id).unwrap().state == "running";
    std::fs::write(&release, b"release").unwrap();
    worker.join().unwrap();
    scanner.join().unwrap();
    unsafe {
        if let Some(path) = previous_path {
            std::env::set_var("PATH", path);
        } else {
            std::env::remove_var("PATH");
        }
    }
    assert!(was_entered, "reaper never reached the git barrier");
    assert!(
        owns_lifecycle,
        "barrier did not observe reaper lifecycle ownership"
    );
    assert!(
        progressed_while_blocked && recipient_running,
        "unrelated mail delivery waited for blocked cleanup"
    );
}

#[test]
fn reap_subagent_ephemeral_dirs_preserves_dirty_worktree_including_build_names() {
    let data = tempfile::tempdir().unwrap();
    let agent_id = anyagents_core::AgentId::generate();
    let agent_root = data.path().join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let work_dir = agent_root.join("work");
    std::fs::create_dir_all(config_dir.join(".gemini/antigravity-cli/brain")).unwrap();
    std::fs::write(
        config_dir.join(".gemini/antigravity-cli/brain/transcript.jsonl"),
        b"large transcript",
    )
    .unwrap();
    std::fs::create_dir_all(config_dir.join(".cache/sub")).unwrap();
    std::fs::create_dir_all(agent_root.join("tmp/sub")).unwrap();
    std::fs::create_dir_all(work_dir.join(".git")).unwrap();
    std::fs::create_dir_all(work_dir.join("target/debug")).unwrap();
    std::fs::create_dir_all(work_dir.join("apps/web/node_modules/pkg")).unwrap();
    std::fs::write(work_dir.join("dirty.rs"), b"fn main() {}").unwrap();

    reap_subagent_ephemeral_dirs(data.path(), &agent_id);

    assert_eq!(
        std::fs::read(config_dir.join(".gemini/antigravity-cli/brain/transcript.jsonl")).unwrap(),
        b"large transcript"
    );
    assert!(!config_dir.join(".cache").exists());
    assert!(!agent_root.join("tmp").exists());
    // Dirty worktree contents are preserved regardless of build-like names:
    assert!(work_dir.join("dirty.rs").exists());
    assert!(work_dir.join("target").exists());
    assert!(work_dir.join("apps/web/node_modules").exists());
}

#[cfg(unix)]
#[test]
fn reap_and_delete_tolerate_symlinked_host_mount_roots_and_reject_inner_symlinks() {
    use std::os::unix::fs::symlink;
    let phys = tempfile::tempdir().unwrap();
    let real_data = phys.path().join("private/var/folders/data");
    let outside = phys.path().join("outside-keep");
    std::fs::create_dir_all(&real_data).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("precious"), b"must stay").unwrap();
    let var_alias = phys.path().join("var");
    symlink(phys.path().join("private/var"), &var_alias).unwrap();
    let data = var_alias.join("folders/data");

    let store = Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(&data, &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    std::fs::create_dir_all(cfg.join("cache-home")).unwrap();
    std::fs::create_dir_all(cfg.join("tmp")).unwrap();
    reap_archived_agents(&store, &data, &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert!(!cfg.join("cache-home").exists());
    assert!(!cfg.join("tmp").exists());
    assert!(cfg.join(".anyagents-reaped").exists());

    // A symlink planted beneath `data_dir` is still rejected fail-closed.
    let tampered = archived_reap_fixture(&store, "tampered", &parent);
    let tampered_root = data.join("agents").join(tampered.id.as_str());
    std::fs::remove_dir_all(&tampered_root).unwrap_or(());
    std::fs::create_dir_all(&tampered_root).unwrap();
    std::fs::create_dir_all(outside.join(".cache")).unwrap();
    symlink(&outside, tampered_root.join("config")).unwrap();
    assert!(!reap_subagent_ephemeral_dirs(&data, &tampered.id));
    assert!(outside.join(".cache").exists());
    assert_eq!(
        std::fs::read(outside.join("precious")).unwrap(),
        b"must stay"
    );

    // Identity removal also works under symlinked host mount roots.
    let shopper = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    let shopper_dir = data.join("agents").join(shopper.id.as_str());
    let profile = data.join("browser-profiles").join("shopper");
    std::fs::create_dir_all(shopper_dir.join("config")).unwrap();
    std::fs::create_dir_all(profile.join("user-data")).unwrap();
    remove_agent_identity_files(&data, &shopper, &shopper_dir, None);
    assert!(!profile.exists());
    assert!(!shopper_dir.join("config").exists());
}

#[test]
fn reap_subagent_ephemeral_dirs_removes_browser_profiles_for_reaped_agent() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "ephem-browser",
            "claude-code",
        ))
        .unwrap();
    let bp = data.join("browser-profiles").join(&agent.name);
    std::fs::create_dir_all(&bp).unwrap();
    std::fs::write(bp.join("cookies.sqlite"), b"db").unwrap();
    store.set_agent_state(&agent.id, "archived").unwrap();

    reap_subagent_ephemeral_dirs_in(&store, data, &agent.id);
    assert!(
        !bp.exists(),
        "browser-profiles/{} must be reaped",
        agent.name
    );
}
