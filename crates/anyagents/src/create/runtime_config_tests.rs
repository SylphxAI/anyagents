use anyagents_core::{NewAgent, agent_config_dir, agent_work_dir};

#[cfg(unix)]
use super::archetypes::read_archetype_file;
use super::credentials::{
    CredentialChange, OAUTH_REFRESH_ATTEMPT_FILE, OAUTH_REFRESH_RESERVATION_FILE,
    copy_profile_tree, persist_refreshed_credentials, propagate_credentials,
    refresh_anthropic_profile_oauth_if_due,
};
use super::profile_rotation::{
    rebalance_provider_profiles, select_or_bind_profile, try_rotate_exhausted_profile_in,
};
use super::profiles::ensure_provider_profile_meta;
use super::quota::{
    observed_agy_tier, observed_fraction_pct, observed_remaining_pct, observed_window_min,
};
#[cfg(unix)]
use super::signin::signin_staging_owner_exited;
use super::signin::validate_staged_credentials;
use super::spawn::{create_with_consumer, create_with_seat_and_sandbox_mode};
use super::*;

#[test]
fn required_writes_markers_and_refresh_recovery_report_destination_failures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("required");
    write_required_file(&path, b"first", 0o600).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"first");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(write_required_file(&path, b"second", 0o600).is_err());
    assert!(path.is_dir());
    write_profile_marker(dir.path(), ".anyagents-profile", "provider/one\n").unwrap();
    assert_eq!(
        bound_profile_name(dir.path()).as_deref(),
        Some("provider/one")
    );
    std::fs::create_dir(dir.path().join("blocked-marker")).unwrap();
    assert!(write_profile_marker(dir.path(), "blocked-marker", "provider/two\n").is_err());
    std::fs::create_dir(dir.path().join(".credentials.json")).unwrap();
    assert!(
        persist_refreshed_credentials(dir.path(), &[".credentials.json"], b"replacement-token")
            .is_err()
    );
    assert_eq!(
        std::fs::read(dir.path().join(".anyagents-pending-credentials.json")).unwrap(),
        b"replacement-token"
    );
    std::fs::remove_dir(dir.path().join(".credentials.json")).unwrap();
    persist_refreshed_credentials(dir.path(), &[".credentials.json"], b"replacement-token")
        .unwrap();
    assert!(
        !dir.path()
            .join(".anyagents-pending-credentials.json")
            .exists()
    );
}

#[test]
fn concurrent_instruction_renders_publish_one_complete_set_of_mirrors() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "concurrent-render",
            "claude-code",
        ))
        .unwrap();
    let config = agent_config_dir(dir.path(), &agent.id);
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for writer in 0..4 {
            let mut agent = agent.clone();
            agent.description = Some(format!("CONCURRENT_RENDER_{writer}"));
            let config = &config;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..8 {
                    write_agent_instructions(config, &agent, None).unwrap();
                }
            });
        }
    });
    let system = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
    let last = (0..4)
        .map(|writer| format!("CONCURRENT_RENDER_{writer}"))
        .find(|marker| system.contains(marker))
        .unwrap();
    for name in [
        "AGENTS.md",
        "CLAUDE.md",
        ".claude/CLAUDE.md",
        "opencode/AGENTS.md",
        ".config/opencode/AGENTS.md",
        ".gemini/config/GEMINI.md",
        "muse/AGENTS.md",
        ".config/muse/AGENTS.md",
        ".omp/agent/APPEND_SYSTEM.md",
        ".omp/agent/AGENTS.md",
    ] {
        let mirror = std::fs::read_to_string(config.join(name)).unwrap();
        assert!(mirror.contains(&last), "{name} contains another render");
    }
}

#[test]
fn prompt_and_cloud_marker_failures_are_not_acknowledged_as_synced() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let config = agent_config_dir(dir.path(), &agent.id);
    write_agent_instructions(&config, &agent, None).unwrap();
    let text = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(!text.contains("## Memory policy"));
    agent.launch_env.push((
        "ANYAGENTS_MEMORY_GUIDANCE".into(),
        "Use the operator-selected facts source.".into(),
    ));
    write_agent_instructions(&config, &agent, None).unwrap();
    assert!(
        std::fs::read_to_string(config.join("SYSTEM_PROMPT.md"))
            .unwrap()
            .contains("Use the operator-selected facts source.")
    );
    std::fs::remove_file(config.join("SYSTEM_PROMPT.md")).unwrap();
    std::fs::create_dir(config.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(sync_agent_description_files(&store, &agent, &config).is_err());
    provision_cloud_sandbox_worktree_marker(
        &store,
        &agent,
        &config,
        "https://git.example.test/team/repo",
        Some("feature"),
        Some("main"),
    )
    .unwrap();
    let marker = config.join(".anyagents-managed-worktree.json");
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    assert_eq!(value["branch"], "feature");
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    assert!(
        provision_cloud_sandbox_worktree_marker(
            &store,
            &agent,
            &config,
            "example/repo",
            None,
            None
        )
        .is_err()
    );
}

#[test]
fn staged_signin_copy_failure_preserves_source_and_missing_credentials_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    std::fs::create_dir(&stage).unwrap();
    assert!(
        resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).is_err()
    );
    assert!(stage.is_dir());
    let bytes = br#"{"tokens":{"access_token":"fixture-only"}}"#;
    std::fs::write(stage.join("auth.json"), bytes).unwrap();
    let final_dir = dir.path().join("profiles/openai/target");
    std::fs::create_dir_all(final_dir.join("auth.json")).unwrap();
    assert!(
        resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).is_err()
    );
    assert_eq!(std::fs::read(stage.join("auth.json")).unwrap(), bytes);
    std::fs::remove_dir(final_dir.join("auth.json")).unwrap();
    let (name, destination, _) =
        resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).unwrap();
    assert_eq!(name, "target");
    assert_eq!(std::fs::read(destination.join("auth.json")).unwrap(), bytes);
    assert!(!stage.exists());
}

#[cfg(unix)]
#[test]
fn credential_propagation_returns_post_write_sharing_errors() {
    use std::os::unix::fs::MetadataExt;

    // Principal admission is process-wide; isolate it from parallel tests.
    const PROBE: &str = "ANYAGENTS_TEST_PROPAGATION_SHARING_PROBE";
    if std::env::var_os(PROBE).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env(PROBE, "1")
            .env_remove("ANYAGENTS_PRINCIPALS")
            .env_remove("ANYAGENTS_PRINCIPAL_REGISTRY")
            .env_remove("ANYAGENTS_AGENTS")
            .env_remove("ANYAGENTS_AGENT_UID")
            .env_remove("ANYAGENTS_AGENT_GID")
            .env_remove("ANYAGENTS_SOCKET_GID")
            .env_remove("ANYAGENTS_SEAT_LAUNCHER")
            .args([
                "--exact",
                "create::runtime_config_tests::credential_propagation_returns_post_write_sharing_errors",
                "--nocapture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "probe failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
            "probe must run exactly one test: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        return;
    }

    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/target\n").unwrap();
    let owner = std::fs::metadata(&cfg).unwrap();
    let uid = if owner.uid() == 42001 { 42002 } else { 42001 };
    let gid = owner.gid();
    unsafe {
        std::env::set_var(
            "ANYAGENTS_PRINCIPALS",
            serde_json::json!({
                agent.id.as_str(): {
                    "user": "registered-worker", "uid": uid,
                    "gid": gid, "socket_gid": gid
                }
            })
            .to_string(),
        );
    }
    assert!(resolved_agent_seat_info(&store, &agent).is_some());
    let prof = dir.path().join("profiles/openai/target");
    std::fs::create_dir_all(&prof).unwrap();
    let credentials = br#"{"tokens":{"access_token":"fixture-only"}}"#;
    std::fs::write(prof.join("auth.json"), credentials).unwrap();
    let overlay = cfg.parent().unwrap().join("memory");
    std::fs::write(&overlay, "not a directory").unwrap();
    let markers = [
        ".anyagents-seeded-profile",
        ".anyagents-quota-cleared-ms",
        ".anyagents-profile-rotated-ms",
        ".anyagents-last-error.json",
    ];
    for marker in markers {
        std::fs::write(cfg.join(marker), "previous-state\n").unwrap();
    }
    // Preparation must succeed: this fixture fails only in post-write sharing.
    crate::sandbox::prepare_config_with_seat_uid(&cfg, uid, gid).unwrap();

    let err = propagate_credentials(
        dir.path(),
        "openai",
        "target",
        &["auth.json"],
        CredentialChange::Replaced,
    )
    .unwrap_err();
    match err {
        Error::WorkdirPermission { path, message } => {
            assert_eq!(path, cfg.display().to_string());
            assert!(
                message.contains("cannot share harness overlays"),
                "{message}"
            );
            assert!(
                message.contains("refusing non-directory host overlay"),
                "{message}"
            );
        }
        other => panic!("expected checked overlay sharing refusal, got {other:?}"),
    }
    assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), credentials);
    for marker in markers {
        assert_eq!(
            std::fs::read_to_string(cfg.join(marker)).unwrap(),
            "previous-state\n",
            "marker changed after sharing failed: {marker}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(&overlay).unwrap(),
        "not a directory"
    );

    // The same real caller succeeds once the overlay is repaired.
    std::fs::remove_file(&overlay).unwrap();
    propagate_credentials(
        dir.path(),
        "openai",
        "target",
        &["auth.json"],
        CredentialChange::Replaced,
    )
    .unwrap();
    assert!(overlay.is_dir());
    assert_eq!(
        std::fs::read_to_string(cfg.join(".anyagents-seeded-profile")).unwrap(),
        "openai/target\n"
    );
    let rotated = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms")).unwrap();
    assert!(rotated.trim().parse::<i64>().unwrap() > 0);
    assert_eq!(
        std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms")).unwrap(),
        rotated
    );
    assert!(!cfg.join(".anyagents-last-error.json").exists());
    assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), credentials);
}

#[test]
fn credential_propagation_does_not_seed_or_rotate_a_failed_destination() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/target\n").unwrap();
    let prof = dir.path().join("profiles/openai/target");
    std::fs::create_dir_all(&prof).unwrap();
    std::fs::write(
        prof.join("auth.json"),
        br#"{"tokens":{"access_token":"fixture-only"}}"#,
    )
    .unwrap();
    std::fs::create_dir(cfg.join("auth.json")).unwrap();
    assert!(
        propagate_profile_files_to_bound_agents(dir.path(), "openai", "target", &["auth.json"])
            .is_err()
    );
    assert!(!cfg.join(".anyagents-seeded-profile").exists());
    assert!(!cfg.join(".anyagents-profile-rotated-ms").exists());
    std::fs::remove_dir(cfg.join("auth.json")).unwrap();
    propagate_profile_files_to_bound_agents(dir.path(), "openai", "target", &["auth.json"])
        .unwrap();
    assert_eq!(seeded_profile_name(&cfg).as_deref(), Some("openai/target"));
    assert!(prof.join("auth.json").is_file());
}

#[test]
fn a_renewed_token_refreshes_files_without_rotating_the_running_process() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "agy"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    let rel = ".gemini/antigravity-cli/antigravity-oauth-token";
    std::fs::create_dir_all(cfg.join(".gemini/antigravity-cli")).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "google-antigravity/target\n").unwrap();
    std::fs::write(cfg.join(".anyagents-last-error.json"), b"{}").unwrap();
    let prof = dir.path().join("profiles/google-antigravity/target");
    std::fs::create_dir_all(prof.join(".gemini/antigravity-cli")).unwrap();
    std::fs::write(prof.join(rel), b"renewed-token").unwrap();

    propagate_credentials(
        dir.path(),
        "google-antigravity",
        "target",
        &[rel],
        CredentialChange::Renewed,
    )
    .unwrap();
    assert_eq!(std::fs::read(cfg.join(rel)).unwrap(), b"renewed-token");
    assert!(!cfg.join(".anyagents-profile-rotated-ms").exists());
    assert!(cfg.join(".anyagents-last-error.json").exists());

    propagate_profile_files_to_bound_agents(dir.path(), "google-antigravity", "target", &[rel])
        .unwrap();
    assert!(cfg.join(".anyagents-profile-rotated-ms").exists());
    assert!(!cfg.join(".anyagents-last-error.json").exists());
}

#[test]
fn unknown_or_malformed_quota_observations_never_fabricate_remaining_quota() {
    for value in [
        serde_json::json!(null),
        serde_json::json!("0"),
        serde_json::json!({}),
        serde_json::json!(-1),
        serde_json::json!(101),
    ] {
        assert!(observed_remaining_pct(&value).is_none());
    }
    assert_eq!(observed_remaining_pct(&serde_json::json!(25)), Some(75));
    assert_eq!(observed_remaining_pct(&serde_json::json!(100)), Some(0));
    assert_eq!(observed_fraction_pct(&serde_json::json!(0.7)), Some(70));
    assert!(observed_fraction_pct(&serde_json::json!(null)).is_none());
    assert_eq!(
        observed_agy_tier(&serde_json::json!({"paidTier":{"id":"ultra"}})).as_deref(),
        Some("ultra")
    );
    for value in [
        serde_json::json!({}),
        serde_json::json!({"paidTier":{"id":17}}),
        serde_json::json!({"currentTier":{"id":"unknown"}}),
    ] {
        assert!(observed_agy_tier(&value).is_none());
    }
    assert_eq!(observed_window_min(Some(70), Some(80)), Some(70));
    assert!(observed_window_min(None, Some(80)).is_none());
    assert!(observed_window_min(Some(70), None).is_none());
}

#[test]
fn display_formatting_does_not_reserve_company_style_name_prefixes() {
    assert_eq!(
        format_agent_display("owner-example", Some("Editor")),
        "owner-example · Editor"
    );
    assert_eq!(
        format_agent_display("example", Some("Editor")),
        "example · Editor"
    );
    assert_eq!(format_agent_display("owner-example", None), "owner-example");
}
#[test]
fn failed_profile_cutover_keeps_binding_and_source_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/previous\n").unwrap();
    let prof = dir.path().join("profiles/openai/target");
    std::fs::create_dir_all(&prof).unwrap();
    std::fs::write(prof.join("auth.json"), br#"{"account_id":"fixture-only"}"#).unwrap();
    std::fs::create_dir(cfg.join("auth.json")).unwrap();
    assert!(
        configure_agent_runtime(
            &store,
            dir.path(),
            "operator",
            &agent.name,
            None,
            None,
            Some("openai/target")
        )
        .is_err()
    );
    assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/previous"));
    assert!(!cfg.join(".anyagents-profile-rotated-ms").exists());
    assert_eq!(
        std::fs::read(prof.join("auth.json")).unwrap(),
        br#"{"account_id":"fixture-only"}"#
    );
}

#[test]
fn deduplication_retains_failed_source_and_preserves_successful_merge_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let provider = dir.path().join("profiles/openai");
    let canonical = provider.join("default");
    let duplicate = provider.join("duplicate");
    for path in [&canonical, &duplicate] {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(
            path.join("profile.json"),
            br#"{"account":"sample@example.test"}"#,
        )
        .unwrap();
    }
    let bytes = br#"{"account_id":"same-account","token":"newer-fixture"}"#;
    std::fs::write(duplicate.join("auth.json"), bytes).unwrap();
    std::fs::create_dir(canonical.join("auth.json")).unwrap();
    deduplicate_same_account_profiles(dir.path());
    assert_eq!(std::fs::read(duplicate.join("auth.json")).unwrap(), bytes);
    std::fs::remove_dir(canonical.join("auth.json")).unwrap();
    deduplicate_same_account_profiles(dir.path());
    assert_eq!(std::fs::read(canonical.join("auth.json")).unwrap(), bytes);
    assert!(!duplicate.exists());
    // The canonical profile now holds every byte the recovery copy held.
    let leftovers: Vec<_> = std::fs::read_dir(provider)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with('.'))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn merged_copies_are_removed_only_when_a_live_profile_holds_all_they_hold() {
    let dir = tempfile::tempdir().unwrap();
    let provider = dir.path().join("profiles/openai");
    let write = |path: PathBuf, bytes: &[u8]| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    };
    let same = br#"{"tokens":{"account_id":"same-account","access_token":"fixture"}}"#;
    write(provider.join("default/auth.json"), same);
    // Superseded: same account, declared files and bookkeeping only.
    write(provider.join(".merged-a-1/auth.json"), same);
    write(provider.join(".merged-a-1/profile.json"), b"{}");
    write(provider.join(".merged-a-1/.anyagents-last-usage.txt"), b"1");
    // Holds a file no live profile has: kept.
    write(provider.join(".merged-b-1/auth.json"), same);
    write(
        provider.join(".merged-b-1/.claude/.credentials.json"),
        b"{}",
    );
    // Holds a declared file the live profile lacks: kept.
    write(provider.join(".merged-c-1/auth.json"), same);
    write(provider.join(".merged-c-1/.codex/auth.json"), same);
    // Another account: kept.
    write(
        provider.join(".merged-d-1/auth.json"),
        br#"{"tokens":{"account_id":"other-account","access_token":"fixture"}}"#,
    );
    // Same identity, different credential bytes: never discard the backup.
    write(
        provider.join(".merged-e-1/auth.json"),
        br#"{"tokens":{"account_id":"same-account","access_token":"different-fixture"}}"#,
    );
    #[cfg(unix)]
    {
        write(provider.join(".merged-f-1/auth.json"), same);
        std::os::unix::fs::symlink("auth.json", provider.join(".merged-f-1/unknown-link")).unwrap();
    }
    deduplicate_same_account_profiles(dir.path());
    assert!(provider.join(".merged-e-1").is_dir());
    #[cfg(unix)]
    assert!(provider.join(".merged-f-1").is_dir());
    assert!(!provider.join(".merged-a-1").exists());
    for kept in [".merged-b-1", ".merged-c-1", ".merged-d-1"] {
        assert!(provider.join(kept).is_dir(), "{kept} was removed");
    }
    assert_eq!(
        std::fs::read(provider.join("default/auth.json")).unwrap(),
        same
    );
}

#[cfg(unix)]
#[test]
fn abandoned_signin_staging_is_reaped_and_a_held_one_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let provider = dir.path().join("profiles/openai");
    // PID_MAX_LIMIT is lower than this on supported Unix hosts.
    let abandoned = provider.join(".signin-staging-2147483647-1");
    assert!(signin_staging_owner_exited(".signin-staging-2147483647-1"));
    std::fs::create_dir_all(&abandoned).unwrap();
    let held = SigninStaging::create(dir.path(), "openai", "test").unwrap();
    assert!(
        !abandoned.exists(),
        "creating a staging reaps empty abandoned ones"
    );
    // Legacy processes and the window before a new process acquires its
    // lock must be protected even without a flock.
    let unlocked = provider.join(format!(".signin-staging-{}", std::process::id()));
    let unknown = provider.join(".signin-staging-agent-legacy-1");
    std::fs::create_dir_all(&unlocked).unwrap();
    std::fs::create_dir_all(&unknown).unwrap();
    std::fs::create_dir_all(&abandoned).unwrap();
    std::fs::write(held.path().join("auth.json"), b"unique fixture").unwrap();
    deduplicate_same_account_profiles(dir.path());
    assert!(!abandoned.exists());
    assert!(unlocked.is_dir());
    assert!(unknown.is_dir());
    assert!(held.path().join("auth.json").is_file());
    let path = held.path().to_path_buf();
    drop(held);
    assert_eq!(
        std::fs::read(path.join("auth.json")).unwrap(),
        b"unique fixture",
        "failed sign-in credentials remain recoverable"
    );
    let empty = SigninStaging::create(dir.path(), "openai", "empty").unwrap();
    let path = empty.path().to_path_buf();
    drop(empty);
    assert!(!path.exists());
}

#[cfg(unix)]
#[test]
fn abandoned_staging_requires_byte_identical_live_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let provider = dir.path().join("profiles/openai");
    let live = provider.join("default");
    let copy = provider.join(".signin-staging-2147483647-1");
    std::fs::create_dir_all(&live).unwrap();
    std::fs::create_dir_all(&copy).unwrap();
    let bytes = br#"{"tokens":{"account_id":"same","access_token":"fixture"}}"#;
    std::fs::write(live.join("auth.json"), bytes).unwrap();
    std::fs::write(copy.join("auth.json"), bytes).unwrap();
    std::fs::write(copy.join("unknown"), b"unique").unwrap();
    deduplicate_same_account_profiles(dir.path());
    assert!(copy.is_dir());
    std::fs::remove_file(copy.join("unknown")).unwrap();
    deduplicate_same_account_profiles(dir.path());
    assert!(!copy.exists());
    assert_eq!(std::fs::read(live.join("auth.json")).unwrap(), bytes);
}

#[test]
fn newer_signin_alias_is_retained_when_root_alias_cannot_commit() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(stage.join(".claude")).unwrap();
    std::fs::create_dir(stage.join(".credentials.json")).unwrap();
    let bytes = br#"{"claudeAiOauth":{"refreshToken":"newer-fixture"}}"#;
    std::fs::write(stage.join(".claude/.credentials.json"), bytes).unwrap();
    assert!(
        resolve_and_commit_staged_signin(dir.path(), "anthropic", &stage, Some("target")).is_err()
    );
    assert_eq!(
        std::fs::read(stage.join(".claude/.credentials.json")).unwrap(),
        bytes
    );
    assert!(!dir.path().join("profiles/anthropic/target").exists());
}

#[test]
fn staged_credential_validation_accepts_each_provider_and_refuses_non_credentials() {
    for (provider, rel, bytes) in [
        (
            "anthropic",
            ".credentials.json",
            br#"{"claudeAiOauth":{"refreshToken":"fixture-token"}}"#.as_slice(),
        ),
        (
            "openai",
            "auth.json",
            br#"{"OPENAI_API_KEY":"fixture-token"}"#,
        ),
        (
            "google-antigravity",
            ".gemini/antigravity-cli/antigravity-oauth-token",
            br#"{"access_token":"fixture-token"}"#,
        ),
        (
            "google-antigravity",
            ".gemini/antigravity-cli/antigravity-oauth-token",
            b"fixture-refresh-token\n",
        ),
        ("muse", "muse/auth.json", br#"{"token":"fixture-token"}"#),
        ("xai", ".grok/auth.json", br#"{"apiKey":"fixture-token"}"#),
    ] {
        let dir = tempfile::tempdir().unwrap();
        assert!(validate_staged_credentials(dir.path(), provider).is_err());
        let path = dir.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        validate_staged_credentials(dir.path(), provider).unwrap();
        for invalid in [
            b"{}".as_slice(),
            b"{invalid",
            br#"{"account_id":"metadata-only"}"#,
            br#"{"access_token":" "}"#,
        ] {
            std::fs::write(&path, invalid).unwrap();
            assert!(validate_staged_credentials(dir.path(), provider).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), invalid);
        }
    }
}

#[test]
fn finalization_without_usable_credentials_never_marks_agent_seeded() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    assert!(finalize_agent_signin(dir.path(), &agent).is_err());
    assert!(!cfg.join(".anyagents-seeded-profile").exists());
    std::fs::write(cfg.join("auth.json"), b"{malformed").unwrap();
    assert!(finalize_agent_signin(dir.path(), &agent).is_err());
    assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), b"{malformed");
    assert!(!cfg.join(".anyagents-seeded-profile").exists());
    for invalid in [
        br#"{"account_id":"metadata-only"}"#.as_slice(),
        br#"{"tokens":{"access_token":" "}}"#,
    ] {
        std::fs::write(cfg.join("auth.json"), invalid).unwrap();
        assert!(finalize_agent_signin(dir.path(), &agent).is_err());
        assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), invalid);
        assert!(!cfg.join(".anyagents-seeded-profile").exists());
    }
}

#[test]
fn rebalance_does_not_move_unknown_profiles_when_other_profiles_are_measured() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agents: Vec<_> = (0..3)
        .map(|n| {
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(
                    format!("sample-{n}"),
                    "codex",
                ))
                .unwrap();
            store.set_agent_state(&agent.id, "idle").unwrap()
        })
        .collect();
    for agent in &agents {
        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "openai/unknown\n").unwrap();
    }
    let entry = |profile: &str, usage: Option<&str>| ProviderProfileEntry {
        provider: "openai".into(),
        profile: profile.into(),
        kind: "subscription".into(),
        usage: usage.map(str::to_string),
        quota_5h: None,
        quota_weekly: None,
        account: None,
        compatible_harnesses: vec!["codex".into()],
        assigned_agents: vec![],
        subagent_count: 0,
    };
    let entries = vec![
        entry("healthy-one", Some("5h: 80% left")),
        entry("healthy-two", Some("5h: 90% left")),
        entry("unknown", None),
    ];
    rebalance_provider_profiles(&store, dir.path(), &agents, &entries);
    for agent in &agents {
        assert_eq!(
            bound_profile_name(&agent_config_dir(dir.path(), &agent.id)).as_deref(),
            Some("openai/unknown")
        );
    }
}
#[test]
fn capability_admission_rejects_foreign_identity_and_accepts_minted_agent_identity() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let selected = store
        .create_agent(anyagents_core::NewAgent::new("selected", "codex"))
        .unwrap();
    let foreign = store
        .create_agent(anyagents_core::NewAgent::new("foreign", "codex"))
        .unwrap();
    crate::operator::ensure(&store, &foreign.id).unwrap();
    let config = agent_config_dir(dir.path(), &selected.id);
    std::fs::create_dir_all(&config).unwrap();
    let foreign_token =
        std::fs::read(agent_config_dir(dir.path(), &foreign.id).join("mcp-capability")).unwrap();
    std::fs::write(config.join("mcp-capability"), foreign_token).unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir(&work).unwrap();
    let error =
        reconcile_agent_runtime_config(&store, dir.path(), &selected, &config, &work).unwrap_err();
    assert!(error.to_string().contains("another agent"), "{error}");
    std::fs::remove_file(config.join("mcp-capability")).unwrap();
    reconcile_agent_runtime_config(&store, dir.path(), &selected, &config, &work).unwrap();
    let token = std::fs::read_to_string(config.join("mcp-capability")).unwrap();
    assert_eq!(
        store.authenticate_capability(token.trim()).unwrap(),
        selected.id
    );
}

#[test]
fn shared_home_mounts_and_toolchain_caches_require_explicit_configuration() {
    // A mutex cannot protect unrelated runtime readers of these variables.
    // Keep both valid overrides and refusal inputs in an isolated process.
    const WORKER: &str = "ANYAGENTS_SHARED_HOME_TEST";
    if std::env::var_os(WORKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "create::runtime_config_tests::shared_home_mounts_and_toolchain_caches_require_explicit_configuration",
                "--nocapture",
            ])
            .env(WORKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }
    let keys = [
        "ANYAGENTS_SHARED_HOME_DIRS",
        "ANYAGENTS_RUSTUP_HOME",
        "ANYAGENTS_CARGO_HOME",
    ];
    let old = keys.map(std::env::var_os);
    unsafe {
        for key in keys {
            std::env::remove_var(key);
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("config");
    ensure_seat_environment(&cfg, dir.path()).unwrap();
    #[cfg(unix)]
    for name in ["tmp", "data-home", "cache-home", "state-home"] {
        use std::os::unix::fs::PermissionsExt as _;
        let path = cfg.join(name);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            0o1777
        );
        // A repeated preparation repairs legacy non-sticky runtime trees.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    ensure_seat_environment(&cfg, dir.path()).unwrap();
    #[cfg(unix)]
    for name in ["tmp", "data-home", "cache-home", "state-home"] {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(cfg.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o1777
        );
    }
    assert!(!cfg.join(".rustup").exists());
    assert!(!cfg.join(".cargo").exists());
    assert!(!cfg.join(".local/share/desk-claude").exists());
    let cache = dir.path().join("selected-cache");
    std::fs::create_dir(&cache).unwrap();
    unsafe {
        std::env::set_var(keys[1], &cache);
    }
    ensure_seat_environment(&cfg, dir.path()).unwrap();
    assert_eq!(std::fs::read_link(cfg.join(".rustup")).unwrap(), cache);
    unsafe {
        std::env::set_var(keys[0], r#"["../escape"]"#);
    }
    assert!(ensure_seat_environment(&cfg, dir.path()).is_err());
    unsafe {
        for (key, value) in keys.into_iter().zip(old) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[test]
fn zero_byte_and_corrupted_mcp_configs_are_self_healed_across_profile_and_agent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("agy-sample", "agy"))
        .unwrap();
    let profile = dir.path().join("profiles/google-antigravity/default");
    std::fs::create_dir_all(profile.join(".gemini/config")).unwrap();
    std::fs::create_dir_all(profile.join(".gemini/antigravity-cli")).unwrap();
    std::fs::write(
        profile.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"access_token":"fixture-token"}"#,
    )
    .unwrap();
    let profile_mcp = profile.join(".gemini/config/mcp_config.json");
    std::fs::write(&profile_mcp, b"").unwrap();

    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(cfg.join(".gemini/config")).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "google-antigravity/default\n").unwrap();
    let agent_mcp = cfg.join(".gemini/config/mcp_config.json");
    std::fs::write(&agent_mcp, b"").unwrap();

    seed_harness_credentials(&store, dir.path(), &agent, &cfg).unwrap();
    let parsed_profile: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&profile_mcp).unwrap()).unwrap();
    assert!(parsed_profile.get("mcpServers").unwrap().is_object());
    let parsed_agent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&agent_mcp).unwrap()).unwrap();
    assert!(parsed_agent.get("mcpServers").unwrap().is_object());

    // Truncate again and verify reconcile_agent_runtime_config self-heals
    // 0-byte or truncated JSON and writes the agent's MCP capability config.
    std::fs::write(&profile_mcp, b"{truncated").unwrap();
    std::fs::write(&agent_mcp, b"").unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    reconcile_agent_runtime_config(&store, dir.path(), &agent, &cfg, &work).unwrap();
    let healed_profile: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&profile_mcp).unwrap()).unwrap();
    assert!(healed_profile.get("mcpServers").unwrap().is_object());
    let healed_agent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&agent_mcp).unwrap()).unwrap();
    assert!(
        healed_agent
            .pointer("/mcpServers/anyagents/command")
            .and_then(serde_json::Value::as_str)
            .is_some()
    );
}

fn agy_seat_fixture() -> (tempfile::TempDir, Store, Agent, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("agy-seat", "agy"))
        .unwrap();
    let agent = store.set_agent_state(&agent.id, "running").unwrap();
    let profile = dir.path().join("profiles/google-antigravity/default");
    std::fs::create_dir_all(profile.join(".gemini/config")).unwrap();
    std::fs::write(
        profile.join(".gemini/config/mcp_config.json"),
        b"{\"mcpServers\":{}}\n",
    )
    .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(cfg.join(".gemini/config")).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "google-antigravity/default\n").unwrap();
    write_profile_marker(
        &cfg,
        ".anyagents-seeded-profile",
        "google-antigravity/default\n",
    )
    .unwrap();
    (dir, store, agent, cfg)
}

/// The live outage: a seat's `mcp_config.json` held `{"mcpServers":{}}`,
/// the running Antigravity process reloaded it, and `call_mcp_tool` left
/// its tool list (`server anyagents is not allowed in this context`) until
/// a restart rewrote the file. The periodic seat pass restores the
/// registration without a restart and keeps the seat's other servers.
#[test]
fn seat_pass_restores_a_dropped_agy_mcp_registration_without_a_restart() {
    let (dir, store, agent, cfg) = agy_seat_fixture();
    let seat_mcp = cfg.join(".gemini/config/mcp_config.json");
    let args_of_seat = mcp_server_args(dir.path(), &cfg);
    let foreign = format!(
        r#"{{"mcpServers":{{"anyagents":{{"command":"/bin/anyagents","args":["--data","{}","mcp","--config","/elsewhere/agents/other/config"]}}}}}}"#,
        dir.path().display()
    );
    for broken in [
        foreign.as_bytes(),
        &b"{\"mcpServers\":{}}\n"[..],
        b"",
        b"{truncated",
        br#"{"mcpServers":{"other":{"command":"/bin/other"},"anyagents":{"command":"/bin/x","disabled":true}}}"#,
    ] {
        std::fs::write(&seat_mcp, broken).unwrap();
        assert!(!agy_mcp_registered(&seat_mcp, &args_of_seat));
        sync_fresher_bound_agent_credentials_in(&store, dir.path());
        assert!(agy_mcp_registered(&seat_mcp, &args_of_seat), "not restored from {broken:?}");
        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&seat_mcp).unwrap()).unwrap();
        let args: Vec<&str> = root
            .pointer("/mcpServers/anyagents/args")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert_eq!(args[2..4], ["mcp", "--config"]);
        assert_eq!(args[4], cfg.display().to_string());
        if broken.starts_with(br#"{"mcpServers":{"other""#) {
            assert!(root.pointer("/mcpServers/other/command").is_some());
        }
    }

    // A healthy registration is left byte-for-byte alone.
    let before = std::fs::read(&seat_mcp).unwrap();
    sync_fresher_bound_agent_credentials_in(&store, dir.path());
    assert_eq!(std::fs::read(&seat_mcp).unwrap(), before);

    // The seeding path every wake runs restores it as well.
    std::fs::write(&seat_mcp, b"{\"mcpServers\":{}}\n").unwrap();
    seed_harness_credentials(&store, dir.path(), &agent, &cfg).unwrap();
    assert!(agy_mcp_registered(&seat_mcp, &args_of_seat));

    // A transition in progress owns the seat: the pass leaves it alone.
    std::fs::write(&seat_mcp, b"{\"mcpServers\":{}}\n").unwrap();
    let held = store.lock_agent_lifecycle(&agent.id).unwrap();
    sync_fresher_bound_agent_credentials_in(&store, dir.path());
    assert!(!agy_mcp_registered(&seat_mcp, &args_of_seat));
    drop(held);
    sync_fresher_bound_agent_credentials_in(&store, dir.path());
    assert!(agy_mcp_registered(&seat_mcp, &args_of_seat));
}

/// A profile switch copies the profile's credentials into the seat, and
/// nothing the seat owns: not the profile's markers and usage stamps, its
/// empty server map, its vendor settings or hooks (the seat's carry the
/// tool guard and deny list), its instruction mirrors, or a concurrent
/// writer's temporary file. A zero-byte file left by an older writer must
/// not abort the copy half-way, leaving the seat with some of the
/// profile's files and none of its own rewrites.
#[test]
fn profile_copy_carries_credentials_and_keeps_seat_owned_files() {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile");
    let seat = dir.path().join("seat");
    let write = |path: PathBuf, bytes: &[u8]| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    };
    // Profile side.
    write(profile.join(".anyagents-agy-tier.txt"), b"pro\n");
    write(
        profile.join(".anyagents-profile-meta-sync-ms"),
        b"1800000000000\n",
    );
    write(
        profile.join(".anyagents-quota-reset-ms"),
        b"1800000000000\n",
    );
    write(profile.join(".anyagents-oauth-refresh-attempt-ms"), b"");
    write(
        profile.join(".anyagents-profile"),
        b"google-antigravity/other\n",
    );
    write(
        profile.join(".gemini/config/.config.json.4242.0.anyagents-tmp"),
        b"{\"half\":",
    );
    write(
        profile.join(".gemini/config/mcp_config.json"),
        b"{\"mcpServers\":{}}\n",
    );
    write(
        profile.join(".gemini/config/hooks.json"),
        b"{\"hooks\":{}}\n",
    );
    write(profile.join(".gemini/config/GEMINI.md"), b"profile doc\n");
    write(
        profile.join(".gemini/antigravity-cli/settings.json"),
        b"{\"permissions\":{}}\n",
    );
    write(profile.join("settings.json"), b"{\"profile\":true}\n");
    write(profile.join(".claude.json"), b"{}\n");
    write(profile.join("empty-cache.json"), b"");
    write(profile.join(".gemini/config/config.json"), b"{\"k\":1}\n");
    write(
        profile.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"access_token":"fixture"}"#,
    );
    write(profile.join("brain/state"), b"vendor runtime");
    write(profile.join("profile.json"), b"{}");
    // Seat side.
    let registered = br#"{"mcpServers":{"anyagents":{"command":"/bin/anyagents"}}}"#;
    let guarded = br#"{"permissions":{"deny":["command(*)"]}}"#;
    write(seat.join(".gemini/config/mcp_config.json"), registered);
    write(
        seat.join(".gemini/config/hooks.json"),
        b"{\"seat-guard\":1}\n",
    );
    write(seat.join(".gemini/antigravity-cli/settings.json"), guarded);
    write(
        seat.join(".anyagents-profile"),
        b"google-antigravity/default\n",
    );
    write(seat.join(".anyagents-agy-tier.txt"), b"ultra\n");

    copy_profile_tree(&profile, &seat).unwrap();

    let read = |rel: &str| std::fs::read(seat.join(rel)).unwrap();
    assert_eq!(read(".gemini/config/mcp_config.json"), registered);
    assert_eq!(read(".gemini/config/hooks.json"), b"{\"seat-guard\":1}\n");
    assert_eq!(read(".gemini/antigravity-cli/settings.json"), guarded);
    assert_eq!(read(".anyagents-profile"), b"google-antigravity/default\n");
    assert_eq!(read(".anyagents-agy-tier.txt"), b"ultra\n");
    for absent in [
        ".anyagents-profile-meta-sync-ms",
        ".anyagents-quota-reset-ms",
        ".anyagents-oauth-refresh-attempt-ms",
        ".gemini/config/.config.json.4242.0.anyagents-tmp",
        ".gemini/config/GEMINI.md",
        "settings.json",
        ".claude.json",
        "empty-cache.json",
        "brain",
        "profile.json",
    ] {
        assert!(!seat.join(absent).exists(), "{absent} was copied");
    }
    assert_eq!(read(".gemini/config/config.json"), b"{\"k\":1}\n");
    assert_eq!(
        read(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"access_token":"fixture"}"#
    );
}

/// Transitions of one agent run one at a time across threads and
/// processes: an operator switch waits for the holder of the agent's
/// lifecycle lock; an automatic rotation skips the busy agent.
#[test]
fn harness_transitions_of_one_agent_are_serialized() {
    let (dir, store, agent, _cfg) = agy_seat_fixture();
    let data = dir.path().to_path_buf();
    let held = store.lock_agent_lifecycle(&agent.id).unwrap();

    let rotated = configure_agent_runtime(
        &store,
        &data,
        "system:quota-rotate",
        &agent.name,
        None,
        None,
        None,
    );
    assert!(matches!(rotated, Err(Error::AgentBusy)), "{rotated:?}");

    let (tx, rx) = std::sync::mpsc::channel();
    let name = agent.name.clone();
    let waiter_data = data.clone();
    let waiter = std::thread::spawn(move || {
        let store = Store::open(waiter_data.join("state.db")).unwrap();
        let started = std::time::Instant::now();
        let _ = configure_agent_runtime(&store, &waiter_data, "operator", &name, None, None, None);
        tx.send(started.elapsed()).unwrap();
    });
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(300))
            .is_err(),
        "an operator switch ran while another transition held the agent"
    );
    drop(held);
    let waited = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the switch never ran after the lock was released");
    assert!(waited >= std::time::Duration::from_millis(300));
    waiter.join().unwrap();
    assert!(store.try_lock_agent_lifecycle(&agent.id).is_ok());
}

fn serve_one_http_request(
    mut stream: std::net::TcpStream,
    handler: impl FnOnce(&str) -> (&'static str, &'static str),
) {
    use std::io::{Read, Write};
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    let mut req = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let mut expected_total: Option<usize> = None;
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                req.extend_from_slice(&chunk[..n]);
                if expected_total.is_none()
                    && let Some(pos) = req.windows(4).position(|w| {
                        w == b"

"
                    })
                {
                    let hdr_end = pos + 4;
                    let hdr_str = String::from_utf8_lossy(&req[..hdr_end]);
                    let content_len = hdr_str
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    expected_total = Some(hdr_end + content_len);
                }
                if expected_total.is_some_and(|total| req.len() >= total) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let req_str = String::from_utf8_lossy(&req);
    let (status, body) = handler(&req_str);
    let resp = format!(
        "HTTP/1.1 {status}
Content-Type: application/json
Content-Length: {}
Connection: close

{}",
        body.len(),
        body
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(200)));
    let _ = stream.read(&mut chunk);
}

// Exercise production sanitization without changing the environment of parallel tests.
fn claude_projection_test_worker(test: &str) -> bool {
    const WORKER: &str = "ANYAGENTS_CLAUDE_PROJECTION_TEST";
    if std::env::var_os(WORKER).is_some() {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(WORKER, "1")
        .env(
            "ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL",
            "http://127.0.0.1:1/oauth/token",
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    false
}

fn claude_projection_credentials(token: &str, expiry: i64, refresh: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"claudeAiOauth": {
        "accessToken": token,
        "refreshToken": if refresh { Some("profile-only-refresh") } else { None },
        "expiresAt": expiry,
        "accountUuid": "selected-account"
    }}))
    .unwrap()
}

fn assert_claude_projection(config: &Path, token: &str, expiry: i64) {
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(config.join(rel)).unwrap()).unwrap();
        assert_eq!(value["claudeAiOauth"]["accessToken"], token, "{rel}");
        assert_eq!(value["claudeAiOauth"]["expiresAt"], expiry, "{rel}");
        assert!(value["claudeAiOauth"]["refreshToken"].is_null(), "{rel}");
    }
}

#[test]
fn claude_projection_seed_respects_source_target_expiry_and_isolation() {
    if !claude_projection_test_worker(
        "create::runtime_config_tests::claude_projection_seed_respects_source_target_expiry_and_isolation",
    ) {
        return;
    }
    let now = anyagents_core::now_ms();
    // Each alias can be the only authoritative source; an absent or expired
    // source must never regress a valid seat or borrow another profile.
    for source_rel in [".credentials.json", ".claude/.credentials.json"] {
        for source_exp in [None, Some(now - 1_000), Some(now + 7_200_000)] {
            for target_exp in [
                None,
                Some(now - 2_000),
                Some(now + 3_600_000),
                Some(now + 10_800_000),
            ] {
                for target_identity in ["selected-account", "other-account"] {
                    let dir = tempfile::tempdir().unwrap();
                    let store = Store::open(dir.path().join("state.db")).unwrap();
                    let agent = store
                        .create_agent(anyagents_core::NewAgent::new("sample", "claude-code"))
                        .unwrap();
                    let profile = dir.path().join("profiles/anthropic/selected");
                    let other = dir.path().join("profiles/anthropic/default");
                    let cfg = agent_config_dir(dir.path(), &agent.id);
                    for root in [&profile, &other, &cfg] {
                        std::fs::create_dir_all(root.join(".claude")).unwrap();
                    }
                    // Suppress a network refresh, not the production sanitizer.
                    write_profile_marker(&profile, OAUTH_REFRESH_ATTEMPT_FILE, &now.to_string())
                        .unwrap();
                    write_profile_marker(&cfg, ".anyagents-profile", "anthropic/selected\n")
                        .unwrap();
                    write_profile_marker(&cfg, ".anyagents-seeded-profile", "anthropic/selected\n")
                        .unwrap();
                    let foreign =
                        claude_projection_credentials("other-profile", now + 14_400_000, true);
                    std::fs::write(other.join(".credentials.json"), &foreign).unwrap();
                    let source =
                        source_exp.map(|exp| claude_projection_credentials("source", exp, true));
                    if let Some(ref bytes) = source {
                        std::fs::write(profile.join(source_rel), bytes).unwrap();
                    }
                    if let Some(exp) = target_exp {
                        let mut value: serde_json::Value = serde_json::from_slice(
                            &claude_projection_credentials("target", exp, false),
                        )
                        .unwrap();
                        value["claudeAiOauth"]["accountUuid"] = target_identity.into();
                        if source.is_some() {
                            // A retained token must also have its refresh field sanitized.
                            value["claudeAiOauth"]["refreshToken"] = "".into();
                        }
                        let bytes = serde_json::to_vec(&value).unwrap();
                        for rel in [".credentials.json", ".claude/.credentials.json"] {
                            std::fs::write(cfg.join(rel), &bytes).unwrap();
                        }
                    }
                    seed_harness_credentials(&store, dir.path(), &agent, &cfg).unwrap();
                    let expected = match (source_exp, target_exp) {
                        (None, target) => target.map(|exp| ("target", exp)),
                        (Some(src), Some(dst))
                            if dst > now && dst > src && target_identity == "selected-account" =>
                        {
                            Some(("target", dst))
                        }
                        (Some(src), _) if src > now => Some(("source", src)),
                        _ => Some(("", 0)),
                    };
                    if let Some((token, expiry)) = expected {
                        assert_claude_projection(&cfg, token, expiry);
                        let projected: serde_json::Value = serde_json::from_slice(
                            &std::fs::read(cfg.join(".credentials.json")).unwrap(),
                        )
                        .unwrap();
                        assert_eq!(
                            projected["claudeAiOauth"]["accountUuid"],
                            if token == "target" {
                                target_identity
                            } else {
                                "selected-account"
                            }
                        );
                    } else {
                        assert!(!cfg.join(".credentials.json").exists());
                        assert!(!cfg.join(".claude/.credentials.json").exists());
                    }
                    if let Some(bytes) = source {
                        assert_eq!(std::fs::read(profile.join(source_rel)).unwrap(), bytes);
                    }
                    assert_eq!(
                        std::fs::read(other.join(".credentials.json")).unwrap(),
                        foreign
                    );
                    assert_eq!(
                        bound_profile_name(&cfg).as_deref(),
                        Some("anthropic/selected")
                    );
                }
            }
        }
    }
}

#[test]
fn claude_projection_switch_and_launch_choose_fresh_selected_profile_alias() {
    if !claude_projection_test_worker(
        "create::runtime_config_tests::claude_projection_switch_and_launch_choose_fresh_selected_profile_alias",
    ) {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    let profile = dir.path().join("profiles/anthropic/selected");
    for root in [&cfg, &profile] {
        std::fs::create_dir_all(root.join(".claude")).unwrap();
    }
    let now = anyagents_core::now_ms();
    let stale = claude_projection_credentials("expired", now - 1_000, true);
    let fresh = claude_projection_credentials("selected-fresh", now + 7_200_000, true);
    let previous = claude_projection_credentials("previous-profile", now + 14_400_000, false);
    std::fs::write(profile.join(".credentials.json"), &stale).unwrap();
    std::fs::write(profile.join(".claude/.credentials.json"), &fresh).unwrap();
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        std::fs::write(cfg.join(rel), &previous).unwrap();
    }
    write_profile_marker(&cfg, ".anyagents-profile", "openai/default\n").unwrap();
    write_profile_marker(&cfg, ".anyagents-seeded-profile", "openai/default\n").unwrap();
    // The stale alias must not trigger refresh while a fresh alias exists.
    let result = configure_agent_runtime(
        &store,
        dir.path(),
        "operator",
        &agent.name,
        Some("claude-code"),
        Some("anthropic/claude-opus-4-6"),
        Some("anthropic/selected"),
    )
    .unwrap();
    assert_eq!(result["harness"], "claude-code");
    assert_eq!(result["profile"], "anthropic/selected");
    assert_claude_projection(&cfg, "selected-fresh", now + 7_200_000);
    assert!(!profile.join(OAUTH_REFRESH_ATTEMPT_FILE).exists());
    assert_eq!(
        std::fs::read(profile.join(".credentials.json")).unwrap(),
        stale
    );
    assert_eq!(
        std::fs::read(profile.join(".claude/.credentials.json")).unwrap(),
        fresh
    );

    // Ordinary launch reconciles via the same owner, with the other alias fresh.
    std::fs::write(profile.join(".credentials.json"), &fresh).unwrap();
    std::fs::write(profile.join(".claude/.credentials.json"), &stale).unwrap();
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        std::fs::write(
            cfg.join(rel),
            claude_projection_credentials("expired-seat", now - 2_000, false),
        )
        .unwrap();
    }
    let updated = store.agent(&agent.id).unwrap();
    reconcile_agent_runtime_config_for_launch(
        &store,
        dir.path(),
        &updated,
        &cfg,
        &agent_work_dir(dir.path(), &agent.id),
    )
    .unwrap();
    assert_claude_projection(&cfg, "selected-fresh", now + 7_200_000);
    assert_eq!(
        std::fs::read(profile.join(".credentials.json")).unwrap(),
        fresh
    );
    assert_eq!(
        std::fs::read(profile.join(".claude/.credentials.json")).unwrap(),
        stale
    );

    // Required credential writes still fail closed on an unwritable destination.
    std::fs::remove_file(cfg.join(".credentials.json")).unwrap();
    std::fs::create_dir(cfg.join(".credentials.json")).unwrap();
    assert!(seed_harness_credentials(&store, dir.path(), &updated, &cfg).is_err());
    std::fs::remove_dir(cfg.join(".credentials.json")).unwrap();

    // A profile switch with no credentials clears old-profile credentials,
    // rather than retaining a valid token from a different account.
    let empty = dir.path().join("profiles/anthropic/empty");
    std::fs::create_dir_all(&empty).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "anthropic/empty\n").unwrap();
    seed_harness_credentials(&store, dir.path(), &updated, &cfg).unwrap();
    assert!(!cfg.join(".credentials.json").exists());
    assert!(!cfg.join(".claude/.credentials.json").exists());
    assert_eq!(
        seeded_profile_name(&cfg).as_deref(),
        Some("anthropic/empty")
    );
}

#[test]
fn mid_session_anthropic_oauth_refresh_updates_profile_and_sanitized_agent_credentials() {
    const WORKER: &str = "ANYAGENTS_ANTHROPIC_REFRESH_TEST";
    if std::env::var_os(WORKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "create::runtime_config_tests::mid_session_anthropic_oauth_refresh_updates_profile_and_sanitized_agent_credentials",
                "--nocapture",
            ])
            .env(WORKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            serve_one_http_request(stream, |_| {
                (
                    "200 OK",
                    r#"{"access_token":"fresh-access-token","refresh_token":"fresh-refresh-token","expires_in":28800}"#,
                )
            });
        }
    });

    let token_url = format!("http://{addr}/v1/oauth/token");
    unsafe {
        std::env::set_var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL", &token_url);
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "claude-sample",
            "claude-code",
        ))
        .unwrap();
    // Mid-session: a running seat (a seat still being created is skipped).
    store.set_agent_state(&agent.id, "running").unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // Expiring in 10 minutes (< 30 minute proactive refresh threshold).
    let expiring_soon = now_ms + 10 * 60 * 1000;
    let profile = dir.path().join("profiles/anthropic/default");
    std::fs::create_dir_all(profile.join(".claude")).unwrap();
    let initial_profile_creds = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "old-access-token",
            "refreshToken": "old-refresh-token",
            "expiresAt": expiring_soon,
            "subscriptionType": "max"
        }
    });
    let raw_initial = serde_json::to_vec(&initial_profile_creds).unwrap();
    std::fs::write(profile.join(".credentials.json"), &raw_initial).unwrap();
    std::fs::write(profile.join(".claude/.credentials.json"), &raw_initial).unwrap();

    let cfg = agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(cfg.join(".claude")).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "anthropic/default\n").unwrap();
    let stale_agent_creds = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "old-access-token",
            "refreshToken": null,
            "expiresAt": expiring_soon,
            "subscriptionType": "max"
        }
    });
    let raw_stale = serde_json::to_vec(&stale_agent_creds).unwrap();
    std::fs::write(cfg.join(".credentials.json"), &raw_stale).unwrap();
    std::fs::write(cfg.join(".claude/.credentials.json"), &raw_stale).unwrap();

    sync_fresher_bound_agent_credentials(dir.path());
    server.join().unwrap();

    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let prof_val: serde_json::Value =
            serde_json::from_slice(&std::fs::read(profile.join(rel)).unwrap()).unwrap();
        assert_eq!(
            prof_val
                .pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str),
            Some("fresh-access-token")
        );
        assert_eq!(
            prof_val
                .pointer("/claudeAiOauth/refreshToken")
                .and_then(serde_json::Value::as_str),
            Some("fresh-refresh-token")
        );

        let agent_val: serde_json::Value =
            serde_json::from_slice(&std::fs::read(cfg.join(rel)).unwrap()).unwrap();
        assert_eq!(
            agent_val
                .pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str),
            Some("fresh-access-token")
        );
        assert!(
            agent_val
                .pointer("/claudeAiOauth/refreshToken")
                .is_some_and(serde_json::Value::is_null),
            "seat credentials must strip refreshToken: {agent_val}"
        );
        assert!(
            agent_val
                .pointer("/claudeAiOauth/expiresAt")
                .and_then(serde_json::Value::as_i64)
                .unwrap()
                > expiring_soon
        );
    }

    unsafe {
        std::env::remove_var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL");
    }
}
/// A worker in a repository without a role catalog still gets the shared
/// rules repository's template when the data directory's `AGENTS.md`
/// links into it, never the built-in fallback.
#[cfg(unix)]
#[test]
fn archetype_lookup_follows_the_data_dir_agents_md_into_its_rules_repo() {
    let dir = crate::test_tempdir();
    let rules = dir.path().join("rules-repo");
    let data_dir = dir.path().join("data");
    let parent_wd = dir.path().join("product-repo/parent");
    let child_wd = dir.path().join("product-repo/child");
    for path in [
        rules.join(".agents/agents"),
        data_dir.clone(),
        parent_wd.clone(),
        child_wd.clone(),
    ] {
        std::fs::create_dir_all(path).unwrap();
    }
    std::fs::write(rules.join("AGENTS.md"), "# Rules\n").unwrap();
    std::fs::write(
        rules.join(".agents/agents/general.md"),
        "---\ndescription: Default worker\nharness: codex\nmodel: worker-model\n---\nShared worker template.\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(rules.join("AGENTS.md"), data_dir.join("AGENTS.md")).unwrap();

    let tpl = resolve_archetype_template_with_ancestors(
        "general",
        Some(&child_wd),
        std::slice::from_ref(&parent_wd),
        &data_dir,
    )
    .unwrap()
    .expect("general template");
    assert_eq!(tpl.harness.as_deref(), Some("codex"));
    assert_eq!(tpl.model.as_deref(), Some("worker-model"));
    assert!(tpl.body.contains("Shared worker template."));

    // A workspace with its own catalog keeps precedence over the rules repo.
    std::fs::create_dir_all(child_wd.join(".agents/agents")).unwrap();
    std::fs::write(
        child_wd.join(".agents/agents/general.md"),
        "---\ndescription: Local worker\nharness: claude-code\n---\nLocal template.\n",
    )
    .unwrap();
    let tpl = resolve_archetype_template_with_ancestors(
        "general",
        Some(&child_wd),
        &[parent_wd],
        &data_dir,
    )
    .unwrap()
    .expect("general template");
    assert_eq!(tpl.harness.as_deref(), Some("claude-code"));
}

#[cfg(unix)]
#[test]
fn archetype_lookup_allows_symlinked_trusted_base_roots_and_blocks_relative_escapes() {
    let dir = crate::test_tempdir();
    let storage = dir.path().join("workspace-data/workspace/repo");
    let data_dir = dir.path().join("data");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(storage.join(".agents/agents")).unwrap();
    std::fs::create_dir_all(data_dir.join(".agents/agents")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();

    std::fs::write(
        storage.join(".agents/agents/architect.md"),
        "---\ndescription: Workspace architect\n---\nFrom symlinked base.\n",
    )
    .unwrap();
    std::fs::write(
        data_dir.join(".agents/agents/architect.md"),
        "---\ndescription: Fallback architect\n---\nFrom data_dir.\n",
    )
    .unwrap();
    std::fs::write(outside.join("secret.md"), "From outside.\n").unwrap();

    let base_symlink = dir.path().join("workspace-link");
    std::os::unix::fs::symlink(&storage, &base_symlink).unwrap();

    let roots = archetype_search_roots(Some(Path::new("worktree")), Some(&base_symlink), &data_dir);
    assert_eq!(
        roots[0],
        (base_symlink.clone(), PathBuf::from(".agents/agents"))
    );

    let via_explicit =
        resolve_archetype_template("architect", Some(&base_symlink), None, &data_dir)
            .unwrap()
            .expect("template under symlinked base must resolve");
    assert_eq!(
        via_explicit.description.as_deref(),
        Some("Workspace architect")
    );
    assert_eq!(via_explicit.body, "From symlinked base.");

    let via_parent = resolve_archetype_template(
        "architect",
        Some(Path::new("private")),
        Some(&base_symlink),
        &data_dir,
    )
    .unwrap()
    .expect("template under symlinked parent_workdir must resolve");
    assert_eq!(via_parent.body, "From symlinked base.");

    let missing_base = dir.path().join("nonexistent-workdir");
    let via_fallback =
        resolve_archetype_template("architect", Some(&missing_base), None, &data_dir)
            .unwrap()
            .expect("missing explicit workdir should fall through to data_dir");
    assert_eq!(via_fallback.body, "From data_dir.");

    std::os::unix::fs::symlink(
        outside.join("secret.md"),
        storage.join(".agents/agents/evil.md"),
    )
    .unwrap();
    std::fs::write(
        data_dir.join(".agents/agents/evil.md"),
        "Fallback evil should never be reached.\n",
    )
    .unwrap();
    let err = resolve_archetype_template("evil", Some(&base_symlink), None, &data_dir)
        .expect_err("relative symlink inside symlinked base must fail closed");
    assert!(err.to_string().contains("symlink"), "{err}");

    let trav = read_archetype_file(
        &base_symlink,
        Path::new(".agents/agents/../../../outside/secret.md"),
    )
    .expect_err("parent traversal in relative path must fail closed");
    assert!(trav.to_string().contains("escapes base"), "{trav}");
}

#[test]
fn a_creation_its_profile_cannot_run_is_refused_and_leaves_no_agent() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    // Report #34's pairing is a supported one (see vendor's Sylphx route).
    ensure_harness_runs_profile(
        data,
        "claude-code",
        Some("sylphx/default"),
        Some("sylphx/openai/gpt-6.1-sol"),
    )
    .unwrap();
    let refused = create(
        &store,
        data,
        CreateRequest {
            name: "agy-on-sylphx".to_string(),
            role: None,
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: Some("sylphx/default".to_string()),
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(data.to_path_buf()),
            description: Some("test".to_string()),
            instructions: Some("test".to_string()),
            parent_id: None,
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        refused.contains("not compatible with provider profile `sylphx/default`"),
        "{refused}"
    );
    // Nothing to roll back: no row was written.
    assert!(store.agent_by_name("agy-on-sylphx").is_err());
}

#[test]
fn archetype_profile_inherits_when_compatible_and_skips_on_harness_override() {
    let dir = crate::test_tempdir();
    let data = dir.path().join("data");
    let ws = dir.path().join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(ws.join(".agents/agents")).unwrap();
    std::fs::write(
        ws.join(".agents/agents/general.md"),
        "---\nrole: general\nscope: private\nharness: spiron\nmodel: sylphx/openai/gpt-6.1-sol#xhigh\nprofile: sylphx/default\ndescription: General worker\n---\nGeneral body.\n",
    )
    .unwrap();
    std::fs::write(
        ws.join(".agents/agents/profile-only.md"),
        "---\nrole: profile-only\nscope: private\nprofile: sylphx/default\ndescription: Profile-only worker\n---\nProfile body.\n",
    )
    .unwrap();

    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut parent_new = NewAgent::new("parent", "spiron");
    parent_new.workdir = Some(ws.to_string_lossy().into_owned());
    let parent = store.create_agent(parent_new).unwrap();

    // 1. Omitted harness/model/profile inherits the matching template defaults.
    let mut default_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "default-general",
            "role": "general",
            "harness": "",
        }))
        .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent), &mut default_req).unwrap();
    assert_eq!(default_req.harness.as_deref(), Some("spiron"));
    assert_eq!(
        default_req.model.as_deref(),
        Some("sylphx/openai/gpt-6.1-sol#xhigh")
    );
    assert_eq!(default_req.profile.as_deref(), Some("sylphx/default"));

    // 2. Explicit `harness: "claude-code"` with omitted `profile` skips `tpl.profile`
    //    so `claude-code` resolves its own native default profile cleanly.
    let mut cc_req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "cc-override",
        "role": "general",
        "harness": "claude-code",
        "model": "claude-opus-5-5#high",
    }))
    .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent), &mut cc_req).unwrap();
    assert_eq!(cc_req.harness.as_deref(), Some("claude-code"));
    assert_eq!(cc_req.model.as_deref(), Some("claude-opus-5-5#high"));
    assert_eq!(cc_req.profile, None);

    // 3. Explicit `harness: "claude-code"` with omitted `model` and `profile` skips both.
    let mut cc_bare: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "cc-bare",
        "role": "general",
        "harness": "claude-code",
    }))
    .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent), &mut cc_bare).unwrap();
    assert_eq!(cc_bare.harness.as_deref(), Some("claude-code"));
    assert_eq!(cc_bare.model, None);
    assert_eq!(cc_bare.profile, None);

    // 4. Template without `harness` whose `profile` provider prefix mismatches
    //    `effective_harness` also skips `tpl.profile` when omitted.
    let mut mismatch_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "cc-profile-only",
            "role": "profile-only",
            "harness": "claude-code",
            "model": "claude-opus-5-5#high",
        }))
        .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent), &mut mismatch_req).unwrap();
    assert_eq!(mismatch_req.profile, None);

    // 5. Explicit incompatible `profile` from the caller still fails closed in `create()`.
    let mut explicit_bad: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "explicit-incompatible",
            "role": "general",
            "harness": "agy",
            "profile": "sylphx/default",
        }))
        .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent), &mut explicit_bad).unwrap();
    assert_eq!(explicit_bad.profile.as_deref(), Some("sylphx/default"));
    let err = create(
        &store,
        &data,
        CreateRequest {
            name: explicit_bad.name,
            role: explicit_bad.role,
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: explicit_bad.model,
            profile: explicit_bad.profile,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(ws),
            description: explicit_bad.description,
            instructions: explicit_bad.instructions,
            parent_id: Some(parent.id),
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("not compatible with provider profile `sylphx/default`"),
        "{err}"
    );
}

#[test]
fn vendor_neutral_profiles_and_fail_closed_harness_mappings() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    assert_eq!(upstream_provider_of("opencode", None), "opencode");
    assert_eq!(upstream_provider_of("omp", None), "omp");
    assert_eq!(upstream_provider_of("spiron", None), "spiron");
    assert_eq!(upstream_provider_of("spiron-cloud", None), "spiron");
    assert_eq!(
        upstream_provider_of("opencode", Some("sylphx/openai/gpt-6.1-sol")),
        "sylphx"
    );

    for (prov, expected_harness) in [
        ("anthropic", "claude-code"),
        ("openai", "codex"),
        ("google-antigravity", "agy"),
        ("muse", "muse"),
        ("sylphx", "spiron"),
    ] {
        ensure_provider_profile_meta(
            &data.join("profiles").join(prov).join("default"),
            prov,
            None,
        );
        let (_, compat) = default_kind_and_harnesses(prov);
        assert!(
            compat.iter().any(|h| h == expected_harness),
            "{prov}: {compat:?}"
        );
    }

    for unmapped in [
        "anthropic/default",
        "openai/default",
        "google-antigravity/default",
        "muse/default",
    ] {
        let res = create(
            &store,
            data,
            CreateRequest {
                name: format!("bad-{}", unmapped.replace('/', "-")),
                role: None,
                harness: "opencode".to_string(),
                model: None,
                profile: Some(unmapped.to_string()),
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                description: Some("test".to_string()),
                instructions: Some("test".to_string()),
                parent_id: None,
                ..Default::default()
            },
        );
        assert!(
            res.is_err(),
            "expected fail-closed rejection for {unmapped} on opencode"
        );
    }

    let narrowed = data.join("profiles/sylphx/spiron-only");
    std::fs::create_dir_all(&narrowed).unwrap();
    std::fs::write(
        narrowed.join("profile.json"),
        r#"{"provider":"sylphx","kind":"api-key","compatible_harnesses":["spiron"]}"#,
    )
    .unwrap();
    assert!(
        ensure_profile_compatible_and_mapped(
            data,
            "sylphx",
            "spiron-only",
            "codex",
            Some("sylphx/openai/gpt-6.1-sol"),
            None
        )
        .is_err(),
        "narrowed compatible_harnesses in profile.json must be strictly honored"
    );
    assert!(
        ensure_profile_compatible_and_mapped(data, "sylphx", "default", "opencode", None, None)
            .is_err(),
        "sylphx/default without key, provider block, or sylphx/ model must fail closed on opencode"
    );
}

#[test]
fn oauth_refresh_fails_closed_without_network_request_when_reservation_cannot_persist() {
    const WORKER: &str = "ANYAGENTS_OAUTH_RESERVATION_TEST";
    if std::env::var_os(WORKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "create::runtime_config_tests::oauth_refresh_fails_closed_without_network_request_when_reservation_cannot_persist",
                "--nocapture",
            ])
            .env(WORKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}
{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let token_hits = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let token_hits_srv = Arc::clone(&token_hits);
    let stop_srv = Arc::clone(&stop);
    let server = std::thread::spawn(move || {
        while !stop_srv.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    serve_one_http_request(stream, |req| {
                        if req.starts_with("POST /v1/oauth/token") {
                            token_hits_srv.fetch_add(1, Ordering::SeqCst);
                            (
                                "200 OK",
                                r#"{"access_token":"rotated-access-token","refresh_token":"rotated-refresh-token","expires_in":28800}"#,
                            )
                        } else if req.starts_with("GET /api/oauth/usage") {
                            (
                                "200 OK",
                                r#"{"five_hour":{"utilization":12.0},"seven_day":{"utilization":25.0}}"#,
                            )
                        } else {
                            ("404 Not Found", r#"{"error":"not_found"}"#)
                        }
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    let token_url = format!("http://{addr}/v1/oauth/token");
    let usage_url = format!("http://{addr}/api/oauth/usage");
    unsafe {
        std::env::set_var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL", &token_url);
        std::env::set_var("ANYAGENTS_ANTHROPIC_OAUTH_USAGE_URL", &usage_url);
    }

    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profiles/anthropic/default");
    std::fs::create_dir_all(profile.join(".claude")).unwrap();
    let now_ms = anyagents_core::now_ms();
    let initial_creds = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "initial-access-token",
            "refreshToken": "single-use-refresh-token",
            "expiresAt": now_ms - 60_000,
            "subscriptionType": "max"
        }
    });
    let raw_initial = serde_json::to_vec(&initial_creds).unwrap();
    std::fs::write(profile.join(".credentials.json"), &raw_initial).unwrap();
    std::fs::write(profile.join(".claude/.credentials.json"), &raw_initial).unwrap();

    // Case 1: Reservation write fails (blocked reservation target + read-only profile_root).
    std::fs::create_dir(profile.join(OAUTH_REFRESH_RESERVATION_FILE)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o555)).unwrap();
    }
    assert!(
        refresh_anthropic_profile_oauth_if_due(dir.path(), &profile).is_err(),
        "refresh must fail closed when disk reservation cannot persist"
    );
    let _ = probe_one_profile_usage(dir.path(), "anthropic", "default");
    assert_eq!(
        token_hits.load(Ordering::SeqCst),
        0,
        "zero OAuth refresh HTTP requests may fire when reservation fails"
    );
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        assert_eq!(std::fs::read(profile.join(rel)).unwrap(), raw_initial);
    }

    // Restore directory permissions and remove reservation blocker.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::remove_dir(profile.join(OAUTH_REFRESH_RESERVATION_FILE)).unwrap();

    // Case 2: Checked `.anyagents-oauth-refresh-attempt-ms` write fails; reservation is cleaned up.
    std::fs::create_dir(profile.join(OAUTH_REFRESH_ATTEMPT_FILE)).unwrap();
    assert!(
        refresh_anthropic_profile_oauth_if_due(dir.path(), &profile).is_err(),
        "refresh must fail closed when attempt-ms marker write fails"
    );
    assert_eq!(token_hits.load(Ordering::SeqCst), 0);
    assert!(!profile.join(OAUTH_REFRESH_RESERVATION_FILE).exists());
    assert_eq!(
        std::fs::read(profile.join(".credentials.json")).unwrap(),
        raw_initial
    );
    std::fs::remove_dir(profile.join(OAUTH_REFRESH_ATTEMPT_FILE)).unwrap();

    // Case 3: Writable profile_root reserves space, rotates once, persists atomically, and cleans reservation.
    refresh_anthropic_profile_oauth_if_due(dir.path(), &profile).unwrap();
    assert_eq!(token_hits.load(Ordering::SeqCst), 1);
    assert!(!profile.join(OAUTH_REFRESH_RESERVATION_FILE).exists());
    assert!(!profile.join(".anyagents-pending-credentials.json").exists());
    assert!(profile.join(OAUTH_REFRESH_ATTEMPT_FILE).is_file());
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let val: serde_json::Value =
            serde_json::from_slice(&std::fs::read(profile.join(rel)).unwrap()).unwrap();
        assert_eq!(
            val.pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str),
            Some("rotated-access-token")
        );
        assert_eq!(
            val.pointer("/claudeAiOauth/refreshToken")
                .and_then(serde_json::Value::as_str),
            Some("rotated-refresh-token")
        );
    }

    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();
}

#[test]
#[cfg(unix)]
fn enospc_and_write_failures_preserve_antigravity_and_anthropic_credentials() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    let agy_prof = dir.path().join("profiles/google-antigravity/default");
    let agy_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
    let agy_tok = agy_prof.join(agy_rel);
    let agy_initial = br#"{"token":{"access_token":"ya29.orig","refresh_token":"1//keep-this-refresh"},"email":"operator@example.com"}"#;
    write_required_file(&agy_tok, agy_initial, 0o600).unwrap();

    // Simulate mid-write ENOSPC (errno 28) during token refresh persistence.
    let enospc_err = anyagents_harness::atomic_write_with(
        &agy_tok,
        br#"{"token":{"access_token":"ya29.new","refresh_token":"1//keep-this-refresh"}}"#,
        0o600,
        true,
        |file, data| {
            use std::io::Write as _;
            file.write_all(&data[..20])?;
            Err(std::io::Error::from_raw_os_error(28))
        },
    )
    .unwrap_err();
    assert_eq!(enospc_err.raw_os_error(), Some(28));
    assert_eq!(std::fs::read(&agy_tok).unwrap(), agy_initial);

    // Lock parent dir (0o500) while leaving token file writable (0o600):
    // naive std::fs::write with O_TRUNC would truncate agy_tok to 0 bytes,
    // whereas write_required_file / persist_refreshed_credentials preserves it intact.
    let agy_parent = agy_tok.parent().unwrap();
    std::fs::set_permissions(agy_parent, std::fs::Permissions::from_mode(0o500)).unwrap();
    let unprivileged = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(agy_parent.join(".probe"))
        .is_err();
    let write_res = write_required_file(
        &agy_tok,
        br#"{"token":{"access_token":"ya29.blocked"}}"#,
        0o600,
    );
    std::fs::set_permissions(agy_parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    let _ = std::fs::remove_file(agy_parent.join(".probe"));
    if unprivileged {
        assert!(write_res.is_err());
        assert_eq!(std::fs::read(&agy_tok).unwrap(), agy_initial);
    }

    // Also verify Anthropic dual-file credential persistence never truncates on write failure.
    let claude_prof = dir.path().join("profiles/anthropic/default");
    let claude_initial = br#"{"claudeAiOauth":{"accessToken":"sk-ant-oat01-orig","refreshToken":"sk-ant-ort01-keep"}}"#;
    write_required_file(
        &claude_prof.join(".credentials.json"),
        claude_initial,
        0o600,
    )
    .unwrap();
    write_required_file(
        &claude_prof.join(".claude/.credentials.json"),
        claude_initial,
        0o600,
    )
    .unwrap();
    let claude_sub = claude_prof.join(".claude");
    std::fs::set_permissions(&claude_sub, std::fs::Permissions::from_mode(0o500)).unwrap();
    let res_claude = persist_refreshed_credentials(
        &claude_prof,
        &[".credentials.json", ".claude/.credentials.json"],
        br#"{"claudeAiOauth":{"accessToken":"sk-ant-oat01-next","refreshToken":"sk-ant-ort01-next"}}"#, 
    );
    std::fs::set_permissions(&claude_sub, std::fs::Permissions::from_mode(0o700)).unwrap();
    if unprivileged {
        assert!(res_claude.is_err());
        assert_eq!(
            std::fs::read(claude_prof.join(".claude/.credentials.json")).unwrap(),
            claude_initial
        );
    }
}

#[test]
#[cfg(unix)]
fn staged_signin_commits_declared_agy_files_ignoring_cli_log_symlink_and_runtime_garbage() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    let dir = tempfile::tempdir().unwrap();
    let held = SigninStaging::create(dir.path(), "google-antigravity", "4242").unwrap();
    let staging = held.path().to_path_buf();
    let agy_dir = staging.join(".gemini/antigravity-cli");
    let cfg_dir = staging.join(".gemini/config");
    std::fs::create_dir_all(agy_dir.join("log")).unwrap();
    std::fs::create_dir_all(agy_dir.join("cache")).unwrap();
    std::fs::create_dir_all(agy_dir.join("crashes")).unwrap();
    std::fs::create_dir_all(agy_dir.join("brain/session-1")).unwrap();
    std::fs::create_dir_all(agy_dir.join("builtin/skills")).unwrap();
    std::fs::create_dir_all(&cfg_dir).unwrap();

    let oauth_bytes = br#"{"token":{"access_token":"ya29.staged-ok","refresh_token":"1//staged-refresh"},"email":"operator@example.com","account_id":"acct-staged-1"}"#;
    std::fs::write(agy_dir.join("antigravity-oauth-token"), oauth_bytes).unwrap();
    std::fs::write(
        agy_dir.join("antigravity_state.pbtxt"),
        b"onboarding_done: true\n",
    )
    .unwrap();
    std::fs::write(agy_dir.join("jetski_state.pbtxt"), b"tier: standard\n").unwrap();
    std::fs::write(cfg_dir.join("config.json"), br#"{"theme":"dark"}"#).unwrap();
    std::fs::write(cfg_dir.join(".migrated"), b"1\n").unwrap();

    // Vendor runtime artifacts created by agy during interactive OAuth sign-in:
    std::fs::write(agy_dir.join("log/cli-20261111.log"), b"agy startup log\n").unwrap();
    symlink("log/cli-20261111.log", agy_dir.join("cli.log")).unwrap();
    symlink("/nonexistent/agy.sock", agy_dir.join("dangling.sock")).unwrap();
    std::fs::write(agy_dir.join("cache/blob.bin"), b"ephemeral").unwrap();
    std::fs::write(agy_dir.join("crashes/minidump.dmp"), b"crash").unwrap();
    std::fs::write(agy_dir.join("brain/session-1/notes.md"), b"scratch").unwrap();
    std::fs::write(agy_dir.join("builtin/skills/SKILL.md"), b"builtin").unwrap();
    std::fs::write(agy_dir.join("conversation_summaries.db"), b"sqlite-cache").unwrap();
    std::fs::write(agy_dir.join("conversation_summaries.db-wal"), b"wal").unwrap();

    let (chosen, final_dir, acct) =
        resolve_and_commit_staged_signin(dir.path(), "google-antigravity", &staging, None)
            .expect("staged sign-in must succeed despite cli.log symlink and runtime caches");

    assert_eq!(chosen, "default");
    assert_eq!(acct.as_deref(), Some("operator@example.com"));
    assert!(
        !staging.exists(),
        "staging_dir must be cleaned up after commit"
    );

    let committed_tok = final_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
    assert_eq!(std::fs::read(&committed_tok).unwrap(), oauth_bytes);
    assert_eq!(
        std::fs::metadata(&committed_tok)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(
        final_dir
            .join(".gemini/antigravity-cli/antigravity_state.pbtxt")
            .is_file()
    );
    assert!(
        final_dir
            .join(".gemini/antigravity-cli/jetski_state.pbtxt")
            .is_file()
    );
    assert!(final_dir.join(".gemini/config/config.json").is_file());
    assert!(final_dir.join(".gemini/config/.migrated").is_file());

    for excluded in [
        ".gemini/antigravity-cli/cli.log",
        ".gemini/antigravity-cli/dangling.sock",
        ".gemini/antigravity-cli/log",
        ".gemini/antigravity-cli/cache",
        ".gemini/antigravity-cli/crashes",
        ".gemini/antigravity-cli/brain",
        ".gemini/antigravity-cli/builtin",
        ".gemini/antigravity-cli/conversation_summaries.db",
        ".gemini/antigravity-cli/conversation_summaries.db-wal",
    ] {
        assert!(
            std::fs::symlink_metadata(final_dir.join(excluded)).is_err(),
            "vendor runtime artifact must not be copied into profile dir: {excluded}"
        );
    }
}

#[test]
#[cfg(unix)]
fn staged_signin_refuses_symlinked_credential_or_state_file_fail_closed() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let outside_secret = dir.path().join("outside-secret.json");
    std::fs::write(
        &outside_secret,
        br#"{"token":{"access_token":"ya29.stolen","refresh_token":"1//stolen"},"email":"victim@example.com"}"#, 
    )
    .unwrap();

    // Case 1: Primary credential file itself is a symlink -> fail closed.
    let stage1 = dir
        .path()
        .join("profiles/google-antigravity/.signin-staging-sym-1");
    std::fs::create_dir_all(stage1.join(".gemini/antigravity-cli")).unwrap();
    symlink(
        &outside_secret,
        stage1.join(".gemini/antigravity-cli/antigravity-oauth-token"),
    )
    .unwrap();
    let err1 = resolve_and_commit_staged_signin(dir.path(), "google-antigravity", &stage1, None)
        .unwrap_err();
    assert!(
        err1.to_string().contains("regular file"),
        "unexpected error for symlinked primary credential: {err1}"
    );
    assert!(
        !dir.path()
            .join("profiles/google-antigravity/default")
            .exists(),
        "failed staged sign-in must not commit profile directory"
    );

    // Case 2: Companion state file (jetski_state.pbtxt) is a symlink -> fail closed.
    let stage2 = dir
        .path()
        .join("profiles/google-antigravity/.signin-staging-sym-2");
    std::fs::create_dir_all(stage2.join(".gemini/antigravity-cli")).unwrap();
    std::fs::write(
        stage2.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"token":{"access_token":"ya29.valid","refresh_token":"1//valid"},"email":"operator@example.com"}"#, 
    )
    .unwrap();
    symlink(
        &outside_secret,
        stage2.join(".gemini/antigravity-cli/jetski_state.pbtxt"),
    )
    .unwrap();
    let err2 = resolve_and_commit_staged_signin(dir.path(), "google-antigravity", &stage2, None)
        .unwrap_err();
    assert!(
        err2.to_string().contains("regular file"),
        "unexpected error for symlinked state file: {err2}"
    );
    assert!(
        !dir.path()
            .join("profiles/google-antigravity/default")
            .exists()
    );

    // Case 3: Parent segment (.gemini/antigravity-cli) is a symlink -> fail closed.
    let external_dir = dir.path().join("external-agy-dir");
    std::fs::create_dir_all(&external_dir).unwrap();
    std::fs::write(
        external_dir.join("antigravity-oauth-token"),
        br#"{"token":{"access_token":"ya29.ext","refresh_token":"1//ext"}}"#,
    )
    .unwrap();
    let stage3 = dir
        .path()
        .join("profiles/google-antigravity/.signin-staging-sym-3");
    std::fs::create_dir_all(stage3.join(".gemini")).unwrap();
    symlink(&external_dir, stage3.join(".gemini/antigravity-cli")).unwrap();
    let err3 = resolve_and_commit_staged_signin(dir.path(), "google-antigravity", &stage3, None)
        .unwrap_err();
    assert!(
        err3.to_string().contains("symlink"),
        "unexpected error for symlinked parent segment: {err3}"
    );
    assert!(
        !dir.path()
            .join("profiles/google-antigravity/default")
            .exists()
    );
}

#[test]
fn archetype_defaults_walk_parent_id_ancestors_with_cycle_guard_and_nofollow() {
    let dir = crate::test_tempdir();
    let data_dir = dir.path().join("data");
    let root_repo = dir.path().join("root-repo");
    std::fs::create_dir_all(root_repo.join(".agents/agents")).unwrap();
    std::fs::create_dir_all(data_dir.join(".agents/agents")).unwrap();

    std::fs::write(
        root_repo.join(".agents/agents/orchestrator.md"),
        "---\nscope: private\n---\nRoot orchestrator instructions.",
    )
    .unwrap();
    std::fs::write(
        root_repo.join(".agents/agents/writer.md"),
        "---\nscope: isolated\n---\nRoot writer instructions.",
    )
    .unwrap();
    std::fs::write(
        root_repo.join(".agents/agents/engineer.md"),
        "---\nscope: private\n---\nFresh root engineer instructions.",
    )
    .unwrap();
    std::fs::write(
        data_dir.join(".agents/agents/engineer.md"),
        "---\nscope: private\n---\nStale data_dir engineer instructions.",
    )
    .unwrap();
    std::fs::write(
        data_dir.join(".agents/agents/retired-director.md"),
        "---\nscope: public\n---\nStale retired role in data_dir.",
    )
    .unwrap();

    let store = Store::open(data_dir.join("state.db")).unwrap();
    let mut l0_new = NewAgent::new("l0-root", "spiron");
    l0_new.workdir = Some(root_repo.display().to_string());
    let l0 = store.create_agent(l0_new).unwrap();

    let mut l1_new = NewAgent::new("l1-mid", "spiron");
    l1_new.parent_id = Some(l0.id.clone());
    let l1 = store.create_agent(l1_new).unwrap();

    for (role_name, expected_scope, expected_body) in [
        ("orchestrator", "private", "Root orchestrator instructions."),
        ("writer", "isolated", "Root writer instructions."),
        ("engineer", "private", "Fresh root engineer instructions."),
    ] {
        let mut req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
            "name": format!("l2-{role_name}"),
            "role": role_name,
            "workdir": "private"
        }))
        .unwrap();
        apply_archetype_defaults(Some(&store), &data_dir, Some(&l1), &mut req).unwrap();
        assert_eq!(req.scope.as_deref(), Some(expected_scope));
        assert!(req.instructions.is_none());
        let mut probe = l1.clone();
        probe.role = req.role.clone();
        probe.instructions = req.instructions.clone();
        probe.workdir = None;
        assert_eq!(
            effective_agent_instructions(Some(&store), &data_dir, &probe).as_deref(),
            Some(format!("{BUILTIN_GENERAL_ROLE_BODY}\n\n{expected_body}").as_str())
        );
    }

    // A role deleted/retired in the authoritative workspace .agents/agents must NOT fall back
    // to a stale residual file under data_dir/.agents/agents.
    let mut retired_role_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "l2-retired-role",
            "role": "retired-director",
            "workdir": "private"
        }))
        .unwrap();
    let retired_err =
        apply_archetype_defaults(Some(&store), &data_dir, Some(&l1), &mut retired_role_req)
            .unwrap_err()
            .to_string();
    assert!(
        retired_err.contains("unknown archetype template \"retired-director\"")
            && !retired_err.contains("available: retired-director")
            && !retired_err.contains(", retired-director"),
        "stale data_dir/.agents/agents template must not satisfy a workspace role: {retired_err}"
    );
    assert!(
        retired_role_req.instructions.is_none(),
        "stale data_dir/.agents/agents template must not pollute workspace .agents/agents"
    );
    let mut retired_arch_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "l2-retired-arch",
            "archetype": "retired-director",
            "workdir": "private"
        }))
        .unwrap();
    assert!(
        apply_archetype_defaults(Some(&store), &data_dir, Some(&l1), &mut retired_arch_req)
            .is_err()
    );

    let l1_work = anyagents_core::agent_work_dir(&data_dir, &l1.id);
    std::fs::create_dir_all(l1_work.join(".agents/agents")).unwrap();
    std::fs::write(
        l1_work.join(".agents/agents/writer.md"),
        "---\nscope: private\n---\nL1 local writer override.",
    )
    .unwrap();
    let mut override_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "l2-writer-override",
            "role": "writer",
            "workdir": "private"
        }))
        .unwrap();
    apply_archetype_defaults(Some(&store), &data_dir, Some(&l1), &mut override_req).unwrap();
    assert!(override_req.instructions.is_none());
    let mut override_probe = l1.clone();
    override_probe.parent_id = Some(l1.id.clone());
    override_probe.role = override_req.role.clone();
    override_probe.instructions = override_req.instructions.clone();
    override_probe.workdir = None;
    assert_eq!(
        effective_agent_instructions(Some(&store), &data_dir, &override_probe).as_deref(),
        Some(format!("{BUILTIN_GENERAL_ROLE_BODY}\n\nL1 local writer override.").as_str())
    );

    {
        let conn = rusqlite::Connection::open(data_dir.join("state.db")).unwrap();
        conn.execute(
            "UPDATE agents SET parent_id = ?1 WHERE id = ?2",
            rusqlite::params![l1.id.as_str(), l0.id.as_str()],
        )
        .unwrap();
    }
    let wds = ancestor_archetype_workdirs(Some(&store), &data_dir, Some(&l1)).unwrap();
    assert_eq!(wds.len(), 2);

    #[cfg(unix)]
    {
        let outside = dir.path().join("outside-secret.md");
        std::fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink(&outside, root_repo.join(".agents/agents/escape.md")).unwrap();
        let mut escape_req: crate::api::CreateAgentRequest =
            serde_json::from_value(serde_json::json!({
                "name": "l2-escape",
                "archetype": "escape",
                "workdir": "private"
            }))
            .unwrap();
        let err = apply_archetype_defaults(Some(&store), &data_dir, Some(&l1), &mut escape_req)
            .unwrap_err();
        assert!(err.to_string().contains("symlink"));
    }
}

#[test]
fn reset_agent_session_clears_the_last_error_and_leaves_mail_to_the_cursor() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let mut agent = store
        .create_agent(NewAgent::new("spiron-node", "spiron"))
        .unwrap();
    agent = store.set_agent_state(&agent.id, "paused").unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    let config_dir = root.join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    anyagents_core::host_session::new_session(&root, "spiron").unwrap();
    let err_path = config_dir.join(".anyagents-last-error.json");
    std::fs::write(&err_path, r#"{"error":"stale","timestamp_ms":100}"#).unwrap();
    let anyagents_core::ReceiveOutcome::New(m1) = store
        .receive(&agent.id, anyagents_core::Incoming::new("taken"))
        .unwrap()
    else {
        panic!("expected m1");
    };
    store.note_delivered(&agent.id, m1.seq).unwrap();
    let anyagents_core::ReceiveOutcome::New(m2) = store
        .receive(&agent.id, anyagents_core::Incoming::new("not taken"))
        .unwrap()
    else {
        panic!("expected m2");
    };

    let first = reset_agent_session_locked(&store, &agent).unwrap();
    let second = reset_agent_session_locked(&store, &agent).unwrap();
    assert_ne!(first["generation"], second["generation"]);
    assert!(!err_path.exists(), "a reset clears the stale error");
    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), m1.seq);
    assert_eq!(
        store.next_undelivered(&agent.id).unwrap().unwrap().id,
        m2.id,
        "the new session is handed what the old one never took"
    );
}

fn bind_test_store() -> (tempfile::TempDir, Store) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    (dir, store)
}

fn unbound_test_agent(store: &Store, data: &Path, name: &str) -> Agent {
    let agent = store.create_agent(NewAgent::new(name, "codex")).unwrap();
    let cfg = agent_config_dir(data, &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::remove_file(cfg.join(".anyagents-profile")).ok();
    agent
}

fn stage_profile(data: &Path, name: &str) -> PathBuf {
    let dir = data.join("profiles").join("openai").join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_exhausted_usage(dir: &Path) {
    let nl = char::from_u32(10).unwrap();
    let text = format!("5h: 0% left{nl}7d: 85% left{nl}");
    std::fs::write(dir.join(".anyagents-last-usage.txt"), text).unwrap();
}

fn write_expired_oauth(dir: &Path, with_recent_attempt: bool) {
    let cred = serde_json::json!({
        "claudeAiOauth": {
            "expiresAt": anyagents_core::now_ms() - 60_000,
            "refreshToken": "rust-dummy"
        }
    });
    std::fs::write(dir.join(".credentials.json"), cred.to_string()).unwrap();
    if with_recent_attempt {
        std::fs::write(
            dir.join(OAUTH_REFRESH_ATTEMPT_FILE),
            anyagents_core::now_ms().to_string(),
        )
        .unwrap();
    }
}

#[test]
fn initial_bind_skips_exhausted_default_for_healthy_sibling() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    let healthy = stage_profile(data, "team-alpha");
    write_exhausted_usage(&default);
    let agent = unbound_test_agent(&store, data, "exhausted-default");
    let cfg = agent_config_dir(data, &agent.id);
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![healthy]);
    assert_eq!(
        bound_profile_name(&cfg).as_deref(),
        Some("openai/team-alpha")
    );
}

#[test]
fn initial_bind_keeps_unhealthy_default_when_no_healthy_sibling() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    write_exhausted_usage(&default);
    let agent = unbound_test_agent(&store, data, "lonely-default");
    let cfg = agent_config_dir(data, &agent.id);
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![default]);
}

#[test]
fn initial_bind_keeps_healthy_default() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    let _sibling = stage_profile(data, "team-beta");
    let agent = unbound_test_agent(&store, data, "healthy-default");
    let cfg = agent_config_dir(data, &agent.id);
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![default]);
}

#[test]
fn initial_bind_skips_default_with_expired_oauth_and_recent_failed_refresh() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    let healthy = stage_profile(data, "team-alpha");
    write_expired_oauth(&default, true);
    let agent = unbound_test_agent(&store, data, "expired-default");
    let cfg = agent_config_dir(data, &agent.id);
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![healthy]);
}

#[test]
fn initial_bind_treats_expired_oauth_without_recent_failed_refresh_as_healthy() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    let _sibling = stage_profile(data, "team-alpha");
    write_expired_oauth(&default, false);
    let agent = unbound_test_agent(&store, data, "expired-no-attempt");
    let cfg = agent_config_dir(data, &agent.id);
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![default]);
}

#[test]
fn initial_bind_never_overrides_an_explicit_binding() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = stage_profile(data, "default");
    let explicit = stage_profile(data, "team-beta");
    write_exhausted_usage(&default);
    let agent = unbound_test_agent(&store, data, "explicit-bound");
    let cfg = agent_config_dir(data, &agent.id);
    let nl = char::from_u32(10).unwrap();
    let marker = format!("openai/team-beta{nl}");
    std::fs::write(cfg.join(".anyagents-profile"), marker).unwrap();
    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![explicit]);
}

#[test]
fn worktree_keyword_without_repo_falls_back_to_private_workdir_when_parent_not_git_repo() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = anyagents_core::Store::open(data.join("anyagents.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let non_git_parent = create(
        &store,
        data,
        CreateRequest {
            name: "non-git-parent".to_string(),
            role: Some("director".to_string()),
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: None,
        },
    )
    .unwrap();
    assert!(!is_git_repo_dir(Path::new(
        non_git_parent.workdir.as_deref().unwrap()
    )));

    let child_under_non_git = create(
        &store,
        data,
        CreateRequest {
            name: "child-non-git".to_string(),
            role: Some("engineer".to_string()),
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(PathBuf::from("worktree")),
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: Some(non_git_parent.id.clone()),
        },
    )
    .unwrap();
    let expected_private_wd = anyagents_core::agent_config_dir(data, &child_under_non_git.id)
        .parent()
        .unwrap()
        .join("work");
    assert_eq!(
        PathBuf::from(child_under_non_git.workdir.as_deref().unwrap()),
        expected_private_wd
    );
    assert!(!is_git_repo_dir(&expected_private_wd));

    let explicit_inherit_err = create(
        &store,
        data,
        CreateRequest {
            name: "child-explicit-inherit".to_string(),
            role: Some("engineer".to_string()),
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: Some("inherit".to_string()),
            branch: None,
            base: None,
            workdir: Some(PathBuf::from("worktree")),
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: Some(non_git_parent.id.clone()),
        },
    )
    .unwrap_err();
    assert!(
        explicit_inherit_err
            .to_string()
            .contains("not a git repository"),
        "unexpected error: {explicit_inherit_err}"
    );

    let fixture = crate::repo::tests::Fixture::new(data);
    let git_parent = create_with_consumer(
        &store,
        data,
        CreateRequest {
            name: "git-parent".to_string(),
            role: Some("director".to_string()),
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(fixture.source.clone()),
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: None,
        },
        &fixture.checkouts,
    )
    .unwrap();

    let child_under_git = create_with_consumer(
        &store,
        data,
        CreateRequest {
            name: "child-git".to_string(),
            role: Some("engineer".to_string()),
            harness: "agy".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(PathBuf::from("worktree")),
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: Some(git_parent.id.clone()),
        },
        &fixture.checkouts,
    )
    .unwrap();
    let child_git_wd = PathBuf::from(child_under_git.workdir.as_deref().unwrap());
    fixture.assert_checkout(&child_git_wd);
}

#[test]
fn spiron_standalone_seed_uses_model_provider_key() {
    for profile_layout in [false, true] {
        for (model, provider) in [
            ("sylphx/kimi-code-plan/k3.1-pro#max", "kimi-code-plan"),
            ("openrouter/openai/model#xhigh", "openrouter"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path();
            let store = Store::open(data.join("state.db")).unwrap();
            let mut spec = NewAgent::new("seed-model", "spiron");
            spec.model = Some(model.into());
            let agent = store.create_agent(spec).unwrap();
            let cfg = agent_config_dir(data, &agent.id);
            let source = if profile_layout {
                data.join("profiles").join(provider).join("default")
            } else {
                cfg.clone()
            };
            std::fs::create_dir_all(source.join(".config/opencode")).unwrap();
            std::fs::write(
                source
                    .join(".config/opencode")
                    .join(format!("{provider}.key")),
                "provider-test-key",
            )
            .unwrap();
            std::fs::write(source.join(".config/opencode/sylphx.key"), "wrong-test-key").unwrap();
            seed_harness_credentials(&store, data, &agent, &cfg).unwrap();
            assert_eq!(
                std::fs::read_to_string(
                    source
                        .join(".config/opencode")
                        .join(format!("{provider}.key"))
                )
                .unwrap(),
                "provider-test-key"
            );
            for mirror in ["auth.json", ".spiron/auth.json", ".config/sylphx/auth.json"] {
                let auth: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(cfg.join(mirror)).unwrap()).unwrap();
                assert_eq!(auth["access_token"], "provider-test-key");
            }
        }
    }
}

#[test]
fn spiron_muse_profile_preserves_gateway_auth_and_normalizes_model() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let model = "sylphx/openai/gpt-6.1-sol#high";
    let mut spec = NewAgent::new("spiron-profile", "spiron");
    spec.model = Some(model.into());
    let agent = store.create_agent(spec).unwrap();
    let cfg = agent_config_dir(data, &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    let profile = data.join("profiles/sylphx/muse-default");
    std::fs::create_dir_all(&profile).unwrap();
    let selector = br#"{"gateway_base_url":"https://gateway.example/v1"}"#;
    let auth = br#"{"access_token":"test-profile-token"}"#;
    std::fs::write(profile.join("sylphx-agent.json"), selector).unwrap();
    std::fs::write(profile.join("auth.json"), auth).unwrap();
    std::fs::write(
        profile.join("profile.json"),
        br#"{"compatible_harnesses":["spiron"]}"#,
    )
    .unwrap();
    std::fs::write(profile.join("config.toml"), "model = \"stale/model\"\n").unwrap();
    ensure_profile_compatible_and_mapped(
        data,
        "sylphx",
        "muse-default",
        "spiron",
        Some(model),
        Some(&cfg),
    )
    .unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "sylphx/muse-default\n").unwrap();
    seed_harness_credentials(&store, data, &agent, &cfg).unwrap();
    let work = data.join("work");
    std::fs::create_dir_all(&work).unwrap();
    set_agent_harness(
        &store,
        data,
        "operator",
        &agent.name,
        "spiron",
        Some(model),
        Some("sylphx/muse-default"),
    )
    .unwrap();
    let agent = store.agent(&agent.id).unwrap();
    reconcile_agent_runtime_config(&store, data, &agent, &cfg, &work).unwrap();
    verify_harness_runtime_config(&cfg, "spiron", Some(model)).unwrap();
    for mirror in [".spiron/auth.json", ".config/sylphx/auth.json"] {
        let auth: serde_json::Value =
            serde_json::from_slice(&std::fs::read(cfg.join(mirror)).unwrap()).unwrap();
        assert_eq!(auth["access_token"], "test-profile-token");
    }
    assert_eq!(
        std::fs::read(cfg.join("sylphx-agent.json")).unwrap(),
        selector
    );
    let seeded: serde_json::Value =
        serde_json::from_slice(&std::fs::read(cfg.join("auth.json")).unwrap()).unwrap();
    assert_eq!(seeded["access_token"], "test-profile-token");
    let config = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
    assert!(!config.contains("stale/model"), "{config}");
    assert!(
        config.contains("sylphx_ai_url = \"https://gateway.example/v1\""),
        "{config}"
    );
    assert!(
        config.contains("model = \"openai/gpt-6.1-sol#high\""),
        "{config}"
    );
}

#[test]
fn spiron_profile_switch_removes_stale_seat_selector_and_refreshes_gateway_url() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let mut new_agent = NewAgent::new("spiron-switch", "spiron");
    new_agent.model = Some("openrouter/model-a".to_string());
    let agent = store.create_agent(new_agent).unwrap();
    let cfg = agent_config_dir(data, &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();

    let alt = data.join("profiles/gw/alt");
    std::fs::create_dir_all(&alt).unwrap();
    std::fs::write(
        alt.join("sylphx-agent.json"),
        br#"{"gateway_base_url":"https://alt.example/v1"}"#,
    )
    .unwrap();
    std::fs::write(
        alt.join("config.toml"),
        b"sylphx_ai_url = \"https://alt.example/v1\"\nalways_approve = true\n",
    )
    .unwrap();
    std::fs::write(alt.join("auth.json"), br#"{"access_token":"tok-a"}"#).unwrap();

    let primary = data.join("profiles/gw/default");
    std::fs::create_dir_all(primary.join(".config/opencode")).unwrap();
    std::fs::write(
        primary.join(".config/opencode/opencode.json"),
        br#"{"provider":{"openrouter":{"options":{"baseURL":"https://gw.example/v1"}}}}"#,
    )
    .unwrap();
    std::fs::write(primary.join("auth.json"), br#"{"access_token":"tok-b"}"#).unwrap();

    write_profile_marker(&cfg, ".anyagents-profile", "gw/alt\n").unwrap();
    seed_harness_credentials(&store, data, &agent, &cfg).unwrap();
    assert!(cfg.join("sylphx-agent.json").exists());

    write_profile_marker(&cfg, ".anyagents-profile", "gw/default\n").unwrap();
    seed_harness_credentials(&store, data, &agent, &cfg).unwrap();
    crate::spiron::write_spiron_config(
        &cfg,
        agent.model.as_deref(),
        std::path::Path::new("/usr/bin/anyagents"),
        &["mcp".to_string()],
    )
    .unwrap();

    assert!(!cfg.join("sylphx-agent.json").exists());
    let toml = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
    assert!(toml.contains("https://gw.example/v1"), "{toml}");
    assert!(!toml.contains("https://alt.example/v1"), "{toml}");
}

#[test]
fn seed_harness_credentials_writes_empty_gemini_migrated_marker() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("agy-migrated", "agy"))
        .unwrap();
    let cfg = agent_config_dir(data, &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();

    let prof = data.join("profiles/google-antigravity/default");
    std::fs::create_dir_all(prof.join(".gemini/antigravity-cli")).unwrap();
    std::fs::create_dir_all(prof.join(".gemini/config")).unwrap();
    std::fs::write(
        prof.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"email":"a@example.com","token":{"access_token":"ya29.ok","refresh_token":"1//r"}}"#,
    )
    .unwrap();
    std::fs::write(prof.join(".gemini/config/.migrated"), b"").unwrap();

    seed_harness_credentials(&store, data, &agent, &cfg).unwrap();
    assert!(
        cfg.join(".gemini/antigravity-cli/antigravity-oauth-token")
            .is_file()
    );
    let migrated = cfg.join(".gemini/config/.migrated");
    assert!(migrated.is_file());
    assert_eq!(std::fs::metadata(&migrated).unwrap().len(), 0);
}

#[test]
fn initial_bind_skips_google_default_with_expired_oauth_and_refresh_failure() {
    let (dir, store) = bind_test_store();
    let data = dir.path();
    let default = data.join("profiles/google-antigravity/default");
    let healthy = data.join("profiles/google-antigravity/team-alpha");
    std::fs::create_dir_all(default.join(".gemini/antigravity-cli")).unwrap();
    std::fs::create_dir_all(healthy.join(".gemini/antigravity-cli")).unwrap();

    let past_expiry = (chrono::Utc::now() - chrono::Duration::hours(2))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    std::fs::write(
        default.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        serde_json::json!({
            "email": "expired@example.com",
            "token": {
                "access_token": "ya29.expired",
                "expiry": past_expiry,
                "refresh_token": "1//revoked"
            }
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        default.join(OAUTH_REFRESH_ATTEMPT_FILE),
        anyagents_core::now_ms().to_string(),
    )
    .unwrap();
    std::fs::write(
        default.join(".anyagents-last-usage.txt"),
        b"__NONE__
",
    )
    .unwrap();

    std::fs::write(
        healthy.join(".gemini/antigravity-cli/antigravity-oauth-token"),
        br#"{"email":"healthy@example.com","token":{"access_token":"ya29.ok","refresh_token":"1//ok"}}"#,
    )
    .unwrap();

    let mut new_agent = NewAgent::new("agy-expired-default", "agy");
    new_agent.model = Some("google-antigravity/gemini-3-flash".to_string());
    let agent = store.create_agent(new_agent).unwrap();
    let cfg = agent_config_dir(data, &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();

    let picked = select_or_bind_profile(data, &[], &agent, &cfg);
    assert_eq!(picked, vec![healthy]);
    assert_eq!(
        bound_profile_name(&cfg).as_deref(),
        Some("google-antigravity/team-alpha")
    );
}

#[test]
fn sylphx_and_opencode_quota_probe_parse_and_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    // 1) Parse edge-gateway/online, active, currency ratios, and quota-limited strings.
    assert_eq!(
        parse_remaining_pct_from_usage("online (gateway-edge)", None),
        Some(100)
    );
    assert_eq!(
        parse_remaining_pct_from_usage("active (opencode-auth)", None),
        Some(100)
    );
    assert_eq!(
        parse_remaining_pct_from_usage("limited ↻10m · online (gateway-edge)", None),
        Some(0)
    );
    assert_eq!(
        parse_remaining_pct_from_usage("$4.00/$10.00 remaining", None),
        Some(40)
    );
    assert_eq!(parse_remaining_pct_from_usage("75% left", None), Some(75));
    assert!(compatible_harnesses_for("opencode").contains(&"omp"));
    assert!(compatible_harnesses_for("spiron-local").contains(&"sylphx"));

    // 2) Sylphx profiles rotate cleanly on active `.anyagents-quota-reset-ms`.
    let future_reset = anyagents_core::now_ms() + 300_000;
    let exhausted = data.join("profiles/sylphx/exhausted/.config/opencode");
    let healthy = data.join("profiles/sylphx/healthy/.config/opencode");
    std::fs::create_dir_all(&exhausted).unwrap();
    std::fs::create_dir_all(&healthy).unwrap();
    std::fs::write(exhausted.join("sylphx.key"), "sk-ex\n").unwrap();
    std::fs::write(healthy.join("sylphx.key"), "sk-ok\n").unwrap();
    std::fs::write(
        data.join("profiles/sylphx/exhausted/.anyagents-quota-reset-ms"),
        format!("{future_reset}\n"),
    )
    .unwrap();

    assert_eq!(
        parse_remaining_pct_from_usage(
            &probe_one_profile_usage(data, "sylphx", "exhausted").unwrap(),
            None
        ),
        Some(0)
    );
    assert_eq!(
        parse_remaining_pct_from_usage(
            &probe_one_profile_usage(data, "sylphx", "healthy").unwrap(),
            None
        ),
        Some(100)
    );

    let spiron_agent = store
        .create_agent(NewAgent::new("sylphx-rot", "spiron"))
        .unwrap();
    let spiron_cfg = agent_config_dir(data, &spiron_agent.id);
    std::fs::create_dir_all(&spiron_cfg).unwrap();
    std::fs::write(spiron_cfg.join(".anyagents-profile"), "sylphx/exhausted\n").unwrap();
    let rotated = try_rotate_exhausted_profile_in(&store, data, &spiron_agent, true, true);
    assert_eq!(rotated.as_deref(), Some("sylphx/healthy"));
    assert_eq!(
        bound_profile_name(&spiron_cfg).as_deref(),
        Some("sylphx/healthy")
    );

    // 3) OpenCode profiles probe auth.json expiry and rotate to healthy omp/opencode profile.
    let oc_expired = data.join("profiles/opencode/expired/.local/share/opencode");
    let oc_healthy = data.join("profiles/omp/fresh/.local/share/opencode");
    std::fs::create_dir_all(&oc_expired).unwrap();
    std::fs::create_dir_all(&oc_healthy).unwrap();
    let past_ms = anyagents_core::now_ms() - 60_000;
    let future_ms = anyagents_core::now_ms() + 3_600_000;
    std::fs::write(
        oc_expired.join("auth.json"),
        serde_json::json!({
            "openai": { "access": "tok-old", "expires": past_ms }
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        oc_healthy.join("auth.json"),
        serde_json::json!({
            "openai": { "access": "tok-fresh", "refresh": "ref-fresh", "expires": future_ms }
        })
        .to_string(),
    )
    .unwrap();

    assert_eq!(
        probe_one_profile_usage(data, "opencode", "expired").as_deref(),
        Some("oauth expired")
    );
    assert_eq!(
        probe_one_profile_usage(data, "omp", "fresh").as_deref(),
        Some("active (omp-auth)")
    );

    let oc_agent = store
        .create_agent(NewAgent::new("oc-rot", "opencode"))
        .unwrap();
    let oc_cfg = agent_config_dir(data, &oc_agent.id);
    std::fs::create_dir_all(&oc_cfg).unwrap();
    std::fs::write(oc_cfg.join(".anyagents-profile"), "opencode/expired\n").unwrap();
    let oc_rotated = try_rotate_exhausted_profile_in(&store, data, &oc_agent, true, true);
    assert_eq!(oc_rotated.as_deref(), Some("omp/fresh"));
}

#[test]
fn create_remote_required_fails_closed_without_sandbox_provider() {
    let dir = tempfile::tempdir().unwrap();
    let data = std::path::absolute(dir.path()).unwrap();
    let store = Store::open(data.join("state.db")).unwrap();
    let checkouts = crate::repo::Checkouts::configured(&data);
    let req = CreateRequest {
        name: "thin-test".into(),
        role: None,
        harness: "claude-code".into(),
        runtime: None,
        budget_cap_micros: None,
        model: None,
        profile: None,
        repo: None,
        branch: None,
        base: None,
        workdir: Some(PathBuf::from("isolated")),
        workdir_default: None,
        description: None,
        instructions: None,
        parent_id: None,
    };
    let err = create_with_seat_and_sandbox_mode(
        &store,
        &data,
        req,
        &checkouts,
        None,
        crate::sandbox_providers::SandboxMode::RemoteRequired,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("remote_required"),
        "unexpected error: {err}"
    );
}

#[test]
fn spiron_routes_to_cloud_json_while_spiron_local_requires_mcp_config() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let cloud_agent = store
        .create_agent(NewAgent::new("spiron-default-cloud", "spiron@cloud"))
        .unwrap();
    assert_eq!(cloud_agent.harness, "spiron");
    assert_eq!(cloud_agent.runtime, anyagents_core::ExecutionRuntime::Cloud);
    let cloud_cfg = agent_config_dir(data, &cloud_agent.id);
    let cloud_work = data.join("work-cloud");
    std::fs::create_dir_all(cloud_cfg.join(".spiron")).unwrap();
    std::fs::create_dir_all(&cloud_work).unwrap();
    std::fs::write(
        cloud_cfg.join(".spiron/auth.json"),
        r#"{"api_key":"spiron_sk_test_123"}"#,
    )
    .unwrap();
    reconcile_agent_runtime_config(&store, data, &cloud_agent, &cloud_cfg, &cloud_work).unwrap();
    verify_harness_runtime_config(&cloud_cfg, "spiron@cloud", cloud_agent.model.as_deref())
        .unwrap();
    let cloud_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(cloud_cfg.join("cloud.json")).unwrap()).unwrap();
    assert_eq!(cloud_json["api_key"], "spiron_sk_test_123");
    assert!(!cloud_cfg.join("spiron-mcp.json").exists());

    let local_agent = store
        .create_agent(NewAgent::new("spiron-explicit-local", "spiron-local"))
        .unwrap();
    assert_eq!(local_agent.harness, "spiron");
    assert_eq!(local_agent.runtime, anyagents_core::ExecutionRuntime::Local);
    let local_cfg = agent_config_dir(data, &local_agent.id);
    let local_work = data.join("work-local");
    std::fs::create_dir_all(&local_cfg).unwrap();
    std::fs::create_dir_all(&local_work).unwrap();
    reconcile_agent_runtime_config(&store, data, &local_agent, &local_cfg, &local_work).unwrap();
    verify_harness_runtime_config(&local_cfg, "spiron-local", local_agent.model.as_deref())
        .unwrap();
    assert!(local_cfg.join("spiron-mcp.json").is_file());
    assert!(local_cfg.join("config.toml").is_file());
    assert!(!local_cfg.join("cloud.json").exists());
}
