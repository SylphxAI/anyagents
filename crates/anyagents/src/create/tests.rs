use std::path::{Path, PathBuf};

#[cfg(unix)]
use anyagents_core::AgentId;
use anyagents_core::{Agent, Error, NewAgent, Store, agent_config_dir, agent_work_dir};

use super::profile_rotation::{count_bound_profile_agents, rebalance_provider_profiles};
use super::profiles::{
    COPY_EVALUATIONS, copy_seat_file_if_newer, ensure_provider_profile_meta,
    sync_and_list_provider_profiles_inner,
};
use super::quota::{
    anthropic_tier_label, format_anthropic_usage_summary,
    overlay_active_quota_reset_on_cached_usage,
};
use super::runtime::install_changed_harness;
#[cfg(unix)]
use super::runtime::{prepare_runtime_config_dir, repair_unseated_config_mode};
use super::spawn::create_with_consumer;
#[cfg(unix)]
use super::spawn::{
    MadeDirs, create_dir_all_recorded, create_private_agent_root, create_private_config_dir,
    open_agent_root, remove_made_dirs,
};
#[cfg(unix)]
use super::workspace::SKILL_PROJECTION_OWNER_SCRIPT;
use super::*;

#[cfg(unix)]
#[test]
fn owner_projection_script_flattens_synced_skills_without_touching_local_or_foreign_data() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let global = dir.path().join("skills");
    let agent = dir.path().join("agent");
    let config = agent.join("config");
    let overlay = agent.join("skills");
    let view = agent.join("skills-view");
    let agy = config.join(".gemini/config/skills");
    let skill = |root: &Path, name: &str, body: &str| {
        std::fs::create_dir_all(root.join(name)).unwrap();
        std::fs::write(root.join(name).join("SKILL.md"), body).unwrap();
    };
    skill(&global.join("synced/z"), "sample", "later");
    skill(&global.join("synced/a"), "sample", "first");
    skill(&global.join("synced/.bucket-temp"), "hidden", "hidden");
    skill(&global.join("synced/a"), ".hidden", "hidden");
    skill(&overlay.join("synced/a"), "sample", "overlay");
    skill(&global, "sample", "direct");
    skill(&agy, "local", "local bytes");
    let foreign = dir.path().join("foreign");
    skill(&foreign, "sample", "foreign bytes");
    symlink(foreign.join("sample"), agy.join("foreign")).unwrap();
    std::fs::write(global.join("synced/manifest.json"), "{}").unwrap();
    let project = || {
        for relative in ["skills-view", ".gemini/config/skills"] {
            let result = std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    SKILL_PROJECTION_OWNER_SCRIPT,
                    "skill-projection-owner",
                ])
                .arg(&config)
                .arg(relative)
                .arg(&view)
                .arg(&global)
                .arg(&overlay)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    };
    project();
    assert_eq!(
        std::fs::read_to_string(agy.join("sample/SKILL.md")).unwrap(),
        "direct"
    );
    assert!(!agy.join("hidden").exists());
    assert!(!agy.join(".hidden").exists());
    assert!(!agy.join("manifest.json").exists());
    std::fs::remove_dir_all(global.join("sample")).unwrap();
    project();
    assert_eq!(
        std::fs::read_to_string(agy.join("sample/SKILL.md")).unwrap(),
        "overlay"
    );
    std::fs::remove_dir_all(overlay.join("synced/a/sample")).unwrap();
    project();
    assert_eq!(
        std::fs::read_to_string(agy.join("sample/SKILL.md")).unwrap(),
        "first"
    );
    std::fs::remove_dir_all(global.join("synced/a/sample")).unwrap();
    std::fs::remove_dir_all(global.join("synced/z/sample")).unwrap();
    project();
    assert!(std::fs::symlink_metadata(agy.join("sample")).is_err());
    assert_eq!(
        std::fs::read_to_string(agy.join("local/SKILL.md")).unwrap(),
        "local bytes"
    );
    assert_eq!(
        std::fs::read_to_string(agy.join("foreign/SKILL.md")).unwrap(),
        "foreign bytes"
    );
}

#[test]
fn a_model_change_on_an_unconfigured_harness_installs_nothing() {
    let _environment = crate::server::tests::env_lock();
    let root = crate::test_tempdir();
    let installer = crate::install::Installer::new(root.path());
    // `agy` ships Unconfigured, so installing it fails ...
    assert!(installer.ensure("agy").is_err());
    // ... but a model or profile change keeps the binary the agent runs,
    // including under a legacy spelling of the same harness.
    for (previous, target) in [("agy", "agy"), ("sylphx", "spiron")] {
        let mut asked = Vec::new();
        install_changed_harness(root.path(), previous, target, |_, harness| {
            asked.push(harness.to_string());
            installer.ensure(harness)
        })
        .unwrap();
        assert!(
            asked.is_empty(),
            "{previous} -> {target} installed {asked:?}"
        );
    }
}

#[test]
fn a_harness_change_still_verifies_the_target_harness() {
    let _environment = crate::server::tests::env_lock();
    let root = crate::test_tempdir();
    let installer = crate::install::Installer::new(root.path());
    let mut asked = Vec::new();
    let err = install_changed_harness(root.path(), "codex", "agy", |_, harness| {
        asked.push(harness.to_string());
        installer.ensure(harness)
    })
    .unwrap_err();
    assert_eq!(asked, ["agy"]);
    assert!(err.to_string().contains("unverifiable source"), "{err}");
}

#[test]
fn legacy_harness_selectors_create_and_switch_to_spiron_without_renaming_gateway() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let model = crate::spiron::DEFAULT_SPIRON_MODEL;
    for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
        .into_iter()
        .enumerate()
    {
        let (harness, resolved_model, profile) =
            resolve_subagent_defaults(data, None, legacy, None, Some("sylphx/default".into()));
        assert_eq!(harness, "spiron");
        assert_eq!(resolved_model.as_deref(), Some(model));
        assert_eq!(profile.as_deref(), Some("sylphx/default"));
        let created = create(
            &store,
            data,
            CreateRequest {
                name: format!("legacy-{index}"),
                role: None,
                harness: legacy.into(),
                model: None,
                profile: Some("sylphx/default".into()),
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
        assert_eq!(created.harness, "spiron");
        assert_eq!(created.model.as_deref(), Some(model));
        assert_eq!(upstream_provider_of(legacy, None), "sylphx");
        let switched =
            set_agent_harness(&store, data, "operator", &created.name, legacy, None, None).unwrap();
        assert_eq!(switched["harness"], "spiron");
        assert_eq!(switched["model"], model);
        assert_eq!(switched["profile"], "sylphx/default");
    }
}

#[test]
fn legacy_profile_compatibility_lists_expose_only_canonical_harnesses() {
    let dir = crate::test_tempdir();
    let profile = dir.path().join("profiles/sylphx/default");
    std::fs::create_dir_all(&profile).unwrap();
    std::fs::write(profile.join("profile.json"), serde_json::json!({
        "compatible_harnesses": ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default", "spiron", "codex"]
    }).to_string()).unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let profiles = sync_and_list_provider_profiles_inner(&store, dir.path(), false, false);
    let entry = profiles
        .iter()
        .find(|entry| entry.provider == "sylphx")
        .unwrap();
    assert_eq!(entry.compatible_harnesses, ["spiron", "codex"]);
    for provider in [
        "sylphx",
        "qwen-token-plan",
        "kimi-code-plan",
        "glm-code-plan",
        "minimax-code-plan",
    ] {
        let (_, harnesses) = default_kind_and_harnesses(provider);
        assert_eq!(harnesses.iter().filter(|h| *h == "spiron").count(), 1);
        assert!(!harnesses.iter().any(|h| h.starts_with("sylphx")));
    }
}

#[test]
fn legacy_subagent_defaults_are_canonical_but_provider_profiles_are_preserved() {
    let dir = crate::test_tempdir();
    std::fs::write(dir.path().join("subagent-defaults.json"), serde_json::json!({
        "default": {"mode": "custom", "harness": "sylphx/default", "model": "sylphx/openai/gpt-6.1-sol#medium", "profile": "sylphx/default"},
        "agents": {"parent": {"mode": "custom", "harness": "sylphx-agent", "profile": "sylphx/default"}}
    }).to_string()).unwrap();
    let cfg = load_subagent_defaults(dir.path());
    assert_eq!(cfg.default.harness.as_deref(), Some("spiron"));
    assert_eq!(cfg.agents["parent"].harness.as_deref(), Some("spiron"));
    assert_eq!(cfg.default.profile.as_deref(), Some("sylphx/default"));
    assert_eq!(
        cfg.default.model.as_deref(),
        Some(crate::spiron::DEFAULT_SPIRON_MODEL)
    );
    save_subagent_defaults(dir.path(), &cfg).unwrap();
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("subagent-defaults.json")).unwrap())
            .unwrap();
    assert_eq!(saved["default"]["harness"], "spiron");
    assert_eq!(saved["default"]["profile"], "sylphx/default");
}

fn rebalance_test_profiles(data: &Path, busy_pct: i64) -> Vec<ProviderProfileEntry> {
    [("busy", busy_pct), ("healthy", 80)]
        .into_iter()
        .map(|(profile, pct)| {
            ensure_provider_profile_meta(
                &data.join("profiles/openai").join(profile),
                "openai",
                None,
            );
            ProviderProfileEntry {
                provider: "openai".to_string(),
                profile: profile.to_string(),
                account: None,
                kind: "subscription".to_string(),
                compatible_harnesses: vec!["codex".to_string()],
                assigned_agents: Vec::new(),
                subagent_count: 0,
                usage: Some(format!("5h: {pct}% left · 7d: {pct}% left")),
            }
        })
        .collect()
}

/// A fully created agent on `openai/busy`; rebalancing never moves one
/// whose creation is still in flight.
fn rebalance_test_agent(store: &Store, name: &str, parent: Option<&Agent>) -> Agent {
    let mut new = NewAgent::new(name, "codex");
    new.parent_id = parent.map(|a| a.id.clone());
    let agent = store.create_agent(new).unwrap();
    let agent = store.set_agent_state(&agent.id, "idle").unwrap();
    let cfg = agent_config_dir(store.data_dir(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/busy\n").unwrap();
    agent
}

/// A marker writer holding a row whose tree does not exist (read before a
/// creation made it, or after a failed creation removed it) cannot make an
/// orphan tree; an existing root only gains a private config directory.
#[cfg(unix)]
#[test]
fn markers_never_make_an_agent_tree() {
    use std::os::unix::fs::PermissionsExt;
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("racing", "codex"))
        .unwrap();
    let cfg = agent_config_dir(dir.path(), &agent.id);
    assert!(write_profile_marker(&cfg, ".anyagents-profile", "openai/default\n").is_err());
    assert!(!dir.path().join("agents").join(agent.id.as_str()).exists());
    std::fs::create_dir_all(cfg.parent().unwrap()).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/default\n").unwrap();
    assert_eq!(
        std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/default"));
}

/// The agent root is private before anything is written into it and opens
/// for traversal only through [`open_agent_root`]. A root a racing writer
/// made with umask permissions is closed, root and config alike, and is
/// not this attempt's to remove.
#[cfg(unix)]
#[test]
fn agent_roots_are_private_from_the_first_byte() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    let dir = crate::test_tempdir();
    let root = dir.path().join("agents/fresh");
    let config = root.join("config");
    assert!(create_private_agent_root(&root, &config).unwrap());
    assert_eq!(mode(&root), 0o700);
    create_private_config_dir(&config).unwrap();
    assert_eq!(mode(&config), 0o700);
    open_agent_root(&root).unwrap();
    assert_eq!(mode(&root), 0o755);
    assert_eq!(
        mode(&config),
        0o700,
        "opening the root leaves config private"
    );

    let raced = dir.path().join("agents/raced");
    std::fs::create_dir_all(raced.join("config")).unwrap();
    for path in [&raced, &raced.join("config")] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::write(raced.join("config/.anyagents-profile"), "openai/default\n").unwrap();
    assert!(!create_private_agent_root(&raced, &raced.join("config")).unwrap());
    assert_eq!(mode(&raced), 0o700);
    assert_eq!(mode(&raced.join("config")), 0o700);
    assert!(raced.join("config/.anyagents-profile").is_file());
}

/// A failed creation removes exactly the directories it made: its private
/// agent tree and the levels of an explicit workdir it created, never a
/// directory that existed before it or anything inside one.
#[cfg(unix)]
#[test]
fn failed_creation_removes_exactly_the_directories_it_made() {
    let dir = crate::test_tempdir();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let f = crate::repo::tests::Fixture::new(&data);
    // No capability key: the creation fails after admitting its workdir.
    let store = Store::open(data.join("state.db")).unwrap();
    let existing = dir.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    std::fs::write(existing.join("user-work.txt"), "kept").unwrap();
    let mut request = worktree_request("failing", None);
    request.workdir = Some(existing.join("new/deeper"));
    let err = create_with_consumer(&store, &data, request, &f.checkouts).unwrap_err();
    assert!(err.to_string().contains("capability key"), "{err}");
    assert!(matches!(
        store.agent_by_name("failing"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(std::fs::read_dir(data.join("agents")).unwrap().count(), 0);
    assert!(!existing.join("new").exists(), "made levels are removed");
    assert_eq!(
        std::fs::read_to_string(existing.join("user-work.txt")).unwrap(),
        "kept"
    );

    let mut request = worktree_request("failing", None);
    request.workdir = Some(existing.clone());
    assert!(create_with_consumer(&store, &data, request, &f.checkouts).is_err());
    assert!(existing.is_dir(), "a pre-existing workdir is never removed");
    assert_eq!(std::fs::read_dir(data.join("agents")).unwrap().count(), 0);

    // A made workdir that gained content it did not get from this attempt stays.
    let made = std::cell::RefCell::new(MadeDirs::default());
    let target = dir.path().join("made-then-used");
    create_dir_all_recorded(&target, &made).unwrap();
    std::fs::write(target.join("late.txt"), "kept").unwrap();
    remove_made_dirs(&made.into_inner(), None, &AgentId::generate());
    assert!(target.join("late.txt").is_file());
}

#[cfg(unix)]
#[test]
fn runtime_config_preparation_preserves_seat_permissions() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let dir = crate::test_tempdir();
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    for old_mode in [0o0600, 0o0700, 0o2770] {
        let config = dir.path().join(format!("seat-{old_mode:o}/config"));
        create_private_config_dir(&config).unwrap();
        std::fs::write(config.join("credential"), "retained").unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(old_mode)).unwrap();
        prepare_runtime_config_dir(&config, Some((uid, gid))).unwrap();
        assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
        assert_eq!(
            std::fs::read_to_string(config.join("credential")).unwrap(),
            "retained"
        );
    }
}

#[cfg(unix)]
#[test]
fn runtime_config_preparation_repairs_unseated_modes_and_propagates_failures() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let dir = crate::test_tempdir();
    for (old_mode, expected) in [(0o0600, 0o0700), (0o0660, 0o0770), (0o2770, 0o2770)] {
        let config = dir.path().join(format!("unseated-{old_mode:o}/config"));
        create_private_config_dir(&config).unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(old_mode)).unwrap();
        prepare_runtime_config_dir(&config, None).unwrap();
        assert_eq!(
            std::fs::metadata(&config).unwrap().mode() & 0o7777,
            expected
        );
    }
    let blocked = dir.path().join("file");
    std::fs::write(&blocked, "retained").unwrap();
    assert!(prepare_runtime_config_dir(&blocked, None).is_err());
    assert!(repair_unseated_config_mode(&dir.path().join("missing"), 0o0600).is_err());
    assert_eq!(std::fs::read_to_string(blocked).unwrap(), "retained");
}

#[cfg(unix)]
#[test]
fn new_config_roots_are_private_and_existing_seat_grants_are_preserved() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let dir = crate::test_tempdir();
    let config = dir.path().join("created/config");
    create_private_config_dir(&config).unwrap();
    assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o777, 0o700);
    assert_ne!(
        std::fs::metadata(config.parent().unwrap()).unwrap().mode() & 0o011,
        0,
        "agent root parent must remain traversable when config is created"
    );
    std::fs::write(config.join("credential"), "retained credential").unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o2770)).unwrap();
    create_private_config_dir(&config).unwrap();
    assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
    assert_eq!(
        std::fs::read_to_string(config.join("credential")).unwrap(),
        "retained credential"
    );
    let blocked = dir.path().join("not-a-directory");
    std::fs::write(&blocked, "preserved").unwrap();
    assert!(create_private_config_dir(&blocked).is_err());
    assert_eq!(std::fs::read_to_string(blocked).unwrap(), "preserved");
}

#[cfg(unix)]
#[test]
fn workspaces_dir_keeps_agent_root_traversable_and_avoids_unused_managed_dirs() {
    use std::os::unix::fs::MetadataExt;
    const PROBE: &str = "ANYAGENTS_WORKSPACES_CREATE_PROBE";
    if std::env::var_os(PROBE).is_none() {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        for key in [
            "ANYAGENTS_PRINCIPAL_REGISTRY",
            "ANYAGENTS_PRINCIPALS",
            "ANYAGENTS_AGENTS",
            "ANYAGENTS_AGENT_UID",
            "ANYAGENTS_AGENT_GID",
            "ANYAGENTS_SOCKET_GID",
            "ANYAGENTS_LAUNCHER",
            "ANYAGENTS_SETPRIV",
            "ANYAGENTS_DATA",
            "ANYAGENTS_STATE_DIR",
            "ANYAGENTS_WORKSPACES_DIR",
            "ANYAGENTS_SCRATCH_DIR",
            "ANYAGENTS_GIT_POOL_DIR",
            "ANYAGENTS_PROTECTED_ROOTS",
        ] {
            cmd.env_remove(key);
        }
        let output = cmd
            .env(PROBE, "1")
            .env("ANYAGENTS_TEST_STORAGE_TIERS", "1")
            .args([
                "--exact",
                "create::tests::workspaces_dir_keeps_agent_root_traversable_and_avoids_unused_managed_dirs",
                "--nocapture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "probe failed:
stdout: {}
stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    unsafe {
        libc::umask(0o022);
    }
    let tmp = crate::test_tempdir();
    let data = tmp.path().join("data");
    let workspaces = tmp.path().join("workspaces");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspaces).unwrap();
    unsafe {
        std::env::set_var("ANYAGENTS_WORKSPACES_DIR", &workspaces);
    }
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let managed_agent = create(
        &store,
        &data,
        CreateRequest {
            name: "managed-worker".to_string(),
            role: None,
            harness: "codex".to_string(),
            model: Some("test-model".to_string()),
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
    let expected_managed = workspaces.join(managed_agent.id.as_str());
    assert_eq!(
        managed_agent.workdir.as_deref(),
        Some(expected_managed.to_str().unwrap())
    );
    assert!(expected_managed.is_dir());

    let managed_config = agent_config_dir(&data, &managed_agent.id);
    let managed_root = managed_config.parent().unwrap();
    assert_ne!(
        std::fs::metadata(managed_root).unwrap().mode() & 0o011,
        0,
        "<data>/agents/<id> must be traversable with ANYAGENTS_WORKSPACES_DIR set"
    );
    // Simulate a fresh unshared private config before seat sharing.
    let unshared_root = data.join("agents/unshared-probe");
    let unshared_config = unshared_root.join("config");
    create_private_config_dir(&unshared_config).unwrap();
    assert_eq!(
        std::fs::metadata(&unshared_root).unwrap().mode() & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::metadata(&unshared_config).unwrap().mode() & 0o777,
        0o700
    );
    let (uid, gid) = (crate::sandbox::server_uid(), crate::sandbox::server_gid());
    crate::sandbox::prepare_config_with_seat_uid(&unshared_config, uid, gid).unwrap();
    assert_ne!(std::fs::metadata(&unshared_root).unwrap().mode() & 0o011, 0);
    assert_eq!(
        std::fs::metadata(&unshared_config).unwrap().mode() & 0o7777,
        0o2770
    );

    let explicit_dir = tmp.path().join("explicit-work");
    let explicit_agent = create(
        &store,
        &data,
        CreateRequest {
            name: "explicit-worker".to_string(),
            role: None,
            harness: "codex".to_string(),
            model: Some("test-model".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(explicit_dir.clone()),
            workdir_default: None,
            description: None,
            instructions: None,
            parent_id: None,
        },
    )
    .unwrap();
    assert_eq!(
        explicit_agent.workdir.as_deref(),
        Some(explicit_dir.to_str().unwrap())
    );
    assert!(
        !workspaces.join(explicit_agent.id.as_str()).exists(),
        "explicit workdir must not eagerly create unused dir in ANYAGENTS_WORKSPACES_DIR"
    );
    let explicit_root = agent_config_dir(&data, &explicit_agent.id)
        .parent()
        .unwrap()
        .to_path_buf();
    assert_ne!(std::fs::metadata(&explicit_root).unwrap().mode() & 0o011, 0);

    // If creation fails after creating managed workspace in ANYAGENTS_WORKSPACES_DIR,
    // rollback must remove both <data>/agents/<id> and <workspaces>/<id>.
    unsafe {
        std::env::set_var("ANYAGENTS_PROTECTED_ROOTS", &workspaces);
    }
    let failed = create(
        &store,
        &data,
        CreateRequest {
            name: "rollback-worker".to_string(),
            role: None,
            harness: "codex".to_string(),
            model: Some("test-model".to_string()),
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
    );
    assert!(failed.is_err());
    let workspace_entries: Vec<_> = std::fs::read_dir(&workspaces)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        workspace_entries,
        vec![expected_managed.file_name().unwrap().to_os_string()]
    );
}

#[test]
fn rebalance_provider_profiles_balances_overloaded_roots_and_nested_subagents() {
    for depth in 0..=2 {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let entries = rebalance_test_profiles(data, 50);
        let mut parent = None;
        for level in 0..depth {
            let mut new = NewAgent::new(format!("ancestor-{level}"), "claude-code");
            new.parent_id = parent.as_ref().map(|a: &Agent| a.id.clone());
            parent = Some(store.create_agent(new).unwrap());
        }
        let stay = rebalance_test_agent(&store, "a-stay", parent.as_ref());
        let moved = rebalance_test_agent(&store, "z-move", parent.as_ref());

        rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

        assert_eq!(
            bound_profile_name(&agent_config_dir(data, &stay.id)).as_deref(),
            Some("openai/busy"),
            "depth {depth}"
        );
        assert_eq!(
            bound_profile_name(&agent_config_dir(data, &moved.id)).as_deref(),
            Some("openai/healthy"),
            "depth {depth}"
        );
        let after = store.agent(&moved.id).unwrap();
        assert_eq!(after.parent_id, moved.parent_id);
        assert_eq!(after.harness, moved.harness);
        assert_eq!(after.model, moved.model);
        assert_eq!(after.state, moved.state);
    }
}

/// An agent whose creation is in flight or failed is never moved, even off
/// an exhausted profile: its tree may be gone, and a move would write it.
#[test]
fn rebalance_provider_profiles_never_moves_an_agent_being_created() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let entries = rebalance_test_profiles(data, 0);
    let spawning = store
        .create_agent(NewAgent::new("spawning", "codex"))
        .unwrap();
    let cfg = agent_config_dir(data, &spawning.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "openai/busy\n").unwrap();
    let before = std::fs::read_dir(&cfg).unwrap().count();

    rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

    assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/busy"));
    assert_eq!(std::fs::read_dir(&cfg).unwrap().count(), before);
}

#[test]
fn rebalance_provider_profiles_evacuates_exhausted_roots_and_subagents() {
    for is_child in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let entries = rebalance_test_profiles(data, 0);
        let parent = store
            .create_agent(NewAgent::new("parent", "claude-code"))
            .unwrap();
        let agent = rebalance_test_agent(&store, "worker", is_child.then_some(&parent));

        rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

        assert_eq!(
            bound_profile_name(&agent_config_dir(data, &agent.id)).as_deref(),
            Some("openai/healthy"),
            "is_child={is_child}"
        );
    }
}

#[test]
fn rebalance_provider_profiles_defers_open_turns_but_counts_their_load() {
    for busy_pct in [0, 50] {
        for is_child in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let data = tmp.path();
            let store = Store::open(data.join("state.db")).unwrap();
            store
                .hold_capability_key("0123456789abcdef0123456789abcdef")
                .unwrap();
            let entries = rebalance_test_profiles(data, busy_pct);
            let parent = store
                .create_agent(NewAgent::new("parent", "claude-code"))
                .unwrap();
            let idle = rebalance_test_agent(&store, "a-idle", is_child.then_some(&parent));
            let working = rebalance_test_agent(&store, "z-working", is_child.then_some(&parent));
            for agent in [&idle, &working] {
                store
                    .set_agent_running(&agent.id, &data.join(&agent.name).display().to_string())
                    .unwrap();
            }
            store
                .receive(&working.id, anyagents_core::Incoming::new("active turn"))
                .unwrap();
            let message = store.next_undelivered(&working.id).unwrap().unwrap();
            store.note_delivered(&working.id, message.seq).unwrap();
            assert!(store.has_open_turn(&working.id).unwrap());
            let cfg = agent_config_dir(data, &working.id);
            write_profile_marker(&cfg, ".anyagents-profile-rotated-ms", "123\n").unwrap();

            rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

            assert_eq!(
                bound_profile_name(&cfg).as_deref(),
                Some("openai/busy"),
                "busy_pct={busy_pct}, is_child={is_child}"
            );
            assert_eq!(
                std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms")).unwrap(),
                "123\n"
            );
            assert!(store.has_open_turn(&working.id).unwrap());
            assert_eq!(store.delivered_cursor(&working.id).unwrap(), message.seq);
            // The working seat still contributes load. An idle seat moves
            // even though the alphabetically last seat cannot be moved.
            assert_eq!(
                bound_profile_name(&agent_config_dir(data, &idle.id)).as_deref(),
                Some("openai/healthy")
            );

            if busy_pct == 0 {
                store.note_turn_completed(&working.id).unwrap();
                rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);
                assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/healthy"));
                assert_ne!(
                    std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms")).unwrap(),
                    "123\n"
                );
            }
        }
    }
}

#[test]
fn rebalance_provider_profiles_preserves_archived_and_other_provider_agents() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let entries = rebalance_test_profiles(data, 0);
    let root = rebalance_test_agent(&store, "archived-root", None);
    let child = rebalance_test_agent(&store, "archived-child", Some(&root));
    for agent in [&root, &child] {
        store.set_agent_state(&agent.id, "archived").unwrap();
    }
    let foreign = rebalance_test_agent(&store, "foreign-provider", Some(&root));
    store
        .set_agent_model(&foreign.id, "sylphx/openai/gpt-6.1-sol#xhigh")
        .unwrap();

    rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

    for agent in [&root, &child, &foreign] {
        assert_eq!(
            bound_profile_name(&agent_config_dir(data, &agent.id)).as_deref(),
            Some("openai/busy"),
            "{}",
            agent.name
        );
    }
}

#[test]
fn propagate_profile_files_marks_working_seats_without_holding_their_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let _entries = rebalance_test_profiles(data, 80);
    let idle = rebalance_test_agent(&store, "idle-seat", None);
    let working = rebalance_test_agent(&store, "working-seat", None);
    store
        .receive(&working.id, anyagents_core::Incoming::new("active turn"))
        .unwrap();
    let message = store.next_undelivered(&working.id).unwrap().unwrap();
    store.note_delivered(&working.id, message.seq).unwrap();
    assert!(store.has_open_turn(&working.id).unwrap());

    let idle_cfg = agent_config_dir(data, &idle.id);
    let working_cfg = agent_config_dir(data, &working.id);
    let _ = std::fs::remove_file(idle_cfg.join(".anyagents-profile-rotated-ms"));
    let _ = std::fs::remove_file(working_cfg.join(".anyagents-profile-rotated-ms"));

    propagate_profile_files_to_bound_agents(data, "openai", "busy", &[]).unwrap();

    assert!(
        idle_cfg.join(".anyagents-profile-rotated-ms").exists(),
        "idle seat should receive rotated marker on credential refresh"
    );
    // The supervisor applies a profile change at the next idle point, so
    // the marker never preempts the open turn or holds its mail.
    assert!(
        working_cfg.join(".anyagents-profile-rotated-ms").exists(),
        "working seat should learn of the profile change"
    );
    assert!(
        working_cfg.join(".anyagents-quota-cleared-ms").exists(),
        "working seat should still receive quota-cleared marker"
    );
}

#[test]
fn quota_rotation_counts_subagent_load_and_preserves_exclusions() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let root = rebalance_test_agent(&store, "root", None);
    let rotating = rebalance_test_agent(&store, "rotating", Some(&root));
    let child = rebalance_test_agent(&store, "child", Some(&root));
    let nested = rebalance_test_agent(&store, "nested", Some(&child));
    // Legacy unqualified bindings still use the agent's upstream provider.
    write_profile_marker(
        &agent_config_dir(data, &nested.id),
        ".anyagents-profile",
        "busy\n",
    )
    .unwrap();
    let archived = rebalance_test_agent(&store, "archived", Some(&root));
    store.set_agent_state(&archived.id, "archived").unwrap();
    let foreign = rebalance_test_agent(&store, "foreign", Some(&root));
    write_profile_marker(
        &agent_config_dir(data, &foreign.id),
        ".anyagents-profile",
        "anthropic/busy\n",
    )
    .unwrap();
    let missing = rebalance_test_agent(&store, "missing", Some(&root));
    std::fs::remove_file(agent_config_dir(data, &missing.id).join(".anyagents-profile")).unwrap();

    let counts =
        count_bound_profile_agents(data, &store.list_agents().unwrap(), "openai", &rotating.id);

    assert_eq!(
        counts,
        std::collections::HashMap::from([("busy".to_string(), 3)])
    );
}

#[test]
#[cfg(unix)]
fn seat_skill_wrapper_mounts_overlay_without_shared_library() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("agents/agent-id/config");
    let skill = dir.path().join("agents/agent-id/skills/private/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(&skill, "---\nname: private\n---\nexact overlay").unwrap();
    ensure_seat_skills(&config, dir.path());
    assert_eq!(
        std::fs::read(config.join(".claude/skills/private/SKILL.md")).unwrap(),
        std::fs::read(skill).unwrap()
    );
    assert!(!dir.path().join("skills").exists());
}

#[test]
fn rendered_whole_memory_block_respects_caps_separate_from_fixed_file_policy() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "caps-rendered",
            "claude-code",
        ))
        .unwrap();
    agent.launch_env.push((
        "ANYAGENTS_MEMORY_GUIDANCE".into(),
        "Operator memory policy".into(),
    ));
    let config = agent_config_dir(dir.path(), &agent.id);
    let root = config.parent().unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "繁體長記憶".repeat(3000)).unwrap();
    for harness in ["claude-code", "spiron"] {
        agent.harness = harness.into();
        let pin = anyagents_core::host_session::new_session(root, harness).unwrap();
        write_agent_instructions(&config, &agent, None).unwrap();
        for name in ["AGENTS.md", "CLAUDE.md", "SYSTEM_PROMPT.md"] {
            let doc = std::fs::read_to_string(config.join(name)).unwrap();
            // Find full delimiters at line boundaries, not their mention in
            // the independent fixed operating instructions.
            let start = doc.find("\n<learned_memory>\n").unwrap() + 1;
            let end = doc[start..].find("</learned_memory>\n").unwrap()
                + start
                + "</learned_memory>\n".len();
            let block = &doc[start..end];
            assert_eq!(block, pin.memory.section);
            assert!(block.len() <= 4096, "{name}: {} bytes", block.len());
            assert!(block.lines().count() <= 150);
            assert!(block.contains("Memory truncated"));
            assert!(!block.contains("fixed; not learned memory"));
            assert!(doc[..start].contains("Operator memory policy"));
        }
    }
}

#[test]
fn instruction_mirrors_share_one_pinned_snapshot_across_harnesses() {
    use anyagents_core::host_session;
    // Resume-pointer reads reject symlinked ancestors, including macOS /var.
    let tmp = crate::test_tempdir();
    let data = tmp.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "memory-renderer",
            "claude-code",
        ))
        .unwrap();
    let config = agent_config_dir(data, &agent.id);
    let root = config.parent().unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "PINNED_FIRST_FACT 繁體\n").unwrap();
    let first = host_session::new_session(root, &agent.harness).unwrap();
    write_agent_instructions(&config, &agent, None).unwrap();
    let paths = [
        "AGENTS.md",
        "CLAUDE.md",
        "SYSTEM_PROMPT.md",
        ".claude/CLAUDE.md",
        ".config/opencode/AGENTS.md",
        ".gemini/config/GEMINI.md",
    ];
    for path in paths {
        let text = std::fs::read_to_string(config.join(path)).unwrap();
        assert!(text.contains(&first.memory.section), "{path}");
    }
    std::fs::write(root.join("memory/MEMORY.md"), "EDITED_SECOND_FACT\n").unwrap();
    agent.description = Some("ordinary describe update".into());
    write_agent_instructions(&config, &agent, None).unwrap();
    let rerender = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(rerender.contains("PINNED_FIRST_FACT"));
    assert!(!rerender.contains("EDITED_SECOND_FACT"));
    // External CLI consumption of this mirror is exercised by the real-pipe
    // Spiron launch/wake probe; AnyAgents no longer assembles model prompts.
    agent.harness = "spiron".into();
    write_agent_instructions(&config, &agent, None).unwrap();
    let switched = host_session::active(root).unwrap().unwrap();
    assert_eq!(switched.harness, "spiron");
    assert_ne!(switched.generation, first.generation);
    for path in paths {
        let text = std::fs::read_to_string(config.join(path)).unwrap();
        assert!(text.contains(&switched.memory.section));
        assert!(!text.contains("PINNED_FIRST_FACT"));
        assert!(text.contains("EDITED_SECOND_FACT"));
    }
    std::fs::remove_file(root.join("memory/MEMORY.md")).unwrap();
    write_agent_instructions(&config, &agent, None).unwrap();
    assert_eq!(host_session::active(root).unwrap().unwrap(), switched);
}
#[test]
fn parse_remaining_pct_from_usage_handles_agy_5h_and_7d_windows() {
    let usage = "gemini: 84% left (5h: 100%, 7d: 84%) · opus: 0% left (5h: 0%, 7d: 0%)";
    assert_eq!(
        parse_remaining_pct_from_usage(usage, Some("gemini-4-argon-high")),
        Some(84)
    );
    assert_eq!(
        parse_remaining_pct_from_usage(usage, Some("claude-opus-5-5")),
        Some(0)
    );
}

#[test]
fn clear_stale_agy_quota_error_steps_resets_step_type_17_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let conv_dir = tmp
        .path()
        .join(".gemini")
        .join("antigravity-cli")
        .join("conversations");
    std::fs::create_dir_all(&conv_dir).unwrap();
    let db_path = conv_dir.join("test-conv.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "CREATE TABLE steps (idx integer PRIMARY KEY, step_type integer NOT NULL DEFAULT 0, status integer NOT NULL DEFAULT 0, error_details blob, step_payload blob)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (10, 17, 5, X'0102', X'08112005')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (11, 15, 5, NULL, X'080f2005')",
        [],
    )
    .unwrap();
    drop(conn);

    clear_stale_agy_quota_error_steps(tmp.path());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let (st, status, ed, sp): (i64, i64, Option<Vec<u8>>, Vec<u8>) = conn
        .query_row(
            "SELECT step_type, status, error_details, step_payload FROM steps WHERE idx = 10",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(st, 0);
    assert_eq!(status, 5);
    assert!(ed.is_none());
    assert_eq!(sp, b" \x05*\x03\xd2\x01\x00");
}

#[test]
fn set_agent_harness_preserves_identity_history_and_refuses_incompatible() {
    let tmp = crate::test_tempdir();
    let data = tmp.path();
    let owner_workdir = data.join("owner-workdir");
    std::fs::create_dir(&owner_workdir).unwrap();
    std::fs::create_dir_all(data.join("profiles/sylphx/default")).unwrap();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent {
            name: "sample-parent".to_string(),
            role: None,
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
            workdir: Some(owner_workdir.display().to_string()),
            description: Some("Root coordinator".to_string()),
            instructions: Some("Root coordinator".to_string()),
            parent_id: None,
        })
        .unwrap();
    let owner = store
        .create_agent(anyagents_core::NewAgent {
            name: "owner-anymd".to_string(),
            role: None,
            harness: "opencode".to_string(),
            model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
            workdir: Some(owner_workdir.display().to_string()),
            description: Some("Product Owner for AnyMD. Paused scope.".to_string()),
            instructions: Some("Product Owner for AnyMD. Paused scope.".to_string()),
            parent_id: Some(parent.id.clone()),
        })
        .unwrap();
    let cfg = agent_config_dir(data, &owner.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_profile_marker(&cfg, ".anyagents-profile", "sylphx/default\n").unwrap();
    let root = cfg.parent().unwrap();
    let initial_session = anyagents_core::host_session::ensure_active(root, "opencode").unwrap();
    anyagents_core::host_session::bind_vendor(root, "opencode", "ses_original").unwrap();
    let mut inc = anyagents_core::Incoming::new("Initial owner task");
    inc.sender = Some("agent:sample-parent".to_string());
    let _ = store.receive(&owner.id, inc).unwrap();
    let _ = store.record_outbound(
        &owner.id,
        "agent:sample-parent",
        None,
        "Completed initial AnyMD audit",
    );
    let _ = store.set_agent_state(&owner.id, "archived").unwrap();

    let bad_harness = set_agent_harness(
        &store,
        data,
        "operator",
        "owner-anymd",
        "unknown-harness",
        None,
        None,
    );
    assert!(bad_harness.is_err());

    let incompat = set_agent_harness(&store, data, "operator", "owner-anymd", "agy", None, None);
    assert!(incompat.is_err());
    assert_eq!(store.agent(&owner.id).unwrap().harness, "opencode");

    let receipt =
        set_agent_harness(&store, data, "operator", "owner-anymd", "codex", None, None).unwrap();
    assert_eq!(receipt["id"], owner.id.as_str());
    assert_eq!(receipt["name"], "owner-anymd");
    assert_eq!(receipt["previous_harness"], "opencode");
    assert_eq!(receipt["harness"], "codex");
    assert_eq!(receipt["model"], "sylphx/openai/gpt-6.1-sol#xhigh");
    assert_eq!(receipt["profile"], "sylphx/default");
    assert_eq!(receipt["state"], "archived");
    assert_eq!(receipt["cutover_status"], "ready_on_next_wake");
    let switched_session = anyagents_core::host_session::active(root).unwrap().unwrap();
    assert_eq!(switched_session.harness, "codex");
    assert_ne!(switched_session.generation, initial_session.generation);

    let after = store.agent(&owner.id).unwrap();
    assert_eq!(after.id, owner.id);
    assert_eq!(after.harness, "codex");
    assert_eq!(
        after.model.as_deref(),
        Some("sylphx/openai/gpt-6.1-sol#xhigh")
    );
    assert_eq!(after.state, "archived");
    assert_eq!(
        after.instructions.as_deref(),
        Some("Product Owner for AnyMD. Paused scope.")
    );
    let toml_text = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
    assert!(toml_text.contains("model = \"gpt-6.1-sol\""));
    assert!(toml_text.contains("model_reasoning_effort = \"xhigh\""));
    assert!(toml_text.contains("model_provider = \"sylphx\""));
    assert!(toml_text.contains("developer_instructions = "));
    assert!(toml_text.contains("Completed initial AnyMD audit"));
    assert!(!cfg.join(".anyagents-harness-continuity.md").exists());

    // Simulate an older opencode session vs a newer codex session + new state.db turn,
    // then switch back to opencode and verify declarative round-trip continuity.
    std::fs::write(cfg.join("opencode-session-id"), "ses_old_opencode\n").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(cfg.join("codex-session-id"), "thread_new_codex\n").unwrap();
    let _ = store.record_outbound(
        &owner.id,
        "agent:sample-parent",
        None,
        "Completed second turn on Codex",
    );
    let roundtrip = configure_agent_runtime(
        &store,
        data,
        "operator",
        "owner-anymd",
        Some("opencode"),
        None,
        None,
    )
    .unwrap();
    assert_eq!(roundtrip["harness"], "opencode");
    let roundtrip_session = anyagents_core::host_session::active(root).unwrap().unwrap();
    assert_eq!(roundtrip_session.harness, "opencode");
    assert_ne!(roundtrip_session.generation, switched_session.generation);
    // The outgoing generation's pin and alias remain resumable.
    assert_eq!(
        anyagents_core::host_session::resume_vendor(root, "opencode", "ses_original").unwrap(),
        initial_session
    );
    assert!(!cfg.join("opencode-session-id").exists());
    let agents_md = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
    assert!(agents_md.contains("Completed second turn on Codex"));

    // Verify model-only update preserves bound profile without requiring --profile.
    let model_only = configure_agent_runtime(
        &store,
        data,
        "operator",
        "owner-anymd",
        Some("codex"),
        Some("sylphx/openai/gpt-5.4-mini#medium"),
        None,
    )
    .unwrap();
    assert_eq!(model_only["harness"], "codex");
    assert_eq!(model_only["model"], "sylphx/openai/gpt-5.4-mini#medium");
    assert_eq!(model_only["profile"], "sylphx/default");
    let toml_after = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
    assert!(toml_after.contains("model = \"gpt-5.4-mini\""));
    assert!(toml_after.contains("model_reasoning_effort = \"medium\""));

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        std::fs::set_permissions(&cfg, std::fs::Permissions::from_mode(0o0700)).unwrap();
        let orig_gid = std::fs::metadata(&cfg).unwrap().gid();
        let updated_desc = store
            .set_agent_instructions(&owner.id, Some("Updated description via describe sync."))
            .unwrap();
        let work_dir = agent_work_dir(data, &owner.id);
        sync_agent_description_files(&store, &updated_desc, &cfg).unwrap();
        let meta_after = std::fs::metadata(&cfg).unwrap();
        assert_eq!(meta_after.permissions().mode() & 0o7777, 0o0700);
        assert_eq!(meta_after.gid(), orig_gid);

        // Even if config dir had its execute bit stripped (0600), prompt sync heals it back to 0700.
        std::fs::set_permissions(&cfg, std::fs::Permissions::from_mode(0o0600)).unwrap();
        sync_agent_description_files(&store, &updated_desc, &cfg).unwrap();
        let healed_mode = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
        assert_eq!(healed_mode & 0o0700, 0o0700);

        reconcile_agent_runtime_config(&store, data, &updated_desc, &cfg, &work_dir).unwrap();
        let healed_reconcile = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
        assert_eq!(healed_reconcile & 0o0700, 0o0700);
    }
}

#[test]
fn spawn_workspace_default_is_private_and_explicit_modes_are_preserved() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path();
    let db = data.join("state.db");
    let store = Store::open(&db).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let parent_workdir = data.join("parent-worktree");
    std::fs::create_dir_all(&parent_workdir).unwrap();
    std::fs::write(parent_workdir.join("parent-only.txt"), "preserve me").unwrap();
    let parent = store
        .create_agent(NewAgent::new("parent", "codex"))
        .unwrap();
    let parent = store
        .set_agent_running(&parent.id, parent_workdir.to_str().unwrap())
        .unwrap();
    let custom_workdir = data.join("explicit-child-path");
    let mut stored_workdirs = vec![(parent.id.clone(), parent.workdir.clone())];
    let mut private_workdirs = Vec::new();

    for (name, workdir) in [
        ("default-first", None),
        ("default-second", None),
        ("private-child", Some(PathBuf::from("private"))),
        ("inherited-child", Some(PathBuf::from("inherit"))),
        ("explicit-child", Some(custom_workdir.clone())),
    ] {
        let child = create(
            &store,
            data,
            CreateRequest {
                name: name.to_string(),
                role: None,
                harness: "codex".to_string(),
                model: Some("test-model".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir,
                // A root override must never become a child default.
                workdir_default: Some(parent_workdir.clone()),
                description: None,
                instructions: None,
                parent_id: Some(parent.id.clone()),
            },
        )
        .unwrap();
        let actual = PathBuf::from(child.workdir.as_deref().unwrap());
        let expected = match name {
            "inherited-child" => parent_workdir.clone(),
            "explicit-child" => custom_workdir.clone(),
            _ => agent_work_dir(data, &child.id),
        };
        assert_eq!(actual, expected, "workspace selection for {name}");
        assert!(actual.is_dir());
        if name.starts_with("default-") || name == "private-child" {
            assert_ne!(actual, parent_workdir);
            assert!(!actual.join("parent-only.txt").exists());
            assert_eq!(std::fs::read_dir(&actual).unwrap().count(), 0);
            assert!(!private_workdirs.contains(&actual));
            private_workdirs.push(actual);
        }
        stored_workdirs.push((child.id.clone(), child.workdir));
        for (id, expected) in &stored_workdirs {
            assert_eq!(&store.agent(id).unwrap().workdir, expected);
        }
    }
    assert_eq!(
        std::fs::read_to_string(parent_workdir.join("parent-only.txt")).unwrap(),
        "preserve me"
    );
    drop(store);
    let reopened = Store::open(&db).unwrap();
    for (id, expected) in stored_workdirs {
        assert_eq!(reopened.agent(&id).unwrap().workdir, expected);
    }
}

#[test]
fn role_label_does_not_select_execution_or_lifecycle_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut new = NewAgent::new("worker", "codex");
    new.instructions = Some("Follow the project's explicit task instructions.".into());
    let mut agent = store.create_agent(new).unwrap();
    let config = dir.path().join("prompt");
    std::fs::create_dir_all(&config).unwrap();
    for parent in [None, Some("parent")] {
        agent.role = None;
        write_agent_instructions(&config, &agent, parent).unwrap();
        let plain = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        agent.role = Some("Researcher".into());
        write_agent_instructions(&config, &agent, parent).unwrap();
        let labelled = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        assert_eq!(
            labelled.replace("- **Role Label**: `Researcher`\n", ""),
            plain
        );
        assert!(plain.contains("Follow the project's explicit task instructions."));
        assert!(plain.contains("Any agent may call"));
        for unwanted in [
            "permanent domain owner",
            "do not** archive yourself",
            "Standing Domain",
            "Hub · Mind · Runner",
        ] {
            assert!(
                !labelled.contains(unwanted),
                "unexpected lifecycle instruction: {unwanted}"
            );
        }
    }
}

#[test]
fn spawn_refuses_when_config_write_is_suppressed_or_invalid() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let empty_cfg = data.join("empty-codex-cfg");
    std::fs::create_dir_all(&empty_cfg).unwrap();
    let err =
        verify_harness_runtime_config(&empty_cfg, "codex", Some("sylphx/openai/gpt-5.4#xhigh"))
            .unwrap_err()
            .to_string();
    assert!(
        err.contains("codex runtime config missing") && err.contains("config.toml"),
        "unexpected err: {err}"
    );

    // Write a partial config missing [mcp_servers.anyagents]
    std::fs::write(empty_cfg.join("config.toml"), "model = \"gpt-5.4\"\n").unwrap();
    let err2 =
        verify_harness_runtime_config(&empty_cfg, "codex", Some("sylphx/openai/gpt-5.4#xhigh"))
            .unwrap_err()
            .to_string();
    assert!(
        err2.contains("[mcp_servers.anyagents]"),
        "unexpected err: {err2}"
    );

    std::fs::write(data.join(".anyagents-suppress-config-write"), "1").unwrap();
    let res = create(
        &store,
        data,
        CreateRequest {
            name: "broken-spawn-child".to_string(),
            role: Some("Worker".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(data.to_path_buf()),
            description: None,
            instructions: Some("Should never start".to_string()),
            parent_id: None,
        },
    );
    let _ = std::fs::remove_file(data.join(".anyagents-suppress-config-write"));
    assert!(
        res.is_err(),
        "expected spawn to fail when config write suppressed"
    );
    assert!(
        store.agent_by_name("broken-spawn-child").is_err(),
        "refused spawn must not leave a running agent row behind"
    );
}

#[test]
fn failing_turn_shows_errored_and_notifies_parent_once() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let parent = create(
        &store,
        data,
        CreateRequest {
            name: "parent-helper".to_string(),
            role: Some("Owner".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(data.to_path_buf()),
            description: None,
            instructions: Some("Parent".to_string()),
            parent_id: None,
        },
    )
    .unwrap();

    let child = create(
        &store,
        data,
        CreateRequest {
            name: "helper-goal-settlement".to_string(),
            role: Some("Worker".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(data.to_path_buf()),
            description: None,
            instructions: Some("Child".to_string()),
            parent_id: Some(parent.id.clone()),
        },
    )
    .unwrap();

    let child_cfg = anyagents_core::agent_config_dir(data, &child.id);
    // The Codex adapter persists a 401 turn fault as this record.
    let persist_fault = |timestamp_ms: i64| {
        std::fs::write(
            child_cfg.join(".anyagents-last-error.json"),
            serde_json::json!({
                "error": "workspace routing discovery unauthorized (401)",
                "quota_reset_at_ms": null,
                "timestamp_ms": timestamp_ms,
            })
            .to_string(),
        )
        .unwrap();
    };
    std::fs::write(
        child_cfg.join(".anyagents-profile-rotated-ms"),
        "1700000000000\n",
    )
    .unwrap();
    persist_fault(1_700_000_000_000);

    let health = anyagents_supervisor::HealthBoard::default();
    health.publish(
        &child.id,
        anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::InTurn,
            delivery: Some(crate::vendor::declared_delivery(&child.harness)),
            quota_reset_at_ms: None,
            restart_at_ms: None,
            pid: None,
            live_harness: Some(child.harness.clone()),
            live_model: child.model.clone(),
            process_started_at_ms: Some(anyagents_core::now_ms()),
            last_error: None,
            retry_at_ms: None,
        },
    );
    let mut views;

    // A fault recorded at the rotation boundary is stale. Do not let
    // wall-clock precision decide freshness.
    views = vec![crate::server::agent_view(&store, &health, &child).unwrap()];
    assert_eq!(views[0].turn_state, "working");
    assert!(views[0].last_error.is_none());
    assert!(store.next_undelivered(&parent.id).unwrap().is_none());

    persist_fault(1_700_000_001_000);
    views = vec![crate::server::agent_view(&store, &health, &child).unwrap()];
    assert_eq!(views[0].turn_state, "errored");
    assert_eq!(
        views[0].last_error.as_deref(),
        Some("workspace routing discovery unauthorized (401)")
    );
    assert_eq!(views[0].block_reason.as_deref(), Some("sign_in_required"));

    let first_msg = store
        .next_undelivered(&parent.id)
        .unwrap()
        .expect("parent notified once");
    assert!(first_msg.text.contains("[Sub-Agent Error]"));
    assert!(
        first_msg
            .text
            .contains("workspace routing discovery unauthorized (401)")
    );
    store.note_delivered(&parent.id, first_msg.seq).unwrap();

    // Second check must not re-notify parent for the same error.
    let _ = crate::server::agent_view(&store, &health, &child).unwrap();
    assert!(
        store.next_undelivered(&parent.id).unwrap().is_none(),
        "parent should only be notified once per error"
    );
}

fn worktree_request(name: &str, repo: Option<String>) -> CreateRequest {
    CreateRequest {
        name: name.into(),
        role: None,
        description: None,
        harness: "codex".into(),
        model: None,
        profile: None,
        repo,
        branch: None,
        base: None,
        workdir: None,
        workdir_default: None,
        instructions: None,
        parent_id: None,
    }
}

#[test]
fn cloud_workers_use_launcher_without_checkout_allocation() {
    let tmp = crate::test_tempdir();
    let fixture = crate::repo::tests::Fixture::new(tmp.path());
    let store = Store::open(tmp.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    for (name, harness, wd_mode) in [
        ("spiron-wt", "spiron-cloud", Some(PathBuf::from("worktree"))),
        ("spiron-omitted", "spiron-cloud", None),
        (
            "spiron-remote",
            "spiron-cloud",
            Some(PathBuf::from("remote")),
        ),
        ("claude-wt", "claude-cloud", Some(PathBuf::from("worktree"))),
        ("openai-wt", "openai-cloud", Some(PathBuf::from("worktree"))),
        ("cursor-wt", "cursor-cloud", Some(PathBuf::from("worktree"))),
    ] {
        let mut request = worktree_request(name, Some("ExampleOrg/demo".into()));
        request.harness = harness.into();
        request.workdir = wd_mode;
        request.branch = Some("feature".into());
        request.base = Some("main".into());
        let agent = create_with_consumer(&store, tmp.path(), request, &fixture.checkouts).unwrap();
        let workdir = Path::new(agent.workdir.as_deref().unwrap());
        assert!(workdir.is_dir());
        assert_eq!(workdir.file_name().unwrap(), "launcher");
        assert!(!workdir.join(".git").exists());
        assert!(!agent_work_dir(tmp.path(), &agent.id).exists());
        assert!(
            !fixture.log.exists(),
            "remote placement reached checkout provider"
        );
        let marker_path = tmp
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config/.anyagents-managed-worktree.json");
        let marker: serde_json::Value =
            serde_json::from_slice(&std::fs::read(marker_path).unwrap()).unwrap();
        assert_eq!(marker["repo"], "ExampleOrg/demo");
        assert_eq!(marker["branch"], "feature");
        assert_eq!(marker["base"], "main");
        assert_eq!(marker["cloud_sandbox"], true);
    }

    let explicit_local = tmp.path().join("explicit-local-spiron-cloud");
    let mut local_req = worktree_request("spiron-explicit-local", None);
    local_req.harness = "spiron-cloud".into();
    local_req.workdir = Some(explicit_local.clone());
    let local_agent =
        create_with_consumer(&store, tmp.path(), local_req, &fixture.checkouts).unwrap();
    assert_eq!(
        Path::new(local_agent.workdir.as_deref().unwrap()),
        std::path::absolute(&explicit_local).unwrap()
    );
    assert!(!agent_work_dir(tmp.path(), &local_agent.id).exists());
}

#[test]
fn invalid_explicit_workdir_is_refused_before_checkout_allocation() {
    let tmp = crate::test_tempdir();
    let fixture = crate::repo::tests::Fixture::new(tmp.path());
    let store = Store::open(tmp.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    for (index, path) in ["", " ", "/", "/tmp"].iter().enumerate() {
        let name = format!("invalid-workdir-{index}");
        let mut request = worktree_request(&name, Some("ExampleOrg/demo".into()));
        request.workdir = Some(PathBuf::from(*path));
        let result = create_with_consumer(&store, tmp.path(), request, &fixture.checkouts);
        assert!(
            matches!(result, Err(Error::WorkdirPermission { .. })),
            "{result:?}"
        );
        assert!(store.agent_by_name(&name).is_err());
        assert!(
            !fixture.log.exists(),
            "invalid workdir reached the checkout provider"
        );
    }
}

#[test]
fn workdir_is_durable_while_checkout_provisioning_is_blocked() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fixture = crate::repo::tests::Fixture::new(&data);
    let entered = data.join("entered");
    let release = data.join("release");
    let provider = data.join("provider");
    let script = std::fs::read_to_string(&provider).unwrap();
    let script = script.replacen("#!/bin/sh\n", &format!(
        "#!/bin/sh\nif [ \"$1\" = worktree ]; then touch {entered:?}; while [ ! -e {release:?} ]; do sleep 0.01; done; fi\n"
    ), 1);
    crate::repo::tests::write_executable(&provider, &script);
    std::fs::write(&fixture.fail, "worktree").unwrap();
    let db = data.join("state.db");
    let store = Store::open(&db).unwrap();
    let thread_data = data.clone();
    let handle = std::thread::spawn(move || {
        let store = Store::open(thread_data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        create_with_consumer(
            &store,
            &thread_data,
            worktree_request("blocked", Some("ExampleOrg/demo".into())),
            &fixture.checkouts,
        )
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !entered.exists() && std::time::Instant::now() < deadline && !handle.is_finished() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let observed = Store::open(&db).and_then(|store| store.agent_by_name("blocked"));
    std::fs::write(&release, "release").unwrap();
    let result = handle.join().unwrap();
    assert!(
        entered.exists(),
        "checkout did not reach the blocking provider: {result:?}"
    );
    let observed = observed.unwrap();
    assert_ne!(observed.state, "running");
    assert_eq!(
        observed.workdir.as_deref(),
        Some(agent_work_dir(&data, &observed.id).to_str().unwrap())
    );
    assert!(result.is_err());
    assert!(matches!(
        store.agent_by_name("blocked"),
        Err(Error::AgentNotFound(_))
    ));
}

#[test]
fn explicit_workdir_is_honoured_without_default_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let f = crate::repo::tests::Fixture::new(&data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let explicit = dir.path().join("explicit");
    std::fs::create_dir(&explicit).unwrap();
    let mut request = worktree_request("explicit", None);
    request.workdir = Some(explicit.clone());
    let agent = create_with_consumer(&store, &data, request, &f.checkouts).unwrap();
    assert_eq!(agent.workdir.as_deref(), explicit.to_str());
    assert!(!f.log.exists());
}

#[test]
fn explicit_unwritable_workdir_is_a_typed_refusal_without_repair() {
    if crate::sandbox::server_uid() == 0 {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let f = crate::repo::tests::Fixture::new(&data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let explicit = dir.path().join("explicit");
    std::fs::create_dir(&explicit).unwrap();
    let blocked = explicit.join("blocked");
    std::fs::write(&blocked, "preserve").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o444)).unwrap();
    let mut request = worktree_request("blocked", None);
    request.workdir = Some(explicit.clone());
    let err = create_with_consumer(&store, &data, request, &f.checkouts).unwrap_err();
    assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
    assert!(err.to_string().contains(blocked.to_str().unwrap()), "{err}");
    assert!(store.agent_by_name("blocked").is_err());
    assert_eq!(std::fs::read_to_string(&blocked).unwrap(), "preserve");
    assert_eq!(
        std::fs::metadata(&blocked).unwrap().permissions().mode() & 0o777,
        0o444
    );
}

#[test]
fn missing_inherited_workdir_is_refused_without_default_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let f = crate::repo::tests::Fixture::new(dir.path());
    let mut request = worktree_request("missing-inherit", None);
    request.workdir = Some("inherit".into());
    let err = create_with_consumer(&store, dir.path(), request, &f.checkouts).unwrap_err();
    assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
    assert!(
        err.to_string().contains("parent has no accessible workdir"),
        "{err}"
    );
    assert!(store.agent_by_name("missing-inherit").is_err());
    assert!(!f.log.exists());
}

#[test]
fn consumer_refusal_rolls_back_spawn_and_private_never_calls_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let fixture = crate::repo::tests::Fixture::new(data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    std::fs::write(&fixture.fail, "worktree").unwrap();
    let err = create_with_consumer(
        &store,
        data,
        worktree_request("refused", Some("ExampleOrg/demo".into())),
        &fixture.checkouts,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::WorktreeProvisioning { ref message, .. }
        if message.contains("injected policy refusal")),
        "{err}"
    );
    assert!(matches!(
        store.agent_by_name("refused"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(std::fs::read_dir(data.join("agents")).unwrap().count(), 0);
    let before = std::fs::read_to_string(&fixture.log).unwrap();
    let mut request = worktree_request("private", None);
    request.workdir = Some("private".into());
    let private = create_with_consumer(&store, data, request, &fixture.checkouts).unwrap();
    let wd = PathBuf::from(private.workdir.unwrap());
    assert!(wd.is_dir());
    assert!(!wd.join(".git").exists());
    assert_eq!(std::fs::read_dir(wd).unwrap().count(), 0);
    assert_eq!(std::fs::read_to_string(&fixture.log).unwrap(), before);
}

/// A refusal after the name was reserved rolls back completely: no row,
/// no directory, no registration left in the provider's store, and the
/// same name (and so the same default branch) is retryable.
#[test]
fn refused_provisioning_rolls_back_and_the_same_name_is_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let fixture = crate::repo::tests::Fixture::new(data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    // The provider leaves a merge in progress: its checkout cannot be adopted.
    std::fs::write(&fixture.fail, "merge").unwrap();
    let err = create_with_consumer(
        &store,
        data,
        worktree_request("retryable", Some("ExampleOrg/demo".into())),
        &fixture.checkouts,
    )
    .unwrap_err();
    assert!(matches!(err, Error::WorktreeProvisioning { .. }), "{err}");
    assert!(matches!(
        store.agent_by_name("retryable"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(std::fs::read_dir(data.join("agents")).unwrap().count(), 0);
    let registered = crate::repo::tests::git(&fixture.source, &["worktree", "list"]);
    assert_eq!(registered.lines().count(), 1, "{registered}");
    std::fs::remove_file(&fixture.fail).unwrap();
    let agent = create_with_consumer(
        &store,
        data,
        worktree_request("retryable", Some("ExampleOrg/demo".into())),
        &fixture.checkouts,
    )
    .unwrap();
    assert_eq!(agent.name, "retryable");
    assert_eq!(agent.state, "running");
    fixture.assert_checkout(Path::new(agent.workdir.as_deref().unwrap()));
}

#[test]
fn existing_git_target_is_not_adopted_or_modified() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let fixture = crate::repo::tests::Fixture::new(data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let before = crate::repo::tests::git(&fixture.source, &["rev-parse", "HEAD"]);
    let mut request = worktree_request("existing", Some("ExampleOrg/demo".into()));
    request.workdir = Some(fixture.source.clone());
    let err = create_with_consumer(&store, data, request, &fixture.checkouts).unwrap_err();
    assert!(
        matches!(err, Error::Message(ref message) if message.contains("already exists and is not empty")),
        "{err}"
    );
    assert_eq!(
        crate::repo::tests::git(&fixture.source, &["rev-parse", "HEAD"]),
        before
    );
    assert!(fixture.source.join("excluded/large").is_file());
    assert!(!fixture.log.exists());
    assert!(matches!(
        store.agent_by_name("existing"),
        Err(Error::AgentNotFound(_))
    ));
}

#[test]
fn local_repo_spawn_uses_consumer_and_refuses_unpushed_head() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let fixture = crate::repo::tests::Fixture::new(data);
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let repo = fixture.source.display().to_string();
    let agent = create_with_consumer(
        &store,
        data,
        worktree_request("local", Some(repo.clone())),
        &fixture.checkouts,
    )
    .unwrap();
    fixture.assert_checkout(Path::new(agent.workdir.as_deref().unwrap()));
    let inherited_workdir = Path::new(agent.workdir.as_deref().unwrap());
    crate::repo::tests::git(
        inherited_workdir,
        &["commit", "--allow-empty", "-m", "Unpublished parent"],
    );
    let mut inherit = worktree_request("unpublished-parent", Some("inherit".into()));
    inherit.parent_id = Some(agent.id.clone());
    let err = create_with_consumer(&store, data, inherit, &fixture.checkouts).unwrap_err();
    assert!(
        matches!(err, Error::WorktreeSourceUnpublished { ref message, .. }
        if message.contains("push HEAD first")),
        "{err}"
    );
    assert!(matches!(
        store.agent_by_name("unpublished-parent"),
        Err(Error::AgentNotFound(_))
    ));
    crate::repo::tests::git(
        &fixture.source,
        &["commit", "--allow-empty", "-m", "Unpublished"],
    );
    let before = std::fs::read_to_string(&fixture.log).unwrap();
    let err = create_with_consumer(
        &store,
        data,
        worktree_request("unpublished", Some(repo)),
        &fixture.checkouts,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::WorktreeSourceUnpublished { ref message, .. }
        if message.contains("push HEAD first")),
        "{err}"
    );
    assert!(matches!(
        store.agent_by_name("unpublished"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(std::fs::read_to_string(&fixture.log).unwrap(), before);
}

#[test]
fn shared_agents_md_and_managed_repo_worktree_lifecycle() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    // 1. Seed shared company AGENTS.md at <data>/AGENTS.md
    std::fs::write(
        data.join("AGENTS.md"),
        "# ExampleOrg Shared Constitution\nAlways reason from first principles.\n",
    )
    .unwrap();

    let fixture = crate::repo::tests::Fixture::new(data);
    let create = |store: &Store, data: &Path, request: CreateRequest| {
        create_with_consumer(store, data, request, &fixture.checkouts)
    };

    // 3. Create L0 Root Agent
    let root_wd = data.join("root-workspace");
    std::fs::create_dir_all(&root_wd).unwrap();
    let root = create(
        &store,
        data,
        CreateRequest {
            name: "root-argon".to_string(),
            role: Some("Owner".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(root_wd.clone()),
            workdir_default: Some(root_wd.clone()),
            description: None,
            instructions: Some("Root owner role description.".to_string()),
            parent_id: None,
        },
    )
    .unwrap();

    // 4. Create L1 Sub-Agent with declarative `repo: "ExampleOrg/demo"` (provisions isolated git worktree)
    let l1 = create(
        &store,
        data,
        CreateRequest {
            name: "l1-feature-worker".to_string(),
            role: Some("Feature Worker".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: Some("ExampleOrg/demo".to_string()),
            branch: Some("feat/l1-work".to_string()),
            base: Some("main".to_string()),
            workdir: None,
            workdir_default: Some(root_wd.clone()),
            description: None,
            instructions: Some("L1 worker role description.".to_string()),
            parent_id: Some(root.id.clone()),
        },
    )
    .unwrap();

    let l1_wt = PathBuf::from(l1.workdir.as_deref().unwrap());
    assert!(l1_wt.join(".git").exists(), "L1 should have a git worktree");
    assert!(l1_wt.join("README.md").is_file());
    fixture.assert_checkout(&l1_wt);

    // 5. Create L2 Nested Sub-Agent with `repo: "inherit"` (branches from L1's git repo)
    let l2 = create(
        &store,
        data,
        CreateRequest {
            name: "l2-nested-worker".to_string(),
            role: Some("Nested Worker".to_string()),
            harness: "codex".to_string(),
            model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
            profile: None,
            repo: Some("inherit".to_string()),
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(root_wd.clone()),
            description: None,
            instructions: Some("L2 nested worker role description.".to_string()),
            parent_id: Some(l1.id.clone()),
        },
    )
    .unwrap();
    let l2_wt = PathBuf::from(l2.workdir.as_deref().unwrap());
    assert_ne!(l2_wt, l1_wt);
    assert!(
        l2_wt.join(".git").exists(),
        "L2 should have its own isolated git worktree"
    );
    assert!(l2_wt.join("README.md").is_file());
    fixture.assert_checkout(&l2_wt);
    assert_eq!(
        crate::repo::tests::git(&l1_wt, &["rev-parse", "HEAD"]),
        crate::repo::tests::git(&l2_wt, &["rev-parse", "HEAD"]),
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.log)
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("worktree "))
            .count(),
        2
    );

    // 6. Spiron identity stays compact at every depth, while other
    // harness entrypoints retain the shared company instructions.
    for (ag, expected_desc) in [
        (&root, "Root owner role description."),
        (&l1, "L1 worker role description."),
        (&l2, "L2 nested worker role description."),
    ] {
        let cfg = anyagents_core::agent_config_dir(data, &ag.id);
        let prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
        let native_prompt = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
        assert!(native_prompt.starts_with(&prompt));
        assert_eq!(
            native_prompt
                .matches("# ExampleOrg Shared Constitution")
                .count(),
            1
        );
        assert!(prompt.len() < 3_500, "{}: {} bytes", ag.name, prompt.len());
        assert!(
            prompt.contains(expected_desc),
            "missing role description for {}",
            ag.name
        );
        assert!(
            !prompt.contains("## 4. Shared Instructions (AGENTS.md)")
                && !prompt.contains("# ExampleOrg Shared Constitution"),
            "shared AGENTS.md must be loaded by the harness for {}",
            ag.name
        );
        if let Some(parent_id) = ag.parent_id.as_ref() {
            let parent = store.agent(parent_id).unwrap();
            assert!(prompt.contains(&format!("Direct parent: `agent:{}`", parent.name)));
        }
        let toml = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
        assert!(toml.contains("project_doc_max_bytes = 131072"));
    }

    // 7. Safe Worktree GC:
    //    - L2's worktree is clean and unmodified -> reaping L2 removes `l2_wt` cleanly.
    crate::server::reap_subagent_ephemeral_dirs(data, &l2.id);
    assert!(
        !l2_wt.exists(),
        "clean unmodified worktree should be reaped without recreating empty work/ dir"
    );

    //    - Dirty L1's worktree with an uncommitted file -> reaping L1 MUST preserve `l1_wt`!
    std::fs::write(l1_wt.join("uncommitted.txt"), "work in progress\n").unwrap();
    crate::server::reap_subagent_ephemeral_dirs(data, &l1.id);
    assert!(
        l1_wt.join("uncommitted.txt").is_file(),
        "dirty worktree must NEVER be deleted on archive"
    );
}

#[test]
fn anthropic_usage_formats_weekly_only_when_five_hour_is_null() {
    let payload = serde_json::json!({
        "five_hour": null,
        "seven_day": {
            "utilization": 75.0,
            "resets_at": "2099-11-05T14:59:59.917112+00:00",
            "limit_dollars": null,
            "used_dollars": null,
            "remaining_dollars": null,
            "locked_reason": null
        },
        "limits": [
            {
                "kind": "weekly_all",
                "group": "weekly",
                "percent": 75,
                "severity": "warning",
                "resets_at": "2099-11-05T14:59:59.917112+00:00",
                "scope": null,
                "is_active": true
            }
        ],
        "spend": {
            "used": { "amount_minor": 0, "currency": "USD", "exponent": 2 },
            "limit": null,
            "percent": 0,
            "enabled": false,
            "balance": null
        }
    });
    let formatted = format_anthropic_usage_summary(&payload, Some("max 20x")).unwrap();
    assert!(formatted.starts_with("7d: 25% left ↻"), "got: {formatted}");
    assert!(formatted.ends_with(" (max 20x)"), "got: {formatted}");
}

#[test]
fn anthropic_usage_formats_both_windows_and_spend_balance() {
    let payload = serde_json::json!({
        "five_hour": {
            "utilization": 10.0,
            "resets_at": "2099-11-01T02:00:00Z"
        },
        "seven_day": {
            "utilization": 40.0,
            "resets_at": "2099-11-05T15:00:00Z"
        },
        "spend": {
            "balance": { "amount_minor": 4250, "currency": "USD", "exponent": 2 }
        }
    });
    let formatted = format_anthropic_usage_summary(&payload, Some("max 5x")).unwrap();
    assert!(formatted.contains("5h: 90% left ↻"), "got: {formatted}");
    assert!(formatted.contains(" · 7d: 60% left ↻"), "got: {formatted}");
    assert!(
        formatted.contains(" · $42.50 left (max 5x)"),
        "got: {formatted}"
    );
}

#[test]
fn anthropic_profiles_never_cross_contaminate_credentials() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let def_dir = data.join("profiles/anthropic/default");
    let secondary_dir = data.join("profiles/anthropic/secondary");
    std::fs::create_dir_all(&def_dir).unwrap();
    std::fs::create_dir_all(&secondary_dir).unwrap();

    let now_ms = anyagents_core::now_ms();
    let def_cred = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "mock-oat-default-fresh",
            "refreshToken": "mock-ort-default-refresh",
            "expiresAt": now_ms + 3_600_000,
            "accountUuid": "acct-default-uuid"
        }
    });
    let secondary_cred = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "mock-oat-secondary-expired",
            "refreshToken": "mock-ort-secondary-refresh",
            "expiresAt": now_ms - 60_000,
            "accountUuid": "acct-secondary-uuid"
        }
    });
    std::fs::write(def_dir.join(".credentials.json"), def_cred.to_string()).unwrap();
    std::fs::write(
        secondary_dir.join(".credentials.json"),
        secondary_cred.to_string(),
    )
    .unwrap();

    sync_fresher_bound_agent_credentials(data);

    let after_secondary: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(secondary_dir.join(".credentials.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        after_secondary
            .pointer("/claudeAiOauth/refreshToken")
            .and_then(serde_json::Value::as_str),
        Some("mock-ort-secondary-refresh"),
        "secondary profile must never be overwritten by default profile credentials"
    );
}

#[test]
fn signin_deduplicates_same_email_and_enforces_one_email_one_profile() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let def_dir = data.join("profiles/anthropic/default");
    let secondary_dir = data.join("profiles/anthropic/secondary");
    let agent_cfg = data.join("agents/secondary-id/config");
    std::fs::create_dir_all(&def_dir).unwrap();
    std::fs::create_dir_all(&secondary_dir).unwrap();
    std::fs::create_dir_all(&agent_cfg).unwrap();
    std::fs::write(
        agent_cfg.join(".anyagents-profile"),
        "anthropic/secondary\n",
    )
    .unwrap();

    let dup_cred = serde_json::json!({
        "claudeAiOauth": {
            "accessToken": "",
            "refreshToken": "rtok-account-one",
            "accountUuid": "uuid-account-one",
            "email": "account-one@example.test"
        }
    });
    std::fs::write(def_dir.join(".credentials.json"), dup_cred.to_string()).unwrap();
    std::fs::write(
        secondary_dir.join(".credentials.json"),
        dup_cred.to_string(),
    )
    .unwrap();

    // Deduplicating same-email profiles merges `secondary` into `default` and updates bound agents.
    deduplicate_same_account_profiles(data);
    assert!(def_dir.is_dir());
    assert!(
        !secondary_dir.exists(),
        "duplicate profile for the same email must be removed"
    );
    assert_eq!(
        bound_profile_name(&agent_cfg).as_deref(),
        Some("anthropic/default")
    );

    // Signing in a new account (`account-two@example.test`) without `--profile`
    // creates its own distinct profile (`account-two`) without touching `default`.
    let held1 = SigninStaging::create(data, "anthropic", "test1").unwrap();
    let stage1 = held1.path().to_path_buf();
    std::fs::write(
        stage1.join(".credentials.json"),
        serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "",
                "refreshToken": "rtok-account-two",
                "accountUuid": "uuid-account-two",
                "email": "account-two@example.test"
            }
        })
        .to_string(),
    )
    .unwrap();

    let (prof1, second_dir, acct1) =
        resolve_and_commit_staged_signin(data, "anthropic", &stage1, None).unwrap();
    assert_eq!(prof1, "account-two");
    assert_eq!(acct1.as_deref(), Some("account-two@example.test"));
    assert_eq!(
        extract_profile_account_identity(&def_dir, "anthropic")
            .0
            .as_deref(),
        Some("account-one@example.test")
    );
    assert_eq!(
        extract_profile_account_identity(&second_dir, "anthropic")
            .0
            .as_deref(),
        Some("account-two@example.test")
    );

    // Re-signing in `account-one@example.test` (even with an explicit `--profile extra`)
    // updates its existing `default` profile rather than creating a duplicate email profile.
    let held2 = SigninStaging::create(data, "anthropic", "test2").unwrap();
    let stage2 = held2.path().to_path_buf();
    std::fs::write(
        stage2.join(".credentials.json"),
        serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "",
                "refreshToken": "rtok-account-one-2",
                "accountUuid": "uuid-account-one",
                "email": "account-one@example.test"
            }
        })
        .to_string(),
    )
    .unwrap();
    let (prof2, _, acct2) =
        resolve_and_commit_staged_signin(data, "anthropic", &stage2, Some("extra")).unwrap();
    assert_eq!(prof2, "default");
    assert_eq!(acct2.as_deref(), Some("account-one@example.test"));
    assert!(!data.join("profiles/anthropic/extra").exists());
}

#[test]
fn overlay_active_quota_reset_replaces_stale_cached_5h_window() {
    let tmp = tempfile::tempdir().unwrap();
    let prof_dir = tmp.path();
    let usage_path = prof_dir.join(".anyagents-last-usage.txt");
    let cached = "5h: 49% left ↻3h01m · 7d: 63% left ↻2d09h (max 20x)";
    std::fs::write(&usage_path, format!("{cached}\n")).unwrap();
    let now_ms = anyagents_core::now_ms();
    let reset_ms = now_ms + 95 * 60 * 1_000;
    std::fs::write(
        prof_dir.join(".anyagents-quota-reset-ms"),
        format!("{reset_ms}\n"),
    )
    .unwrap();

    let overlaid =
        overlay_active_quota_reset_on_cached_usage(prof_dir, &usage_path, cached, now_ms);
    assert!(
        overlaid.starts_with("5h: 0% left ↻1h"),
        "expected 5h: 0% left with live reset ETA, got: {overlaid}"
    );
    assert!(
        overlaid.ends_with(" · 7d: 63% left ↻2d09h (max 20x)"),
        "expected 7d tail preserved, got: {overlaid}"
    );
    assert_eq!(parse_remaining_pct_from_usage(&overlaid, None), Some(0));
}

#[test]
fn overlay_active_quota_reset_never_invents_a_5h_window_for_a_weekly_only_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let prof_dir = tmp.path();
    let usage_path = prof_dir.join(".anyagents-last-usage.txt");
    let cached = "7d: 5% left ↻1d22h (ultra)";
    std::fs::write(&usage_path, format!("{cached}\n")).unwrap();
    let now_ms = anyagents_core::now_ms();
    let reset_ms = now_ms + 140 * 60 * 1_000;
    std::fs::write(
        prof_dir.join(".anyagents-quota-reset-ms"),
        format!("{reset_ms}\n"),
    )
    .unwrap();

    let overlaid =
        overlay_active_quota_reset_on_cached_usage(prof_dir, &usage_path, cached, now_ms);
    assert!(!overlaid.contains("5h:"), "got: {overlaid}");
    assert!(overlaid.starts_with("limited ↻2h"), "got: {overlaid}");
    assert!(
        overlaid.ends_with(" · 7d: 5% left ↻1d22h (ultra)"),
        "got: {overlaid}"
    );
    assert_eq!(parse_remaining_pct_from_usage(&overlaid, None), Some(0));

    for (cached, want) in [("(ultra)", " (ultra)"), ("", "")] {
        let overlaid =
            overlay_active_quota_reset_on_cached_usage(prof_dir, &usage_path, cached, now_ms);
        assert!(!overlaid.contains("5h:"), "got: {overlaid}");
        assert!(overlaid.starts_with("limited ↻2h"), "got: {overlaid}");
        assert!(overlaid.ends_with(want), "got: {overlaid}");
    }
}

#[test]
fn anthropic_tier_label_names_ultra_over_a_leftover_max_tier() {
    assert_eq!(anthropic_tier_label(Some("ultra"), None), Some("ultra"));
    assert_eq!(
        anthropic_tier_label(Some("claude_ultra"), None),
        Some("ultra")
    );
    assert_eq!(
        anthropic_tier_label(Some("max"), Some("default_claude_ultra")),
        Some("ultra")
    );
    assert_eq!(
        anthropic_tier_label(Some("ultra"), Some("default_claude_max_20x")),
        Some("ultra")
    );
    assert_eq!(
        anthropic_tier_label(Some("max"), Some("default_claude_max_20x")),
        Some("max 20x")
    );
    assert_eq!(
        anthropic_tier_label(Some("max"), Some("default_claude_max_5x")),
        Some("max 5x")
    );
    assert_eq!(anthropic_tier_label(Some("pro"), None), Some("pro"));
    assert_eq!(anthropic_tier_label(None, None), None);
}

#[test]
fn a_binding_to_another_providers_profile_names_no_profile_of_this_one() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path();
    assert_eq!(
        bound_profile_of_provider(cfg, "sylphx").as_deref(),
        Some("default")
    );
    for (bound, provider, want) in [
        ("google-antigravity/foo", "sylphx", None),
        ("anthropic/foo", "sylphx", None),
        ("sylphx/foo", "anthropic", None),
        ("sylphx/foo", "sylphx", Some("foo")),
        ("spiron/foo", "sylphx", Some("foo")),
        ("claude-code/foo", "anthropic", Some("foo")),
        ("agy/foo", "google-antigravity", Some("foo")),
        ("anthropic/", "anthropic", Some("default")),
        ("foo", "sylphx", Some("foo")),
    ] {
        std::fs::write(cfg.join(".anyagents-profile"), format!("{bound}\n")).unwrap();
        assert_eq!(
            bound_profile_of_provider(cfg, provider).as_deref(),
            want,
            "{bound} under {provider}"
        );
    }
}

#[test]
fn switching_a_bound_agent_to_sylphx_never_creates_a_sylphx_profile_of_its_old_name() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    std::fs::create_dir_all(data.join("profiles/google-antigravity/foo")).unwrap();
    std::fs::create_dir_all(data.join("profiles/anthropic/bar")).unwrap();
    let store = Store::open(data.join("state.db")).unwrap();
    let opencode =
        r#"{"provider":{"sylphx":{"options":{"baseURL":"https://gateway.example/v1"}}}}"#;
    let seat = |name: &str, bound: Option<&str>| {
        let mut new = anyagents_core::NewAgent::new(name, "spiron");
        new.model = Some("sylphx/openai/gpt-test".to_string());
        let agent = store.create_agent(new).unwrap();
        store.set_agent_state(&agent.id, "running").unwrap();
        let cfg = agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(cfg.join(".config/opencode")).unwrap();
        std::fs::write(cfg.join(".config/opencode/opencode.json"), opencode).unwrap();
        std::fs::create_dir_all(cfg.join(".gemini/config")).unwrap();
        std::fs::write(cfg.join(".gemini/config/mcp_config.json"), "").unwrap();
        if let Some(bound) = bound {
            std::fs::write(cfg.join(".anyagents-profile"), format!("{bound}\n")).unwrap();
        }
        assert_eq!(
            upstream_provider_of(&agent.harness, agent.model.as_deref()),
            "sylphx"
        );
    };
    seat("was-agy", Some("google-antigravity/foo"));
    seat("was-claude", Some("anthropic/bar"));
    seat("unbound", None);

    let profiles = sync_and_list_provider_profiles_inner(&store, data, false, false);
    sync_fresher_bound_agent_credentials_in(&store, data);

    assert!(!data.join("profiles/sylphx/foo").exists());
    assert!(!data.join("profiles/sylphx/bar").exists());
    assert!(
        !profiles
            .iter()
            .any(|p| p.provider == "sylphx" && p.profile != "default"),
        "{:?}",
        profiles
            .iter()
            .map(|p| format!("{}/{}", p.provider, p.profile))
            .collect::<Vec<_>>()
    );
    // An unbound Sylphx seat still seeds the provider's default profile.
    assert!(
        data.join("profiles/sylphx/default/.config/opencode/opencode.json")
            .is_file()
    );
    // The old providers' profiles are left as they were.
    assert!(data.join("profiles/google-antigravity/foo").is_dir());
    assert!(data.join("profiles/anthropic/bar").is_dir());
    assert_eq!(probe_one_profile_usage(data, "sylphx", "foo"), None);
    assert!(!data.join("profiles/sylphx/foo").exists());
}

#[test]
#[cfg(unix)]
fn canonical_skills_pull_only_when_origin_main_moves() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let commit = |repo: &Path, file: &str| {
        std::fs::create_dir_all(repo.join("skills")).unwrap();
        std::fs::write(repo.join("skills").join(file), file).unwrap();
        git(repo, &["add", "."]);
        git(
            repo,
            &[
                "-c",
                "user.name=Example",
                "-c",
                "user.email=example@example.org",
                "commit",
                "-qm",
                file,
            ],
        );
    };
    let origin = dir.path().join("origin.git");
    let author = dir.path().join("author");
    let checkout = dir.path().join("checkout");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    git(
        dir.path(),
        &[
            "init",
            "-q",
            "--bare",
            "-b",
            "main",
            origin.to_str().unwrap(),
        ],
    );
    git(
        dir.path(),
        &[
            "clone",
            "-q",
            origin.to_str().unwrap(),
            author.to_str().unwrap(),
        ],
    );
    git(&author, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    commit(&author, "one.md");
    commit(&author, "two.md");
    git(&author, &["push", "-q", "origin", "main"]);
    git(
        dir.path(),
        &[
            "clone",
            "-q",
            origin.to_str().unwrap(),
            checkout.to_str().unwrap(),
        ],
    );
    std::os::unix::fs::symlink(checkout.join("skills"), data.join("skills")).unwrap();

    sync_canonical_skills(&data);
    let synced = git(&checkout, &["rev-parse", "HEAD"]);
    // Origin has not moved since the last pull: the checkout is left alone,
    // even one a local reset put behind.
    git(&checkout, &["reset", "-q", "--hard", "HEAD~1"]);
    sync_canonical_skills(&data);
    assert_ne!(git(&checkout, &["rev-parse", "HEAD"]), synced);

    commit(&author, "three.md");
    git(&author, &["push", "-q", "origin", "main"]);
    sync_canonical_skills(&data);
    assert_eq!(
        git(&checkout, &["rev-parse", "HEAD"]),
        git(&author, &["rev-parse", "HEAD"])
    );
    assert!(checkout.join("skills/three.md").is_file());
}

#[test]
fn unchanged_credential_pairs_are_not_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("seat/.credentials.json");
    let dst = dir.path().join("profile/.credentials.json");
    std::fs::create_dir_all(src.parent().unwrap()).unwrap();
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
    let cred = |token: &str, expires_at: i64| {
        serde_json::json!({"claudeAiOauth": {
            "accessToken": token,
            "refreshToken": "refresh",
            "expiresAt": expires_at,
        }})
        .to_string()
    };
    std::fs::write(&src, cred("same", 2_000_000_000_000)).unwrap();
    std::fs::write(&dst, cred("same", 2_000_000_000_000)).unwrap();
    let evaluations = || COPY_EVALUATIONS.with(std::cell::Cell::get);
    let start = evaluations();
    assert!(!copy_seat_file_if_newer(&src, &dst));
    assert!(!copy_seat_file_if_newer(&src, &dst));
    assert_eq!(evaluations() - start, 1, "an unchanged pair was read again");

    std::fs::write(&src, cred("fresher", 2_000_000_100_000)).unwrap();
    assert!(copy_seat_file_if_newer(&src, &dst));
    assert!(std::fs::read_to_string(&dst).unwrap().contains("fresher"));
    assert!(!copy_seat_file_if_newer(&src, &dst));
    assert!(!copy_seat_file_if_newer(&src, &dst));
    assert_eq!(evaluations() - start, 3);
}

#[test]
fn copy_seat_file_if_newer_syncs_rotated_refresh_token_and_preserves_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let prof_cred = tmp.path().join("profile-credentials.json");
    let seat_cred = tmp.path().join(".credentials.json");
    let now_ms = anyagents_core::now_ms();
    std::fs::write(
        &prof_cred,
        serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "old-access",
                "refreshToken": "old-refresh-token",
                "expiresAt": now_ms - 10_000,
                "email": "account-two@example.test",
                "accountUuid": "uuid-account-two"
            }
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        &seat_cred,
        serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "new-access",
                "refreshToken": "rotated-refresh-token",
                "expiresAt": now_ms + 3_600_000
            }
        })
        .to_string(),
    )
    .unwrap();

    assert!(copy_seat_file_if_newer(&seat_cred, &prof_cred));
    let synced: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&prof_cred).unwrap()).unwrap();
    assert_eq!(
        synced
            .pointer("/claudeAiOauth/accessToken")
            .and_then(serde_json::Value::as_str),
        Some("new-access")
    );
    assert_eq!(
        synced
            .pointer("/claudeAiOauth/refreshToken")
            .and_then(serde_json::Value::as_str),
        Some("rotated-refresh-token")
    );
    assert_eq!(
        synced
            .pointer("/claudeAiOauth/email")
            .and_then(serde_json::Value::as_str),
        Some("account-two@example.test")
    );
    assert_eq!(
        synced
            .pointer("/claudeAiOauth/accountUuid")
            .and_then(serde_json::Value::as_str),
        Some("uuid-account-two")
    );
}

#[test]
fn shared_agents_md_reaches_native_aliases_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let shared_path = data.join("AGENTS.md");
    let work = data.join("workspace");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    std::fs::write(
        work.join("AGENTS.md"),
        "# Repository rules\nLOCAL_ONLY_RULE\n",
    )
    .unwrap();
    let shared = "# Example/shared-instructions\nSHARED_ONLY_RULE 繁體\n";

    for harness in ["agy", "claude-code", "codex", "spiron"] {
        std::fs::write(&shared_path, shared).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(harness, harness))
            .unwrap();
        let config = agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        write_agent_instructions_with_continuity(&config, &agent, None, None).unwrap();

        let compact = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(!compact.contains("SHARED_ONLY_RULE"), "{harness}");
        assert!(!compact.contains("## 4. Shared Instructions (AGENTS.md)"));
        for path in [
            "AGENTS.md",
            "CLAUDE.md",
            ".claude/CLAUDE.md",
            ".gemini/config/GEMINI.md",
        ] {
            let native = std::fs::read_to_string(config.join(path)).unwrap();
            assert!(native.starts_with(&compact), "{harness}: {path}");
            assert!(native.contains(shared.trim()), "{harness}: {path}");
            assert_eq!(
                native.matches("SHARED_ONLY_RULE").count(),
                1,
                "{harness}: {path}"
            );
            assert_eq!(
                native
                    .matches("## 4. Shared Instructions (AGENTS.md)")
                    .count(),
                1,
                "{harness}: {path}"
            );
        }
        assert!(!config.join(".gemini/config/AGENTS.md").exists());
        assert!(!config.join(".gemini/config/rules/AGENTS.md").exists());
    }
}

#[test]
fn create_compact_prompt_removes_legacy_gemini_rules_and_bounds_continuity() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let shared_constitution = format!(
        "# Example/shared-instructions\n\n{}",
        "Rule line for shared company instructions.\n".repeat(160)
    );
    std::fs::write(data.join("AGENTS.md"), &shared_constitution).unwrap();

    let agent = create(
        &store,
        data,
        CreateRequest {
            name: "sample-agent".to_string(),
            role: Some("Growth Mind".to_string()),
            harness: "agy".to_string(),
            model: Some("claude-opus-4-6-thinking#max".to_string()),
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: Some(data.to_path_buf()),
            description: Some("Public Layer 1 charter for Hermes.".to_string()),
            instructions: Some("Private Layer 2 execution SOP for Hermes.".to_string()),
            parent_id: None,
        },
    )
    .unwrap();

    assert_eq!(
        agent.public_description().as_deref(),
        Some("Public Layer 1 charter for Hermes.")
    );
    assert_eq!(
        agent.instructions.as_deref(),
        Some("Private Layer 2 execution SOP for Hermes.")
    );

    let cfg = anyagents_core::agent_config_dir(data, &agent.id);
    let base_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(base_prompt.len() < 3_500, "{} bytes", base_prompt.len());
    assert!(!base_prompt.contains("## 4. Shared Instructions (AGENTS.md)"));
    assert!(!base_prompt.contains(&shared_constitution));
    assert!(!base_prompt.contains("Cross-Harness Session Continuity"));
    assert!(!base_prompt.contains("\n<learned_memory>\n"));
    assert!(!base_prompt.contains("## Memory policy"));
    assert!(base_prompt.contains("## 3. Hierarchy, Topology & Communication Rules"));
    assert!(!base_prompt.contains("Runtime Awareness & the 6 Canonical MCP Tools"));
    assert!(!base_prompt.contains("## 2.1"));

    let gemini_md = cfg.join(".gemini/config/GEMINI.md");
    let gemini_agents = cfg.join(".gemini/config/AGENTS.md");
    let gemini_rules_agents = cfg.join(".gemini/config/rules/AGENTS.md");
    let base_native_prompt = std::fs::read_to_string(&gemini_md).unwrap();
    assert!(base_native_prompt.starts_with(&base_prompt));
    assert!(base_native_prompt.contains(shared_constitution.trim()));
    assert_eq!(
        base_native_prompt
            .matches("# Example/shared-instructions")
            .count(),
        1
    );
    assert!(!gemini_agents.exists());
    assert!(!gemini_rules_agents.exists());

    // Refresh cleans up old copies without disturbing unrelated rules.
    std::fs::create_dir_all(gemini_rules_agents.parent().unwrap()).unwrap();
    std::fs::write(&gemini_agents, &base_prompt).unwrap();
    std::fs::write(&gemini_rules_agents, &base_prompt).unwrap();
    let other_rule = cfg.join(".gemini/config/rules/custom.md");
    std::fs::write(&other_rule, "Keep this caller-owned rule.\n").unwrap();
    write_agent_instructions_with_continuity(&cfg, &agent, None, None).unwrap();
    assert!(!gemini_agents.exists());
    assert!(!gemini_rules_agents.exists());
    assert_eq!(
        std::fs::read_to_string(&gemini_md).unwrap(),
        base_native_prompt
    );
    std::fs::write(cfg.join("sylphx-cloud-session.json"), "{}").unwrap();

    // The previous symlink deduplication did not work: agy identifies the
    // global rule as user_global and scanned rules by file path.
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("GEMINI.md", &gemini_agents).unwrap();
        std::os::unix::fs::symlink("../GEMINI.md", &gemini_rules_agents).unwrap();
    }

    let huge_continuity = format!(
        "## 4. Cross-Harness Session Continuity (`claude-code` -> `agy`)\n\n{}",
        "- `[OUT (already sent/completed)]` `msg_1`: Completed verification 繁體.\n".repeat(1500)
    );
    write_agent_instructions_with_continuity(&cfg, &agent, None, Some(&huge_continuity)).unwrap();

    let sys_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(sys_prompt.contains("- **Agent Name**: `sample-agent`"));
    assert!(sys_prompt.contains("- **Role Label**: `Growth Mind`"));
    assert!(sys_prompt.contains("Public Layer 1 charter for Hermes."));
    assert!(sys_prompt.contains("Private Layer 2 execution SOP for Hermes."));
    assert!(!sys_prompt.contains("## 4. Shared Instructions (AGENTS.md)"));
    assert!(!sys_prompt.contains(&shared_constitution));
    assert!(sys_prompt.starts_with(&base_prompt));
    assert!(sys_prompt.len() < base_prompt.len() + huge_continuity.len());
    assert!(sys_prompt.contains("## 4. Cross-Harness Session Continuity"));
    assert!(
        sys_prompt.len() < 24_000,
        "prompt must stay under agy's 24,000-byte per-file cap, got {} bytes",
        sys_prompt.len()
    );
    assert_eq!(
        std::fs::read_to_string(cfg.join(".sylphx-cloud-instructions-fingerprint")).unwrap(),
        "stale\n"
    );

    let native_prompt = std::fs::read_to_string(&gemini_md).unwrap();
    assert!(native_prompt.starts_with(&base_native_prompt));
    assert!(native_prompt.contains(shared_constitution.trim()));
    assert_eq!(
        native_prompt
            .matches("# Example/shared-instructions")
            .count(),
        1
    );
    assert!(native_prompt.contains("## 4. Cross-Harness Session Continuity"));
    assert!(
        native_prompt.len() < 24_000,
        "{} bytes",
        native_prompt.len()
    );
    assert!(!gemini_agents.exists());
    assert!(!gemini_rules_agents.exists());
    // exists() alone also accepts dangling symlinks; neither alias may remain.
    assert!(std::fs::symlink_metadata(&gemini_agents).is_err());
    assert!(std::fs::symlink_metadata(&gemini_rules_agents).is_err());
    assert_eq!(
        std::fs::read_to_string(&other_rule).unwrap(),
        "Keep this caller-owned rule.\n"
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("missing.md", &gemini_agents).unwrap();
        std::os::unix::fs::symlink("missing.md", &gemini_rules_agents).unwrap();
    }
    write_agent_instructions_with_continuity(&cfg, &agent, None, Some(&huge_continuity)).unwrap();
    assert!(std::fs::symlink_metadata(&gemini_agents).is_err());
    assert!(std::fs::symlink_metadata(&gemini_rules_agents).is_err());
    assert_eq!(std::fs::read_to_string(&gemini_md).unwrap(), native_prompt);
}
