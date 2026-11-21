use super::super::agents::{
    SpawnFinish, create_agent_route, finish_interrupted_spawns, get_description_route, list_agents,
    record_spawn_finish, set_description_route, spawn_finish_marker,
};
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock, http};
use crate::create;
use axum::Json;
use axum::http::StatusCode;

#[test]
fn an_interrupted_spawn_gets_its_prompt_once_at_the_next_start() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("child", "codex");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let finish = SpawnFinish {
        workdir: Some("/allocated/worktree".into()),
        actor: "agent:parent".into(),
        caller: Some(parent.id.to_string()),
        scope: None,
        public: None,
        lifecycle: Some("task".into()),
        groups: Vec::new(),
        prompt: Some("build the thing".into()),
    };
    // The server stopped after admitting the child, before its prompt.
    let marker = spawn_finish_marker(data, &child.id);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    record_spawn_finish(&marker, &finish).unwrap();

    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert!(!marker.exists());
    let inbox = store.inbox(&child.id, None).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].text, "build the thing");
    assert_eq!(
        inbox[0].sender.as_deref(),
        Some(format!("agent:{}", parent.id).as_str())
    );
    assert_eq!(
        store.agent(&child.id).unwrap().lifecycle().to_string(),
        "task"
    );

    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/allocated/worktree")
    );
    // A later authoritative assignment must survive a stale finish record.
    store
        .set_agent_running(&child.id, "/current/worktree")
        .unwrap();
    // Stopped again after the prompt landed but before the record went:
    // the next start finishes without a second prompt.
    record_spawn_finish(&marker, &finish).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1);
    assert_eq!(finish_interrupted_spawns(&store, data), 0);
    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/current/worktree")
    );
    // Records written before workdir snapshots remain readable.
    let mut legacy = serde_json::to_value(&finish).unwrap();
    legacy.as_object_mut().unwrap().remove("workdir");
    let legacy: SpawnFinish = serde_json::from_value(legacy).unwrap();
    assert!(legacy.workdir.is_none());
    record_spawn_finish(&marker, &legacy).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/current/worktree")
    );
}

#[test]
fn an_interrupted_spawn_that_cannot_finish_keeps_its_record_for_the_next_start() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let child = store
        .create_agent(anyagents_core::NewAgent::new("child", "codex"))
        .unwrap();
    let mut finish = SpawnFinish {
        workdir: None,
        actor: "operator".into(),
        caller: None,
        scope: None,
        public: None,
        // Refused by the store: this start cannot finish the spawn.
        lifecycle: Some("not-a-lifecycle".into()),
        groups: Vec::new(),
        prompt: Some("build the thing".into()),
    };
    let marker = spawn_finish_marker(data, &child.id);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    record_spawn_finish(&marker, &finish).unwrap();

    assert_eq!(finish_interrupted_spawns(&store, data), 0);
    assert!(
        marker.exists(),
        "a failed finish dropped the spawn's record"
    );
    assert!(store.inbox(&child.id, None).unwrap().is_empty());

    // The next start can: the prompt lands and only then the record goes.
    finish.lifecycle = Some("task".into());
    record_spawn_finish(&marker, &finish).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert!(!marker.exists());
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1);
}

#[test]
fn spawn_defaults_to_private_regardless_of_role() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
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
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/researcher.md"),
        "---\nname: researcher\ndescription: Research specialist\n---\nResearch deeply.\n",
    )
    .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (name, role, scope) in [("labelled", Some("Researcher"), "private"), ("task", None, "private")] {
            let body = serde_json::from_value(serde_json::json!({"name":name,"role":role,"harness":"codex","model":"test-model","workdir":"private"})).unwrap();
            let (_, Json(created)) = create_agent_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), axum::http::HeaderMap::new(), Json(body)).await.unwrap_or_else(|e| panic!("{}: {}",e.status,e.message));
            assert_eq!(created.scope, scope);
            let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":true})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
            let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":false})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
            assert_eq!(store.agent_by_name(name).unwrap().scope(),scope);
        }
    });
}

#[test]
fn archetype_templates_apply_frontmatter_defaults_and_prepend_instructions() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent_work = dir.path().join("parent-work");
    let agents_dir = parent_work.join(".agents").join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();
    std::fs::write(
        agents_dir.join("writer.md"),
        "---\ndescription: Ephemeral isolated-worktree sole writer\nscope: isolated\nworkdir: private\nharness: opencode\nmodel: example/model\n---\nAlways run cargo test before opening a PR.",
    )
    .unwrap();
    std::fs::write(
        agents_dir.join("code-reviewer.md"),
        "---\ndescription: Read-only code reviewer\nscope: siblings\nharness: claude-code\n---\nVerify all invariants before approving.",
    )
    .unwrap();

    let data_templates = dir.path().join("agents-templates");
    std::fs::create_dir_all(&data_templates).unwrap();
    std::fs::write(
        data_templates.join("auditor.md"),
        "---\ndescription: Global template auditor\n---\nAudit template body.",
    )
    .unwrap();

    let mut new_parent = anyagents_core::NewAgent::new("calliope", "claude-code");
    new_parent.role = Some("Mind".into());
    new_parent.workdir = Some(parent_work.to_string_lossy().into_owned());
    new_parent.description = Some("Parent mind".into());
    let parent = store.create_agent(new_parent).unwrap();
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
        let (status, Json(created)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "writer-sub-1",
                    "type": "writer",
                    "instructions": "Fix issue #42."
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            created.description.as_deref(),
            Some("Ephemeral isolated-worktree sole writer")
        );
        assert_eq!(created.scope, "isolated");
        assert_eq!(created.harness, "opencode");
        assert_eq!(created.model.as_deref(), Some("example/model"));
        let stored = store.agent_by_name("writer-sub-1").unwrap();
        assert_eq!(stored.instructions.as_deref(), Some("Fix issue #42."));
        assert_eq!(
            created.instructions.as_deref(),
            Some(
                format!(
                    "{}\n\nAlways run cargo test before opening a PR.\n\nFix issue #42.",
                    create::BUILTIN_GENERAL_ROLE_BODY
                )
                .as_str()
            )
        );

        // Role matching a template slug (`role: "code-reviewer"`) applies template defaults
        // when `archetype` is omitted, projecting template instructions dynamically.
        let (_, Json(reviewer)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "reviewer-sub-1",
                    "role": "code-reviewer"
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
        assert_eq!(
            reviewer.description.as_deref(),
            Some("Read-only code reviewer")
        );
        assert_eq!(reviewer.scope, "siblings");
        let reviewer_stored = store.agent_by_name("reviewer-sub-1").unwrap();
        assert!(
            reviewer_stored.instructions.is_none(),
            "template body must not be baked into sqlite: {:?}",
            reviewer_stored.instructions
        );
        assert_eq!(
            reviewer.instructions.as_deref(),
            Some(
                format!(
                    "{}\n\nVerify all invariants before approving.",
                    create::BUILTIN_GENERAL_ROLE_BODY
                )
                .as_str()
            )
        );

        // `<data_dir>/agents-templates/<name>.md` is also searched.
        let (_, Json(auditor)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "auditor-sub-1",
                    "template": "auditor"
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
        assert_eq!(
            auditor.description.as_deref(),
            Some("Global template auditor")
        );
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn http_spawn_uses_deployment_harness_for_roots_but_parent_for_children() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let parent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let Err(unconfigured) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "unconfigured",
                    "parent": "root",
                }))
                .unwrap(),
            ),
        )
        .await
        else {
            panic!("root without a harness or deployment default must be refused");
        };
        assert_eq!(unconfigured.status, StatusCode::BAD_REQUEST);
        assert!(state.store.agent_by_name("unconfigured").is_err());
        std::fs::write(dir.path().join("subagent-defaults.json"), r#"{"default":{"mode":"preset","harness":"codex"}}"#).unwrap();
        for (name, parent_ref, harness, expected) in [
            ("root-default", "root", None, "codex"),
            ("child-default", parent.id.as_str(), None, "opencode"),
            ("child-empty", parent.id.as_str(), Some(""), "opencode"),
            ("child-inherit", parent.id.as_str(), Some("inherit"), "opencode"),
            ("child-explicit", parent.id.as_str(), Some("codex"), "codex"),
        ] {
            let mut body = serde_json::json!({"name": name, "parent": parent_ref, "workdir": dir.path().join(name), "scope": "team"});
            if let Some(harness) = harness { body["harness"] = serde_json::json!(harness); }
            let (status, Json(created)) = create_agent_route(
                Principal::Operator, State(state.clone()), axum::http::HeaderMap::new(), Json(serde_json::from_value(body).unwrap()),
            ).await.unwrap_or_else(|e| panic!("{}", e.message));
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(created.harness, expected, "{name}");
            assert_eq!(created.scope, "siblings");
        }
        // An explicit unknown role must fail closed even when no custom catalog exists on disk.
        let before_count = state.store.list_agents().unwrap().len();
        let unknown_body = serde_json::json!({
            "name": "unmatched-label",
            "parent": parent.id.as_str(),
            "role": "nonexistent-display-label-134",
            "workdir": dir.path().join("unmatched-label"),
        });
        let err = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(serde_json::from_value(unknown_body).unwrap()),
        )
        .await
        .expect_err("unknown explicit role must fail closed");
        assert_eq!(err.status, StatusCode::BAD_REQUEST, "{}", err.message);
        assert!(
            err.message
                .contains("unknown role 'nonexistent-display-label-134'"),
            "{}",
            err.message
        );
        assert!(
            err.message.contains("available roles: [general]"),
            "{}",
            err.message
        );
        assert_eq!(state.store.list_agents().unwrap().len(), before_count);
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

async fn assert_archetype_create_refused(
    state: &AppState,
    body: serde_json::Value,
    expected_error: &str,
) {
    let result = create_agent_route(
        Principal::Operator,
        State(state.clone()),
        axum::http::HeaderMap::new(),
        Json(serde_json::from_value(body).unwrap()),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("invalid archetype unexpectedly created an agent"),
        Err(error) => error,
    };
    assert_eq!(error.status, StatusCode::BAD_REQUEST, "{}", error.message);
    assert!(error.message.contains(expected_error), "{}", error.message);
    assert!(!error.message.contains("OUTSIDE_TEMPLATE_SENTINEL"));
    assert!(state.store.list_agents().unwrap().is_empty());
    let Json(listed) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery::default()),
    )
    .await
    .unwrap_or_else(|error| panic!("{}", error.message));
    assert!(listed.agents.is_empty());
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains("OUTSIDE_TEMPLATE_SENTINEL")
    );
    let description = get_description_route(
        Principal::Operator,
        State(state.clone()),
        AxumPath("rejected-template-agent".into()),
    )
    .await;
    match description {
        Ok(_) => panic!("rejected template must not have a description"),
        Err(error) => assert_eq!(error.status, StatusCode::NOT_FOUND),
    }
    // Validation runs before creation, including config/SYSTEM_PROMPT.md writes.
    let agents_dir = state.store.data_dir().join("agents");
    assert!(
        !agents_dir.exists() || std::fs::read_dir(agents_dir).unwrap().next().is_none(),
        "a rejected template must not leave a SYSTEM_PROMPT.md or agent directory"
    );
}

#[test]
fn create_agent_route_defaults_omitted_and_empty_role_to_builtin_general() {
    let _env = env_lock();
    let prev_home = std::env::var_os("HOME");
    let prev_owner = std::env::var_os("ANYAGENTS_OWNER_WORKDIR");
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        std::env::set_var("HOME", &home);
        std::env::remove_var("ANYAGENTS_OWNER_WORKDIR");
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let state = archetype_validation_state(dir.path());
        for (name, extra) in [
            ("http-omitted", serde_json::json!({})),
            ("http-empty", serde_json::json!({ "role": "" })),
            ("http-general", serde_json::json!({ "role": "general" })),
        ] {
            let mut body = serde_json::json!({
                "name": name,
                "harness": "claude-code",
                "workdir": work,
            });
            if let Some(obj) = extra.as_object() {
                for (k, v) in obj {
                    body[k] = v.clone();
                }
            }
            let (status, Json(agent)) = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(serde_json::from_value(body).unwrap()),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: {}: {}", e.status, e.message));
            assert_eq!(status, StatusCode::CREATED, "{name}");
            assert_eq!(agent.role.as_deref(), Some("general"), "{name}");
            assert_eq!(agent.scope, "public", "{name}");
            assert_eq!(
                agent.instructions.as_deref(),
                Some(create::BUILTIN_GENERAL_ROLE_BODY),
                "{name}"
            );
        }

        // Sub-agent with omitted role gets built-in `general` with `private` scope.
        let (sub_status, Json(sub)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-subagent",
                    "parent": "http-omitted",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("subagent: {}: {}", e.status, e.message));
        assert_eq!(sub_status, StatusCode::CREATED);
        assert_eq!(sub.role.as_deref(), Some("general"));
        assert_eq!(sub.scope, "private");
        assert_eq!(
            sub.instructions.as_deref(),
            Some(create::BUILTIN_GENERAL_ROLE_BODY)
        );

        // Explicit `public: true` / `public: false` without `scope` on a sub-agent.
        for (public_flag, expected_scope) in [(true, "public"), (false, "private")] {
            let (flag_status, Json(flagged)) = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": format!("http-sub-public-{public_flag}"),
                        "parent": "http-omitted",
                        "public": public_flag,
                        "workdir": work,
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("public={public_flag}: {}: {}", e.status, e.message));
            assert_eq!(flag_status, StatusCode::CREATED);
            assert_eq!(flagged.role.as_deref(), Some("general"));
            assert_eq!(flagged.scope, expected_scope);
        }

        // Caller `description` without `instructions` composes with built-in `general`.
        let (legacy_status, Json(legacy_sub)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-sub-legacy-desc",
                    "parent": "http-omitted",
                    "description": "Legacy HTTP brief",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("legacy: {}: {}", e.status, e.message));
        assert_eq!(legacy_status, StatusCode::CREATED);
        assert_eq!(legacy_sub.role.as_deref(), Some("general"));
        assert_eq!(legacy_sub.scope, "private");
        assert_eq!(
            legacy_sub.description.as_deref(),
            Some("Legacy HTTP brief")
        );
        assert_eq!(
            legacy_sub.instructions.as_deref(),
            Some(
                format!("{}\n\nLegacy HTTP brief", create::BUILTIN_GENERAL_ROLE_BODY)
                    .as_str()
            )
        );

        // Workspace .agents/agents/general.md overrides the built-in template even for root.
        std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
        std::fs::write(
            work.join(".agents/agents/general.md"),
            "---\nname: general\ndescription: HTTP General Override\nscope: private\n---\nHTTP general override body.\n",
        )
        .unwrap();
        let (status, Json(overridden)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-overridden",
                    "harness": "claude-code",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("override: {}: {}", e.status, e.message));
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(overridden.role.as_deref(), Some("general"));
        assert_eq!(overridden.scope, "private");
        assert_eq!(
            overridden.description.as_deref(),
            Some("HTTP General Override")
        );
        assert!(
            overridden
                .instructions
                .as_deref()
                .is_some_and(|text| text.contains("HTTP general override body."))
        );
    });

    unsafe {
        match prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match prev_owner {
            Some(v) => std::env::set_var("ANYAGENTS_OWNER_WORKDIR", v),
            None => std::env::remove_var("ANYAGENTS_OWNER_WORKDIR"),
        }
    }
}

#[cfg(unix)]
#[test]
fn archetype_templates_reject_file_and_directory_symlinks_without_leaking() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for variant in [
            "file",
            "agents-dir",
            "dot-agents-dir",
            "nested-dir",
            "data-dir",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = archetype_validation_state(dir.path());
            let work = dir.path().join("work");
            let outside = dir.path().join("outside");
            std::fs::create_dir_all(&outside).unwrap();
            let secret = outside.join("escape.md");
            std::fs::write(&secret, "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::write(outside.join("agent.md"), "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
            match variant {
                "file" => {
                    std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                    symlink(&secret, work.join(".agents/agents/escape.md")).unwrap();
                }
                "agents-dir" => {
                    std::fs::create_dir_all(work.join(".agents")).unwrap();
                    symlink(&outside, work.join(".agents/agents")).unwrap();
                }
                "dot-agents-dir" => {
                    std::fs::create_dir_all(&work).unwrap();
                    std::fs::create_dir_all(outside.join("agents")).unwrap();
                    std::fs::write(
                        outside.join("agents/escape.md"),
                        "OUTSIDE_TEMPLATE_SENTINEL",
                    )
                    .unwrap();
                    symlink(&outside, work.join(".agents")).unwrap();
                }
                "nested-dir" => {
                    std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                    symlink(&outside, work.join(".agents/agents/escape")).unwrap();
                }
                "data-dir" => {
                    std::fs::create_dir_all(&work).unwrap();
                    symlink(&outside, dir.path().join("agents-templates")).unwrap();
                }
                _ => unreachable!(),
            }
            // A lower-priority valid template must never mask a hostile path.
            std::fs::create_dir_all(dir.path().join("archetypes")).unwrap();
            std::fs::write(dir.path().join("archetypes/escape.md"), "Safe fallback.").unwrap();
            for selector in ["archetype", "role"] {
                let mut body = serde_json::json!({
                    "name": "rejected-template-agent",
                    "workdir": work,
                    "harness": "codex",
                });
                body[selector] = serde_json::json!("escape");
                assert_archetype_create_refused(&state, body, "symlink").await;
            }
        }
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archetype_templates_reject_unknown_explicit_names_and_malformed_frontmatter() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let work = dir.path().join("work");
    let templates = work.join(".agents/agents");
    std::fs::create_dir_all(&templates).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for selector in ["archetype", "template", "type", "preset"] {
            let mut body =
                serde_json::json!({"name": "rejected-template-agent", "workdir": work});
            body[selector] = serde_json::json!("nonexistent-role");
            assert_archetype_create_refused(
                &state,
                body,
                "unknown archetype template \"nonexistent-role\"",
            )
            .await;
        }
        assert_archetype_create_refused(
            &state,
            serde_json::json!({"name": "rejected-template-agent", "scope": "invalid-request-scope", "workdir": work}),
            "invalid scope `invalid-request-scope`",
        ).await;
        for name in [
            ".",
            "..",
            "../escape",
            "escape.md",
            "escape/agent",
            "escape\\agent",
        ] {
            assert_archetype_create_refused(
                &state,
                serde_json::json!({
                    "name": "rejected-template-agent", "archetype": name, "workdir": work,
                }),
                "invalid archetype template name",
            )
            .await;
        }
        for (raw, expected) in [
            ("---\ndescription: unclosed", "unclosed YAML frontmatter"),
            (
                "---\nscpoe: isolated\n---\nUNKNOWN_KEY_BODY",
                "unknown archetype template frontmatter key \"scpoe\"",
            ),
            (
                "---\nscope: invalid-review-scope\n---\nINVALID_SCOPE_BODY",
                "invalid scope `invalid-review-scope`",
            ),
            (
                "---\ndescription broken\n---\nBody.",
                "malformed YAML frontmatter",
            ),
            (
                "---\ndescription: [broken\n---\nBody.",
                "malformed YAML frontmatter",
            ),
            (
                "---\ndescription: broken\n---not-a-delimiter\nBody.",
                "unclosed YAML frontmatter",
            ),
        ] {
            std::fs::write(templates.join("broken.md"), raw).unwrap();
            for selector in ["archetype", "role"] {
                let mut body =
                    serde_json::json!({"name": "rejected-template-agent", "workdir": work});
                body[selector] = serde_json::json!("broken");
                assert_archetype_create_refused(&state, body, expected).await;
            }
        }
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_http_only_operator_root_creation_can_request_a_named_seat() {
    let dir = crate::test_tempdir();
    let mut state = archetype_validation_state(dir.path());
    let operator_token = "operator-token-0123456789abcdef0123456789abcdef";
    state.auth = auth::Mode::Confined {
        operator_digest: Some(auth::operator_digest(operator_token)),
    };
    let owner = state
        .store
        .create_agent(anyagents_core::NewAgent::new("owner", "spiron"))
        .unwrap();
    let capability = state.store.issue_capability(&owner.id).unwrap();
    let uid = (42001..42010)
        .find(|&uid| uid != crate::sandbox::server_uid())
        .unwrap();
    let data = state.store.data_dir();
    let registry = data.join("execution-principals.json");
    std::fs::write(
        data.join("test-passwd"),
        format!(
            "seat-claimed:x:{uid}:42010::/:/bin/false\nseat-fresh:x:{}:42010::/:/bin/false\n",
            uid + 100
        ),
    )
    .unwrap();
    let saved = serde_json::json!({owner.id.as_str(): {
        "user":"seat-claimed", "uid":uid, "gid":42010, "socket_gid":42011
    }})
    .to_string();
    std::fs::write(&registry, &saved).unwrap();
    let socket = dir.path().join("admission.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let app = build_router(state.clone());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let workdir = dir.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let explicit = serde_json::json!({
        "name":"fresh", "harness":"spiron", "scope":"isolated", "workdir":workdir,
        "seat_user":"seat-fresh", "seat_uid":uid + 100
    });
    for (mut body, agent_token, operator, want, problem) in [
        (
            explicit.clone(),
            None,
            None,
            StatusCode::UNAUTHORIZED,
            "authenticated",
        ),
        (
            explicit.clone(),
            Some(capability.as_str()),
            None,
            StatusCode::FORBIDDEN,
            "operator-created root",
        ),
        (
            explicit.clone(),
            None,
            Some(operator_token),
            StatusCode::FORBIDDEN,
            "operator-created root",
        ),
        (
            serde_json::json!({"name":"claimed","harness":"spiron","scope":"isolated","workdir":workdir}),
            None,
            Some(operator_token),
            StatusCode::FORBIDDEN,
            "already claimed",
        ),
    ] {
        if operator.is_some() && body["name"] == "fresh" {
            body["parent"] = serde_json::json!(owner.name);
        }
        let (status, reply) = http(
            &socket,
            "POST",
            "/v1/agents",
            Some(body),
            agent_token,
            operator,
        )
        .await;
        assert_eq!(status, want, "{reply}");
        assert!(
            reply["error"].as_str().unwrap().contains(problem),
            "{reply}"
        );
    }
    let mut mismatch = explicit;
    mismatch["seat_uid"] = serde_json::json!(uid + 101);
    let (status, reply) = http(
        &socket,
        "POST",
        "/v1/agents",
        Some(mismatch),
        None,
        Some(operator_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{reply}");
    assert!(
        reply["error"].as_str().unwrap().contains("does not match"),
        "{reply}"
    );
    assert_eq!(std::fs::read_to_string(registry).unwrap(), saved);
    assert!(state.store.agent_by_name("fresh").is_err());
    assert!(state.store.agent_by_name("claimed").is_err());
    server.abort();
}
