use anyagents_core::{NewAgent, agent_config_dir};

#[cfg(unix)]
use super::archetypes::open_archetype_nofollow;
use super::archetypes::{archetype_search_roots_for_owner, read_archetype_file};
use super::*;

#[test]
fn configured_owner_catalog_excludes_all_other_roots_even_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    let owner = dir.path().join("owner");
    let workspace = dir.path().join("workspace");
    let ancestor = dir.path().join("ancestor");
    let data = dir.path().join("data");
    for base in [&workspace, &ancestor, &data] {
        std::fs::create_dir_all(base.join(".agents/agents")).unwrap();
        std::fs::write(base.join(".agents/agents/retired.md"), "Stale.").unwrap();
    }
    for root in ["agents-templates", "archetypes"] {
        std::fs::create_dir_all(data.join(root)).unwrap();
        std::fs::write(data.join(root).join("retired.md"), "Stale.").unwrap();
    }
    let roots = || {
        archetype_search_roots_for_owner(
            Some(&workspace),
            std::slice::from_ref(&ancestor),
            &data,
            Some(&owner),
        )
    };
    let expected = vec![(owner.clone(), PathBuf::from(".agents/agents"))];
    // Missing configuration never widens the search to legacy catalogs.
    assert_eq!(roots(), expected);
    std::fs::create_dir_all(owner.join(".agents/agents")).unwrap();
    assert_eq!(roots(), expected); // Empty catalogs are authoritative too.
    std::fs::write(owner.join(".agents/agents/worker.md"), "Current.").unwrap();
    assert_eq!(roots(), expected);
    for (base, relative) in roots() {
        assert!(
            read_archetype_file(&base, &relative.join("retired.md"))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_archetype_file(&base, &relative.join("worker.md"))
                .unwrap()
                .as_deref(),
            Some("Current.")
        );
    }
    // Unconfigured installations retain workspace and data fallback behavior.
    let fallback = archetype_search_roots_for_owner(Some(&workspace), &[ancestor], &data, None);
    assert!(fallback.contains(&(workspace, PathBuf::from(".agents/agents"))));
    assert!(fallback.contains(&(data, PathBuf::from("agents-templates"))));
}

#[test]
fn empty_unconfigured_catalog_preserves_free_form_role_updates() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".agents/agents")).unwrap();
    assert_eq!(
        normalize_or_validate_agent_role("retired", None, None, dir.path()).unwrap(),
        "retired"
    );
}

#[test]
fn data_archetypes_accept_name_metadata_and_prefer_shipped_templates() {
    let dir = tempfile::tempdir().unwrap();
    for root in [".agents/agents", "agents-templates", "archetypes"] {
        std::fs::create_dir_all(dir.path().join(root)).unwrap();
    }
    for slug in ["worker", "reviewer", "mind"] {
        std::fs::write(
            dir.path().join(format!(".agents/agents/{slug}.md")),
            format!("---\nname: {slug}\narchetype: {slug}\nscope: private\n---\nShipped {slug}."),
        )
        .unwrap();
        for fallback in ["agents-templates", "archetypes"] {
            std::fs::write(
                dir.path().join(format!("{fallback}/{slug}.md")),
                "Fallback.",
            )
            .unwrap();
        }
        let template = resolve_archetype_template(slug, None, None, dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(template.body, format!("Shipped {slug}."));
        assert_eq!(template.scope.as_deref(), Some("private"));
        std::fs::remove_file(dir.path().join(format!(".agents/agents/{slug}.md"))).unwrap();
        assert_eq!(
            resolve_archetype_template(slug, None, None, dir.path())
                .unwrap()
                .unwrap()
                .body,
            "Fallback."
        );
    }
    assert!(parse_archetype_markdown("---\nname: worker\nunknown: rejected\n---\nBody.").is_err());
}

#[test]
fn role_and_archetype_unify_on_canonical_template_slug_and_reject_unknown_roles() {
    let dir = tempfile::tempdir().unwrap();
    let templates_dir = dir.path().join(".agents/agents");
    std::fs::create_dir_all(&templates_dir).unwrap();
    for (slug, scope, body_text) in [
        ("director", "public", "Director template body."),
        ("engineer", "private", "Engineer template body."),
        ("assistant", "isolated", "Assistant template body."),
        ("code-reviewer", "siblings", "Reviewer template body."),
    ] {
        std::fs::write(
            templates_dir.join(format!("{slug}.md")),
            format!(
                "---\nname: {slug}\nscope: {scope}\nharness: spiron\nmodel: gpt-6.1-sol#high\ndescription: Default {slug}\n---\n{body_text}"
            ),
        )
        .unwrap();
    }
    std::fs::write(
        dir.path().join("subagent-defaults.json"),
        serde_json::json!({
            "default": {"mode": "custom", "harness": "muse", "model": "muse-ember-1.0"}
        })
        .to_string(),
    )
    .unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent_agent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "agy"))
        .unwrap();

    for (selector_key, selector_val, expected_slug, expected_scope, expected_body) in [
        (
            "role",
            "director",
            "director",
            "public",
            "Director template body.",
        ),
        (
            "archetype",
            "engineer",
            "engineer",
            "private",
            "Engineer template body.",
        ),
        (
            "role",
            "assistant",
            "assistant",
            "isolated",
            "Assistant template body.",
        ),
        (
            "role",
            "Code Reviewer",
            "code-reviewer",
            "siblings",
            "Reviewer template body.",
        ),
    ] {
        let mut req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
            "name": "sample-agent",
            selector_key: selector_val,
            "instructions": "Custom assignment instructions."
        }))
        .unwrap();
        apply_archetype_defaults(None, dir.path(), Some(&parent_agent), &mut req).unwrap();
        assert_eq!(req.role.as_deref(), Some(expected_slug));
        assert_eq!(req.scope.as_deref(), Some(expected_scope));
        assert_eq!(req.harness.as_deref(), Some("spiron"));
        assert_eq!(req.model.as_deref(), Some("gpt-6.1-sol#high"));
        assert_eq!(
            req.instructions.as_deref(),
            Some("Custom assignment instructions.")
        );
        let mut preview_agent = parent_agent.clone();
        preview_agent.role = req.role.clone();
        preview_agent.instructions = req.instructions.clone();
        preview_agent.workdir = Some(dir.path().display().to_string());
        assert_eq!(
            effective_agent_instructions(None, dir.path(), &preview_agent).as_deref(),
            Some(format!("{BUILTIN_GENERAL_ROLE_BODY}\n\n{expected_body}\n\nCustom assignment instructions.").as_str())
        );
    }

    let mut bad_req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "sample-agent",
        "role": "nonexistent-role",
    }))
    .unwrap();
    let err = apply_archetype_defaults(None, dir.path(), None, &mut bad_req)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unknown archetype template \"nonexistent-role\""),
        "{err}"
    );
    assert!(normalize_or_validate_agent_role("nonexistent-role", None, None, dir.path()).is_err());
    assert_eq!(
        normalize_or_validate_agent_role("Code Reviewer", None, None, dir.path()).unwrap(),
        "code-reviewer"
    );

    let cfg = agent_config_dir(dir.path(), &parent_agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    write_agent_instructions_with_continuity(&cfg, &parent_agent, None, None).unwrap();
    let prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(
        prompt.contains(
            "- Available sub-agent roles (`role` in `agents`/`spawn`; defaults to `general`): `general`, `assistant`, `code-reviewer`, `director`, `engineer` (see `spawn` tool `role` schema for details)."
        ),
        "{prompt}"
    );
}

#[test]
fn spawn_selection_prefers_caller_then_role_then_pure_parent_then_deployment() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let mut agent = NewAgent::new("parent", "agy");
    agent.model = Some("parent-model".into());
    let parent = store.create_agent(agent).unwrap();
    let config = agent_config_dir(data, &parent.id);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join(".anyagents-profile"), "google-antigravity/team").unwrap();
    std::fs::write(data.join("subagent-defaults.json"), serde_json::json!({
        "default": {"mode": "preset", "harness": "codex", "model": "deploy-model", "profile": "openai/team"},
        "agents": {"parent": {"mode": "preset", "harness": "spiron", "model": "ignored-model"}}
    }).to_string()).unwrap();
    let workdir = data.join("workspace");
    let roles = workdir.join(".agents/agents");
    std::fs::create_dir_all(&roles).unwrap();
    for (name, template) in [
        ("empty", ""),
        (
            "chosen",
            "harness: codex\nmodel: role-model\nprofile: openai/team\n",
        ),
        (
            "inherited",
            "harness: inherit\nmodel: inherit\nprofile: inherit\n",
        ),
        ("same-parent", "harness: inherit\nmodel: role-model\n"),
    ] {
        std::fs::write(
            roles.join(format!("{name}.md")),
            format!("---\nrole: {name}\n{template}---\nRole body.\n"),
        )
        .unwrap();
    }
    let resolve = |role: &str, args: serde_json::Value, parent: Option<&anyagents_core::Agent>| {
        let mut request: crate::api::CreateAgentRequest = serde_json::from_value(args).unwrap();
        request.role = Some(role.to_string());
        request.workdir = Some(workdir.display().to_string());
        apply_archetype_defaults(None, data, parent, &mut request).unwrap();
        assert!(
            request.harness.is_some(),
            "HTTP must forward absence instead of defaulting"
        );
        resolve_subagent_defaults(
            data,
            parent,
            request.harness.as_deref().unwrap(),
            request.model,
            request.profile,
        )
    };
    let args = || serde_json::json!({"name": "child"});
    let expected = |h: &str, m: Option<&str>, p: Option<&str>| {
        (h.to_string(), m.map(str::to_string), p.map(str::to_string))
    };
    assert_eq!(
        resolve("empty", args(), None),
        expected("codex", Some("deploy-model"), Some("openai/team"))
    );
    assert_eq!(
        resolve("empty", args(), Some(&parent)),
        expected("agy", Some("parent-model"), Some("google-antigravity/team"))
    );
    assert_eq!(
        resolve("chosen", args(), Some(&parent)),
        expected("codex", Some("role-model"), Some("openai/team"))
    );
    assert_eq!(
        resolve("inherited", args(), Some(&parent)),
        resolve("empty", args(), Some(&parent))
    );
    assert_eq!(resolve("inherited", args(), None), expected("", None, None));
    assert_eq!(
        resolve("same-parent", args(), Some(&parent)),
        expected("agy", Some("role-model"), Some("google-antigravity/team"))
    );
    assert_eq!(
        resolve(
            "chosen",
            serde_json::json!({"name":"child", "harness":"inherit", "model":"caller-model", "profile":"google-antigravity/caller"}),
            Some(&parent)
        ),
        expected(
            "agy",
            Some("caller-model"),
            Some("google-antigravity/caller")
        )
    );
    std::fs::remove_file(data.join("subagent-defaults.json")).unwrap();
    assert_eq!(resolve("empty", args(), None), expected("", None, None));
    assert_eq!(
        resolve(
            "empty",
            serde_json::json!({"name":"child", "harness":"codex"}),
            None
        ),
        expected("codex", None, None)
    );
}

#[test]
fn builtin_general_role_resolves_private_without_disk_file_and_respects_disk_override() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let ws = dir.path().join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&ws).unwrap();

    // No .agents/agents/general.md exists anywhere on disk.
    let tpl = resolve_archetype_template("general", Some(&ws), None, &data)
        .unwrap()
        .expect("built-in general role must resolve without disk file");
    assert_eq!(tpl.role.as_deref(), Some("general"));
    assert_eq!(tpl.scope.as_deref(), Some("private"));
    assert!(tpl.description.as_deref().is_some_and(|d| !d.is_empty()));
    assert!(!tpl.body.is_empty());
    assert_eq!(
        discover_available_archetype_slugs(Some(&ws), None, &data),
        vec!["general".to_string()]
    );

    // Root agent (`parent: None`): built-in `general` assigns role without forcing private scope.
    for role_val in [
        serde_json::Value::Null,
        serde_json::json!(""),
        serde_json::json!("   "),
        serde_json::json!("general"),
    ] {
        let mut req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
            "name": "default-root",
            "role": role_val,
            "workdir": ws,
        }))
        .unwrap();
        apply_archetype_defaults(None, &data, None, &mut req).unwrap();
        assert_eq!(req.role.as_deref(), Some("general"));
        assert!(req.scope.is_none());
        assert!(req.instructions.is_none());
    }

    // Explicit scope override on root is preserved.
    let mut private_root_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "private-root",
            "scope": "private",
            "workdir": ws,
        }))
        .unwrap();
    apply_archetype_defaults(None, &data, None, &mut private_root_req).unwrap();
    assert_eq!(private_root_req.role.as_deref(), Some("general"));
    assert_eq!(private_root_req.scope.as_deref(), Some("private"));

    // Direct `create()` also defaults omitted/empty role to `general`.
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    for (name, role) in [
        ("direct-none", None),
        ("direct-empty", Some("".to_string())),
    ] {
        let agent = create(
            &store,
            &data,
            CreateRequest {
                name: name.to_string(),
                role,
                harness: "agy".to_string(),
                runtime: None,
                budget_cap_micros: None,
                model: None,
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: Some(ws.clone()),
                workdir_default: None,
                description: None,
                instructions: None,
                parent_id: None,
            },
        )
        .unwrap();
        assert_eq!(agent.role.as_deref(), Some("general"));
        assert_eq!(agent.scope(), "public");
    }

    // Sub-agent (`parent: Some`): built-in `general` defaults scope to `private` unless overridden.
    let parent_agent = store.agent_by_name("direct-none").unwrap();
    let mut sub_req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "default-subagent",
        "workdir": ws,
    }))
    .unwrap();
    apply_archetype_defaults(Some(&store), &data, Some(&parent_agent), &mut sub_req).unwrap();
    assert_eq!(sub_req.role.as_deref(), Some("general"));
    assert_eq!(sub_req.scope.as_deref(), Some("private"));

    let mut public_sub_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "public-subagent",
            "scope": "public",
            "workdir": ws,
        }))
        .unwrap();
    apply_archetype_defaults(
        Some(&store),
        &data,
        Some(&parent_agent),
        &mut public_sub_req,
    )
    .unwrap();
    assert_eq!(public_sub_req.role.as_deref(), Some("general"));
    assert_eq!(public_sub_req.scope.as_deref(), Some("public"));

    // Explicit `public: true` / `public: false` without `scope` on a sub-agent stays intact.
    for public_flag in [true, false] {
        let mut bool_sub_req: crate::api::CreateAgentRequest =
            serde_json::from_value(serde_json::json!({
                "name": format!("bool-subagent-{public_flag}"),
                "public": public_flag,
                "workdir": ws,
            }))
            .unwrap();
        apply_archetype_defaults(Some(&store), &data, Some(&parent_agent), &mut bool_sub_req)
            .unwrap();
        assert_eq!(bool_sub_req.role.as_deref(), Some("general"));
        assert!(bool_sub_req.scope.is_none());
        assert_eq!(bool_sub_req.public, Some(public_flag));
    }

    // Caller `description` is preserved into `instructions` when `instructions` is omitted.
    let mut legacy_sub_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "legacy-subagent",
            "description": "Legacy subagent brief",
            "workdir": ws,
        }))
        .unwrap();
    apply_archetype_defaults(
        Some(&store),
        &data,
        Some(&parent_agent),
        &mut legacy_sub_req,
    )
    .unwrap();
    assert_eq!(legacy_sub_req.role.as_deref(), Some("general"));
    assert_eq!(
        legacy_sub_req.description.as_deref(),
        Some("Legacy subagent brief")
    );
    assert_eq!(
        legacy_sub_req.instructions.as_deref(),
        Some("Legacy subagent brief")
    );

    // `.agents/agents/general.md` on disk overrides the built-in template.
    std::fs::create_dir_all(ws.join(".agents/agents")).unwrap();
    std::fs::write(
        ws.join(".agents/agents/general.md"),
        "---\nname: general\nscope: siblings\ndescription: Workspace general override\n---\nWorkspace general body.",
    )
    .unwrap();
    let overridden = resolve_archetype_template("general", Some(&ws), None, &data)
        .unwrap()
        .unwrap();
    assert_eq!(overridden.scope.as_deref(), Some("siblings"));
    assert_eq!(
        overridden.description.as_deref(),
        Some("Workspace general override")
    );
    assert_eq!(overridden.body, "Workspace general body.");

    let mut override_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({
            "name": "overridden-worker",
            "workdir": ws,
        }))
        .unwrap();
    apply_archetype_defaults(None, &data, None, &mut override_req).unwrap();
    assert_eq!(override_req.role.as_deref(), Some("general"));
    assert_eq!(override_req.scope.as_deref(), Some("siblings"));
    assert!(override_req.instructions.is_none());

    // Removing disk override reverts cleanly to built-in general.
    std::fs::remove_file(ws.join(".agents/agents/general.md")).unwrap();
    let reverted = resolve_archetype_template("general", Some(&ws), None, &data)
        .unwrap()
        .unwrap();
    assert_eq!(reverted.scope.as_deref(), Some("private"));
}

#[test]
fn archetype_frontmatter_requires_valid_yaml_and_complete_delimiters() {
    for raw in [
        "---\ndescription: broken",
        "---\ndescription: broken\n---not-a-delimiter\nbody",
        "---\ndescription missing colon\n---\nbody",
        "---\ndescription: \"unclosed\n---\nbody",
        "---\ndescription: [unclosed\n---\nbody",
        "---\ndescription: {unclosed\n---\nbody",
        "---\n: missing key\n---\nbody",
        "---\ndescription:\n---\nbody",
        "---\ndescription: ''\n---\nbody",
        "---\ndescription: first\ndescription: second\n---\nbody",
        "---\n---",
        "---\n# comment only\n---\n",
    ] {
        let error = parse_archetype_markdown(raw).unwrap_err().to_string();
        assert!(error.contains("archetype template"), "{error}");
    }

    for raw in [
        "---\ndescription: 'Reviewer: #1'\n---\nReview this change.",
        "---\r\ndescription: 'Reviewer: #1'\r\n---\r\nReview this change.",
    ] {
        let template = parse_archetype_markdown(raw).unwrap();
        assert_eq!(template.description.as_deref(), Some("Reviewer: #1"));
        assert_eq!(template.body, "Review this change.");
    }
    let fields_only = parse_archetype_markdown("---\nrole: reviewer\n---").unwrap();
    assert_eq!(fields_only.role.as_deref(), Some("reviewer"));
    assert!(fields_only.body.is_empty());
    assert_eq!(
        parse_archetype_markdown("---\n---\nBody.").unwrap().body,
        "Body."
    );
    assert_eq!(
        parse_archetype_markdown("Plain markdown.").unwrap().body,
        "Plain markdown."
    );
}

#[test]
fn archetype_frontmatter_rejects_unknown_keys_and_invalid_scope() {
    for (raw, expected) in [
        (
            "---\nscpoe: isolated\n---\nUNKNOWN_KEY_BODY",
            "unknown archetype template frontmatter key \"scpoe\"",
        ),
        (
            "---\nscope: invalid-review-scope\n---\nINVALID_SCOPE_BODY",
            "invalid scope `invalid-review-scope`",
        ),
    ] {
        let error = parse_archetype_markdown(raw).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }
    let template = parse_archetype_markdown("---\nscope: team\n---\nBody.").unwrap();
    assert_eq!(template.scope.as_deref(), Some("siblings"));
}

#[cfg(unix)]
#[test]
fn archetype_nofollow_opens_refuse_symlinks_but_allow_os_aliases_above_base() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let real = root.join("real");
    let templates = real.join("work/.agents/agents");
    std::fs::create_dir_all(&templates).unwrap();
    std::fs::write(templates.join("writer.md"), "Trusted template.").unwrap();
    let alias = root.join("alias");
    symlink(&real, &alias).unwrap();
    let template =
        resolve_archetype_template("writer", Some(&alias.join("work")), None, dir.path())
            .unwrap()
            .unwrap();
    assert_eq!(template.body, "Trusted template.");

    // These use the descriptor-only open directly, so the race-resistant
    // layer is independently covered rather than masked by metadata checks.
    symlink(templates.join("writer.md"), templates.join("escape.md")).unwrap();
    assert_eq!(
        open_archetype_nofollow(&templates.join("escape.md"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ELOOP)
    );
    symlink(&templates, real.join("redirected")).unwrap();
    let error = open_archetype_nofollow(&real.join("redirected/writer.md")).unwrap_err();
    assert!(
        matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)),
        "{error}"
    );
}

#[test]
fn dynamic_role_ssot_updates_rendered_prompts_on_disk_edit_without_baked_sqlite_instructions() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let ws = dir.path().join("workspace");
    let roles_dir = ws.join(".agents/agents");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&roles_dir).unwrap();
    let role_file = roles_dir.join("specialist.md");
    std::fs::write(
        &role_file,
        "---\nname: specialist\ndescription: Initial specialist summary\nscope: siblings\n---\nRole SSOT v1 instructions.",
    )
    .unwrap();

    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();
    let mut req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "dyn-specialist",
        "role": "specialist",
        "harness": "spiron",
        "workdir": ws,
        "instructions": "Keep only custom task assignment."
    }))
    .unwrap();
    apply_archetype_defaults(None, &data, None, &mut req).unwrap();
    assert_eq!(
        req.instructions.as_deref(),
        Some("Keep only custom task assignment.")
    );

    let agent = create(
        &store,
        &data,
        CreateRequest {
            name: req.name,
            role: req.role,
            harness: "spiron".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(ws.clone()),
            workdir_default: None,
            description: req.description,
            instructions: req.instructions,
            parent_id: None,
        },
    )
    .unwrap();
    assert_eq!(
        agent.instructions.as_deref(),
        Some("Keep only custom task assignment.")
    );
    assert_eq!(
        effective_agent_instructions(Some(&store), &data, &agent).as_deref(),
        Some(format!("{BUILTIN_GENERAL_ROLE_BODY}\n\nRole SSOT v1 instructions.\n\nKeep only custom task assignment.").as_str())
    );
    let cfg = agent_config_dir(&data, &agent.id);
    let prompt_v1 = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(prompt_v1.contains("Role SSOT v1 instructions."));
    assert_eq!(prompt_v1.matches(BUILTIN_GENERAL_ROLE_BODY).count(), 1);
    // Old composed instructions do not duplicate either runtime baseline
    // or the current caller template when rendered again.
    let mut baked = agent.clone();
    baked.instructions = effective_agent_instructions(Some(&store), &data, &agent);
    assert_eq!(
        effective_agent_instructions(Some(&store), &data, &baked),
        baked.instructions
    );
    assert!(prompt_v1.contains("Keep only custom task assignment."));
    assert!(!cfg.join(INSTRUCTIONS_ROTATED_MARKER).exists());

    let mut empty_req: crate::api::CreateAgentRequest = serde_json::from_value(serde_json::json!({
        "name": "dyn-default-only",
        "role": "specialist",
        "harness": "claude-code",
        "workdir": ws,
    }))
    .unwrap();
    apply_archetype_defaults(None, &data, None, &mut empty_req).unwrap();
    let default_only = create(
        &store,
        &data,
        CreateRequest {
            name: empty_req.name,
            role: empty_req.role,
            harness: "claude-code".to_string(),
            runtime: None,
            budget_cap_micros: None,
            model: None,
            profile: None,
            repo: None,
            branch: None,
            base: None,
            workdir: Some(ws.clone()),
            workdir_default: None,
            description: empty_req.description,
            instructions: empty_req.instructions,
            parent_id: None,
        },
    )
    .unwrap();
    assert!(
        default_only.instructions.is_none(),
        "template description/body must not leak into sqlite instructions: {:?}",
        default_only.instructions
    );

    let running = store
        .set_agent_running(&agent.id, &ws.display().to_string())
        .unwrap();
    assert_eq!(running.state, "running");
    std::fs::write(
        &role_file,
        "---\nname: specialist\ndescription: Initial specialist summary\nscope: siblings\n---\nRole SSOT v2 live reload.",
    )
    .unwrap();
    sync_all_agent_instructions(&store, &data).unwrap();

    let prompt_v2 = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(prompt_v2.contains("Role SSOT v2 live reload."));
    assert!(!prompt_v2.contains("Role SSOT v1 instructions."));
    assert!(prompt_v2.contains("Keep only custom task assignment."));
    let agents_md_v2 = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
    assert!(agents_md_v2.contains("Role SSOT v2 live reload."));
    assert!(
        cfg.join(INSTRUCTIONS_ROTATED_MARKER).is_file(),
        "spiron running agent must get instruction-rotation marker on prompt drift"
    );
    let first_marker = std::fs::read_to_string(cfg.join(INSTRUCTIONS_ROTATED_MARKER)).unwrap();

    sync_all_agent_instructions(&store, &data).unwrap();
    assert_eq!(
        std::fs::read_to_string(cfg.join(INSTRUCTIONS_ROTATED_MARKER)).unwrap(),
        first_marker
    );
}

#[test]
fn backfill_historical_agent_roles_losslessly_normalizes_roles_and_strips_baked_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let ws = dir.path().join("workspace");
    let roles_dir = ws.join(".agents/agents");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&roles_dir).unwrap();
    std::fs::write(
        roles_dir.join("code-reviewer.md"),
        "---\nname: code-reviewer\ndescription: Code reviewer\n---\nReview carefully.",
    )
    .unwrap();

    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("0123456789abcdef0123456789abcdef")
        .unwrap();

    let mut null_role = NewAgent::new("hist-null", "claude-code");
    null_role.role = None;
    null_role.description = Some("Preserve historical desc".into());
    null_role.instructions = Some(format!(
        "{BUILTIN_GENERAL_ROLE_BODY}\n\nKeep this historical custom prompt."
    ));
    null_role.workdir = Some(ws.display().to_string());
    let a_null = store.create_agent(null_role).unwrap();

    let mut baked_only = NewAgent::new("hist-blank", "claude-code");
    baked_only.role = Some("   ".into());
    baked_only.instructions = Some(BUILTIN_GENERAL_ROLE_BODY.to_string());
    baked_only.workdir = Some(ws.display().to_string());
    let a_blank = store.create_agent(baked_only).unwrap();

    let mut mixed_case = NewAgent::new("hist-mixed", "claude-code");
    mixed_case.role = Some("Code Reviewer".into());
    mixed_case.instructions = Some("Review carefully.\n\nCheck PR #123.".into());
    mixed_case.workdir = Some(ws.display().to_string());
    let a_mixed = store.create_agent(mixed_case).unwrap();

    let mut retired = NewAgent::new("hist-retired", "claude-code");
    retired.role = Some("retired_legacy_role".into());
    retired.instructions = Some("Explicit task stays intact.".into());
    retired.workdir = Some(ws.display().to_string());
    let a_retired = store.create_agent(retired).unwrap();

    let cfg = agent_config_dir(&data, &a_mixed.id);
    std::fs::create_dir_all(&cfg).unwrap();

    let changed = backfill_historical_agent_roles(&store, &data).unwrap();
    assert_eq!(changed, 4);

    let r_null = store.agent(&a_null.id).unwrap();
    assert_eq!(r_null.role.as_deref(), Some("general"));
    assert_eq!(
        r_null.description.as_deref(),
        Some("Preserve historical desc")
    );
    assert_eq!(
        r_null.instructions.as_deref(),
        Some("Keep this historical custom prompt.")
    );
    assert_eq!(
        effective_agent_instructions(Some(&store), &data, &r_null).as_deref(),
        Some(
            format!("{BUILTIN_GENERAL_ROLE_BODY}\n\nKeep this historical custom prompt.").as_str()
        )
    );

    let r_blank = store.agent(&a_blank.id).unwrap();
    assert_eq!(r_blank.role.as_deref(), Some("general"));
    assert!(r_blank.instructions.is_none());

    let r_mixed = store.agent(&a_mixed.id).unwrap();
    assert_eq!(r_mixed.role.as_deref(), Some("code-reviewer"));
    assert_eq!(r_mixed.instructions.as_deref(), Some("Check PR #123."));
    let rendered_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(rendered_prompt.contains("Review carefully."));
    assert!(rendered_prompt.contains("Check PR #123."));

    let r_retired = store.agent(&a_retired.id).unwrap();
    assert_eq!(r_retired.role.as_deref(), Some("general"));
    assert_eq!(
        r_retired.instructions.as_deref(),
        Some("Explicit task stays intact.")
    );

    assert_eq!(backfill_historical_agent_roles(&store, &data).unwrap(), 0);
}

#[test]
fn hydrated_roles_dir_injects_template_verbatim_and_unknown_explicit_role_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let store = Store::open(data.join("state.db")).unwrap();

    // Even with zero disk templates, an unknown explicit role must fail closed
    // listing available roles rather than silently falling back to `general`.
    let mut missing_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({"name": "bad-role-agent", "role": "auditor"}))
            .unwrap();
    let err = apply_archetype_defaults(Some(&store), &data, None, &mut missing_req)
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown role 'auditor'"), "{err}");
    assert!(err.contains("available roles: [general]"), "{err}");

    // Hydrating `<data>/roles/auditor.md` makes `auditor` discoverable and
    // injects its markdown body verbatim into effective agent instructions.
    std::fs::create_dir_all(data.join("roles")).unwrap();
    std::fs::write(
        data.join("roles/auditor.md"),
        "---\nname: auditor\ndescription: Security auditor\nscope: private\n---\nAudit every change against policy invariants verbatim.\n",
    )
    .unwrap();
    assert_eq!(
        discover_available_archetype_slugs(None, None, &data),
        vec!["general".to_string(), "auditor".to_string()]
    );

    let mut ok_req: crate::api::CreateAgentRequest =
        serde_json::from_value(serde_json::json!({"name": "auditor-agent", "role": "auditor"}))
            .unwrap();
    apply_archetype_defaults(Some(&store), &data, None, &mut ok_req).unwrap();
    assert_eq!(ok_req.role.as_deref(), Some("auditor"));
    assert_eq!(ok_req.description.as_deref(), Some("Security auditor"));

    let mut preview = store
        .create_agent(NewAgent::new("auditor-preview", "codex"))
        .unwrap();
    preview.role = ok_req.role.clone();
    preview.instructions = ok_req.instructions.clone();
    let effective = effective_agent_instructions(Some(&store), &data, &preview).unwrap();
    assert!(
        effective.contains("Audit every change against policy invariants verbatim."),
        "{effective}"
    );
}
