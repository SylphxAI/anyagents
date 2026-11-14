//! The `code` tool's contract end to end: time limits and the errors a
//! wrong snippet gets. Each test runs the real sandbox under small limits set
//! for its own thread.

use super::*;
use serde_json::json;

/// An agent with no server behind it, under `limits` on this test thread:
/// these snippets only touch the workspace.
fn agent_with_limits(dir: &Path, limits: Option<CodeLimits>) -> AgentMcp {
    CodeLimits::set_for_test_thread(limits);
    let workdir = dir.join("work");
    std::fs::create_dir_all(&workdir).unwrap();
    let config = dir.join("agents").join("agt_test").join("config");
    std::fs::create_dir_all(&config).unwrap();
    AgentMcp {
        agent: AgentRef {
            id: AgentId::generate(),
            name: "contract-worker".into(),
            workdir: Some(workdir),
        },
        client: Client::new(
            dir.join("anyagents.sock"),
            config,
            "synthetic-contract-capability".into(),
        ),
        profile: McpProfile::Full,
    }
}

fn limits(default_ms: u64, max_ms: u64) -> Option<CodeLimits> {
    Some(CodeLimits { default_ms, max_ms })
}

async fn run(
    mcp: &AgentMcp,
    code: &str,
    timeout_ms: Option<u64>,
) -> Result<serde_json::Value, String> {
    mcp.code(Parameters(CodeArgs {
        code: code.into(),
        timeout_ms,
    }))
    .await
    .map(|MultimediaJson(value, _)| value)
}

#[tokio::test]
async fn a_command_with_its_own_timeout_stretches_the_call_and_one_without_inherits_it() {
    // Limits leave a slow CI runner seconds to start the sandbox before the
    // snippet's first call; each case still needs more than the default.
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), limits(4000, 15_000));

    // The call's default is 4s; the command asks for 8s and the call
    // stays open for it.
    let stretched = run(
        &mcp,
        "const r = await workspace.exec({cmd: 'sleep 4.5; echo stretched', timeout_ms: 8000}); return r.stdout;",
        None,
    )
    .await
    .unwrap();
    assert_eq!(stretched["result"], "stretched\n", "{stretched}");

    // The call has 12s; a command naming no timeout gets what is left of
    // it, not the 4s default.
    let inherited = run(
        &mcp,
        "const r = await workspace.exec({cmd: 'sleep 4.5; echo inherited'}); return r.stdout;",
        Some(12_000),
    )
    .await
    .unwrap();
    assert_eq!(inherited["result"], "inherited\n", "{inherited}");

    // A command that outlives the time it was given says how to give it
    // more, before the call itself runs out.
    let timed_out = run(&mcp, "return await workspace.exec('sleep 20');", Some(7000))
        .await
        .unwrap_err();
    assert!(
        timed_out.contains("workspace.exec timed out"),
        "{timed_out}"
    );
    assert!(timed_out.contains("up to 15000"), "{timed_out}");
    assert!(timed_out.contains("on its `code` call"), "{timed_out}");
    assert!(!timed_out.contains("background"), "{timed_out}");
}

#[tokio::test]
async fn a_call_that_runs_out_of_time_says_how_to_get_more() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), limits(500, 5000));
    let err = run(
        &mcp,
        "await new Promise((resolve) => setTimeout(resolve, 3000)); return 1;",
        None,
    )
    .await
    .unwrap_err();
    assert!(err.contains("timed out after 500ms"), "{err}");
    assert!(err.contains("`timeout_ms` (up to 5000)"), "{err}");
    assert!(!err.contains("workspace.job"), "{err}");

    let err = run(
        &mcp,
        "await new Promise((resolve) => setTimeout(resolve, 9000)); return 1;",
        Some(60_000),
    )
    .await
    .unwrap_err();
    assert!(err.contains("timed out after 5000ms"), "{err}");
    assert!(err.contains("asked 60000ms"), "{err}");
}

#[tokio::test]
async fn a_command_asking_past_the_ceiling_runs_clamped_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), limits(4000, 15_000));
    // Through `code`: the command runs with what the call may still give it,
    // and the result keeps its shape with a note.
    let clamped = run(
        &mcp,
        "return await workspace.exec({cmd: 'echo clamped', timeout_ms: 3600000});",
        None,
    )
    .await
    .unwrap();
    let result = &clamped["result"];
    assert_eq!(result["stdout"], "clamped\n", "{clamped}");
    assert_eq!(result["exit_code"], 0, "{clamped}");
    let note = result["note"].as_str().unwrap();
    assert!(
        note.starts_with("asked timeout_ms 3600000 but ran with"),
        "{note}"
    );
    assert!(note.contains("at most 15000ms (the ceiling)"), "{note}");
    // Directly through `files`, and a command within the ceiling has no note.
    let direct = mcp
        .files(Parameters(FilesArgs {
            action: Some("exec".into()),
            cmd: Some("echo direct".into()),
            timeout_ms: Some(3_600_000),
            ..FilesArgs::default()
        }))
        .await
        .unwrap()
        .0;
    assert_eq!(direct["stdout"], "direct\n");
    assert_eq!(
        direct["note"],
        "asked timeout_ms 3600000 but ran with 15000ms: one call, start to end, may run at most 15000ms (the ceiling)"
    );
    let within = mcp
        .files(Parameters(FilesArgs {
            action: Some("exec".into()),
            cmd: Some("echo within".into()),
            timeout_ms: Some(5000),
            ..FilesArgs::default()
        }))
        .await
        .unwrap()
        .0;
    assert!(within.get("note").is_none(), "{within}");
    for key in [
        "action",
        "cmd",
        "cwd",
        "exit_code",
        "stdout",
        "stderr",
        "duration_ms",
    ] {
        assert!(
            within.get(key).is_some() && direct.get(key).is_some(),
            "{key}"
        );
    }
}

#[tokio::test]
async fn wrong_snippets_name_their_fix() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), None);

    let esm = run(
        &mcp,
        "import fs from \"fs\";\nreturn fs.readFileSync('x');",
        None,
    )
    .await
    .unwrap_err();
    assert!(
        esm.contains("fix: `import`/`export` cannot be used"),
        "{esm}"
    );
    assert!(!esm.contains("    at "), "host stack frames leak: {esm}");

    let node = run(
        &mcp,
        "const fs = require('fs');\nreturn fs.readdirSync('.');",
        None,
    )
    .await
    .unwrap_err();
    assert!(node.contains("require is not defined"), "{node}");
    assert!(
        node.contains("fix: `require` and Node modules are unavailable"),
        "{node}"
    );

    let guessed = run(&mcp, "return await anyagents.mailbox({});", None)
        .await
        .unwrap_err();
    assert!(
        guessed.contains("fix: `anyagents.mailbox` does not exist"),
        "{guessed}"
    );
    assert!(guessed.contains("inbox"), "{guessed}");
}

#[tokio::test]
async fn the_names_agents_reach_for_exist() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), None);
    let kinds = run(
        &mcp,
        "return [anyagents.inbox, anyagents.schedules, anyagents.skills, tools.anyagents.schedules].map((f) => typeof f);",
        None,
    )
    .await
    .unwrap();
    assert_eq!(kinds["result"], json!(vec!["function"; 4]), "{kinds}");
    let no_jobs = run(&mcp, "return typeof workspace.job;", None)
        .await
        .unwrap();
    assert_eq!(no_jobs["result"], "undefined");

    // A snippet may name its own `tools` or `web`: the sandbox's are
    // parameters, and the snippet's own declarations shadow them.
    let shadowed = run(
        &mcp,
        "const tools = await workspace.exec('echo shadow');\nconst web = tools.stdout.trim();\nreturn web;",
        None,
    )
    .await
    .unwrap();
    assert_eq!(shadowed["result"], "shadow", "{shadowed}");

    // `exec(cmd, opts)` reads the options.
    std::fs::create_dir_all(dir.path().join("work").join("sub")).unwrap();
    let with_opts = run(
        &mcp,
        "return (await workspace.exec('pwd', {cwd: 'sub'})).stdout.trim();",
        None,
    )
    .await
    .unwrap();
    assert!(
        with_opts["result"].as_str().unwrap().ends_with("/sub"),
        "{with_opts}"
    );
}

#[test]
fn every_listed_sandbox_name_is_bound_and_described() {
    for (namespace, names) in code_contract::SANDBOX_API {
        for name in *names {
            let bound = match *namespace {
                "workspace" => format!("          {name}: "),
                "anyagents" => format!("          {name}: "),
                "memory" => format!("memoryFn.{name} = "),
                "web" => format!("webFn.{name} = "),
                _ => name.to_string(),
            };
            assert!(
                CODE_SANDBOX_WORKER_JS.contains(&bound) || *namespace == "tools",
                "`{namespace}.{name}` is listed but not bound in the sandbox"
            );
        }
    }
    for name in [
        "inbox",
        "schedule",
        "skills",
        "send",
        "agents",
        "conversation",
        "groups",
        "browser",
        "interact",
    ] {
        assert!(CODE_TOOL_DESCRIPTION.contains(name), "{name}");
    }
    for call in [
        "read(",
        "write(",
        "edit(",
        "multi_edit(",
        "patch(",
        "glob(",
        "grep(",
        "list(",
        "exec(",
    ] {
        assert!(CODE_TOOL_DESCRIPTION.contains(call), "{call}");
    }
    for gone in ["background", "job(", "workspace.job"] {
        assert!(!CODE_TOOL_DESCRIPTION.contains(gone), "{gone}");
    }
}

#[test]
fn the_described_limits_are_the_configured_ones_for_each_backend() {
    assert!(CODE_TOOL_DESCRIPTION.contains(CODE_TOOL_LIMITS_TEXT));
    let default = CodeLimits::default();
    assert_eq!(
        CODE_TOOL_LIMITS_TEXT,
        format!("default {}, max {}", default.default_ms, default.max_ms)
    );
    let configured = CodeLimits {
        default_ms: 45_000,
        max_ms: 900_000,
    };
    let local = code_tool_description(configured, "local");
    assert!(
        local.contains("`timeout_ms`: default 45000, max 900000."),
        "{local}"
    );
    assert!(!local.contains("remote sandbox"), "{local}");
    assert_eq!(
        code_tool_description(default, "local"),
        CODE_TOOL_DESCRIPTION
    );
    for remote in ["cloud-sandbox", "e2b"] {
        let text = code_tool_description(configured, remote);
        assert!(
            text.contains("default 45000, max 900000; `workspace.exec` runs in your remote sandbox, which stops a command at 120000"),
            "{text}"
        );
    }
    let docker = code_tool_description(configured, "docker");
    assert!(
        docker.contains("does not stop a command at its `timeout_ms`"),
        "{docker}"
    );
}

#[tokio::test]
async fn the_listed_code_tool_carries_the_limits_in_force() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = agent_with_limits(dir.path(), limits(45_000, 900_000));
    let tools = mcp.listed_tools();
    let code = tools.iter().find(|t| t.name == "code").unwrap();
    assert!(
        code.description
            .as_deref()
            .unwrap()
            .contains("default 45000, max 900000."),
        "{:?}",
        code.description
    );
}

#[test]
fn a_code_call_budgets_the_commands_it_runs() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let elapsed = std::time::Duration::from_millis(1_000);
    // No timeout: the command gets what is left, less the grace.
    let deadline = AtomicU64::new(30_000);
    let args = budget_code_call(
        "files",
        json!({"action": "exec", "cmd": "make"}),
        elapsed,
        &deadline,
        600_000,
    );
    assert_eq!(args["timeout_ms"], 27_000);
    assert_eq!(deadline.load(Ordering::SeqCst), 30_000);
    // Its own timeout moves the call's end out.
    let args = budget_code_call(
        "files",
        json!({"action": "exec", "cmd": "make", "timeout": 120_000}),
        elapsed,
        &deadline,
        600_000,
    );
    assert_eq!(args["timeout"], 120_000);
    assert_eq!(deadline.load(Ordering::SeqCst), 123_000);
    // Past the ceiling's room, the command is cut to fit.
    let deadline = AtomicU64::new(30_000);
    let args = budget_code_call(
        "files",
        json!({"action": "exec", "cmd": "make", "timeout_ms": 599_500}),
        elapsed,
        &deadline,
        600_000,
    );
    assert_eq!(args["timeout_ms"], 597_000);
    assert_eq!(deadline.load(Ordering::SeqCst), 600_000);
    assert_eq!(args["requested_timeout_ms"], 599_500);
    // A command beyond the ceiling is clamped the same way, never left to
    // outlive its call.
    let deadline = AtomicU64::new(30_000);
    let args = budget_code_call(
        "files",
        json!({"action": "exec", "cmd": "make", "timeout_ms": 900_000}),
        elapsed,
        &deadline,
        600_000,
    );
    assert_eq!(args["timeout_ms"], 597_000);
    assert_eq!(args["requested_timeout_ms"], 900_000);
    assert_eq!(deadline.load(Ordering::SeqCst), 600_000);
    // A `files` call naming no action but a `cmd` is an exec too.
    let deadline = AtomicU64::new(30_000);
    let args = budget_code_call("files", json!({"cmd": "make"}), elapsed, &deadline, 600_000);
    assert_eq!(args["timeout_ms"], 27_000);
    // Other actions and tools are untouched.
    for (tool, call) in [
        ("files", json!({"action": "read", "path": "x"})),
        ("send", json!({"to": "parent", "text": "x"})),
    ] {
        let deadline = AtomicU64::new(30_000);
        assert_eq!(
            budget_code_call(tool, call.clone(), elapsed, &deadline, 600_000),
            call
        );
        assert_eq!(deadline.load(Ordering::SeqCst), 30_000);
    }
}

#[test]
fn send_reads_null_placeholders_as_absent() {
    let args: SendArgs = serde_json::from_value(json!({
        "action": "send",
        "article": "null",
        "checklist": "null",
        "emoji": "null",
        "idempotency_key": "null",
        "location": "null",
        "media": "null",
        "options": "null",
        "poll": "null",
        "quote": "null",
        "reply_to": "null",
        "target": "null",
        "text": "report",
        "to": ["agent:planner", "null"],
    }))
    .unwrap();
    assert!(args.article.is_none() && args.media.is_none() && args.poll.is_none());
    assert!(args.location.is_none() && args.checklist.is_none());
    assert!(args.reply_to.is_none() && args.target.is_none() && args.quote.is_none());
    assert!(args.idempotency_key.is_none());
    assert!(args.options.is_empty());
    assert_eq!(args.to, vec!["agent:planner".to_string()]);
    // `emoji: ""` clears a reaction; only the placeholder words are absent.
    assert!(args.emoji.is_none());
    let clear: SendArgs =
        serde_json::from_value(json!({"action": "react", "emoji": "", "target": "msg_1"})).unwrap();
    assert_eq!(clear.emoji.as_deref(), Some(""));
    // Nested placeholders too, and a real article still parses.
    let article: SendArgs = serde_json::from_value(json!({
        "to": "planner",
        "article": {"title": "Report", "content": "body", "url": "undefined", "author_name": ""},
    }))
    .unwrap();
    let article = article.article.unwrap();
    assert_eq!(article.content.as_deref(), Some("body"));
    assert!(article.url.is_none());
    // Only `null` and `"null"`/`"undefined"` are placeholders: a poll option
    // named "None", an empty author and other look-alikes keep their value.
    assert_eq!(article.author_name.as_deref(), Some(""));
    let vote: SendArgs = serde_json::from_value(json!({
        "action": "vote",
        "target": "msg_1",
        "options": ["None", "nil", "Null", "NULL"],
        "quote": "None",
        "reply_to": null,
    }))
    .unwrap();
    assert_eq!(vote.options, vec!["None", "nil", "Null", "NULL"]);
    assert_eq!(vote.quote.as_deref(), Some("None"));
    assert!(vote.reply_to.is_none());
}

#[test]
fn relation_words_read_alike_in_any_case() {
    assert_eq!(relation_word("Parent"), Some("parent"));
    assert_eq!(relation_word(" agent:parent "), Some("parent"));
    assert_eq!(relation_word("children"), Some("children"));
    assert_eq!(relation_word("planner"), None);
    assert_eq!(relation_word("agent:parent-of-x"), None);
}
