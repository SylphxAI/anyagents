//! Executable protocol probe: no model, network, or production state is used.
use super::*;
use anyagents_core::{Incoming, NewAgent, NewSchedule, ReceiveOutcome, Store};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};

struct Environment(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl Environment {
    fn set(values: &[(&'static str, &std::ffi::OsStr)]) -> Self {
        Self(
            values
                .iter()
                .map(|(key, value)| {
                    let before = std::env::var_os(key);
                    // All vendor/server fixtures serialize environment changes.
                    unsafe { std::env::set_var(key, value) };
                    (*key, before)
                })
                .collect(),
        )
    }
}
impl Drop for Environment {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}
#[track_caller]
fn eventually(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check() {
        assert!(
            Instant::now() < deadline,
            "external protocol probe timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn message(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.into(),
        seq: 1,
        text: format!("body-{id}"),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    }
}

#[test]
fn spiron_external_spawn_steer_cancel_and_wake_preserve_legacy_state() {
    let _lock = crate::server::tests::env_lock();
    let temp = crate::test_tempdir();
    let probe = temp.path().join("spiron-probe");
    std::fs::write(&probe, r#"#!/usr/bin/env python3
import sys, os, json, pathlib, time
root = pathlib.Path(os.environ['ANYAGENTS_AGENT_CONFIG'])
args = sys.argv[1:]
with (root/'launches.jsonl').open('a') as out:
    out.write(json.dumps({'args':args, 'cwd':os.getcwd(), 'home':os.environ['HOME'], 'instructions':pathlib.Path(args[args.index('--instructions-file')+1]).read_text()})+'\n')
session = args[args.index('--session')+1] if '--session' in args else 'fresh-session'
def emit(kind, id=None, **fields):
    print(json.dumps(dict(type=kind, session_id=session, request_id=id, **fields)), flush=True)
emit('ready')
active = None
for line in sys.stdin:
    msg=json.loads(line)
    with (root/'inputs.jsonl').open('a') as out: out.write(json.dumps(msg)+'\n')
    id=msg.get('request_id')
    if msg['type']=='cancel':
        emit('cancel_ack', turn_id=active, status='cancelling')
        (root/'cancel-ack').touch()
        while not (root/'cancel-complete').exists(): time.sleep(.005)
        emit('result', active, status='cancelled', usage={'input_tokens':11,'output_tokens':7,'cache_read_input_tokens':3,'cache_creation_input_tokens':2})
        active=None
    elif active:
        emit('steer_ack', id, accepted=True, turn_id=active)
        (root/'steer-ack').touch()
        emit('result', 'unrelated-request', status='completed', usage={'input_tokens':999})
    else:
        active=id
        if id=='first':
            (root/'awaiting-ack').touch()
            while not (root/'accept').exists(): time.sleep(.005)
        emit('accepted', id, mode='turn')
        if id=='wake':
            emit('text_delta', id, text='awake')
            emit('result', id, status='completed', usage={'input_tokens':5,'output_tokens':1})
            active=None
        elif id=='failure':
            emit('result', id, status='failed', is_error=True, error='synthetic provider refusal', usage={'input_tokens':13,'output_tokens':2})
            active=None
"#).unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _env = Environment::set(&[
        ("ANYAGENTS_SPIRON_BIN", probe.as_os_str()),
        ("ANYAGENTS_FAKE_MODE", std::ffi::OsStr::new("1")),
    ]);
    for harness_name in [
        "spiron",
        "sylphx",
        "sylphx-agent",
        "sylphx_agent",
        "sylphx/default",
    ] {
        let data = temp.path().join(harness_name.replace('/', "-"));
        std::fs::create_dir_all(&data).unwrap();
        let store = Store::open(data.join("state.db")).unwrap();
        std::fs::create_dir_all(data.join("memory")).unwrap();
        std::fs::write(data.join("memory/MEMORY.md"), "PINNED_EXTERNAL_FACT").unwrap();
        let config = data.join("config");
        let work = data.join("work");
        std::fs::create_dir_all(config.join(".agents/skills")).unwrap();
        let real_work = data.join("work-real");
        std::fs::create_dir_all(&real_work).unwrap();
        std::os::unix::fs::symlink(&real_work, &work).unwrap();
        let mut spec = NewAgent::new("protocol-probe", harness_name);
        spec.model = Some("sylphx/openai/gpt-6.1-sol#xhigh".into());
        spec.workdir = Some(work.display().to_string());
        spec.instructions = Some("preserve this identity".into());
        let agent = store.create_agent(spec).unwrap();
        assert_eq!(agent.harness, "spiron", "{harness_name}");
        let agent = store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
        store
            .receive(&agent.id, Incoming::new("pending mail"))
            .unwrap();
        store
            .record_outbound(&agent.id, "parent", None, "retained reply")
            .unwrap();
        let schedule = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "2099-01-01T00:00:00Z".into(),
                prompt: "keep schedule".into(),
                interval_ms: None,
                next_fire_at_ms: Some(4_070_908_800_000),
                watch_baseline: None,
            })
            .unwrap();
        std::fs::write(config.join("sylphx-session-id"), "legacy-session\n").unwrap();
        let legacy = b"{\"type\":\"user\",\"text\":\"unique original history\"}\n";
        std::fs::write(config.join("sylphx-transcript.jsonl"), legacy).unwrap();
        std::fs::write(config.join("SYSTEM_PROMPT.md"), "preserve this identity").unwrap();
        crate::spiron::write_spiron_config(
            &config,
            agent.model.as_deref(),
            Path::new("/probe/anyagents"),
            &["mcp".into(), "--agent".into(), agent.id.to_string()],
        )
        .unwrap();
        let retained = [
            "sylphx-window.json",
            "sylphx-goal.json",
            "sylphx-cloud-session.json",
        ];
        for file in retained {
            std::fs::write(config.join(file), format!("original-{file}")).unwrap();
        }
        let before =
            serde_json::to_value(store.conversation(&agent.id, None, None, None).unwrap()).unwrap();
        let inbox = store.inbox(&agent.id, None).unwrap();
        let request = || LaunchRequest {
            agent: agent.clone(),
            data_dir: data.clone(),
            config_dir: config.clone(),
            work_dir: work.clone(),
            extra_env: vec![],
        };
        let mut vendor = vendor_launch(request()).unwrap();
        assert!(matches!(vendor, VendorHarness::Spiron(..)));
        let host_pin = anyagents_core::host_session::active(&data)
            .unwrap()
            .unwrap();
        assert_eq!(host_pin.harness, "spiron");
        assert!(host_pin.memory.section.contains("PINNED_EXTERNAL_FACT"));
        std::fs::write(data.join("memory/MEMORY.md"), "EDITED_LATER_FACT").unwrap();
        assert_eq!(
            anyagents_core::host_session::bound_vendor(&data, "spiron", "legacy-session")
                .unwrap()
                .unwrap(),
            host_pin,
        );
        assert_eq!(
            vendor.deliver(&message("first")).unwrap(),
            DeliverOutcome::Deferred
        );
        eventually(|| config.join("awaiting-ack").exists());
        assert_eq!(
            vendor.deliver(&message("first")).unwrap(),
            DeliverOutcome::Deferred
        );
        std::fs::write(config.join("accept"), "").unwrap();
        eventually(|| {
            matches!(
                vendor.deliver(&message("first")),
                Ok(DeliverOutcome::Accepted {
                    turn_finished: false
                })
            )
        });
        // A peer whose ready names no consumption proof is given nothing
        // while its turn runs.
        assert_eq!(
            vendor.deliver(&message("steer")).unwrap(),
            DeliverOutcome::Deferred
        );
        vendor.cancel().unwrap();
        eventually(|| config.join("cancel-ack").exists());
        assert_eq!(vendor.turns_served(), 0, "cancel_ack is not completion");
        std::fs::write(config.join("cancel-complete"), "").unwrap();
        eventually(|| vendor.turns_served() == 1);
        eventually(|| {
            matches!(
                vendor.deliver(&message("steer")),
                Ok(DeliverOutcome::Accepted {
                    turn_finished: false
                })
            )
        });
        assert!(!config.join("spiron-unconfirmed-steers.json").exists());
        assert_eq!(vendor.served_usage().input_tokens, 11);
        assert_eq!(vendor.served_usage().cache_read_tokens, 3);
        assert_eq!(vendor.served_usage().cache_write_tokens, 2);
        drop(vendor);
        let mut vendor = vendor_launch(request()).unwrap();
        eventually(|| {
            matches!(
                vendor.deliver(&message("wake")),
                Ok(DeliverOutcome::Accepted {
                    turn_finished: true
                })
            )
        });
        eventually(|| {
            matches!(
                vendor.deliver(&message("failure")),
                Ok(DeliverOutcome::Accepted {
                    turn_finished: true
                })
            )
        });
        assert_eq!(
            vendor.health().turn_state,
            anyagents_harness::TurnState::Idle
        );
        assert_eq!(
            vendor.health().sign_in_fault.as_deref(),
            Some("synthetic provider refusal"),
            "failed terminal result retains its diagnostic"
        );
        assert_eq!(vendor.turns_served(), 2);
        assert_eq!(vendor.served_usage().input_tokens, 18);
        let launches: Vec<serde_json::Value> =
            std::fs::read_to_string(config.join("launches.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        assert_eq!(launches.len(), 2);
        assert_eq!(
            anyagents_core::host_session::active(&data)
                .unwrap()
                .unwrap(),
            host_pin
        );
        for launch in launches {
            let instructions = launch["instructions"].as_str().unwrap();
            assert!(instructions.contains(&host_pin.memory.section));
            assert!(!instructions.contains("EDITED_LATER_FACT"));
            // getcwd resolves directory aliases (including macOS /var ->
            // /private/var). CLI arguments must retain the configured path,
            // while the child's effective directory has filesystem identity.
            assert_eq!(
                launch["cwd"],
                work.canonicalize().unwrap().display().to_string()
            );
            assert_eq!(launch["home"], config.display().to_string());
            let args: Vec<&str> = launch["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            for (flag, expected) in [
                ("--model", agent.model.clone().unwrap()),
                ("--session", "legacy-session".into()),
                (
                    "--mcp-config",
                    config.join("spiron-mcp.json").display().to_string(),
                ),
                ("--workdir", work.display().to_string()),
                (
                    "--skills-dir",
                    config.join(".agents/skills").display().to_string(),
                ),
                ("--state-dir", config.display().to_string()),
                (
                    "--instructions-file",
                    config.join("SYSTEM_PROMPT.md").display().to_string(),
                ),
                ("--agent-id", agent.id.to_string()),
                ("--agent-name", agent.name.clone()),
            ] {
                assert_eq!(
                    args[args.iter().position(|arg| *arg == flag).unwrap() + 1],
                    expected
                );
            }
            assert!(args.contains(&"--headless"));
            assert!(
                !args.contains(&"--profile"),
                "coordinator profile selectors are not TOML files"
            );
        }
        for file in retained {
            assert_eq!(
                std::fs::read_to_string(config.join(file)).unwrap(),
                format!("original-{file}")
            );
        }
        let inputs: Vec<serde_json::Value> = std::fs::read_to_string(config.join("inputs.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            inputs.len(),
            5,
            "each mailbox request crosses stdin only once"
        );
        assert_eq!(inputs[0]["request_id"], "first");
        assert_eq!(inputs[1]["type"], "cancel");
        assert_eq!(inputs[2]["request_id"], "steer");
        assert!(inputs[2]["text"].as_str().unwrap().contains("body-steer"));
        assert_eq!(inputs[3]["request_id"], "wake");
        assert_eq!(inputs[4]["request_id"], "failure");
        assert_eq!(store.agent(&agent.id).unwrap(), agent);
        assert_eq!(store.inbox(&agent.id, None).unwrap(), inbox);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 0);
        assert_eq!(
            serde_json::to_value(store.conversation(&agent.id, None, None, None).unwrap()).unwrap(),
            before
        );
        assert_eq!(store.get_schedule(&schedule.id).unwrap().unwrap(), schedule);
        assert_eq!(
            std::fs::read(config.join("sylphx-transcript.jsonl")).unwrap(),
            legacy
        );
        assert_eq!(
            std::fs::read_to_string(config.join("sylphx-session-id")).unwrap(),
            "legacy-session\n"
        );
        assert_eq!(
            std::fs::read_to_string(config.join("spiron-session-id")).unwrap(),
            "legacy-session\n"
        );
    }
}

fn external_probe(
    temp: &Path,
    script: &str,
) -> (
    std::sync::MutexGuard<'static, ()>,
    Store,
    anyagents_core::Agent,
    SpironHarness,
) {
    // A child inherits process-global environment even when this fixture never
    // mutates it. Keep the shared guard until its harness and restart are done,
    // not merely until launch returns.
    let environment = crate::server::tests::env_lock();
    let config = temp.join("config");
    let work = temp.join("work");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    let program = temp.join("peer");
    std::fs::write(&program, format!("#!/usr/bin/env python3\n{script}")).unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = Store::open(temp.join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("external-probe", "spiron"))
        .unwrap();
    let harness = start_spiron(&program, &config, &work, &agent, None, &[]).unwrap();
    (environment, store, agent, harness)
}

#[test]
fn spiron_external_ingestion_masks_all_nine_forms_and_fragmented_text() {
    let temp = crate::test_tempdir();
    let script = r#"
import sys,json
canary='SYNTHETIC_FIXTURE_0123456789012345'
forms=['Bearer '+canary,'bearer '+canary,'api_key='+canary,'sk-'+canary,'xoxb-'+canary,
 'https://fixture:'+canary+'@example.invalid/path','https://example.invalid/path?access_token='+canary+'&page=2',
 'OPENAI_API_KEY='+canary,'X-Api-Key: '+canary]
print(json.dumps({'type':'ready'}),flush=True)
for line in sys.stdin:
 m=json.loads(line); id=m['request_id']
 print(json.dumps({'type':'accepted','request_id':id}),flush=True)
 text='\n'.join(forms)+'\nfailed to parse config.toml; 測試🙂'
 # Transport frame boundaries are unrelated to credential boundaries.
 for char in text:
  print(json.dumps({'type':'text_delta','request_id':id,'text':char}),flush=True)
 print(json.dumps({'type':'result','request_id':id,'status':'completed','usage':{'input_tokens':7}}),flush=True)
"#;
    let (_environment, store, agent, mut harness) = external_probe(temp.path(), script);
    eventually(|| {
        matches!(
            harness.deliver(&message("mask")),
            Ok(DeliverOutcome::Accepted {
                turn_finished: true
            })
        )
    });
    let text = harness.text_for("mask").unwrap();
    assert!(!text.contains("SYNTHETIC_FIXTURE"));
    assert_eq!(text.matches("[redacted]").count(), 9);
    assert!(text.ends_with("failed to parse config.toml; 測試🙂"));
    assert!(text.contains("&page=2"));
    assert_eq!(harness.usage_for("mask").unwrap().input_tokens, 7);
    store
        .record_tool_event(&agent.id, "external", &text)
        .unwrap();
    let rows = store
        .search_history(&anyagents_core::HistorySearchQuery {
            agent: Some(agent.name),
            only_tools: true,
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].text.contains("failed to parse config.toml"));
    assert!(!rows[0].text.contains("SYNTHETIC_FIXTURE"));
}

#[test]
fn spiron_external_nonzero_exit_retains_masked_bounded_stderr_and_success_does_not_fault() {
    for (exit, after_input) in [(1, false), (1, true), (0, false)] {
        let temp = crate::test_tempdir();
        let script = format!(
            r#"
import sys,json
if {after_input}:
 print(json.dumps({{'type':'ready'}}),flush=True)
 m=json.loads(sys.stdin.readline())
 print(json.dumps({{'type':'accepted','request_id':m['request_id']}}),flush=True)
sys.stderr.write('failed to parse config.toml; 測試🙂\n')
canary='SYNTHETIC_FIXTURE_0123456789012345'
forms=['Bearer '+canary,'bearer '+canary,'api_key='+canary,'sk-'+canary,'xoxb-'+canary,
 'https://fixture:'+canary+'@example.invalid/path','https://example.invalid/path?access_token='+canary+'&page=2',
 'OPENAI_API_KEY='+canary,'X-Api-Key: '+canary]
# Split writes deliberately; then exceed pipe and retention capacities.
for item in '\n'.join(forms)+'\n':
 sys.stderr.write(item); sys.stderr.flush()
sys.stderr.write('long actionable diagnostic\n'+'x'*200000+'api_key='+canary)
sys.exit({exit})
"#,
            after_input = if after_input { "True" } else { "False" }
        );
        let (_environment, _, _, mut harness) = external_probe(temp.path(), &script);
        if after_input {
            // The peer can die between writing and observing acceptance.
            let _ = harness.deliver(&message("startup"));
        }
        eventually(|| !harness.health().process_alive);
        if exit == 0 {
            assert!(harness.health().sign_in_fault.is_none());
            assert!(
                !temp
                    .path()
                    .join("config/.anyagents-last-error.json")
                    .exists()
            );
        } else {
            let error = harness.health().sign_in_fault.unwrap();
            assert!(error.contains("exit status: 1"), "{error}");
            assert!(error.contains("failed to parse config.toml; 測試🙂"));
            assert!(error.contains("long actionable diagnostic"));
            assert!(error.contains("stderr truncated"));
            assert_eq!(error.matches("[redacted]").count(), 9);
            assert!(!error.contains("SYNTHETIC_FIXTURE"));
            assert!(error.len() < 17 * 1024);
            let persisted = read_persisted_last_error(&temp.path().join("config"))
                .unwrap()
                .0
                .unwrap();
            assert_eq!(persisted, error);
        }
    }
}

#[test]
fn spiron_runtime_config_writes_always_approve_and_materializes_seat_auth() {
    let temp = crate::test_tempdir();
    let config = temp.path().join("config");
    std::fs::create_dir_all(config.join(".config/sylphx")).unwrap();
    std::fs::write(
        config.join(".config/sylphx/credentials.json"),
        r#"{"api_key":"sk-sylphx-seat-key","org_id":"org_test_1"}"#,
    )
    .unwrap();
    // Stale codex config.toml should be replaced with Spiron's config.toml
    std::fs::write(
        config.join("config.toml"),
        "[mcp_servers.anyagents]\ncommand = \"/old/anyagents\"\n",
    )
    .unwrap();

    crate::spiron::write_spiron_config(
        &config,
        Some("sylphx/openai/gpt-6.1-sol#xhigh"),
        Path::new("/usr/bin/anyagents"),
        &["mcp".to_string()],
    )
    .unwrap();

    crate::create::verify_harness_runtime_config(
        &config,
        "spiron",
        Some("sylphx/openai/gpt-6.1-sol#xhigh"),
    )
    .unwrap();

    let toml_text = std::fs::read_to_string(config.join("config.toml")).unwrap();
    assert!(toml_text.contains("always_approve = true"), "{toml_text}");
    assert!(
        toml_text.contains("model = \"openai/gpt-6.1-sol#xhigh\""),
        "{toml_text}"
    );
    assert!(
        !toml_text.contains("[mcp_servers.anyagents]"),
        "{toml_text}"
    );

    let auth_raw = std::fs::read_to_string(config.join("auth.json")).unwrap();
    let auth_json: serde_json::Value = serde_json::from_str(&auth_raw).unwrap();
    assert_eq!(auth_json["access_token"], "sk-sylphx-seat-key");
    assert_eq!(auth_json["org_id"], "org_test_1");
}

#[test]
fn spiron_supervisor_recovers_from_transient_mid_turn_server_error_via_recovery() {
    use anyagents_supervisor::{Supervisor, SystemClock};
    use std::sync::Arc;

    let _lock = crate::server::tests::env_lock();
    let temp = crate::test_tempdir();
    let probe = temp.path().join("spiron-server-error-probe");
    std::fs::write(
        &probe,
        r#"#!/usr/bin/env python3
import json, os, pathlib, sys
cfg = pathlib.Path(sys.argv[sys.argv.index("--state-dir") + 1])
launch_file = cfg / "launch_count"
launch_num = int(launch_file.read_text()) + 1 if launch_file.exists() else 1
launch_file.write_text(str(launch_num))

def emit(kind, req_id=None, **fields):
    payload = {"type": kind, "session_id": "spiron-rollout-sess"}
    if req_id is not None:
        payload["request_id"] = req_id
    payload.update(fields)
    print(json.dumps(payload), flush=True)

emit("ready")
for raw in sys.stdin:
    msg = json.loads(raw)
    with (cfg / "wire.jsonl").open("a") as out:
        out.write(json.dumps({"launch": launch_num, "msg": msg}) + "\n")
    req_id = msg.get("request_id")
    emit("accepted", req_id, mode="turn")
    if launch_num == 1:
        emit(
            "result",
            req_id,
            status="failed",
            is_error=True,
            error='Sylphx AI failure Some("server_error"): The model failed to generate a response.',
        )
    else:
        emit("text_delta", req_id, text="done-after-rollout")
        emit("result", req_id, status="completed", usage={"input_tokens": 8, "output_tokens": 3})
"#,
    )
    .unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _env = Environment::set(&[
        ("ANYAGENTS_SPIRON_BIN", probe.as_os_str()),
        ("ANYAGENTS_FAKE_MODE", std::ffi::OsStr::new("1")),
    ]);

    let data = temp.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let store = Arc::new(Store::open(data.join("state.db")).unwrap());
    let mut spec = NewAgent::new("spiron-rollout", "spiron");
    spec.model = Some("sylphx/openai/gpt-6.1-sol#xhigh".into());
    let agent = store.create_agent(spec).unwrap();
    let agent_root = data.join("agents").join(agent.id.as_str());
    let cfg = agent_root.join("config");
    let work = agent_root.join("work");
    std::fs::create_dir_all(cfg.join(".agents/skills")).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    crate::spiron::write_spiron_config(
        &cfg,
        agent.model.as_deref(),
        Path::new("/probe/anyagents"),
        &["mcp".into()],
    )
    .unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    store
        .receive(&agent.id, Incoming::new("survive gateway rollout"))
        .unwrap();

    let probe_launch = probe.clone();
    let supervisor = Supervisor::<VendorHarness>::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        Arc::new(move |req| {
            anyagents_core::host_session::ensure_active(req.config_dir.parent().unwrap(), "spiron")
                .map_err(|e| e.to_string())?;
            let session = session_from_file(&req.config_dir, "spiron-session-id")?;
            let h = start_spiron(
                &probe_launch,
                &req.config_dir,
                &req.work_dir,
                &req.agent,
                session.as_deref(),
                &[],
            )?;
            let vh = VendorHarness::Spiron(
                Box::new(h),
                req.config_dir.join("spiron-session-id").into(),
                "spiron".into(),
            );
            vh.sync_session()?;
            Ok(vh)
        }),
        vec![],
    )
    .unwrap();

    eventually(|| {
        cfg.join("launch_count").exists()
            && std::fs::read_to_string(cfg.join("launch_count"))
                .unwrap_or_default()
                .trim()
                == "2"
            && !store.has_open_turn(&agent.id).unwrap()
    });
    assert!(!cfg.join(".anyagents-last-error.json").exists());
    let wire_lines: Vec<serde_json::Value> = std::fs::read_to_string(cfg.join("wire.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(wire_lines.len(), 2);
    assert!(
        wire_lines[1]["msg"]["text"]
            .as_str()
            .unwrap()
            .contains("[Recovery] Your previous turn was interrupted")
    );
    drop(supervisor);
}

/// The fake Spiron of the stream harness tests, launched the way AnyAgents
/// launches Spiron: its fixture directory is the agent's config directory.
const FAKE_SPIRON: &str = include_str!("../../../anyagents-harness-stream/tests/fake_spiron.py");

struct FakeSpiron {
    _environment: std::sync::MutexGuard<'static, ()>,
    program: PathBuf,
    config: PathBuf,
    work: PathBuf,
    agent: anyagents_core::Agent,
    capable: bool,
}

impl FakeSpiron {
    fn new(temp: &Path, capable: bool) -> (Self, Store) {
        let environment = crate::server::tests::env_lock();
        let config = temp.join("config");
        let work = temp.join("work");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        let program = temp.join("fake-spiron");
        std::fs::write(&program, format!("#!/usr/bin/env python3\n{FAKE_SPIRON}")).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let store = Store::open(temp.join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("steered", "spiron"))
            .unwrap();
        // The fake names its session, which binds to the agent's host session.
        anyagents_core::host_session::ensure_active(temp, "spiron").unwrap();
        let fake = Self {
            _environment: environment,
            program,
            config,
            work,
            agent,
            capable,
        };
        (fake, store)
    }

    fn launch(&self) -> VendorHarness {
        let capable = if self.capable { "1" } else { "0" };
        let harness = start_spiron(
            &self.program,
            &self.config,
            &self.work,
            &self.agent,
            None,
            &[("FAKE_SPIRON_CAPABLE".to_string(), capable.to_string())],
        )
        .unwrap();
        assert_eq!(harness.proves_consumption(), self.capable);
        VendorHarness::Spiron(
            Box::new(harness),
            self.config.join("spiron-session-id").into(),
            "spiron".into(),
        )
    }

    fn delivery(&self, store: Store) -> anyagents_agent::DeliveryLoop<VendorHarness> {
        anyagents_agent::DeliveryLoop::start(store, self.agent.id.clone(), self.launch()).unwrap()
    }

    fn receive(&self, store: &Store, text: &str) -> anyagents_core::Message {
        match store.receive(&self.agent.id, Incoming::new(text)).unwrap() {
            ReceiveOutcome::New(message) => message,
            ReceiveOutcome::Duplicate(_) => panic!("fresh mailbox message expected"),
        }
    }

    /// Ask the fake to act on `name` and wait until it has.
    fn act(&self, name: &str) {
        std::fs::write(self.config.join(name), "").unwrap();
        eventually(|| !self.config.join(name).exists());
    }

    fn lines(&self, name: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.config.join(name))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// `(launch, line type, request id)` of every line the fake read.
    fn wire(&self) -> Vec<(u64, String, String)> {
        self.lines("wire.jsonl")
            .into_iter()
            .map(|line| {
                (
                    line["launch"].as_u64().unwrap(),
                    line["msg"]["type"].as_str().unwrap().to_string(),
                    line["msg"]["request_id"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn journaled(&self) -> Vec<String> {
        self.lines("journal.jsonl")
            .into_iter()
            .map(|line| line["key"].as_str().unwrap().to_string())
            .collect()
    }
}

/// Run passes until the cursor reaches `seq`, adding up what they delivered.
fn deliver_through(delivery: &mut anyagents_agent::DeliveryLoop<VendorHarness>, seq: i64) -> u64 {
    let mut delivered = 0;
    eventually(|| {
        if let anyagents_agent::RunOutcome::Delivered { delivered: more } =
            delivery.run_once().unwrap()
        {
            delivered += more;
        }
        delivery.delivered_cursor().unwrap() >= seq
    });
    assert_eq!(delivery.delivered_cursor().unwrap(), seq);
    delivered
}

#[test]
fn spiron_steers_five_mid_turn_messages_into_its_next_step_and_commits_them_in_order() {
    let temp = crate::test_tempdir();
    let (fake, store) = FakeSpiron::new(temp.path(), true);
    let first = fake.receive(&store, "start the work");
    let mut delivery = fake.delivery(store);
    assert_eq!(deliver_through(&mut delivery, first.seq), 1);
    let steers: Vec<_> = (1..=5)
        .map(|n| fake.receive(delivery.store(), &format!("steer {n}")))
        .collect();
    // One pass writes all five into the running turn; none is committed,
    // since none is consumed yet.
    assert_eq!(
        delivery.run_once().unwrap(),
        anyagents_agent::RunOutcome::Idle
    );
    eventually(|| fake.wire().len() == 6);
    assert_eq!(delivery.delivered_cursor().unwrap(), first.seq);
    let ids: Vec<String> = steers.iter().map(|m| m.id.to_string()).collect();
    let written = fake.wire();
    assert_eq!(written[0], (1, "prompt".into(), first.id.to_string()));
    for (line, id) in written[1..].iter().zip(&ids) {
        assert_eq!(line, &(1, "steer".to_string(), id.clone()));
    }
    assert!(fake.lines("steps.jsonl").is_empty(), "no step ran yet");
    // The next step takes the whole inbox together, in order.
    fake.act("step");
    let steps = fake.lines("steps.jsonl");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["consumed"], json!(ids));
    // Each consumption proof moves the cursor over the contiguous prefix.
    let last = steers.last().unwrap().seq;
    assert_eq!(deliver_through(&mut delivery, last), 5);
    for message in &steers {
        let stored = delivery
            .store()
            .message(&fake.agent.id, message.id.as_str())
            .unwrap()
            .unwrap();
        assert!(stored.delivered_at_ms.is_some(), "{}", message.id);
    }
    assert_eq!(fake.wire().len(), 6, "nothing was written twice");
    fake.act("finish");
    eventually(|| {
        delivery.run_once().unwrap();
        delivery.harness().health().turn_state == anyagents_harness::TurnState::Idle
    });
    assert_eq!(delivery.harness().turns_served(), 6);
}

#[test]
fn spiron_crash_between_consumption_and_commit_replays_once_and_the_cursor_lands_right() {
    let temp = crate::test_tempdir();
    let (fake, store) = FakeSpiron::new(temp.path(), true);
    let first = fake.receive(&store, "start the work");
    let mut delivery = fake.delivery(store);
    deliver_through(&mut delivery, first.seq);
    let steer = fake.receive(delivery.store(), "consumed before the crash");
    assert_eq!(
        delivery.run_once().unwrap(),
        anyagents_agent::RunOutcome::Idle
    );
    eventually(|| fake.wire().len() == 2);
    fake.act("step");
    // The journal has it; the crash comes before any pass records it.
    assert_eq!(fake.journaled().len(), 2);
    drop(delivery);
    let store = Store::open(temp.path().join("state.db")).unwrap();
    assert_eq!(store.delivered_cursor(&fake.agent.id).unwrap(), first.seq);
    let fresh = fake.receive(&store, "arrived during the restart");
    let mut delivery = fake.delivery(store);
    // The replay is answered as a duplicate and committed without counting
    // as new work; the fresh line is the only delivery.
    assert_eq!(deliver_through(&mut delivery, fresh.seq), 1);
    assert_eq!(
        fake.journaled(),
        [
            format!("input:{}", first.id),
            format!("input:{}", steer.id),
            format!("input:{}", fresh.id),
        ],
        "every input is journaled exactly once"
    );
    let second_launch: Vec<_> = fake
        .wire()
        .into_iter()
        .filter(|(launch, _, _)| *launch == 2)
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(second_launch[0], ("prompt".into(), steer.id.to_string()));
    assert_eq!(
        second_launch.last().unwrap(),
        &("prompt".to_string(), fresh.id.to_string())
    );
    // The interrupted turn is still open for its recovery note.
    assert!(delivery.store().has_open_turn(&fake.agent.id).unwrap());
}

#[test]
fn spiron_undelivered_steer_ids_are_redelivered_as_the_next_turn() {
    let temp = crate::test_tempdir();
    let (fake, store) = FakeSpiron::new(temp.path(), true);
    let first = fake.receive(&store, "start the work");
    let mut delivery = fake.delivery(store);
    deliver_through(&mut delivery, first.seq);
    let second = fake.receive(delivery.store(), "steered, never consumed");
    let third = fake.receive(delivery.store(), "also never consumed");
    delivery.run_once().unwrap();
    eventually(|| fake.wire().len() == 3);
    // The turn ends before a step: its result lists both as undelivered.
    fake.act("finish");
    deliver_through(&mut delivery, second.seq);
    // The third is steered into the new turn before its step.
    eventually(|| fake.wire().len() == 5);
    fake.act("step");
    deliver_through(&mut delivery, third.seq);
    let written: Vec<_> = fake
        .wire()
        .into_iter()
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(
        written,
        [
            ("prompt".to_string(), first.id.to_string()),
            ("steer".to_string(), second.id.to_string()),
            ("steer".to_string(), third.id.to_string()),
            ("prompt".to_string(), second.id.to_string()),
            ("steer".to_string(), third.id.to_string()),
        ]
    );
    assert_eq!(
        fake.journaled(),
        [
            format!("input:{}", first.id),
            format!("input:{}", second.id),
            format!("input:{}", third.id),
        ]
    );
    let health = delivery.harness().health();
    assert!(health.sign_in_fault.is_none());
}

#[test]
fn spiron_without_the_capabilities_gets_one_input_per_turn() {
    let temp = crate::test_tempdir();
    let (fake, store) = FakeSpiron::new(temp.path(), false);
    let first = fake.receive(&store, "start the work");
    let mut delivery = fake.delivery(store);
    assert_eq!(
        delivery.harness().health().delivery.level,
        anyagents_harness::DeliveryLevel::TurnEnd
    );
    deliver_through(&mut delivery, first.seq);
    let second = fake.receive(delivery.store(), "waits for the turn to end");
    let third = fake.receive(delivery.store(), "waits behind it");
    // Nothing reaches the running turn: nothing would prove it took it.
    delivery.run_once().unwrap();
    fake.act("step");
    delivery.run_once().unwrap();
    assert_eq!(delivery.delivered_cursor().unwrap(), first.seq);
    assert_eq!(fake.wire().len(), 1);
    fake.act("finish");
    deliver_through(&mut delivery, second.seq);
    fake.act("finish");
    deliver_through(&mut delivery, third.seq);
    let written: Vec<_> = fake
        .wire()
        .into_iter()
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(
        written,
        [
            ("user".to_string(), first.id.to_string()),
            ("user".to_string(), second.id.to_string()),
            ("user".to_string(), third.id.to_string()),
        ]
    );
    assert!(!fake.config.join("spiron-unconfirmed-steers.json").exists());
}

#[test]
fn a_hold_an_older_anyagents_left_is_settled_against_the_spiron_journal_at_launch() {
    let temp = crate::test_tempdir();
    let (fake, store) = FakeSpiron::new(temp.path(), false);
    let applied = fake.receive(&store, "journaled by the old process");
    let lost = fake.receive(&store, "acknowledged, never journaled");
    // The hold names both; only the first reached the session journal.
    std::fs::write(
        fake.config.join("spiron-unconfirmed-steers.json"),
        json!({"turn_id": applied.id, "request_ids": [applied.id, lost.id]}).to_string(),
    )
    .unwrap();
    // As Spiron journals them: the applied input as a user text event; the
    // other only quoted in a tool result, which is no input.
    write_spiron_journal(
        &fake.config,
        &[
            json!({"type": "text", "sessionKey": "cli:old", "data": {
                "role": "user",
                "content": format!("anyagents-message:{}\n<message>body</message>", applied.id),
            }}),
            json!({"type": "tool_result", "sessionKey": "cli:old", "data": {
                "callId": "call_1", "toolName": "mcp__anyagents__inbox",
                "result": {"content": [{"text": format!("\"content\":\"anyagents-message:{}", lost.id)}]},
            }}),
            json!({"type": "text", "sessionKey": "cli:old", "data": {
                "role": "assistant",
                "content": format!("anyagents-message:{}", lost.id),
            }}),
        ],
    );
    let mut delivery = fake.delivery(store);
    assert!(!fake.config.join("spiron-unconfirmed-steers.json").exists());
    assert!(
        fake.config
            .join("spiron-unconfirmed-steers.reconciled.json")
            .is_file()
    );
    deliver_through(&mut delivery, lost.seq);
    // The applied one is committed without being written again; the other
    // is delivered as new input.
    let written: Vec<_> = fake.wire().into_iter().map(|(_, _, id)| id).collect();
    assert_eq!(written, [lost.id.to_string()]);
}

/// A Spiron journal with these events in one session, as `sessions.sqlite3`
/// stores them.
fn write_spiron_journal(config: &Path, events: &[serde_json::Value]) {
    let journal = rusqlite::Connection::open(config.join("sessions.sqlite3")).unwrap();
    journal
        .execute_batch(
            "CREATE TABLE session_entries (
                session_id TEXT NOT NULL, idx INTEGER NOT NULL,
                event TEXT NOT NULL CHECK(json_valid(event)),
                PRIMARY KEY(session_id, idx));",
        )
        .unwrap();
    for (idx, event) in events.iter().enumerate() {
        journal
            .execute(
                "INSERT INTO session_entries VALUES ('old-session', ?1, ?2)",
                rusqlite::params![idx as i64, event.to_string()],
            )
            .unwrap();
    }
}

#[test]
fn a_settled_hold_names_its_applied_ids_again_after_a_launch_that_failed() {
    let temp = crate::test_tempdir();
    let config = temp.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join(LEGACY_SPIRON_HOLD),
        json!({"turn_id": "msg_a", "request_ids": ["msg_a", "msg_b"]}).to_string(),
    )
    .unwrap();
    write_spiron_journal(
        &config,
        &[json!({"type": "text", "data": {"role": "user", "content": "anyagents-message:msg_a"}})],
    );
    assert_eq!(settle_legacy_spiron_hold(&config).unwrap(), ["msg_a"]);
    assert!(!config.join(LEGACY_SPIRON_HOLD).exists());
    // The launch after this one failed, so nothing was committed: the next
    // launch still answers msg_a as applied, from the reconciled record.
    assert_eq!(settle_legacy_spiron_hold(&config).unwrap(), ["msg_a"]);
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config.join(LEGACY_SPIRON_RECONCILED)).unwrap())
            .unwrap();
    assert_eq!(record["applied"], json!(["msg_a"]));
    assert_eq!(record["hold"]["request_ids"], json!(["msg_a", "msg_b"]));
}

#[test]
fn spiron_supervisor_sends_the_recovery_note_first_after_the_lease_and_commits_a_journaled_line_once()
 {
    use anyagents_supervisor::{Supervisor, SystemClock};
    use std::sync::Arc;

    let _lock = crate::server::tests::env_lock();
    let temp = crate::test_tempdir();
    let program = temp.path().join("fake-spiron");
    std::fs::write(&program, format!("#!/usr/bin/env python3\n{FAKE_SPIRON}")).unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    let data = temp.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let store = Arc::new(Store::open(data.join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("steered-recovery", "spiron"))
        .unwrap();
    let agent_root = data.join("agents").join(agent.id.as_str());
    let config = agent_root.join("config");
    let work = agent_root.join("work");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let ReceiveOutcome::New(first) = store
        .receive(&agent.id, Incoming::new("the long task"))
        .unwrap()
    else {
        panic!("fresh mailbox message expected");
    };
    let launch_program = program.clone();
    let supervisor = Supervisor::<VendorHarness>::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        Arc::new(move |req| {
            anyagents_core::host_session::ensure_active(req.config_dir.parent().unwrap(), "spiron")
                .map_err(|e| e.to_string())?;
            let session = session_from_file(&req.config_dir, "spiron-session-id")?;
            let harness = start_spiron(
                &launch_program,
                &req.config_dir,
                &req.work_dir,
                &req.agent,
                session.as_deref(),
                &[("FAKE_SPIRON_CAPABLE".to_string(), "1".to_string())],
            )?;
            let vendor = VendorHarness::Spiron(
                Box::new(harness),
                req.config_dir.join("spiron-session-id").into(),
                "spiron".into(),
            );
            vendor.sync_session()?;
            Ok(vendor)
        }),
        vec![],
    )
    .unwrap();
    eventually(|| store.delivered_cursor(&agent.id).unwrap() == first.seq);
    let ReceiveOutcome::New(steer) = store
        .receive(&agent.id, Incoming::new("a detail for the running turn"))
        .unwrap()
    else {
        panic!("fresh mailbox message expected");
    };
    store.poke(anyagents_core::Wake::Agent {
        agent_id: agent.id.clone(),
    });
    let wire = || -> Vec<serde_json::Value> {
        std::fs::read_to_string(config.join("wire.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    };
    eventually(|| wire().len() == 2);
    // The step journals the steer and the process dies before it can say so,
    // still holding the session's lease.
    std::fs::write(config.join("lease"), "").unwrap();
    std::fs::write(config.join("journal-crash"), "").unwrap();
    eventually(|| !config.join("journal-crash").exists());
    // The replacement cannot start a turn while the lease is held. The
    // interrupted turn's recovery note goes first and the steer follows it;
    // each refusal hands both back, in that order, with no fault and no
    // restart.
    let second = || -> Vec<serde_json::Value> {
        wire()
            .into_iter()
            .filter(|line| line["launch"] == 2)
            .collect()
    };
    let is_note = |line: &serde_json::Value| {
        line["msg"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("[Recovery]"))
    };
    eventually(|| second().iter().filter(|line| is_note(line)).count() >= 2);
    assert!(is_note(&second()[0]), "{:?}", second());
    assert_eq!(
        std::fs::read_to_string(config.join("launches")).unwrap(),
        "2"
    );
    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), first.seq);
    std::fs::remove_file(config.join("lease")).unwrap();
    // Once a turn can start, the note opens it and the steer follows into
    // that turn; at the turn's next step Spiron finds the steer journaled
    // already and answers it as a duplicate, which commits it.
    eventually(|| {
        let lines = second();
        let steered = lines.iter().rposition(&is_note).is_some_and(|at| {
            lines[at + 1..]
                .iter()
                .any(|line| line["msg"]["request_id"] == steer.id.to_string())
        });
        if steered && !config.join("step").exists() {
            std::fs::write(config.join("step"), "").unwrap();
        }
        store.delivered_cursor(&agent.id).unwrap() == steer.seq
    });
    let second = second();
    assert_eq!(
        std::fs::read_to_string(config.join("launches")).unwrap(),
        "2"
    );
    let note = second
        .iter()
        .rev()
        .find(|line| is_note(line))
        .and_then(|line| line["msg"]["text"].as_str())
        .unwrap();
    assert!(note.contains("the long task"), "{note}");
    let journal: Vec<String> = std::fs::read_to_string(config.join("journal.jsonl"))
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["key"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        journal
            .iter()
            .filter(|key| **key == format!("input:{}", steer.id))
            .count(),
        1,
        "{journal:?}"
    );
    assert!(!config.join("spiron-unconfirmed-steers.json").exists());
    drop(supervisor);
}
