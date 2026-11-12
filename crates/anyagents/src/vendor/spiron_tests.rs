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
        assert_eq!(
            vendor.deliver(&message("steer")).unwrap(),
            DeliverOutcome::Deferred
        );
        eventually(|| config.join("steer-ack").exists());
        assert_eq!(
            vendor.deliver(&message("steer")).unwrap(),
            DeliverOutcome::Deferred,
            "socket acknowledgement is not journal commit"
        );
        assert_eq!(
            vendor.turns_served(),
            0,
            "unrelated result cannot close this turn"
        );
        vendor.cancel().unwrap();
        eventually(|| config.join("cancel-ack").exists());
        assert_eq!(vendor.turns_served(), 0, "cancel_ack is not completion");
        std::fs::write(config.join("cancel-complete"), "").unwrap();
        eventually(|| vendor.turns_served() == 2);
        assert_eq!(
            vendor.deliver(&message("steer")).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: true
            }
        );
        assert!(
            !config.join("spiron-unconfirmed-steers.json").exists(),
            "confirmed terminal completion releases the pre-dispatch hold"
        );
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
        assert_eq!(inputs[1]["request_id"], "steer");
        assert!(inputs[1]["text"].as_str().unwrap().contains("body-steer"));
        assert_eq!(inputs[2]["type"], "cancel");
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
    StreamHarness,
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
fn spiron_external_cancelled_unconfirmed_steer_retains_mailbox_custody_across_restart() {
    use anyagents_agent::DeliveryLoop;
    let temp = crate::test_tempdir();
    // Vesta's exact cancel/cancel_ack/cancelled-result shape: acknowledged
    // steering text is returned verbatim in undelivered_steers, without IDs.
    let script = r#"
import sys,json,pathlib,os
pending=None
first=None
for line in sys.stdin:
 m=json.loads(line)
 if m['type']=='cancel':
  print(json.dumps({'type':'cancel_ack','turn_id':first,'status':'cancelling'}),flush=True)
  print(json.dumps({'type':'result','request_id':first,'status':'cancelled','is_error':False,'undelivered_steers':[pending]}),flush=True)
 elif first is None:
  first=m['request_id']
  print(json.dumps({'type':'accepted','request_id':first}),flush=True)
 else:
  pending=m['text']
  print(json.dumps({'type':'steer_ack','request_id':m['request_id'],'accepted':True,'turn_id':first}),flush=True)
  pathlib.Path(os.environ['HOME'],'steer-ack').touch()
"#;
    let (_environment, store, agent, harness) = external_probe(temp.path(), script);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, Incoming::new("first")).unwrap()
    else {
        panic!("fresh mailbox message expected")
    };
    let mut delivery = DeliveryLoop::start(
        store,
        agent.id.clone(),
        VendorHarness::Spiron(
            Box::new(harness),
            temp.path().join("config/spiron-session-id"),
            "spiron".into(),
        ),
    )
    .unwrap();
    eventually(|| {
        delivery.run_once().unwrap();
        delivery.store().delivered_cursor(&agent.id).unwrap() == first.seq
    });
    let ReceiveOutcome::New(steer) = delivery
        .store()
        .receive(&agent.id, Incoming::new("steering work"))
        .unwrap()
    else {
        panic!("fresh mailbox message expected")
    };
    delivery.run_once().unwrap();
    eventually(|| temp.path().join("config/steer-ack").exists());
    delivery.run_once().unwrap();
    assert_eq!(
        delivery.store().delivered_cursor(&agent.id).unwrap(),
        first.seq
    );
    assert!(
        delivery
            .store()
            .message(&agent.id, steer.id.as_str())
            .unwrap()
            .unwrap()
            .delivered_at_ms
            .is_none()
    );
    delivery.harness_mut().cancel().unwrap();
    eventually(|| delivery.harness().turns_served() > 0);
    delivery.run_once().unwrap();
    let health = delivery.harness().health();
    assert_eq!(health.turn_state, anyagents_harness::TurnState::InTurn);
    assert!(
        health
            .sign_in_fault
            .unwrap()
            .contains("unconfirmed steering input")
    );
    assert_eq!(delivery.harness().turns_served(), 1);
    assert!(delivery.store().has_open_turn(&agent.id).unwrap());
    assert_eq!(
        delivery
            .store()
            .next_undelivered(&agent.id)
            .unwrap()
            .unwrap()
            .id,
        steer.id
    );
    assert!(
        temp.path()
            .join("config/spiron-unconfirmed-steers.json")
            .exists()
    );
    drop(delivery);
    let program = temp.path().join("peer");
    let mut restarted = start_spiron(
        &program,
        &temp.path().join("config"),
        &temp.path().join("work"),
        &agent,
        None,
        &[],
    )
    .unwrap();
    assert_eq!(
        restarted
            .deliver(
                &message("steer"),
                &json!({"type":"user","request_id":"steer","text":"must not replay"})
            )
            .unwrap(),
        DeliverOutcome::Deferred
    );
    assert!(restarted.written_ids().is_empty());
    assert!(
        restarted
            .health()
            .sign_in_fault
            .unwrap()
            .contains("Reconcile")
    );
}

#[test]
fn spiron_external_hold_write_failure_refuses_unsent_steer_and_restart_keeps_custody() {
    use anyagents_agent::DeliveryLoop;
    let temp = crate::test_tempdir();
    let script = r#"
import sys,json,pathlib
for line in sys.stdin:
 m=json.loads(line)
 with pathlib.Path(__file__).with_name('wire.jsonl').open('a') as out:
  out.write(json.dumps(m)+'\n')
 print(json.dumps({'type':'accepted','request_id':m['request_id']}),flush=True)
"#;
    let (_environment, store, agent, harness) = external_probe(temp.path(), script);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, Incoming::new("first")).unwrap()
    else {
        panic!("fresh mailbox message expected")
    };
    let mut delivery = DeliveryLoop::start(
        store,
        agent.id.clone(),
        VendorHarness::Spiron(
            Box::new(harness),
            temp.path().join("config/spiron-session-id"),
            "spiron".into(),
        ),
    )
    .unwrap();
    eventually(|| {
        delivery.run_once().unwrap();
        delivery.store().delivered_cursor(&agent.id).unwrap() == first.seq
    });
    let ReceiveOutcome::New(steer) = delivery
        .store()
        .receive(&agent.id, Incoming::new("steering work"))
        .unwrap()
    else {
        panic!("fresh mailbox message expected")
    };
    // A missing configured directory deterministically returns NotFound even
    // under root. Existing accepted work and its hold remain in the moved dir;
    // only the hold write for the new input fails (no chmod/ENOSPC assumptions).
    let config = temp.path().join("config");
    let offline = temp.path().join("offline-config");
    std::fs::rename(&config, &offline).unwrap();
    let error = delivery.run_once().unwrap_err();
    assert!(
        matches!(error, anyagents_agent::Error::HarnessFailed(ref reason)
        if reason.contains("cannot persist Spiron input hold before dispatch")),
        "{error}"
    );
    assert_eq!(
        delivery.store().delivered_cursor(&agent.id).unwrap(),
        first.seq
    );
    assert!(
        delivery
            .store()
            .message(&agent.id, steer.id.as_str())
            .unwrap()
            .unwrap()
            .delivered_at_ms
            .is_none()
    );
    let wire = temp.path().join("wire.jsonl");
    assert_eq!(std::fs::read_to_string(&wire).unwrap().lines().count(), 1);
    assert!(offline.join("spiron-unconfirmed-steers.json").exists());
    drop(delivery);
    std::fs::rename(&offline, &config).unwrap();

    let store = Store::open(temp.path().join("state.db")).unwrap();
    let restarted = start_spiron(
        &temp.path().join("peer"),
        &config,
        &temp.path().join("work"),
        &agent,
        None,
        &[],
    )
    .unwrap();
    let mut restarted = DeliveryLoop::start(
        store,
        agent.id.clone(),
        VendorHarness::Spiron(
            Box::new(restarted),
            config.join("spiron-session-id"),
            "spiron".into(),
        ),
    )
    .unwrap();
    restarted.run_once().unwrap();
    assert_eq!(
        restarted.store().delivered_cursor(&agent.id).unwrap(),
        first.seq
    );
    assert_eq!(
        restarted
            .store()
            .next_undelivered(&agent.id)
            .unwrap()
            .unwrap()
            .id,
        steer.id
    );
    assert!(
        restarted
            .harness()
            .health()
            .sign_in_fault
            .unwrap()
            .contains("Reconcile")
    );
    assert_eq!(
        std::fs::read_to_string(wire).unwrap().lines().count(),
        1,
        "fresh real delivery loop cannot resend unreconciled input"
    );
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
            harness.deliver(
                &message("mask"),
                &json!({"type":"user","request_id":"mask","text":"go"})
            ),
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
            let _ = harness.deliver(
                &message("startup"),
                &json!({"type":"user","request_id":"startup","text":"go"}),
            );
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

/// Shared probe prelude: a Spiron double that journals each input it admits
/// exactly as Spiron does (a `text` event with `role: "user"`) plus a model
/// event after it when it is processed, honours the owner-fenced session
/// lease, and records every wire input with its time.
const SPIRON_JOURNAL_PROBE_PRELUDE: &str = r#"#!/usr/bin/env python3
import json, os, pathlib, sqlite3, sys, time
cfg = pathlib.Path(sys.argv[sys.argv.index("--state-dir") + 1])
args = sys.argv[1:]
session = args[args.index("--session") + 1] if "--session" in args else "probe-session"
lf = cfg / "launch_count"
launch = int(lf.read_text()) + 1 if lf.exists() else 1
lf.write_text(str(launch))
owner = "host-%d" % launch
try:
    db = sqlite3.connect(cfg / "sessions.sqlite3")
    db.execute("CREATE TABLE IF NOT EXISTS session_entries(session_id TEXT NOT NULL, idx INTEGER NOT NULL, event TEXT NOT NULL, PRIMARY KEY(session_id, idx))")
    db.execute("CREATE TABLE IF NOT EXISTS session_leases(session_id TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at_ms INTEGER NOT NULL)")
    db.commit()
except sqlite3.DatabaseError:
    db = None
def now_ms():
    return int(time.time() * 1000)
def emit(kind, req_id=None, **fields):
    payload = {"type": kind, "session_id": session}
    if req_id is not None:
        payload["request_id"] = req_id
    payload.update(fields)
    print(json.dumps(payload), flush=True)
def record(msg):
    with (cfg / "wire.jsonl").open("a") as out:
        out.write(json.dumps({"launch": launch, "t_ms": now_ms(), "msg": msg}) + "\n")
def journal_user(text):
    n = db.execute("SELECT COUNT(*) FROM session_entries WHERE session_id = ?", (session,)).fetchone()[0]
    db.execute("INSERT INTO session_entries VALUES(?, ?, ?)", (session, n, json.dumps({"type": "text", "data": {"role": "user", "content": text}})))
    db.commit()
def journal_model():
    n = db.execute("SELECT COUNT(*) FROM session_entries WHERE session_id = ?", (session,)).fetchone()[0]
    db.execute("INSERT INTO session_entries VALUES(?, ?, ?)", (session, n, json.dumps({"type": "tool_call", "data": {"call": "probe"}})))
    db.commit()
def progress(note):
    with (cfg / "probe-progress.log").open("a") as out:
        out.write(json.dumps({"launch": launch, "t_ms": now_ms(), "note": note}) + "\n")
def claim(lease_ms):
    row = db.execute("SELECT owner, expires_at_ms FROM session_leases WHERE session_id = ?", (session,)).fetchone()
    if row is not None and row[0] != owner and row[1] > now_ms():
        return False
    db.execute("INSERT OR REPLACE INTO session_leases VALUES(?, ?, ?)", (session, owner, now_ms() + lease_ms))
    db.commit()
    return True
def release():
    db.execute("DELETE FROM session_leases WHERE session_id = ? AND owner = ?", (session, owner))
    db.commit()
def complete(req_id, text):
    if not claim(180000):
        emit("result", req_id, status="failed", is_error=True, error="This Session already has an active turn.")
        return
    journal_user(text)
    journal_model()
    emit("accepted", req_id, mode="turn")
    release()
    emit("result", req_id, status="completed", usage={"input_tokens": 1, "output_tokens": 1})
emit("ready")
"#;

struct SpironRig {
    store: std::sync::Arc<Store>,
    agent: anyagents_core::Agent,
    cfg: PathBuf,
    supervisor: Option<anyagents_supervisor::Supervisor<VendorHarness>>,
    _env: Environment,
    _temp: tempfile::TempDir,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl SpironRig {
    fn new(name: &str, body: &str) -> Self {
        let lock = crate::server::tests::env_lock();
        let temp = crate::test_tempdir();
        let probe = temp.path().join(format!("{name}-probe"));
        std::fs::write(&probe, format!("{SPIRON_JOURNAL_PROBE_PRELUDE}{body}")).unwrap();
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let env = Environment::set(&[
            ("ANYAGENTS_SPIRON_BIN", probe.as_os_str()),
            ("ANYAGENTS_FAKE_MODE", std::ffi::OsStr::new("1")),
        ]);
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let store = std::sync::Arc::new(Store::open(data.join("state.db")).unwrap());
        let mut spec = NewAgent::new(name, "spiron");
        spec.model = Some("probe/model".into());
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
        Self {
            store,
            agent,
            cfg,
            supervisor: None,
            _env: env,
            _temp: temp,
            _lock: lock,
        }
    }

    fn start(&mut self) {
        use anyagents_supervisor::{Supervisor, SystemClock};
        let probe = std::env::var_os("ANYAGENTS_SPIRON_BIN").unwrap();
        let probe = PathBuf::from(probe);
        self.supervisor = Some(
            Supervisor::<VendorHarness>::start(
                std::sync::Arc::clone(&self.store),
                std::sync::Arc::new(SystemClock::new()),
                std::sync::Arc::new(move |req| {
                    anyagents_core::host_session::ensure_active(
                        req.config_dir.parent().unwrap(),
                        "spiron",
                    )
                    .map_err(|e| e.to_string())?;
                    let session = session_from_file(&req.config_dir, "spiron-session-id")?;
                    let h = start_spiron(
                        &probe,
                        &req.config_dir,
                        &req.work_dir,
                        &req.agent,
                        session.as_deref(),
                        &[],
                    )?;
                    let vh = VendorHarness::Spiron(
                        Box::new(h),
                        req.config_dir.join("spiron-session-id"),
                        "spiron".into(),
                    );
                    vh.sync_session()?;
                    Ok(vh)
                }),
                vec![],
            )
            .unwrap(),
        );
    }

    fn receive(&self, text: &str) -> anyagents_core::Message {
        let ReceiveOutcome::New(message) = self
            .store
            .receive(&self.agent.id, Incoming::new(text))
            .unwrap()
        else {
            panic!("expected a new message");
        };
        self.store.poke(anyagents_core::Wake::Agent {
            agent_id: self.agent.id.clone(),
        });
        message
    }

    fn wire(&self) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.cfg.join("wire.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn wire_ids(&self) -> Vec<String> {
        self.wire()
            .iter()
            .map(|v| {
                v["msg"]["request_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    /// Snapshot for diagnosing a stall (the supervisor makes no progress).
    fn stall_report(&self) -> String {
        let hold = std::fs::read_to_string(self.cfg.join("spiron-unconfirmed-steers.json"))
            .map(|s| format!("{} bytes", s.len()))
            .unwrap_or_else(|_| "absent".to_string());
        let last_error = std::fs::read_to_string(self.cfg.join(".anyagents-last-error.json"))
            .unwrap_or_else(|_| "absent".to_string());
        let launch = std::fs::read_to_string(self.cfg.join("launch_count"))
            .unwrap_or_else(|_| "absent".to_string());
        let progress = std::fs::read_to_string(self.cfg.join("probe-progress.log"))
            .map(|s| {
                let tail = if s.len() > 800 {
                    &s[s.len() - 800..]
                } else {
                    &s
                };
                tail.trim_end().to_string()
            })
            .unwrap_or_else(|_| "absent".to_string());
        let journal = rusqlite::Connection::open(self.cfg.join("sessions.sqlite3"))
            .map(|db| {
                (
                    db.query_row("SELECT COUNT(*) FROM session_entries", [], |r| {
                        r.get::<_, i64>(0)
                    }),
                    db.query_row("SELECT COUNT(*) FROM session_leases", [], |r| {
                        r.get::<_, i64>(0)
                    }),
                )
            })
            .map(|(entries, leases)| format!("entries={entries:?} leases={leases:?}"))
            .unwrap_or_else(|_| "unreadable".to_string());
        let supervisor = self
            .supervisor
            .as_ref()
            .map(|s| s.live(&self.agent.id))
            .unwrap_or(None);
        let cursor = self.store.delivered_cursor(&self.agent.id);
        let next = self
            .store
            .next_undelivered(&self.agent.id)
            .map(|m| m.map(|m| m.id.to_string()));
        format!(
            "wire={:?} launched={launch} progress={progress} hold={hold} last_error={last_error} journal={journal} cursor={cursor:?} next={next:?} supervisor={supervisor:?}",
            self.wire()
        )
    }

    /// How many times the session journal recorded `id` as user input.
    fn journaled(&self, id: &str) -> usize {
        let db = rusqlite::Connection::open(self.cfg.join("sessions.sqlite3")).unwrap();
        let mut stmt = db
            .prepare("SELECT json_extract(event, '$.data.content') FROM session_entries")
            .unwrap();
        let marker = format!("anyagents-message:{id}");
        stmt.query_map([], |r| r.get::<_, Option<String>>(0))
            .unwrap()
            .flatten()
            .flatten()
            .filter(|c| c.split('\n').next() == Some(marker.as_str()))
            .count()
    }
}

impl Drop for SpironRig {
    fn drop(&mut self) {
        self.supervisor.take();
    }
}

#[test]
fn spiron_restart_resumes_consumed_inputs_and_redelivers_unconsumed_exactly_once() {
    // The first host admits m1, acknowledges the steer m2 on its socket, and
    // dies before m2 reaches the model: m2 is never journaled.
    let mut rig = SpironRig::new(
        "steer-crash",
        r#"
active = None
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    req_id = msg.get("request_id")
    if launch == 1 and active is None:
        active = req_id
        journal_user(msg["text"])
        journal_model()
        emit("accepted", req_id, mode="turn")
    elif launch == 1:
        emit("steer_ack", req_id, accepted=True, turn_id=active)
        time.sleep(0.05)
        sys.exit(1)
    else:
        complete(req_id, msg["text"])
"#,
    );
    let m1 = rig.receive("initial assignment");
    rig.start();
    // m1 reached the probe; mailbox cursor reads can trail the writer's WAL.
    eventually(|| {
        rig.wire()
            .iter()
            .any(|v| v["msg"]["request_id"].as_str() == Some(m1.id.as_str()))
    });
    let m2 = rig.receive("mid-turn steer detail");

    eventually(|| rig.wire().iter().filter(|v| v["launch"] == 2).count() >= 2);
    // The redelivered input's own hold shares the file with the reconciled
    // one; it clears when its turn confirms.
    eventually(|| !rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    let archived = rig.cfg.join("spiron-unconfirmed-steers.reconciled.json");
    let held =
        anyagents_harness_stream::InputHold::parse(&std::fs::read(&archived).unwrap()).unwrap();
    assert_eq!(held.request_ids, vec![m1.id.to_string(), m2.id.to_string()]);
    assert_eq!(held.session_id.as_deref(), Some("probe-session"));
    assert!(!rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    assert!(!rig.cfg.join(".anyagents-last-error.json").exists());

    let launch2: Vec<_> = rig
        .wire()
        .into_iter()
        .filter(|v| v["launch"] == 2)
        .collect();
    assert_eq!(
        launch2.len(),
        2,
        "recovery, then the one redelivery: {launch2:?}"
    );
    let recovery = launch2[0]["msg"]["text"].as_str().unwrap();
    assert!(recovery.contains("[Recovery] Your previous turn was interrupted"));
    assert!(recovery.contains("initial assignment"));
    assert!(!recovery.contains("mid-turn steer detail"));
    assert!(recovery.contains(&format!(
        "received by the session before the interruption: [{}]",
        m1.id
    )));
    assert!(recovery.contains(&format!("delivered again after this message: [{}]", m2.id)));
    assert_eq!(launch2[1]["msg"]["request_id"], m2.id.as_str());
    assert_eq!(
        rig.journaled(m1.id.as_str()),
        1,
        "consumed input is never replayed"
    );
    assert_eq!(
        rig.journaled(m2.id.as_str()),
        1,
        "unconsumed input arrives once"
    );
}

/// Isolated pending the live-process ack stall tracked in the follow-up issue:
/// the probe records m1, journals it, and emits accepted, yet the mailbox never
/// accepts it within 30s while the supervisor stays healthy. Stall-state
/// instrumentation (probe progress log, stall report) is kept to diagnose it.
#[test]
#[ignore]
fn spiron_unconfirmed_steer_in_a_live_process_reconciles_without_restart() {
    // A gateway failure ends the turn with the steer acknowledged but never
    // injected; the process stays alive.
    let mut rig = SpironRig::new(
        "steer-live",
        r#"
active = None
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    progress("recorded")
    req_id = msg.get("request_id")
    if launch == 1 and active is None:
        active = req_id
        claim(180000)
        progress("claimed")
        journal_user(msg["text"])
        progress("journaled-user")
        journal_model()
        progress("journaled-model")
        emit("accepted", req_id, mode="turn")
        progress("emitted-accepted")
        time.sleep(0.2)
        emit("accepted", req_id, mode="turn")
    elif launch == 1 and active != "done":
        emit("steer_ack", req_id, accepted=True, turn_id=active)
        release()
        emit("result", active, status="failed", is_error=True, error="service_unavailable", undelivered_steers=["acknowledged input"])
        active = "done"
    else:
        complete(req_id, msg["text"])
"#,
    );
    let m1 = rig.receive("long running task");
    rig.start();
    // Delivery reached the probe first (wire capture is the freshest
    // signal); the mailbox cursor may lag the writer's commit.
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.wire().is_empty() {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.journaled(m1.id.as_str()) < 1 {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    let m2 = rig.receive("steer cut off by the gateway");

    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.wire().len() < 4 {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.cfg.join("spiron-unconfirmed-steers.json").exists() {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    let wire = rig.wire();
    assert!(
        wire.iter().all(|v| v["launch"] == 1),
        "the live process recovers in place: {wire:?}"
    );
    assert_eq!(wire.len(), 4, "{wire:?}");
    assert!(
        wire[2]["msg"]["text"]
            .as_str()
            .unwrap()
            .starts_with(&format!("anyagents-message:recovery_{}", m1.id))
    );
    assert_eq!(rig.wire_ids()[3], m2.id.as_str());
    assert_eq!(rig.journaled(m1.id.as_str()), 1);
    assert_eq!(rig.journaled(m2.id.as_str()), 1);
    assert!(!rig.cfg.join("spiron-unconfirmed-steers.json").exists());
}

#[test]
fn spiron_stranded_turn_lease_defers_recovery_instead_of_failing_admission() {
    // The first host dies mid-turn holding the session lease; the replacement
    // must not start a turn Spiron would refuse with "already has an active
    // turn", and resumes once the stranded lease lapses.
    let mut rig = SpironRig::new(
        "stranded-lease",
        r#"
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    req_id = msg.get("request_id")
    if launch == 1:
        claim(2500)
        (cfg / "stranded_until_ms").write_text(str(now_ms() + 2500))
        journal_user(msg["text"])
        journal_model()
        emit("accepted", req_id, mode="turn")
        time.sleep(0.05)
        os._exit(1)
    else:
        complete(req_id, msg["text"])
"#,
    );
    let m1 = rig.receive("work cut off by the gateway");
    rig.start();
    eventually(|| rig.wire().iter().any(|v| v["launch"] == 2));
    eventually(|| !rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    let stranded_until: i64 = std::fs::read_to_string(rig.cfg.join("stranded_until_ms"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let launch2: Vec<_> = rig
        .wire()
        .into_iter()
        .filter(|v| v["launch"] == 2)
        .collect();
    assert_eq!(
        launch2.len(),
        1,
        "one recovery, no refused attempt: {launch2:?}"
    );
    assert!(launch2[0]["t_ms"].as_i64().unwrap() >= stranded_until);
    assert!(
        launch2[0]["msg"]["text"]
            .as_str()
            .unwrap()
            .contains("[Recovery] Your previous turn was interrupted")
    );
    assert_eq!(rig.journaled(m1.id.as_str()), 1);
    let fault = rig.cfg.join(".anyagents-last-error.json");
    assert!(
        !std::fs::read_to_string(&fault)
            .unwrap_or_default()
            .contains("already has an active turn")
    );
}

#[test]
fn spiron_unreadable_journal_keeps_custody_and_delivers_nothing() {
    let mut rig = SpironRig::new(
        "unreadable-journal",
        r#"
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    complete(msg.get("request_id"), msg["text"])
"#,
    );
    let m1 = rig.receive("held before restart");
    rig.store.note_delivered(&rig.agent.id, m1.seq).unwrap();
    std::fs::write(
        rig.cfg.join("spiron-unconfirmed-steers.json"),
        json!({"turn_id": m1.id.as_str(), "request_ids": [m1.id.as_str()]}).to_string(),
    )
    .unwrap();
    std::fs::write(rig.cfg.join("sessions.sqlite3"), b"not a database").unwrap();
    rig.start();
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        rig.wire().is_empty(),
        "nothing is replayed blind: {:?}",
        rig.wire()
    );
    assert!(rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    eventually(|| rig.store.delivered_cursor(&rig.agent.id).unwrap() == m1.seq);
}

#[test]
fn spiron_lost_hold_recovers_from_mailbox_without_replaying_consumed() {
    // Both steers were delivered before the hold file was truncated to 0
    // bytes. The journal proves m1 was processed and m2 was journaled but
    // never processed (only `done` after it). Reconciliation must recover
    // m1 without raw replay and return m2 for exactly one redelivery.
    let mut rig = SpironRig::new(
        "lost-hold",
        r#"
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    complete(msg.get("request_id"), msg["text"])
"#,
    );
    let m1 = rig.receive("consumed steer");
    rig.store.note_delivered(&rig.agent.id, m1.seq).unwrap();
    let m2 = rig.receive("unconsumed steer");
    rig.store.note_delivered(&rig.agent.id, m2.seq).unwrap();
    std::fs::write(rig.cfg.join("spiron-unconfirmed-steers.json"), b"").unwrap();
    spiron_journal(
        &rig.cfg,
        &[
            ("probe-session", user_input(m1.id.as_ref())),
            ("probe-session", tool_call()),
            ("probe-session", user_input(m2.id.as_ref())),
            ("probe-session", json!({"type": "done"})),
        ],
    );
    rig.start();
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.wire().len() < 2 {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    eventually(|| !rig.cfg.join("spiron-unconfirmed-steers.json").exists());

    let wire = rig.wire();
    assert_eq!(wire.len(), 2, "{wire:?}");
    assert!(wire.iter().all(|v| v["launch"] == 1));
    let recovery = wire[0]["msg"]["text"].as_str().unwrap();
    assert!(
        recovery.starts_with(&format!("anyagents-message:recovery_{}", m1.id)),
        "{recovery}"
    );
    assert!(recovery.contains("[Recovery] Your previous turn was interrupted"));
    assert!(recovery.contains("consumed steer"));
    assert!(!recovery.contains("unconsumed steer"));
    assert!(recovery.contains(&format!(
        "received by the session before the interruption: [{}]",
        m1.id
    )));
    assert!(recovery.contains(&format!("delivered again after this message: [{}]", m2.id)));
    assert_eq!(wire[1]["msg"]["request_id"], m2.id.as_str());
    // The consumed input never re-appears as a raw request; the unconsumed
    // one arrives once more after the fixture's pre-crash journal entry.
    assert_eq!(rig.journaled(m1.id.as_str()), 1);
    // The probe journals the redelivery just after recording it on the wire.
    eventually(|| rig.journaled(m2.id.as_str()) == 2);
    assert!(!rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    // The mailbox cursor may lag the writer's WAL; the wire and journal
    // above already prove the deliveries.
    eventually(|| rig.store.delivered_cursor(&rig.agent.id).unwrap() == m2.seq);
}

#[test]
fn spiron_lost_hold_promotes_consumed_in_flight_input_without_replaying_it() {
    // m1 was delivered; m2 was consumed but never cursor-recorded (crash
    // between the harness taking it and the host noting it). The lost hold
    // falls back to mailbox custody, promotes m2 into the interrupted turn,
    // and replays neither.
    let mut rig = SpironRig::new(
        "lost-hold-inflight",
        r#"
for raw in sys.stdin:
    msg = json.loads(raw)
    record(msg)
    complete(msg.get("request_id"), msg["text"])
"#,
    );
    let m1 = rig.receive("delivered steer");
    rig.store.note_delivered(&rig.agent.id, m1.seq).unwrap();
    let m2 = rig.receive("in flight steer");
    std::fs::write(rig.cfg.join("spiron-unconfirmed-steers.json"), b"").unwrap();
    spiron_journal(
        &rig.cfg,
        &[
            ("probe-session", user_input(m1.id.as_ref())),
            ("probe-session", tool_call()),
            ("probe-session", user_input(m2.id.as_ref())),
            ("probe-session", tool_call()),
        ],
    );
    rig.start();
    // Reconciliation cleared the lost hold; the recovery turn's own hold
    // clears when it confirms.
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.wire().is_empty() {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while rig.cfg.join("spiron-unconfirmed-steers.json").exists() {
        assert!(Instant::now() < deadline, "{}", rig.stall_report());
        std::thread::sleep(Duration::from_millis(5));
    }

    let wire = rig.wire();
    assert_eq!(wire.len(), 1, "recovery only: {wire:?}");
    let recovery = wire[0]["msg"]["text"].as_str().unwrap();
    assert!(recovery.contains("[Recovery] Your previous turn was interrupted"));
    assert!(recovery.contains("delivered steer"));
    assert!(recovery.contains("in flight steer"));
    assert!(recovery.contains(&format!(
        "received by the session before the interruption: [{}, {}]",
        m1.id, m2.id
    )));
    assert_eq!(rig.journaled(m1.id.as_str()), 1);
    assert_eq!(rig.journaled(m2.id.as_str()), 1);
    assert!(!rig.cfg.join("spiron-unconfirmed-steers.json").exists());
    // The mailbox cursor may lag the writer's WAL; the wire and journal
    // above already prove the deliveries.
    eventually(|| rig.store.delivered_cursor(&rig.agent.id).unwrap() == m2.seq);
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
                req.config_dir.join("spiron-session-id"),
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

#[test]
fn spiron_pause_and_reset_session_clears_hold_and_resumes_new_generation_via_recovery() {
    use anyagents_supervisor::{Supervisor, SystemClock};
    use std::sync::Arc;

    let _lock = crate::server::tests::env_lock();
    let temp = crate::test_tempdir();
    let probe = temp.path().join("spiron-reset-probe");
    std::fs::write(
        &probe,
        r#"#!/usr/bin/env python3
import json, os, pathlib, sys
cfg = pathlib.Path(sys.argv[sys.argv.index("--state-dir") + 1])
args = sys.argv[1:]
session = args[args.index("--session") + 1] if "--session" in args else "brand-new-spiron-session"
with (cfg / "sessions_seen.txt").open("a") as out:
    out.write(session + "\n")
print(json.dumps({"type": "ready", "session_id": session}), flush=True)
for raw in sys.stdin:
    msg = json.loads(raw)
    with (cfg / "wire.jsonl").open("a") as out:
        out.write(json.dumps(msg) + "\n")
    req_id = msg["request_id"]
    print(json.dumps({"type": "accepted", "session_id": session, "request_id": req_id, "mode": "turn"}), flush=True)
    print(json.dumps({"type": "result", "session_id": session, "request_id": req_id, "status": "completed", "usage": {"input_tokens": 6, "output_tokens": 2}}), flush=True)
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
    let mut spec = NewAgent::new("spiron-reset-node", "spiron");
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

    anyagents_core::host_session::ensure_active(&agent_root, "spiron").unwrap();
    anyagents_core::host_session::bind_vendor(&agent_root, "spiron", "old-stuck-session").unwrap();
    std::fs::write(cfg.join("spiron-session-id"), "old-stuck-session\n").unwrap();

    let ReceiveOutcome::New(m1) = store
        .receive(&agent.id, Incoming::new("task part 1"))
        .unwrap()
    else {
        panic!("expected m1");
    };
    store.note_delivered(&agent.id, m1.seq).unwrap();
    let ReceiveOutcome::New(m2) = store
        .receive(&agent.id, Incoming::new("task part 2"))
        .unwrap()
    else {
        panic!("expected m2");
    };
    // Simulate unconfirmed steer hold where m2 cursor had even been rewound!
    std::fs::write(
        cfg.join("spiron-unconfirmed-steers.json"),
        json!({"turn_id": m1.id, "request_ids": [m1.id, m2.id]}).to_string(),
    )
    .unwrap();
    std::fs::write(
        cfg.join(".anyagents-last-error.json"),
        r#"{"error":"Spiron reported unconfirmed steering input; Reconcile spiron-unconfirmed-steers.json","timestamp_ms":1}"#,
    )
    .unwrap();
    let paused = store.set_agent_state(&agent.id, "paused").unwrap();
    let reset_out = crate::create::reset_agent_session_locked(&store, &paused).unwrap();
    let new_gen = reset_out["generation"].as_str().unwrap();
    assert!(!cfg.join("spiron-unconfirmed-steers.json").exists());
    assert!(!cfg.join(".anyagents-last-error.json").exists());
    assert!(!cfg.join("spiron-session-id").exists());
    assert!(
        agent_root
            .join("sessions")
            .join(format!("retired-{new_gen}"))
            .join("spiron-unconfirmed-steers.json")
            .is_file()
    );

    store
        .set_agent_running(&agent.id, &work.display().to_string())
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
                req.config_dir.join("spiron-session-id"),
                "spiron".into(),
            );
            vh.sync_session()?;
            Ok(vh)
        }),
        vec![],
    )
    .unwrap();

    eventually(|| !store.has_open_turn(&agent.id).unwrap());
    let seen_sessions = std::fs::read_to_string(cfg.join("sessions_seen.txt")).unwrap();
    assert_eq!(seen_sessions.trim(), "brand-new-spiron-session");
    let wire_lines: Vec<serde_json::Value> = std::fs::read_to_string(cfg.join("wire.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(wire_lines.len(), 1);
    let text = wire_lines[0]["text"].as_str().unwrap();
    assert!(text.contains("[Recovery] Your previous turn was interrupted"));
    assert!(text.contains("task part 1"));
    assert!(text.contains("task part 2"));
    assert!(!cfg.join(".anyagents-session-reset-recovery").exists());
    drop(supervisor);
}

fn spiron_journal(cfg: &Path, rows: &[(&str, serde_json::Value)]) {
    let db = rusqlite::Connection::open(cfg.join("sessions.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS session_entries(session_id TEXT NOT NULL, idx INTEGER NOT NULL, event TEXT NOT NULL, PRIMARY KEY(session_id, idx));
         CREATE TABLE IF NOT EXISTS session_leases(session_id TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at_ms INTEGER NOT NULL);",
    )
    .unwrap();
    for (session, event) in rows {
        let idx: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM session_entries WHERE session_id = ?1",
                [session],
                |r| r.get(0),
            )
            .unwrap();
        db.execute(
            "INSERT INTO session_entries(session_id, idx, event) VALUES (?1, ?2, ?3)",
            rusqlite::params![session, idx, event.to_string()],
        )
        .unwrap();
    }
}

fn user_input(id: &str) -> serde_json::Value {
    json!({"type": "text", "data": {"role": "user", "content": format!("anyagents-message:{id}\n<message id=\"{id}\">\n<body>\nwork\n</body>\n</message>")}})
}

fn tool_call() -> serde_json::Value {
    json!({"type": "tool_call", "data": {"call": "probe"}})
}

fn assistant_text(content: &str) -> serde_json::Value {
    json!({"type": "text", "data": {"role": "assistant", "content": content}})
}

fn tool_result(content: &str) -> serde_json::Value {
    json!({"type": "tool_result", "data": {"result": content}})
}

#[test]
fn spiron_journal_consumption_is_exact_user_input_only() {
    let temp = crate::test_tempdir();
    let cfg = temp.path();
    // Entry order is idx order per session. Unconsumed cases sit after every
    // model event in their session, so nothing later can prove them.
    spiron_journal(
        cfg,
        &[
            ("s-old", user_input("msg_consumed")),
            ("s-old", tool_call()),
            ("s-old", user_input("msg_assistant")),
            ("s-old", assistant_text("ok")),
            // Mentions of an ID anywhere but a user input's marker line prove
            // nothing about consumption.
            (
                "s-old",
                assistant_text("anyagents-message:msg_quoted\nreply"),
            ),
            ("s-old", tool_result("anyagents-message:msg_tool\nmsg_tool")),
            (
                "s-old",
                json!({"type": "text", "data": {"role": "user", "content": "please look at anyagents-message:msg_inline"}}),
            ),
            ("s-old", user_input("msg_prefix_longer")),
            ("s-old", user_input("msg_done_only")),
            ("s-old", json!({"type": "done"})),
            ("s-old", user_input("msg_toolresult_only")),
            ("s-old", tool_result("out")),
            ("s-old", user_input("msg_journaled_only")),
            ("s-other", user_input("msg_elsewhere")),
            ("s-other", tool_call()),
        ],
    );
    let ids: Vec<String> = [
        "msg_consumed",
        "msg_assistant",
        "msg_quoted",
        "msg_tool",
        "msg_inline",
        "msg_prefix",
        "msg_done_only",
        "msg_toolresult_only",
        "msg_journaled_only",
        "msg_elsewhere",
        "msg_absent",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let scoped = spiron_journal_consumed(cfg, Some("s-old"), &ids).unwrap();
    assert_eq!(
        scoped,
        ["msg_consumed".to_string(), "msg_assistant".to_string()]
            .into_iter()
            .collect(),
        "only the hold's own session counts when it is recorded"
    );
    let legacy = spiron_journal_consumed(cfg, None, &ids).unwrap();
    assert_eq!(
        legacy,
        [
            "msg_consumed".to_string(),
            "msg_assistant".to_string(),
            "msg_elsewhere".to_string()
        ]
        .into_iter()
        .collect(),
        "a hold without a session is matched across this state directory"
    );
    // Journaled but followed only by done, a tool result, or nothing is
    // never processed.
    assert!(!scoped.contains("msg_done_only"));
    assert!(!scoped.contains("msg_toolresult_only"));
    assert!(!scoped.contains("msg_journaled_only"));
    assert!(!legacy.contains("msg_done_only"));
    assert!(!legacy.contains("msg_toolresult_only"));
    assert!(!legacy.contains("msg_journaled_only"));
}

#[test]
fn spiron_journal_missing_is_empty_but_unreadable_fails_closed() {
    let temp = crate::test_tempdir();
    let ids = vec!["msg_1".to_string()];
    let missing = temp.path().join("missing");
    std::fs::create_dir_all(&missing).unwrap();
    assert!(
        spiron_journal_consumed(&missing, None, &ids)
            .unwrap()
            .is_empty()
    );

    let garbage = temp.path().join("garbage");
    std::fs::create_dir_all(&garbage).unwrap();
    std::fs::write(garbage.join("sessions.sqlite3"), b"not a database at all").unwrap();
    assert!(spiron_journal_consumed(&garbage, None, &ids).is_err());

    let dir_not_file = temp.path().join("dir");
    std::fs::create_dir_all(dir_not_file.join("sessions.sqlite3")).unwrap();
    assert!(spiron_journal_consumed(&dir_not_file, None, &ids).is_err());

    let no_table = temp.path().join("no-table");
    std::fs::create_dir_all(&no_table).unwrap();
    rusqlite::Connection::open(no_table.join("sessions.sqlite3"))
        .unwrap()
        .execute_batch("CREATE TABLE unrelated(x)")
        .unwrap();
    assert!(spiron_journal_consumed(&no_table, None, &ids).is_err());
}

#[test]
fn spiron_lease_expiry_reports_only_a_live_foreign_lease() {
    let temp = crate::test_tempdir();
    let cfg = temp.path();
    assert_eq!(spiron_live_lease_expiry(cfg, "s1", 1_000).unwrap(), None);
    spiron_journal(cfg, &[]);
    assert_eq!(spiron_live_lease_expiry(cfg, "s1", 1_000).unwrap(), None);
    let db = rusqlite::Connection::open(cfg.join("sessions.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO session_leases(session_id, owner, expires_at_ms) VALUES ('s1', 'previous-host', 5000)",
        [],
    )
    .unwrap();
    drop(db);
    assert_eq!(
        spiron_live_lease_expiry(cfg, "s1", 1_000).unwrap(),
        Some(5_000)
    );
    assert_eq!(spiron_live_lease_expiry(cfg, "s1", 5_000).unwrap(), None);
    assert_eq!(spiron_live_lease_expiry(cfg, "s2", 1_000).unwrap(), None);
}
