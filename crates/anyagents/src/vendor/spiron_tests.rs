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
fn eventually(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let temp = tempfile::tempdir().unwrap();
    let probe = temp.path().join("spiron-probe");
    std::fs::write(&probe, r#"#!/usr/bin/python3
import sys, os, json, pathlib, time
root = pathlib.Path(os.environ['ANYAGENTS_AGENT_CONFIG'])
args = sys.argv[1:]
with (root/'launches.jsonl').open('a') as out:
    out.write(json.dumps({'args':args, 'cwd':os.getcwd(), 'home':os.environ['HOME']})+'\n')
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
    for harness_name in ["spiron", "sylphx", "sylphx/default"] {
        let data = temp.path().join(harness_name.replace('/', "-"));
        std::fs::create_dir_all(&data).unwrap();
        let store = Store::open(data.join("state.db")).unwrap();
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
        for launch in launches {
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

fn external_probe(temp: &Path, script: &str) -> (Store, anyagents_core::Agent, StreamHarness) {
    let config = temp.join("config");
    let work = temp.join("work");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    let program = temp.join("peer");
    std::fs::write(&program, format!("#!/usr/bin/python3\n{script}")).unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = Store::open(temp.join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("external-probe", "spiron"))
        .unwrap();
    let harness = start_spiron(&program, &config, &work, &agent, None, &[]).unwrap();
    (store, agent, harness)
}

#[test]
fn spiron_external_cancelled_unconfirmed_steer_retains_mailbox_custody_across_restart() {
    use anyagents_agent::DeliveryLoop;
    let temp = tempfile::tempdir().unwrap();
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
    let (store, agent, harness) = external_probe(temp.path(), script);
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
    let temp = tempfile::tempdir().unwrap();
    let script = r#"
import sys,json,pathlib
for line in sys.stdin:
 m=json.loads(line)
 with pathlib.Path(__file__).with_name('wire.jsonl').open('a') as out:
  out.write(json.dumps(m)+'\n')
 print(json.dumps({'type':'accepted','request_id':m['request_id']}),flush=True)
"#;
    let (store, agent, harness) = external_probe(temp.path(), script);
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
        VendorHarness::Spiron(Box::new(restarted), config.join("spiron-session-id")),
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
    let temp = tempfile::tempdir().unwrap();
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
    let (store, agent, mut harness) = external_probe(temp.path(), script);
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
        let temp = tempfile::tempdir().unwrap();
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
        let (_, _, mut harness) = external_probe(temp.path(), &script);
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
