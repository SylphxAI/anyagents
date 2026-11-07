//! Executable protocol probe: no model, network, or production state is used.
use super::*;
use anyagents_core::{Incoming, NewAgent, NewSchedule, Store};
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
        eventually(|| {
            matches!(
                vendor.deliver(&message("steer")),
                Ok(DeliverOutcome::Accepted {
                    turn_finished: false
                })
            )
        });
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
