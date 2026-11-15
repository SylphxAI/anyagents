//! Native Unix process exits retain useful, bounded and masked diagnostics.
use std::path::Path;
use std::time::{Duration, Instant};

use anyagents_harness::{Harness, OutboundMessage};
use anyagents_harness_spiron::{ListenError, SpironHarness, SpironLaunch};

const PEER: &str = r#"
import json, os, pathlib, socket, sys
mode = sys.argv[1]
path = sys.argv[sys.argv.index('--listen') + 1]
def diagnostic():
    canary='SYNTHETIC_FIXTURE_0123456789012345'
    forms=['Bearer '+canary,'bearer '+canary,'api_key='+canary,'sk-'+canary,'xoxb-'+canary,
        'https://fixture:'+canary+'@example.invalid/path',
        'https://example.invalid/path?access_token='+canary+'&page=2',
        'OPENAI_API_KEY='+canary,'X-Api-Key: '+canary]
    sys.stderr.write('failed to parse config.toml; 測試🙂\n'+'\n'.join(forms)+'\nlong actionable diagnostic\n')
    sys.stderr.write('x'*200000+'api_key='+canary)
    sys.stderr.flush()
if mode.startswith('startup'):
    diagnostic()
    sys.exit(1 if mode=='startup-error' else 0)
listener=socket.socket(socket.AF_UNIX)
listener.bind(path)
listener.listen()
while True:
    stream, _ = listener.accept()
    reader=stream.makefile('r')
    line=reader.readline()
    if not line:
        reader.close(); stream.close(); continue
    attach=json.loads(line)
    assert attach['type']=='attach' and attach['protocol']=='spiron.headless/v2'
    def emit(seq, kind, request_id):
        stream.sendall((json.dumps(dict(type=kind,seq=seq,session_id='native',request_id=request_id,protocol='spiron.headless/v2'))+'\n').encode())
    emit(1,'ready',attach['request_id'])
    msg=json.loads(reader.readline())
    assert msg['type']=='prompt' and msg['request_id']
    emit(2,'accepted',msg['request_id'])
    diagnostic()
    sys.exit(1 if mode=='input-error' else 0)
"#;

fn launch(root: &Path, mode: &str) -> Result<SpironHarness, ListenError> {
    let script = root.join("peer.py");
    std::fs::write(&script, PEER).unwrap();
    SpironHarness::launch_listening(
        SpironLaunch {
            program: "/usr/bin/python3".into(),
            args: vec![script.display().to_string(), mode.into()],
            environment: Vec::new(),
            work_dir: root.into(),
            session: None,
            state_dir: root.into(),
        },
        &root.join("s.sock"),
        &root.join("spiron.log"),
        Duration::from_secs(10),
        |_| Ok(()),
    )
}

fn check(error: &str) {
    assert!(error.contains("exit status: 1"), "{error}");
    assert!(
        error.contains("failed to parse config.toml; 測試🙂"),
        "{error}"
    );
    assert!(error.contains("long actionable diagnostic"));
    assert!(error.contains("stderr truncated"));
    assert_eq!(error.matches("[redacted]").count(), 9, "{error}");
    assert!(!error.contains("SYNTHETIC_FIXTURE"));
    assert!(!error.contains("api_key=SYNTH"));
    assert!(error.len() < 17 * 1024);
}

#[test]
fn startup_exit_retains_the_bounded_masked_prefix_and_status() {
    let dir = tempfile::tempdir().unwrap();
    let Some(ListenError::Failed(error)) = launch(dir.path(), "startup-error").err() else {
        panic!("startup must fail before ready")
    };
    check(&error);
}

#[test]
fn native_exit_after_input_persists_faults_but_success_does_not_fault() {
    for (mode, failed) in [("input-error", true), ("input-success", false)] {
        let dir = tempfile::tempdir().unwrap();
        let mut harness = launch(dir.path(), mode).unwrap();
        let message = OutboundMessage {
            id: "input".into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        };
        let _ = harness.deliver(&message).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while harness.health().process_alive {
            assert!(Instant::now() < deadline, "native process never exited");
            std::thread::sleep(Duration::from_millis(5));
        }
        let fault = harness.health().sign_in_fault;
        let path = dir.path().join(".anyagents-last-error.json");
        if failed {
            let fault = fault.expect("nonzero native exit must retain diagnostic");
            check(&fault);
            let persisted: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(persisted["error"], fault);
        } else {
            assert!(fault.is_none(), "{fault:?}");
            assert!(!path.exists());
        }
    }
}
