//! Real end-to-end loopback tests for native Remote User-Device / Edge Nodes
//! (`anyagents node pair/run/status/pause/resume/revoke` + Coordinator REST +
//! outbound WebSocket reverse-tunnel + `nodes` MCP tool + process-group kill-switch).

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::{NewAgent, Store};
use serde_json::{Value, json};

const OPERATOR_TOKEN: &str = "test-node-e2e-operator-token-0123456789";

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn take(&mut self) -> Option<Child> {
        self.0.take()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .into()
}

fn read_json(resp: &mut ureq::http::Response<ureq::Body>) -> (u16, Value) {
    use std::io::Read as _;
    let status = resp.status().as_u16();
    let mut buf = Vec::new();
    let _ = resp
        .body_mut()
        .as_reader()
        .take(4 * 1024 * 1024)
        .read_to_end(&mut buf);
    let val = serde_json::from_slice(&buf)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&buf).into_owned() }));
    (status, val)
}

fn call_mcp(agent: &ureq::Agent, base_url: &str, remote_token: &str, args: Value) -> (u16, Value) {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "nodes",
            "arguments": args
        }
    });
    let mut resp = agent
        .post(&format!("{base_url}/mcp"))
        .header("authorization", format!("Bearer {remote_token}"))
        .header("content-type", "application/json")
        .send_json(&payload)
        .expect("POST /mcp");
    read_json(&mut resp)
}

fn op_get(agent: &ureq::Agent, base_url: &str, path: &str) -> (u16, Value) {
    let mut resp = agent
        .get(&format!("{base_url}{path}"))
        .header("x-anyagents-operator", OPERATOR_TOKEN)
        .header("accept", "application/json")
        .call()
        .expect("operator GET");
    read_json(&mut resp)
}

fn op_post(agent: &ureq::Agent, base_url: &str, path: &str, body: Value) -> (u16, Value) {
    let mut resp = agent
        .post(&format!("{base_url}{path}"))
        .header("x-anyagents-operator", OPERATOR_TOKEN)
        .header("content-type", "application/json")
        .send_json(&body)
        .expect("operator POST");
    read_json(&mut resp)
}

fn pid_alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn wait_for_file(path: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(path) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for non-empty file {}", path.display());
}

fn setup_isolated_node_cmd(bin: &str, data: &Path, cfg: &Path) -> Command {
    let mut cmd = Command::new(bin);
    cmd.env("ANYAGENTS_TESTS", "contract-double")
        .env("ANYAGENTS_OPERATOR_TOKEN", OPERATOR_TOKEN)
        .env_remove("ANYAGENTS_CONTROL_AUTH_FILE")
        .env_remove("ANYAGENTS_CONTROL_URL")
        .arg("--data")
        .arg(data)
        .arg("node")
        .arg("--config")
        .arg(cfg);
    cmd
}

#[test]
fn e2e_node_pair_ws_tunnel_mcp_list_exec_denial_and_kill_switch() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let cfg = tmp.path().join("cfg").join("node.json");

    // Pre-create two sandboxed agents with remote session tokens for real HTTP MCP calls.
    let (alpha_id, alpha_token, beta_token) = {
        let store = Store::open(data.join("state.db")).unwrap();
        let alpha = store
            .create_agent(NewAgent::new("node-alpha", "claude-cloud"))
            .unwrap();
        let beta = store
            .create_agent(NewAgent::new("node-beta", "claude-cloud"))
            .unwrap();
        store
            .set_agent_running(&alpha.id, &data.join("alpha-work").to_string_lossy())
            .unwrap();
        store
            .set_agent_running(&beta.id, &data.join("beta-work").to_string_lossy())
            .unwrap();
        let tok_a = store
            .begin_remote_session(&alpha.id, 3_600_000)
            .unwrap()
            .token;
        let tok_b = store
            .begin_remote_session(&beta.id, 3_600_000)
            .unwrap()
            .token;
        (alpha.id.to_string(), tok_a, tok_b)
    };

    let bin = env!("CARGO_BIN_EXE_anyagents");
    let server_err = tmp.path().join("server.err");
    let server_child = Command::new(bin)
        .env("ANYAGENTS_TESTS", "contract-double")
        .env("ANYAGENTS_SERVERLESS", "true")
        .env("ANYAGENTS_WEBHOOK_LISTEN", "127.0.0.1:0")
        .env("ANYAGENTS_OPERATOR_TOKEN", OPERATOR_TOKEN)
        .env("ANYAGENTS_PUBLIC_URL", "http://127.0.0.1:8788")
        .env(
            "ANYAGENTS_CAPABILITY_KEY",
            "test-capability-key-0123456789abcdef",
        )
        .arg("--data")
        .arg(&data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(fs::File::create(&server_err).unwrap())
        .spawn()
        .expect("spawn coordinator");
    let _server = ChildGuard::new(server_child);

    let webhook_addr = wait_for_file(&data.join("webhook.addr"), Duration::from_secs(10));
    let base_url = format!("http://{webhook_addr}");
    let http = http_agent();

    // Wait until /healthz responds 200.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = http.get(&format!("{base_url}/healthz")).call()
            && resp.status().as_u16() == 200
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "coordinator never became ready: {}",
            fs::read_to_string(&server_err).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // 1. Pair node via `anyagents node pair`.
    let pair_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args(["pair", "--url", &base_url, "--name", "dev-laptop", "--json"])
        .output()
        .expect("run node pair");
    assert!(
        pair_out.status.success(),
        "node pair failed: {}",
        String::from_utf8_lossy(&pair_out.stderr)
    );
    let pair_json: Value = serde_json::from_slice(&pair_out.stdout).unwrap();
    let node_id = pair_json["node_id"].as_str().unwrap().to_string();
    assert!(node_id.starts_with("nd_"), "{pair_json}");

    // Verify strict 0600 file permissions and that stdout never leaked the token.
    let mode = fs::metadata(&cfg).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "node.json must have 0600 permissions");
    let stdout_str = String::from_utf8_lossy(&pair_out.stdout);
    assert!(
        !stdout_str.contains("ndt_"),
        "stdout must never print node_token"
    );

    // 2. Start `anyagents node run` outbound WS client.
    let node_err = tmp.path().join("node.err");
    let runner_child = setup_isolated_node_cmd(bin, &data, &cfg)
        .arg("run")
        .stdout(Stdio::null())
        .stderr(fs::File::create(&node_err).unwrap())
        .spawn()
        .expect("spawn node run");
    let mut _runner = ChildGuard::new(runner_child);

    // Wait for status == "online".
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (st, val) = op_get(&http, &base_url, &format!("/v1/nodes/{node_id}"));
        if st == 200 && val["node"]["status"] == "online" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node did not come online: {val} stderr={}",
            fs::read_to_string(&node_err).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // 3. Before grant: `nodes(list)` returns empty for both alpha and beta; `nodes(exec)` denied.
    let (st, list_before) = call_mcp(&http, &base_url, &alpha_token, json!({ "action": "list" }));
    assert_eq!(st, 200);
    assert_eq!(
        list_before["result"]["structuredContent"]["nodes"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    let sentinel = tmp.path().join("ungranted-sentinel");
    let (_, denied_exec) = call_mcp(
        &http,
        &base_url,
        &beta_token,
        json!({
            "action": "exec",
            "node_id": node_id,
            "command": "touch",
            "args": [sentinel.to_string_lossy()],
        }),
    );
    assert_eq!(denied_exec["result"]["isError"], true, "{denied_exec}");
    assert!(!sentinel.exists(), "unauthorized exec must never run");

    // 4. Grant `exec` to alpha (not beta).
    let (st, grant_resp) = op_post(
        &http,
        &base_url,
        &format!("/v1/nodes/{node_id}/grants"),
        json!({ "agent_id": alpha_id, "capability": "exec" }),
    );
    assert_eq!(st, 201, "{grant_resp}");

    let (_, list_alpha) = call_mcp(&http, &base_url, &alpha_token, json!({ "action": "list" }));
    let alpha_nodes = list_alpha["result"]["structuredContent"]["nodes"]
        .as_array()
        .unwrap();
    assert_eq!(alpha_nodes.len(), 1);
    assert_eq!(alpha_nodes[0]["id"], node_id);
    assert_eq!(alpha_nodes[0]["status"], "online");

    let (_, list_beta) = call_mcp(&http, &base_url, &beta_token, json!({ "action": "list" }));
    assert_eq!(
        list_beta["result"]["structuredContent"]["nodes"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // 5. Granted exec runs real command, captures stdout/stderr, masks secrets, and writes audit.
    let (_, exec_ok) = call_mcp(
        &http,
        &base_url,
        &alpha_token,
        json!({
            "action": "exec",
            "node_id": node_id,
            "command": "sh",
            "args": ["-c", "echo out_ok; echo err_ok >&2; echo Bearer secretcanarytoken123456"],
        }),
    );
    assert_ne!(exec_ok["result"]["isError"], true, "{exec_ok}");
    let sc = &exec_ok["result"]["structuredContent"];
    assert_eq!(sc["exit_code"], 0);
    assert!(sc["stdout"].as_str().unwrap().contains("out_ok"));
    assert!(sc["stdout"].as_str().unwrap().contains("[redacted]"));
    assert!(
        !sc["stdout"]
            .as_str()
            .unwrap()
            .contains("secretcanarytoken123456")
    );
    assert!(sc["stderr"].as_str().unwrap().contains("err_ok"));

    // 6. Timeout kills slow process cleanly.
    let (_, exec_timeout) = call_mcp(
        &http,
        &base_url,
        &alpha_token,
        json!({
            "action": "exec",
            "node_id": node_id,
            "command": "sleep",
            "args": ["15"],
            "timeout_secs": 1,
        }),
    );
    assert_eq!(
        exec_timeout["result"]["structuredContent"]["timed_out"], true,
        "{exec_timeout}"
    );

    // 7. Instant local kill-switch (`anyagents node pause`) kills active child process group!
    let parent_pid_file = tmp.path().join("parent.pid");
    let child_pid_file = tmp.path().join("child.pid");
    let script = format!(
        "echo $$ > '{}'; sleep 60 & echo $! > '{}'; wait",
        parent_pid_file.display(),
        child_pid_file.display()
    );
    let base_url_bg = base_url.clone();
    let alpha_tok_bg = alpha_token.clone();
    let node_id_bg = node_id.clone();
    let exec_handle = std::thread::spawn(move || {
        let agent = http_agent();
        call_mcp(
            &agent,
            &base_url_bg,
            &alpha_tok_bg,
            json!({
                "action": "exec",
                "node_id": node_id_bg,
                "command": "sh",
                "args": ["-c", script],
                "timeout_secs": 30,
            }),
        )
    });

    let parent_pid: i32 = wait_for_file(&parent_pid_file, Duration::from_secs(5))
        .parse()
        .unwrap();
    let child_pid: i32 = wait_for_file(&child_pid_file, Duration::from_secs(5))
        .parse()
        .unwrap();
    assert!(pid_alive(parent_pid));
    assert!(pid_alive(child_pid));

    let pause_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args(["pause", "--url", &base_url, "--json"])
        .output()
        .expect("run node pause");
    assert!(
        pause_out.status.success(),
        "node pause failed: {}",
        String::from_utf8_lossy(&pause_out.stderr)
    );

    let (_, killed_exec) = exec_handle.join().unwrap();
    assert_eq!(
        killed_exec["result"]["structuredContent"]["cancelled"], true,
        "{killed_exec}"
    );

    // Give kernel a brief moment to reap SIGKILLed processes, verify zero survivors.
    let reap_deadline = Instant::now() + Duration::from_secs(3);
    while (pid_alive(parent_pid) || pid_alive(child_pid)) && Instant::now() < reap_deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !pid_alive(parent_pid),
        "parent shell process must be killed"
    );
    assert!(
        !pid_alive(child_pid),
        "grandchild sleep process must be killed"
    );

    // While paused, new exec is refused fail-closed.
    let (_, paused_denial) = call_mcp(
        &http,
        &base_url,
        &alpha_token,
        json!({ "action": "exec", "node_id": node_id, "command": "echo", "args": ["nope"] }),
    );
    assert_eq!(paused_denial["result"]["isError"], true, "{paused_denial}");

    // 8. Resume restores online execution.
    let resume_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args(["resume", "--url", &base_url, "--json"])
        .output()
        .expect("run node resume");
    assert!(
        resume_out.status.success(),
        "node resume failed: {}",
        String::from_utf8_lossy(&resume_out.stderr)
    );
    let (_, resumed_exec) = call_mcp(
        &http,
        &base_url,
        &alpha_token,
        json!({ "action": "exec", "node_id": node_id, "command": "echo", "args": ["resumed"] }),
    );
    assert_eq!(
        resumed_exec["result"]["structuredContent"]["exit_code"], 0,
        "{resumed_exec}"
    );
    assert!(
        resumed_exec["result"]["structuredContent"]["stdout"]
            .as_str()
            .unwrap()
            .contains("resumed")
    );

    // Check audit trail recorded execs, denial, pause, resume.
    let (st, audit_resp) = op_get(
        &http,
        &base_url,
        &format!("/v1/nodes/{node_id}/audit?limit=50"),
    );
    assert_eq!(st, 200, "{audit_resp}");
    let events = audit_resp["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|e| e["action"] == "exec" && e["outcome"] == "denied")
    );
    assert!(
        events
            .iter()
            .any(|e| e["action"] == "exec" && e["outcome"] == "ok")
    );
    assert!(events.iter().any(|e| e["action"] == "pause"));
    assert!(events.iter().any(|e| e["action"] == "resume"));
    assert!(
        !audit_resp.to_string().contains("secretcanarytoken123456"),
        "audit log must never persist raw secrets"
    );

    // 9. Revoke node: stops daemon, invalidates token, hides node from agent list.
    let revoke_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args(["revoke", "--url", &base_url, "--json"])
        .output()
        .expect("run node revoke");
    assert!(
        revoke_out.status.success(),
        "node revoke failed: {}",
        String::from_utf8_lossy(&revoke_out.stderr)
    );
    assert!(!cfg.exists(), "revoked node.json must be removed");

    if let Some(mut child) = _runner.take() {
        let wait_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(Some(_)) = child.try_wait() {
                break;
            }
            if Instant::now() >= wait_deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("node run daemon did not exit after revoke");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    let (st, revoked_view) = op_get(&http, &base_url, &format!("/v1/nodes/{node_id}"));
    assert_eq!(st, 200);
    assert_eq!(revoked_view["node"]["status"], "revoked");
    let (_, list_after_revoke) =
        call_mcp(&http, &base_url, &alpha_token, json!({ "action": "list" }));
    assert_eq!(
        list_after_revoke["result"]["structuredContent"]["nodes"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn e2e_exit_only_node_rejects_exec_and_reports_status() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let cfg = tmp.path().join("cfg").join("exit-node.json");

    let (alpha_id, alpha_token) = {
        let store = Store::open(data.join("state.db")).unwrap();
        let alpha = store
            .create_agent(NewAgent::new("exit-tester", "claude-cloud"))
            .unwrap();
        store
            .set_agent_running(&alpha.id, &data.join("exit-work").to_string_lossy())
            .unwrap();
        let tok_a = store
            .begin_remote_session(&alpha.id, 3_600_000)
            .unwrap()
            .token;
        (alpha.id.to_string(), tok_a)
    };

    let bin = env!("CARGO_BIN_EXE_anyagents");
    let server_child = Command::new(bin)
        .env("ANYAGENTS_TESTS", "contract-double")
        .env("ANYAGENTS_SERVERLESS", "true")
        .env("ANYAGENTS_WEBHOOK_LISTEN", "127.0.0.1:0")
        .env("ANYAGENTS_OPERATOR_TOKEN", OPERATOR_TOKEN)
        .env("ANYAGENTS_PUBLIC_URL", "http://127.0.0.1:8788")
        .env(
            "ANYAGENTS_CAPABILITY_KEY",
            "test-capability-key-0123456789abcdef",
        )
        .arg("--data")
        .arg(&data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn coordinator");
    let _server = ChildGuard::new(server_child);

    let webhook_addr = wait_for_file(&data.join("webhook.addr"), Duration::from_secs(10));
    let base_url = format!("http://{webhook_addr}");
    let http = http_agent();

    let pair_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args([
            "pair",
            "--url",
            &base_url,
            "--name",
            "exit-only-node",
            "--capability",
            "exit",
            "--json",
        ])
        .output()
        .expect("pair exit-only node");
    assert!(
        pair_out.status.success(),
        "{}",
        String::from_utf8_lossy(&pair_out.stderr)
    );
    let pair_json: Value = serde_json::from_slice(&pair_out.stdout).unwrap();
    let node_id = pair_json["node_id"].as_str().unwrap().to_string();
    assert_eq!(pair_json["capabilities"], json!(["exit"]));

    let runner_child = setup_isolated_node_cmd(bin, &data, &cfg)
        .arg("run")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exit node run");
    let _runner = ChildGuard::new(runner_child);

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (st, val) = op_get(&http, &base_url, &format!("/v1/nodes/{node_id}"));
        if st == 200 && val["node"]["status"] == "online" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "exit-only node never came online"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let status_out = setup_isolated_node_cmd(bin, &data, &cfg)
        .args(["status", "--url", &base_url, "--json"])
        .output()
        .expect("node status");
    assert!(status_out.status.success());
    let status_json: Value = serde_json::from_slice(&status_out.stdout).unwrap();
    assert_eq!(status_json["local"]["connected"], true);
    assert_eq!(status_json["remote"]["node"]["status"], "online");

    let (st, _) = op_post(
        &http,
        &base_url,
        &format!("/v1/nodes/{node_id}/grants"),
        json!({ "agent_id": alpha_id, "capability": "exec" }),
    );
    assert_eq!(st, 201);

    let (_, exec_denied) = call_mcp(
        &http,
        &base_url,
        &alpha_token,
        json!({
            "action": "exec",
            "node_id": node_id,
            "command": "echo",
            "args": ["should-not-run"],
        }),
    );
    assert_eq!(exec_denied["result"]["isError"], true, "{exec_denied}");
}
