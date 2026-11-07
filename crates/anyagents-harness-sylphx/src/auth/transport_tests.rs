use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use super::{
    PROVIDER_KEY_ENVIRONMENT, curl_command, curl_post_json_fallback,
    external_provider_key_environment, remove_provider_key_environment, run_json_post_command,
};

#[test]
fn provider_key_environment_lookup_preserves_alias_names_and_order() {
    assert_eq!(
        PROVIDER_KEY_ENVIRONMENT.sylphx,
        ["SYLPHX_API_KEY", "SYLPHX_AI_KEY", "SYLPHX_GATEWAY_KEY"]
    );
    let external: &[(&str, &[&str])] = &[
        (
            "kimi-code-plan",
            &["KIMI_CODE_PLAN_API_KEY", "KIMI_API_KEY"],
        ),
        (
            "qwen-token-plan",
            &["QWEN_TOKEN_PLAN_API_KEY", "DASHSCOPE_API_KEY"],
        ),
        ("glm-code-plan", &["GLM_CODE_PLAN_API_KEY", "ZAI_API_KEY"]),
        (
            "minimax-code-plan",
            &["MINIMAX_CODE_PLAN_API_KEY", "MINIMAX_API_KEY"],
        ),
        ("openrouter", &["OPENROUTER_API_KEY"]),
    ];
    for (provider, expected) in external {
        assert_eq!(external_provider_key_environment(provider), *expected);
    }
    assert!(external_provider_key_environment("unsupported-provider").is_empty());
}

fn assert_large_post(status: u16, response_body: &str) {
    let payload = serde_json::json!({
        "history": "粵語\nline\r\n@literal ".repeat(20_000),
        "instructions": "Preserve the existing session and model."
    })
    .to_string();
    assert!(payload.len() > 256 * 1024);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/responses", listener.local_addr().unwrap());
    let response_body = response_body.to_string();
    let expected_response = response_body.clone();
    let server = thread::spawn(move || -> std::io::Result<(String, Vec<u8>)> {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "no request reached the loopback server",
                        ));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error),
            }
        };
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        let mut request = Vec::new();
        let mut buf = [0; 8192];
        let header_end = loop {
            let count = stream.read(&mut buf)?;
            assert_ne!(count, 0, "request ended before its headers");
            request.extend_from_slice(&buf[..count]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
        let content_length: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .expect("POST has a content length");
        while request.len() - header_end < content_length {
            let count = stream.read(&mut buf)?;
            assert_ne!(count, 0, "request body was truncated");
            request.extend_from_slice(&buf[..count]);
        }
        write!(
            stream,
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        )?;
        stream.flush()?;
        Ok((headers, request[header_end..].to_vec()))
    });

    let result = curl_post_json_fallback(&url, Some("synthetic-test-token"), &payload);
    let received = server.join().unwrap();
    assert_eq!(result, Ok((status, expected_response)));
    let (headers, body) = received.unwrap();
    assert!(headers.starts_with("POST /responses HTTP/1.1\r\n"));
    assert!(headers.contains("Authorization: Bearer synthetic-test-token\r\n"));
    assert_eq!(body, payload.as_bytes());
}

#[test]
fn large_utf8_json_reaches_the_http_fallback_unchanged() {
    assert_large_post(200, "{\"accepted\":true}");
}

#[test]
fn large_json_preserves_a_quota_refusal() {
    assert_large_post(429, "{\"error\":\"included_capacity_unavailable\"}");
}

#[test]
fn an_early_http_refusal_survives_a_closed_request_pipe() {
    let mut client = Command::new("sh");
    client.args([
        "-c",
        "printf '%s\\n' '{\"error\":\"included_capacity_unavailable\"}' '429'",
    ]);
    let payload = "x".repeat(512 * 1024);
    assert_eq!(
        run_json_post_command(client, &payload),
        Ok((
            429,
            "{\"error\":\"included_capacity_unavailable\"}".to_string()
        ))
    );
}

#[test]
fn a_closed_request_pipe_without_an_http_response_remains_an_error() {
    let mut client = Command::new("sh");
    client.args(["-c", "printf '\\n000'"]);
    let result = run_json_post_command(client, &"x".repeat(512 * 1024));
    assert!(result.unwrap_err().contains("writing HTTP request body"));
}

#[test]
fn curl_spawn_argv_excludes_the_bearer_credential() {
    let token = format!("synthetic-argv-sentinel-{}", std::process::id());
    let (mut cmd, header_file) = curl_command(Some(&token)).unwrap();
    let header_file = header_file.unwrap();
    let header_path = header_file.path().to_path_buf();
    assert!(
        std::fs::read_to_string(&header_path)
            .unwrap()
            .contains(&token),
        "the private file must carry the credential"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            header_file
                .as_file()
                .metadata()
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    cmd.arg("--version");
    // Record the complete argv of the very Command spawned below. Avoid
    // printing either argv or the sentinel if this assertion ever fails.
    let argv: Vec<_> = cmd
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert!(
        argv.iter().all(|arg| !arg.contains(&token)),
        "curl argv must not contain the bearer credential"
    );
    assert!(cmd.output().unwrap().status.success());
    drop(header_file);
    assert!(!header_path.exists(), "the private header must be removed");
}

#[test]
fn unauthenticated_curl_does_not_create_a_header_file() {
    let (cmd, header_file) = curl_command(None).unwrap();
    assert!(header_file.is_none());
    assert_eq!(cmd.get_args().count(), 0);
}

#[test]
fn curl_children_drop_provider_keys_but_preserve_operational_environment() {
    let provider_keys = [
        "SYLPHX_API_KEY",
        "SYLPHX_AI_KEY",
        "SYLPHX_GATEWAY_KEY",
        "KIMI_CODE_PLAN_API_KEY",
        "KIMI_API_KEY",
        "QWEN_TOKEN_PLAN_API_KEY",
        "DASHSCOPE_API_KEY",
        "GLM_CODE_PLAN_API_KEY",
        "ZAI_API_KEY",
        "MINIMAX_CODE_PLAN_API_KEY",
        "MINIMAX_API_KEY",
        "OPENROUTER_API_KEY",
    ];
    for bearer_token in [None, Some("synthetic-environment-test-token")] {
        let (cmd, _header_file) = curl_command(bearer_token).unwrap();
        for key in provider_keys {
            assert!(
                cmd.get_envs()
                    .any(|(name, value)| name == key && value.is_none()),
                "curl must explicitly remove {key}"
            );
        }
    }

    let operational = [
        ("HTTP_PROXY", "http://proxy.invalid:8080"),
        ("HTTPS_PROXY", "http://proxy.invalid:8081"),
        ("ALL_PROXY", "socks5://proxy.invalid:1080"),
        ("NO_PROXY", "localhost,127.0.0.1"),
        ("http_proxy", "http://proxy.invalid:8082"),
        ("https_proxy", "http://proxy.invalid:8083"),
        ("all_proxy", "socks5://proxy.invalid:1081"),
        ("no_proxy", "localhost"),
        ("CURL_CA_BUNDLE", "/synthetic/ca.pem"),
        ("SSL_CERT_FILE", "/synthetic/cert.pem"),
        ("SSL_CERT_DIR", "/synthetic/certs"),
        ("PATH", "/usr/bin:/bin"),
        ("TMPDIR", "/synthetic/tmp"),
        ("UNRELATED_SETTING", "preserve-this-value"),
    ];
    // Only this synthetic fixture clears its environment. Never mutate the
    // test process's environment or read live credentials into the fixture.
    let mut child = Command::new("/usr/bin/env");
    child.env_clear().envs(operational);
    for key in provider_keys {
        child.env(key, "synthetic-provider-key");
    }
    let before = child.output().unwrap();
    assert!(before.status.success());
    let before = String::from_utf8(before.stdout).unwrap();
    for key in provider_keys {
        assert!(
            before
                .lines()
                .any(|line| line == format!("{key}=synthetic-provider-key"))
        );
    }

    remove_provider_key_environment(&mut child);
    let after = child.output().unwrap();
    assert!(after.status.success());
    let after = String::from_utf8(after.stdout).unwrap();
    for key in provider_keys {
        assert!(
            !after
                .lines()
                .any(|line| line.starts_with(&format!("{key}="))),
            "child must not receive {key}"
        );
    }
    for (name, value) in operational {
        assert!(after.lines().any(|line| line == format!("{name}={value}")));
    }
}
