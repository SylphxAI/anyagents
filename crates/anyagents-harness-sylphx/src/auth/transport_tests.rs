use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use super::{curl_post_json_fallback, run_json_post_command};

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
