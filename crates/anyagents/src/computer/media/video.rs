//! Asynchronous video jobs against the configured gateway's /v1/videos API.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

pub(crate) const MIME_TYPE: &str = "video/mp4";
const JOB_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_JOB_BYTES: u64 = 64 * 1024;
const MAX_VIDEO_BYTES: u64 = 64 * 1024 * 1024;
const TIMEOUT_ERROR: &str = "media.video_generate timed out before receiving the completed video";

#[derive(Deserialize)]
struct VideoRequest {
    prompt: String,
    model: Option<String>,
    size: Option<String>,
    seconds: Option<Seconds>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Seconds {
    Text(String),
    Number(u64),
}

#[derive(Deserialize)]
struct VideoJob {
    id: String,
    status: String,
    model: Option<String>,
    error: Option<JobError>,
}

#[derive(Deserialize)]
struct JobError {
    code: Option<String>,
}

pub(super) async fn generate(
    base_url: Option<String>,
    api_key: Option<String>,
    work_dir: &Path,
    req: &Value,
) -> Result<(Value, Vec<u8>), String> {
    generate_with_limits(base_url, api_key, work_dir, req, JOB_TIMEOUT, POLL_INTERVAL).await
}

async fn generate_with_limits(
    base_url: Option<String>,
    api_key: Option<String>,
    work_dir: &Path,
    req: &Value,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Value, Vec<u8>), String> {
    let base_url = base_url.ok_or("media.video_generate requires a configured AI gateway URL")?;
    let mut url =
        Url::parse(&base_url).map_err(|_| "invalid AI gateway URL for video generation")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("invalid AI gateway URL for video generation".into());
    }
    let api_key = api_key.ok_or("media.video_generate requires a configured AI gateway API key")?;
    let req = VideoRequest::deserialize(req)
        .map_err(|err| format!("invalid media.video_generate arguments: {err}"))?;
    if req.prompt.trim().is_empty() {
        return Err("media.video_generate requires a non-empty prompt".into());
    }
    let seconds = req.seconds.map(|s| match s {
        Seconds::Text(s) => s,
        Seconds::Number(n) => n.to_string(),
    });
    let mut fields = vec![("prompt", req.prompt.as_str())];
    for (name, value) in [
        ("model", req.model.as_deref()),
        ("size", req.size.as_deref()),
        ("seconds", seconds.as_deref()),
    ] {
        if let Some(value) = value {
            if value.trim().is_empty() {
                return Err(format!("media.video_generate requires a non-empty {name}"));
            }
            fields.push((name, value.trim()));
        }
    }
    let (content_type, body) = multipart(&fields);
    url.path_segments_mut()
        .map_err(|_| "invalid AI gateway URL for video generation")?
        .pop_if_empty()
        .extend(["v1", "videos"]);
    let gateway = Gateway {
        agent: ureq::Agent::config_builder()
            .max_redirects(0)
            .build()
            .into(),
        auth: format!("Bearer {api_key}"),
        deadline: Instant::now() + timeout,
    };
    let mut job = decode_job(
        &gateway
            .request("create", &url, Some((content_type, body)), MAX_JOB_BYTES)
            .await?,
    )?;
    // Treat the ID as an opaque segment, never a URL or a filesystem path.
    if job.id.is_empty()
        || !job
            .id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err("video gateway returned an invalid job ID".into());
    }
    let id = job.id.clone();
    url.path_segments_mut()
        .expect("validated gateway URL")
        .push(&id);
    let mut model = None;
    loop {
        if job.id != id {
            return Err("video gateway returned a different job ID while polling".into());
        }
        if let Some(name) = job.model.filter(|s| !s.trim().is_empty()) {
            model = Some(name);
        }
        match job.status.as_str() {
            "completed" => break,
            "queued" | "in_progress" => {}
            "failed" | "refused" | "cancelled" | "canceled" => {
                let code = job.error.and_then(|e| e.code).unwrap_or_default();
                let code: String = code.chars().filter(|c| !c.is_control()).take(128).collect();
                return Err(format!(
                    "video job {id} ended with status {}{}",
                    job.status,
                    if code.is_empty() {
                        String::new()
                    } else {
                        format!(": {code}")
                    }
                ));
            }
            _ => return Err("video gateway returned an unrecognized job status".into()),
        }
        let remaining = gateway.remaining()?;
        tokio::time::sleep(poll_interval.min(remaining)).await;
        job = decode_job(&gateway.request("poll", &url, None, MAX_JOB_BYTES).await?)?;
    }
    let model = model
        .or(req.model)
        .ok_or("video gateway did not report the video model")?;
    url.path_segments_mut()
        .expect("validated gateway URL")
        .push("content");
    let bytes = gateway
        .request("download", &url, None, MAX_VIDEO_BYTES)
        .await?;
    // Do not save an empty body or a JSON/HTML error page as a video.
    if bytes.len() < 16 || &bytes[4..8] != b"ftyp" {
        return Err("video gateway did not return MP4 content".into());
    }
    gateway.remaining()?;
    let (path, bytes) = {
        let work_dir = work_dir.to_owned();
        tokio::task::spawn_blocking(move || publish(&work_dir, bytes))
            .await
            .map_err(|err| format!("video artifact task failed: {err}"))??
    };
    Ok((
        json!({"path": path, "id": id, "model": model,
        "mime_type": MIME_TYPE, "bytes": bytes.len()}),
        bytes,
    ))
}

struct Gateway {
    agent: ureq::Agent,
    auth: String,
    deadline: Instant,
}

impl Gateway {
    fn remaining(&self) -> Result<Duration, String> {
        remaining(self.deadline)
    }

    async fn request(
        &self,
        stage: &'static str,
        url: &Url,
        upload: Option<(String, Vec<u8>)>,
        limit: u64,
    ) -> Result<Vec<u8>, String> {
        self.remaining()?;
        let (agent, auth, deadline, url) = (
            self.agent.clone(),
            self.auth.clone(),
            self.deadline,
            url.clone(),
        );
        tokio::task::spawn_blocking(move || {
            let timeout = remaining(deadline)?;
            let result = match upload {
                Some((content_type, body)) => agent
                    .post(url.as_str())
                    .header("Authorization", &auth)
                    .header("Content-Type", content_type)
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .send(body),
                None => agent
                    .get(url.as_str())
                    .header("Authorization", &auth)
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .call(),
            };
            let mut response = result.map_err(|err| http_error(stage, err))?;
            if !response.status().is_success() {
                return Err(format!(
                    "video gateway {stage} returned HTTP {}",
                    response.status().as_u16()
                ));
            }
            let bytes = response
                .body_mut()
                .with_config()
                .limit(limit)
                .read_to_vec()
                .map_err(|err| http_error(stage, err))?;
            remaining(deadline)?;
            Ok(bytes)
        })
        .await
        .map_err(|err| format!("video gateway {stage} task failed: {err}"))?
    }
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| TIMEOUT_ERROR.to_owned())
}

fn http_error(stage: &str, err: ureq::Error) -> String {
    match err {
        ureq::Error::Timeout(_) => TIMEOUT_ERROR.into(),
        ureq::Error::StatusCode(code) => format!("video gateway {stage} returned HTTP {code}"),
        err => format!("video gateway {stage} request failed: {err}"),
    }
}

fn decode_job(bytes: &[u8]) -> Result<VideoJob, String> {
    serde_json::from_slice(bytes).map_err(|err| format!("invalid video job response: {err}"))
}

fn multipart(fields: &[(&str, &str)]) -> (String, Vec<u8>) {
    // Choose a delimiter that cannot be mistaken for part of any input field.
    let boundary = (0..)
        .map(|n| format!("anyagents-video-boundary-{n}"))
        .find(|boundary| fields.iter().all(|(_, value)| !value.contains(boundary)))
        .expect("finite input cannot contain all boundaries");
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn publish(work_dir: &Path, bytes: Vec<u8>) -> Result<(PathBuf, Vec<u8>), String> {
    let result = (|| -> std::io::Result<_> {
        let media_dir = work_dir.canonicalize()?.join("media");
        match std::fs::create_dir(&media_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if !std::fs::symlink_metadata(&media_dir)?.is_dir() {
                    return Err(std::io::Error::other(
                        "workspace media path is not a directory",
                    ));
                }
            }
            Err(err) => return Err(err),
        }
        // Use a private temporary file, then publish only after the full write.
        let mut file = tempfile::Builder::new()
            .prefix(".video-")
            .tempfile_in(&media_dir)?;
        file.write_all(&bytes)?;
        let name = file
            .path()
            .file_name()
            .expect("temporary filename")
            .to_string_lossy();
        let dest = media_dir.join(format!("{}.mp4", name.trim_start_matches('.')));
        file.persist_noclobber(&dest)?;
        Ok((dest, bytes))
    })();
    result.map_err(|err| format!("failed to save video in workspace media directory: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead as _, BufReader, Read as _};
    use std::net::TcpListener;

    const MP4: &[u8] = b"\0\0\0\x18ftypisom\0\0\x02\0isommp42";
    const QUEUED: &[u8] = br#"{"id":"video_1","status":"queued","model":"demo/video"}"#;
    const COMPLETED: &[u8] = br#"{"id":"video_1","status":"completed"}"#;

    struct Reply {
        request: &'static str,
        status: &'static str,
        headers: &'static str,
        body: &'static [u8],
    }

    impl Reply {
        fn ok(request: &'static str, body: &'static [u8]) -> Self {
            Self {
                request,
                status: "200 OK",
                headers: "",
                body,
            }
        }
    }

    fn mock_gateway(replies: Vec<Reply>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/gateway", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for reply in replies {
                let until = Instant::now() + Duration::from_secs(5);
                let mut conn = loop {
                    match listener.accept() {
                        Ok((conn, _)) => break conn,
                        Err(err)
                            if err.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < until =>
                        {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(err) => panic!("accepting {}: {err}", reply.request),
                    }
                };
                conn.set_nonblocking(false).unwrap();
                conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut reader = BufReader::new(&mut conn);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert_eq!(line.trim_end(), reply.request);
                let mut headers = Vec::new();
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (name, value) = line.trim_end().split_once(':').unwrap();
                    headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
                }
                let header = |name: &str| {
                    headers
                        .iter()
                        .find(|(key, _)| key == name)
                        .map(|(_, value)| value.as_str())
                };
                assert_eq!(header("authorization"), Some("Bearer mock-key"));
                let size = header("content-length").unwrap_or("0").parse().unwrap();
                let mut body = vec![0; size];
                reader.read_exact(&mut body).unwrap();
                if reply.request.starts_with("POST ") {
                    let boundary = header("content-type")
                        .unwrap()
                        .strip_prefix("multipart/form-data; boundary=")
                        .unwrap();
                    assert!(body.starts_with(format!("--{boundary}\r\n").as_bytes()));
                    assert!(body.ends_with(format!("--{boundary}--\r\n").as_bytes()));
                }
                bodies.push(String::from_utf8(body).unwrap());
                write!(
                    conn,
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
                    reply.status,
                    reply.body.len(),
                    reply.headers
                )
                .unwrap();
                conn.write_all(reply.body).unwrap();
            }
            bodies
        });
        (endpoint, server)
    }

    const CREATE: &str = "POST /gateway/v1/videos HTTP/1.1";
    const POLL: &str = "GET /gateway/v1/videos/video_1 HTTP/1.1";
    const DOWNLOAD: &str = "GET /gateway/v1/videos/video_1/content HTTP/1.1";

    #[tokio::test]
    async fn creates_polls_and_saves_completed_video() {
        let (url, server) = mock_gateway(vec![
            Reply::ok(CREATE, QUEUED),
            Reply::ok(POLL, br#"{"id":"video_1","status":"in_progress"}"#),
            Reply::ok(POLL, COMPLETED),
            Reply::ok(DOWNLOAD, MP4),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let req = json!({"prompt": "a quiet beach", "model": "requested/video", "size": "720x1280", "seconds": 4});
        let (value, attached) = generate_with_limits(
            Some(url),
            Some("mock-key".into()),
            dir.path(),
            &req,
            Duration::from_secs(5),
            Duration::ZERO,
        )
        .await
        .unwrap();
        let path = Path::new(value["path"].as_str().unwrap());
        assert_eq!(
            path.parent(),
            Some(dir.path().canonicalize().unwrap().join("media").as_path())
        );
        assert_eq!(path.extension().unwrap(), "mp4");
        assert_eq!(std::fs::read(path).unwrap(), MP4);
        assert_eq!(attached, MP4);
        assert_eq!(
            value,
            json!({"path": path, "id": "video_1", "model": "demo/video", "mime_type": "video/mp4", "bytes": MP4.len()})
        );
        let bodies = server.join().unwrap();
        for (field, value) in [
            ("prompt", "a quiet beach"),
            ("model", "requested/video"),
            ("size", "720x1280"),
            ("seconds", "4"),
        ] {
            assert!(bodies[0].contains(&format!("name=\"{field}\"\r\n\r\n{value}\r\n")));
        }
        assert!(bodies[1..].iter().all(String::is_empty));
    }

    async fn fails(replies: Vec<Reply>, expected: &str) {
        let (url, server) = mock_gateway(replies);
        let dir = tempfile::tempdir().unwrap();
        let err = generate_with_limits(
            Some(url),
            Some("mock-key".into()),
            dir.path(),
            &json!({"prompt": "test"}),
            Duration::from_secs(5),
            Duration::ZERO,
        )
        .await
        .unwrap_err();
        assert!(err.contains(expected), "{err}");
        assert!(!dir.path().join("media").exists());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn refuses_failed_and_mismatched_jobs_without_downloading() {
        fails(
            vec![
                Reply::ok(CREATE, QUEUED),
                Reply::ok(
                    POLL,
                    br#"{"id":"video_1","status":"failed","error":{"code":"moderation_blocked"}}"#,
                ),
            ],
            "ended with status failed: moderation_blocked",
        )
        .await;
        fails(
            vec![
                Reply::ok(CREATE, QUEUED),
                Reply::ok(POLL, br#"{"id":"video_2","status":"completed"}"#),
            ],
            "different job ID",
        )
        .await;
    }

    #[tokio::test]
    async fn refuses_redirects_and_non_mp4_downloads() {
        fails(
            vec![Reply {
                request: CREATE,
                status: "307 Temporary Redirect",
                headers: "Location: /gateway/elsewhere\r\n",
                body: b"",
            }],
            "create returned HTTP 307",
        )
        .await;
        fails(
            vec![
                Reply::ok(
                    CREATE,
                    br#"{"id":"video_1","status":"completed","model":"demo/video"}"#,
                ),
                Reply::ok(DOWNLOAD, br#"{"error":"no video available"}"#),
            ],
            "did not return MP4",
        )
        .await;
    }

    #[tokio::test]
    async fn times_out_queued_job_without_publishing() {
        let (url, server) = mock_gateway(vec![Reply::ok(CREATE, QUEUED)]);
        let dir = tempfile::tempdir().unwrap();
        let err = generate_with_limits(
            Some(url),
            Some("mock-key".into()),
            dir.path(),
            &json!({"prompt": "test"}),
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .await
        .unwrap_err();
        assert_eq!(err, TIMEOUT_ERROR);
        assert!(!dir.path().join("media").exists());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn missing_config_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let req = json!({"prompt": "test"});
        assert!(
            generate(None, Some("mock-key".into()), dir.path(), &req)
                .await
                .unwrap_err()
                .contains("configured AI gateway URL")
        );
        assert!(
            generate(Some("http://127.0.0.1:1".into()), None, dir.path(), &req)
                .await
                .unwrap_err()
                .contains("configured AI gateway API key")
        );
        assert!(!dir.path().join("media").exists());
    }
}
