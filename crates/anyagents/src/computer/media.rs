//! Media generation and transcription through the configured AI gateway.

use super::*;

mod video;

pub const DEFAULT_IMAGE_MODEL: &str = "openai/gpt-image-2";
pub const DEFAULT_SPEECH_MODEL: &str = "elevenlabs/eleven-flash-v2";
pub const DEFAULT_ELEVENLABS_VOICE_ID: &str = "21m00Tcm4TlvDq8ikWAM";
pub const DEFAULT_TRANSCRIBE_MODEL: &str = "soniox/stt-async-v5";

fn default_speech_voice(model: &str) -> &'static str {
    if model.starts_with("elevenlabs/") {
        DEFAULT_ELEVENLABS_VOICE_ID
    } else if model.starts_with("google/") {
        "Kore"
    } else {
        "alloy"
    }
}

/// Return a media receipt and, for generated videos, the downloaded MP4 bytes
/// separately so callers can attach them without putting binary in the receipt.
pub async fn execute_media_request(
    data_dir: Option<&Path>,
    config_dir: &Path,
    work_dir: &Path,
    req: Value,
) -> Result<(Value, Option<Vec<u8>>), String> {
    let action = req
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("image_generate")
        .trim()
        .to_ascii_lowercase();

    if action == "video_generate" {
        let (receipt, mp4) = video::generate(
            configured_gateway_base_url(),
            resolve_sylphx_api_key(data_dir, config_dir),
            work_dir,
            &req,
        )
        .await?;
        return Ok((receipt, Some(mp4)));
    }

    let base_url = resolve_sylphx_gateway_base_url();
    let api_key = resolve_sylphx_api_key(data_dir, config_dir).ok_or_else(|| {
        "missing Sylphx AI Gateway key (set SYLPHX_API_KEY or configure .config/sylphx/key)"
            .to_string()
    })?;

    let receipt = match action.as_str() {
        "image_generate" | "image" | "generate_image" => {
            let prompt = req
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing `prompt` for media.image_generate".to_string())?;
            let model = req
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_IMAGE_MODEL);
            let mut body = json!({
                "prompt": prompt,
                "model": model,
            });
            if let Some(size) = req.get("size").and_then(Value::as_str) {
                body["size"] = Value::String(size.to_string());
            }
            if let Some(n) = req.get("n").and_then(Value::as_u64) {
                body["n"] = Value::Number(n.into());
            }
            let url = format!("{base_url}/v1/images/generations");
            let key_clone = api_key.clone();
            let body_clone = body.clone();
            let (status, resp_bytes) = tokio::task::spawn_blocking(move || {
                post_json_bytes(&url, &key_clone, &body_clone, 120)
            })
            .await
            .map_err(|e| format!("media.image_generate task failed: {e}"))??;
            let resp_json: Value = serde_json::from_slice(&resp_bytes)
                .map_err(|e| format!("invalid JSON from /v1/images/generations ({status}): {e}"))?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "/v1/images/generations returned {status}: {resp_json}"
                ));
            }
            if let Some(output_path_raw) = req
                .get("output_path")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                && let Some(b64) = resp_json
                    .get("data")
                    .and_then(Value::as_array)
                    .and_then(|arr| arr.first())
                    .and_then(|item| item.get("b64_json"))
                    .and_then(Value::as_str)
            {
                let bytes = base64_decode(b64)
                    .map_err(|e| format!("invalid b64_json in image response: {e}"))?;
                let out_path = resolve_work_path(work_dir, output_path_raw)?;
                if let Some(parent) = out_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                tokio::fs::write(&out_path, &bytes)
                    .await
                    .map_err(|e| format!("failed to write image to {}: {e}", out_path.display()))?;
            }
            Ok(resp_json)
        }
        "speech" | "tts" => {
            let input = req
                .get("input")
                .or_else(|| req.get("text"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing `input` text for media.speech".to_string())?;
            let model = req
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_SPEECH_MODEL);
            let voice = req
                .get("voice")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| default_speech_voice(model));
            let response_format = req
                .get("response_format")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("mp3");

            let url = format!("{base_url}/v1/audio/speech");
            let payload = json!({
                "model": model,
                "input": input,
                "voice": voice,
                "response_format": response_format,
            });
            let key_clone = api_key.clone();
            let (status, bytes) = tokio::task::spawn_blocking(move || {
                post_json_bytes(&url, &key_clone, &payload, 120)
            })
            .await
            .map_err(|e| format!("media.speech task failed: {e}"))??;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "/v1/audio/speech returned {status}: {}",
                    String::from_utf8_lossy(&bytes)
                ));
            }
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let sha256 = format!("{:x}", hasher.finalize());

            let saved_path = if let Some(output_path_raw) = req
                .get("output_path")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let out_path = resolve_work_path(work_dir, output_path_raw)?;
                if let Some(parent) = out_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                tokio::fs::write(&out_path, &bytes)
                    .await
                    .map_err(|e| format!("failed to write audio to {}: {e}", out_path.display()))?;
                Some(out_path.to_string_lossy().to_string())
            } else {
                None
            };

            let b64 = base64_encode(&bytes);
            Ok(json!({
                "action": "speech",
                "model": model,
                "voice": voice,
                "response_format": response_format,
                "bytes": bytes.len(),
                "sha256": sha256,
                "output_path": saved_path,
                "audio_base64": b64,
            }))
        }
        "transcribe" | "transcription" | "stt" => {
            let file_path_raw = req
                .get("file_path")
                .or_else(|| req.get("path"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing `file_path` for media.transcribe".to_string())?;
            let file_path = resolve_work_path(work_dir, file_path_raw)?;
            if !file_path.exists() {
                return Err(format!(
                    "audio file does not exist: {}",
                    file_path.display()
                ));
            }
            let model = req
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_TRANSCRIBE_MODEL);

            let audio_bytes = tokio::fs::read(&file_path)
                .await
                .map_err(|e| format!("failed to read audio file {}: {e}", file_path.display()))?;
            let filename = file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("audio.bin")
                .replace(['"', '\r', '\n'], "_");
            let boundary = format!(
                "----anyagentsmedia{:016x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(1)
            );
            let mut body = Vec::with_capacity(audio_bytes.len() + 512);
            let append_field = |buf: &mut Vec<u8>, name: &str, val: &str| {
                buf.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
                buf.extend_from_slice(
                    format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
                );
                buf.extend_from_slice(val.as_bytes());
                buf.extend_from_slice(b"\r\n");
            };
            append_field(&mut body, "model", model);
            if let Some(lang) = req
                .get("language")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                append_field(&mut body, "language", lang);
            }
            if let Some(prompt) = req
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                append_field(&mut body, "prompt", prompt);
            }
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(&audio_bytes);
            body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

            let url = format!("{base_url}/v1/audio/transcriptions");
            let content_type = format!("multipart/form-data; boundary={boundary}");
            let key_clone = api_key.clone();
            let (status, resp_bytes) = tokio::task::spawn_blocking(move || {
                post_raw_bytes(&url, &key_clone, &content_type, &body, 120)
            })
            .await
            .map_err(|e| format!("media.transcribe task failed: {e}"))??;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "/v1/audio/transcriptions returned {status}: {}",
                    String::from_utf8_lossy(&resp_bytes)
                ));
            }
            let text_out = String::from_utf8_lossy(&resp_bytes);
            match serde_json::from_str::<Value>(&text_out) {
                Ok(v) => Ok(v),
                Err(_) => Ok(json!({ "text": text_out.trim() })),
            }
        }
        other => Err(format!(
            "unsupported media action `{other}` (expected image_generate|video_generate|speech|transcribe)"
        )),
    }?;
    Ok((receipt, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead as _, BufReader, Write as _};

    #[test]
    fn media_requests_use_current_catalog_defaults_against_gateway() {
        let _environment = crate::server::tests::env_lock();
        let prev_ai = std::env::var_os("SYLPHX_AI_BASE_URL");
        let prev_key = std::env::var_os("SYLPHX_API_KEY");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, String)>::new()));
        let captured_srv = std::sync::Arc::clone(&captured);

        let server = std::thread::spawn(move || {
            for idx in 0..3 {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut length = 0usize;
                let mut expect_100 = false;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap_or(0);
                        } else if name.eq_ignore_ascii_case("expect")
                            && value.trim().eq_ignore_ascii_case("100-continue")
                        {
                            expect_100 = true;
                        }
                    }
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                if expect_100 {
                    reader
                        .get_mut()
                        .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                        .unwrap();
                }
                let mut body = vec![0u8; length];
                reader.read_exact(&mut body).unwrap();
                captured_srv.lock().unwrap().push((
                    request_line.trim().to_string(),
                    String::from_utf8_lossy(&body).into_owned(),
                ));
                let mut stream = reader.into_inner();
                let (ctype, resp): (&str, &[u8]) = match idx {
                    0 => ("application/json", br#"{"data":[{"b64_json":"aGVsbG8="}]}"#),
                    1 => ("audio/mpeg", b"ID3-audio"),
                    _ => ("application/json", br#"{"text":"hello world"}"#),
                };
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    resp.len()
                )
                .unwrap();
                stream.write_all(resp).unwrap();
            }
        });

        let dir = crate::test_tempdir();
        let audio_in = dir.path().join("sample.wav");
        std::fs::write(&audio_in, b"RIFF0000WAVE").unwrap();

        unsafe {
            std::env::set_var("SYLPHX_AI_BASE_URL", format!("http://127.0.0.1:{port}/v1"));
            std::env::set_var("SYLPHX_API_KEY", "sk_test_media");
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let (img, attachment) = runtime
            .block_on(execute_media_request(
                None,
                dir.path(),
                dir.path(),
                json!({
                    "action": "image_generate",
                    "prompt": "minimal geometric icon",
                    "output_path": "out.png",
                }),
            ))
            .unwrap();
        assert!(attachment.is_none());
        assert_eq!(img["data"][0]["b64_json"], "aGVsbG8=");
        assert_eq!(std::fs::read(dir.path().join("out.png")).unwrap(), b"hello");

        let (tts, attachment) = runtime
            .block_on(execute_media_request(
                None,
                dir.path(),
                dir.path(),
                json!({
                    "action": "speech",
                    "input": "ok",
                    "output_path": "out.mp3",
                }),
            ))
            .unwrap();
        assert!(attachment.is_none());
        assert_eq!(tts["model"], DEFAULT_SPEECH_MODEL);
        assert_eq!(tts["voice"], DEFAULT_ELEVENLABS_VOICE_ID);
        assert_eq!(
            std::fs::read(dir.path().join("out.mp3")).unwrap(),
            b"ID3-audio"
        );

        let (stt, attachment) = runtime
            .block_on(execute_media_request(
                None,
                dir.path(),
                dir.path(),
                json!({
                    "action": "transcribe",
                    "file_path": "sample.wav",
                }),
            ))
            .unwrap();
        assert!(attachment.is_none());
        assert_eq!(stt["text"], "hello world");

        unsafe {
            match prev_ai {
                Some(v) => std::env::set_var("SYLPHX_AI_BASE_URL", v),
                None => std::env::remove_var("SYLPHX_AI_BASE_URL"),
            }
            match prev_key {
                Some(v) => std::env::set_var("SYLPHX_API_KEY", v),
                None => std::env::remove_var("SYLPHX_API_KEY"),
            }
        }
        server.join().unwrap();

        let reqs = captured.lock().unwrap();
        assert_eq!(reqs.len(), 3);
        assert_eq!(reqs[0].0, "POST /v1/images/generations HTTP/1.1");
        let img_req: Value = serde_json::from_str(&reqs[0].1).unwrap();
        assert_eq!(img_req["model"], DEFAULT_IMAGE_MODEL);

        assert_eq!(reqs[1].0, "POST /v1/audio/speech HTTP/1.1");
        let tts_req: Value = serde_json::from_str(&reqs[1].1).unwrap();
        assert_eq!(tts_req["model"], DEFAULT_SPEECH_MODEL);
        assert_eq!(tts_req["voice"], DEFAULT_ELEVENLABS_VOICE_ID);

        assert_eq!(reqs[2].0, "POST /v1/audio/transcriptions HTTP/1.1");
        assert!(
            reqs[2].1.contains(DEFAULT_TRANSCRIBE_MODEL),
            "multipart body missing default transcribe model: {}",
            reqs[2].1
        );
    }
}
