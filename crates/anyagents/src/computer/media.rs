//! `tools.media`: image generation, speech and transcription through the
//! Sylphx AI Gateway.

use super::*;

/// Execute a `tools.media` (Multimodal Image Generation, Speech TTS, and Audio Transcription)
/// request via the Sylphx AI Gateway (`/v1/images/generations`, `/v1/audio/speech`,
/// `/v1/audio/transcriptions`).
pub async fn execute_media_request(
    data_dir: Option<&Path>,
    config_dir: &Path,
    work_dir: &Path,
    req: Value,
) -> Result<Value, String> {
    let action = req
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("image_generate")
        .trim()
        .to_ascii_lowercase();

    let base_url = resolve_sylphx_gateway_base_url();
    let api_key = resolve_sylphx_api_key(data_dir, config_dir).ok_or_else(|| {
        "missing Sylphx AI Gateway key (set SYLPHX_API_KEY or configure .config/sylphx/key)"
            .to_string()
    })?;

    match action.as_str() {
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
                .unwrap_or("gpt-image-1");
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
                .unwrap_or("tts-1");
            let voice = req
                .get("voice")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("alloy");
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
                .unwrap_or("whisper-1");

            let url = format!("{base_url}/v1/audio/transcriptions");
            let file_str = file_path.to_string_lossy().to_string();
            let file_arg = format!("file=@{file_str}");
            let model_arg = format!("model={model}");
            // Pass Authorization header via curl `-K -` stdin so API key never appears in `ps aux` argv!
            let curl_config = format!("header = \"Authorization: Bearer {api_key}\"\n");
            let mut curl_args: Vec<String> = vec![
                "-sS".into(),
                "-K".into(),
                "-".into(),
                "-X".into(),
                "POST".into(),
                url,
                "-F".into(),
                file_arg,
                "-F".into(),
                model_arg,
            ];
            if let Some(lang) = req
                .get("language")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                curl_args.push("-F".into());
                curl_args.push(format!("language={lang}"));
            }
            if let Some(prompt) = req
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                curl_args.push("-F".into());
                curl_args.push(format!("prompt={prompt}"));
            }
            let arg_refs: Vec<&str> = curl_args.iter().map(String::as_str).collect();
            let out =
                run_cmd_with_stdin("curl", &arg_refs, &[], curl_config.as_bytes(), 120_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "media.transcribe failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            let text_out = String::from_utf8_lossy(&out.stdout);
            match serde_json::from_str::<Value>(&text_out) {
                Ok(v) => Ok(v),
                Err(_) => Ok(json!({ "text": text_out.trim() })),
            }
        }
        other => Err(format!(
            "unsupported media action `{other}` (expected image_generate|speech|transcribe)"
        )),
    }
}
