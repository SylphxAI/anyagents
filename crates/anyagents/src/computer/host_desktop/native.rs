//! Native backends keep scripts and user text on stdin, never in process argv.
use super::*;

async fn script(platform: DesktopPlatform, req: &Value) -> Result<Value, String> {
    let (program, args, source) = script_command(platform, req)?;
    // Permission prompts (notably macOS TCC) can wait forever on headless hosts.
    let out = run_cmd_with_stdin(program, &args, &[], source.as_bytes(), 3_000).await?;
    let bytes = checked_output(out, program)?;
    serde_json::from_slice(&bytes).map_err(|e| format!("invalid {program} desktop response: {e}"))
}

fn script_command(
    platform: DesktopPlatform,
    req: &Value,
) -> Result<(&'static str, Vec<&'static str>, String), String> {
    let (program, args, source) = match platform {
        DesktopPlatform::MacOs => (
            "osascript",
            vec!["-l", "JavaScript", "-"],
            format!(
                "const request = {};\n{}",
                serde_json::to_string(req).map_err(|e| e.to_string())?,
                include_str!("macos.js")
            ),
        ),
        DesktopPlatform::Windows => (
            "powershell",
            vec!["-NoProfile", "-NonInteractive", "-Command", "-"],
            format!(
                "$ErrorActionPreference = 'Stop'\ntry {{\n$r = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{}')) | ConvertFrom-Json\n{}\n}} catch {{ [Console]::Error.WriteLine('native desktop operation failed'); exit 1 }}\n\n",
                base64_encode(&serde_json::to_vec(req).map_err(|e| e.to_string())?),
                include_str!("windows.ps1")
            ),
        ),
        _ => return Err("native script requires macOS or Windows".into()),
    };
    Ok((program, args, source))
}

pub(super) async fn list_windows(platform: DesktopPlatform) -> Result<Vec<Value>, String> {
    let response = script(platform, &json!({"action": "list_windows"})).await?;
    serde_json::from_value(response["windows"].clone()).map_err(|e| e.to_string())
}

pub(super) async fn collect_nodes(
    platform: DesktopPlatform,
    req: &Value,
) -> Result<Vec<RawA11yNode>, String> {
    let mut request = req.clone();
    request["action"] = json!("snapshot");
    let response = script(platform, &request).await?;
    serde_json::from_value(response["nodes"].clone())
        .map_err(|e| format!("invalid native accessibility tree: {e}"))
}

pub(super) async fn execute_aux(platform: DesktopPlatform, req: &Value) -> Result<Value, String> {
    let mut normalized = req.clone();
    normalized["action"] = json!(
        req.get("action")
            .or_else(|| req.get("type"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    );
    let req = &normalized;
    if platform == DesktopPlatform::MacOs {
        match req["action"].as_str().unwrap_or_default() {
            "clipboard_get" => {
                let bytes = checked_output(run_cmd("pbpaste", &[], &[], 5_000).await?, "pbpaste")?;
                return Ok(
                    json!({"action": "clipboard_get", "backend": "pbpaste", "text": String::from_utf8_lossy(&bytes)}),
                );
            }
            "clipboard_set" => {
                let text = req["text"]
                    .as_str()
                    .ok_or("missing `text` for clipboard_set")?;
                checked_output(
                    run_cmd_with_stdin("pbcopy", &[], &[], text.as_bytes(), 5_000).await?,
                    "pbcopy",
                )?;
                return Ok(
                    json!({"action": "clipboard_set", "backend": "pbcopy", "bytes": text.len(), "stdin_piped": true}),
                );
            }
            _ => {}
        }
    }
    script(platform, req).await
}

pub(super) async fn execute_action(
    platform: DesktopPlatform,
    action: &ComputerAction,
    target: Option<&RefEntry>,
) -> Result<Value, String> {
    if let ComputerAction::Wait { duration_ms } = action {
        tokio::time::sleep(Duration::from_millis(u64::from(*duration_ms).min(60_000))).await;
        return Ok(json!({"action": "wait", "duration_ms": duration_ms}));
    }
    if platform == DesktopPlatform::MacOs
        && matches!(
            action,
            ComputerAction::Screenshot | ComputerAction::Zoom { .. }
        )
    {
        let mut args = vec!["-x".to_string(), "-t".into(), "png".into()];
        if let ComputerAction::Zoom { region: r } = action {
            args.extend([
                "-R".into(),
                format!("{},{},{},{}", r.x, r.y, r.width, r.height),
            ]);
        }
        args.push("-".into());
        let argv: Vec<_> = args.iter().map(String::as_str).collect();
        let png = checked_output(
            run_cmd("screencapture", &argv, &[], 15_000).await?,
            "screencapture",
        )?;
        let (width, height) =
            parse_png_dimensions(&png).ok_or("screencapture did not return a PNG")?;
        return Ok(
            json!({"action": if matches!(action, ComputerAction::Zoom { .. }) { "zoom" } else { "screenshot" }, "backend": "screencapture", "mime_type": "image/png", "width": width, "height": height, "bytes": png.len(), "image_base64": base64_encode(&png)}),
        );
    }
    let mut req = serde_json::to_value(action).map_err(|e| e.to_string())?;
    if let Some(entry) = target {
        req["target"] = serde_json::to_value(entry).map_err(|e| e.to_string())?;
    }
    script(platform, &req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_scripts_keep_injection_shaped_text_off_argv() {
        let text = "sensitive-text\n\"; $(exit 1); ' 世界";
        let req = json!({"action":"type","text":text});
        let (program, args, source) = script_command(DesktopPlatform::MacOs, &req).unwrap();
        assert_eq!(program, "osascript");
        assert_eq!(args, ["-l", "JavaScript", "-"]);
        let line = source
            .lines()
            .next()
            .unwrap()
            .strip_prefix("const request = ")
            .unwrap()
            .trim_end_matches(';');
        assert_eq!(serde_json::from_str::<Value>(line).unwrap(), req);
        let (program, args, source) = script_command(DesktopPlatform::Windows, &req).unwrap();
        assert_eq!(program, "powershell");
        assert_eq!(args, ["-NoProfile", "-NonInteractive", "-Command", "-"]);
        assert!(!source.contains(text));
        assert!(source.contains(&base64_encode(&serde_json::to_vec(&req).unwrap())));
        assert!(source.contains("UIAutomationClient"));
        assert!(args.iter().all(|arg| !arg.contains("sensitive-text")));
    }
}
