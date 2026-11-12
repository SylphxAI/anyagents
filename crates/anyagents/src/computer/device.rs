//! `tools.device`: Android (adb, including ChromeOS ARC) and iOS control.

use super::*;

mod mobile;

/// Map logical mobile hardware button name to Android keyevent code.
pub fn android_keyevent_for_button(button: &str) -> Result<&'static str, String> {
    match button.trim().to_ascii_lowercase().as_str() {
        "home" => Ok("KEYCODE_HOME"),
        "back" => Ok("KEYCODE_BACK"),
        "app_switch" | "recent" | "recents" => Ok("KEYCODE_APP_SWITCH"),
        "power" => Ok("KEYCODE_POWER"),
        "volume_up" => Ok("KEYCODE_VOLUME_UP"),
        "volume_down" => Ok("KEYCODE_VOLUME_DOWN"),
        "enter" | "return" => Ok("KEYCODE_ENTER"),
        other => Err(format!(
            "unsupported mobile button `{other}` (expected home|back|app_switch|power|volume_up|volume_down|enter)"
        )),
    }
}

/// Escape text for `adb shell input text`.
pub fn escape_adb_input_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    for ch in text.chars() {
        match ch {
            ' ' => out.push_str("%s"),
            '&' | '|' | ';' | '<' | '>' | '(' | ')' | '$' | '`' | '\\' | '"' | '\'' | '*' | '?'
            | '[' | ']' | '#' | '~' | '=' | '%' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

async fn run_adb(serial: Option<&str>, args: &[&str], timeout_ms: u64) -> Result<Vec<u8>, String> {
    let mut full_args: Vec<&str> = Vec::with_capacity(args.len() + 2);
    if let Some(s) = serial.map(str::trim).filter(|s| !s.is_empty()) {
        full_args.push("-s");
        full_args.push(s);
    }
    full_args.extend_from_slice(args);
    let out = run_cmd("adb", &full_args, &[], timeout_ms).await?;
    if !out.status.success() {
        return Err(format!(
            "adb {} failed: {}",
            args.first().copied().unwrap_or(""),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(out.stdout)
}

/// Execute a `tools.device` (Android/iOS mobile device control) request.
pub async fn execute_device_request(work_dir: &Path, req: Value) -> Result<Value, String> {
    mobile::execute(work_dir, req).await.map_err(|err| {
        if err.contains("\"disposition\"") { err } else {
            let mut disposition = classify_computer_error(&err);
            disposition.hint = Some(match disposition.kind.as_str() {
                "stale_ref" => "Call tools.device.snapshot() for fresh refs on this device.",
                "settled_absence" => "Inspect the device and take a new tools.device.snapshot(); do not blindly retry.",
                "retryable_failure" => "Transient device transport failure; retry once.",
                _ => "Check platform, device identity, and adb/idb/simctl/WDA configuration.",
            }.into());
            format_computer_error(err, disposition)
        }
    })
}

async fn execute_android_request(work_dir: &Path, req: Value) -> Result<Value, String> {
    let action = req
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();
    let serial = req.get("serial").and_then(Value::as_str);

    let get_i32 = |key: &str| -> Result<i32, String> {
        req.get(key)
            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f.round() as i64)))
            .map(|n| n as i32)
            .ok_or_else(|| format!("missing integer `{key}` for device.{action}"))
    };

    match action.as_str() {
        "screenshot" => {
            let png = run_adb(serial, &["exec-out", "screencap", "-p"], 15_000).await?;
            let (width, height) = parse_png_dimensions(&png).unwrap_or((0, 0));
            let b64 = base64_encode(&png);
            Ok(json!({
                "action": "screenshot",
                "serial": serial,
                "mime_type": "image/png",
                "width": width,
                "height": height,
                "bytes": png.len(),
                "image_base64": b64,
            }))
        }
        "tap" => {
            let x = get_i32("x")?.to_string();
            let y = get_i32("y")?.to_string();
            run_adb(serial, &["shell", "input", "tap", &x, &y], 8_000).await?;
            Ok(json!({
                "action": "tap",
                "serial": serial,
                "x": x.parse::<i32>().unwrap_or(0),
                "y": y.parse::<i32>().unwrap_or(0),
            }))
        }
        "swipe" => {
            let from_x = get_i32("from_x")?.to_string();
            let from_y = get_i32("from_y")?.to_string();
            let to_x = get_i32("to_x")?.to_string();
            let to_y = get_i32("to_y")?.to_string();
            let duration_ms = req
                .get("duration_ms")
                .and_then(Value::as_u64)
                .unwrap_or(300)
                .clamp(50, 10_000)
                .to_string();
            run_adb(
                serial,
                &[
                    "shell",
                    "input",
                    "swipe",
                    &from_x,
                    &from_y,
                    &to_x,
                    &to_y,
                    &duration_ms,
                ],
                12_000,
            )
            .await?;
            Ok(json!({
                "action": "swipe",
                "serial": serial,
                "from_x": from_x.parse::<i32>().unwrap_or(0),
                "from_y": from_y.parse::<i32>().unwrap_or(0),
                "to_x": to_x.parse::<i32>().unwrap_or(0),
                "to_y": to_y.parse::<i32>().unwrap_or(0),
                "duration_ms": duration_ms.parse::<u64>().unwrap_or(300),
            }))
        }
        "type" => {
            let text = req
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `text` for device.type".to_string())?;
            let escaped = escape_adb_input_text(text);
            run_adb(serial, &["shell", "input", "text", &escaped], 10_000).await?;
            Ok(json!({
                "action": "type",
                "serial": serial,
                "chars": text.chars().count(),
            }))
        }
        "press" => {
            let button = req
                .get("button")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `button` for device.press".to_string())?;
            let keycode = android_keyevent_for_button(button)?;
            run_adb(serial, &["shell", "input", "keyevent", keycode], 8_000).await?;
            Ok(json!({
                "action": "press",
                "serial": serial,
                "button": button,
                "keycode": keycode,
            }))
        }
        "install_app" => {
            let raw_path = req
                .get("apk_path")
                .or_else(|| req.get("path"))
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `apk_path` for device.install_app".to_string())?;
            let apk_path = resolve_work_path(work_dir, raw_path)?;
            let apk_str = apk_path.to_string_lossy().to_string();
            let out = run_adb(serial, &["install", "-r", &apk_str], 120_000).await?;
            Ok(json!({
                "action": "install_app",
                "serial": serial,
                "apk_path": apk_str,
                "output": String::from_utf8_lossy(&out).trim(),
            }))
        }
        "launch_app" => {
            let package = req
                .get("package")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `package` for device.launch_app".to_string())?;
            if let Some(activity) = req
                .get("activity")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let component = if activity.contains('/') {
                    activity.to_string()
                } else {
                    format!("{package}/{activity}")
                };
                run_adb(serial, &["shell", "am", "start", "-n", &component], 10_000).await?;
            } else {
                run_adb(
                    serial,
                    &[
                        "shell",
                        "monkey",
                        "-p",
                        package,
                        "-c",
                        "android.intent.category.LAUNCHER",
                        "1",
                    ],
                    10_000,
                )
                .await?;
            }
            Ok(json!({
                "action": "launch_app",
                "serial": serial,
                "package": package,
                "activity": req.get("activity"),
            }))
        }
        "terminate_app" => {
            let package = req
                .get("package")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `package` for device.terminate_app".to_string())?;
            run_adb(serial, &["shell", "am", "force-stop", package], 8_000).await?;
            Ok(json!({
                "action": "terminate_app",
                "serial": serial,
                "package": package,
            }))
        }
        "open_url" => {
            let url = req
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `url` for device.open_url".to_string())?;
            run_adb(
                serial,
                &[
                    "shell",
                    "am",
                    "start",
                    "-a",
                    "android.intent.action.VIEW",
                    "-d",
                    url,
                ],
                10_000,
            )
            .await?;
            Ok(json!({
                "action": "open_url",
                "serial": serial,
                "url": url,
            }))
        }
        "rotate" => {
            let orientation = req
                .get("orientation")
                .and_then(Value::as_str)
                .unwrap_or("portrait")
                .trim()
                .to_ascii_lowercase();
            let rotation_val = match orientation.as_str() {
                "portrait" => "0",
                "landscape" => "1",
                other => {
                    return Err(format!(
                        "unsupported orientation `{other}` (expected portrait|landscape)"
                    ));
                }
            };
            run_adb(
                serial,
                &[
                    "shell",
                    "settings",
                    "put",
                    "system",
                    "accelerometer_rotation",
                    "0",
                ],
                8_000,
            )
            .await?;
            run_adb(
                serial,
                &[
                    "shell",
                    "settings",
                    "put",
                    "system",
                    "user_rotation",
                    rotation_val,
                ],
                8_000,
            )
            .await?;
            Ok(json!({
                "action": "rotate",
                "serial": serial,
                "orientation": orientation,
            }))
        }
        other => Err(format!("unsupported device action `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_mobile_keycodes_and_escapes_adb_text() {
        assert_eq!(android_keyevent_for_button("home").unwrap(), "KEYCODE_HOME");
        assert_eq!(escape_adb_input_text("hello world&1"), "hello%sworld\\&1");
    }
}
