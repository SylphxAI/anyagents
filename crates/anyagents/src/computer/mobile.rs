//! Android and iOS semantic snapshots and device-specific, snapshot-scoped targeting.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MobilePlatform {
    Android,
    Ios,
}

fn requested_platform(req: &Value) -> Result<Option<MobilePlatform>, String> {
    match req
        .get("platform")
        .or_else(|| req.get("os"))
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("android") => Ok(Some(MobilePlatform::Android)),
        Some("ios" | "iphone" | "ipad") => Ok(Some(MobilePlatform::Ios)),
        Some(other) => Err(format!("unsupported device platform `{other}`")),
        None if req.get("udid").is_some() || req.get("wda_url").is_some() => {
            Ok(Some(MobilePlatform::Ios))
        }
        None => Ok(None),
    }
}

async fn booted_simulator() -> Option<String> {
    let out = run_cmd(
        "xcrun",
        &["simctl", "list", "devices", "booted", "--json"],
        &[],
        5_000,
    )
    .await
    .ok()?;
    if !out.status.success() {
        return None;
    }
    let value: Value = serde_json::from_slice(&out.stdout).ok()?;
    let devices: Vec<_> = value["devices"]
        .as_object()?
        .values()
        .filter_map(Value::as_array)
        .flatten()
        .filter(|d| d["state"] == "Booted")
        .collect();
    if devices.len() != 1 {
        return None;
    }
    devices[0]["udid"].as_str().map(str::to_owned)
}

fn ref_dir(work_dir: &Path, platform: MobilePlatform, identity: &str) -> PathBuf {
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{platform:?}:{identity}").as_bytes())
    );
    work_dir.join(".anyagents-device-refs").join(digest)
}

fn resolve_target(req: &mut Value, dir: &Path, action: &str) -> Result<bool, String> {
    let Some(raw_ref) = req
        .get("ref")
        .or_else(|| req.get("ref_id"))
        .and_then(Value::as_str)
    else {
        return Ok(false);
    };
    if !matches!(action, "tap" | "type") {
        return Err("device refs are supported by tap and type".into());
    }
    let entry = resolve_snapshot_ref(&load_ref_map(dir), raw_ref).map_err(|disposition| {
        format_computer_error(
            "STALE_REF: device ref is not in this device's active snapshot",
            disposition,
        )
    })?;
    let center = entry.bounds.center();
    // A semantic ref is authoritative: explicit coordinates cannot silently override its target.
    req["x"] = json!(center.x);
    req["y"] = json!(center.y);
    Ok(true)
}

pub(super) async fn execute(work_dir: &Path, mut req: Value) -> Result<Value, String> {
    let action = req
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();
    req["action"] = json!(action);
    let explicit = requested_platform(&req)?;
    if req["backend"]
        .as_str()
        .is_some_and(|b| !matches!(b, "idb" | "simctl" | "wda" | "adb" | "auto"))
    {
        return Err("unsupported device backend (expected auto|adb|simctl|idb|wda)".into());
    }
    if req["backend"] == "wda" && req.get("wda_url").is_none() {
        return Err("backend:wda requires wda_url".into());
    }
    let booted = if std::env::consts::OS == "macos" && explicit != Some(MobilePlatform::Android) {
        booted_simulator().await
    } else {
        None
    };
    let platform = explicit.unwrap_or(if booted.is_some() {
        MobilePlatform::Ios
    } else {
        MobilePlatform::Android
    });
    let identity = match platform {
        MobilePlatform::Android => {
            if let Some(serial) = req
                .get("serial")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                serial.to_owned()
            } else {
                let devices = run_adb(None, &["devices"], 5_000).await?;
                let text = String::from_utf8_lossy(&devices);
                let serials: Vec<_> = text
                    .lines()
                    .skip(1)
                    .filter_map(|l| {
                        let mut p = l.split_whitespace();
                        let serial = p.next()?;
                        (p.next()? == "device").then_some(serial)
                    })
                    .collect();
                if serials.len() != 1 {
                    return Err("select `serial`: expected exactly one authorized adb device (including ChromeOS ARC bridge)".into());
                }
                serials[0].to_owned()
            }
        }
        MobilePlatform::Ios => req
            .get("wda_url")
            .and_then(Value::as_str)
            .or_else(|| req.get("udid").and_then(Value::as_str))
            .map(str::to_owned)
            .or(booted)
            .ok_or("iOS requires udid, wda_url, or exactly one booted simulator")?,
    };
    match platform {
        MobilePlatform::Android => req["serial"] = json!(identity),
        MobilePlatform::Ios if req.get("wda_url").is_none() => req["udid"] = json!(identity),
        _ => {}
    }
    // Include UDID and WDA session identity: a ref must never cross devices or sessions.
    let ref_identity = format!(
        "{}:{}:{}",
        identity,
        req["udid"].as_str().unwrap_or_default(),
        req["wda_session_id"].as_str().unwrap_or_default()
    );
    let dir = ref_dir(work_dir, platform, &ref_identity);
    if action == "snapshot" {
        let raw = match platform {
            MobilePlatform::Android => android_nodes(Some(&identity)).await?,
            MobilePlatform::Ios => ios_nodes(&req).await?,
        };
        let snapshot_id = generate_snapshot_id();
        let (elements, refs) = build_snapshot_tree(
            &snapshot_id,
            raw,
            req["skeleton"].as_bool().unwrap_or(false),
            req["interactive_only"].as_bool().unwrap_or(false),
        );
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("cannot persist device refs: {e}"))?;
        save_ref_map(&dir, &refs);
        return Ok(
            json!({"action": "snapshot", "platform": if platform == MobilePlatform::Ios { "ios" } else { "android" }, "device": identity,
            "snapshot_id": snapshot_id, "element_count": refs.refs.len(), "elements": elements}),
        );
    }
    let targeted = resolve_target(&mut req, &dir, &action)?;
    if targeted && action == "type" {
        let mut tap = req.clone();
        tap["action"] = json!("tap");
        match platform {
            MobilePlatform::Android => {
                execute_android_request(work_dir, tap).await?;
            }
            MobilePlatform::Ios => {
                execute_ios(work_dir, &tap).await?;
            }
        }
    }
    let mut result = match platform {
        MobilePlatform::Android => execute_android_request(work_dir, req.clone()).await?,
        MobilePlatform::Ios => execute_ios(work_dir, &req).await?,
    };
    result["platform"] = json!(if platform == MobilePlatform::Ios {
        "ios"
    } else {
        "android"
    });
    if targeted {
        result["ref"] = req
            .get("ref")
            .or_else(|| req.get("ref_id"))
            .cloned()
            .unwrap_or_default();
    }
    // Rotation changes every coordinate, so old refs must no longer be usable.
    if action == "rotate" {
        save_ref_map(&dir, &SnapshotRefMap::default());
    }
    Ok(result)
}

async fn android_nodes(serial: Option<&str>) -> Result<Vec<RawA11yNode>, String> {
    if let Ok(bytes) = run_adb(
        serial,
        &["exec-out", "uiautomator", "dump", "/dev/tty"],
        20_000,
    )
    .await
        && let Ok(nodes) = parse_android_tree(&String::from_utf8_lossy(&bytes))
    {
        return Ok(nodes);
    }
    run_adb(
        serial,
        &["shell", "uiautomator", "dump", "/sdcard/window_dump.xml"],
        20_000,
    )
    .await?;
    let bytes = run_adb(
        serial,
        &["exec-out", "cat", "/sdcard/window_dump.xml"],
        10_000,
    )
    .await?;
    parse_android_tree(&String::from_utf8_lossy(&bytes))
}

fn android_bounds(raw: &str) -> Result<Region, String> {
    let values = raw
        .split(['[', ']', ','])
        .filter(|s| !s.is_empty())
        .map(str::parse::<i32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "invalid Android bounds")?;
    if values.len() != 4 || values[2] < values[0] || values[3] < values[1] {
        return Err("invalid Android bounds".into());
    }
    Ok(Region {
        x: values[0],
        y: values[1],
        width: (values[2] - values[0]) as u32,
        height: (values[3] - values[1]) as u32,
    })
}

fn parse_android_tree(text: &str) -> Result<Vec<RawA11yNode>, String> {
    let start = text
        .find("<hierarchy")
        .ok_or("Android UI hierarchy not found")?;
    let end = text[start..]
        .find("</hierarchy>")
        .map(|n| start + n + "</hierarchy>".len())
        .ok_or("incomplete Android hierarchy")?;
    let doc = roxmltree::Document::parse(&text[start..end])
        .map_err(|e| format!("invalid Android hierarchy: {e}"))?;
    fn node(
        n: roxmltree::Node<'_, '_>,
        depth: usize,
        count: &mut usize,
    ) -> Result<RawA11yNode, String> {
        *count += 1;
        if depth > 64 || *count > 5000 {
            return Err("Android accessibility tree exceeds safety limit".into());
        }
        let class = n.attribute("class").unwrap_or("unknown");
        let text = n.attribute("text").unwrap_or_default();
        let name = if text.is_empty() {
            n.attribute("content-desc").unwrap_or_default()
        } else {
            text
        };
        let interactive = n.attribute("enabled") != Some("false")
            && ["clickable", "long-clickable", "focusable", "scrollable"]
                .iter()
                .any(|key| n.attribute(*key) == Some("true"));
        let role = if class.ends_with("EditText") {
            "textbox"
        } else if class.ends_with("Button") {
            "button"
        } else if class.ends_with("TextView") {
            "text"
        } else if class.ends_with("CheckBox") {
            "checkbox"
        } else if class.ends_with("Layout")
            || class.ends_with("ViewGroup")
            || class.ends_with("View")
        {
            "group"
        } else {
            class
        };
        let children = n
            .children()
            .filter(|n| n.has_tag_name("node"))
            .map(|n| node(n, depth + 1, count))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RawA11yNode {
            role: role.into(),
            name: name.into(),
            window_id: None,
            app: n.attribute("package").map(str::to_owned),
            bounds: android_bounds(n.attribute("bounds").unwrap_or("[0,0][0,0]"))?,
            interactive,
            children,
        })
    }
    let mut count = 0;
    doc.root_element()
        .children()
        .filter(|n| n.has_tag_name("node"))
        .map(|n| node(n, 0, &mut count))
        .collect()
}

fn parse_ios_tree(value: &Value) -> Result<Vec<RawA11yNode>, String> {
    fn nodes(v: &Value, depth: usize, count: &mut usize) -> Result<Vec<RawA11yNode>, String> {
        if depth > 64 || *count > 5000 {
            return Err("iOS accessibility tree exceeds safety limit".into());
        }
        if let Some(array) = v.as_array() {
            let mut out = Vec::new();
            for v in array {
                out.extend(nodes(v, depth + 1, count)?);
            }
            return Ok(out);
        }
        if !v.is_object() {
            return Err("invalid iOS accessibility node".into());
        }
        if let Some(value) = v.get("value").filter(|v| v.is_object() || v.is_array()) {
            return nodes(value, depth + 1, count);
        }
        if let Some(tree) = v.get("tree") {
            return nodes(tree, depth + 1, count);
        }
        if v.get("type")
            .or_else(|| v.get("role"))
            .or_else(|| v.get("AXRole"))
            .is_none()
        {
            if let Some(children) = v.get("children").or_else(|| v.get("elements")) {
                return nodes(children, depth + 1, count);
            }
            return Err("iOS accessibility response has no element tree".into());
        }
        *count += 1;
        if v["visible"] == false || v["isVisible"] == false {
            return Ok(vec![]);
        }
        let raw_role = v
            .get("type")
            .or_else(|| v.get("role"))
            .or_else(|| v.get("AXRole"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let role = match raw_role
            .trim_start_matches("XCUIElementType")
            .to_ascii_lowercase()
            .as_str()
        {
            "other" | "group" | "axgroup" => "group",
            "scrollview" => "scroll_pane",
            "button" | "axbutton" => "button",
            "textfield" | "securetextfield" | "textview" => "textbox",
            "statictext" => "text",
            "application" => "application",
            "window" => "window",
            _ => raw_role,
        };
        let name = ["label", "AXLabel", "name", "value", "AXValue"]
            .iter()
            .filter_map(|k| v.get(k).and_then(Value::as_str))
            .find(|s| !s.is_empty())
            .unwrap_or_default();
        let frame = v.get("rect").or_else(|| v.get("frame")).unwrap_or(v);
        let number = |key: &str| frame.get(key).and_then(Value::as_f64).unwrap_or(0.0);
        let bounds = Region {
            x: number("x").round() as i32,
            y: number("y").round() as i32,
            width: number("width").max(0.0).round() as u32,
            height: number("height").max(0.0).round() as u32,
        };
        let children = v
            .get("children")
            .map(|c| nodes(c, depth + 1, count))
            .transpose()?
            .unwrap_or_default();
        let interactive = v["enabled"] != false
            && (v["isEnabled"] != false)
            && (v["hittable"] == true
                || matches!(role, "button" | "textbox" | "switch" | "link" | "cell"));
        Ok(vec![RawA11yNode {
            role: role.into(),
            name: name.into(),
            window_id: None,
            app: None,
            bounds,
            interactive,
            children,
        }])
    }
    nodes(value, 0, &mut 0)
}

async fn idb(req: &Value, args: &[&str], timeout: u64) -> Result<Vec<u8>, String> {
    let udid = req["udid"].as_str().ok_or("idb requires udid")?;
    let mut argv = args.to_vec();
    argv.extend(["--udid", udid]);
    desktop::checked_output(run_cmd("idb", &argv, &[], timeout).await?, "idb")
}

async fn simctl(
    req: &Value,
    command: &str,
    args: &[&str],
    timeout: u64,
) -> Result<Vec<u8>, String> {
    let udid = req["udid"].as_str().ok_or("simctl requires udid")?;
    let mut argv = vec!["simctl", command, udid];
    argv.extend_from_slice(args);
    desktop::checked_output(run_cmd("xcrun", &argv, &[], timeout).await?, "simctl")
}

async fn wda_http(url: String, method: &'static str, payload: Value) -> Result<Value, String> {
    tokio::task::spawn_blocking(move || {
        let http = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        let response = if method == "GET" {
            http.get(&url).call()
        } else {
            http.post(&url).send_json(&payload)
        }
        .map_err(|e| format!("WDA transport failed: {e}"))?;
        let status = response.status().as_u16();
        let value: Value = response
            .into_body()
            .read_json()
            .map_err(|_| "invalid WDA JSON response")?;
        if !(200..300).contains(&status) || value.pointer("/value/error").is_some() {
            // WDA errors can echo typed secrets. Only expose the error code/status.
            return Err(format!(
                "WDA request failed: HTTP {status} ({})",
                value
                    .pointer("/value/error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        Ok(value)
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn wda(
    req: &Value,
    method: &'static str,
    path: &str,
    payload: Value,
) -> Result<Value, String> {
    let base = req["wda_url"]
        .as_str()
        .ok_or("missing wda_url")?
        .trim_end_matches('/');
    if !(base.starts_with("http://") || base.starts_with("https://")) {
        return Err("wda_url must use http:// or https://".into());
    }
    let session_base = if base.contains("/session/") {
        base.to_owned()
    } else {
        let supplied = req
            .get("wda_session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let session = if let Some(id) = supplied {
            id
        } else {
            let status = wda_http(format!("{base}/status"), "GET", Value::Null).await?;
            if let Some(id) = status.get("sessionId").and_then(Value::as_str) {
                id.to_owned()
            } else {
                let created = wda_http(
                    format!("{base}/session"),
                    "POST",
                    json!({"capabilities": {"alwaysMatch": {}}}),
                )
                .await?;
                created
                    .pointer("/value/sessionId")
                    .or_else(|| created.get("sessionId"))
                    .and_then(Value::as_str)
                    .ok_or("WDA did not return sessionId")?
                    .to_owned()
            }
        };
        if !session
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err("invalid WDA session id".into());
        }
        format!("{base}/session/{session}")
    };
    let response = wda_http(format!("{session_base}{path}"), method, payload).await?;
    Ok(response.get("value").cloned().unwrap_or(response))
}

async fn ios_nodes(req: &Value) -> Result<Vec<RawA11yNode>, String> {
    if req.get("wda_url").is_some() {
        return parse_ios_tree(&wda(req, "GET", "/source?format=json", Value::Null).await?);
    }
    let bytes = idb(req, &["ui", "describe-all", "--json", "--nested"], 30_000).await?;
    let value = serde_json::from_slice::<Value>(&bytes)
        .or_else(|_| {
            String::from_utf8_lossy(&bytes)
                .lines()
                .filter(|s| !s.trim().is_empty())
                .map(serde_json::from_str::<Value>)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        })
        .map_err(|e| format!("invalid idb accessibility JSON: {e}"))?;
    parse_ios_tree(&value)
}

fn field<'a>(req: &'a Value, key: &str) -> Result<&'a str, String> {
    req.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing `{key}` for device action"))
}

fn coordinate(req: &Value, key: &str) -> Result<i32, String> {
    req.get(key)
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n >= 0.0 && *n <= f64::from(i32::MAX))
        .map(|n| n.round() as i32)
        .ok_or_else(|| format!("missing or invalid coordinate `{key}`"))
}

async fn execute_ios(work_dir: &Path, req: &Value) -> Result<Value, String> {
    let action = field(req, "action")?;
    let use_wda = req.get("wda_url").is_some();
    let use_idb = req["backend"] == "idb";
    let mut result = json!({"action": action, "udid": req.get("udid"), "backend": if use_wda { "wda" } else if use_idb { "idb" } else { "simctl/idb" }});
    match action {
        "screenshot" => {
            let png = if use_wda {
                let value = wda(req, "GET", "/screenshot", Value::Null).await?;
                base64_decode(value.as_str().ok_or("WDA screenshot is not base64")?)?
            } else {
                let path = std::env::temp_dir()
                    .join(format!("anyagents-ios-{}.png", generate_snapshot_id()));
                let name = path.to_string_lossy();
                let captured = if use_idb {
                    idb(req, &["screenshot", &name], 15_000).await
                } else {
                    simctl(req, "io", &["screenshot", "--type=png", &name], 15_000).await
                };
                let bytes = match captured {
                    Ok(_) => tokio::fs::read(&path).await.map_err(|e| e.to_string()),
                    Err(err) => Err(err),
                };
                let _ = tokio::fs::remove_file(&path).await;
                bytes?
            };
            let (width, height) =
                parse_png_dimensions(&png).ok_or("iOS backend returned invalid PNG")?;
            result["mime_type"] = json!("image/png");
            result["width"] = json!(width);
            result["height"] = json!(height);
            result["bytes"] = json!(png.len());
            result["image_base64"] = json!(base64_encode(&png));
        }
        "tap" => {
            let x = coordinate(req, "x")?;
            let y = coordinate(req, "y")?;
            if use_wda {
                wda(req, "POST", "/wda/tap", json!({"x":x,"y":y})).await?;
            } else {
                idb(req, &["ui", "tap", &x.to_string(), &y.to_string()], 10_000).await?;
            }
            result["x"] = json!(x);
            result["y"] = json!(y);
        }
        "swipe" => {
            let x1 = coordinate(req, "from_x")?;
            let y1 = coordinate(req, "from_y")?;
            let x2 = coordinate(req, "to_x")?;
            let y2 = coordinate(req, "to_y")?;
            let duration =
                req["duration_ms"].as_u64().unwrap_or(300).clamp(50, 10_000) as f64 / 1000.0;
            if use_wda {
                wda(
                    req,
                    "POST",
                    "/wda/dragfromtoforduration",
                    json!({"fromX":x1,"fromY":y1,"toX":x2,"toY":y2,"duration":duration}),
                )
                .await?;
            } else {
                idb(
                    req,
                    &[
                        "ui",
                        "swipe",
                        &x1.to_string(),
                        &y1.to_string(),
                        &x2.to_string(),
                        &y2.to_string(),
                        "--duration",
                        &duration.to_string(),
                    ],
                    15_000,
                )
                .await?;
            }
        }
        "type" => {
            let text = field(req, "text")?;
            if use_wda {
                wda(
                    req,
                    "POST",
                    "/wda/keys",
                    json!({"value": text.chars().map(|c| c.to_string()).collect::<Vec<_>>()}),
                )
                .await?;
            } else {
                idb(req, &["ui", "text", text], 15_000).await?;
            }
            result["chars"] = json!(text.chars().count());
        }
        "press" => {
            let button = field(req, "button")?.to_ascii_lowercase();
            if use_wda {
                let name = match button.as_str() {
                    "home" => "home",
                    "power" | "lock" => "lock",
                    "volume_up" => "volumeUp",
                    "volume_down" => "volumeDown",
                    _ => return Err("unsupported iOS button".into()),
                };
                wda(req, "POST", "/wda/pressButton", json!({"name":name})).await?;
            } else {
                let name = match button.as_str() {
                    "home" => "HOME",
                    "power" | "lock" => "LOCK",
                    "volume_up" => "VOLUME_UP",
                    "volume_down" => "VOLUME_DOWN",
                    "siri" => "SIRI",
                    _ => return Err("unsupported iOS button".into()),
                };
                idb(req, &["ui", "button", name], 10_000).await?;
            }
        }
        "install_app" => {
            let path = req
                .get("app_path")
                .or_else(|| req.get("path"))
                .and_then(Value::as_str)
                .ok_or("missing `app_path` for iOS install_app (.app/.ipa)")?;
            let path = resolve_work_path(work_dir, path)?;
            let canonical = path
                .canonicalize()
                .map_err(|e| format!("invalid app path: {e}"))?;
            if !canonical.starts_with(work_dir.canonicalize().map_err(|e| e.to_string())?) {
                return Err("app path escapes agent workdir".into());
            }
            let path = canonical.to_string_lossy();
            // WDA intentionally has no install API; idb supports physical devices and .ipa files.
            if use_idb || use_wda {
                idb(req, &["install", &path], 120_000).await?;
            } else {
                simctl(req, "install", &[&path], 120_000).await?;
            }
        }
        "launch_app" | "terminate_app" => {
            let bundle = req
                .get("bundle_id")
                .or_else(|| req.get("package"))
                .and_then(Value::as_str)
                .ok_or("missing iOS bundle_id")?;
            let launch = action == "launch_app";
            if use_wda {
                wda(
                    req,
                    "POST",
                    if launch {
                        "/wda/apps/launch"
                    } else {
                        "/wda/apps/terminate"
                    },
                    json!({"bundleId":bundle}),
                )
                .await?;
            } else if use_idb {
                idb(
                    req,
                    &[if launch { "launch" } else { "terminate" }, bundle],
                    15_000,
                )
                .await?;
            } else {
                simctl(
                    req,
                    if launch { "launch" } else { "terminate" },
                    &[bundle],
                    15_000,
                )
                .await?;
            }
        }
        "open_url" => {
            let url = field(req, "url")?;
            if use_wda {
                wda(req, "POST", "/url", json!({"url":url})).await?;
            } else if use_idb {
                idb(req, &["open", url], 10_000).await?;
            } else {
                simctl(req, "openurl", &[url], 10_000).await?;
            }
        }
        "rotate" => {
            let orientation = req["orientation"]
                .as_str()
                .unwrap_or("portrait")
                .to_ascii_lowercase();
            let orientation = match orientation.as_str() {
                "portrait" => "PORTRAIT",
                "landscape" | "landscape_left" => "LANDSCAPE_LEFT",
                "landscape_right" => "LANDSCAPE_RIGHT",
                "portrait_upside_down" => "PORTRAIT_UPSIDE_DOWN",
                _ => return Err("unsupported iOS orientation".into()),
            };
            if use_wda {
                wda(req,"POST","/orientation",json!({"orientation": if orientation.starts_with("LANDSCAPE") {"LANDSCAPE"} else {"PORTRAIT"}})).await?;
            } else {
                idb(req, &["ui", "rotate", orientation], 10_000).await?;
            }
        }
        other => return Err(format!("unsupported iOS device action `{other}`")),
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wda_device_snapshot_tap_type_actions_and_stale_refs_end_to_end() {
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
        let recorded = calls.clone();
        let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
        png.extend_from_slice(&13u32.to_be_bytes());
        png.extend_from_slice(b"IHDR");
        png.extend_from_slice(&400u32.to_be_bytes());
        png.extend_from_slice(&800u32.to_be_bytes());
        let png = base64_encode(&png);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0u8; 4096];
                let (header_end, length) = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let length = header
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|s| s.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (end + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let request_line = header.lines().next().unwrap().to_owned();
                let body = if length > 0 {
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
                } else {
                    Value::Null
                };
                let value = if request_line.contains("/source?") {
                    json!({"type":"XCUIElementTypeOther","children":[{"type":"XCUIElementTypeTextField","label":"Search","rect":{"x":10,"y":20,"width":100,"height":40},"enabled":true}]})
                } else if request_line.contains("/screenshot") {
                    json!(png)
                } else {
                    Value::Null
                };
                recorded.lock().unwrap().push((request_line, body));
                let body =
                    serde_json::to_vec(&json!({"sessionId":"fixture","value":value})).unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let base = json!({"platform":"ios","wda_url":format!("http://{address}"),"wda_session_id":"fixture"});
        let request = |extra: Value| {
            let mut r = base.clone();
            r.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            r
        };
        let first = execute_device_request(dir.path(), request(json!({"action":"snapshot"})))
            .await
            .unwrap();
        let first_ref = first["elements"][0]["ref_id"].as_str().unwrap();
        assert_eq!(first["elements"][0]["name"], "Search");
        let tap =
            execute_device_request(dir.path(), request(json!({"action":"tap","ref":first_ref})))
                .await
                .unwrap();
        assert_eq!(tap["x"], 60);
        assert_eq!(tap["y"], 40);
        execute_device_request(
            dir.path(),
            request(json!({"action":"type","ref":first_ref,"text":"hello \"世界\""})),
        )
        .await
        .unwrap();
        for extra in [
            json!({"action":"swipe","from_x":1,"from_y":2,"to_x":3,"to_y":4}),
            json!({"action":"press","button":"home"}),
            json!({"action":"launch_app","bundle_id":"com.test.app"}),
            json!({"action":"terminate_app","bundle_id":"com.test.app"}),
            json!({"action":"open_url","url":"https://example.test"}),
            json!({"action":"rotate","orientation":"landscape"}),
        ] {
            execute_device_request(dir.path(), request(extra))
                .await
                .unwrap();
        }
        let image = execute_device_request(dir.path(), request(json!({"action":"screenshot"})))
            .await
            .unwrap();
        assert_eq!(image["width"], 400);
        let second = execute_device_request(dir.path(), request(json!({"action":"snapshot"})))
            .await
            .unwrap();
        assert_ne!(first["snapshot_id"], second["snapshot_id"]);
        let before = calls.lock().unwrap().len();
        let error =
            execute_device_request(dir.path(), request(json!({"action":"tap","ref":first_ref})))
                .await
                .unwrap_err();
        assert!(error.contains("STALE_REF"));
        assert_eq!(before, calls.lock().unwrap().len());
        let calls = calls.lock().unwrap();
        assert!(calls.iter().any(|(line, body)| {
            line.contains("/wda/keys")
                && body["value"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<String>()
                    == "hello \"世界\""
        }));
        assert!(
            calls
                .iter()
                .any(|(line, _)| line.contains("/wda/apps/launch"))
        );
        server.abort();
    }

    #[test]
    fn android_semantics_prune_wrappers_decode_entities_and_assign_refs() {
        let raw = parse_android_tree(r#"UI dump: <hierarchy><node class="android.widget.FrameLayout" bounds="[0,0][400,800]"><node text="Save &amp; Close" content-desc="save" class="android.widget.Button" clickable="true" bounds="[10,20][110,60]"/></node></hierarchy> dumped"#).unwrap();
        let (tree, map) = build_snapshot_tree("sandroid", raw, false, false);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].role, "button");
        assert_eq!(tree[0].name, "Save & Close");
        assert_eq!(
            resolve_snapshot_ref(&map, "@sandroid:e1")
                .unwrap()
                .bounds
                .center(),
            Point { x: 60, y: 40 }
        );
        assert!(parse_android_tree("not XML").is_err());
        assert!(android_bounds("[10,0][0,20]").is_err());
    }

    #[test]
    fn ios_wda_and_idb_semantics() {
        for value in [
            json!({"value":{"type":"XCUIElementTypeOther","children":[{"type":"XCUIElementTypeButton","label":"Continue","rect":{"x":20,"y":40,"width":100,"height":60},"enabled":true}]}}),
            json!([{"role":"AXButton","AXLabel":"Continue","frame":{"x":20,"y":40,"width":100,"height":60}}]),
        ] {
            let (tree, map) =
                build_snapshot_tree("sios", parse_ios_tree(&value).unwrap(), false, false);
            assert_eq!(tree.len(), 1);
            assert_eq!(tree[0].name, "Continue");
            assert_eq!(
                resolve_snapshot_ref(&map, "@sios:e1")
                    .unwrap()
                    .bounds
                    .center(),
                Point { x: 70, y: 70 }
            );
        }
    }

    #[test]
    fn refs_are_device_scoped_and_stale_after_new_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let a = ref_dir(dir.path(), MobilePlatform::Android, "a");
        let b = ref_dir(dir.path(), MobilePlatform::Android, "b");
        std::fs::create_dir_all(&a).unwrap();
        let raw = parse_ios_tree(&json!({"type":"XCUIElementTypeButton","label":"Save","rect":{"x":0,"y":0,"width":20,"height":40}})).unwrap();
        let (_, map) = build_snapshot_tree("s1", raw.clone(), false, false);
        save_ref_map(&a, &map);
        let mut req = json!({"ref":"@s1:e1","x":999,"y":999});
        assert!(resolve_target(&mut req, &a, "tap").unwrap());
        assert_eq!(req["x"], 10);
        assert!(
            resolve_target(&mut req, &b, "tap")
                .unwrap_err()
                .contains("STALE_REF")
        );
        let (_, map) = build_snapshot_tree("s2", raw, false, false);
        save_ref_map(&a, &map);
        assert!(
            resolve_target(&mut req, &a, "type")
                .unwrap_err()
                .contains("STALE_REF")
        );
    }

    #[test]
    fn platform_selection_is_explicit_or_ios_identity_driven() {
        assert_eq!(
            requested_platform(&json!({"udid":"x"})).unwrap(),
            Some(MobilePlatform::Ios)
        );
        assert_eq!(
            requested_platform(&json!({"wda_url":"http://localhost:8100"})).unwrap(),
            Some(MobilePlatform::Ios)
        );
        assert_eq!(
            requested_platform(&json!({"platform":"android","udid":"x"})).unwrap(),
            Some(MobilePlatform::Android)
        );
        assert!(requested_platform(&json!({"platform":"other"})).is_err());
    }
}
