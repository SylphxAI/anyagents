//! Host desktop control: the machine this process runs on. macOS (JXA),
//! Windows (PowerShell), ChromeOS (Crostini and ARC), and a real Linux X11 or
//! Wayland display named by `display` / `wayland_display`. The per-session
//! virtual desktop lives in `session_desktop`; this module never touches it.

use super::*;

mod native;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesktopPlatform {
    Linux,
    MacOs,
    Windows,
    ChromeOs,
}

/// Whether this process runs in ChromeOS Crostini.
pub fn is_crostini() -> bool {
    std::env::var_os("SOMMELIER_VERSION").is_some()
        || std::env::var_os("CHROMEOS_RELEASE_NAME").is_some()
}

impl DesktopPlatform {
    pub fn detect(req: &Value) -> Result<Self, String> {
        let platform = req
            .get("platform")
            .or_else(|| req.get("os"))
            .and_then(Value::as_str);
        Self::from_environment(platform, std::env::consts::OS, is_crostini())
    }

    fn from_environment(
        platform: Option<&str>,
        host: &str,
        crostini: bool,
    ) -> Result<Self, String> {
        let name = platform
            .unwrap_or(if crostini { "chromeos" } else { host })
            .trim()
            .to_ascii_lowercase();
        match name.as_str() {
            "linux" => Ok(Self::Linux),
            "macos" | "mac_os" | "mac" | "darwin" => Ok(Self::MacOs),
            "windows" | "win32" | "win" => Ok(Self::Windows),
            "chromeos" | "chrome_os" | "chromebook" | "crostini" => Ok(Self::ChromeOs),
            other => Err(format!("unsupported desktop platform `{other}`")),
        }
    }
}

fn skip_host_desktop_queries(req: &Value) -> bool {
    // Tests must not query the host desktop or trigger headless permission prompts.
    // An explicit platform/os still opts into exercising that backend.
    crate::sandbox::is_isolated_test_env()
        && req.get("platform").or_else(|| req.get("os")).is_none()
}

async fn list_local_windows(envs: &[(&str, &str)]) -> Result<Vec<Value>, String> {
    // 1. Try `wmctrl -l -G -p`
    if let Ok(out) = run_cmd("wmctrl", &["-l", "-G", "-p"], envs, 5_000).await
        && out.status.success()
    {
        let text = String::from_utf8_lossy(&out.stdout);
        let mut windows = Vec::new();
        for line in text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 9 {
                let win_id = parts[0].to_string();
                let desktop = parts[1].to_string();
                let pid = parts[2].parse::<u32>().unwrap_or(0);
                let x = parts[3].parse::<i32>().unwrap_or(0);
                let y = parts[4].parse::<i32>().unwrap_or(0);
                let width = parts[5].parse::<u32>().unwrap_or(800);
                let height = parts[6].parse::<u32>().unwrap_or(600);
                let title = parts[8..].join(" ");
                windows.push(json!({
                    "window_id": win_id,
                    "desktop": desktop,
                    "pid": pid,
                    "title": title,
                    "bounds": { "x": x, "y": y, "width": width, "height": height },
                }));
            }
        }
        return Ok(windows);
    }

    // 2. Fallback to `xdotool search --onlyvisible --name ""`
    if let Ok(out) = run_cmd(
        "xdotool",
        &["search", "--onlyvisible", "--name", ""],
        envs,
        5_000,
    )
    .await
        && out.status.success()
    {
        let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .take(40)
            .map(ToOwned::to_owned)
            .collect();
        let mut windows = Vec::new();
        for wid in ids {
            let title = run_cmd("xdotool", &["getwindowname", &wid], envs, 2_000)
                .await
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .unwrap_or_default();
            let geom = run_cmd(
                "xdotool",
                &["getwindowgeometry", "--shell", &wid],
                envs,
                2_000,
            )
            .await
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
            .unwrap_or_default();
            let mut x = 0i32;
            let mut y = 0i32;
            let mut width = 800u32;
            let mut height = 600u32;
            for line in geom.lines() {
                if let Some(v) = line.strip_prefix("X=") {
                    x = v.trim().parse().unwrap_or(0);
                } else if let Some(v) = line.strip_prefix("Y=") {
                    y = v.trim().parse().unwrap_or(0);
                } else if let Some(v) = line.strip_prefix("WIDTH=") {
                    width = v.trim().parse().unwrap_or(800);
                } else if let Some(v) = line.strip_prefix("HEIGHT=") {
                    height = v.trim().parse().unwrap_or(600);
                }
            }
            windows.push(json!({
                "window_id": wid,
                "title": title,
                "bounds": { "x": x, "y": y, "width": width, "height": height },
            }));
        }
        return Ok(windows);
    }

    Ok(Vec::new())
}

async fn collect_local_a11y_nodes(
    envs: &[(&str, &str)],
    app_filter: Option<&str>,
    window_filter: Option<&str>,
) -> Vec<RawA11yNode> {
    let windows = list_local_windows(envs).await.unwrap_or_default();
    a11y_nodes_from_windows(windows, app_filter, window_filter)
}

fn a11y_nodes_from_windows(
    windows: Vec<Value>,
    app_filter: Option<&str>,
    window_filter: Option<&str>,
) -> Vec<RawA11yNode> {
    let mut nodes = Vec::new();
    for w in windows {
        let wid = w
            .get("window_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = w
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("Window")
            .to_string();
        if let Some(wf) = window_filter.filter(|s| !s.trim().is_empty())
            && !wid.eq_ignore_ascii_case(wf.trim())
            && !title
                .to_ascii_lowercase()
                .contains(&wf.trim().to_ascii_lowercase())
        {
            continue;
        }
        if let Some(af) = app_filter.filter(|s| !s.trim().is_empty())
            && !title
                .to_ascii_lowercase()
                .contains(&af.trim().to_ascii_lowercase())
        {
            continue;
        }
        let bounds = Region {
            x: w.pointer("/bounds/x").and_then(Value::as_i64).unwrap_or(0) as i32,
            y: w.pointer("/bounds/y").and_then(Value::as_i64).unwrap_or(0) as i32,
            width: w
                .pointer("/bounds/width")
                .and_then(Value::as_u64)
                .unwrap_or(800) as u32,
            height: w
                .pointer("/bounds/height")
                .and_then(Value::as_u64)
                .unwrap_or(600) as u32,
        };
        nodes.push(RawA11yNode {
            role: "window".to_string(),
            name: title.clone(),
            window_id: Some(wid),
            app: Some(title),
            bounds,
            interactive: true,
            children: Vec::new(),
        });
    }
    if nodes.is_empty() {
        nodes.push(RawA11yNode {
            role: "desktop".to_string(),
            name: "Root Desktop".to_string(),
            window_id: Some("root".to_string()),
            app: None,
            bounds: Region {
                x: 0,
                y: 0,
                width: 1920,
                height: 1080,
            },
            interactive: true,
            children: Vec::new(),
        });
    }
    nodes
}

async fn capture_local_screenshot(
    envs: &[(&str, &str)],
    crop: Option<Region>,
) -> Result<(Vec<u8>, &'static str), String> {
    // 1. ImageMagick `import` (X11)
    let mut import_args = vec!["-window", "root"];
    let crop_spec;
    if let Some(r) = crop {
        crop_spec = format!("{}x{}+{}+{}", r.width, r.height, r.x.max(0), r.y.max(0));
        import_args.push("-crop");
        import_args.push(&crop_spec);
        import_args.push("+repage");
    }
    import_args.push("png:-");
    if let Ok(out) = run_cmd("import", &import_args, envs, 10_000).await
        && out.status.success()
        && !out.stdout.is_empty()
    {
        return Ok((out.stdout, "import"));
    }

    // 2. Wayland `grim`
    let mut grim_args = Vec::new();
    let grim_geom;
    if let Some(r) = crop {
        grim_geom = format!("{},{} {}x{}", r.x.max(0), r.y.max(0), r.width, r.height);
        grim_args.push("-g");
        grim_args.push(&grim_geom);
    }
    grim_args.push("-");
    if let Ok(out) = run_cmd("grim", &grim_args, envs, 10_000).await
        && out.status.success()
        && !out.stdout.is_empty()
    {
        return Ok((out.stdout, "grim"));
    }

    // 3. `scrot` via temporary file
    let tmp_path = std::env::temp_dir().join(format!(
        "anyagents-screen-{}-{}.png",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let tmp_str = tmp_path.to_string_lossy().to_string();
    let mut scrot_args = vec!["-o"];
    let scrot_crop;
    if let Some(r) = crop {
        scrot_crop = format!("{},{},{},{}", r.x.max(0), r.y.max(0), r.width, r.height);
        scrot_args.push("-a");
        scrot_args.push(&scrot_crop);
    }
    scrot_args.push(&tmp_str);
    if let Ok(out) = run_cmd("scrot", &scrot_args, envs, 10_000).await
        && out.status.success()
        && let Ok(bytes) = tokio::fs::read(&tmp_path).await
    {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        if !bytes.is_empty() {
            return Ok((bytes, "scrot"));
        }
    }
    let _ = tokio::fs::remove_file(&tmp_path).await;

    Err(
        "failed to capture desktop screenshot: none of `import` (ImageMagick/X11), `grim` (Wayland), or `scrot` succeeded (ensure DISPLAY/WAYLAND_DISPLAY or cloud `session_id` is configured)"
            .to_string(),
    )
}

/// Execute a `tools.computer` request against the host desktop, supporting both
/// **Semantic Accessibility Snapshot Mode (`@<snapshot_id>:e<N>`)** and
/// **Visual Pixel Mode** with structured error retry dispositions.
pub async fn execute(config_dir: &Path, req: Value) -> Result<Value, String> {
    match execute_inner(config_dir, req).await {
        Ok(val) => Ok(val),
        Err(err) if err.contains("\"disposition\"") => Err(err),
        Err(err) => {
            let disp = classify_computer_error(&err);
            Err(format_computer_error(err, disp))
        }
    }
}

async fn execute_inner(config_dir: &Path, req: Value) -> Result<Value, String> {
    let mut payload = if let Some(inner) = req.get("action_json").or_else(|| req.get("payload")) {
        inner.clone()
    } else {
        req.clone()
    };

    let display_owned = payload
        .get("display")
        .or_else(|| req.get("display"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let display = display_owned.as_deref();
    let mut envs = Vec::new();
    if let Some(d) = display.filter(|s| !s.trim().is_empty()) {
        envs.push(("DISPLAY", d));
    }
    let wayland_owned = payload
        .get("wayland_display")
        .or_else(|| req.get("wayland_display"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(d) = wayland_owned.as_deref().filter(|s| !s.trim().is_empty()) {
        envs.push(("WAYLAND_DISPLAY", d));
    }

    let raw_action = payload
        .get("action")
        .or_else(|| payload.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();

    let platform_req = if payload
        .get("platform")
        .or_else(|| payload.get("os"))
        .is_some()
    {
        &payload
    } else {
        &req
    };
    let platform = DesktopPlatform::detect(platform_req)?;
    let skip_host_queries = skip_host_desktop_queries(platform_req);
    if platform == DesktopPlatform::ChromeOs {
        if raw_action == "open_url" {
            let url = payload
                .get("url")
                .and_then(Value::as_str)
                .ok_or("missing `url` for computer.open_url")?;
            let out = run_cmd("garcon-url-handler", &[url], &envs, 10_000).await?;
            checked_output(out, "garcon-url-handler")?;
            return Ok(json!({"action": "open_url", "platform": "chrome_os", "url": url}));
        }
        if raw_action == "device"
            || payload.get("target").and_then(Value::as_str) == Some("android")
        {
            let mut device_req = payload
                .get("device_request")
                .cloned()
                .unwrap_or_else(|| payload.clone());
            device_req["platform"] = json!("android");
            return super::device::execute_device_request(config_dir, device_req).await;
        }
    }

    // Handle Semantic Accessibility Snapshot, Window Management & Clipboard actions first:
    match raw_action.as_str() {
        "snapshot" => {
            let app_filter = payload.get("app").and_then(Value::as_str);
            let window_filter = payload.get("window_id").and_then(Value::as_str);
            let skeleton = payload
                .get("skeleton")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let interactive_only = payload
                .get("interactive_only")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let root_ref = payload
                .get("root_ref")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let resolved_root = if let Some(rref) = root_ref {
                let prev_map = load_ref_map(config_dir);
                match resolve_snapshot_ref(&prev_map, rref) {
                    Ok(entry) => Some(entry),
                    Err(disp) => {
                        return Err(format_computer_error(
                            format!("invalid `root_ref` `{rref}`"),
                            disp,
                        ));
                    }
                }
            } else {
                None
            };

            let snapshot_id = generate_snapshot_id();
            let mut raw_nodes = if skip_host_queries {
                a11y_nodes_from_windows(Vec::new(), app_filter, window_filter)
            } else {
                match platform {
                    DesktopPlatform::MacOs | DesktopPlatform::Windows => {
                        native::collect_nodes(platform, &payload).await?
                    }
                    _ => collect_local_a11y_nodes(&envs, app_filter, window_filter).await,
                }
            };
            if let Some(ref root_entry) = resolved_root {
                raw_nodes = filter_a11y_subtree_by_ref(raw_nodes, root_entry);
            }
            let (elements, ref_map) =
                build_snapshot_tree(&snapshot_id, raw_nodes, skeleton, interactive_only);
            save_ref_map(config_dir, &ref_map);
            return Ok(json!({
                "action": "snapshot",
                "snapshot_id": snapshot_id,
                "platform": platform,
                "root_ref": resolved_root.map(|e| e.ref_id),
                "element_count": ref_map.refs.len(),
                "elements": elements,
            }));
        }
        "list_windows" | "windows" => {
            let windows = if skip_host_queries {
                Vec::new()
            } else {
                match platform {
                    DesktopPlatform::MacOs | DesktopPlatform::Windows => {
                        native::list_windows(platform).await?
                    }
                    _ => list_local_windows(&envs).await?,
                }
            };
            return Ok(json!({
                "action": "list_windows",
                "count": windows.len(),
                "windows": windows,
            }));
        }
        "focus_window" => {
            if matches!(platform, DesktopPlatform::MacOs | DesktopPlatform::Windows) {
                return native::execute_aux(platform, &payload).await;
            }
            let wid = payload
                .get("window_id")
                .or_else(|| payload.get("id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing `window_id` for computer.focus_window".to_string())?;
            if let Ok(out) =
                run_cmd("xdotool", &["windowactivate", "--sync", wid], &envs, 5_000).await
                && out.status.success()
            {
                return Ok(json!({
                    "action": "focus_window",
                    "window_id": wid,
                    "status": "focused",
                }));
            }
            if let Ok(out) = run_cmd("wmctrl", &["-i", "-a", wid], &envs, 5_000).await
                && out.status.success()
            {
                return Ok(json!({
                    "action": "focus_window",
                    "window_id": wid,
                    "status": "focused",
                }));
            }
            return Err(format_computer_error(
                format!("window `{wid}` not found or could not be focused"),
                ErrorDisposition::settled_absence(
                    "Call tools.computer.list_windows() to verify active window IDs.",
                ),
            ));
        }
        "clipboard_get" => {
            if matches!(platform, DesktopPlatform::MacOs | DesktopPlatform::Windows) {
                return native::execute_aux(platform, &payload).await;
            }
            for (prog, args) in [
                ("xclip", &["-selection", "clipboard", "-o"][..]),
                ("xsel", &["--clipboard", "--output"][..]),
                ("wl-paste", &["--no-newline"][..]),
            ] {
                if let Ok(out) = run_cmd(prog, args, &envs, 5_000).await
                    && out.status.success()
                {
                    let text = String::from_utf8_lossy(&out.stdout).to_string();
                    return Ok(json!({
                        "action": "clipboard_get",
                        "backend": prog,
                        "text": text,
                    }));
                }
            }
            return Err(
                "clipboard_get failed: none of `xclip`, `xsel`, or `wl-paste` succeeded"
                    .to_string(),
            );
        }
        "clipboard_set" => {
            if matches!(platform, DesktopPlatform::MacOs | DesktopPlatform::Windows) {
                return native::execute_aux(platform, &payload).await;
            }
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `text` for computer.clipboard_set".to_string())?;
            for (prog, args) in [
                ("xclip", &["-selection", "clipboard", "-i"][..]),
                ("xsel", &["--clipboard", "--input"][..]),
                ("wl-copy", &[][..]),
            ] {
                if let Ok(out) = run_cmd_with_stdin(prog, args, &envs, text.as_bytes(), 5_000).await
                    && out.status.success()
                {
                    return Ok(json!({
                        "action": "clipboard_set",
                        "backend": prog,
                        "bytes": text.len(),
                        "stdin_piped": true,
                    }));
                }
            }
            return Err(
                "clipboard_set failed: none of `xclip`, `xsel`, or `wl-copy` succeeded".to_string(),
            );
        }
        _ => {}
    }

    // If `ref` (`@<snapshot_id>:e<N>`) is supplied on `click`, `move`, `type`, `scroll`, etc.,
    // validate against `SnapshotRefMap` and resolve target coordinates or window focus!
    let mut resolved_ref_entry: Option<RefEntry> = None;
    if let Some(raw_ref) = payload
        .get("ref")
        .or_else(|| payload.get("ref_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let ref_map = load_ref_map(config_dir);
        match resolve_snapshot_ref(&ref_map, raw_ref) {
            Ok(entry) => {
                let center = entry.bounds.center();
                if let Some(obj) = payload.as_object_mut() {
                    obj.entry("x".to_string())
                        .or_insert_with(|| json!(center.x));
                    obj.entry("y".to_string())
                        .or_insert_with(|| json!(center.y));
                }
                resolved_ref_entry = Some(entry);
            }
            Err(disp) => {
                return Err(format_computer_error(
                    disp.hint.clone().unwrap_or_else(|| "STALE_REF".to_string()),
                    disp,
                ));
            }
        }
    }

    let action = parse_computer_action(&payload)?;
    if matches!(platform, DesktopPlatform::MacOs | DesktopPlatform::Windows) {
        return native::execute_action(platform, &action, resolved_ref_entry.as_ref()).await;
    }
    if let Some(ref entry) = resolved_ref_entry
        && let Some(ref wid) = entry.window_id
        && wid != "root"
    {
        let _ = run_cmd("xdotool", &["windowactivate", "--sync", wid], &envs, 2_000).await;
    }

    match action {
        ComputerAction::Screenshot => {
            let (png, backend) = capture_local_screenshot(&envs, None).await?;
            let (width, height) = parse_png_dimensions(&png).unwrap_or((0, 0));
            let b64 = base64_encode(&png);
            Ok(json!({
                "action": "screenshot",
                "backend": backend,
                "mime_type": "image/png",
                "width": width,
                "height": height,
                "bytes": png.len(),
                "image_base64": b64,
            }))
        }
        ComputerAction::Zoom { region } => {
            let (png, backend) = capture_local_screenshot(&envs, Some(region)).await?;
            let (width, height) =
                parse_png_dimensions(&png).unwrap_or((region.width, region.height));
            let b64 = base64_encode(&png);
            Ok(json!({
                "action": "zoom",
                "backend": backend,
                "region": region,
                "mime_type": "image/png",
                "width": width,
                "height": height,
                "bytes": png.len(),
                "image_base64": b64,
            }))
        }
        ComputerAction::CursorPosition => {
            let out = run_cmd("xdotool", &["getmouselocation", "--shell"], &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool getmouselocation failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            let text = String::from_utf8_lossy(&out.stdout);
            let mut x = 0i32;
            let mut y = 0i32;
            let mut screen = 0i32;
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("X=") {
                    x = v.trim().parse().unwrap_or(0);
                } else if let Some(v) = line.strip_prefix("Y=") {
                    y = v.trim().parse().unwrap_or(0);
                } else if let Some(v) = line.strip_prefix("SCREEN=") {
                    screen = v.trim().parse().unwrap_or(0);
                }
            }
            Ok(json!({
                "action": "cursor_position",
                "x": x,
                "y": y,
                "screen": screen,
            }))
        }
        ComputerAction::MouseMove { coordinate } => {
            let xs = coordinate.x.to_string();
            let ys = coordinate.y.to_string();
            let out = run_cmd("xdotool", &["mousemove", "--sync", &xs, &ys], &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool mousemove failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "mouse_move",
                "x": coordinate.x,
                "y": coordinate.y,
                "ref": resolved_ref_entry.map(|e| e.ref_id),
            }))
        }
        ComputerAction::Click {
            coordinate,
            button,
            count,
            modifiers,
        } => {
            let xs = coordinate.x.to_string();
            let ys = coordinate.y.to_string();
            let repeat = count.max(1).to_string();
            let btn = button.xdotool_code();
            let mut args: Vec<String> = vec!["mousemove".into(), "--sync".into(), xs, ys];
            for m in &modifiers {
                args.push("keydown".into());
                args.push(m.clone());
            }
            args.extend(["click".into(), "--repeat".into(), repeat, btn.into()]);
            for m in modifiers.iter().rev() {
                args.push("keyup".into());
                args.push(m.clone());
            }
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let out = run_cmd("xdotool", &arg_refs, &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool click failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "click",
                "x": coordinate.x,
                "y": coordinate.y,
                "button": button,
                "count": count,
                "modifiers": modifiers,
                "ref": resolved_ref_entry.map(|e| e.ref_id),
            }))
        }
        ComputerAction::MouseDown { coordinate, button } => {
            let mut args: Vec<String> = Vec::new();
            if let Some(c) = coordinate {
                args.extend([
                    "mousemove".into(),
                    "--sync".into(),
                    c.x.to_string(),
                    c.y.to_string(),
                ]);
            }
            args.extend(["mousedown".into(), button.xdotool_code().into()]);
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let out = run_cmd("xdotool", &arg_refs, &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool mousedown failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "mouse_down",
                "coordinate": coordinate,
                "button": button,
            }))
        }
        ComputerAction::MouseUp { coordinate, button } => {
            let mut args: Vec<String> = Vec::new();
            if let Some(c) = coordinate {
                args.extend([
                    "mousemove".into(),
                    "--sync".into(),
                    c.x.to_string(),
                    c.y.to_string(),
                ]);
            }
            args.extend(["mouseup".into(), button.xdotool_code().into()]);
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let out = run_cmd("xdotool", &arg_refs, &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool mouseup failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "mouse_up",
                "coordinate": coordinate,
                "button": button,
            }))
        }
        ComputerAction::Drag { start, end, button } => {
            let btn = button.xdotool_code();
            let s_x = start.x.to_string();
            let s_y = start.y.to_string();
            let e_x = end.x.to_string();
            let e_y = end.y.to_string();
            let out = run_cmd(
                "xdotool",
                &[
                    "mousemove",
                    "--sync",
                    &s_x,
                    &s_y,
                    "mousedown",
                    btn,
                    "mousemove",
                    "--sync",
                    &e_x,
                    &e_y,
                    "mouseup",
                    btn,
                ],
                &envs,
                8_000,
            )
            .await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool drag failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "drag",
                "start": start,
                "end": end,
                "button": button,
            }))
        }
        ComputerAction::Scroll {
            coordinate,
            direction,
            amount,
            modifiers,
        } => {
            let xs = coordinate.x.to_string();
            let ys = coordinate.y.to_string();
            let repeat = amount.max(1).to_string();
            let btn = direction.xdotool_button();
            let mut args: Vec<String> = vec!["mousemove".into(), "--sync".into(), xs, ys];
            for m in &modifiers {
                args.push("keydown".into());
                args.push(m.clone());
            }
            args.extend(["click".into(), "--repeat".into(), repeat, btn.into()]);
            for m in modifiers.iter().rev() {
                args.push("keyup".into());
                args.push(m.clone());
            }
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let out = run_cmd("xdotool", &arg_refs, &envs, 5_000).await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool scroll failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "scroll",
                "coordinate": coordinate,
                "direction": direction,
                "amount": amount,
            }))
        }
        ComputerAction::Type { text, delay_ms } => {
            if let Some(ref entry) = resolved_ref_entry {
                let c = entry.bounds.center();
                let _ = run_cmd(
                    "xdotool",
                    &[
                        "mousemove",
                        "--sync",
                        &c.x.to_string(),
                        &c.y.to_string(),
                        "click",
                        "1",
                    ],
                    &envs,
                    3_000,
                )
                .await;
            }
            let delay = delay_ms.unwrap_or(12).to_string();
            let out = run_cmd_with_stdin(
                "xdotool",
                &["type", "--clearmodifiers", "--delay", &delay, "--file", "-"],
                &envs,
                text.as_bytes(),
                15_000,
            )
            .await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool type failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "type",
                "chars": text.chars().count(),
                "stdin_piped": true,
                "ref": resolved_ref_entry.map(|e| e.ref_id),
            }))
        }
        ComputerAction::Key { key, modifiers } => {
            let chord = if modifiers.is_empty() {
                key.clone()
            } else {
                format!("{}+{key}", modifiers.join("+"))
            };
            let out = run_cmd(
                "xdotool",
                &["key", "--clearmodifiers", "--", &chord],
                &envs,
                5_000,
            )
            .await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool key failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "key",
                "key": chord,
            }))
        }
        ComputerAction::HoldKey { key, duration_ms } => {
            let sleep_secs = format!("{:.3}", (duration_ms as f64) / 1000.0);
            let out = run_cmd(
                "xdotool",
                &["keydown", &key, "sleep", &sleep_secs, "keyup", &key],
                &envs,
                (duration_ms as u64).saturating_add(5_000),
            )
            .await?;
            if !out.status.success() {
                return Err(format!(
                    "xdotool hold_key failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(json!({
                "action": "hold_key",
                "key": key,
                "duration_ms": duration_ms,
            }))
        }
        ComputerAction::Wait { duration_ms } => {
            tokio::time::sleep(Duration::from_millis(duration_ms as u64)).await;
            Ok(json!({
                "action": "wait",
                "duration_ms": duration_ms,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_platform_detection_and_overrides() {
        for (name, expected) in [
            ("linux", DesktopPlatform::Linux),
            ("darwin", DesktopPlatform::MacOs),
            ("windows", DesktopPlatform::Windows),
            ("chromebook", DesktopPlatform::ChromeOs),
        ] {
            assert_eq!(
                DesktopPlatform::from_environment(Some(name), "linux", false).unwrap(),
                expected
            );
        }
        assert_eq!(
            DesktopPlatform::from_environment(None, "linux", true).unwrap(),
            DesktopPlatform::ChromeOs
        );
        assert_eq!(
            DesktopPlatform::from_environment(Some("linux"), "linux", true).unwrap(),
            DesktopPlatform::Linux
        );
        assert_eq!(
            DesktopPlatform::from_environment(None, "macos", false).unwrap(),
            DesktopPlatform::MacOs
        );
        assert!(DesktopPlatform::from_environment(Some("unsupported"), "linux", false).is_err());
    }

    #[test]
    fn isolated_desktop_queries_require_an_explicit_platform() {
        assert!(skip_host_desktop_queries(&json!({"action": "snapshot"})));
        for key in ["platform", "os"] {
            for platform in ["linux", "macos", "windows", "chromeos"] {
                let mut req = json!({"action": "snapshot"});
                req[key] = json!(platform);
                assert!(!skip_host_desktop_queries(&req), "{req}");
            }
        }
    }

    #[test]
    fn isolated_desktop_snapshot_and_window_list_complete_without_subprocesses() {
        use futures::FutureExt as _;

        let dir = tempfile::tempdir().unwrap();
        for req in [
            json!({"action": "snapshot"}),
            json!({"action_json": {"action": "snapshot"}}),
            json!({"payload": {"action": "snapshot"}}),
        ] {
            // No runtime is needed: isolated snapshots must complete on the first poll,
            // without spawning desktop backends or awaiting a permission prompt.
            let snap = execute(dir.path(), req)
                .now_or_never()
                .expect("isolated snapshot must not wait for host desktop queries")
                .unwrap();
            assert_eq!(snap["element_count"], 1);
            assert_eq!(snap["elements"][0]["role"], "desktop");
            let map = load_ref_map(dir.path());
            let entry = resolve_snapshot_ref(&map, snap["elements"][0]["ref_id"].as_str().unwrap())
                .unwrap();
            assert_eq!(entry.window_id.as_deref(), Some("root"));
        }
        for action in ["list_windows", "windows"] {
            let windows = execute(dir.path(), json!({"action": action}))
                .now_or_never()
                .expect("isolated window lists must not wait for host desktop queries")
                .unwrap();
            assert_eq!(windows["count"], 0);
            assert_eq!(windows["windows"], json!([]));
        }
    }
}
