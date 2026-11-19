//! Pure-Rust native host desktop backend powered by `agent_workstation::PlatformAdapter`.
//!
//! Routes accessibility tree snapshots, window enumeration, and native actions
//! directly through the compile-time OS adapter (Linux AT-SPI2/Wayland, macOS
//! Accessibility/CoreGraphics, Windows UI Automation) with zero JXA or
//! PowerShell host scripts.

use std::sync::OnceLock;

use agent_workstation::{
    AdapterError, Button, DesktopAction, DesktopNode, DesktopTree, DesktopWindow, NodeRef,
    PlatformAdapter, TreeOptions, native_adapter,
};

use super::*;

static NATIVE_ADAPTER: OnceLock<Box<dyn PlatformAdapter>> = OnceLock::new();

fn shared_adapter() -> &'static dyn PlatformAdapter {
    NATIVE_ADAPTER.get_or_init(native_adapter).as_ref()
}

fn format_adapter_error(err: AdapterError) -> String {
    let disp = match &err {
        AdapterError::StaleRef(_) => ErrorDisposition::stale_ref(
            "Call tools.computer.snapshot() to refresh @<snapshot_id>:e<N> refs.",
        ),
        AdapterError::NodeNotFound(_) => ErrorDisposition::settled_absence(
            "Target node was not found in the active snapshot; refresh via snapshot() or list_windows().",
        ),
        AdapterError::PermissionDenied(_) => ErrorDisposition::terminal(
            "Host accessibility or input permission denied; grant accessibility access on this host.",
        ),
        AdapterError::UnsupportedPlatform(_) | AdapterError::UnsupportedAction(_) => {
            let mut d = ErrorDisposition::terminal(
                "This host desktop capability is not supported on the current platform.",
            );
            d.code = Some("UNSUPPORTED".to_string());
            d
        }
        AdapterError::InvalidRef(_) | AdapterError::InvalidInput(_) => {
            let mut d = ErrorDisposition::terminal("Fix the desktop action arguments or node ref.");
            d.code = Some("INVALID_INPUT".to_string());
            d
        }
        AdapterError::Backend(_) => ErrorDisposition::retryable(
            "Transient host desktop accessibility backend error; retry once or inspect desktop state.",
        ),
        _ => ErrorDisposition::terminal("Host desktop adapter error."),
    };
    format_computer_error(err.to_string(), disp)
}

fn ensure_host_platform(platform: DesktopPlatform) -> Result<(), String> {
    let current = match std::env::consts::OS {
        "linux" => DesktopPlatform::Linux,
        "macos" => DesktopPlatform::MacOs,
        "windows" => DesktopPlatform::Windows,
        _ => {
            return Err(format_adapter_error(AdapterError::UnsupportedPlatform(
                format!("unsupported host OS `{}`", std::env::consts::OS),
            )));
        }
    };
    if platform != current {
        return Err(format_adapter_error(AdapterError::UnsupportedPlatform(
            format!(
                "requested host platform `{platform:?}` does not match running host `{current:?}`"
            ),
        )));
    }
    Ok(())
}

fn tree_options_from_req(req: &Value) -> TreeOptions {
    let mut opts = TreeOptions::default();
    if let Some(d) = req.get("max_depth").and_then(Value::as_u64) {
        opts.max_depth = d as usize;
    }
    if let Some(n) = req.get("max_nodes").and_then(Value::as_u64) {
        opts.max_nodes = n as usize;
    }
    if let Some(pid) = req.get("pid").and_then(Value::as_u64) {
        opts.pid = u32::try_from(pid).ok();
    }
    opts
}

fn window_to_json(w: DesktopWindow) -> Value {
    json!({
        "window_id": w.id.to_string(),
        "title": w.name,
        "app": w.app_id,
        "pid": w.pid,
        "focused": w.focused,
        "bounds": {
            "x": w.rect.x,
            "y": w.rect.y,
            "width": w.rect.width,
            "height": w.rect.height,
        },
    })
}

fn desktop_node_to_raw(node: DesktopNode) -> RawA11yNode {
    let bounds = node.bounds.map_or(
        Region {
            x: 0,
            y: 0,
            width: 0,
            height: 0,
        },
        |b| Region {
            x: b.x,
            y: b.y,
            width: b.width,
            height: b.height,
        },
    );
    let interactive = !node.actions.is_empty()
        || matches!(
            node.role.to_ascii_lowercase().as_str(),
            "button"
                | "push_button"
                | "link"
                | "text"
                | "text_field"
                | "entry"
                | "textbox"
                | "searchbox"
                | "checkbox"
                | "radio_button"
                | "menu_item"
                | "tab"
                | "window"
        );
    RawA11yNode {
        role: node.role,
        name: node.name,
        window_id: Some(node.qualified_ref),
        app: node.app_id,
        bounds,
        interactive,
        children: node.children.into_iter().map(desktop_node_to_raw).collect(),
    }
}

fn filter_raw_nodes(
    nodes: Vec<RawA11yNode>,
    app_filter: Option<&str>,
    window_filter: Option<&str>,
) -> Vec<RawA11yNode> {
    let app_q = app_filter
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase);
    let win_q = window_filter
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase);
    if app_q.is_none() && win_q.is_none() {
        return nodes;
    }
    nodes
        .into_iter()
        .filter(|n| {
            let app_ok = app_q.as_ref().is_none_or(|q| {
                n.app
                    .as_deref()
                    .is_some_and(|a| a.to_ascii_lowercase().contains(q))
                    || n.name.to_ascii_lowercase().contains(q)
            });
            let win_ok = win_q.as_ref().is_none_or(|q| {
                n.window_id
                    .as_deref()
                    .is_some_and(|w| w.to_ascii_lowercase().contains(q))
                    || n.name.to_ascii_lowercase().contains(q)
            });
            app_ok && win_ok
        })
        .collect()
}

pub(super) async fn list_windows_with_adapter(
    adapter: &dyn PlatformAdapter,
) -> Result<Vec<Value>, String> {
    let windows = adapter.list_windows().await.map_err(format_adapter_error)?;
    Ok(windows.into_iter().map(window_to_json).collect())
}

pub(super) async fn list_windows(platform: DesktopPlatform) -> Result<Vec<Value>, String> {
    ensure_host_platform(platform)?;
    list_windows_with_adapter(shared_adapter()).await
}

pub(super) async fn snapshot_tree_with_adapter(
    adapter: &dyn PlatformAdapter,
    req: &Value,
) -> Result<DesktopTree, String> {
    let opts = tree_options_from_req(req);
    adapter.snapshot(opts).await.map_err(format_adapter_error)
}

pub(super) async fn collect_nodes_with_adapter(
    adapter: &dyn PlatformAdapter,
    req: &Value,
) -> Result<Vec<RawA11yNode>, String> {
    let tree = snapshot_tree_with_adapter(adapter, req).await?;
    let app_filter = req.get("app").and_then(Value::as_str);
    let window_filter = req.get("window_id").and_then(Value::as_str);
    let raw = tree.roots.into_iter().map(desktop_node_to_raw).collect();
    Ok(filter_raw_nodes(raw, app_filter, window_filter))
}

pub(super) async fn collect_nodes(
    platform: DesktopPlatform,
    req: &Value,
) -> Result<Vec<RawA11yNode>, String> {
    ensure_host_platform(platform)?;
    collect_nodes_with_adapter(shared_adapter(), req).await
}

fn map_button(button: MouseButton) -> Button {
    match button {
        MouseButton::Left => Button::Left,
        MouseButton::Right => Button::Right,
        MouseButton::Middle => Button::Middle,
        MouseButton::Back => Button::Back,
        MouseButton::Forward => Button::Forward,
    }
}

fn resolve_target_node_ref(
    target: Option<&RefEntry>,
    raw_ref: Option<&str>,
) -> Result<NodeRef, String> {
    if let Some(entry) = target
        && let Some(ref wid) = entry.window_id
        && let Ok(parsed) = NodeRef::parse(wid)
    {
        return Ok(parsed);
    }
    if let Some(raw) = raw_ref
        && !raw.trim().is_empty()
    {
        return NodeRef::parse(raw).map_err(format_adapter_error);
    }
    if let Some(entry) = target {
        return NodeRef::parse(&entry.ref_id).map_err(format_adapter_error);
    }
    Err(format_adapter_error(AdapterError::InvalidInput(
        "native desktop action requires a valid `@<snapshot>:e<N>` node ref from tools.computer.snapshot()".into(),
    )))
}

pub(super) async fn execute_aux_with_adapter(
    adapter: &dyn PlatformAdapter,
    platform: DesktopPlatform,
    req: &Value,
) -> Result<Value, String> {
    let action = req
        .get("action")
        .or_else(|| req.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match action.as_str() {
        "clipboard_get" if platform == DesktopPlatform::MacOs => {
            let bytes = checked_output(run_cmd("pbpaste", &[], &[], 5_000).await?, "pbpaste")?;
            Ok(json!({
                "action": "clipboard_get",
                "backend": "pbpaste",
                "text": String::from_utf8_lossy(&bytes),
            }))
        }
        "clipboard_set" if platform == DesktopPlatform::MacOs => {
            let text = req
                .get("text")
                .and_then(Value::as_str)
                .ok_or("missing `text` for clipboard_set")?;
            checked_output(
                run_cmd_with_stdin("pbcopy", &[], &[], text.as_bytes(), 5_000).await?,
                "pbcopy",
            )?;
            Ok(json!({
                "action": "clipboard_set",
                "backend": "pbcopy",
                "bytes": text.len(),
                "stdin_piped": true,
            }))
        }
        "focus_window" => {
            let raw_target = req
                .get("ref")
                .or_else(|| req.get("ref_id"))
                .or_else(|| req.get("window_id"))
                .or_else(|| req.get("id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    "missing `window_id` or `ref` for computer.focus_window".to_string()
                })?;
            let node_ref = NodeRef::parse(raw_target).map_err(format_adapter_error)?;
            adapter
                .act(DesktopAction::Focus { target: node_ref })
                .await
                .map_err(format_adapter_error)?;
            Ok(json!({
                "action": "focus_window",
                "window_id": raw_target,
                "backend": adapter.capabilities().backend,
                "status": "focused",
            }))
        }
        other => Err(format_adapter_error(AdapterError::UnsupportedAction(
            format!(
                "action `{other}` is not supported by `{}`",
                adapter.capabilities().backend
            ),
        ))),
    }
}

pub(super) async fn execute_aux(platform: DesktopPlatform, req: &Value) -> Result<Value, String> {
    ensure_host_platform(platform)?;
    execute_aux_with_adapter(shared_adapter(), platform, req).await
}

pub(super) async fn execute_action_with_adapter(
    adapter: &dyn PlatformAdapter,
    platform: DesktopPlatform,
    action: &ComputerAction,
    target: Option<&RefEntry>,
) -> Result<Value, String> {
    let backend = adapter.capabilities().backend;
    match action {
        ComputerAction::Wait { duration_ms } => {
            tokio::time::sleep(Duration::from_millis(u64::from(*duration_ms).min(60_000))).await;
            Ok(json!({ "action": "wait", "duration_ms": duration_ms }))
        }
        ComputerAction::Screenshot | ComputerAction::Zoom { .. }
            if platform == DesktopPlatform::MacOs =>
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
            Ok(json!({
                "action": if matches!(action, ComputerAction::Zoom { .. }) { "zoom" } else { "screenshot" },
                "backend": "screencapture",
                "mime_type": "image/png",
                "width": width,
                "height": height,
                "bytes": png.len(),
                "image_base64": base64_encode(&png),
            }))
        }
        ComputerAction::Click {
            button,
            count,
            coordinate,
            ..
        } => {
            let node_ref = resolve_target_node_ref(target, None)?;
            let desktop_action = if *count >= 2 && *button == MouseButton::Left {
                DesktopAction::DoubleClick { target: node_ref }
            } else {
                DesktopAction::Click {
                    target: node_ref,
                    button: Some(map_button(*button)),
                }
            };
            adapter
                .act(desktop_action)
                .await
                .map_err(format_adapter_error)?;
            Ok(json!({
                "action": "click",
                "backend": backend,
                "coordinate": coordinate,
                "count": count,
            }))
        }
        ComputerAction::Type { text, .. } => {
            if let Ok(node_ref) = resolve_target_node_ref(target, None)
                && adapter
                    .act(DesktopAction::SetValue {
                        target: node_ref,
                        value: text.clone(),
                    })
                    .await
                    .is_ok()
            {
                return Ok(json!({
                    "action": "type",
                    "backend": backend,
                    "bytes": text.len(),
                    "mode": "set_value",
                }));
            }
            adapter
                .act(DesktopAction::TypeText { text: text.clone() })
                .await
                .map_err(format_adapter_error)?;
            Ok(json!({
                "action": "type",
                "backend": backend,
                "bytes": text.len(),
                "mode": "type_text",
            }))
        }
        ComputerAction::Key { key, modifiers } => {
            let chord = if modifiers.is_empty() {
                key.clone()
            } else {
                format!("{}+{key}", modifiers.join("+"))
            };
            adapter
                .act(DesktopAction::PressKey {
                    chord: chord.clone(),
                })
                .await
                .map_err(format_adapter_error)?;
            Ok(json!({
                "action": "key",
                "backend": backend,
                "key": chord,
            }))
        }
        ComputerAction::Scroll {
            direction, amount, ..
        } => {
            let ticks = i32::try_from(*amount).unwrap_or(3).max(1);
            let (dx, dy) = match direction {
                ScrollDirection::Up => (0, -ticks),
                ScrollDirection::Down => (0, ticks),
                ScrollDirection::Left => (-ticks, 0),
                ScrollDirection::Right => (ticks, 0),
            };
            let target_ref = resolve_target_node_ref(target, None).ok();
            adapter
                .act(DesktopAction::Scroll {
                    target: target_ref,
                    dx,
                    dy,
                })
                .await
                .map_err(format_adapter_error)?;
            Ok(json!({
                "action": "scroll",
                "backend": backend,
                "dx": dx,
                "dy": dy,
            }))
        }
        other => Err(format_adapter_error(AdapterError::UnsupportedAction(
            format!(
                "action `{other:?}` requires a supported native PlatformAdapter primitive on `{backend}`"
            ),
        ))),
    }
}

pub(super) async fn execute_action(
    platform: DesktopPlatform,
    action: &ComputerAction,
    target: Option<&RefEntry>,
) -> Result<Value, String> {
    ensure_host_platform(platform)?;
    execute_action_with_adapter(shared_adapter(), platform, action, target).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_workstation::DesktopRect;
    use agent_workstation::platform::{MockPlatformAdapter, RawNode};

    fn sample_mock_adapter() -> MockPlatformAdapter {
        MockPlatformAdapter::with_roots(vec![RawNode {
            handle: "win-1".into(),
            role: "window".into(),
            name: "Editor".into(),
            value: None,
            app_id: Some("code.editor".into()),
            pid: Some(4242),
            enabled: true,
            focused: true,
            bounds: Some(DesktopRect {
                x: 10,
                y: 20,
                width: 1200,
                height: 800,
            }),
            actions: vec!["focus".into()],
            children: vec![
                RawNode {
                    handle: "btn-save".into(),
                    role: "button".into(),
                    name: "Save".into(),
                    value: None,
                    app_id: Some("code.editor".into()),
                    pid: Some(4242),
                    enabled: true,
                    focused: false,
                    bounds: Some(DesktopRect {
                        x: 100,
                        y: 60,
                        width: 80,
                        height: 32,
                    }),
                    actions: vec!["press".into()],
                    children: Vec::new(),
                },
                RawNode {
                    handle: "input-search".into(),
                    role: "text_field".into(),
                    name: "Search".into(),
                    value: Some(String::new()),
                    app_id: Some("code.editor".into()),
                    pid: Some(4242),
                    enabled: true,
                    focused: false,
                    bounds: Some(DesktopRect {
                        x: 200,
                        y: 60,
                        width: 240,
                        height: 32,
                    }),
                    actions: vec!["set_value".into(), "focus".into()],
                    children: Vec::new(),
                },
            ],
        }])
    }

    #[tokio::test]
    async fn platform_adapter_snapshot_actions_windows_and_stale_ref_enforcement() {
        let adapter = sample_mock_adapter();

        // 1. Window enumeration via pure-Rust PlatformAdapter
        let windows = list_windows_with_adapter(&adapter).await.unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0]["title"], "Editor");
        assert_eq!(windows[0]["app"], "code.editor");

        // 2. Accessibility snapshot allocates @<snap>:eN qualified refs
        let nodes = collect_nodes_with_adapter(&adapter, &json!({"action": "snapshot"}))
            .await
            .unwrap();
        assert_eq!(nodes.len(), 1);
        let save_ref = nodes[0].children[0].window_id.clone().unwrap();
        let search_ref = nodes[0].children[1].window_id.clone().unwrap();
        assert!(save_ref.starts_with("@p1:e"), "{save_ref}");
        assert!(search_ref.starts_with("@p1:e"), "{search_ref}");

        let entry_for = |qref: &str| RefEntry {
            ref_id: qref.to_string(),
            snapshot_id: "p1".into(),
            role: "button".into(),
            name: "Save".into(),
            window_id: Some(qref.to_string()),
            bounds: Region {
                x: 100,
                y: 60,
                width: 80,
                height: 32,
            },
            available_actions: vec!["click".into()],
        };

        // 3. Click, Type (with injection-shaped text safely passed in-process, zero shell/argv), Key, Scroll
        let click_res = execute_action_with_adapter(
            &adapter,
            DesktopPlatform::Linux,
            &ComputerAction::Click {
                coordinate: Point { x: 140, y: 76 },
                button: MouseButton::Left,
                count: 1,
                modifiers: Vec::new(),
            },
            Some(&entry_for(&save_ref)),
        )
        .await
        .unwrap();
        assert_eq!(click_res["backend"], "mock-in-memory");

        let injection_text = "sensitive-text\n\"; $(exit 1); ' 世界";
        let type_res = execute_action_with_adapter(
            &adapter,
            DesktopPlatform::Linux,
            &ComputerAction::Type {
                text: injection_text.to_string(),
                delay_ms: None,
            },
            Some(&entry_for(&search_ref)),
        )
        .await
        .unwrap();
        assert_eq!(type_res["mode"], "set_value");

        let key_res = execute_action_with_adapter(
            &adapter,
            DesktopPlatform::Linux,
            &ComputerAction::Key {
                key: "s".into(),
                modifiers: vec!["ctrl".into()],
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(key_res["key"], "ctrl+s");

        let scroll_res = execute_action_with_adapter(
            &adapter,
            DesktopPlatform::Linux,
            &ComputerAction::Scroll {
                coordinate: Point { x: 140, y: 76 },
                direction: ScrollDirection::Down,
                amount: 3,
                modifiers: Vec::new(),
            },
            Some(&entry_for(&save_ref)),
        )
        .await
        .unwrap();
        assert_eq!(scroll_res["dy"], 3);

        // 4. Rotating snapshot invalidates old @p1:eN refs fail-closed with STALE_REF
        let _next_nodes = collect_nodes_with_adapter(&adapter, &json!({"action": "snapshot"}))
            .await
            .unwrap();
        let stale_err = execute_action_with_adapter(
            &adapter,
            DesktopPlatform::Linux,
            &ComputerAction::Click {
                coordinate: Point { x: 140, y: 76 },
                button: MouseButton::Left,
                count: 1,
                modifiers: Vec::new(),
            },
            Some(&entry_for(&save_ref)),
        )
        .await
        .unwrap_err();
        assert!(
            stale_err.contains("STALE_REF") || stale_err.contains("stale_ref"),
            "{stale_err}"
        );
    }
}
