//! Session `tools.computer`: pixel operations on the agent's session desktop,
//! the same Wayland output its browser runs on.

use std::path::Path;
use std::time::Duration;

use agent_workstation::{Button, Frame, Input};
use anyagents_core::Error;
use serde_json::{Value, json};

use super::ToolError;
use crate::computer::{
    ComputerAction, MouseButton, Point, Region, ScrollDirection, base64_encode,
    parse_computer_action,
};

/// Execute a session `tools.computer` request for the calling agent.
pub async fn execute(data: &Path, default_profile: &str, req: Value) -> Result<Value, Error> {
    run(data, default_profile, req)
        .await
        .map_err(|e| e.into_error(false))
}

fn button(b: MouseButton) -> Result<Button, ToolError> {
    match b {
        MouseButton::Left => Ok(Button::Left),
        MouseButton::Middle => Ok(Button::Middle),
        MouseButton::Right => Ok(Button::Right),
        // P6 pending: Button::Back / Forward.
        MouseButton::Back | MouseButton::Forward => Err(ToolError::unsupported(
            "back/forward mouse buttons are not available on the session desktop in this release",
        )),
    }
}

fn at(p: Point) -> Input {
    Input::Move {
        x: f64::from(p.x),
        y: f64::from(p.y),
    }
}

/// B4 pending: modifiers held across pointer input need Input::KeyDown / KeyUp.
fn no_modifiers(modifiers: &[String]) -> Result<(), ToolError> {
    if modifiers.is_empty() {
        Ok(())
    } else {
        Err(ToolError::unsupported(
            "holding modifier keys during pointer input is not available on the session desktop \
             in this release",
        ))
    }
}

fn png_json(action: &str, png: &[u8], width: u32, height: u32) -> Value {
    json!({
        "action": action,
        "backend": "session-desktop",
        "mime_type": "image/png",
        "width": width,
        "height": height,
        "bytes": png.len(),
        "image_base64": base64_encode(png),
    })
}

/// The part of `frame` inside `region`, clamped to the frame.
fn crop(frame: &Frame, region: Region) -> Result<Frame, ToolError> {
    let x0 = u32::try_from(region.x.max(0)).unwrap_or(0).min(frame.width);
    let y0 = u32::try_from(region.y.max(0))
        .unwrap_or(0)
        .min(frame.height);
    let w = region.width.min(frame.width - x0);
    let h = region.height.min(frame.height - y0);
    if w == 0 || h == 0 {
        return Err(ToolError::invalid("zoom region is outside the desktop"));
    }
    let mut rgba = Vec::with_capacity((w * h * 4) as usize);
    for row in y0..y0 + h {
        let start = ((row * frame.width + x0) * 4) as usize;
        rgba.extend_from_slice(&frame.rgba[start..start + (w * 4) as usize]);
    }
    Ok(Frame {
        width: w,
        height: h,
        rgba,
    })
}

async fn run(data: &Path, default_profile: &str, req: Value) -> Result<Value, ToolError> {
    let payload = req
        .get("action_json")
        .or_else(|| req.get("payload"))
        .cloned()
        .unwrap_or_else(|| req.clone());
    let raw_action = payload
        .get("action")
        .or_else(|| payload.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();
    let profile =
        super::sanitize_profile_name(req.get("profile").and_then(Value::as_str), default_profile);

    match raw_action.as_str() {
        // P4 pending: clipboard_get / clipboard_set.
        "clipboard_get" | "clipboard_set" => {
            return Err(ToolError::unsupported(
                "the session desktop clipboard is not available in this release",
            ));
        }
        // P5 pending: pointer_position.
        "cursor_position" => {
            return Err(ToolError::unsupported(
                "cursor_position is not available on the session desktop in this release",
            ));
        }
        // B4 pending: Input::KeyDown / KeyUp.
        "hold_key" => {
            return Err(ToolError::unsupported(
                "holding keys is not available on the session desktop in this release",
            ));
        }
        _ => {}
    }
    if payload
        .get("ref")
        .or_else(|| payload.get("ref_id"))
        .is_some()
    {
        return Err(ToolError::unsupported(
            "the session desktop's semantic refs belong to its browser: take browser({action: \
             'snapshot'}) and act with browser click/fill",
        ));
    }

    let live = super::session::acquire(data, &profile, default_profile, None).await?;
    let s = &live.session;
    let out = match raw_action.as_str() {
        "snapshot" => {
            let snap = s.snapshot().await?;
            let id = crate::computer::generate_snapshot_id();
            live.set_snapshot(Some(super::session::SnapshotMark {
                id: id.clone(),
                url: snap.url.clone(),
            }));
            json!({
                "action": "snapshot",
                "platform": "session_desktop",
                "snapshot_id": id,
                "url": snap.url,
                "snapshot": snap.text,
                "refs": snap.refs,
                "hint": "Refs belong to the browser: use browser click/fill with `@<snapshot_id>:eN`.",
            })
        }
        "list_windows" | "windows" => {
            let title = s
                .tabs()
                .await?
                .into_iter()
                .find(|t| t.active)
                .map(|t| t.title)
                .unwrap_or_default();
            let (w, h) = s.size();
            json!({
                "action": "list_windows",
                "count": 1,
                "windows": [{ "window_id": "browser", "title": title, "x": 0, "y": 0, "width": w, "height": h }],
            })
        }
        "focus_window" => {
            let wid = payload
                .get("window_id")
                .or_else(|| payload.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("browser");
            if wid != "browser" {
                return Err(ToolError {
                    code: "NOT_FOUND",
                    message: format!("window `{wid}` is not on the session desktop"),
                    hint: "The session desktop has one window, `browser`.",
                    retryable: false,
                });
            }
            json!({ "action": "focus_window", "window_id": wid, "status": "focused" })
        }
        _ => {
            let action = parse_computer_action(&payload).map_err(ToolError::invalid)?;
            pixel(s, action).await?
        }
    };
    live.touch();
    Ok(out)
}

async fn pixel(
    s: &agent_workstation::DesktopSession,
    action: ComputerAction,
) -> Result<Value, ToolError> {
    Ok(match action {
        ComputerAction::Screenshot => {
            let frame = s.screenshot().await?;
            let png = frame.to_png()?;
            png_json("screenshot", &png, frame.width, frame.height)
        }
        ComputerAction::Zoom { region } => {
            let frame = crop(&s.screenshot().await?, region)?;
            let png = frame.to_png()?;
            let mut v = png_json("zoom", &png, frame.width, frame.height);
            v["region"] = json!(region);
            v
        }
        ComputerAction::CursorPosition => {
            return Err(ToolError::unsupported(
                "cursor_position is not available on the session desktop in this release",
            ));
        }
        ComputerAction::MouseMove { coordinate } => {
            s.input(vec![at(coordinate)]).await?;
            json!({ "action": "move", "coordinate": coordinate })
        }
        ComputerAction::Click {
            coordinate,
            button: b,
            count,
            modifiers,
        } => {
            no_modifiers(&modifiers)?;
            let b = button(b)?;
            let mut steps = vec![at(coordinate)];
            for _ in 0..count.max(1) {
                steps.push(Input::Button {
                    button: b,
                    pressed: true,
                });
                steps.push(Input::Button {
                    button: b,
                    pressed: false,
                });
            }
            s.input(steps).await?;
            json!({ "action": "click", "coordinate": coordinate, "count": count.max(1) })
        }
        ComputerAction::MouseDown {
            coordinate,
            button: b,
        }
        | ComputerAction::MouseUp {
            coordinate,
            button: b,
        } => {
            let pressed = matches!(action, ComputerAction::MouseDown { .. });
            let mut steps: Vec<Input> = coordinate.map(at).into_iter().collect();
            steps.push(Input::Button {
                button: button(b)?,
                pressed,
            });
            s.input(steps).await?;
            json!({ "action": if pressed { "mouse_down" } else { "mouse_up" }, "coordinate": coordinate })
        }
        ComputerAction::Drag {
            start,
            end,
            button: b,
        } => {
            let b = button(b)?;
            let mut steps = vec![
                at(start),
                Input::Button {
                    button: b,
                    pressed: true,
                },
            ];
            // Intermediate moves so pages see a drag, not a jump.
            for i in 1..=10 {
                let t = f64::from(i) / 10.0;
                steps.push(Input::Move {
                    x: f64::from(start.x) + (f64::from(end.x - start.x)) * t,
                    y: f64::from(start.y) + (f64::from(end.y - start.y)) * t,
                });
                steps.push(Input::Wait(Duration::from_millis(10)));
            }
            steps.push(Input::Button {
                button: b,
                pressed: false,
            });
            s.input(steps).await?;
            json!({ "action": "drag", "start": start, "end": end })
        }
        ComputerAction::Scroll {
            coordinate,
            direction,
            amount,
            modifiers,
        } => {
            no_modifiers(&modifiers)?;
            let n = i32::try_from(amount).unwrap_or(i32::MAX);
            let (dx, dy) = match direction {
                ScrollDirection::Up => (0, -n),
                ScrollDirection::Down => (0, n),
                ScrollDirection::Left => (-n, 0),
                ScrollDirection::Right => (n, 0),
            };
            s.input(vec![at(coordinate), Input::Scroll { dx, dy }])
                .await?;
            json!({ "action": "scroll", "coordinate": coordinate, "dx": dx, "dy": dy })
        }
        ComputerAction::Type { text, .. } => {
            s.type_text(&text).await?;
            json!({ "action": "type", "chars": text.chars().count() })
        }
        ComputerAction::Key { key, modifiers } => {
            let mut parts = modifiers;
            parts.push(key);
            let chord = super::browser::chord(&parts.join("+"));
            s.key(&chord).await?;
            json!({ "action": "key", "key": chord })
        }
        ComputerAction::HoldKey { .. } => {
            return Err(ToolError::unsupported(
                "holding keys is not available on the session desktop in this release",
            ));
        }
        ComputerAction::Wait { duration_ms } => {
            tokio::time::sleep(Duration::from_millis(u64::from(duration_ms).min(60_000))).await;
            json!({ "action": "wait", "duration_ms": duration_ms })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zoom_crops_and_clamps_to_the_desktop() {
        let frame = Frame {
            width: 4,
            height: 3,
            rgba: (0..48).collect(),
        };
        let c = crop(
            &frame,
            Region {
                x: 2,
                y: 1,
                width: 10,
                height: 10,
            },
        )
        .unwrap();
        assert_eq!((c.width, c.height), (2, 2));
        assert_eq!(&c.rgba[..4], &[24, 25, 26, 27]);
        assert!(
            crop(
                &frame,
                Region {
                    x: 9,
                    y: 0,
                    width: 1,
                    height: 1
                }
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn pending_desktop_capabilities_are_typed_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        for req in [
            json!({"action": "clipboard_get"}),
            json!({"action": "cursor_position"}),
            json!({"action": "hold_key", "key": "w", "duration_ms": 100}),
            json!({"action": "click", "ref": "@s1:e1"}),
        ] {
            let err = execute(dir.path(), "agent-a", req.clone())
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("\"code\":\"UNSUPPORTED\""), "{req}: {err}");
        }
    }
}
