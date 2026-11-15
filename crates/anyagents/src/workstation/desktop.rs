//! Session `tools.computer`: pixel operations on the agent's session desktop,
//! the same Wayland output its browser runs on.

use std::path::Path;
use std::time::Duration;

use agent_workstation::{Button, Frame, Input};
use anyagents_core::Error;
use serde_json::{Value, json};

use super::{Caller, ToolError};
use crate::computer::{
    ComputerAction, MouseButton, Point, Region, ScrollDirection, base64_encode,
    parse_computer_action,
};

/// Execute a session `tools.computer` request for `caller`.
pub async fn execute(data: &Path, caller: &Caller, req: Value) -> Result<Value, Error> {
    let mutating = crate::computer::computer_request_mutates(&req);
    run(data, caller, req)
        .await
        .map_err(|e| e.into_action_error(false, mutating))
}

fn button(b: MouseButton) -> Button {
    match b {
        MouseButton::Left => Button::Left,
        MouseButton::Middle => Button::Middle,
        MouseButton::Right => Button::Right,
        MouseButton::Back => Button::Back,
        MouseButton::Forward => Button::Forward,
    }
}

/// Translate browser-style key names (`ArrowUp`, `PageDown`, `Control+a`)
/// into the desktop keyboard's chord names; anything else passes through.
pub(crate) fn chord(raw: &str) -> String {
    raw.split('+')
        .map(|part| {
            let p = part.trim();
            match p.to_ascii_lowercase().as_str() {
                "arrowup" => "up".into(),
                "arrowdown" => "down".into(),
                "arrowleft" => "left".into(),
                "arrowright" => "right".into(),
                "pageup" | "prior" => "pageup".into(),
                "pagedown" | "next" => "pagedown".into(),
                "control" | "ctl" => "ctrl".into(),
                "option" => "alt".into(),
                "command" | "win" | "windows" => "super".into(),
                "spacebar" => "space".into(),
                "" if raw.trim() == "+" => "+".into(),
                _ => p.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("+")
}

fn at(p: Point) -> Input {
    Input::Move {
        x: f64::from(p.x),
        y: f64::from(p.y),
    }
}

/// Pointer `steps` with `modifiers` held around them.
fn with_modifiers(modifiers: &[String], steps: Vec<Input>) -> Vec<Input> {
    if modifiers.is_empty() {
        return steps;
    }
    let held = chord(&modifiers.join("+"));
    let mut all = Vec::with_capacity(steps.len() + 2);
    all.push(Input::KeyDown(held.clone()));
    all.extend(steps);
    all.push(Input::KeyUp(held));
    all
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

async fn run(data: &Path, caller: &Caller, req: Value) -> Result<Value, ToolError> {
    let payload = req
        .get("action_json")
        .or_else(|| req.get("payload"))
        .or_else(|| req.get("request"))
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
        super::sanitize_profile_name(req.get("profile").and_then(Value::as_str), &caller.profile);
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

    let live = super::session::acquire(data, &profile, &caller.principal, None).await?;
    let s = live
        .session
        .lease(&caller.principal)
        .map_err(ToolError::from)?;
    let out = match raw_action.as_str() {
        "snapshot" => {
            let snap = s.snapshot().await?;
            json!({
                "action": "snapshot",
                "platform": "session_desktop",
                "snapshot_id": snap.id,
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
            let (w, h) = live.session.size();
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
        "clipboard_get" => {
            json!({ "action": "clipboard_get", "text": s.clipboard_text().await? })
        }
        "clipboard_set" => {
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| ToolError::invalid("clipboard_set requires `text`"))?;
            s.set_clipboard_text(text).await?;
            json!({ "action": "clipboard_set", "chars": text.chars().count() })
        }
        _ => {
            let action = parse_computer_action(&payload).map_err(ToolError::invalid)?;
            pixel(&s, action).await?
        }
    };
    live.touch();
    Ok(out)
}

async fn pixel(
    s: &agent_workstation::Agent<'_>,
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
            let (x, y) = s.pointer_position()?.ok_or_else(|| {
                ToolError::unsupported("the pointer has not been placed on this desktop yet")
            })?;
            json!({ "action": "cursor_position", "x": x.round() as i64, "y": y.round() as i64 })
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
            let b = button(b);
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
            s.input(with_modifiers(&modifiers, steps)).await?;
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
                button: button(b),
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
            let b = button(b);
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
            let n = i32::try_from(amount).unwrap_or(i32::MAX);
            let (dx, dy) = match direction {
                ScrollDirection::Up => (0, -n),
                ScrollDirection::Down => (0, n),
                ScrollDirection::Left => (-n, 0),
                ScrollDirection::Right => (n, 0),
            };
            s.input(with_modifiers(
                &modifiers,
                vec![at(coordinate), Input::Scroll { dx, dy }],
            ))
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
            let chord = chord(&parts.join("+"));
            s.key(&chord).await?;
            json!({ "action": "key", "key": chord })
        }
        ComputerAction::HoldKey { key, duration_ms } => {
            let chord = chord(&key);
            s.hold_key(&chord, Duration::from_millis(u64::from(duration_ms)))
                .await?;
            json!({ "action": "hold_key", "key": chord, "duration_ms": duration_ms })
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

    #[test]
    fn browser_key_names_become_desktop_chords() {
        assert_eq!(chord("ArrowUp"), "up");
        assert_eq!(chord("Control+a"), "ctrl+a");
        assert_eq!(chord("Enter"), "Enter");
        assert_eq!(chord("PageDown"), "pagedown");
    }

    #[test]
    fn modifiers_are_held_around_pointer_steps() {
        let steps = vec![Input::Move { x: 1.0, y: 2.0 }];
        assert_eq!(with_modifiers(&[], steps.clone()), steps);
        let held = with_modifiers(&["Control".into(), "shift".into()], steps);
        assert_eq!(held.first(), Some(&Input::KeyDown("ctrl+shift".into())));
        assert_eq!(held.last(), Some(&Input::KeyUp("ctrl+shift".into())));
        assert_eq!(held.len(), 3);
    }

    #[tokio::test]
    async fn refs_on_the_session_desktop_point_to_the_browser() {
        let dir = tempfile::tempdir().unwrap();
        let caller = Caller {
            principal: "agent-a".into(),
            profile: "agent-a".into(),
        };
        let err = execute(
            dir.path(),
            &caller,
            json!({"action": "click", "ref": "@s1:e1"}),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("\"code\":\"UNSUPPORTED\""), "{err}");
        assert!(super::super::session::running(dir.path(), "agent-a").is_none());
    }
}
