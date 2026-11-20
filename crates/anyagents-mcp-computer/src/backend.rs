//! Cross-platform desktop backend built on `agent_workstation::PlatformAdapter`
//! plus OS screenshot/input helpers, with fail-closed `UNSUPPORTED` reporting on
//! headless containers without a display.

use std::path::Path;
use std::process::{Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_workstation::{
    Button, DesktopAction, DesktopTree, NodeRef, PlatformAdapter, TreeOptions, native_adapter,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use crate::config::ComputerServerConfig;
use crate::error::ComputerError;

const CMD_TIMEOUT_MS: u64 = 10_000;

/// Rectangular screen capture region in logical pixels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScreenRegion {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
}

/// Mouse button selection for `computer_click`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MouseButton {
    Left,
    Right,
    Middle,
}

impl MouseButton {
    pub fn parse(raw: Option<&str>) -> Result<Self, ComputerError> {
        let cleaned = raw.unwrap_or("left").trim().to_ascii_lowercase();
        match cleaned.as_str() {
            "" | "left" | "primary" | "1" => Ok(Self::Left),
            "right" | "secondary" | "3" => Ok(Self::Right),
            "middle" | "2" => Ok(Self::Middle),
            other => Err(ComputerError::invalid_input(format!(
                "unknown mouse button `{other}`; expected `left`, `right`, or `middle`"
            ))),
        }
    }

    pub const fn to_workstation(self) -> Button {
        match self {
            Self::Left => Button::Left,
            Self::Right => Button::Right,
            Self::Middle => Button::Middle,
        }
    }

    pub const fn xdotool_code(self) -> &'static str {
        match self {
            Self::Left => "1",
            Self::Middle => "2",
            Self::Right => "3",
        }
    }
}

/// Scroll direction for `computer_scroll`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrollDirection {
    Up,
    Down,
    Left,
    Right,
}

impl ScrollDirection {
    pub fn parse(raw: &str) -> Result<Self, ComputerError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "up" => Ok(Self::Up),
            "down" => Ok(Self::Down),
            "left" => Ok(Self::Left),
            "right" => Ok(Self::Right),
            other => Err(ComputerError::invalid_input(format!(
                "unknown scroll direction `{other}`; expected `up`, `down`, `left`, or `right`"
            ))),
        }
    }

    pub const fn xdotool_button(self) -> &'static str {
        match self {
            Self::Up => "4",
            Self::Down => "5",
            Self::Left => "6",
            Self::Right => "7",
        }
    }

    pub const fn delta(self, ticks: i32) -> (i32, i32) {
        match self {
            Self::Up => (0, -ticks),
            Self::Down => (0, ticks),
            Self::Left => (-ticks, 0),
            Self::Right => (ticks, 0),
        }
    }
}

/// Detected display availability on the host running `anyagents-mcp-computer`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DisplayAvailability {
    /// Linux host with an active X11 and/or Wayland display environment.
    LinuxDisplay {
        display: Option<String>,
        wayland_display: Option<String>,
    },
    /// macOS host using native Accessibility / CoreGraphics / `screencapture`.
    MacOs,
    /// Windows host using native UI Automation / Win32 input.
    Windows,
    /// Headless container or unsupported OS with no GUI display available.
    Headless { reason: String },
}

impl DisplayAvailability {
    /// Detect display availability from the current host OS and server config.
    pub fn detect(config: &ComputerServerConfig) -> Self {
        Self::detect_for(
            std::env::consts::OS,
            config.effective_display(),
            config.effective_wayland_display(),
        )
    }

    /// Pure, testable display detector independent of global process env.
    pub fn detect_for(os: &str, display: Option<&str>, wayland_display: Option<&str>) -> Self {
        match os {
            "linux" => {
                let d = display.map(str::trim).filter(|s| !s.is_empty());
                let w = wayland_display.map(str::trim).filter(|s| !s.is_empty());
                if d.is_none() && w.is_none() {
                    Self::Headless {
                        reason: "desktop GUI is unavailable in this headless environment \
                                 (neither DISPLAY nor WAYLAND_DISPLAY is set)"
                            .to_string(),
                    }
                } else {
                    Self::LinuxDisplay {
                        display: d.map(str::to_owned),
                        wayland_display: w.map(str::to_owned),
                    }
                }
            }
            "macos" => Self::MacOs,
            "windows" => Self::Windows,
            other => Self::Headless {
                reason: format!("desktop GUI automation is not supported on OS `{other}`"),
            },
        }
    }

    /// Fail fast with a typed `UNSUPPORTED` error when running headless.
    pub fn ensure_available(&self) -> Result<(), ComputerError> {
        if let Self::Headless { reason } = self {
            Err(ComputerError::unsupported(reason.clone()))
        } else {
            Ok(())
        }
    }

    fn env_pairs(&self) -> Vec<(&'static str, &str)> {
        let mut envs = Vec::new();
        if let Self::LinuxDisplay {
            display,
            wayland_display,
        } = self
        {
            if let Some(d) = display.as_deref() {
                envs.push(("DISPLAY", d));
            }
            if let Some(w) = wayland_display.as_deref() {
                envs.push(("WAYLAND_DISPLAY", w));
            }
        }
        envs
    }
}

/// Decoupled desktop automation backend combining `agent_workstation::PlatformAdapter`
/// with OS screenshot and X11/Wayland/macOS input primitives.
#[derive(Clone, Debug)]
pub struct ComputerBackend {
    display: DisplayAvailability,
    adapter: Arc<dyn PlatformAdapter>,
}

impl ComputerBackend {
    /// Construct a production backend for the current host OS and `config`.
    pub fn from_config(config: &ComputerServerConfig) -> Self {
        Self {
            display: DisplayAvailability::detect(config),
            adapter: Arc::from(native_adapter()),
        }
    }

    /// Construct a backend with an explicit `DisplayAvailability` and `PlatformAdapter`
    /// (used for deterministic unit testing).
    pub fn with_parts(display: DisplayAvailability, adapter: Arc<dyn PlatformAdapter>) -> Self {
        Self { display, adapter }
    }

    /// Return the detected display availability.
    pub fn display(&self) -> &DisplayAvailability {
        &self.display
    }

    /// Capture an accessibility snapshot tree via `PlatformAdapter`.
    pub async fn snapshot(&self, options: TreeOptions) -> Result<DesktopTree, ComputerError> {
        self.display.ensure_available()?;
        self.adapter.snapshot(options).await.map_err(Into::into)
    }

    /// Capture a PNG screenshot of the full screen or `region`.
    ///
    /// Returns `(png_bytes, backend_name, width, height)`.
    pub async fn screenshot(
        &self,
        region: Option<ScreenRegion>,
    ) -> Result<(Vec<u8>, &'static str, u32, u32), ComputerError> {
        self.display.ensure_available()?;
        if let Some(r) = region
            && (r.width == 0 || r.height == 0)
        {
            return Err(ComputerError::invalid_input(
                "screenshot `region` width and height must be positive",
            ));
        }
        let (png, backend) = match &self.display {
            DisplayAvailability::Headless { reason } => {
                return Err(ComputerError::unsupported(reason.clone()));
            }
            DisplayAvailability::MacOs => capture_macos_screenshot(region).await?,
            DisplayAvailability::LinuxDisplay {
                wayland_display, ..
            } => {
                capture_linux_screenshot(
                    &self.display.env_pairs(),
                    wayland_display.is_some(),
                    region,
                )
                .await?
            }
            DisplayAvailability::Windows => {
                return Err(ComputerError::unsupported(
                    "screenshot capture is not yet supported on Windows without a display capture helper",
                ));
            }
        };
        let (width, height) = parse_png_dimensions(&png)
            .unwrap_or_else(|| region.map_or((0, 0), |r| (r.width, r.height)));
        Ok((png, backend, width, height))
    }

    /// Click either a semantic accessibility ref (`@<snap>:eN`) or logical `(x, y)` coordinates.
    pub async fn click(
        &self,
        node_ref: Option<&str>,
        coord: Option<(i32, i32)>,
        button: MouseButton,
        count: u32,
        modifiers: &[String],
    ) -> Result<Value, ComputerError> {
        self.display.ensure_available()?;
        let count = count.clamp(1, 3);

        if let Some(raw_ref) = node_ref.map(str::trim).filter(|s| !s.is_empty()) {
            let parsed = NodeRef::parse(raw_ref)?;
            let action = if count >= 2 && button == MouseButton::Left {
                DesktopAction::DoubleClick { target: parsed }
            } else {
                DesktopAction::Click {
                    target: parsed,
                    button: Some(button.to_workstation()),
                }
            };
            self.adapter.act(action).await?;
            return Ok(json!({
                "action": "click",
                "backend": self.adapter.capabilities().backend,
                "ref": raw_ref,
                "button": button,
                "count": count,
            }));
        }

        let Some((x, y)) = coord else {
            return Err(ComputerError::invalid_input(
                "`computer_click` requires either `ref` (`@<snapshot>:eN`) or both `x` and `y` coordinates",
            ));
        };

        match &self.display {
            DisplayAvailability::LinuxDisplay { .. } => {
                let envs = self.display.env_pairs();
                let mut args: Vec<String> = vec![
                    "mousemove".into(),
                    "--sync".into(),
                    x.to_string(),
                    y.to_string(),
                ];
                for m in modifiers {
                    args.push("keydown".into());
                    args.push(m.clone());
                }
                args.extend([
                    "click".into(),
                    "--repeat".into(),
                    count.to_string(),
                    button.xdotool_code().into(),
                ]);
                for m in modifiers.iter().rev() {
                    args.push("keyup".into());
                    args.push(m.clone());
                }
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                run_checked_tool("xdotool", &refs, &envs, None).await?;
                Ok(json!({
                    "action": "click",
                    "backend": "xdotool",
                    "x": x,
                    "y": y,
                    "button": button,
                    "count": count,
                    "modifiers": modifiers,
                }))
            }
            _ => Err(ComputerError::unsupported(
                "coordinate-based click requires `xdotool` on Linux X11; use `computer_snapshot` + `ref` on this platform",
            )),
        }
    }

    /// Type text into a target ref, coordinate, or currently focused desktop control.
    pub async fn type_text(
        &self,
        text: &str,
        node_ref: Option<&str>,
        coord: Option<(i32, i32)>,
        delay_ms: Option<u32>,
    ) -> Result<Value, ComputerError> {
        self.display.ensure_available()?;
        if text.is_empty() {
            return Err(ComputerError::invalid_input(
                "`computer_type` requires non-empty `text`",
            ));
        }

        if let Some(raw_ref) = node_ref.map(str::trim).filter(|s| !s.is_empty()) {
            let parsed = NodeRef::parse(raw_ref)?;
            if self
                .adapter
                .act(DesktopAction::SetValue {
                    target: parsed.clone(),
                    value: text.to_string(),
                })
                .await
                .is_ok()
            {
                return Ok(json!({
                    "action": "type",
                    "backend": self.adapter.capabilities().backend,
                    "mode": "set_value",
                    "ref": raw_ref,
                    "chars": text.chars().count(),
                }));
            }
            self.adapter
                .act(DesktopAction::Focus { target: parsed })
                .await?;
        } else if let Some((x, y)) = coord {
            self.click(None, Some((x, y)), MouseButton::Left, 1, &[])
                .await?;
        }

        match self
            .adapter
            .act(DesktopAction::TypeText {
                text: text.to_string(),
            })
            .await
        {
            Ok(()) => {
                return Ok(json!({
                    "action": "type",
                    "backend": self.adapter.capabilities().backend,
                    "mode": "type_text",
                    "chars": text.chars().count(),
                }));
            }
            Err(err) if !matches!(self.display, DisplayAvailability::LinuxDisplay { .. }) => {
                return Err(err.into());
            }
            Err(_) => {}
        }

        let envs = self.display.env_pairs();
        if let DisplayAvailability::LinuxDisplay {
            wayland_display: Some(_),
            ..
        } = &self.display
            && run_checked_tool("wtype", &["-"], &envs, Some(text.as_bytes()))
                .await
                .is_ok()
        {
            return Ok(json!({
                "action": "type",
                "backend": "wtype",
                "chars": text.chars().count(),
            }));
        }

        let delay = delay_ms.unwrap_or(12).to_string();
        run_checked_tool(
            "xdotool",
            &["type", "--clearmodifiers", "--delay", &delay, "--file", "-"],
            &envs,
            Some(text.as_bytes()),
        )
        .await?;
        Ok(json!({
            "action": "type",
            "backend": "xdotool",
            "chars": text.chars().count(),
        }))
    }

    /// Press a key or key chord (e.g. `"Return"`, `"ctrl+c"`, `"alt+Tab"`).
    pub async fn press_key(&self, key: &str, modifiers: &[String]) -> Result<Value, ComputerError> {
        self.display.ensure_available()?;
        let key = key.trim();
        if key.is_empty() {
            return Err(ComputerError::invalid_input(
                "`computer_key` requires a non-empty `key`",
            ));
        }
        let chord = if modifiers.is_empty() {
            key.to_string()
        } else {
            format!("{}+{key}", modifiers.join("+"))
        };

        match self
            .adapter
            .act(DesktopAction::PressKey {
                chord: chord.clone(),
            })
            .await
        {
            Ok(()) => {
                return Ok(json!({
                    "action": "key",
                    "backend": self.adapter.capabilities().backend,
                    "key": chord,
                }));
            }
            Err(err) if !matches!(self.display, DisplayAvailability::LinuxDisplay { .. }) => {
                return Err(err.into());
            }
            Err(_) => {}
        }

        let envs = self.display.env_pairs();
        run_checked_tool(
            "xdotool",
            &["key", "--clearmodifiers", "--", &chord],
            &envs,
            None,
        )
        .await?;
        Ok(json!({
            "action": "key",
            "backend": "xdotool",
            "key": chord,
        }))
    }

    /// Scroll vertically or horizontally at a target node ref, coordinates, or current pointer.
    pub async fn scroll(
        &self,
        direction: ScrollDirection,
        amount: u32,
        node_ref: Option<&str>,
        coord: Option<(i32, i32)>,
    ) -> Result<Value, ComputerError> {
        self.display.ensure_available()?;
        let ticks = i32::try_from(amount.clamp(1, 100)).unwrap_or(3);
        let (dx, dy) = direction.delta(ticks);

        if let Some(raw_ref) = node_ref.map(str::trim).filter(|s| !s.is_empty()) {
            let target = NodeRef::parse(raw_ref)?;
            self.adapter
                .act(DesktopAction::Scroll {
                    target: Some(target),
                    dx,
                    dy,
                })
                .await?;
            return Ok(json!({
                "action": "scroll",
                "backend": self.adapter.capabilities().backend,
                "direction": direction,
                "amount": ticks,
                "ref": raw_ref,
            }));
        }

        if coord.is_none() {
            match self
                .adapter
                .act(DesktopAction::Scroll {
                    target: None,
                    dx,
                    dy,
                })
                .await
            {
                Ok(()) => {
                    return Ok(json!({
                        "action": "scroll",
                        "backend": self.adapter.capabilities().backend,
                        "direction": direction,
                        "amount": ticks,
                    }));
                }
                Err(err) if !matches!(self.display, DisplayAvailability::LinuxDisplay { .. }) => {
                    return Err(err.into());
                }
                Err(_) => {}
            }
        }

        match &self.display {
            DisplayAvailability::LinuxDisplay { .. } => {
                let envs = self.display.env_pairs();
                let mut args: Vec<String> = Vec::new();
                if let Some((x, y)) = coord {
                    args.extend([
                        "mousemove".into(),
                        "--sync".into(),
                        x.to_string(),
                        y.to_string(),
                    ]);
                }
                args.extend([
                    "click".into(),
                    "--repeat".into(),
                    ticks.to_string(),
                    direction.xdotool_button().into(),
                ]);
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                run_checked_tool("xdotool", &refs, &envs, None).await?;
                Ok(json!({
                    "action": "scroll",
                    "backend": "xdotool",
                    "direction": direction,
                    "amount": ticks,
                    "x": coord.map(|(x, _)| x),
                    "y": coord.map(|(_, y)| y),
                }))
            }
            _ => Err(ComputerError::unsupported(
                "coordinate-based scroll requires `xdotool` on Linux X11; use `ref` or omit coordinates on this platform",
            )),
        }
    }
}

/// Parse `(width, height)` from a valid PNG image's IHDR chunk.
pub fn parse_png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 24 || &bytes[0..8] != b"\x89PNG\r\n\x1a\n" || &bytes[12..16] != b"IHDR" {
        return None;
    }
    let w = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let h = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
    Some((w, h))
}

async fn capture_macos_screenshot(
    region: Option<ScreenRegion>,
) -> Result<(Vec<u8>, &'static str), ComputerError> {
    let mut args = vec!["-x".to_string(), "-t".into(), "png".into()];
    if let Some(r) = region {
        args.extend([
            "-R".into(),
            format!("{},{},{},{}", r.x, r.y, r.width, r.height),
        ]);
    }
    args.push("-".into());
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = run_checked_tool("screencapture", &refs, &[], None).await?;
    if out.stdout.is_empty() {
        return Err(ComputerError::backend(
            "screencapture produced empty output",
        ));
    }
    Ok((out.stdout, "screencapture"))
}

async fn capture_linux_screenshot(
    envs: &[(&str, &str)],
    prefer_wayland: bool,
    region: Option<ScreenRegion>,
) -> Result<(Vec<u8>, &'static str), ComputerError> {
    if prefer_wayland && let Some(bytes) = try_grim(envs, region).await {
        return Ok((bytes, "grim"));
    }

    if let Some(bytes) = try_imagemagick_import(envs, region).await {
        return Ok((bytes, "import"));
    }

    if !prefer_wayland && let Some(bytes) = try_grim(envs, region).await {
        return Ok((bytes, "grim"));
    }

    if let Some(bytes) = try_scrot(envs, region).await {
        return Ok((bytes, "scrot"));
    }

    Err(ComputerError::unsupported(
        "no screenshot backend (`grim`, `import`, or `scrot`) succeeded on the configured display",
    ))
}

async fn try_grim(envs: &[(&str, &str)], region: Option<ScreenRegion>) -> Option<Vec<u8>> {
    let mut args = Vec::new();
    let geom;
    if let Some(r) = region {
        geom = format!("{},{} {}x{}", r.x.max(0), r.y.max(0), r.width, r.height);
        args.push("-g");
        args.push(&geom);
    }
    args.push("-");
    let out = run_raw_cmd("grim", &args, envs, None).await.ok()?;
    (out.status.success() && !out.stdout.is_empty()).then_some(out.stdout)
}

async fn try_imagemagick_import(
    envs: &[(&str, &str)],
    region: Option<ScreenRegion>,
) -> Option<Vec<u8>> {
    let mut args = vec!["-window", "root"];
    let crop_spec;
    if let Some(r) = region {
        crop_spec = format!("{}x{}+{}+{}", r.width, r.height, r.x.max(0), r.y.max(0));
        args.push("-crop");
        args.push(&crop_spec);
        args.push("+repage");
    }
    args.push("png:-");
    let out = run_raw_cmd("import", &args, envs, None).await.ok()?;
    (out.status.success() && !out.stdout.is_empty()).then_some(out.stdout)
}

async fn try_scrot(envs: &[(&str, &str)], region: Option<ScreenRegion>) -> Option<Vec<u8>> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_path = std::env::temp_dir().join(format!(
        "anyagents-mcp-computer-{}-{stamp}.png",
        std::process::id()
    ));
    let tmp_str = tmp_path.to_string_lossy().to_string();
    let mut args = vec!["-o"];
    let area;
    if let Some(r) = region {
        area = format!("{},{},{},{}", r.x.max(0), r.y.max(0), r.width, r.height);
        args.push("-a");
        args.push(&area);
    }
    args.push(&tmp_str);
    let out = run_raw_cmd("scrot", &args, envs, None).await;
    let bytes = match out {
        Ok(o) if o.status.success() => tokio::fs::read(&tmp_path).await.ok(),
        _ => None,
    };
    let _ = tokio::fs::remove_file(&tmp_path).await;
    bytes.filter(|b| !b.is_empty())
}

async fn run_raw_cmd(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_bytes: Option<&[u8]>,
) -> Result<Output, std::io::Error> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if stdin_bytes.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn()?;
    if let Some(bytes) = stdin_bytes
        && let Some(mut stdin) = child.stdin.take()
    {
        stdin.write_all(bytes).await?;
        drop(stdin);
    }
    tokio::time::timeout(
        Duration::from_millis(CMD_TIMEOUT_MS),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "command timed out"))?
}

async fn run_checked_tool(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_bytes: Option<&[u8]>,
) -> Result<Output, ComputerError> {
    match run_raw_cmd(program, args, envs, stdin_bytes).await {
        Ok(out) if out.status.success() => Ok(out),
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            Err(ComputerError::backend(format!(
                "`{program}` failed ({}): {stderr}",
                out.status
            )))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(ComputerError::unsupported(
            format!("required desktop helper `{program}` is not installed on this host"),
        )),
        Err(err) => Err(ComputerError::backend(format!(
            "failed executing `{program}`: {err}"
        ))),
    }
}

/// Write PNG bytes to `dest`, creating parent directories when needed.
pub async fn save_png(dest: &Path, bytes: &[u8]) -> Result<(), ComputerError> {
    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            ComputerError::backend(format!(
                "failed creating screenshot directory `{}`: {e}",
                parent.display()
            ))
        })?;
    }
    tokio::fs::write(dest, bytes).await.map_err(|e| {
        ComputerError::backend(format!(
            "failed writing screenshot `{}`: {e}",
            dest.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_workstation::platform::RawNode;
    use agent_workstation::{DesktopRect, MockPlatformAdapter};

    fn sample_mock_adapter() -> Arc<MockPlatformAdapter> {
        Arc::new(MockPlatformAdapter::with_roots(vec![RawNode {
            handle: "win-1".into(),
            role: "window".into(),
            name: "Settings".into(),
            value: None,
            app_id: Some("settings".into()),
            pid: Some(42),
            enabled: true,
            focused: true,
            bounds: Some(DesktopRect {
                x: 0,
                y: 0,
                width: 1024,
                height: 768,
            }),
            actions: vec!["focus".into()],
            children: vec![
                RawNode {
                    handle: "input-search".into(),
                    role: "text_field".into(),
                    name: "Search".into(),
                    value: Some(String::new()),
                    app_id: Some("settings".into()),
                    pid: Some(42),
                    enabled: true,
                    focused: false,
                    bounds: Some(DesktopRect {
                        x: 20,
                        y: 40,
                        width: 240,
                        height: 32,
                    }),
                    actions: vec!["focus".into(), "set_value".into()],
                    children: Vec::new(),
                },
                RawNode {
                    handle: "btn-save".into(),
                    role: "button".into(),
                    name: "Save".into(),
                    value: None,
                    app_id: Some("settings".into()),
                    pid: Some(42),
                    enabled: true,
                    focused: false,
                    bounds: Some(DesktopRect {
                        x: 20,
                        y: 90,
                        width: 100,
                        height: 32,
                    }),
                    actions: vec!["press".into()],
                    children: Vec::new(),
                },
            ],
        }]))
    }

    #[test]
    fn detects_headless_linux_when_display_and_wayland_are_absent() {
        assert!(matches!(
            DisplayAvailability::detect_for("linux", None, None),
            DisplayAvailability::Headless { .. }
        ));
        assert!(matches!(
            DisplayAvailability::detect_for("linux", Some("   "), Some("")),
            DisplayAvailability::Headless { .. }
        ));
        assert_eq!(
            DisplayAvailability::detect_for("linux", Some(":99"), None),
            DisplayAvailability::LinuxDisplay {
                display: Some(":99".into()),
                wayland_display: None,
            }
        );
        assert_eq!(
            DisplayAvailability::detect_for("linux", None, Some("wayland-0")),
            DisplayAvailability::LinuxDisplay {
                display: None,
                wayland_display: Some("wayland-0".into()),
            }
        );
        assert_eq!(
            DisplayAvailability::detect_for("macos", None, None),
            DisplayAvailability::MacOs
        );
    }

    #[test]
    fn parses_png_ihdr_dimensions() {
        let mut header = Vec::new();
        header.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        header.extend_from_slice(&13u32.to_be_bytes());
        header.extend_from_slice(b"IHDR");
        header.extend_from_slice(&1440u32.to_be_bytes());
        header.extend_from_slice(&900u32.to_be_bytes());
        assert_eq!(parse_png_dimensions(&header), Some((1440, 900)));
        assert_eq!(parse_png_dimensions(b"not a png"), None);
    }

    #[tokio::test]
    async fn headless_backend_returns_typed_unsupported_for_all_actions() {
        let backend = ComputerBackend::with_parts(
            DisplayAvailability::detect_for("linux", None, None),
            sample_mock_adapter(),
        );
        assert_eq!(
            backend.screenshot(None).await.unwrap_err().code,
            "UNSUPPORTED"
        );
        assert_eq!(
            backend
                .click(None, Some((10, 20)), MouseButton::Left, 1, &[])
                .await
                .unwrap_err()
                .code,
            "UNSUPPORTED"
        );
        assert_eq!(
            backend
                .type_text("hello", None, None, None)
                .await
                .unwrap_err()
                .code,
            "UNSUPPORTED"
        );
        assert_eq!(
            backend.press_key("Return", &[]).await.unwrap_err().code,
            "UNSUPPORTED"
        );
        assert_eq!(
            backend
                .scroll(ScrollDirection::Down, 3, None, None)
                .await
                .unwrap_err()
                .code,
            "UNSUPPORTED"
        );
        assert_eq!(
            backend
                .snapshot(TreeOptions::default())
                .await
                .unwrap_err()
                .code,
            "UNSUPPORTED"
        );
    }

    #[tokio::test]
    async fn mock_platform_adapter_drives_snapshot_click_type_key_scroll_and_stale_ref() {
        let mock = sample_mock_adapter();
        let adapter: Arc<dyn PlatformAdapter> = mock.clone();
        let backend = ComputerBackend::with_parts(DisplayAvailability::MacOs, adapter);

        let tree = backend.snapshot(TreeOptions::default()).await.unwrap();
        assert_eq!(tree.snapshot_id, "p1");
        assert_eq!(tree.total_nodes, 3);

        let typed = backend
            .type_text("anyagents", Some("@p1:e2"), None, None)
            .await
            .unwrap();
        assert_eq!(typed["mode"], "set_value");

        let clicked = backend
            .click(Some("@p1:e3"), None, MouseButton::Left, 1, &[])
            .await
            .unwrap();
        assert_eq!(clicked["ref"], "@p1:e3");

        let pressed = backend.press_key("s", &["ctrl".to_string()]).await.unwrap();
        assert_eq!(pressed["key"], "ctrl+s");

        let scrolled = backend
            .scroll(ScrollDirection::Down, 4, Some("@p1:e1"), None)
            .await
            .unwrap();
        assert_eq!(scrolled["amount"], 4);

        // Rotate snapshot generation and verify old ref fails closed with STALE_REF.
        let tree2 = backend.snapshot(TreeOptions::default()).await.unwrap();
        assert_eq!(tree2.snapshot_id, "p2");
        let stale = backend
            .click(Some("@p1:e3"), None, MouseButton::Left, 1, &[])
            .await
            .unwrap_err();
        assert_eq!(stale.code, "STALE_REF");
    }
}
