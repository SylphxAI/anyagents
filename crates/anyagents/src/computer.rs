use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

mod desktop;
mod mobile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesktopPlatform {
    Linux,
    MacOs,
    Windows,
    ChromeOs,
}

impl DesktopPlatform {
    pub fn detect(req: &Value) -> Result<Self, String> {
        let platform = req
            .get("platform")
            .or_else(|| req.get("os"))
            .and_then(Value::as_str);
        Self::from_environment(
            platform,
            std::env::consts::OS,
            std::env::var_os("SOMMELIER_VERSION").is_some()
                || std::env::var_os("CHROMEOS_RELEASE_NAME").is_some(),
        )
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

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub(crate) fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(B64_ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_ALPHABET[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let filtered: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !filtered.len().is_multiple_of(4) {
        return Err("invalid base64 length".to_string());
    }
    let mut out = Vec::with_capacity((filtered.len() / 4) * 3);
    let decode_val = |b: u8| -> Result<u32, String> {
        match b {
            b'A'..=b'Z' => Ok((b - b'A') as u32),
            b'a'..=b'z' => Ok((b - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((b - b'0' + 52) as u32),
            b'+' | b'-' => Ok(62),
            b'/' | b'_' => Ok(63),
            other => Err(format!("invalid base64 character `{}`", other as char)),
        }
    };
    for chunk in filtered.chunks(4) {
        let v0 = decode_val(chunk[0])?;
        let v1 = decode_val(chunk[1])?;
        let v2 = if chunk[2] == b'=' {
            0
        } else {
            decode_val(chunk[2])?
        };
        let v3 = if chunk[3] == b'=' {
            0
        } else {
            decode_val(chunk[3])?
        };
        let triple = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
        out.push(((triple >> 16) & 0xff) as u8);
        if chunk[2] != b'=' {
            out.push(((triple >> 8) & 0xff) as u8);
        }
        if chunk[3] != b'=' {
            out.push((triple & 0xff) as u8);
        }
    }
    Ok(out)
}

fn post_json_bytes(
    url: &str,
    bearer_token: &str,
    payload: &Value,
    timeout_secs: u64,
) -> Result<(u16, Vec<u8>), String> {
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let body_bytes = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
    let resp = http
        .post(url)
        .header("Authorization", &format!("Bearer {bearer_token}"))
        .header("Content-Type", "application/json")
        .send(body_bytes.as_slice())
        .map_err(|e| format!("HTTP request failed: {e}"))?;
    let status = resp.status().as_u16();
    let mut bytes = Vec::new();
    resp.into_body()
        .into_reader()
        .take(32 * 1024 * 1024)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("failed reading HTTP response bytes: {e}"))?;
    Ok((status, bytes))
}

/// 2D screen coordinate in logical pixels (`(0, 0)` is top-left).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

/// Rectangular screen region in logical pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Region {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
}

impl Region {
    pub fn center(self) -> Point {
        Point {
            x: self.x + (self.width as i32) / 2,
            y: self.y + (self.height as i32) / 2,
        }
    }
}

/// Structured error retry disposition inspired by `vercel-labs/agent-desktop` so agents
/// never blind-retry a settled absence or stale accessibility reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDisposition {
    /// `"settled_absence" | "retryable_failure" | "stale_ref" | "terminal_failure"`
    pub kind: String,
    /// `"safe" | "never"`
    pub retry: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl ErrorDisposition {
    pub fn stale_ref(hint: impl Into<String>) -> Self {
        Self {
            kind: "stale_ref".to_string(),
            retry: "never".to_string(),
            code: Some("STALE_REF".to_string()),
            hint: Some(hint.into()),
        }
    }

    pub fn settled_absence(hint: impl Into<String>) -> Self {
        Self {
            kind: "settled_absence".to_string(),
            retry: "never".to_string(),
            code: Some("SETTLED_ABSENCE".to_string()),
            hint: Some(hint.into()),
        }
    }

    pub fn retryable(hint: impl Into<String>) -> Self {
        Self {
            kind: "retryable_failure".to_string(),
            retry: "safe".to_string(),
            code: Some("RETRYABLE_FAILURE".to_string()),
            hint: Some(hint.into()),
        }
    }

    pub fn terminal(hint: impl Into<String>) -> Self {
        Self {
            kind: "terminal_failure".to_string(),
            retry: "never".to_string(),
            code: Some("TERMINAL_FAILURE".to_string()),
            hint: Some(hint.into()),
        }
    }
}

/// Format a structured computer error string carrying JSON `disposition` metadata.
pub fn format_computer_error(message: impl Into<String>, disposition: ErrorDisposition) -> String {
    let msg = message.into();
    let payload = json!({
        "error": msg,
        "code": disposition.code,
        "disposition": {
            "kind": disposition.kind,
            "retry": disposition.retry,
            "hint": disposition.hint,
        }
    });
    format!("error: {msg} | {payload}")
}

/// Classify a raw computer error string into a structured `ErrorDisposition`.
pub fn classify_computer_error(err: &str) -> ErrorDisposition {
    let lower = err.to_ascii_lowercase();
    if lower.contains("stale_ref")
        || lower.contains("stale ref")
        || lower.contains("bare ref")
        || lower.contains("snapshot_id")
    {
        ErrorDisposition::stale_ref(
            "Call tools.computer.snapshot() to obtain fresh @<snapshot_id>:e<N> refs before acting.",
        )
    } else if lower.contains("not found")
        || lower.contains("no such window")
        || lower.contains("settled_absence")
        || lower.contains("no matching window")
    {
        ErrorDisposition::settled_absence(
            "Target window or element is absent; inspect state via tools.computer.list_windows() or tools.computer.snapshot() instead of blind retrying.",
        )
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporarily unavailable")
        || lower.contains("429")
        || lower.contains("503")
    {
        ErrorDisposition::retryable("Transient timeout or busy display server; safe to retry once.")
    } else {
        ErrorDisposition::terminal("Unrecoverable computer-use input or environment error.")
    }
}

/// Semantic Accessibility Element in a `tools.computer.snapshot()` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotElement {
    /// Snapshot-qualified reference (`@<snapshot_id>:e<N>`, e.g. `"@s8f3k2p9:e1"`).
    pub ref_id: String,
    pub role: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    pub bounds: Region,
    pub available_actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<SnapshotElement>,
}

/// Raw accessibility tree node prior to Web-Wrapper Skipping and ref assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawA11yNode {
    pub role: String,
    pub name: String,
    pub window_id: Option<String>,
    pub app: Option<String>,
    pub bounds: Region,
    pub interactive: bool,
    pub children: Vec<RawA11yNode>,
}

/// Stored reference entry inside the bounded `RefMap`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefEntry {
    pub ref_id: String,
    pub snapshot_id: String,
    pub role: String,
    pub name: String,
    pub window_id: Option<String>,
    pub bounds: Region,
    pub available_actions: Vec<String>,
}

/// Bounded snapshot `RefMap` validating `@<snapshot_id>:e<N>` references across calls.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotRefMap {
    pub active_snapshot_id: Option<String>,
    pub refs: BTreeMap<String, RefEntry>,
    #[serde(default)]
    pub history_snapshot_ids: Vec<String>,
}

static GLOBAL_REF_MAP: OnceLock<Mutex<BTreeMap<PathBuf, SnapshotRefMap>>> = OnceLock::new();

fn global_ref_map() -> &'static Mutex<BTreeMap<PathBuf, SnapshotRefMap>> {
    GLOBAL_REF_MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn resolve_agent_dir(config_or_dir: &Path) -> &Path {
    if config_or_dir.is_file()
        || config_or_dir.file_name().and_then(|s| s.to_str()) == Some("config")
    {
        config_or_dir.parent().unwrap_or(config_or_dir)
    } else {
        config_or_dir
    }
}

fn load_ref_map(config_dir: &Path) -> SnapshotRefMap {
    let dir = resolve_agent_dir(config_dir);
    let path = dir.join("computer-refmap.json");
    if let Ok(raw) = std::fs::read_to_string(&path)
        && let Ok(parsed) = serde_json::from_str::<SnapshotRefMap>(&raw)
    {
        return parsed;
    }
    global_ref_map()
        .lock()
        .ok()
        .and_then(|g| g.get(dir).cloned())
        .unwrap_or_default()
}

fn save_ref_map(config_dir: &Path, map: &SnapshotRefMap) {
    let dir = resolve_agent_dir(config_dir);
    let mut merged = map.clone();
    let prev = load_ref_map(dir);
    for id in prev.history_snapshot_ids {
        if !merged.history_snapshot_ids.contains(&id) && merged.history_snapshot_ids.len() < 16 {
            merged.history_snapshot_ids.push(id);
        }
    }
    if let Ok(mut g) = global_ref_map().lock() {
        g.insert(dir.to_path_buf(), merged.clone());
    }
    let path = dir.join("computer-refmap.json");
    if let Ok(raw) = serde_json::to_string_pretty(&merged) {
        let _ = std::fs::write(path, raw);
    }
}

fn filter_a11y_subtree_by_ref(nodes: Vec<RawA11yNode>, target: &RefEntry) -> Vec<RawA11yNode> {
    fn find_matching(nodes: &[RawA11yNode], target: &RefEntry) -> Option<RawA11yNode> {
        for n in nodes {
            if n.role == target.role
                && n.name == target.name
                && n.window_id == target.window_id
                && n.bounds == target.bounds
            {
                return Some(n.clone());
            }
            if let Some(found) = find_matching(&n.children, target) {
                return Some(found);
            }
        }
        None
    }
    if let Some(sub) = find_matching(&nodes, target) {
        vec![sub]
    } else {
        nodes
    }
}

/// Generate an 8-character snapshot identifier (`s` + 7 lowercase base36 chars, e.g. `s8f3k2p9`).
pub fn generate_snapshot_id() -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x5f3759df);
    let mut state = nanos ^ ((std::process::id() as u64).rotate_left(17));
    let mut out = String::with_capacity(8);
    out.push('s');
    for _ in 0..7 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let idx = (state as usize) % ALPHABET.len();
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// Returns true if an accessibility node is an empty anonymous wrapper (`group`, `generic`,
/// `section`, `div`, `panel`, `filler`, `unknown`, `scroll_pane`, `layered_pane`) that should
/// be skipped so its children are hoisted directly (`Web Wrapper Skipping`).
pub fn is_anonymous_web_wrapper(role: &str, name: &str, interactive: bool) -> bool {
    if interactive || !name.trim().is_empty() {
        return false;
    }
    matches!(
        role.trim().to_ascii_lowercase().as_str(),
        "group"
            | "generic"
            | "section"
            | "div"
            | "panel"
            | "filler"
            | "unknown"
            | "scroll_pane"
            | "scroll pane"
            | "layered_pane"
            | "layered pane"
            | "viewport"
            | "document_frame"
    )
}

/// Prune empty anonymous wrapper nodes and assign `@<snapshot_id>:e<N>` refs in top-down pre-order.
pub fn build_snapshot_tree(
    snapshot_id: &str,
    raw_nodes: Vec<RawA11yNode>,
    skeleton: bool,
    interactive_only: bool,
) -> (Vec<SnapshotElement>, SnapshotRefMap) {
    fn prune_nodes(nodes: Vec<RawA11yNode>, interactive_only: bool) -> Vec<RawA11yNode> {
        let mut out = Vec::new();
        for mut node in nodes {
            let pruned_children = prune_nodes(node.children, interactive_only);
            if is_anonymous_web_wrapper(&node.role, &node.name, node.interactive) {
                out.extend(pruned_children);
                continue;
            }
            if interactive_only && !node.interactive && pruned_children.is_empty() {
                continue;
            }
            node.children = pruned_children;
            out.push(node);
        }
        out
    }

    fn assign_refs(
        snapshot_id: &str,
        nodes: Vec<RawA11yNode>,
        skeleton: bool,
        counter: &mut usize,
        refs: &mut BTreeMap<String, RefEntry>,
    ) -> Vec<SnapshotElement> {
        let mut out = Vec::new();
        for node in nodes {
            *counter += 1;
            let ref_id = format!("@{snapshot_id}:e{}", *counter);
            let actions = default_actions_for_role(&node.role, node.interactive);
            refs.insert(
                ref_id.clone(),
                RefEntry {
                    ref_id: ref_id.clone(),
                    snapshot_id: snapshot_id.to_string(),
                    role: node.role.clone(),
                    name: node.name.clone(),
                    window_id: node.window_id.clone(),
                    bounds: node.bounds,
                    available_actions: actions.clone(),
                },
            );
            let children = assign_refs(snapshot_id, node.children, skeleton, counter, refs);
            out.push(SnapshotElement {
                ref_id,
                role: node.role,
                name: node.name,
                window_id: node.window_id,
                app: node.app,
                bounds: node.bounds,
                available_actions: actions,
                children: if skeleton { Vec::new() } else { children },
            });
        }
        out
    }

    let pruned = prune_nodes(raw_nodes, interactive_only);
    let mut counter = 0usize;
    let mut refs = BTreeMap::new();
    let tree = assign_refs(snapshot_id, pruned, skeleton, &mut counter, &mut refs);
    let map = SnapshotRefMap {
        active_snapshot_id: Some(snapshot_id.to_string()),
        refs,
        history_snapshot_ids: vec![snapshot_id.to_string()],
    };
    (tree, map)
}

fn default_actions_for_role(role: &str, interactive: bool) -> Vec<String> {
    let lower = role.trim().to_ascii_lowercase();
    match lower.as_str() {
        "window" | "frame" | "dialog" | "application" => {
            vec!["focus".into(), "click".into(), "type".into(), "key".into()]
        }
        "text" | "entry" | "textbox" | "searchbox" | "password_text" | "terminal" => {
            vec!["click".into(), "type".into(), "key".into()]
        }
        "button" | "push_button" | "link" | "menu_item" | "checkbox" | "radio_button" | "tab" => {
            vec!["click".into()]
        }
        _ if interactive => vec!["click".into(), "type".into()],
        _ => vec!["click".into()],
    }
}

/// Resolve and validate a snapshot-qualified reference (`@<snapshot_id>:e<N>`).
/// Rejects bare refs (`@e1`) or outdated `snapshot_id`s with `STALE_REF`.
pub fn resolve_snapshot_ref(
    map: &SnapshotRefMap,
    raw_ref: &str,
) -> Result<RefEntry, ErrorDisposition> {
    let trimmed = raw_ref.trim();
    let Some(without_at) = trimmed.strip_prefix('@') else {
        return Err(ErrorDisposition::stale_ref(format!(
            "Invalid ref `{trimmed}`: refs must be snapshot-qualified (`@<snapshot_id>:e<N>`, e.g. `@s8f3k2p9:e1`)."
        )));
    };
    let Some((snap_id, elem_part)) = without_at.split_once(':') else {
        return Err(ErrorDisposition::stale_ref(format!(
            "STALE_REF: bare ref `{trimmed}` without `@<snapshot_id>:` prefix is rejected. Call tools.computer.snapshot() and pass `@<snapshot_id>:e<N>`."
        )));
    };
    if snap_id.is_empty() || !elem_part.starts_with('e') {
        return Err(ErrorDisposition::stale_ref(format!(
            "STALE_REF: malformed ref `{trimmed}` (expected `@<snapshot_id>:e<N>`)."
        )));
    }
    let Some(ref active_id) = map.active_snapshot_id else {
        return Err(ErrorDisposition::stale_ref(
            "STALE_REF: no active desktop snapshot exists. Call tools.computer.snapshot() first.",
        ));
    };
    if snap_id != active_id {
        return Err(ErrorDisposition::stale_ref(format!(
            "STALE_REF: ref `{trimmed}` belongs to outdated snapshot `{snap_id}` (active snapshot is `{active_id}`). Call tools.computer.snapshot() to refresh."
        )));
    }
    map.refs.get(trimmed).cloned().ok_or_else(|| {
        ErrorDisposition::stale_ref(format!(
            "STALE_REF: element `{trimmed}` not found in active snapshot `{active_id}`."
        ))
    })
}

/// Mouse button identifier across Anthropic/OpenAI computer-use schemas.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MouseButton {
    #[default]
    Left,
    Right,
    Middle,
    Back,
    Forward,
}

impl MouseButton {
    pub fn xdotool_code(self) -> &'static str {
        match self {
            Self::Left => "1",
            Self::Middle => "2",
            Self::Right => "3",
            Self::Back => "8",
            Self::Forward => "9",
        }
    }
}

/// Scroll direction on 2D surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrollDirection {
    Up,
    Down,
    Left,
    Right,
}

impl ScrollDirection {
    pub fn xdotool_button(self) -> &'static str {
        match self {
            Self::Up => "4",
            Self::Down => "5",
            Self::Left => "6",
            Self::Right => "7",
        }
    }
}

/// Provider-neutral computer-use action model supporting both Anthropic
/// (`computer_20250124` / `computer_20251124`) and OpenAI CUA (`computer_use_preview`)
/// tool schemas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ComputerAction {
    /// Capture current display as PNG image bytes.
    Screenshot,
    /// Query current cursor coordinates.
    CursorPosition,
    /// Move cursor to target coordinates without clicking.
    MouseMove { coordinate: Point },
    /// Click at coordinate (single, double, triple, right, or middle).
    Click {
        coordinate: Point,
        #[serde(default)]
        button: MouseButton,
        #[serde(default = "default_click_count")]
        count: u8,
        #[serde(default)]
        modifiers: Vec<String>,
    },
    /// Press mouse button down at optional coordinate.
    MouseDown {
        #[serde(default)]
        coordinate: Option<Point>,
        #[serde(default)]
        button: MouseButton,
    },
    /// Release mouse button at optional coordinate.
    MouseUp {
        #[serde(default)]
        coordinate: Option<Point>,
        #[serde(default)]
        button: MouseButton,
    },
    /// Drag cursor from `start` to `end` while holding button.
    Drag {
        start: Point,
        end: Point,
        #[serde(default)]
        button: MouseButton,
    },
    /// Scroll at coordinate in a direction by `amount` ticks.
    Scroll {
        coordinate: Point,
        direction: ScrollDirection,
        #[serde(default = "default_scroll_amount")]
        amount: u32,
        #[serde(default)]
        modifiers: Vec<String>,
    },
    /// Type literal UTF-8 text.
    Type {
        text: String,
        #[serde(default)]
        delay_ms: Option<u32>,
    },
    /// Press a key or chord (e.g. `"Return"`, `"ctrl+l"`, `"super+r"`).
    Key {
        key: String,
        #[serde(default)]
        modifiers: Vec<String>,
    },
    /// Hold a key down for `duration_ms` milliseconds.
    HoldKey { key: String, duration_ms: u32 },
    /// Pause execution for `duration_ms` milliseconds before capturing state.
    Wait { duration_ms: u32 },
    /// Inspect a zoomed rectangular crop of the screen at full resolution.
    Zoom { region: Region },
}

const fn default_click_count() -> u8 {
    1
}

const fn default_scroll_amount() -> u32 {
    3
}

impl ComputerAction {
    /// Returns true if the action mutates display or input state.
    #[allow(dead_code)]
    pub fn is_mutating(&self) -> bool {
        !matches!(
            self,
            Self::Screenshot | Self::CursorPosition | Self::Wait { .. } | Self::Zoom { .. }
        )
    }

    /// Normalize Anthropic `computer_20250124` / `computer_20251124` tool input JSON
    /// into a `ComputerAction`.
    pub fn from_anthropic(input: &Value) -> Result<Self, String> {
        let action = input.get("action").and_then(Value::as_str).ok_or_else(|| {
            "missing `action` string in Anthropic computer tool input".to_string()
        })?;

        let parse_coord = |field: &str| -> Result<Point, String> {
            let arr = input
                .get(field)
                .and_then(Value::as_array)
                .ok_or_else(|| format!("missing `{field}: [x, y]` for action `{action}`"))?;
            if arr.len() != 2 {
                return Err(format!("`{field}` must have 2 elements `[x, y]`"));
            }
            let x = arr[0]
                .as_i64()
                .ok_or_else(|| format!("`{field}[0]` must be an integer"))?
                as i32;
            let y = arr[1]
                .as_i64()
                .ok_or_else(|| format!("`{field}[1]` must be an integer"))?
                as i32;
            Ok(Point { x, y })
        };

        let modifiers = input
            .get("text")
            .and_then(Value::as_str)
            .filter(|_| {
                matches!(
                    action,
                    "left_click"
                        | "right_click"
                        | "middle_click"
                        | "double_click"
                        | "triple_click"
                        | "scroll"
                )
            })
            .map(|s| {
                s.split('+')
                    .map(str::trim)
                    .filter(|p| !p.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        match action {
            "screenshot" => Ok(Self::Screenshot),
            "cursor_position" => Ok(Self::CursorPosition),
            "mouse_move" => Ok(Self::MouseMove {
                coordinate: parse_coord("coordinate")?,
            }),
            "left_click" => Ok(Self::Click {
                coordinate: parse_coord("coordinate")?,
                button: MouseButton::Left,
                count: 1,
                modifiers,
            }),
            "right_click" => Ok(Self::Click {
                coordinate: parse_coord("coordinate")?,
                button: MouseButton::Right,
                count: 1,
                modifiers,
            }),
            "middle_click" => Ok(Self::Click {
                coordinate: parse_coord("coordinate")?,
                button: MouseButton::Middle,
                count: 1,
                modifiers,
            }),
            "double_click" => Ok(Self::Click {
                coordinate: parse_coord("coordinate")?,
                button: MouseButton::Left,
                count: 2,
                modifiers,
            }),
            "triple_click" => Ok(Self::Click {
                coordinate: parse_coord("coordinate")?,
                button: MouseButton::Left,
                count: 3,
                modifiers,
            }),
            "left_mouse_down" => Ok(Self::MouseDown {
                coordinate: parse_coord("coordinate").ok(),
                button: MouseButton::Left,
            }),
            "left_mouse_up" => Ok(Self::MouseUp {
                coordinate: parse_coord("coordinate").ok(),
                button: MouseButton::Left,
            }),
            "left_click_drag" => Ok(Self::Drag {
                start: parse_coord("start_coordinate")?,
                end: parse_coord("coordinate")?,
                button: MouseButton::Left,
            }),
            "scroll" => {
                let coordinate = parse_coord("coordinate")?;
                let dir_str = input
                    .get("scroll_direction")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `scroll_direction` for `scroll`".to_string())?;
                let direction = match dir_str {
                    "up" => ScrollDirection::Up,
                    "down" => ScrollDirection::Down,
                    "left" => ScrollDirection::Left,
                    "right" => ScrollDirection::Right,
                    other => return Err(format!("invalid `scroll_direction`: `{other}`")),
                };
                let amount = input
                    .get("scroll_amount")
                    .and_then(Value::as_u64)
                    .unwrap_or(3) as u32;
                Ok(Self::Scroll {
                    coordinate,
                    direction,
                    amount,
                    modifiers,
                })
            }
            "type" => {
                let text = input
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `text` for `type`".to_string())?
                    .to_owned();
                Ok(Self::Type {
                    text,
                    delay_ms: None,
                })
            }
            "key" => {
                let key = input
                    .get("text")
                    .or_else(|| input.get("key"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `text` or `key` for `key`".to_string())?
                    .to_owned();
                Ok(Self::Key {
                    key,
                    modifiers: Vec::new(),
                })
            }
            "hold_key" => {
                let key = input
                    .get("text")
                    .or_else(|| input.get("key"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `text` for `hold_key`".to_string())?
                    .to_owned();
                let duration_secs = input
                    .get("duration")
                    .and_then(Value::as_f64)
                    .unwrap_or(1.0)
                    .clamp(0.05, 30.0);
                Ok(Self::HoldKey {
                    key,
                    duration_ms: (duration_secs * 1000.0).round() as u32,
                })
            }
            "wait" => {
                let duration_secs = input
                    .get("duration")
                    .and_then(Value::as_f64)
                    .unwrap_or(1.0)
                    .clamp(0.05, 60.0);
                Ok(Self::Wait {
                    duration_ms: (duration_secs * 1000.0).round() as u32,
                })
            }
            "zoom" => {
                let arr = input
                    .get("region")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "missing `region: [x1, y1, x2, y2]` for `zoom`".to_string())?;
                if arr.len() != 4 {
                    return Err("`region` must have 4 elements `[x1, y1, x2, y2]`".to_string());
                }
                let x1 = arr[0].as_i64().unwrap_or(0) as i32;
                let y1 = arr[1].as_i64().unwrap_or(0) as i32;
                let x2 = arr[2].as_i64().unwrap_or(0) as i32;
                let y2 = arr[3].as_i64().unwrap_or(0) as i32;
                if x2 <= x1 || y2 <= y1 {
                    return Err("`region` requires `x2 > x1` and `y2 > y1`".to_string());
                }
                Ok(Self::Zoom {
                    region: Region {
                        x: x1,
                        y: y1,
                        width: (x2 - x1) as u32,
                        height: (y2 - y1) as u32,
                    },
                })
            }
            other => Err(format!("unsupported Anthropic computer action `{other}`")),
        }
    }

    /// Normalize OpenAI CUA (`computer_call` action) JSON into a `ComputerAction`.
    pub fn from_openai(input: &Value) -> Result<Self, String> {
        let kind = input
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `type` string in OpenAI computer action".to_string())?;

        let point = |obj: &Value| -> Result<Point, String> {
            let x = obj
                .get("x")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("missing integer `x` for `{kind}`"))?
                as i32;
            let y = obj
                .get("y")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("missing integer `y` for `{kind}`"))?
                as i32;
            Ok(Point { x, y })
        };

        let parse_button = |b: Option<&str>| -> MouseButton {
            match b.unwrap_or("left") {
                "right" => MouseButton::Right,
                "middle" | "wheel" => MouseButton::Middle,
                "back" => MouseButton::Back,
                "forward" => MouseButton::Forward,
                _ => MouseButton::Left,
            }
        };

        match kind {
            "screenshot" => Ok(Self::Screenshot),
            "move" => Ok(Self::MouseMove {
                coordinate: point(input)?,
            }),
            "click" => Ok(Self::Click {
                coordinate: point(input)?,
                button: parse_button(input.get("button").and_then(Value::as_str)),
                count: 1,
                modifiers: Vec::new(),
            }),
            "double_click" => Ok(Self::Click {
                coordinate: point(input)?,
                button: MouseButton::Left,
                count: 2,
                modifiers: Vec::new(),
            }),
            "drag" => {
                let path = input
                    .get("path")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "missing `path` array for OpenAI `drag`".to_string())?;
                if path.len() < 2 {
                    return Err("`path` must contain at least 2 points".to_string());
                }
                let start = point(&path[0])?;
                let end = point(path.last().expect("len >= 2"))?;
                Ok(Self::Drag {
                    start,
                    end,
                    button: MouseButton::Left,
                })
            }
            "scroll" => {
                let coordinate = point(input)?;
                let scroll_x = input.get("scroll_x").and_then(Value::as_i64).unwrap_or(0);
                let scroll_y = input.get("scroll_y").and_then(Value::as_i64).unwrap_or(0);
                let (direction, delta) = if scroll_y.abs() >= scroll_x.abs() {
                    if scroll_y < 0 {
                        (ScrollDirection::Up, scroll_y.unsigned_abs() as u32)
                    } else {
                        (ScrollDirection::Down, scroll_y.unsigned_abs() as u32)
                    }
                } else if scroll_x < 0 {
                    (ScrollDirection::Left, scroll_x.unsigned_abs() as u32)
                } else {
                    (ScrollDirection::Right, scroll_x.unsigned_abs() as u32)
                };
                let amount = (delta / 100).max(1);
                Ok(Self::Scroll {
                    coordinate,
                    direction,
                    amount,
                    modifiers: Vec::new(),
                })
            }
            "type" => {
                let text = input
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `text` for OpenAI `type`".to_string())?
                    .to_owned();
                Ok(Self::Type {
                    text,
                    delay_ms: None,
                })
            }
            "keypress" => {
                let keys = input
                    .get("keys")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "missing `keys` array for OpenAI `keypress`".to_string())?;
                let parts: Vec<String> = keys
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect();
                if parts.is_empty() {
                    return Err("`keys` array in `keypress` must not be empty".to_string());
                }
                Ok(Self::Key {
                    key: parts.join("+"),
                    modifiers: Vec::new(),
                })
            }
            "wait" => {
                let duration_ms = input
                    .get("duration_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(1000)
                    .clamp(50, 60_000) as u32;
                Ok(Self::Wait { duration_ms })
            }
            other => Err(format!("unsupported OpenAI computer action `{other}`")),
        }
    }
}

/// Parse an action payload that may be formatted in neutral `ComputerAction`,
/// Anthropic `computer_20250124`, OpenAI CUA format, or flat SDK arguments
/// (`{ action: "click", x: 100, y: 200, ... }`).
pub fn parse_computer_action(input: &Value) -> Result<ComputerAction, String> {
    if let Ok(neutral) = serde_json::from_value::<ComputerAction>(input.clone()) {
        return Ok(neutral);
    }
    if input.get("type").is_some() && input.get("action").is_none() {
        return ComputerAction::from_openai(input);
    }
    if let Ok(anth) = ComputerAction::from_anthropic(input) {
        return Ok(anth);
    }
    parse_flat_computer_action(input)
}

fn parse_flat_computer_action(input: &Value) -> Result<ComputerAction, String> {
    let action = input
        .get("action")
        .or_else(|| input.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();

    let get_i32 = |key: &str| -> Option<i32> {
        input
            .get(key)
            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f.round() as i64)))
            .map(|n| n as i32)
    };
    let get_u32 = |key: &str| -> Option<u32> {
        input
            .get(key)
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f.round() as u64)))
            .map(|n| n as u32)
    };
    let parse_coord_pair = |x_key: &str, y_key: &str, arr_key: &str| -> Result<Point, String> {
        if let (Some(x), Some(y)) = (get_i32(x_key), get_i32(y_key)) {
            return Ok(Point { x, y });
        }
        if let Some(obj) = input.get(arr_key).and_then(Value::as_object)
            && let (Some(x), Some(y)) = (
                obj.get("x").and_then(Value::as_i64),
                obj.get("y").and_then(Value::as_i64),
            )
        {
            return Ok(Point {
                x: x as i32,
                y: y as i32,
            });
        }
        if let Some(arr) = input.get(arr_key).and_then(Value::as_array)
            && arr.len() == 2
            && let (Some(x), Some(y)) = (arr[0].as_i64(), arr[1].as_i64())
        {
            return Ok(Point {
                x: x as i32,
                y: y as i32,
            });
        }
        Err(format!(
            "missing coordinates (`{x_key}`, `{y_key}` or `{arr_key}`) for computer action `{action}`"
        ))
    };

    let parse_button = || -> MouseButton {
        match input
            .get("button")
            .and_then(Value::as_str)
            .unwrap_or("left")
            .to_ascii_lowercase()
            .as_str()
        {
            "right" => MouseButton::Right,
            "middle" | "wheel" => MouseButton::Middle,
            "back" => MouseButton::Back,
            "forward" => MouseButton::Forward,
            _ => MouseButton::Left,
        }
    };

    let modifiers: Vec<String> = input
        .get("modifiers")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    match action.as_str() {
        "screenshot" => Ok(ComputerAction::Screenshot),
        "cursor_position" | "cursor" => Ok(ComputerAction::CursorPosition),
        "move" | "mouse_move" => Ok(ComputerAction::MouseMove {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
        }),
        "click" | "left_click" => Ok(ComputerAction::Click {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
            button: parse_button(),
            count: get_u32("count").unwrap_or(1).clamp(1, 3) as u8,
            modifiers,
        }),
        "right_click" => Ok(ComputerAction::Click {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
            button: MouseButton::Right,
            count: 1,
            modifiers,
        }),
        "middle_click" => Ok(ComputerAction::Click {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
            button: MouseButton::Middle,
            count: 1,
            modifiers,
        }),
        "double_click" => Ok(ComputerAction::Click {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
            button: MouseButton::Left,
            count: 2,
            modifiers,
        }),
        "triple_click" => Ok(ComputerAction::Click {
            coordinate: parse_coord_pair("x", "y", "coordinate")?,
            button: MouseButton::Left,
            count: 3,
            modifiers,
        }),
        "mouse_down" | "left_mouse_down" => Ok(ComputerAction::MouseDown {
            coordinate: parse_coord_pair("x", "y", "coordinate").ok(),
            button: parse_button(),
        }),
        "mouse_up" | "left_mouse_up" => Ok(ComputerAction::MouseUp {
            coordinate: parse_coord_pair("x", "y", "coordinate").ok(),
            button: parse_button(),
        }),
        "drag" | "left_click_drag" => {
            let start = parse_coord_pair("from_x", "from_y", "start")
                .or_else(|_| parse_coord_pair("start_x", "start_y", "start_coordinate"))?;
            let end = parse_coord_pair("to_x", "to_y", "end")
                .or_else(|_| parse_coord_pair("x", "y", "coordinate"))?;
            Ok(ComputerAction::Drag {
                start,
                end,
                button: parse_button(),
            })
        }
        "scroll" => {
            let coordinate =
                parse_coord_pair("x", "y", "coordinate").unwrap_or(Point { x: 0, y: 0 });
            let dir_str = input
                .get("direction")
                .or_else(|| input.get("scroll_direction"))
                .and_then(Value::as_str)
                .unwrap_or("down")
                .to_ascii_lowercase();
            let direction = match dir_str.as_str() {
                "up" => ScrollDirection::Up,
                "down" => ScrollDirection::Down,
                "left" => ScrollDirection::Left,
                "right" => ScrollDirection::Right,
                other => return Err(format!("invalid scroll direction `{other}`")),
            };
            let amount = get_u32("amount")
                .or_else(|| get_u32("scroll_amount"))
                .unwrap_or(3)
                .clamp(1, 100);
            Ok(ComputerAction::Scroll {
                coordinate,
                direction,
                amount,
                modifiers,
            })
        }
        "type" => {
            let text = input
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `text` for computer.type".to_string())?
                .to_string();
            Ok(ComputerAction::Type {
                text,
                delay_ms: get_u32("delay_ms"),
            })
        }
        "key" | "keypress" => {
            let key = input
                .get("key")
                .or_else(|| input.get("text"))
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `key` for computer.key".to_string())?
                .to_string();
            Ok(ComputerAction::Key { key, modifiers })
        }
        "hold_key" => {
            let key = input
                .get("key")
                .or_else(|| input.get("text"))
                .and_then(Value::as_str)
                .ok_or_else(|| "missing `key` for computer.hold_key".to_string())?
                .to_string();
            let duration_ms = get_u32("duration_ms")
                .or_else(|| {
                    input
                        .get("duration")
                        .and_then(Value::as_f64)
                        .map(|s| (s * 1000.0).round() as u32)
                })
                .unwrap_or(1000)
                .clamp(50, 30_000);
            Ok(ComputerAction::HoldKey { key, duration_ms })
        }
        "wait" => {
            let duration_ms = get_u32("duration_ms")
                .or_else(|| {
                    input
                        .get("duration")
                        .and_then(Value::as_f64)
                        .map(|s| (s * 1000.0).round() as u32)
                })
                .unwrap_or(1000)
                .clamp(10, 60_000);
            Ok(ComputerAction::Wait { duration_ms })
        }
        "zoom" => {
            if let (Some(x), Some(y), Some(width), Some(height)) = (
                get_i32("x"),
                get_i32("y"),
                get_u32("width"),
                get_u32("height"),
            ) {
                if width == 0 || height == 0 {
                    return Err("zoom `width` and `height` must be > 0".to_string());
                }
                return Ok(ComputerAction::Zoom {
                    region: Region {
                        x,
                        y,
                        width,
                        height,
                    },
                });
            }
            ComputerAction::from_anthropic(input)
        }
        other => Err(format!("unsupported computer action `{other}`")),
    }
}

/// Extract width and height from a PNG byte buffer's IHDR chunk.
pub fn parse_png_dimensions(png: &[u8]) -> Option<(u32, u32)> {
    const PNG_SIG: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    if png.len() < 24 || &png[0..8] != PNG_SIG || &png[12..16] != b"IHDR" {
        return None;
    }
    let width = u32::from_be_bytes([png[16], png[17], png[18], png[19]]);
    let height = u32::from_be_bytes([png[20], png[21], png[22], png[23]]);
    Some((width, height))
}

async fn run_cmd(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    timeout_ms: u64,
) -> Result<std::process::Output, String> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    tokio::time::timeout(Duration::from_millis(timeout_ms), cmd.output())
        .await
        .map_err(|_| format!("`{program}` timed out after {timeout_ms}ms"))?
        .map_err(|e| format!("failed to spawn `{program}`: {e}"))
}

/// Pipe `stdin_bytes` via `Stdio::piped()` so sensitive clipboard content never appears in `ps aux` argv.
async fn run_cmd_with_stdin(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_bytes: &[u8],
    timeout_ms: u64,
) -> Result<std::process::Output, String> {
    use tokio::io::AsyncWriteExt as _;
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .kill_on_drop(true)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn `{program}`: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("child stdin unavailable")?;
    let write = async {
        stdin.write_all(stdin_bytes).await?;
        stdin.shutdown().await
    };
    // Drain stdout/stderr concurrently with stdin: native scripts can emit large trees/images.
    let operation = async {
        let (written, output) = tokio::join!(write, child.wait_with_output());
        written?;
        output
    };
    tokio::time::timeout(Duration::from_millis(timeout_ms), operation)
        .await
        .map_err(|_| format!("`{program}` timed out after {timeout_ms}ms"))?
        .map_err(|e| format!("failed waiting for `{program}`: {e}"))
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

/// Execute a `tools.computer` request against a cloud sandbox (when `session_id` or
/// `provider == "cloud"` / `SYLPHX_SANDBOX_URL` is requested) or local X11/Wayland desktop,
/// supporting both **Semantic Accessibility Snapshot Mode (`@<snapshot_id>:e<N>`)** and
/// **Visual Pixel Mode** with structured error retry dispositions.
pub async fn execute_computer_request(
    data_dir: Option<&Path>,
    config_dir: &Path,
    req: Value,
) -> Result<Value, String> {
    match execute_computer_request_inner(data_dir, config_dir, req).await {
        Ok(val) => Ok(val),
        Err(err) if err.contains("\"disposition\"") => Err(err),
        Err(err) => {
            let disp = classify_computer_error(&err);
            Err(format_computer_error(err, disp))
        }
    }
}

async fn execute_computer_request_inner(
    data_dir: Option<&Path>,
    config_dir: &Path,
    req: Value,
) -> Result<Value, String> {
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

    // Forward every cloud action (including snapshots/clipboard) before touching the local host.
    let session_id = req
        .get("session_id")
        .or_else(|| req.get("sandbox_id"))
        .and_then(Value::as_str);
    if session_id.is_some()
        || req
            .get("provider")
            .and_then(Value::as_str)
            .is_some_and(|p| p.eq_ignore_ascii_case("cloud"))
    {
        let base_url = std::env::var("SYLPHX_SANDBOX_URL")
            .unwrap_or_else(|_| resolve_sylphx_gateway_base_url());
        let api_key = resolve_sylphx_api_key(data_dir, config_dir).ok_or(
            "cloud computer sandbox requires SYLPHX_API_KEY or configured Sylphx gateway key",
        )?;
        let endpoint = format!(
            "{}/v1/sandboxes/{}/computer",
            base_url.trim_end_matches('/'),
            session_id.unwrap_or("default")
        );
        // Preserve the cloud pixel-action protocol; semantic/ref requests stay intact.
        let cloud_payload = if payload
            .get("ref")
            .or_else(|| payload.get("ref_id"))
            .is_none()
        {
            parse_computer_action(&payload)
                .ok()
                .map(|action| json!({"action": action, "display": display}))
        } else {
            None
        }
        .unwrap_or(payload);
        let (status, bytes) = tokio::task::spawn_blocking(move || {
            post_json_bytes(&endpoint, &api_key, &cloud_payload, 30)
        })
        .await
        .map_err(|e| e.to_string())??;
        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid cloud computer JSON: {e}"))?;
        return if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(format!("cloud computer sandbox returned {status}: {body}"))
        };
    }
    let platform = DesktopPlatform::detect(
        if payload
            .get("platform")
            .or_else(|| payload.get("os"))
            .is_some()
        {
            &payload
        } else {
            &req
        },
    )?;
    if platform == DesktopPlatform::ChromeOs {
        if raw_action == "open_url" {
            let url = payload
                .get("url")
                .and_then(Value::as_str)
                .ok_or("missing `url` for computer.open_url")?;
            let out = run_cmd("garcon-url-handler", &[url], &envs, 10_000).await?;
            desktop::checked_output(out, "garcon-url-handler")?;
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
            return execute_device_request(config_dir, device_req).await;
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
            let mut raw_nodes = match platform {
                DesktopPlatform::MacOs | DesktopPlatform::Windows => {
                    desktop::collect_nodes(platform, &payload).await?
                }
                _ => collect_local_a11y_nodes(&envs, app_filter, window_filter).await,
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
            let windows = match platform {
                DesktopPlatform::MacOs | DesktopPlatform::Windows => {
                    desktop::list_windows(platform).await?
                }
                _ => list_local_windows(&envs).await?,
            };
            return Ok(json!({
                "action": "list_windows",
                "count": windows.len(),
                "windows": windows,
            }));
        }
        "focus_window" => {
            if matches!(platform, DesktopPlatform::MacOs | DesktopPlatform::Windows) {
                return desktop::execute_aux(platform, &payload).await;
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
                return desktop::execute_aux(platform, &payload).await;
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
                return desktop::execute_aux(platform, &payload).await;
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
        return desktop::execute_action(platform, &action, resolved_ref_entry.as_ref()).await;
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

fn normalize_computer_lexical_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else if !out.is_absolute() {
                    out.push("..");
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

fn resolve_work_path(work_dir: &Path, raw: &str) -> Result<PathBuf, String> {
    if raw.contains('\0') {
        return Err("path must not contain null bytes".to_string());
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("path must not be empty".to_string());
    }
    let p = Path::new(trimmed);
    let norm_work = normalize_computer_lexical_path(work_dir);
    let candidate = if p.is_absolute() {
        normalize_computer_lexical_path(p)
    } else {
        normalize_computer_lexical_path(&norm_work.join(p))
    };
    if !candidate.starts_with(&norm_work) {
        return Err(format!(
            "path `{}` escapes agent workdir `{}`",
            candidate.display(),
            norm_work.display()
        ));
    }
    Ok(candidate)
}

pub fn resolve_sylphx_gateway_base_url() -> String {
    let raw = std::env::var("SYLPHX_AI_BASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("SYLPHX_GATEWAY_URL")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| "https://ai.sylphx.com".to_string());
    let trimmed = raw.trim().trim_end_matches('/');
    trimmed.strip_suffix("/v1").unwrap_or(trimmed).to_string()
}

pub fn resolve_sylphx_api_key(data_dir: Option<&Path>, config_dir: &Path) -> Option<String> {
    for env_key in ["SYLPHX_API_KEY", "SYLPHX_GATEWAY_KEY"] {
        if let Ok(val) = std::env::var(env_key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    let agent_dir = resolve_agent_dir(config_dir);
    let mut candidates = vec![
        agent_dir.join(".config/sylphx/key"),
        agent_dir.join(".config/opencode/sylphx.key"),
        config_dir.join(".config/sylphx/key"),
        config_dir.join(".config/opencode/sylphx.key"),
    ];
    if let Some(dd) = data_dir {
        candidates.push(dd.join("profiles/sylphx/default/.config/sylphx/key"));
        candidates.push(dd.join("profiles/sylphx/default/.config/opencode/sylphx.key"));
    }
    for path in candidates {
        if let Ok(raw) = std::fs::read_to_string(&path) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

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
    fn parses_anthropic_computer_actions() {
        let click = ComputerAction::from_anthropic(&json!({
            "action": "left_click",
            "coordinate": [420, 310],
            "text": "shift+ctrl"
        }))
        .expect("valid anthropic left_click");
        assert_eq!(
            click,
            ComputerAction::Click {
                coordinate: Point { x: 420, y: 310 },
                button: MouseButton::Left,
                count: 1,
                modifiers: vec!["shift".to_string(), "ctrl".to_string()],
            }
        );

        let zoom = ComputerAction::from_anthropic(&json!({
            "action": "zoom",
            "region": [10, 20, 210, 120]
        }))
        .expect("valid anthropic zoom");
        assert_eq!(
            zoom,
            ComputerAction::Zoom {
                region: Region {
                    x: 10,
                    y: 20,
                    width: 200,
                    height: 100
                }
            }
        );
        assert!(!zoom.is_mutating());
        assert!(click.is_mutating());
    }

    #[test]
    fn parses_openai_computer_actions() {
        let drag = ComputerAction::from_openai(&json!({
            "type": "drag",
            "path": [{"x": 10, "y": 15}, {"x": 50, "y": 55}, {"x": 100, "y": 120}]
        }))
        .expect("valid openai drag");
        assert_eq!(
            drag,
            ComputerAction::Drag {
                start: Point { x: 10, y: 15 },
                end: Point { x: 100, y: 120 },
                button: MouseButton::Left,
            }
        );

        let scroll = ComputerAction::from_openai(&json!({
            "type": "scroll",
            "x": 500,
            "y": 400,
            "scroll_x": 0,
            "scroll_y": -300
        }))
        .expect("valid openai scroll");
        assert_eq!(
            scroll,
            ComputerAction::Scroll {
                coordinate: Point { x: 500, y: 400 },
                direction: ScrollDirection::Up,
                amount: 3,
                modifiers: Vec::new(),
            }
        );
    }

    #[test]
    fn parses_flat_sdk_computer_actions() {
        let click = parse_computer_action(&json!({
            "action": "click",
            "x": 128,
            "y": 256,
            "button": "right",
            "count": 2
        }))
        .expect("valid flat click");
        assert_eq!(
            click,
            ComputerAction::Click {
                coordinate: Point { x: 128, y: 256 },
                button: MouseButton::Right,
                count: 2,
                modifiers: Vec::new(),
            }
        );
    }

    #[test]
    fn snapshot_tree_skips_anonymous_web_wrappers_and_validates_snapshot_qualified_refs() {
        let raw = vec![RawA11yNode {
            role: "window".to_string(),
            name: "Browser".to_string(),
            window_id: Some("0x0100001".to_string()),
            app: Some("Chromium".to_string()),
            bounds: Region {
                x: 0,
                y: 0,
                width: 1280,
                height: 800,
            },
            interactive: true,
            children: vec![RawA11yNode {
                role: "group".to_string(),
                name: "".to_string(),
                window_id: Some("0x0100001".to_string()),
                app: Some("Chromium".to_string()),
                bounds: Region {
                    x: 10,
                    y: 10,
                    width: 500,
                    height: 200,
                },
                interactive: false,
                children: vec![RawA11yNode {
                    role: "button".to_string(),
                    name: "Submit Order".to_string(),
                    window_id: Some("0x0100001".to_string()),
                    app: Some("Chromium".to_string()),
                    bounds: Region {
                        x: 100,
                        y: 50,
                        width: 120,
                        height: 40,
                    },
                    interactive: true,
                    children: Vec::new(),
                }],
            }],
        }];

        let (tree, ref_map) = build_snapshot_tree("s8f3k2p9", raw, false, true);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].ref_id, "@s8f3k2p9:e1");
        assert_eq!(
            tree[0].children.len(),
            1,
            "anonymous group wrapper must be skipped"
        );
        assert_eq!(tree[0].children[0].ref_id, "@s8f3k2p9:e2");
        assert_eq!(tree[0].children[0].name, "Submit Order");

        // Valid snapshot-qualified ref resolves cleanly:
        let btn = resolve_snapshot_ref(&ref_map, "@s8f3k2p9:e2").expect("valid ref");
        assert_eq!(btn.bounds.center(), Point { x: 160, y: 70 });

        // Bare ref (`@e2`) is rejected with STALE_REF disposition:
        let bare_err = resolve_snapshot_ref(&ref_map, "@e2").unwrap_err();
        assert_eq!(bare_err.kind, "stale_ref");
        assert_eq!(bare_err.retry, "never");
        assert_eq!(bare_err.code.as_deref(), Some("STALE_REF"));

        // Outdated snapshot_id (`@s0000000:e2`) is rejected with STALE_REF disposition:
        let old_err = resolve_snapshot_ref(&ref_map, "@s0000000:e2").unwrap_err();
        assert_eq!(old_err.kind, "stale_ref");
        assert_eq!(old_err.retry, "never");
        assert_eq!(old_err.code.as_deref(), Some("STALE_REF"));
    }

    #[test]
    fn parses_png_dimensions_and_mobile_keycodes() {
        let mut fake_png = Vec::new();
        fake_png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        fake_png.extend_from_slice(&13u32.to_be_bytes());
        fake_png.extend_from_slice(b"IHDR");
        fake_png.extend_from_slice(&1920u32.to_be_bytes());
        fake_png.extend_from_slice(&1080u32.to_be_bytes());
        assert_eq!(parse_png_dimensions(&fake_png), Some((1920, 1080)));
        assert_eq!(android_keyevent_for_button("home").unwrap(), "KEYCODE_HOME");
        assert_eq!(escape_adb_input_text("hello world&1"), "hello%sworld\\&1");
    }

    #[test]
    fn ref_map_persists_alongside_agent_config_file_and_work_path_blocks_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let config_file = dir.path().join("config");
        std::fs::write(&config_file, "{}").unwrap();

        let (_, map) = build_snapshot_tree(
            "s1234567",
            vec![RawA11yNode {
                role: "button".into(),
                name: "Confirm".into(),
                window_id: Some("0x1".into()),
                app: Some("App".into()),
                bounds: Region {
                    x: 10,
                    y: 20,
                    width: 100,
                    height: 30,
                },
                interactive: true,
                children: Vec::new(),
            }],
            false,
            true,
        );
        save_ref_map(&config_file, &map);
        assert!(dir.path().join("computer-refmap.json").is_file());
        let loaded = load_ref_map(&config_file);
        assert_eq!(loaded.active_snapshot_id.as_deref(), Some("s1234567"));

        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&work_dir).unwrap();
        assert!(resolve_work_path(&work_dir, "out/speech.mp3").is_ok());
        assert!(resolve_work_path(&work_dir, "../../etc/passwd").is_err());
        assert!(resolve_work_path(&work_dir, "/etc/shadow").is_err());
    }
}
