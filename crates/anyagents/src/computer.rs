//! `tools.computer`, `tools.device` and `tools.media`, and the vocabulary
//! they share: provider-neutral computer actions, semantic snapshot refs, and
//! structured error dispositions.
//!
//! A `tools.computer` request reaches exactly one desktop ([`target`]):
//! a cloud sandbox, the host machine's own desktop, or the agent's session
//! desktop (the headless Wayland desktop that also runs its browser, owned by
//! the server's workstation registry).

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

mod cloud_sandbox;
mod device;
mod host_desktop;
mod media;

pub use device::execute_device_request;
pub use media::execute_media_request;

/// The desktop a `tools.computer` request drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// A cloud sandbox named by `session_id` / `sandbox_id` / `provider: "cloud"`.
    Cloud,
    /// The machine this process runs on.
    Host,
    /// The agent's session desktop, served by the AnyAgents server.
    Session,
}

/// Select the desktop for `req`.
///
/// The host is chosen when the request names a host desktop (`platform` /
/// `os`, `display`, `wayland_display`), when this build has no session desktop
/// (non-Linux), or when the host is ChromeOS Crostini, whose desktop is the
/// ChromeOS one. Everything else drives the session desktop.
pub fn target(req: &Value) -> Target {
    if cloud_sandbox::is_cloud_request(req) {
        return Target::Cloud;
    }
    let payload = req
        .get("action_json")
        .or_else(|| req.get("payload"))
        .or_else(|| req.get("request"))
        .unwrap_or(req);
    let names_host = ["platform", "os", "display", "wayland_display"]
        .iter()
        .any(|k| payload.get(k).is_some() || req.get(k).is_some());
    if names_host || !cfg!(target_os = "linux") || host_desktop::is_crostini() {
        Target::Host
    } else {
        Target::Session
    }
}

/// Execute a cloud or host `tools.computer` request in this process. Session
/// requests belong to the server and are refused here.
pub async fn execute_local(
    data_dir: Option<&Path>,
    config_dir: &Path,
    req: Value,
) -> Result<Value, String> {
    let mutating = computer_request_mutates(&req);
    let result = match target(&req) {
        Target::Cloud => cloud_sandbox::execute(data_dir, config_dir, req).await,
        Target::Host => return host_desktop::execute(config_dir, req).await,
        Target::Session => {
            Err("session desktop requests are served by the AnyAgents server".into())
        }
    };
    result.map_err(|err| {
        if err.contains("\"disposition\"") {
            err
        } else {
            let disp = classify_computer_error(&err, mutating);
            format_computer_error(err, disp)
        }
    })
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

fn post_raw_bytes(
    url: &str,
    bearer_token: &str,
    content_type: &str,
    body_bytes: &[u8],
    timeout_secs: u64,
) -> Result<(u16, Vec<u8>), String> {
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let resp = http
        .post(url)
        .header("Authorization", &format!("Bearer {bearer_token}"))
        .header("Content-Type", content_type)
        .send(body_bytes)
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

fn post_json_bytes(
    url: &str,
    bearer_token: &str,
    payload: &Value,
    timeout_secs: u64,
) -> Result<(u16, Vec<u8>), String> {
    let body_bytes = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
    post_raw_bytes(
        url,
        bearer_token,
        "application/json",
        &body_bytes,
        timeout_secs,
    )
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
    /// `"settled_absence" | "retryable_failure" | "delivery_uncertain" | "stale_ref" |
    /// "terminal_failure"`
    pub kind: String,
    /// `"safe" | "verify_first" | "never"`
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

    /// A mutating action failed in a way that does not say whether it reached
    /// its target (a timeout after dispatch). Repeating it blindly could
    /// deliver it twice; verify the current state first.
    pub fn delivery_uncertain(hint: impl Into<String>) -> Self {
        Self {
            kind: "delivery_uncertain".to_string(),
            retry: "verify_first".to_string(),
            code: Some("DELIVERY_UNCERTAIN".to_string()),
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

/// The disposition of a transient failure, or `None` when `lower` (an error
/// message in lower case) is not one. A read that timed out or was refused can
/// be retried. A mutating action can be retried only when it was refused before
/// dispatch (429 or 503 at admission, a browser that never started); a timeout
/// may come after the action was delivered, so it is `delivery_uncertain`.
pub(crate) fn transient_disposition(
    lower: &str,
    mutating: bool,
    retry_hint: &str,
    verify_hint: &str,
) -> Option<ErrorDisposition> {
    let timed_out = lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporarily unavailable");
    if !mutating {
        return (timed_out || lower.contains("429") || lower.contains("503"))
            .then(|| ErrorDisposition::retryable(retry_hint));
    }
    if lower.contains("startup timed out") {
        Some(ErrorDisposition::retryable(retry_hint))
    } else if timed_out {
        Some(ErrorDisposition::delivery_uncertain(verify_hint))
    } else if mentions_status(lower, "429") || mentions_status(lower, "503") {
        Some(ErrorDisposition::retryable(retry_hint))
    } else {
        None
    }
}

/// Whether `code` appears in `text` as a number of its own, not as digits of
/// a longer one.
fn mentions_status(text: &str, code: &str) -> bool {
    text.match_indices(code).any(|(at, _)| {
        !text[..at].ends_with(|c: char| c.is_ascii_digit())
            && !text[at + code.len()..].starts_with(|c: char| c.is_ascii_digit())
    })
}

/// Whether a `tools.computer` request can change the desktop. Only the reads
/// named here are not; a request this cannot parse counts as mutating.
pub(crate) fn computer_request_mutates(req: &Value) -> bool {
    let payload = req
        .get("action_json")
        .or_else(|| req.get("payload"))
        .or_else(|| req.get("request"))
        .unwrap_or(req);
    let raw_action = payload
        .get("action")
        .or_else(|| payload.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();
    match raw_action.as_str() {
        "snapshot" | "list_windows" | "windows" | "clipboard_get" => false,
        "focus_window" | "clipboard_set" | "open_url" | "device" => true,
        _ => parse_computer_action(payload).map_or(true, |action| action.is_mutating()),
    }
}

/// Whether a `tools.device` request can change the device: everything but a
/// screenshot or a snapshot.
fn device_request_mutates(req: &Value) -> bool {
    let action = req
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("screenshot")
        .trim()
        .to_ascii_lowercase();
    !matches!(action.as_str(), "screenshot" | "snapshot")
}

/// Classify a raw computer error string into a structured `ErrorDisposition`.
/// `mutating`: whether the action can change the screen or device (a click, a
/// keystroke, a scroll, a drag); see [`transient_disposition`].
pub fn classify_computer_error(err: &str, mutating: bool) -> ErrorDisposition {
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
    } else if let Some(disposition) = transient_disposition(
        &lower,
        mutating,
        "Transient timeout or busy display server; safe to retry once.",
        "The action may already have been delivered; call tools.computer.snapshot() or \
         tools.computer.screenshot() and verify the result before repeating it.",
    ) {
        disposition
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
    if let Ok(raw) = serde_json::to_string_pretty(&merged)
        && let Err(err) =
            anyagents_harness::atomic_write_nonempty_file(&path, raw.as_bytes(), 0o644)
    {
        eprintln!("cannot persist {}: {err}", path.display());
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

fn checked_output(out: std::process::Output, backend: &str) -> Result<Vec<u8>, String> {
    if out.status.success() {
        Ok(out.stdout)
    } else {
        // Native scripts may contain secret text; do not echo script diagnostics.
        Err(format!(
            "{backend} failed (exit {}); check screen recording/accessibility permissions and backend availability",
            out.status
        ))
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

/// The declared path of a desktop or device tool (`xdotool`, `adb`, `idb`, ...).
/// A tool the toolchain refuses is a typed error naming it, before any spawn.
fn computer_tool(program: &str) -> Result<std::path::PathBuf, String> {
    anyagents_toolchain::tool(program, "computer")
        .map(|tool| tool.path)
        .map_err(|err| err.to_string())
}

async fn run_cmd(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    timeout_ms: u64,
) -> Result<std::process::Output, String> {
    let mut cmd = tokio::process::Command::new(computer_tool(program)?);
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
    let mut cmd = tokio::process::Command::new(computer_tool(program)?);
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
        .unwrap_or_else(|| "https://api.sylphx.com".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sylphx_gateway_base_url_defaults_to_api_and_strips_v1_suffix() {
        let _environment = crate::server::tests::env_lock();
        let prev_ai = std::env::var_os("SYLPHX_AI_BASE_URL");
        let prev_gw = std::env::var_os("SYLPHX_GATEWAY_URL");

        unsafe {
            std::env::remove_var("SYLPHX_AI_BASE_URL");
            std::env::remove_var("SYLPHX_GATEWAY_URL");
        }
        assert_eq!(resolve_sylphx_gateway_base_url(), "https://api.sylphx.com");

        unsafe {
            std::env::set_var("SYLPHX_AI_BASE_URL", "   ");
            std::env::set_var("SYLPHX_GATEWAY_URL", "https://gateway.sylphx.com/v1/");
        }
        assert_eq!(
            resolve_sylphx_gateway_base_url(),
            "https://gateway.sylphx.com"
        );

        unsafe {
            std::env::set_var("SYLPHX_AI_BASE_URL", "https://api.sylphx.com/v1/");
        }
        assert_eq!(resolve_sylphx_gateway_base_url(), "https://api.sylphx.com");

        unsafe {
            match prev_ai {
                Some(v) => std::env::set_var("SYLPHX_AI_BASE_URL", v),
                None => std::env::remove_var("SYLPHX_AI_BASE_URL"),
            }
            match prev_gw {
                Some(v) => std::env::set_var("SYLPHX_GATEWAY_URL", v),
                None => std::env::remove_var("SYLPHX_GATEWAY_URL"),
            }
        }
    }

    #[test]
    fn a_mutating_action_that_timed_out_is_delivery_uncertain() {
        for err in [
            "`xdotool` timed out after 2000ms",
            "HTTP request failed: timeout: global",
            "Resource temporarily unavailable (os error 11)",
            "cloud computer sandbox returned 503: {\"error\":\"upstream timed out\"}",
        ] {
            let read = classify_computer_error(err, false);
            assert_eq!(
                (read.kind.as_str(), read.retry.as_str()),
                ("retryable_failure", "safe")
            );
            let write = classify_computer_error(err, true);
            assert_eq!(
                (write.kind.as_str(), write.retry.as_str()),
                ("delivery_uncertain", "verify_first"),
                "{err}"
            );
            assert_eq!(write.code.as_deref(), Some("DELIVERY_UNCERTAIN"));
            assert!(write.hint.unwrap().contains("snapshot()"));
        }
        // Refused at admission, so never delivered: safe to retry either way.
        for err in [
            "cloud computer sandbox returned 429: {\"error\":\"rate limited\"}",
            "cloud computer sandbox returned 503: {\"error\":\"no capacity\"}",
        ] {
            assert_eq!(classify_computer_error(err, true).kind, "retryable_failure");
            assert_eq!(
                classify_computer_error(err, false).kind,
                "retryable_failure"
            );
        }
        // Digits of a longer number are not a status code.
        let err = "cloud computer sandbox returned 500: {\"request\":\"r-14290\"}";
        assert_eq!(classify_computer_error(err, true).kind, "terminal_failure");
        // Refusals before dispatch keep their dispositions.
        assert_eq!(
            classify_computer_error("STALE_REF @s1:e1", true).kind,
            "stale_ref"
        );
        assert_eq!(
            classify_computer_error("no such window 0x42", true).kind,
            "settled_absence"
        );
    }

    #[test]
    fn computer_and_device_requests_know_which_actions_mutate() {
        for req in [
            json!({"action": "left_click", "coordinate": [1, 2]}),
            json!({"action": "click", "x": 1, "y": 2}),
            json!({"type": "click", "x": 1, "y": 2, "button": "left"}),
            json!({"action": "type", "text": "hello"}),
            json!({"action": "key", "text": "Return"}),
            json!({"action": "scroll", "coordinate": [1, 2], "scroll_direction": "down"}),
            json!({"action": "left_click_drag", "start_coordinate": [1, 2], "coordinate": [3, 4]}),
            json!({"action": "click", "ref": "@s1234567:e1"}),
            json!({"action": "focus_window", "window_id": "0x42"}),
            json!({"action": "clipboard_set", "text": "x"}),
            json!({"action": "open_url", "url": "https://example.com"}),
            json!({"action": "something_new"}),
            json!({"action_json": {"action": "left_click", "coordinate": [1, 2]}}),
            json!({"payload": {"action": "type", "text": "x"}}),
        ] {
            assert!(computer_request_mutates(&req), "{req}");
        }
        for req in [
            json!({}),
            json!({"action": "screenshot"}),
            json!({"type": "screenshot"}),
            json!({"action": "cursor_position"}),
            json!({"action": "wait", "duration": 1}),
            json!({"action": "zoom", "region": [0, 0, 10, 10]}),
            json!({"action": "snapshot"}),
            json!({"action": "list_windows"}),
            json!({"action": "clipboard_get"}),
            json!({"payload": {"action": "snapshot"}}),
        ] {
            assert!(!computer_request_mutates(&req), "{req}");
        }
        for action in [
            "tap",
            "swipe",
            "type",
            "press",
            "launch_app",
            "open_url",
            "rotate",
        ] {
            assert!(
                device_request_mutates(&json!({ "action": action })),
                "{action}"
            );
        }
        for req in [
            json!({}),
            json!({"action": "screenshot"}),
            json!({"action": "snapshot"}),
        ] {
            assert!(!device_request_mutates(&req), "{req}");
        }
    }

    /// End to end through the cloud sandbox: the same timed-out response is a
    /// safe retry for a screenshot and `delivery_uncertain` for a click; a 429
    /// at admission is a safe retry for both.
    #[test]
    fn a_cloud_click_that_timed_out_is_not_retried_blindly() {
        use std::io::{BufRead as _, BufReader, Write as _};
        let _environment = crate::server::tests::env_lock();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let responses = [
            ("504 Gateway Timeout", r#"{"error":"upstream timed out"}"#),
            ("504 Gateway Timeout", r#"{"error":"upstream timed out"}"#),
            ("429 Too Many Requests", r#"{"error":"rate limited"}"#),
        ];
        let server = std::thread::spawn(move || {
            for (status, body) in responses {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = value.trim().parse().unwrap();
                    }
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                std::io::Read::read_exact(&mut reader, &mut vec![0; length]).unwrap();
                let mut stream = reader.into_inner();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let dir = crate::test_tempdir();
        std::fs::create_dir_all(dir.path().join(".config/sylphx")).unwrap();
        std::fs::write(dir.path().join(".config/sylphx/key"), "synthetic-key").unwrap();
        unsafe { std::env::set_var("SYLPHX_SANDBOX_URL", format!("http://127.0.0.1:{port}")) };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut outcomes = Vec::new();
        for req in [
            json!({"session_id": "s1", "action": "screenshot"}),
            json!({"session_id": "s1", "action": "left_click", "coordinate": [1, 2]}),
            json!({"session_id": "s1", "action": "left_click", "coordinate": [1, 2]}),
        ] {
            outcomes.push(
                runtime
                    .block_on(execute_local(None, dir.path(), req))
                    .unwrap_err(),
            );
        }
        unsafe { std::env::remove_var("SYLPHX_SANDBOX_URL") };
        server.join().unwrap();
        assert!(outcomes[0].contains("RETRYABLE_FAILURE"), "{}", outcomes[0]);
        assert!(
            outcomes[1].contains("DELIVERY_UNCERTAIN"),
            "{}",
            outcomes[1]
        );
        assert!(outcomes[1].contains("verify_first"), "{}", outcomes[1]);
        assert!(outcomes[2].contains("RETRYABLE_FAILURE"), "{}", outcomes[2]);
    }

    #[test]
    fn requests_route_to_exactly_one_desktop() {
        assert_eq!(
            target(&json!({"action": "screenshot", "session_id": "s1"})),
            Target::Cloud
        );
        assert_eq!(
            target(&json!({"action": "screenshot", "provider": "cloud"})),
            Target::Cloud
        );
        for key in ["platform", "os", "display", "wayland_display"] {
            let mut req = json!({"action": "screenshot"});
            req[key] = json!("x");
            assert_eq!(target(&req), Target::Host, "{req}");
            assert_eq!(
                target(&json!({"action_json": {"action": "click", key: "x"}})),
                Target::Host
            );
        }
        let plain = target(&json!({"action": "screenshot"}));
        if cfg!(target_os = "linux") && !host_desktop::is_crostini() {
            assert_eq!(plain, Target::Session);
        } else {
            assert_eq!(plain, Target::Host);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_timeout_covers_blocked_stdin_and_output() {
        // More than a pipe buffer ensures the timeout also covers a blocked write.
        let input = vec![b'x'; 1024 * 1024];
        let started = std::time::Instant::now();
        let err = run_cmd_with_stdin("/bin/sh", &["-c", "exec /bin/sleep 30"], &[], &input, 25)
            .await
            .unwrap_err();
        assert!(err.contains("timed out after 25ms"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(3));
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
    fn parses_png_dimensions() {
        let mut fake_png = Vec::new();
        fake_png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        fake_png.extend_from_slice(&13u32.to_be_bytes());
        fake_png.extend_from_slice(b"IHDR");
        fake_png.extend_from_slice(&1920u32.to_be_bytes());
        fake_png.extend_from_slice(&1080u32.to_be_bytes());
        assert_eq!(parse_png_dimensions(&fake_png), Some((1920, 1080)));
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
