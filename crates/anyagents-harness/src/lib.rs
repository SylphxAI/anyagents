//! Adapter contract for a vendor harness, plus a scripted fake used by tests.
//!
//! A harness is admitted with whatever delivery mode it can actually do. The
//! mailbox and the delivery loop do not change between modes. Adapters carry
//! the mailbox id on the first line of the user text as `anyagents-message:<id>`.

mod fake;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;

pub use fake::FakeHarness;

/// Internal environment key the server sets when an agent has an OS seat. It
/// carries `<launcher>\t<uid>\t<gid>\t<groups>` and is removed from the child's
/// environment before it starts, so a harness never passes it on.
pub const SEAT_LAUNCH_ENV: &str = "ANYAGENTS_SEAT_LAUNCH";

/// The line that names the mailbox message a bare answer reaches, in the user
/// text every harness shows its model. The server writes it from the record it
/// stored; a model cannot forge it, and a reply that names it is resolved by the
/// server against that record.
pub const REPLY_HINT_PREFIX: &str = "answer with reply_to: ";

/// The reply reference a message names: `anyagents:reply:<message id>`.
pub fn reply_ref(message_id: &str) -> String {
    format!("anyagents:reply:{message_id}")
}

/// The mailbox reference a reply names, when it is one.
pub fn ref_message(reference: &str) -> Option<&str> {
    let id = reference.strip_prefix("anyagents:reply:")?;
    (!id.is_empty() && !id.contains(':')).then_some(id)
}

/// The text one stored message becomes for every harness: the ID line, then a
/// structured `<message>` XML envelope containing optional `<quoted_message>`
/// context and the `<body>`.
pub fn compose_message(id: &str, context: &Context, body: &str) -> String {
    let (main_body, quoted_xml) = extract_quoted_xml(body, context.reply_to_author.as_deref());
    let open_tag = context.open_tag(id);
    match quoted_xml {
        Some(q) => format!(
            "{MESSAGE_MARKER_PREFIX}{id}\n{open_tag}\n{q}\n<body>\n{main_body}\n</body>\n</message>"
        ),
        None => format!(
            "{MESSAGE_MARKER_PREFIX}{id}\n{open_tag}\n<body>\n{main_body}\n</body>\n</message>"
        ),
    }
}

fn xml_attr_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn extract_quoted_xml<'a>(
    body: &'a str,
    reply_to_author: Option<&str>,
) -> (&'a str, Option<String>) {
    const FENCE_START: &str = "\n\n----- quoted message -----\n";
    const FENCE_END: &str = "\n----- end of quoted message -----";
    let Some((main_text, rest)) = body.split_once(FENCE_START) else {
        return (body, None);
    };
    let Some(inner) = rest.strip_suffix(FENCE_END) else {
        return (body, None);
    };
    let Some((first_line, mut q_rest)) = inner.split_once('\n') else {
        return (body, None);
    };
    let (role, raw_author) = if let Some(a) = first_line
        .strip_prefix("The agent replied to ")
        .and_then(|s| s.strip_suffix(':'))
    {
        ("agent", a)
    } else if let Some(a) = first_line
        .strip_prefix("The person replied to ")
        .and_then(|s| s.strip_suffix(':'))
    {
        ("user", a)
    } else {
        ("unknown", first_line.trim_end_matches(':'))
    };
    let mut q_mentions: Option<&str> = None;
    if let Some((before_last, last_line)) = q_rest.rsplit_once('\n')
        && let Some(m) = last_line
            .strip_prefix("(mentions: ")
            .and_then(|s| s.strip_suffix(')'))
    {
        q_mentions = Some(m);
        q_rest = before_last;
    }
    let q_text = q_rest
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(q_rest);
    let author_label = reply_to_author.unwrap_or(raw_author);
    let mut tag = format!(
        "<quoted_message author=\"{}\" author_role=\"{}\"",
        xml_attr_escape(author_label),
        role
    );
    if let Some(m) = q_mentions {
        tag.push_str(&format!(" mentions=\"{}\"", xml_attr_escape(m)));
    }
    tag.push('>');
    let xml = format!("{tag}\n{q_text}\n</quoted_message>");
    (main_text, Some(xml))
}

/// The trusted header of one message, as the server wrote it.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Context {
    /// The authenticated author: `agent:<name>`, `telegram:user:<id>`, ...
    pub sender: Option<String>,
    /// Human-readable display name of the sender (e.g. `Kyle (@shtse8)`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    /// The recipient agent (`agent:<name>`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
    /// The channel it arrived through, when it came by one.
    pub channel: Option<String>,
    /// The provider chat, when the message is one.
    pub chat_id: Option<i64>,
    /// Human-readable title of the chat/channel/group, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_title: Option<String>,
    /// Kind of chat (`private`, `group`, `supergroup`, `channel`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_type: Option<String>,
    /// The forum topic inside that chat.
    pub thread_id: Option<i64>,
    /// The provider id of the incoming message a bare answer reaches.
    pub message_id: Option<i64>,
    /// Where a bare answer goes, as the send tool takes it.
    pub destination: Option<String>,
    /// Special channel event (`"pinned"`, `"edited"`, etc.), when this line is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Who this message is directly replying to (quoted message author), when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to_author: Option<String>,
    /// Handles or agents mentioned in this message (`@username`, `agent:<name>`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<String>,
}

impl Context {
    /// XML opening tag `<message ...>` with structured metadata attributes.
    pub fn open_tag(&self, id: &str) -> String {
        let mut out = format!(
            "<message id=\"{}\" from=\"{}\"",
            xml_attr_escape(id),
            xml_attr_escape(self.sender.as_deref().unwrap_or("unknown"))
        );
        if let Some(sender_name) = &self.sender_name {
            out.push_str(&format!(
                " sender_name=\"{}\"",
                xml_attr_escape(sender_name)
            ));
        }
        if let Some(recipient) = &self.recipient {
            out.push_str(&format!(" to=\"{}\"", xml_attr_escape(recipient)));
        }
        if let Some(channel) = &self.channel {
            out.push_str(&format!(" channel=\"{}\"", xml_attr_escape(channel)));
        }
        if let Some(chat) = self.chat_id {
            out.push_str(&format!(" chat=\"{chat}\""));
        }
        if let Some(chat_title) = &self.chat_title {
            out.push_str(&format!(" chat_title=\"{}\"", xml_attr_escape(chat_title)));
        }
        if let Some(chat_type) = &self.chat_type {
            out.push_str(&format!(" chat_type=\"{}\"", xml_attr_escape(chat_type)));
        }
        if let Some(thread) = self.thread_id {
            out.push_str(&format!(" topic=\"{thread}\""));
        }
        if let Some(message) = self.message_id {
            out.push_str(&format!(" message_id=\"{message}\""));
        }
        if let Some(event) = &self.event {
            out.push_str(&format!(" event=\"{}\"", xml_attr_escape(event)));
        }
        if let Some(reply_to_author) = &self.reply_to_author {
            out.push_str(&format!(
                " in_reply_to=\"{}\"",
                xml_attr_escape(reply_to_author)
            ));
        }
        if !self.mentions.is_empty() {
            out.push_str(&format!(
                " mentions=\"{}\"",
                xml_attr_escape(&self.mentions.join(", "))
            ));
        }
        out.push_str(&format!(
            " reply_to=\"{}\"",
            xml_attr_escape(&reply_ref(id))
        ));
        if let Some(destination) = &self.destination {
            out.push_str(&format!(" answer_to=\"{}\"", xml_attr_escape(destination)));
        }
        out.push('>');
        out
    }

    /// The lines a model reads, in a fixed order. The reply reference is this
    /// message's own id, so it is always there.
    pub fn lines(&self, id: &str) -> String {
        let mut out = String::from("----- message -----\n");
        out.push_str(&format!(
            "from: {}\n",
            self.sender.as_deref().unwrap_or("unknown")
        ));
        if let Some(channel) = &self.channel {
            out.push_str(&format!("channel: {channel}\n"));
        }
        if let Some(chat) = self.chat_id {
            out.push_str(&format!("chat: {chat}\n"));
        }
        if let Some(thread) = self.thread_id {
            out.push_str(&format!("topic: {thread}\n"));
        }
        if let Some(message) = self.message_id {
            out.push_str(&format!("message: {message}\n"));
        }
        if let Some(event) = &self.event {
            out.push_str(&format!("event: {event}\n"));
        }
        if let Some(reply_to_author) = &self.reply_to_author {
            out.push_str(&format!("replying to: {reply_to_author}\n"));
        }
        if !self.mentions.is_empty() {
            out.push_str(&format!("mentions: {}\n", self.mentions.join(", ")));
        }
        out.push_str(&format!("{REPLY_HINT_PREFIX}{}\n", reply_ref(id)));
        if let Some(destination) = &self.destination {
            out.push_str(&format!("answer to: {destination}\n"));
        }
        out.push_str("----- end of message -----");
        out
    }
}

/// The server-only keys a harness process never inherits: the seat spec, the
/// operator's token path, the capability key and the seat configuration.
pub const SERVER_ONLY_ENV: &[&str] = &[
    SEAT_LAUNCH_ENV,
    "ANYAGENTS_OPERATOR_TOKEN_FILE",
    "ANYAGENTS_CAPABILITY_KEY",
    "ANYAGENTS_AGENT_UID",
    "ANYAGENTS_AGENT_GID",
    "ANYAGENTS_SOCKET_GID",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
];

/// Build the command that starts a harness process. When `environment` names a
/// seat, the process is started inside it with `setpriv --reuid … --no-new-privs`,
/// and no server-only key reaches it.
pub fn spawn_command(
    program: &std::path::Path,
    environment: &[(String, String)],
) -> std::process::Command {
    let seat = environment
        .iter()
        .find(|(key, _)| key == SEAT_LAUNCH_ENV)
        .map(|(_, value)| value.as_str());
    let mut command = match seat {
        Some(spec) => match spec.split('\t').collect::<Vec<_>>().as_slice() {
            [launcher, uid, gid, groups] => {
                let mut command = std::process::Command::new(launcher);
                command.args([
                    "--reuid",
                    uid,
                    "--regid",
                    gid,
                    "--groups",
                    groups,
                    "--no-new-privs",
                    "--",
                ]);
                command.arg(program);
                command
            }
            _ => std::process::Command::new(program),
        },
        None => std::process::Command::new(program),
    };
    for key in SERVER_ONLY_ENV {
        command.env_remove(key);
    }
    for (key, value) in environment {
        if key == SEAT_LAUNCH_ENV {
            continue;
        }
        command.env(key, value);
    }
    command
}

/// Send `signal` (e.g. `"-TERM"` or `"-KILL"`) to a harness child process PID,
/// running as the child's seat UID when `environment` names one.
pub fn kill_pid(pid: u32, signal: &str, environment: &[(String, String)]) {
    use std::process::Stdio;
    let seat = environment
        .iter()
        .find(|(k, _)| k == SEAT_LAUNCH_ENV)
        .map(|(_, v)| v.as_str());
    if let Some(spec) = seat
        && let [launcher, uid, gid, _] = spec.split('\t').collect::<Vec<_>>().as_slice()
    {
        let _ = std::process::Command::new(launcher)
            .args([
                "--reuid",
                uid,
                "--regid",
                gid,
                "--clear-groups",
                "--no-new-privs",
                "--",
                "/bin/kill",
                signal,
                &pid.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    } else {
        let _ = std::process::Command::new("kill")
            .args([signal, &pid.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Kill `child` inside its seat UID boundary when `environment` names one.
pub fn kill_child(child: &mut std::process::Child, environment: &[(String, String)]) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    let pid = child.id();
    kill_pid(pid, "-TERM", environment);
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    kill_pid(pid, "-KILL", environment);
    let _ = child.kill();
}

/// How a harness takes input that arrives during a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Input enters the active turn, including while a tool runs.
    Steer,
    /// The tool takes the input now and acts on it when the current turn ends.
    Followup,
    /// The tool cannot take input during a turn. Deliver it when the turn ends.
    Queue,
}

/// Whether this harness has shown it can steer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SteerSupport {
    Yes,
    No,
    Unproven,
}

/// Whether a turn is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnState {
    Idle,
    InTurn,
}

/// What `health` reports. Quota and sign-in trouble are states, not crashes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessHealth {
    pub process_alive: bool,
    pub turn_state: TurnState,
    pub delivery_mode: DeliveryMode,
    pub steer_support: SteerSupport,
    pub quota_reset_at_ms: Option<i64>,
    pub sign_in_fault: Option<String>,
}

/// One mailbox message handed to a harness. The id is what a replay is deduped on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundMessage {
    pub id: String,
    pub seq: i64,
    pub text: String,
    pub sender: Option<String>,
    pub reply_to: Option<String>,
    pub source_time_ms: Option<i64>,
    /// The server's trusted header for this message: author, channel, chat,
    /// topic, the reply a bare answer names and where it goes.
    pub context: Context,
}

/// Result of one `deliver` call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliverOutcome {
    /// The harness has the message. `turn_finished` means the turn that
    /// contained it has already completed.
    Accepted { turn_finished: bool },
    /// Input cannot take the complete message yet; it stays in the mailbox.
    Deferred,
    /// The vendor asked us to wait. Not a failure and not a restart.
    Quota { reset_at_ms: i64 },
}

/// The harness process could not take the message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HarnessError {
    #[error("harness process is not running")]
    Dead,
    #[error("{0}")]
    Failed(String),
}

/// A mailbox message that entered an adapter's between-steps input, with the
/// order it arrived in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SteerItem {
    pub message: OutboundMessage,
    pub order: u64,
}

/// Calls every harness adapter implements. The last calls have defaults so a
/// scripted fake can stay a delivery double without a process.
pub trait Harness: Send {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError>;
    fn background(&mut self) -> Result<(), HarnessError>;
    fn resume(&mut self, session: &str) -> Result<(), HarnessError>;
    fn cancel(&mut self) -> Result<(), HarnessError>;
    fn health(&self) -> HarnessHealth;

    /// False while transport backpressure prevents input. watch_activity must
    /// notify the owner when input becomes writable again.
    fn input_ready(&self) -> bool {
        true
    }

    /// How long a turn must stay silent before its answer stands still, for a
    /// vendor that streams text and never closes a turn with a result line.
    /// `None` means the vendor always closes its turn.
    fn ends_on_quiet(&self) -> Option<Duration> {
        None
    }

    /// Wait until the turn is silent long enough to stand still, giving up
    /// after `patience`.
    fn quiet_for(&self, _id: &str, _quiet: Duration, _patience: Duration) -> bool {
        false
    }

    /// Usage the vendor reported for this process, as of now, for the turns it
    /// finished. Served totals, not the newest turn: a burst is several turns,
    /// and the host reads the difference. It is never part of delivery.
    fn served_usage(&self) -> TurnUsage {
        TurnUsage::default()
    }

    /// How many turns this process has served so far. The host records one
    /// usage row per turn; an adapter that does not count reports 0.
    fn turns_served(&self) -> u32 {
        0
    }

    /// Usage the vendor reported for the turns this process ran, as of now. The
    /// host reads it once per burst and records it; it is never part of delivery.
    fn last_usage(&self) -> TurnUsage {
        self.served_usage()
    }

    /// Forget a quota wait so delivery can resume after the reset time.
    fn clear_quota(&mut self) {}

    /// Take the oldest message that entered input the vendor reads between
    /// model steps (a before-step hook, a steering socket), for the host to
    /// write into the next step with [`Harness::inject`]. `None` means this
    /// harness has no such channel, or nothing new arrived. A taken message
    /// stays held until `inject` reports the vendor took it, so a crash loses
    /// nothing.
    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        Ok(None)
    }

    /// Put one message taken by [`Harness::poll_steer`] into the live process
    /// through the harness's own mid-turn mechanism. `Ok(true)` means the vendor
    /// took it: the host advances the delivered cursor exactly once. `Ok(false)`
    /// means it is still held for the next step, cursor unchanged.
    fn inject(&mut self, _message: &OutboundMessage) -> Result<bool, HarnessError> {
        Ok(false)
    }

    /// Put the held messages back for the next step, except `keep`, which an
    /// `inject` is still holding. Called when `inject` cannot take them now.
    fn release_steer(&mut self, _keep: &str) {}

    /// Operating system id of the vendor process, when it has one.
    fn process_id(&self) -> Option<u32> {
        None
    }

    /// Call `notify` after the vendor process reports activity or exits.
    ///
    /// The callback runs on a background thread. It must not call back into
    /// this harness.
    fn watch_activity(&self, _notify: std::sync::Arc<dyn Fn() + Send + Sync>) {}
}

impl TurnUsage {
    /// What was served between `after` and this reading.
    pub fn difference(&self, after: TurnUsage) -> TurnUsage {
        TurnUsage {
            input_tokens: self.input_tokens.saturating_sub(after.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(after.output_tokens),
            cache_read_tokens: self
                .cache_read_tokens
                .saturating_sub(after.cache_read_tokens),
            cache_write_tokens: self
                .cache_write_tokens
                .saturating_sub(after.cache_write_tokens),
            reported_cost_micros: match (self.reported_cost_micros, after.reported_cost_micros) {
                (Some(now), Some(before)) => Some(now - before),
                (Some(now), None) => Some(now),
                _ => None,
            },
        }
    }
}

/// Token and cost totals one vendor turn reported.
///
/// Cost is in millionths of the vendor's currency unit when the vendor reports
/// a cost. A vendor that reports only tokens leaves the cost empty.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reported_cost_micros: Option<i64>,
}

/// One line handed over by a between-steps channel: the mailbox id, its
/// sequence, and the text.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SteerLine {
    pub id: String,
    pub seq: i64,
    pub message: String,
}

/// The messages a before-step hook or a steering socket pushed, in arrival
/// order, held until the vendor takes them.
///
/// The pushing side appends one JSON [`SteerLine`] per message to the file.
/// The host takes the oldest `limit` lines and keeps them, so a crash between
/// the take and the vendor's acceptance loses nothing: they are still held by
/// the host that took them and, after a crash, [`SteerQueue::release`] puts
/// them back. A file read or write failure leaves the cursor where it is.
#[derive(Clone, Debug)]
pub struct SteerQueue {
    path: std::path::PathBuf,
    limit: usize,
    held: std::sync::Arc<std::sync::Mutex<Vec<SteerItem>>>,
    order: std::sync::Arc<AtomicU64>,
}

impl SteerQueue {
    /// `path` is the push file. `limit` is the most lines one step takes.
    pub fn new(path: impl Into<std::path::PathBuf>, limit: usize) -> Self {
        Self {
            path: path.into(),
            limit: limit.max(1),
            held: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            order: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// The push file's lines, oldest first.
    pub fn lines(&self) -> Vec<SteerLine> {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return Vec::new();
        };
        text.lines()
            .filter_map(|line| serde_json::from_str::<SteerLine>(line).ok())
            .collect()
    }

    /// Take the oldest lines, unless one taken earlier is still held.
    pub fn take(&self) -> Option<SteerItem> {
        let mut held = self.lock();
        if !held.is_empty() {
            return None;
        }
        let mut lines = self.lines();
        lines.truncate(self.limit);
        if lines.is_empty() {
            return None;
        }
        let rest = &self.lines()[lines.len()..];
        write_lines(&self.path, rest).ok()?;
        *held = lines
            .into_iter()
            .map(|line| SteerItem {
                order: self.order.fetch_add(1, Ordering::SeqCst),
                message: OutboundMessage {
                    id: line.id,
                    seq: line.seq,
                    text: line.message,
                    sender: None,
                    reply_to: None,
                    source_time_ms: None,
                    context: Context::default(),
                },
            })
            .collect();
        held.first().cloned()
    }

    /// Records the vendor's acceptance of `id`, keeping the older held ones.
    pub fn settle(&self, id: &str) {
        let mut held = self.lock();
        held.retain(|item| item.message.id != id);
    }

    /// Drop a held line the state proves already taken, without writing it again.
    pub fn consume(&self, id: &str) {
        self.lock().retain(|item| item.message.id != id);
    }

    /// Put every held line except `except` back at the front of the push file,
    /// oldest first, so the next step, or a restarted host, takes it again.
    pub fn release_for(&self, except: &str) {
        let mut held = self.lock();
        let back_lines: Vec<SteerItem> = held
            .iter()
            .filter(|item| item.message.id != except)
            .cloned()
            .collect();
        if back_lines.is_empty() {
            return;
        }
        let mut lines: Vec<SteerLine> = back_lines
            .iter()
            .map(|item| SteerLine {
                id: item.message.id.clone(),
                seq: item.message.seq,
                message: item.message.text.clone(),
            })
            .collect();
        let ids: std::collections::HashSet<String> = back_lines
            .iter()
            .map(|item| item.message.id.clone())
            .collect();
        let rest: Vec<SteerLine> = self
            .lines()
            .into_iter()
            .filter(|line| !ids.contains(&line.id))
            .collect();
        lines.extend(rest);
        if write_lines(&self.path, &lines).is_ok() {
            held.retain(|item| item.message.id == except);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<SteerItem>> {
        self.held.lock().unwrap_or_else(|err| err.into_inner())
    }
}

fn write_lines(path: &std::path::Path, lines: &[SteerLine]) -> std::io::Result<()> {
    let text: String = lines
        .iter()
        .filter_map(|line| serde_json::to_string(line).ok())
        .map(|line| line + "\n")
        .collect();
    std::fs::write(path, text)
}

/// First line of user text. The rest of the text is the mailbox body.
pub const MESSAGE_MARKER_PREFIX: &str = "anyagents-message:";

/// User text whose first line is the mailbox id.
pub fn marked_message(id: &str, text: &str) -> String {
    format!("{MESSAGE_MARKER_PREFIX}{id}\n{text}")
}

/// Mailbox id when `text` starts with the marker line.
pub fn message_id_in_text(text: &str) -> Option<&str> {
    let id = text.lines().next()?.strip_prefix(MESSAGE_MARKER_PREFIX)?;
    if id.is_empty() { None } else { Some(id) }
}

/// Whether any string inside `value` starts with the mailbox id line.
pub fn mentions_marker(value: &Value, id: &str) -> bool {
    match value {
        Value::String(text) => message_id_in_text(text) == Some(id),
        Value::Array(items) => items.iter().any(|item| mentions_marker(item, id)),
        Value::Object(map) => map.values().any(|item| mentions_marker(item, id)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_is_only_the_first_line() {
        let text = marked_message("msg_1", "hello\nanyagents-message:other");
        assert_eq!(message_id_in_text(&text), Some("msg_1"));
        assert_eq!(message_id_in_text("hello"), None);
        assert_eq!(message_id_in_text("anyagents-message:"), None);
    }

    #[test]
    fn a_queue_take_settle_and_release_never_duplicates_a_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("steer.jsonl");
        let queue = SteerQueue::new(&path, 2);
        std::fs::write(
            &path,
            "{\"id\":\"m1\",\"seq\":1,\"message\":\"one\"}\n{\"id\":\"m2\",\"seq\":2,\"message\":\"two\"}\n{\"id\":\"m3\",\"seq\":3,\"message\":\"three\"}\n",
        )
        .unwrap();
        let first = queue.take().expect("first");
        assert_eq!(first.message.id, "m1");
        assert!(
            queue.take().is_none(),
            "a held line is not taken twice into one step"
        );
        queue.settle("m1");
        assert!(queue.take().is_none(), "m2 and m3 are already held");
        queue.settle("m2");
        // Taking m2 had already taken m3 with it, as one step takes a batch.
        queue.release_for("m3");
        queue.settle("m3");
        let lines = queue.lines();
        let ids: Vec<&str> = lines.iter().map(|line| line.id.as_str()).collect();
        assert_eq!(ids, vec!["m3"]);
        assert_eq!(queue.take().unwrap().message.id, "m3");
    }

    #[test]
    fn a_release_puts_an_unsent_line_back_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("steer.jsonl");
        std::fs::write(&path, "{\"id\":\"m1\",\"seq\":1,\"message\":\"one\"}\n").unwrap();
        let queue = SteerQueue::new(&path, 4);
        queue.take().expect("taken");
        std::fs::write(&path, "{\"id\":\"m2\",\"seq\":2,\"message\":\"two\"}\n").unwrap();
        queue.release_for("m2");
        let ids: Vec<String> = queue.lines().into_iter().map(|line| line.id).collect();
        assert_eq!(ids, vec!["m1", "m2"]);
    }
}
