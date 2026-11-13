//! Adapter contract for a vendor harness, plus a scripted fake used by tests.
//!
//! A harness is admitted with whatever delivery mode it can actually do. The
//! mailbox and the delivery loop do not change between modes. Adapters carry
//! the mailbox id on the first line of the user text as `anyagents-message:<id>`.

mod atomic_fs;
mod fake;
pub mod masking;
mod name;
mod seat;
mod stamp;

use std::borrow::Cow;
use std::time::Duration;

use serde_json::Value;

pub use atomic_fs::{
    atomic_write_file, atomic_write_in_existing_dir, atomic_write_nonempty_file, atomic_write_with,
};
pub use fake::FakeHarness;
pub use name::normalize_harness_name;
pub use stamp::FileStamp;

/// Internal environment key the server sets when an agent has an OS seat. It
/// carries `<launcher>\t<uid>\t<gid>\t<groups>` and is removed from the child's
/// environment before it starts, so a harness never passes it on.
pub const SEAT_LAUNCH_ENV: &str = "ANYAGENTS_SEAT_LAUNCH";
/// Host-resolved agent-root principals; never inherited by vendor children.
pub const SEAT_PEERS_ENV: &str = "ANYAGENTS_SEAT_PEERS";
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SeatPeer {
    pub root: std::path::PathBuf,
    pub uid: u32,
    pub gid: u32,
}

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

/// The text one stored message becomes for every harness: the ID line, then an
/// optional `<topology>` event envelope (initial snapshot or incremental delta),
/// followed by the structured `<message>` XML envelope containing optional
/// `<quoted_message>` context and the `<body>`.
pub fn compose_message(id: &str, context: &Context, body: &str) -> String {
    let (main_body, quoted_xml) = extract_quoted_xml(body, context.reply_to_author.as_deref());
    let open_tag = context.open_tag(id);
    match (context.topology.as_deref(), quoted_xml) {
        (Some(topo), Some(q)) => format!(
            "{MESSAGE_MARKER_PREFIX}{id}\n{topo}\n{open_tag}\n{q}\n<body>\n{main_body}\n</body>\n</message>"
        ),
        (Some(topo), None) => format!(
            "{MESSAGE_MARKER_PREFIX}{id}\n{topo}\n{open_tag}\n<body>\n{main_body}\n</body>\n</message>"
        ),
        (None, Some(q)) => format!(
            "{MESSAGE_MARKER_PREFIX}{id}\n{open_tag}\n{q}\n<body>\n{main_body}\n</body>\n</message>"
        ),
        (None, None) => format!(
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
    /// Human-readable display name of the sender (e.g. `Alex (@alex)`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    /// Role label of the sender agent (e.g. `Reviewer`), when the sender is an agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_role: Option<String>,
    /// Structural relationship of the sender to the recipient (`parent`, `sibling`, `child`, `peer`, `external_user`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_rel: Option<String>,
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
    /// Agents participating in this group or channel conversation (`name (role)`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub participants: Vec<String>,
    /// Event-driven topology snapshot (first user message) or incremental delta
    /// when peers, siblings, children, or groups change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<String>,
    /// The stored message a reply to this line names, when the line is not
    /// one itself: a `[Recovery]` note names the last message of the turn it
    /// covers, which the mailbox holds and a reply can bind to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to_message: Option<String>,
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
        if let Some(sender_role) = &self.sender_role {
            out.push_str(&format!(
                " sender_role=\"{}\"",
                xml_attr_escape(sender_role)
            ));
        }
        if let Some(sender_rel) = &self.sender_rel {
            out.push_str(&format!(" sender_rel=\"{}\"", xml_attr_escape(sender_rel)));
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
        if !self.participants.is_empty() {
            out.push_str(&format!(
                " participants=\"{}\"",
                xml_attr_escape(&self.participants.join(", "))
            ));
        }
        out.push_str(&format!(
            " reply_to=\"{}\"",
            xml_attr_escape(&reply_ref(self.reply_to_message.as_deref().unwrap_or(id)))
        ));
        if let Some(destination) = &self.destination {
            out.push_str(&format!(" answer_to=\"{}\"", xml_attr_escape(destination)));
        }
        out.push('>');
        out
    }

    /// The lines a model reads, in a fixed order. The reply reference is this
    /// message's own id, or the stored message it names, so it is always there.
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
        if !self.participants.is_empty() {
            out.push_str(&format!("participants: {}\n", self.participants.join(", ")));
        }
        out.push_str(&format!(
            "{REPLY_HINT_PREFIX}{}\n",
            reply_ref(self.reply_to_message.as_deref().unwrap_or(id))
        ));
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
    SEAT_PEERS_ENV,
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
/// and no server-only key reaches it. On Unix, places the child in its own
/// process group so wrapper scripts and their descendants can be torn down together.
pub fn spawn_command(
    program: &std::path::Path,
    environment: &[(String, String)],
) -> Result<std::process::Command, HarnessError> {
    let mut command = match environment
        .iter()
        .rev()
        .find(|(key, _)| key == SEAT_LAUNCH_ENV)
    {
        Some((_, spec)) => seat::admitted_command(program, spec, environment)?,
        None => std::process::Command::new(program),
    };
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    for (key, value) in environment {
        if !SERVER_ONLY_ENV.contains(&key.as_str()) {
            command.env(key, value);
        }
    }
    for key in SERVER_ONLY_ENV {
        command.env_remove(key);
    }
    Ok(command)
}

#[cfg(unix)]
mod unix_signal {
    pub(super) const TERM: i32 = 15;
    pub(super) const KILL: i32 = 9;
    const EPERM: i32 = 1;

    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    pub(super) fn signal_number(signal: &str) -> Option<i32> {
        match signal {
            "-TERM" | "TERM" => Some(TERM),
            "-KILL" | "KILL" => Some(KILL),
            _ => None,
        }
    }

    pub(super) fn signal_target(target: i32, sig: i32) -> Result<(), i32> {
        // SAFETY: POSIX `kill(2)` on a validated PID/PGID target and signal number.
        let rc = unsafe { kill(target, sig) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
        }
    }

    pub(super) fn pgid_alive(pgid: i32) -> bool {
        match signal_target(-pgid, 0) {
            Ok(()) => true,
            Err(errno) => errno == EPERM,
        }
    }
}

fn seat_spec(environment: &[(String, String)]) -> Option<(&str, &str, &str)> {
    let spec = environment
        .iter()
        .rev()
        .find(|(k, _)| k == SEAT_LAUNCH_ENV)
        .map(|(_, v)| v.as_str())?;
    let mut parts = spec.split('\t');
    let (Some(launcher), Some(uid), Some(gid), Some(_), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    Some((launcher, uid, gid))
}

fn run_seat_kill(launcher: &str, uid: &str, gid: &str, args: &[&str]) -> bool {
    use std::process::Stdio;
    let mut cmd_args = vec![
        "--reuid",
        uid,
        "--regid",
        gid,
        "--clear-groups",
        "--no-new-privs",
        "--",
        "/bin/kill",
    ];
    cmd_args.extend_from_slice(args);
    std::process::Command::new(launcher)
        .args(cmd_args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Send `signal` (e.g. `"-TERM"` or `"-KILL"`) to a harness child process PID,
/// running as the child's seat UID when `environment` names one.
pub fn kill_pid(pid: u32, signal: &str, environment: &[(String, String)]) {
    use std::process::Stdio;
    if let Some((launcher, uid, gid)) = seat_spec(environment) {
        let pid_str = pid.to_string();
        let _ = run_seat_kill(launcher, uid, gid, &[signal, &pid_str]);
        return;
    }
    #[cfg(unix)]
    if let (Some(raw_pid), Some(sig)) = (
        i32::try_from(pid).ok().filter(|pid| *pid > 0),
        unix_signal::signal_number(signal),
    ) {
        let _ = unix_signal::signal_target(raw_pid, sig);
        return;
    }
    let _ = std::process::Command::new("kill")
        .args([signal, &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn kill_process_group(pid: u32, signal: &str, environment: &[(String, String)]) {
    if let Some((launcher, uid, gid)) = seat_spec(environment) {
        let pgid_str = format!("-{pid}");
        let pid_str = pid.to_string();
        let _ = run_seat_kill(launcher, uid, gid, &[signal, "--", &pgid_str, &pid_str]);
        return;
    }
    #[cfg(unix)]
    if let (Ok(raw_pid), Some(sig)) = (i32::try_from(pid), unix_signal::signal_number(signal)) {
        let _ = unix_signal::signal_target(-raw_pid, sig);
        let _ = unix_signal::signal_target(raw_pid, sig);
        return;
    }
    kill_pid(pid, signal, environment);
}

/// Whether any process is left in the group `pid` leads. A probe with signal
/// 0 needs no permission to answer: a group of another uid (a seat) answers
/// `EPERM`, which still means it exists, so no seat launcher is spawned.
fn pgid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    if let Ok(raw_pid) = i32::try_from(pid) {
        return unix_signal::pgid_alive(raw_pid);
    }
    false
}

#[cfg(target_os = "linux")]
mod pidfd {
    use std::ffi::{c_int, c_long, c_ulong};
    use std::time::Duration;

    const SYS_PIDFD_OPEN: c_long = 434;
    const POLLIN: i16 = 0x1;

    #[repr(C)]
    struct PollFd {
        fd: c_int,
        events: i16,
        revents: i16,
    }

    unsafe extern "C" {
        fn syscall(number: c_long, ...) -> c_long;
        fn poll(fds: *mut PollFd, count: c_ulong, timeout: c_int) -> c_int;
        fn close(fd: c_int) -> c_int;
    }

    /// Wait until `pid` exits or `timeout` passes: `Some(true)` when it has
    /// exited, `Some(false)` on timeout, `None` when the kernel offers no
    /// pidfd for it or the wait failed.
    pub(super) fn wait_exit(pid: u32, timeout: Duration) -> Option<bool> {
        let pid = c_int::try_from(pid).ok()?;
        // SAFETY: pidfd_open takes a pid and flags and returns a new
        // descriptor or -1.
        let fd = unsafe { syscall(SYS_PIDFD_OPEN, pid, 0 as c_int) };
        let fd = c_int::try_from(fd).ok().filter(|fd| *fd >= 0)?;
        let mut polled = PollFd {
            fd,
            events: POLLIN,
            revents: 0,
        };
        let millis = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
        // SAFETY: `polled` is one valid entry; `fd` is owned here.
        let ready = unsafe { poll(&mut polled, 1, millis) };
        // SAFETY: `fd` was opened above and is closed once.
        unsafe { close(fd) };
        (ready >= 0).then_some(ready > 0)
    }
}

/// Wait up to `grace` for `child` to exit and its process group to empty.
/// The child's own exit is an event (its pidfd); only the descendants left in
/// its group, which nothing reports, are probed on a short interval.
fn wait_child_or_group_exit(
    child: &mut std::process::Child,
    pid: u32,
    grace: std::time::Duration,
) -> bool {
    const GROUP_PROBE: std::time::Duration = std::time::Duration::from_millis(25);
    let deadline = std::time::Instant::now() + grace;
    while !matches!(child.try_wait(), Ok(Some(_))) {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return false;
        }
        #[cfg(target_os = "linux")]
        if pidfd::wait_exit(pid, left).is_some() {
            continue;
        }
        std::thread::sleep(left.min(GROUP_PROBE));
    }
    loop {
        if !pgid_alive(pid) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(GROUP_PROBE);
    }
}

/// Kill `child` and its process group inside its seat UID boundary when `environment` names one.
pub fn kill_child(child: &mut std::process::Child, environment: &[(String, String)]) {
    const TERM_GRACE: std::time::Duration = std::time::Duration::from_millis(800);
    const KILL_REAP_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

    let pid = child.id();
    if matches!(child.try_wait(), Ok(Some(_))) && !pgid_alive(pid) {
        return;
    }
    kill_process_group(pid, "-TERM", environment);
    if wait_child_or_group_exit(child, pid, TERM_GRACE) {
        return;
    }
    kill_process_group(pid, "-KILL", environment);
    let _ = child.kill();
    let _ = wait_child_or_group_exit(child, pid, KILL_REAP_GRACE);
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

/// What a harness guarantees for mail that reaches an agent while its turn
/// runs, as `docs/delivery.md` defines the levels. Each level keeps every
/// guarantee of the one before it, and the conformance suite in
/// `tests/invariants` holds every adapter to the level it reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeliveryLevel {
    /// Mail for a busy agent is applied when the running turn ends: it starts
    /// the next turn.
    TurnEnd,
    /// The vendor takes mail into the running turn as it arrives. Its
    /// acceptance (a written line, an answered request) is the only proof;
    /// nothing says at which step the turn read it.
    MidTurn,
    /// The vendor proves, message by message, that the running turn consumed
    /// it at its next step, every pending message together in mailbox order.
    NextStep,
}

impl DeliveryLevel {
    pub fn name(self) -> &'static str {
        match self {
            Self::TurnEnd => "turn-end",
            Self::MidTurn => "mid-turn",
            Self::NextStep => "next-step",
        }
    }
}

/// The delivery one harness offers and what it rests on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub level: DeliveryLevel,
    /// A message an earlier process of the same session already consumed is
    /// recognised when it is handed over again, and is not applied twice.
    pub idempotent: bool,
    /// The vendor signal the level rests on, in a few words. Borrowed for an
    /// adapter's own constant; owned when it crosses a process boundary.
    pub proof: Cow<'static, str>,
    /// What the running process said about itself in its handshake (vendor,
    /// version, protocol, capabilities). `None` before a process announced
    /// itself, and for a vendor whose handshake names nothing.
    pub peer: Option<String>,
}

impl Delivery {
    /// The same delivery, as the running process announced itself.
    pub fn with_peer(mut self, peer: Option<String>) -> Self {
        self.peer = peer;
        self
    }
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
    pub delivery: Delivery,
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
    /// The whole message is in the vendor's native steering input, and the
    /// vendor has not yet proved it consumed it. It stays in the mailbox, but
    /// the next message may be written behind it at once: the vendor takes
    /// every pending input together, in order, at its next step.
    InFlight,
    /// The vendor proved this session already consumed the message before
    /// (idempotent input replayed to a new process). It is delivered and
    /// started no new work.
    Duplicate,
    /// Input cannot take the complete message yet; it stays in the mailbox.
    Deferred,
    /// The vendor asked us to wait. Not a failure and not a restart.
    Quota { reset_at_ms: i64 },
    /// The vendor refused the message for good, without applying it: no
    /// retry and no new process can make it take this message (a malformed
    /// request, an id another record holds, a code no retry clears). The
    /// loop parks it as rejected, past the cursor, and tells its sender. An
    /// adapter answers the same when the same id is offered again.
    ///
    /// `message_specific` says the reason belongs to this message alone (its
    /// size, its content): it is parked at once and never counts toward the
    /// loop's breaker. Any other reason might hold for every message, so the
    /// same one twice, or too many in one pass, fails the harness instead.
    Rejected {
        reason: String,
        message_specific: bool,
    },
}

impl DeliverOutcome {
    /// A refusal for good; see [`DeliverOutcome::Rejected`].
    pub fn rejected(reason: impl Into<String>, message_specific: bool) -> Self {
        Self::Rejected {
            reason: reason.into(),
            message_specific,
        }
    }
}

/// Whether a vendor's refusal names the size or length of the message itself
/// ("too large", "exceeds the maximum length", `inputTooLarge`): a reason that
/// belongs to that one message.
pub fn names_message_size(reason: &str) -> bool {
    let reason = reason.to_ascii_lowercase();
    [
        "too large",
        "too long",
        "toolarge",
        "toolong",
        "too_large",
        "too_long",
        "exceeds the maximum",
        "maximum length",
        "payload too large",
    ]
    .iter()
    .any(|phrase| reason.contains(phrase))
}

/// What a harness already proved about one message, read without writing
/// anything (`Harness::proven`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proof {
    /// The vendor took it with the proof the harness's level requires;
    /// `turn_finished` means the turn that contained it already ended.
    Taken { turn_finished: bool },
    /// The session already held it from an earlier process.
    Duplicate,
}

/// The harness process could not take the message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HarnessError {
    #[error("harness process is not running")]
    Dead,
    #[error("{0}")]
    Failed(String),
}

/// Calls every harness adapter implements. The last calls have defaults so a
/// scripted fake can stay a delivery double without a process.
pub trait Harness: Send {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError>;

    /// What this harness already proved about `message_id`, without writing
    /// anything and whether or not its process still runs. The host commits
    /// these before it replaces the process or waits out a quota: a proof
    /// outlives the process that gave it, but not the adapter that holds it.
    /// `deliver` answers a proven message the same way, before it looks at
    /// liveness or quota. `None` when nothing is proven yet.
    fn proven(&self, _message_id: &str) -> Option<Proof> {
        None
    }

    /// Stop the vendor process and read everything it already wrote, so
    /// every proof it gave is known to `proven` before the host replaces it.
    /// The adapter stays usable for `proven` afterwards. Called once, right
    /// before the host swaps in a new process; dropping the adapter must
    /// still stop the process when this was never called.
    fn stop(&mut self) {}

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

    /// Secret-safe process-exit evidence, never unfiltered vendor output.
    /// Kept separate from sign-in state and delivery/restart decisions.
    fn exit_diagnostic(&self) -> Option<String> {
        None
    }

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
    fn a_size_refusal_is_one_messages_own_and_a_malformed_one_is_not() {
        for reason in [
            "muse refused the message: -32002 inputTooLarge",
            "Input exceeds the maximum length of 1048576 characters.",
            "dsh session/prompt: HTTP 413: payload too large",
        ] {
            assert!(names_message_size(reason), "{reason}");
        }
        for reason in [
            "muse refused the message: -32602",
            "spiron rejected the input: wrong_session",
        ] {
            assert!(!names_message_size(reason), "{reason}");
        }
    }

    #[test]
    fn marker_is_only_the_first_line() {
        let text = marked_message("msg_1", "hello\nanyagents-message:other");
        assert_eq!(message_id_in_text(&text), Some("msg_1"));
        assert_eq!(message_id_in_text("hello"), None);
        assert_eq!(message_id_in_text("anyagents-message:"), None);
    }

    #[cfg(unix)]
    #[test]
    fn kill_child_reaps_wrapper_and_descendant_process_group() {
        use std::io::{BufRead as _, BufReader};
        use std::process::Stdio;
        let mut cmd = spawn_command(std::path::Path::new("/bin/sh"), &[]).unwrap();
        cmd.args(["-c", "sleep 30 & echo $!; wait"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = cmd.spawn().unwrap();
        let wrapper_pid = child.id();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let grandchild_pid: i32 = line.trim().parse().expect("grandchild pid");
        assert!(pgid_alive(wrapper_pid));
        kill_child(&mut child, &[]);
        assert!(matches!(child.try_wait(), Ok(Some(_))));
        assert!(
            !pgid_alive(wrapper_pid),
            "process group still alive after kill_child"
        );
        assert!(
            matches!(unix_signal::signal_target(grandchild_pid, 0), Err(3)),
            "grandchild pid {grandchild_pid} still alive after kill_child"
        );
    }
}
