//! Typed argument schemas for AnyAgents MCP tools.

use super::*;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct SendArgs {
    /// Destinations: agent names or IDs (`planner`, `agent:planner`, `agt_...`, `parent`,
    /// `children`), internal groups (`group:owners`, `group:<name>`, `group:all`, `all`),
    /// or external channel targets (`telegram:chat:<chat_id>[:<topic_id>]`,
    /// `telegram:user:<user_id>[:<message_id>]`). An authored send addressed to
    /// exactly one archived agent by name or `agt_...` ID wakes its session.
    /// Broadcasts, channel updates and system events do not. May be
    /// empty (`[]`) when `reply_to` names a mailbox reference (`anyagents:reply:<msg_id>`)
    /// or when performing a message interaction (`action`: `"react"` | `"vote"` | `"edit"` | `"delete"` | `"pin"` | `"unpin"` | `"stop_poll"`).
    #[serde(default, deserialize_with = "deserialize_send_targets")]
    pub(super) to: Vec<String>,
    /// Message text or caption (Markdown is automatically rendered as rich HTML on Telegram).
    /// Also used as new message text when `action = "edit"` or optional comment when `action = "vote"`.
    #[serde(default)]
    pub(super) text: String,
    /// Optional reply or interaction target: `anyagents:reply:<msg_id>` from the message header
    /// (answers or reacts in the same chat/topic/agent), or a plain destination address.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) reply_to: Option<String>,
    /// Optional quoted substring or message text to attach as a quote.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) quote: Option<String>,
    /// Optional media attachment (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`)
    /// by local file path, `https://` URL, or Telegram `file_id` (plus optional `title` and `performer` for `music`/`audio`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) media: Option<SendMedia>,
    /// Optional GPS coordinates or venue (`latitude`, `longitude`, optional `title` and `address`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) location: Option<SendLocation>,
    /// Optional interactive poll (`question`, `options`, optional `is_anonymous` and `allows_multiple_answers`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) poll: Option<SendPoll>,
    /// Optional interactive checklist (`title`, `tasks`, optional `business_connection_id`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) checklist: Option<SendChecklist>,
    /// Optional rich Instant View article (`title`, `content` or `url`, optional `author_name`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) article: Option<SendArticle>,
    /// Optional message interaction action (`"send"` default, or `"react"`, `"vote"`, `"check"`, `"uncheck"`,
    /// `"edit"`, `"delete"`, `"pin"`, `"unpin"`, `"stop_poll"` on an existing message identified by `target` or `reply_to`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) action: Option<String>,
    /// Target message for interactions (`anyagents:reply:msg_...`, `msg_...`, or `telegram:chat:<chat_id>:<message_id>`).
    /// Falls back to `reply_to` if omitted.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) target: Option<String>,
    /// Emoji reaction when `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, `"🎉"`, or `""` to clear).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) emoji: Option<String>,
    /// Selected poll option(s) when `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text)
    /// or checklist item numbers when `action = "check" | "uncheck"`.
    #[serde(default, deserialize_with = "deserialize_send_targets")]
    pub(super) options: Vec<String>,
    /// Optional own channel bot to post or act through (`@username` or channel id),
    /// needed only when this agent has more than one bot. Unknown bots and other
    /// agents' bots are refused.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) bot: Option<String>,
    /// Names this send. Send it again with the same value only when an earlier
    /// attempt got no answer.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) idempotency_key: Option<String>,
}

/// A destination word that names agents by their relation to the sender.
pub(super) fn relation_word(raw: &str) -> Option<&'static str> {
    let raw = raw.trim();
    let word = raw.strip_prefix("agent:").unwrap_or(raw);
    ["parent", "children", "siblings", "peers", "team", "all"]
        .into_iter()
        .find(|known| word.eq_ignore_ascii_case(known))
}

/// `send` destinations or options, without the `"null"` placeholders models
/// write for an unused list.
pub(super) fn deserialize_send_targets<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut targets = crate::api::deserialize_string_or_vec(deserializer)?;
    targets.retain(|target| !crate::api::is_null_word(target));
    Ok(targets)
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct ConversationArgs {
    /// Opaque indexed result ID for exact raw readback, never a file path.
    #[serde(default)]
    pub(super) raw_id: Option<String>,
    /// Expected whole-event SHA-256 from the result's `raw.sha256`; required with raw_id.
    #[serde(default)]
    pub(super) raw_sha256: Option<String>,
    /// Byte offset relative to the selected whole raw event/document (default 0).
    #[serde(default)]
    pub(super) raw_offset: Option<u64>,
    /// Requested bytes 1..=65536 (default 65536), clipped at the event end.
    #[serde(default)]
    pub(super) raw_length: Option<u64>,
    /// Search both sources (default), only learned memory, or only transcripts.
    #[serde(default)]
    pub(super) scope: Option<String>,
    /// Optional memory topic slug (e.g. `"architecture-decisions"`, `"billing-rules"`) to write,
    /// upsert, or delete in `<agent_root>/memory/<topic>.md` and automatically rebuild `MEMORY.md`
    /// without needing host paths. When set, `scope` defaults to `"memory"` and `agent` defaults to `self`.
    #[serde(default)]
    pub(super) topic: Option<String>,
    /// Optional 1-line description / trigger phrase for `topic` when upserting learned memory.
    #[serde(default)]
    pub(super) description: Option<String>,
    /// Markdown body to write into `<agent_root>/memory/<topic>.md` when upserting learned memory.
    #[serde(default, alias = "body", alias = "text")]
    pub(super) content: Option<String>,
    /// When `true` with `topic`, deletes `<agent_root>/memory/<topic>.md` and rebuilds `MEMORY.md`.
    #[serde(default)]
    pub(super) delete: Option<bool>,
    /// Optional lexical + structured search query (`Raw-RAG`: SQLite FTS5 BM25 with hybrid
    /// ASCII identifier + CJK bigram tokenization, exact phrase boost, and substring fallback).
    /// When set, searches authorized historical and live turns without loading the
    /// full history. Omit `query` to read recent chronological turns or filter by tool/range.
    #[serde(default, alias = "q", alias = "search")]
    pub(super) query: Option<String>,
    /// Optional array (or comma-separated list) of additional search queries / synonyms
    /// OR-combined with `query` in a single turn.
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    pub(super) queries: Vec<String>,
    /// Optional negative terms — any turn containing any of these substrings is excluded.
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    pub(super) exclude: Vec<String>,
    /// Agent name (`planner`, `builder`, `tester`, `"all"`) or `agt_...` ID whose complete
    /// two-way (`in` + `out`) conversation to read or search. Defaults to `"all"` when
    /// searching, or this agent when reading recent chronological turns. Non-admin
    /// callers can read only self and descendants; `all` means that authorized set.
    #[serde(default)]
    pub(super) agent: Option<String>,
    /// Optional peer filter (e.g. `alice`, `telegram:user:42`, `telegram:chat:-1001234567890`,
    /// `group:owners`, or `agent:beta`).
    #[serde(default)]
    pub(super) with: Option<String>,
    /// Optional direction filter (`"in"`, `"out"`, or `"all"`).
    #[serde(default)]
    pub(super) direction: Option<String>,
    /// Optional sender substring filter.
    #[serde(default)]
    pub(super) sender: Option<String>,
    /// Whether to include tool/system history (`true` by default). Set `false` to search
    /// only human/agent conversation messages without tool/schedule/sub-agent events.
    #[serde(default)]
    pub(super) include_tools: Option<bool>,
    /// When `true`, searches ONLY tool/system history (`kind = "tool"`).
    #[serde(default)]
    pub(super) only_tools: Option<bool>,
    /// Optional tool name filter (e.g. `"browser"`, `"agents"`, `"schedule"`, `"send"`).
    #[serde(default)]
    pub(super) tool_name: Option<String>,
    /// Optional lower timestamp bound in milliseconds (`timestamp_ms > since_ms`).
    #[serde(default)]
    pub(super) since_ms: Option<i64>,
    /// Optional upper timestamp bound in milliseconds (`timestamp_ms <= until_ms`).
    #[serde(default)]
    pub(super) until_ms: Option<i64>,
    /// Optional sequence range lower bound (`seq >= from_seq`).
    #[serde(default)]
    pub(super) from_seq: Option<i64>,
    /// Optional sequence range upper bound (`seq <= to_seq`).
    #[serde(default)]
    pub(super) to_seq: Option<i64>,
    /// Optional sequence range shorthand (`"10-25"` or `"seq:10-25"`).
    #[serde(default)]
    pub(super) range: Option<String>,
    /// Result ordering: `"relevance"` (default for search), `"newest"` (`timestamp_ms DESC`),
    /// or `"oldest"` (`timestamp_ms ASC`).
    #[serde(default)]
    pub(super) sort: Option<String>,
    /// Contiguous neighbor turns (`0..=10`) to include around each matched hit so causal
    /// `(Trigger -> Action -> Outcome)` chains stay intact.
    #[serde(default)]
    pub(super) radius: Option<usize>,
    /// Optional maximum number of turns to return (defaults to 20 when searching,
    /// or 50 for chronological history).
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct AgentsArgs {
    /// Action to perform:
    /// - `"list"` (default: list agents and sub-agents with optional `state` and `parent` filters)
    /// - `"inspect"` / `"status"` (inspect an agent or sub-agent's live status, schedules, and recent conversation turns)
    /// - `"spawn"` (spawn a new sub-agent under this agent, with optional `repo` / `branch` / `base` / `workdir` for an isolated Git repository of its own)
    /// - `"create"` (admin-only: create a standalone root agent with no parent)
    /// - `"describe"` (read any agent's role & responsibility `description` / `instructions`, defaulting `name` to `"self"`, or update a target's `description` / `instructions` / `role` / `scope` — mutations require the target's Parent/Ancestor agent or an Admin)
    /// - `"steer"` / `"send"` (send/steer a follow-up `prompt` into an agent or sub-agent, waking it if archived)
    /// - `"wake"` / `"resume"` (wake an archived or paused agent, with optional `prompt`)
    /// - `"reset_session"` / `"new_session"` (new session for a quiescent managed agent; preserves old pins and raw state)
    /// - `"pause"` (pause an agent)
    /// - `"archive"` (any agent, including roots and labelled children, may self-archive; preserves browser profiles; auto-reaps clean/pushed managed checkouts while preserving dirty ones)
    /// - `"delete"` / `"kill"` (permanently delete a sub-agent or agent)
    /// - `"rename"` (Parent/Ancestor or Admin: rename a descendant or agent in place while keeping its `agt_...` ID and old name as an alias)
    /// - `"role"` (Parent/Ancestor or Admin: set or clear an agent's short role label)
    /// - `"harness"` (declaratively update an agent's harness/model/profile)
    /// - `"models"` (list upstream provider profile model catalogs)
    /// - `"usage"` (structured token/cost usage aggregation; `name` is the optional
    ///   target agent or `"all"`, `include_descendants`/`subtree` selects the subtree,
    ///   `since_ms`/`until_ms`/`days` bound the window, `group_by` is one of
    ///   `agent`/`day`/`model`/`harness`. Callers with `scope == "public"`, roots and
    ///   the local operator see the whole workspace; other callers see only self plus
    ///   causal descendants, and an explicit out-of-scope target is refused.)
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Target body id when action = "redact"; `transcript:<zero-based-line>` addresses
    /// a legacy first-party output/content string in config/sylphx-transcript.jsonl.
    #[serde(default)]
    pub(super) message_id: Option<String>,
    /// Mailbox sequence instead of message_id when action = "redact".
    #[serde(default)]
    pub(super) seq: Option<i64>,
    /// Optional end-exclusive decoded-text span. Omit to tombstone the whole body.
    #[serde(default)]
    pub(super) span: Option<crate::api::RedactionSpan>,
    /// Required audit reason for redact (never repeat sensitive text here).
    #[serde(default)]
    pub(super) reason: Option<String>,
    /// Optional state filter for `list`: `"running"`, `"working"`, `"errored"`, `"idle"`, `"paused"`, `"archived"`, or `"all"` (default `"all"`).
    #[serde(default)]
    pub(super) state: Option<String>,
    /// Optional parent filter for `list`: `"self"` / `"children"` (direct sub-agents of this agent),
    /// `"subagents"` (all sub-agents across all parents), `"root"` (root agents with no parent), or an agent name/`agt_...` ID.
    #[serde(default)]
    pub(super) parent: Option<String>,
    /// Optional case-insensitive multi-token search query for `list`: every whitespace-separated token must match `name`, `id`, `role`, `description`, `workdir`, `harness`, `model`, or `aliases`.
    #[serde(default, alias = "search", alias = "filter", alias = "q")]
    pub(super) query: Option<String>,
    /// Target agent name or `agt_...` ID (`"self"`, `"all"`, or a peer/sub-agent name; optional on `spawn` where a `<caller>-sub-<id>` name is auto-generated if omitted).
    #[serde(
        default,
        alias = "agent",
        alias = "id",
        alias = "agent_id",
        alias = "target"
    )]
    pub(super) name: Option<String>,
    /// New agent name when `action = "rename"`.
    #[serde(default, alias = "to")]
    pub(super) new_name: Option<String>,
    /// Agent role template name (`"general"` first by default — built-in end-to-end execution role; or any `.agents/agents/<role>.md` template, e.g. `"general"`, `"planner"`, `"builder"`, `"tester"`) when `action` is `"spawn"`, `"create"`, `"describe"`, or `"role"`.
    #[serde(
        default,
        alias = "archetype",
        alias = "type",
        alias = "template",
        alias = "preset"
    )]
    pub(super) role: Option<String>,
    /// Set `true` to clear the description or role label when `action` is `"describe"` or `"role"`.
    #[serde(default)]
    pub(super) clear: Option<bool>,
    /// Harness catalog ID when `action` is `"spawn"`, `"create"`, or `"harness"` (`spiron`, `opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`, or `"inherit"`).
    #[serde(default)]
    pub(super) harness: Option<String>,
    /// Optional model override when `action` is `"spawn"`, `"create"`, or `"harness"`.
    #[serde(default)]
    pub(super) model: Option<String>,
    /// Optional upstream provider account profile when `action` is `"spawn"`, `"create"`, or `"harness"`.
    #[serde(default)]
    pub(super) profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"` to branch from the parent's repo) when `action` is `"spawn"` or `"create"` to automatically provision an isolated, lifecycle-managed Git repository of its own at `<data>/agents/<id>/work`.
    #[serde(default)]
    pub(super) repo: Option<String>,
    /// Optional Git branch name for the managed checkout when `repo` is provided (defaults to `agent/<name>`).
    #[serde(default)]
    pub(super) branch: Option<String>,
    /// Optional starting Git ref/branch when `repo` is provided (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    pub(super) base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    pub(super) workdir: Option<String>,
    /// Concise public role/mission summary (`description`, Layer 1 — visible to all peers and group rosters) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default)]
    pub(super) description: Option<String>,
    /// Detailed private standing instructions (`instructions`, Layer 2 — injected only into the agent's own private system prompt) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    pub(super) instructions: Option<String>,
    /// Internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default)]
    pub(super) scope: Option<String>,
    /// Optional agent lifecycle mode (`"task"` | `"standing"`, defaulting to `"task"` for sub-agents and `"standing"` for root agents) when `action` is `"spawn"`, `"create"`, or `"describe"`. `"task"` sub-agents are single-mission freelancers closed (`archive`) by their parent upon completion; `"standing"` agents stay on idle standby between assignments.
    #[serde(default)]
    pub(super) lifecycle: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage (and allows outside agents to invite it to virtual groups). Defaults to `false` (`private`) when spawning a sub-agent.
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated, e.g. `"auth-migration"`) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    pub(super) group: Option<String>,
    /// Task prompt or follow-up message when `action` is `"spawn"`, `"create"`, `"steer"`, `"send"`, or `"wake"`.
    #[serde(default, alias = "text", alias = "message")]
    pub(super) prompt: Option<String>,
    /// Suggested Telegram `@username` (or existing spare bot `@username`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(
        default,
        alias = "bot",
        alias = "suggested_username",
        alias = "username"
    )]
    pub(super) bot_username: Option<String>,
    /// Telegram bot user ID (`getManagedBotToken`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    pub(super) bot_id: Option<i64>,
    /// Optional raw Telegram bot HTTP API token when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "bot_token")]
    pub(super) token: Option<String>,
    /// Optional persistent browser profile names to grant when `action` is `"spawn"`, `"create"`, `"update"`, `"describe"`, or `"browser_profiles"` (enforces exact `child ⊆ parent` delegation; non-admin self-mutation is denied).
    #[serde(default)]
    pub(super) browser_profiles: Option<Vec<String>>,
    /// Zero-based position in the deterministically sorted list. Continue with
    /// `next_cursor` and the same query, parent and state; this is not a snapshot.
    #[serde(default, alias = "offset")]
    pub(super) cursor: Option<usize>,
    /// Page size for list (default 100, clamped to 1..=100), or recent turns for inspect (default 10).
    #[serde(default)]
    pub(super) limit: Option<usize>,
    /// For `list` (with `parent`) and `usage`: include all causal descendants of the target/parent (children, grandchildren, ...; alias `subtree`).
    #[serde(default, alias = "subtree")]
    pub(super) include_descendants: Option<bool>,
    /// For `usage`: window start, inclusive (unix ms).
    #[serde(default)]
    pub(super) since_ms: Option<i64>,
    /// For `usage`: window end, inclusive (unix ms).
    #[serde(default)]
    pub(super) until_ms: Option<i64>,
    /// For `usage`: trailing window in days, intersected with `since_ms`/`until_ms`.
    #[serde(default)]
    pub(super) days: Option<i64>,
    /// For `usage`: grouping dimension (`agent`, `day`, `model`, `harness`); for
    /// `outcomes`: `model`, `harness`, `role`, `profile`, `agent` or `skill`.
    #[serde(default)]
    pub(super) group_by: Option<String>,
    /// Optional list of named deployment secret bundles to grant when `action` is
    /// `"spawn"`, `"create"`, `"update"`, or `"secrets"`. Defaults to `[]` on
    /// create/spawn. Non-admin parents may only grant secrets they themselves hold.
    #[serde(default)]
    pub(super) secrets: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct SpawnArgs {
    /// Optional short agent name (`[a-z0-9][a-z0-9_-]*`). If omitted, a unique
    /// `<caller>-sub-<id>` name is generated automatically.
    #[serde(
        default,
        alias = "agent",
        alias = "id",
        alias = "agent_id",
        alias = "target"
    )]
    pub(super) name: Option<String>,
    /// Agent role template name (`"general"` first by default — built-in end-to-end execution role; or any `.agents/agents/<role>.md` template, e.g. `"general"`, `"planner"`, `"builder"`, `"tester"`).
    #[serde(
        default,
        alias = "archetype",
        alias = "type",
        alias = "template",
        alias = "preset"
    )]
    pub(super) role: Option<String>,
    /// Harness catalog ID (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    /// Defaults to the caller's own harness (or `claude-code` when the caller runs on `agy`).
    #[serde(default)]
    pub(super) harness: Option<String>,
    /// Optional model override (e.g. `sylphx/openai/gpt-6.1-sol#xhigh`, `claude-opus-5-5#high`).
    #[serde(default)]
    pub(super) model: Option<String>,
    /// Optional upstream provider account profile (e.g. `acct-a`, `openai/default`, `sylphx/default`).
    #[serde(default)]
    pub(super) profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"` to branch from the parent's repo) to automatically provision an isolated, lifecycle-managed Git repository of its own as the sub-agent's `workdir`.
    #[serde(default)]
    pub(super) repo: Option<String>,
    /// Optional Git branch name to create in the managed checkout when `repo` is provided (defaults to `agent/<name>`).
    #[serde(default)]
    pub(super) branch: Option<String>,
    /// Optional starting Git ref/branch for the managed checkout when `repo` is provided (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    pub(super) base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    pub(super) workdir: Option<String>,
    /// Optional concise public description (`description`, Layer 1) for the spawned agent.
    #[serde(default)]
    pub(super) description: Option<String>,
    /// Optional detailed private standing instructions (`instructions`, Layer 2) for the spawned agent.
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    pub(super) instructions: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    pub(super) scope: Option<String>,
    /// Optional agent lifecycle mode (`"task"` | `"standing"`, defaulting to `"task"` for sub-agents). `"task"` sub-agents are single-mission freelancers closed (`archive`) by their parent upon completion; `"standing"` agents stay on idle standby between assignments.
    #[serde(default)]
    pub(super) lifecycle: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage. Defaults to `false` (`private`).
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated, e.g. `"auth-migration"`) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    pub(super) group: Option<String>,
    /// Optional initial task/message to deliver immediately to the spawned agent.
    #[serde(default)]
    pub(super) prompt: Option<String>,
    /// Optional persistent browser profile names to grant to the spawned child (`child ⊆ parent`).
    #[serde(default)]
    pub(super) browser_profiles: Option<Vec<String>>,
    /// Optional list of named deployment secret bundles to grant to the spawned agent.
    /// Defaults to `[]` when omitted. Non-admin parents may only grant secrets they hold.
    #[serde(default)]
    pub(super) secrets: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct DescribeArgs {
    /// Target agent name (`"self"`, `"tester"`, `"planner"`, `"all"`, or `agt_...` ID). Defaults to `"self"`.
    #[serde(default, alias = "name", alias = "target")]
    pub(super) agent: Option<String>,
    /// Concise public role/responsibility summary (`description`, Layer 1 — visible to all peers and group rosters).
    /// Omit to read the current description.
    #[serde(default, alias = "text")]
    pub(super) description: Option<String>,
    /// Detailed private standing instructions (`instructions`, Layer 2 — injected only into the agent's own private system prompt).
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    pub(super) instructions: Option<String>,
    /// Agent role template name (`"general"` first by default — built-in end-to-end execution role; or any `.agents/agents/<role>.md` template).
    #[serde(
        default,
        alias = "archetype",
        alias = "type",
        alias = "template",
        alias = "preset"
    )]
    pub(super) role: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    pub(super) scope: Option<String>,
    /// Optional agent lifecycle mode (`"task"` | `"standing"`, defaulting to `"task"` for sub-agents). `"task"` sub-agents are single-mission freelancers closed (`archive`) by their parent upon completion; `"standing"` agents stay on idle standby between assignments.
    #[serde(default)]
    pub(super) lifecycle: Option<String>,
    /// Optional toggle (`true` or `false`) for whether a sub-agent accepts external messages from agents outside its direct parent/child lineage.
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    /// Set `true` to clear the agent's custom description/instructions.
    #[serde(default)]
    pub(super) clear: Option<bool>,
    /// Optional persistent browser profile names to grant to the target agent (`child ⊆ parent`).
    #[serde(default)]
    pub(super) browser_profiles: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct SubagentsArgs {
    /// Action to perform: `"list"` (default: list sub-agents and their live status),
    /// `"inspect"` / `"status"` (inspect a sub-agent's live status + recent conversation turns),
    /// `"spawn"` (create and start a new cross-harness sub-agent),
    /// `"steer"` / `"send"` (send/steer a follow-up prompt into a sub-agent, waking it if archived),
    /// `"wake"` / `"resume"` (wake an archived or paused sub-agent, with optional `prompt`),
    /// `"pause"` (pause a sub-agent), `"archive"` (archive a sub-agent to scale to zero),
    /// or `"delete"` / `"kill"` (permanently delete a sub-agent).
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Target sub-agent name or `agt_...` ID (for `inspect`, `steer`, `wake`, `pause`, `archive`, `delete`, or `name` for `spawn`).
    #[serde(
        default,
        alias = "id",
        alias = "name",
        alias = "agent_id",
        alias = "target"
    )]
    pub(super) agent: Option<String>,
    /// Optional state filter for `list`: `"running"`, `"paused"`, `"archived"`, or `"all"` (default `"all"`).
    #[serde(default)]
    pub(super) state: Option<String>,
    /// Optional parent filter for `list`: `"self"` (default: this agent's sub-agents), `"all"` (all sub-agents across all parents), or a parent agent name/ID.
    #[serde(default)]
    pub(super) parent: Option<String>,
    /// Optional case-insensitive multi-token search query for `list`.
    #[serde(default, alias = "search", alias = "filter", alias = "q")]
    pub(super) query: Option<String>,
    /// Agent role template name (`"general"` first by default — built-in end-to-end execution role; or any `.agents/agents/<role>.md` template, e.g. `"general"`, `"planner"`, `"builder"`, `"tester"`) when `action = "spawn"`.
    #[serde(
        default,
        alias = "archetype",
        alias = "type",
        alias = "template",
        alias = "preset"
    )]
    pub(super) role: Option<String>,
    /// Harness catalog ID when `action = "spawn"` (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    #[serde(default)]
    pub(super) harness: Option<String>,
    /// Optional model override when `action = "spawn"`.
    #[serde(default)]
    pub(super) model: Option<String>,
    /// Optional account profile when `action = "spawn"`.
    #[serde(default)]
    pub(super) profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"`) when `action = "spawn"` to automatically provision an isolated, lifecycle-managed Git repository of its own.
    #[serde(default)]
    pub(super) repo: Option<String>,
    /// Optional Git branch name for the managed checkout when `action = "spawn"` and `repo` is set (defaults to `agent/<name>`).
    #[serde(default)]
    pub(super) branch: Option<String>,
    /// Optional starting Git ref/branch when `action = "spawn"` and `repo` is set (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    pub(super) base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    pub(super) workdir: Option<String>,
    /// Optional concise public description when `action = "spawn"`.
    #[serde(default)]
    pub(super) description: Option<String>,
    /// Optional private system/role instructions when `action = "spawn"`.
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    pub(super) instructions: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    pub(super) scope: Option<String>,
    /// Optional agent lifecycle mode (`"task"` | `"standing"`, defaulting to `"task"` for sub-agents). `"task"` sub-agents are single-mission freelancers closed (`archive`) by their parent upon completion; `"standing"` agents stay on idle standby between assignments.
    #[serde(default)]
    pub(super) lifecycle: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage. Defaults to `false` (`private`).
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    pub(super) group: Option<String>,
    /// Task prompt or follow-up message when `action` is `"spawn"`, `"steer"`, `"send"`, `"wake"`, or `"resume"`.
    #[serde(default, alias = "text", alias = "message")]
    pub(super) prompt: Option<String>,
    /// Suggested Telegram `@username` (or existing spare bot `@username`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(
        default,
        alias = "bot",
        alias = "suggested_username",
        alias = "username"
    )]
    pub(super) bot_username: Option<String>,
    /// Telegram bot user ID (`getManagedBotToken`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    pub(super) bot_id: Option<i64>,
    /// Optional raw Telegram bot HTTP API token when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "bot_token")]
    pub(super) token: Option<String>,
    /// Optional persistent browser profile names to grant when `action` is `"spawn"`, `"create"`, `"update"`, `"describe"`, or `"browser_profiles"` (enforces exact `child ⊆ parent` delegation; non-admin self-mutation is denied).
    #[serde(default)]
    pub(super) browser_profiles: Option<Vec<String>>,
    /// Zero-based position in the deterministically sorted list. Continue with
    /// `next_cursor` and the same query, parent and state; this is not a snapshot.
    #[serde(default, alias = "offset")]
    pub(super) cursor: Option<usize>,
    /// Page size for list (default 100, clamped to 1..=100), or recent turns for inspect (default 10).
    #[serde(default)]
    pub(super) limit: Option<usize>,
    /// For `list`: include all causal descendants of `parent` (children, grandchildren, ...; alias `subtree`).
    #[serde(default, alias = "subtree")]
    pub(super) include_descendants: Option<bool>,
    /// Optional list of named deployment secret bundles when `action` is `"spawn"`,
    /// `"update"`, or `"secrets"`. Non-admin parents may only grant secrets they hold.
    #[serde(default)]
    pub(super) secrets: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct ArchiveArgs {
    /// Agent name or `agt_...` ID to archive (scales process to zero while keeping
    /// its permanent `agt_...` ID, mailbox, and session on disk so an intentional
    /// direct `send` or `wake` call resumes it). Defaults to this agent itself when omitted.
    #[serde(
        default,
        alias = "id",
        alias = "name",
        alias = "agent_id",
        alias = "target"
    )]
    pub(super) agent: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct WakeArgs {
    /// Agent name or `agt_...` ID to wake and resume from `archived` or `paused` state.
    #[serde(alias = "id", alias = "name", alias = "agent_id", alias = "target")]
    pub(super) agent: String,
    /// Optional follow-up prompt/message to deliver immediately after waking.
    #[serde(default, alias = "text", alias = "message")]
    pub(super) prompt: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct GroupsArgs {
    /// Action: `"list"` (list all groups and members), `"members"` (list one group's
    /// members), `"join"` (create/join an internal group), or `"leave"` (leave a group).
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Group name (e.g. `"owners"`, `"review-team"`). Required for `"join"`, `"leave"`, `"members"`.
    #[serde(default, alias = "name")]
    pub(super) group: Option<String>,
    /// Optional agent name(s) or `agt_...` ID(s) to add/remove (comma-separated, defaults to this agent). When creating a new group and adding another agent, the creator is automatically joined too.
    #[serde(default)]
    pub(super) agent: Option<String>,
    /// Optional list of agent names or `agt_...` IDs to add/remove in a single batch call.
    #[serde(
        default,
        alias = "members",
        deserialize_with = "crate::api::deserialize_string_or_vec"
    )]
    pub(super) agents: Vec<String>,
    /// Optional group visibility toggle: `false` (default = Private / Invite-Only group; participation requires membership or an invitation) or `true` (Public / Open group).
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    /// Optional flag for `action: "list"`: include archived groups (default `false`).
    #[serde(default)]
    pub(super) include_archived: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct InteractArgs {
    /// Channel/message action: `"react"` (set/clear emoji reaction on a message),
    /// `"vote"` (cast a ballot on a poll), `"edit"` (edit message text),
    /// `"delete"` (delete a message), `"pin"` (pin a message), `"unpin"` (unpin a message),
    /// or `"stop_poll"` (close an active poll).
    pub(super) action: String,
    /// Target message: `anyagents:reply:msg_...` from the message header, `msg_...`,
    /// or `telegram:chat:<chat_id>:<message_id>`.
    pub(super) target: String,
    /// Emoji reaction when `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, `"🎉"`, or `""` to clear).
    #[serde(default)]
    pub(super) emoji: Option<String>,
    /// Selected poll option(s) when `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text).
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    pub(super) options: Vec<String>,
    /// New message text when `action = "edit"`, or optional rationale/comment when `action = "vote"`.
    #[serde(default)]
    pub(super) text: Option<String>,
    /// Optional own channel bot to act through (`@username` or channel id).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) bot: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct ScheduleArgs {
    /// `"create"` (exactly one of `at`, `every`, `watch`, plus `prompt`), `"list"`, or
    /// `"cancel"` (`id` or `"all"`). `"at"`, `"every"` and `"watch"` create with that trigger.
    /// Inferred when omitted: a trigger creates, an `id` cancels, otherwise list.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) action: Option<String>,
    /// One-off wake time: relative duration (`"1m"`, `"30s"`, `"2h"`) or ISO-8601 / Unix timestamp (`"2026-10-29T16:30:00Z"`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) at: Option<String>,
    /// Recurring wake interval (`"1m"`, `"5m"`, `"15m"`, `"30m"`, `"1h"`). Each tick enters your mailbox like any other message; ticks missed while one is still undelivered coalesce into it.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) every: Option<String>,
    /// Event watch specification:
    /// - `"webhook:<topic>"` / `"event:<topic>"` (generic authenticated webhook/event trigger)
    /// - `"agent:idle:<name_or_id>"` (AnyAgents agent transitioning to idle)
    /// - `"pid:<pid>"` / `"process:<pid>"` (local background process PID exiting)
    /// - `"file:<path>"` / `"path:<path>"` (local file/directory created or modified)
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(super) watch: Option<String>,
    /// The trigger value when `action` names the trigger (`{action: "at", spec: "20m"}`).
    #[serde(
        default,
        alias = "kind",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) spec: Option<String>,
    /// Prompt delivered to the agent when the schedule or event watch fires.
    #[serde(
        default,
        alias = "text",
        alias = "message",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) prompt: Option<String>,
    /// Schedule ID (`sch_...`) or `"all"` when `action = "cancel"`.
    #[serde(
        default,
        alias = "schedule_id",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) id: Option<String>,
    /// Optional target agent name or `agt_...` ID (defaults to this agent; owners/parents can also inspect or manage a peer/child's schedules).
    #[serde(
        default,
        alias = "name",
        alias = "target",
        deserialize_with = "crate::api::absent_if_null"
    )]
    pub(super) agent: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct MemoryArgs {
    /// Learned-memory operation to perform:
    /// - `"list"` (default when no `topic`, `query`, `content`, or `delete` is passed): Read `MEMORY.md` and list all topic entries.
    /// - `"read"` (inferred when `topic` is provided without `content` or `delete`): Read `<agent_root>/memory/<topic>.md` in O(1).
    /// - `"write"` / `"upsert"` (inferred when `content` is provided): Create or update `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"delete"` (inferred when `delete: true` is provided): Delete `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"search"` (inferred when `query` is provided): Search the FTS5 learned-memory index.
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Learned-memory topic slug (`<agent_root>/memory/<topic>.md`, e.g. `"deployment-runbook"`).
    #[serde(default, alias = "slug", alias = "name")]
    pub(super) topic: Option<String>,
    /// 1-line trigger description indexed in `MEMORY.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "summary", alias = "trigger")]
    pub(super) description: Option<String>,
    /// Markdown body to write into `<agent_root>/memory/<topic>.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "body", alias = "text", alias = "markdown")]
    pub(super) content: Option<String>,
    /// Search query when `action = "search"`.
    #[serde(default, alias = "q", alias = "search")]
    pub(super) query: Option<String>,
    /// When `true`, deletes `<agent_root>/memory/<topic>.md` and rebuilds `MEMORY.md`.
    #[serde(default)]
    pub(super) delete: Option<bool>,
    /// Target agent name or `agt_...` ID (defaults to this agent itself).
    #[serde(default, alias = "target")]
    pub(super) agent: Option<String>,
    /// Maximum number of results when `action = "search"` or `"list"` (default 20).
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct InboxArgs {
    /// Optional sequence number cursor (`seq > since`).
    #[serde(default, alias = "after_seq", alias = "from_seq")]
    pub(super) since: Option<i64>,
    /// Optional maximum number of inbox messages to return.
    #[serde(default)]
    pub(super) limit: Option<usize>,
    /// When `true`, returns only unread messages.
    #[serde(default)]
    pub(super) unread: Option<bool>,
    /// Optional sender substring filter.
    #[serde(default, alias = "from")]
    pub(super) sender: Option<String>,
}

pub(super) fn generate_subagent_name(parent_name: &str) -> String {
    let raw = AgentId::generate().to_string();
    let short = raw
        .strip_prefix("agt_")
        .unwrap_or(&raw)
        .chars()
        .take(8)
        .collect::<String>()
        .to_ascii_lowercase();
    format!("{parent_name}-sub-{short}")
}

pub(super) fn send_turn_result(
    response: SendResponse,
    closing: bool,
) -> Result<serde_json::Value, String> {
    let mut result = serde_json::to_value(response).map_err(|e| e.to_string())?;
    if closing {
        result["status"] = serde_json::json!("turn_complete_no_reply_sent");
        result["turn_complete_hint"] = serde_json::json!(
            "No message was sent. End this turn immediately. Do not call send or any other tool again."
        );
    } else {
        result["turn_complete_hint"] = serde_json::json!(
            "The send request is complete. If your work is finished, end this turn without calling send again for a closing token."
        );
    }
    Ok(result)
}
