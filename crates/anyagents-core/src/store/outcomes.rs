//! Outcomes of agent work, and the skill reads beside them (ADR 0008).
//!
//! An outcome is a fact from a source the measured agent does not control: a
//! GitHub pull request's state seen by the agent's watch, or a verdict an
//! ancestor or the operator recorded. Rows are append-only and idempotent per
//! `(agent, dedupe_key)`. Which agent a shared fact counts for, and which
//! facts a verdict supersedes, is decided when a report is read, so that
//! policy can change without rewriting history.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::{Store, agent_by_id, agent_exists};
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;

const MAX_NOTE_CHARS: usize = 2_000;
const MAX_KEY_BYTES: usize = 512;
const MAX_SKILL_NAME_BYTES: usize = 128;

/// What happened. Its source, and whether it ends a piece of work, follow
/// from the kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeKind {
    /// A watched pull request merged.
    PrMerged,
    /// A watched pull request closed without merging.
    PrClosed,
    /// The configured review check failed on a pull request head.
    ReviewFailed,
    /// An ancestor or the operator accepted the agent's deliverable.
    Accepted,
    /// An ancestor or the operator asked for changes.
    Rework,
    /// An ancestor or the operator judged the deliverable unacceptable.
    Rejected,
    /// The work stopped without a deliverable. Not a judgement of its quality.
    Abandoned,
}

impl OutcomeKind {
    const ALL: [Self; 7] = [
        Self::PrMerged,
        Self::PrClosed,
        Self::ReviewFailed,
        Self::Accepted,
        Self::Rework,
        Self::Rejected,
        Self::Abandoned,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PrMerged => "pr_merged",
            Self::PrClosed => "pr_closed",
            Self::ReviewFailed => "review_failed",
            Self::Accepted => "accepted",
            Self::Rework => "rework",
            Self::Rejected => "rejected",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == text)
    }

    /// A kind a caller may record as a verdict on another agent's work.
    pub fn parse_verdict(text: &str) -> Option<Self> {
        Self::parse(&text.trim().to_ascii_lowercase()).filter(|kind| kind.is_verdict())
    }

    pub fn is_verdict(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::Rework | Self::Rejected | Self::Abandoned
        )
    }

    fn source(self) -> &'static str {
        if self.is_verdict() {
            "verdict"
        } else {
            "github"
        }
    }

    /// Ends a piece of work; review failures and rework requests do not.
    fn is_terminal(self) -> bool {
        !self.is_rework()
    }

    fn is_success(self) -> bool {
        matches!(self, Self::PrMerged | Self::Accepted)
    }

    fn is_failure(self) -> bool {
        matches!(self, Self::PrClosed | Self::Rejected)
    }

    fn is_rework(self) -> bool {
        matches!(self, Self::ReviewFailed | Self::Rework)
    }
}

/// One fact to record. Harness, model and role are read from the agent row
/// in the recording transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOutcome {
    pub agent_id: AgentId,
    pub kind: OutcomeKind,
    /// What the fact is about: `github:pr:<owner>/<repo>#<n>` or `agent:<id>`.
    pub subject: String,
    /// Recording the same key for the same agent again changes nothing.
    pub dedupe_key: String,
    /// `system`, `operator` or `agent:<name>`.
    pub actor: String,
    /// When the agent took the subject on, such as its watch's creation.
    pub claimed_at_ms: i64,
    pub observed_at_ms: i64,
    /// The credential profile the agent was bound to.
    pub profile: Option<String>,
    pub note: Option<String>,
}

impl NewOutcome {
    /// A pull request state seen by `agent_id`'s watch. The key names the
    /// pull request, the kind and (for a review failure) the head, never the
    /// agent, so every watcher of one pull request records the same fact.
    #[allow(clippy::too_many_arguments)]
    pub fn pull_request(
        agent_id: AgentId,
        owner: &str,
        repo: &str,
        number: u64,
        kind: OutcomeKind,
        head: Option<&str>,
        claimed_at_ms: i64,
        observed_at_ms: i64,
        profile: Option<String>,
    ) -> Self {
        let subject = format!(
            "github:pr:{}/{}#{number}",
            owner.to_ascii_lowercase(),
            repo.to_ascii_lowercase()
        );
        let dedupe_key = match head.filter(|head| !head.is_empty()) {
            Some(head) if kind == OutcomeKind::ReviewFailed => {
                format!("{subject}:{}@{head}", kind.as_str())
            }
            _ => format!("{subject}:{}", kind.as_str()),
        };
        Self {
            agent_id,
            kind,
            subject,
            dedupe_key,
            actor: "system".to_string(),
            claimed_at_ms,
            observed_at_ms,
            profile,
            note: None,
        }
    }

    /// A verdict on `agent_id`'s work. Every call is a new event: a parent
    /// may ask for rework twice, then accept.
    pub fn verdict(
        agent_id: AgentId,
        kind: OutcomeKind,
        actor: String,
        note: Option<String>,
        observed_at_ms: i64,
        profile: Option<String>,
    ) -> Self {
        Self {
            subject: format!("agent:{agent_id}"),
            dedupe_key: format!("verdict:{}", uuid::Uuid::now_v7()),
            agent_id,
            kind,
            actor,
            claimed_at_ms: observed_at_ms,
            observed_at_ms,
            profile,
            note,
        }
    }
}

/// One stored fact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub id: String,
    pub agent_id: AgentId,
    pub kind: OutcomeKind,
    pub subject: String,
    pub dedupe_key: String,
    pub actor: String,
    pub harness: String,
    pub model: Option<String>,
    pub role: Option<String>,
    pub profile: Option<String>,
    pub note: Option<String>,
    pub claimed_at_ms: i64,
    pub observed_at_ms: i64,
}

const OUTCOME_COLS: &str = "id, agent_id, kind, subject, dedupe_key, actor, harness, model, role, \
     profile, note, claimed_at_ms, observed_at_ms";

/// A row whose kind this binary does not know reads as `None`, so a newer
/// writer never breaks an older reader.
fn outcome_from_row(row: &Row<'_>) -> Result<Option<Outcome>, Error> {
    let kind: String = row.get(2)?;
    let Some(kind) = OutcomeKind::parse(&kind) else {
        return Ok(None);
    };
    let agent: String = row.get(1)?;
    Ok(Some(Outcome {
        id: row.get(0)?,
        agent_id: AgentId::parse(&agent)?,
        kind,
        subject: row.get(3)?,
        dedupe_key: row.get(4)?,
        actor: row.get(5)?,
        harness: row.get(6)?,
        model: row.get(7)?,
        role: row.get(8)?,
        profile: row.get(9)?,
        note: row.get(10)?,
        claimed_at_ms: row.get(11)?,
        observed_at_ms: row.get(12)?,
    }))
}

fn bounded(field: &str, value: &str) -> Result<(), Error> {
    if value.trim().is_empty() || value.len() > MAX_KEY_BYTES {
        return Err(Error::Message(format!(
            "outcome {field} must be 1 to {MAX_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

/// The relative path of a file inside one skill directory, as the `skills`
/// tool accepts it.
fn valid_skill_file(file: &str) -> bool {
    let path = std::path::Path::new(file);
    !file.is_empty()
        && file.len() <= MAX_KEY_BYTES
        && !path.is_absolute()
        && path
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
}

impl Store {
    /// Record one fact. Returns the stored row and whether this call added
    /// it: a repeated `(agent, dedupe_key)` returns the first row unchanged.
    pub fn record_outcome(&self, new: NewOutcome) -> Result<(Outcome, bool), Error> {
        bounded("subject", &new.subject)?;
        bounded("dedupe_key", &new.dedupe_key)?;
        bounded("actor", &new.actor)?;
        let note = new
            .note
            .as_deref()
            .map(str::trim)
            .filter(|note| !note.is_empty())
            .map(|note| {
                crate::masking::mask_known_secrets(note)
                    .chars()
                    .take(MAX_NOTE_CHARS)
                    .collect::<String>()
            });
        let profile = new
            .profile
            .as_deref()
            .map(str::trim)
            .filter(|profile| !profile.is_empty());
        let id = format!("out_{}", uuid::Uuid::now_v7().simple());
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let agent = agent_by_id(&tx, &new.agent_id)?;
            let inserted = tx.execute(
                "INSERT INTO outcomes (id, agent_id, kind, source, subject, dedupe_key, actor,
                                       harness, model, role, profile, note, claimed_at_ms,
                                       observed_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                 ON CONFLICT (agent_id, dedupe_key) DO NOTHING",
                params![
                    id,
                    agent.id.as_str(),
                    new.kind.as_str(),
                    new.kind.source(),
                    new.subject,
                    new.dedupe_key,
                    new.actor,
                    agent.harness,
                    agent.model,
                    agent.role,
                    profile,
                    note,
                    new.claimed_at_ms,
                    new.observed_at_ms,
                ],
            )? == 1;
            let stored = {
                let mut statement = tx.prepare(&format!(
                    "SELECT {OUTCOME_COLS} FROM outcomes WHERE agent_id = ?1 AND dedupe_key = ?2"
                ))?;
                let mut rows = statement.query(params![agent.id.as_str(), new.dedupe_key])?;
                match rows.next()? {
                    Some(row) => outcome_from_row(row)?,
                    None => None,
                }
            }
            .ok_or_else(|| Error::Message("a newer binary recorded this outcome".to_string()))?;
            tx.commit()?;
            Ok((stored, inserted))
        })
    }

    /// Record that `agent_id` read `file` of skill `skill`, whose bytes had
    /// digest `sha256`.
    pub fn record_skill_read(
        &self,
        agent_id: &AgentId,
        skill: &str,
        file: &str,
        sha256: &str,
    ) -> Result<(), Error> {
        if skill.is_empty()
            || skill.len() > MAX_SKILL_NAME_BYTES
            || skill.starts_with('.')
            || skill.contains(['/', '\\'])
        {
            return Err(Error::Message(format!("invalid skill name `{skill}`")));
        }
        if !valid_skill_file(file) {
            return Err(Error::Message(format!("invalid skill file path `{file}`")));
        }
        if sha256.len() != 64
            || !sha256
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Error::Message(
                "skill read digest must be 64 lowercase hex digits".to_string(),
            ));
        }
        let at = now_ms();
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            agent_exists(&tx, agent_id)?;
            tx.execute(
                "INSERT INTO skill_reads (agent_id, skill, file, sha256, read_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_id.as_str(), skill, file, sha256, at],
            )?;
            tx.commit()?;
            Ok(())
        })
    }

    /// Successes, failures, recovery and cost per success for `agent_ids`, for
    /// outcomes observed in `[since_ms, until_ms]`. See [`OutcomeReport`] for
    /// how facts are counted and how usage is attributed to them.
    pub fn outcome_report(
        &self,
        agent_ids: &[AgentId],
        since_ms: Option<i64>,
        until_ms: i64,
        group_by: OutcomeGroupBy,
    ) -> Result<OutcomeReport, Error> {
        let conn = self.read_conn();
        // One snapshot: facts, usage and skill reads agree with each other.
        let snapshot = conn.unchecked_transaction()?;
        build_report(&snapshot, agent_ids, since_ms, until_ms, group_by)
    }
}

/// How an outcome report groups agents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeGroupBy {
    Harness,
    /// Harness and model together: the same model behaves differently in
    /// different harnesses.
    Model,
    Role,
    Profile,
    Agent,
    /// Each skill an agent read for the work; one agent can be in several
    /// rows, and agents that read none share the row whose `skill` is null.
    Skill,
}

impl OutcomeGroupBy {
    /// Parse a caller-supplied grouping. Empty text means harness and model.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.trim().to_ascii_lowercase().as_str() {
            "" | "model" => Ok(Self::Model),
            "harness" => Ok(Self::Harness),
            "role" => Ok(Self::Role),
            "profile" => Ok(Self::Profile),
            "agent" => Ok(Self::Agent),
            "skill" => Ok(Self::Skill),
            other => Err(format!(
                "unsupported group_by `{other}`: expected model, harness, role, profile, agent or skill"
            )),
        }
    }
}

/// Usage records summed for a report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeCost {
    /// Usage records: one per vendor turn.
    pub turns: i64,
    /// Records that carried a vendor-reported cost. `reported_cost_micros`
    /// sums only these, so it is a full cost only when this equals `turns`.
    pub turns_with_cost: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: i64,
}

impl OutcomeCost {
    fn add(&mut self, other: &Self) {
        self.turns += other.turns;
        self.turns_with_cost += other.turns_with_cost;
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        self.reported_cost_micros += other.reported_cost_micros;
    }

    fn from_row(row: &Row<'_>, at: usize) -> rusqlite::Result<Self> {
        Ok(Self {
            turns: row.get(at)?,
            turns_with_cost: row.get(at + 1)?,
            input_tokens: row.get(at + 2)?,
            output_tokens: row.get(at + 3)?,
            cache_read_tokens: row.get(at + 4)?,
            cache_write_tokens: row.get(at + 5)?,
            reported_cost_micros: row.get(at + 6)?,
        })
    }
}

const COST_SUMS: &str = "COUNT(*), COUNT(reported_cost_micros), COALESCE(SUM(input_tokens), 0), \
     COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cache_read_tokens), 0), \
     COALESCE(SUM(cache_write_tokens), 0), COALESCE(SUM(reported_cost_micros), 0)";

/// One report row. Key fields that are not part of the grouping are null.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeGroup {
    pub harness: Option<String>,
    pub model: Option<String>,
    pub role: Option<String>,
    pub profile: Option<String>,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub skill: Option<String>,
    /// Agents with at least one counted fact in the window.
    pub agents: u64,
    /// Counted terminal outcomes that are merges or acceptances.
    pub succeeded: u64,
    /// Counted terminal outcomes that are closed pull requests or rejections.
    pub failed: u64,
    pub abandoned: u64,
    /// Terminal outcomes whose work had a review failure or rework request
    /// before it ended.
    pub reworked: u64,
    /// The `reworked` outcomes that were successes: recovery after failure.
    pub recovered: u64,
    /// Every fact in the window by kind, including evidence a verdict superseded.
    pub events: BTreeMap<String, u64>,
    /// Agents whose attributed usage ran on more than one harness and model.
    pub mixed_agents: u64,
    /// Usage attributed to the counted terminal outcomes in the window.
    pub cost: OutcomeCost,
    /// `cost.reported_cost_micros / succeeded`, only when there is a success
    /// and every attributed turn reported a cost. Otherwise unknown, never 0.
    pub cost_per_success_micros: Option<i64>,
    /// Median time from the previous terminal outcome (or the agent's
    /// creation) to a success.
    pub median_ms_to_success: Option<i64>,
}

/// Outcomes and the usage behind them, grouped.
///
/// Counting rules, applied when the report is read:
///
/// - One GitHub fact seen by several watchers counts once, for the agent
///   whose watch was created first (the claimant).
/// - An agent with a terminal verdict (accepted, rejected or abandoned) is
///   counted by its verdicts; its pull request facts stay evidence. An agent
///   without one is counted by its pull requests.
/// - The cost of a terminal outcome is the agent's usage since its previous
///   counted terminal outcome. Every usage record therefore belongs to at
///   most one outcome, across any sequence of windows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeReport {
    pub group_by: OutcomeGroupBy,
    pub since_ms: Option<i64>,
    pub until_ms: i64,
    pub groups: Vec<OutcomeGroup>,
    /// Usage recorded in the window that no counted terminal outcome covers:
    /// work still in progress, and work no outcome measures. Shown beside
    /// every ratio so spend outside measurement stays visible.
    pub unattributed: OutcomeCost,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    harness: Option<String>,
    model: Option<String>,
    role: Option<String>,
    profile: Option<String>,
    agent: Option<(String, String)>,
    skill: Option<String>,
}

/// One agent's contribution to the window.
#[derive(Default)]
struct AgentTally {
    succeeded: u64,
    failed: u64,
    abandoned: u64,
    reworked: u64,
    recovered: u64,
    events: BTreeMap<String, u64>,
    mixed: bool,
    cost: OutcomeCost,
    to_success: Vec<i64>,
}

#[derive(Default)]
struct Accum {
    agents: u64,
    mixed_agents: u64,
    tally: AgentTally,
}

fn build_report(
    conn: &Connection,
    agent_ids: &[AgentId],
    since_ms: Option<i64>,
    until_ms: i64,
    group_by: OutcomeGroupBy,
) -> Result<OutcomeReport, Error> {
    let since = since_ms.unwrap_or(i64::MIN);
    let allowed: HashSet<&str> = agent_ids.iter().map(AgentId::as_str).collect();
    let mut facts: Vec<Outcome> = Vec::new();
    {
        let mut statement = conn.prepare(&format!(
            "SELECT {OUTCOME_COLS} FROM outcomes WHERE observed_at_ms <= ?1
             ORDER BY observed_at_ms, id"
        ))?;
        let mut rows = statement.query([until_ms])?;
        while let Some(row) = rows.next()? {
            facts.extend(outcome_from_row(row)?);
        }
    }
    // The claimant of each GitHub fact, chosen over every agent: who may read
    // the report changes what it shows, never whom a fact counts for.
    let mut claimant: HashMap<&str, (i64, &str)> = HashMap::new();
    for fact in facts.iter().filter(|fact| !fact.kind.is_verdict()) {
        let candidate = (fact.claimed_at_ms, fact.agent_id.as_str());
        claimant
            .entry(fact.dedupe_key.as_str())
            .and_modify(|held| *held = (*held).min(candidate))
            .or_insert(candidate);
    }
    let mut by_agent: BTreeMap<&str, Vec<&Outcome>> = BTreeMap::new();
    for fact in &facts {
        let counted = fact.kind.is_verdict()
            || claimant.get(fact.dedupe_key.as_str())
                == Some(&(fact.claimed_at_ms, fact.agent_id.as_str()));
        if counted && allowed.contains(fact.agent_id.as_str()) {
            by_agent
                .entry(fact.agent_id.as_str())
                .or_default()
                .push(fact);
        }
    }

    let mut groups: BTreeMap<GroupKey, Accum> = BTreeMap::new();
    let mut unattributed = OutcomeCost::default();
    let mut attributed: HashSet<&str> = HashSet::new();
    let mut agent_row = conn.prepare("SELECT name, created_at_ms FROM agents WHERE id = ?1")?;
    for (agent, agent_facts) in &by_agent {
        let in_window = |fact: &&&Outcome| fact.observed_at_ms >= since;
        let Some(latest) = agent_facts.iter().rfind(in_window) else {
            continue;
        };
        let Some((name, created_at)) = agent_row
            .query_row([agent], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?
        else {
            continue;
        };
        let by_verdict = agent_facts
            .iter()
            .any(|fact| fact.kind.is_verdict() && fact.kind.is_terminal());
        let terminal: Vec<&Outcome> = agent_facts
            .iter()
            .copied()
            .filter(|fact| fact.kind.is_terminal() && fact.kind.is_verdict() == by_verdict)
            .collect();
        let mut tally = AgentTally::default();
        for fact in agent_facts.iter().filter(in_window) {
            *tally
                .events
                .entry(fact.kind.as_str().to_string())
                .or_default() += 1;
        }
        let mut previous: Option<i64> = None;
        let mut covered_from = i64::MIN;
        let mut covered_to: Option<i64> = None;
        let mut snapshot: &Outcome = latest;
        for end in terminal {
            if end.observed_at_ms < since {
                previous = Some(end.observed_at_ms);
                covered_from = end.observed_at_ms;
                continue;
            }
            let start = previous.unwrap_or(i64::MIN);
            let had_rework = agent_facts.iter().any(|fact| {
                fact.kind.is_rework()
                    && fact.observed_at_ms > start
                    && fact.observed_at_ms <= end.observed_at_ms
            });
            tally.reworked += u64::from(had_rework);
            if end.kind.is_success() {
                tally.succeeded += 1;
                tally.recovered += u64::from(had_rework);
                tally
                    .to_success
                    .push(end.observed_at_ms - start.max(created_at));
            } else if end.kind.is_failure() {
                tally.failed += 1;
            } else {
                tally.abandoned += 1;
            }
            previous = Some(end.observed_at_ms);
            covered_to = Some(end.observed_at_ms);
            snapshot = end;
        }
        let read_until = match covered_to {
            Some(to) => {
                let (cost, runs) = usage_between(conn, agent, covered_from, to)?;
                tally.cost = cost;
                tally.mixed = runs > 1;
                unattributed.add(&usage_between(conn, agent, to, until_ms)?.0);
                attributed.insert(*agent);
                to
            }
            None => until_ms,
        };
        let skills = if group_by == OutcomeGroupBy::Skill {
            skills_read(conn, agent, covered_from, read_until)?
        } else {
            BTreeSet::new()
        };
        for key in group_keys(group_by, snapshot, agent, &name, skills) {
            let accum = groups.entry(key).or_default();
            accum.agents += 1;
            accum.mixed_agents += u64::from(tally.mixed);
            merge(&mut accum.tally, &tally);
        }
    }
    // Agents with no counted terminal outcome: their whole window is unattributed.
    let mut window = conn.prepare(&format!(
        "SELECT agent_id, {COST_SUMS} FROM usage_records
         WHERE recorded_at_ms >= ?1 AND recorded_at_ms <= ?2 GROUP BY agent_id"
    ))?;
    let mut rows = window.query(params![since, until_ms])?;
    while let Some(row) = rows.next()? {
        let agent: String = row.get(0)?;
        if allowed.contains(agent.as_str()) && !attributed.contains(agent.as_str()) {
            unattributed.add(&OutcomeCost::from_row(row, 1)?);
        }
    }

    let groups = groups
        .into_iter()
        .map(|(key, accum)| finish(key, accum))
        .collect();
    Ok(OutcomeReport {
        group_by,
        since_ms,
        until_ms,
        groups,
        unattributed,
    })
}

/// Usage of `agent` recorded in `(after, through]`, and how many distinct
/// harness and model pairs it ran on.
fn usage_between(
    conn: &Connection,
    agent: &str,
    after: i64,
    through: i64,
) -> Result<(OutcomeCost, i64), Error> {
    let mut statement = conn.prepare_cached(&format!(
        "SELECT {COST_SUMS}, COUNT(DISTINCT harness || char(31) || COALESCE(model, ''))
         FROM usage_records
         WHERE agent_id = ?1 AND recorded_at_ms > ?2 AND recorded_at_ms <= ?3"
    ))?;
    Ok(statement.query_row(params![agent, after, through], |row| {
        Ok((OutcomeCost::from_row(row, 0)?, row.get(7)?))
    })?)
}

fn skills_read(
    conn: &Connection,
    agent: &str,
    after: i64,
    through: i64,
) -> Result<BTreeSet<String>, Error> {
    let mut statement = conn.prepare_cached(
        "SELECT DISTINCT skill FROM skill_reads
         WHERE agent_id = ?1 AND read_at_ms > ?2 AND read_at_ms <= ?3",
    )?;
    let rows = statement.query_map(params![agent, after, through], |row| row.get(0))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn group_keys(
    group_by: OutcomeGroupBy,
    snapshot: &Outcome,
    agent: &str,
    name: &str,
    skills: BTreeSet<String>,
) -> Vec<GroupKey> {
    let key = match group_by {
        OutcomeGroupBy::Harness => GroupKey {
            harness: Some(snapshot.harness.clone()),
            ..GroupKey::default()
        },
        OutcomeGroupBy::Model => GroupKey {
            harness: Some(snapshot.harness.clone()),
            model: snapshot.model.clone(),
            ..GroupKey::default()
        },
        OutcomeGroupBy::Role => GroupKey {
            role: snapshot.role.clone(),
            ..GroupKey::default()
        },
        OutcomeGroupBy::Profile => GroupKey {
            profile: snapshot.profile.clone(),
            ..GroupKey::default()
        },
        OutcomeGroupBy::Agent => GroupKey {
            agent: Some((agent.to_string(), name.to_string())),
            ..GroupKey::default()
        },
        OutcomeGroupBy::Skill if skills.is_empty() => GroupKey::default(),
        OutcomeGroupBy::Skill => {
            return skills
                .into_iter()
                .map(|skill| GroupKey {
                    skill: Some(skill),
                    ..GroupKey::default()
                })
                .collect();
        }
    };
    vec![key]
}

fn merge(into: &mut AgentTally, from: &AgentTally) {
    into.succeeded += from.succeeded;
    into.failed += from.failed;
    into.abandoned += from.abandoned;
    into.reworked += from.reworked;
    into.recovered += from.recovered;
    for (kind, count) in &from.events {
        *into.events.entry(kind.clone()).or_default() += count;
    }
    into.cost.add(&from.cost);
    into.to_success.extend_from_slice(&from.to_success);
}

fn finish(key: GroupKey, accum: Accum) -> OutcomeGroup {
    let Accum {
        agents,
        mixed_agents,
        mut tally,
    } = accum;
    tally.to_success.sort_unstable();
    let median =
        (!tally.to_success.is_empty()).then(|| tally.to_success[(tally.to_success.len() - 1) / 2]);
    let cost = tally.cost;
    let cost_per_success =
        (tally.succeeded > 0 && cost.turns > 0 && cost.turns_with_cost == cost.turns)
            .then(|| cost.reported_cost_micros / tally.succeeded as i64);
    let (agent_id, agent_name) = key.agent.unzip();
    OutcomeGroup {
        harness: key.harness,
        model: key.model,
        role: key.role,
        profile: key.profile,
        agent_id,
        agent_name,
        skill: key.skill,
        agents,
        succeeded: tally.succeeded,
        failed: tally.failed,
        abandoned: tally.abandoned,
        reworked: tally.reworked,
        recovered: tally.recovered,
        events: tally.events,
        mixed_agents,
        cost,
        cost_per_success_micros: cost_per_success,
        median_ms_to_success: median,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::UsageId;
    use crate::{Agent, NewAgent};

    fn open() -> (tempfile::TempDir, Store) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    fn agent(store: &Store, name: &str, harness: &str, model: &str) -> Agent {
        let mut new = NewAgent::new(name, harness);
        new.model = Some(model.to_string());
        new.role = Some("builder".to_string());
        store.create_agent(new).unwrap()
    }

    /// A usage record at an exact time; `record_usage` stamps the clock.
    fn usage(store: &Store, agent: &Agent, at: i64, cost: Option<i64>) {
        store
            .conn()
            .execute(
                "INSERT INTO usage_records (id, agent_id, harness, model, input_tokens,
                     output_tokens, cache_read_tokens, cache_write_tokens, reported_cost_micros,
                     estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms)
                 VALUES (?1, ?2, ?3, ?4, 3, 2, 0, 0, ?5, NULL, 'subscription', NULL,
                         '2026-11-13', ?6)",
                params![
                    UsageId::generate().as_str(),
                    agent.id.as_str(),
                    agent.harness,
                    agent.model,
                    cost,
                    at
                ],
            )
            .unwrap();
    }

    fn pull(agent: &Agent, number: u64, kind: OutcomeKind, claimed: i64, at: i64) -> NewOutcome {
        NewOutcome::pull_request(
            agent.id.clone(),
            "Example-Org",
            "Repo",
            number,
            kind,
            Some("abc123"),
            claimed,
            at,
            Some("anthropic/team".to_string()),
        )
    }

    fn verdict(store: &Store, agent: &Agent, kind: OutcomeKind, at: i64) {
        store
            .record_outcome(NewOutcome::verdict(
                agent.id.clone(),
                kind,
                "operator".to_string(),
                Some("checked against the spec".to_string()),
                at,
                None,
            ))
            .unwrap();
    }

    fn report(
        store: &Store,
        agents: &[&Agent],
        since: Option<i64>,
        until: i64,
        by: OutcomeGroupBy,
    ) -> OutcomeReport {
        let ids: Vec<AgentId> = agents.iter().map(|agent| agent.id.clone()).collect();
        store.outcome_report(&ids, since, until, by).unwrap()
    }

    fn row<'a>(report: &'a OutcomeReport, name: &str) -> &'a OutcomeGroup {
        report
            .groups
            .iter()
            .find(|group| group.agent_name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("no row for {name}"))
    }

    #[test]
    fn a_repeated_fact_is_one_row_and_snapshots_what_the_agent_ran() {
        let (_dir, store) = open();
        let worker = agent(&store, "worker", "claude-code", "claude-opus-5-5#high");
        let lead = agent(&store, "lead", "spiron", "gpt-6.1-sol#high");
        let t = worker.created_at_ms;
        let (first, added) = store
            .record_outcome(pull(&worker, 7, OutcomeKind::PrMerged, t, t + 10))
            .unwrap();
        assert!(added);
        assert_eq!(first.subject, "github:pr:example-org/repo#7");
        assert_eq!(first.dedupe_key, "github:pr:example-org/repo#7:pr_merged");
        assert_eq!(first.harness, "claude-code");
        assert_eq!(first.model.as_deref(), Some("claude-opus-5-5#high"));
        assert_eq!(first.role.as_deref(), Some("builder"));
        assert_eq!(first.profile.as_deref(), Some("anthropic/team"));
        assert_eq!(first.actor, "system");
        let (again, added) = store
            .record_outcome(pull(&worker, 7, OutcomeKind::PrMerged, t, t + 99))
            .unwrap();
        assert!(!added);
        assert_eq!(again, first);
        // Another watcher records the same fact for itself.
        let (theirs, added) = store
            .record_outcome(pull(&lead, 7, OutcomeKind::PrMerged, t + 5, t + 10))
            .unwrap();
        assert!(added);
        assert_eq!(theirs.dedupe_key, first.dedupe_key);
        assert_ne!(theirs.id, first.id);
        // A review failure is keyed by the head it failed on.
        let (review, _) = store
            .record_outcome(pull(&worker, 7, OutcomeKind::ReviewFailed, t, t + 5))
            .unwrap();
        assert_eq!(
            review.dedupe_key,
            "github:pr:example-org/repo#7:review_failed@abc123"
        );
    }

    #[test]
    fn verdicts_are_events_and_inputs_are_bounded() {
        let (_dir, store) = open();
        let worker = agent(&store, "worker", "codex", "gpt-6.1");
        let t = worker.created_at_ms;
        verdict(&store, &worker, OutcomeKind::Rework, t + 1);
        verdict(&store, &worker, OutcomeKind::Rework, t + 2);
        let count: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM outcomes WHERE agent_id = ?1 AND kind = 'rework'
                 AND source = 'verdict' AND subject = ?2",
                params![worker.id.as_str(), format!("agent:{}", worker.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            OutcomeKind::parse_verdict(" Accepted "),
            Some(OutcomeKind::Accepted)
        );
        assert_eq!(OutcomeKind::parse_verdict("pr_merged"), None);
        let mut empty = pull(&worker, 1, OutcomeKind::PrMerged, t, t);
        empty.subject = " ".to_string();
        assert!(store.record_outcome(empty).is_err());

        let sha = "a".repeat(64);
        store
            .record_skill_read(&worker.id, "prove-it", "SKILL.md", &sha)
            .unwrap();
        store
            .record_skill_read(&worker.id, "prove-it", "references/a.md", &sha)
            .unwrap();
        for (skill, file, digest) in [
            ("../x", "SKILL.md", sha.as_str()),
            ("a/b", "SKILL.md", sha.as_str()),
            (".hidden", "SKILL.md", sha.as_str()),
            ("ok", "../SKILL.md", sha.as_str()),
            ("ok", "/etc/passwd", sha.as_str()),
            ("ok", "SKILL.md", "ABC"),
            ("ok", "SKILL.md", &"A".repeat(64)),
        ] {
            assert!(
                store
                    .record_skill_read(&worker.id, skill, file, digest)
                    .is_err(),
                "{skill} {file} {digest}"
            );
        }
        let unknown = AgentId::generate();
        assert!(
            store
                .record_skill_read(&unknown, "prove-it", "SKILL.md", &sha)
                .is_err()
        );
    }

    #[test]
    fn a_shared_pull_request_counts_once_for_the_agent_that_watched_it_first() {
        let (_dir, store) = open();
        let worker = agent(&store, "worker", "claude-code", "opus");
        let lead = agent(&store, "lead", "spiron", "sol");
        let t = worker.created_at_ms.max(lead.created_at_ms);
        usage(&store, &worker, t + 100, Some(10));
        usage(&store, &lead, t + 100, Some(1_000));
        // The lead's watch fires first, but the worker watched first.
        store
            .record_outcome(pull(&lead, 3, OutcomeKind::PrMerged, t + 50, t + 500))
            .unwrap();
        store
            .record_outcome(pull(&worker, 3, OutcomeKind::PrMerged, t + 20, t + 500))
            .unwrap();
        let all = report(
            &store,
            &[&worker, &lead],
            None,
            t + 1_000,
            OutcomeGroupBy::Agent,
        );
        assert_eq!(all.groups.len(), 1);
        let worked = row(&all, "worker");
        assert_eq!((worked.agents, worked.succeeded), (1, 1));
        assert_eq!(worked.cost.reported_cost_micros, 10);
        assert_eq!(worked.cost_per_success_micros, Some(10));
        // The lead's spend is outside every counted outcome.
        assert_eq!(all.unattributed.reported_cost_micros, 1_000);
        // A reader who sees only the lead still sees the fact counted elsewhere.
        let lead_only = report(&store, &[&lead], None, t + 1_000, OutcomeGroupBy::Agent);
        assert!(lead_only.groups.is_empty());
        assert_eq!(lead_only.unattributed.reported_cost_micros, 1_000);
    }

    #[test]
    fn verdicts_supersede_pull_requests_and_recovery_is_counted() {
        let (_dir, store) = open();
        let accepted = agent(&store, "accepted", "claude-code", "opus");
        let merged = agent(&store, "merged", "claude-code", "opus");
        let rejected = agent(&store, "rejected", "spiron", "sol");
        let recovered = agent(&store, "recovered", "claude-code", "opus");
        let t = recovered.created_at_ms;
        store
            .record_outcome(pull(&accepted, 1, OutcomeKind::PrMerged, t, t + 100))
            .unwrap();
        verdict(&store, &accepted, OutcomeKind::Accepted, t + 200);
        store
            .record_outcome(pull(&merged, 2, OutcomeKind::PrMerged, t, t + 100))
            .unwrap();
        verdict(&store, &rejected, OutcomeKind::Rework, t + 50);
        verdict(&store, &rejected, OutcomeKind::Rejected, t + 90);
        store
            .record_outcome(pull(&recovered, 4, OutcomeKind::ReviewFailed, t, t + 30))
            .unwrap();
        store
            .record_outcome(pull(&recovered, 4, OutcomeKind::PrMerged, t, t + 80))
            .unwrap();
        let everyone = [&accepted, &merged, &rejected, &recovered];
        let by_agent = report(&store, &everyone, None, t + 1_000, OutcomeGroupBy::Agent);
        let a = row(&by_agent, "accepted");
        assert_eq!((a.succeeded, a.failed), (1, 0));
        assert_eq!(a.events.get("pr_merged"), Some(&1));
        assert_eq!(a.events.get("accepted"), Some(&1));
        assert_eq!(row(&by_agent, "merged").succeeded, 1);
        let r = row(&by_agent, "rejected");
        assert_eq!(
            (r.succeeded, r.failed, r.reworked, r.recovered),
            (0, 1, 1, 0)
        );
        let v = row(&by_agent, "recovered");
        assert_eq!((v.succeeded, v.reworked, v.recovered), (1, 1, 1));
        assert_eq!(v.median_ms_to_success, Some(80));

        let by_model = report(&store, &everyone, None, t + 1_000, OutcomeGroupBy::Model);
        assert_eq!(by_model.groups.len(), 2);
        let claude = by_model
            .groups
            .iter()
            .find(|group| group.harness.as_deref() == Some("claude-code"))
            .unwrap();
        assert_eq!(claude.model.as_deref(), Some("opus"));
        assert_eq!(
            (claude.agents, claude.succeeded, claude.recovered),
            (3, 3, 1)
        );
        // No usage was recorded, so cost per success is unknown, not zero.
        assert_eq!(claude.cost_per_success_micros, None);
        assert!(claude.agent_id.is_none() && claude.role.is_none());
    }

    #[test]
    fn cost_runs_from_the_previous_terminal_outcome_and_is_conserved_across_windows() {
        let (_dir, store) = open();
        let worker = agent(&store, "worker", "claude-code", "opus");
        let partial = agent(&store, "partial", "spiron", "sol");
        let t = worker.created_at_ms.max(partial.created_at_ms);
        for (offset, cost) in [(100, 10), (600, 20), (1_200, 40), (1_600, 80)] {
            usage(&store, &worker, t + offset, Some(cost));
        }
        store
            .record_outcome(pull(&worker, 1, OutcomeKind::PrMerged, t, t + 1_000))
            .unwrap();
        store
            .record_outcome(pull(&worker, 2, OutcomeKind::PrClosed, t, t + 1_500))
            .unwrap();
        let first = report(&store, &[&worker], None, t + 1_000, OutcomeGroupBy::Model);
        assert_eq!(first.groups[0].cost.reported_cost_micros, 30);
        assert_eq!(first.groups[0].cost_per_success_micros, Some(30));
        assert_eq!(first.unattributed.turns, 0);
        let second = report(
            &store,
            &[&worker],
            Some(t + 1_001),
            t + 2_000,
            OutcomeGroupBy::Model,
        );
        let group = &second.groups[0];
        assert_eq!((group.succeeded, group.failed), (0, 1));
        assert_eq!(group.cost.reported_cost_micros, 40);
        assert_eq!(group.cost_per_success_micros, None);
        assert_eq!(second.unattributed.reported_cost_micros, 80);
        assert_eq!(
            first.groups[0].cost.reported_cost_micros
                + first.unattributed.reported_cost_micros
                + group.cost.reported_cost_micros
                + second.unattributed.reported_cost_micros,
            150
        );
        let whole = report(&store, &[&worker], None, t + 2_000, OutcomeGroupBy::Model);
        assert_eq!(whole.groups[0].cost.reported_cost_micros, 70);
        assert_eq!(whole.groups[0].cost.turns, 3);
        assert_eq!(whole.unattributed.reported_cost_micros, 80);

        // A turn without a reported cost makes the ratio unknown.
        usage(&store, &partial, t + 100, Some(5));
        usage(&store, &partial, t + 200, None);
        verdict(&store, &partial, OutcomeKind::Accepted, t + 300);
        let mixed = report(&store, &[&partial], None, t + 2_000, OutcomeGroupBy::Model);
        let group = &mixed.groups[0];
        assert_eq!((group.cost.turns, group.cost.turns_with_cost), (2, 1));
        assert_eq!(group.cost.reported_cost_micros, 5);
        assert_eq!(group.cost_per_success_micros, None);
    }

    #[test]
    fn group_by_skill_compares_readers_with_agents_that_read_none() {
        let (_dir, store) = open();
        let reader = agent(&store, "reader", "claude-code", "opus");
        let other = agent(&store, "other", "claude-code", "opus");
        let sha = "b".repeat(64);
        store
            .record_skill_read(&reader.id, "prove-it", "SKILL.md", &sha)
            .unwrap();
        store
            .record_skill_read(&reader.id, "deliver", "SKILL.md", &sha)
            .unwrap();
        // Outcomes land after the reads, whose time is the real clock.
        let t = now_ms() + 60_000;
        verdict(&store, &reader, OutcomeKind::Accepted, t);
        verdict(&store, &other, OutcomeKind::Rejected, t);
        let by_skill = report(&store, &[&reader, &other], None, t, OutcomeGroupBy::Skill);
        let skills: Vec<(Option<&str>, u64, u64)> = by_skill
            .groups
            .iter()
            .map(|group| (group.skill.as_deref(), group.succeeded, group.failed))
            .collect();
        assert_eq!(
            skills,
            vec![
                (None, 0, 1),
                (Some("deliver"), 1, 0),
                (Some("prove-it"), 1, 0)
            ]
        );
    }

    #[test]
    fn unknown_kinds_are_skipped_and_deleting_an_agent_removes_its_evidence() {
        let (_dir, store) = open();
        let worker = agent(&store, "worker", "claude-code", "opus");
        let t = worker.created_at_ms;
        store
            .conn()
            .execute(
                "INSERT INTO outcomes (id, agent_id, kind, source, subject, dedupe_key, actor,
                     harness, claimed_at_ms, observed_at_ms)
                 VALUES ('out_future', ?1, 'ci_failed', 'github', 's', 'k', 'system',
                         'claude-code', ?2, ?2)",
                params![worker.id.as_str(), t + 1],
            )
            .unwrap();
        verdict(&store, &worker, OutcomeKind::Accepted, t + 2);
        store
            .record_skill_read(&worker.id, "prove-it", "SKILL.md", &"c".repeat(64))
            .unwrap();
        let read = report(&store, &[&worker], None, t + 10, OutcomeGroupBy::Agent);
        assert_eq!(read.groups.len(), 1);
        assert_eq!(read.groups[0].events.len(), 1);
        store.delete_agent(&worker.id).unwrap();
        for table in ["outcomes", "skill_reads"] {
            let left: i64 = store
                .conn()
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(left, 0, "{table}");
        }
    }
}
