//! Token/cost usage persistence, UTC day bucketing, aggregation, and access scope resolution.

use super::helpers::agent_exists;
use super::types::{Agent, BillingMode, NewUsage, UsageRecord};
use super::{Store, now_ms};
use crate::db::{DbExecExt, DbRow};
use crate::db_params;
use crate::error::Error;
use crate::id::{AgentId, UsageId};

impl Store {
    pub fn record_usage(&self, new: NewUsage) -> Result<UsageRecord, Error> {
        let record = UsageRecord {
            id: UsageId::generate(),
            agent_id: new.agent_id.clone(),
            harness: new.harness,
            model: new.model,
            input_tokens: new.input_tokens,
            output_tokens: new.output_tokens,
            cache_read_tokens: new.cache_read_tokens,
            cache_write_tokens: new.cache_write_tokens,
            reported_cost_micros: new.reported_cost_micros,
            estimated_cost_micros: new.estimated_cost_micros,
            billing_mode: new.billing_mode,
            quota_state: new.quota_state,
            day: new.day,
            recorded_at_ms: now_ms(),
        };
        // The id is fixed before the first attempt: a retry can never add a
        // second row for the same usage.
        self.write_tx(|tx| {
            agent_exists(tx, &record.agent_id)?;
            tx.db_execute(
                "INSERT INTO usage_records (
                    id, agent_id, harness, model, input_tokens, output_tokens,
                    cache_read_tokens, cache_write_tokens, reported_cost_micros,
                    estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                &db_params![
                    record.id.as_str(),
                    record.agent_id.as_str(),
                    record.harness,
                    record.model,
                    record.input_tokens,
                    record.output_tokens,
                    record.cache_read_tokens,
                    record.cache_write_tokens,
                    record.reported_cost_micros,
                    record.estimated_cost_micros,
                    record.billing_mode.as_str(),
                    record.quota_state,
                    record.day,
                    record.recorded_at_ms,
                ],
            )?;
            Ok(())
        })?;
        Ok(record)
    }

    pub fn list_usage(&self, agent_id: Option<&AgentId>) -> Result<Vec<UsageRecord>, Error> {
        let conn = self.read_conn();
        let (sql, params) = match agent_id {
            Some(id) => {
                agent_exists(&conn, id)?;
                (
                    "SELECT id, agent_id, harness, model, input_tokens, output_tokens,
                            cache_read_tokens, cache_write_tokens, reported_cost_micros,
                            estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
                     FROM usage_records WHERE agent_id = ?1 ORDER BY recorded_at_ms, id",
                    db_params![id.as_str()],
                )
            }
            None => (
                "SELECT id, agent_id, harness, model, input_tokens, output_tokens,
                        cache_read_tokens, cache_write_tokens, reported_cost_micros,
                        estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
                 FROM usage_records ORDER BY recorded_at_ms, id",
                Vec::new(),
            ),
        };
        conn.db_query_vec(sql, &params, usage_from_row_db)
    }
}

fn usage_from_row_db(row: &DbRow<'_>) -> Result<UsageRecord, Error> {
    let billing: String = row.get(10)?;
    Ok(UsageRecord {
        id: row.parse_str(0)?,
        agent_id: row.parse_str(1)?,
        harness: row.get(2)?,
        model: row.get(3)?,
        input_tokens: row.get(4)?,
        output_tokens: row.get(5)?,
        cache_read_tokens: row.get(6)?,
        cache_write_tokens: row.get(7)?,
        reported_cost_micros: row.get(8)?,
        estimated_cost_micros: row.get(9)?,
        billing_mode: BillingMode::parse(&billing)?,
        quota_state: row.get(11)?,
        day: row.get(12)?,
        recorded_at_ms: row.get(13)?,
    })
}

/// UTC calendar day for a Unix millisecond timestamp, `YYYY-MM-DD`.
pub fn utc_day(unix_ms: i64) -> String {
    let days = unix_ms.div_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Howard Hinnant's `civil_from_days`. `days` is days since 1970-01-01.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year as i32, month as u32, day as u32)
}

/// One summed usage row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageTotal {
    pub agent_id: AgentId,
    pub day: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
}

/// Sum usage rows by agent, or by agent and day.
pub fn sum_usage(records: &[UsageRecord], by_day: bool) -> Vec<UsageTotal> {
    use std::collections::BTreeMap;
    let mut totals: BTreeMap<(String, String), UsageTotal> = BTreeMap::new();
    for record in records {
        let day_key = if by_day {
            record.day.clone()
        } else {
            String::new()
        };
        let key = (record.agent_id.as_str().to_string(), day_key);
        let entry = totals.entry(key).or_insert_with(|| UsageTotal {
            agent_id: record.agent_id.clone(),
            day: if by_day {
                Some(record.day.clone())
            } else {
                None
            },
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reported_cost_micros: None,
        });
        entry.input_tokens += record.input_tokens;
        entry.output_tokens += record.output_tokens;
        entry.cache_read_tokens += record.cache_read_tokens;
        entry.cache_write_tokens += record.cache_write_tokens;
        if let Some(cost) = record.reported_cost_micros {
            entry.reported_cost_micros = Some(entry.reported_cost_micros.unwrap_or(0) + cost);
        }
    }
    totals.into_values().collect()
}

/// How a structured usage query groups its detail buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageGroupBy {
    Agent,
    Day,
    Model,
    Harness,
    Profile,
}

impl UsageGroupBy {
    /// Parse a caller-supplied grouping. Empty text means per-agent buckets.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.trim().to_ascii_lowercase().as_str() {
            "" | "agent" => Ok(Self::Agent),
            "day" => Ok(Self::Day),
            "model" => Ok(Self::Model),
            "harness" => Ok(Self::Harness),
            "profile" => Ok(Self::Profile),
            other => Err(format!(
                "unsupported group_by `{other}`: expected agent, day, model, harness, or profile"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Day => "day",
            Self::Model => "model",
            Self::Harness => "harness",
            Self::Profile => "profile",
        }
    }
}

/// Inclusive time-window and grouping options for a usage aggregation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UsageFilter {
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// `days = N` keeps records with `recorded_at_ms >= now_ms - N days`,
    /// intersected with any explicit bounds.
    pub days: Option<i64>,
    pub group_by: UsageGroupBy,
}

impl Default for UsageFilter {
    fn default() -> Self {
        Self {
            since_ms: None,
            until_ms: None,
            days: None,
            group_by: UsageGroupBy::Agent,
        }
    }
}

impl UsageFilter {
    /// Resolve the effective inclusive `(since_ms, until_ms)` window.
    pub fn window(self, now_ms: i64) -> Result<(Option<i64>, Option<i64>), String> {
        let mut since = self.since_ms;
        if let Some(days) = self.days {
            if days <= 0 {
                return Err("days must be a positive number of days".to_string());
            }
            let span = days
                .checked_mul(86_400_000)
                .ok_or_else(|| "days is too large".to_string())?;
            let bound = now_ms.checked_sub(span).unwrap_or(i64::MIN);
            since = Some(since.map_or(bound, |s| s.max(bound)));
        }
        if let (Some(since), Some(until)) = (since, self.until_ms)
            && since > until
        {
            return Err("since_ms must not be after until_ms".to_string());
        }
        Ok((since, self.until_ms))
    }
}

/// Aggregated usage metrics shared by grand totals and per-bucket detail.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageTotals {
    pub turns: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
}

impl UsageTotals {
    fn add_record(&mut self, record: &UsageRecord) {
        self.turns += 1;
        self.input_tokens += record.input_tokens;
        self.output_tokens += record.output_tokens;
        self.cache_read_tokens += record.cache_read_tokens;
        self.cache_write_tokens += record.cache_write_tokens;
        if let Some(cost) = record.reported_cost_micros {
            self.reported_cost_micros = Some(self.reported_cost_micros.unwrap_or(0) + cost);
        }
    }

    fn add_totals(&mut self, other: &Self) {
        self.turns += other.turns;
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        if let Some(cost) = other.reported_cost_micros {
            self.reported_cost_micros = Some(self.reported_cost_micros.unwrap_or(0) + cost);
        }
    }
}

/// One grouped usage bucket. Buckets are always per-agent; the optional
/// `day`/`model`/`harness`/`profile` fields carry the extra grouping dimension.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucket {
    pub agent_id: AgentId,
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

/// Per-profile usage rollup across all agents bound to the same provider profile.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProfileUsageBucket {
    pub profile: String,
    pub agent_ids: Vec<AgentId>,
    pub agent_names: Vec<String>,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

/// A structured usage aggregation: grand totals plus grouped buckets,
/// deterministically sorted by agent, then day/model/harness/profile.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageReport {
    pub group_by: UsageGroupBy,
    pub agent_ids: Vec<AgentId>,
    pub totals: UsageTotals,
    pub buckets: Vec<UsageBucket>,
    /// Populated when `group_by == UsageGroupBy::Profile`: per-profile rollups
    /// summing all agents bound to each profile.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<ProfileUsageBucket>,
}

/// Aggregate `records` for `agent_ids` in the filter window.
///
/// `agent_names` supplies best-effort display names. The window is tested
/// against each record's `recorded_at_ms` with inclusive bounds.
pub fn aggregate_usage(
    records: &[UsageRecord],
    agent_ids: &[AgentId],
    agent_names: &std::collections::HashMap<AgentId, String>,
    filter: UsageFilter,
    now_ms: i64,
) -> Result<UsageReport, String> {
    aggregate_usage_with_profiles(
        records,
        agent_ids,
        agent_names,
        &std::collections::HashMap::new(),
        filter,
        now_ms,
    )
}

/// Aggregate `records` for `agent_ids` in the filter window, resolving bound
/// provider profiles from `agent_profiles` when `group_by == UsageGroupBy::Profile`.
pub fn aggregate_usage_with_profiles(
    records: &[UsageRecord],
    agent_ids: &[AgentId],
    agent_names: &std::collections::HashMap<AgentId, String>,
    agent_profiles: &std::collections::HashMap<AgentId, String>,
    filter: UsageFilter,
    now_ms: i64,
) -> Result<UsageReport, String> {
    use std::collections::{BTreeMap, HashSet};
    let (since, until) = filter.window(now_ms)?;
    let allowed: HashSet<&AgentId> = agent_ids.iter().collect();
    // Key: (agent id, day, model, harness, profile); only the grouped dimension is set.
    type Key = (String, String, String, String, String);
    let mut buckets: BTreeMap<Key, UsageBucket> = BTreeMap::new();
    let mut profile_rollups: BTreeMap<String, (BTreeMap<String, AgentId>, UsageTotals)> =
        BTreeMap::new();
    let mut totals = UsageTotals::default();
    for record in records {
        if !allowed.contains(&record.agent_id) {
            continue;
        }
        if since.is_some_and(|s| record.recorded_at_ms < s)
            || until.is_some_and(|u| record.recorded_at_ms > u)
        {
            continue;
        }
        let (day, model, harness, profile) = match filter.group_by {
            UsageGroupBy::Agent => (None, None, None, None),
            UsageGroupBy::Day => (Some(record.day.clone()), None, None, None),
            UsageGroupBy::Model => (None, record.model.clone(), None, None),
            UsageGroupBy::Harness => (None, None, Some(record.harness.clone()), None),
            UsageGroupBy::Profile => {
                let resolved = agent_profiles
                    .get(&record.agent_id)
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .unwrap_or_else(|| "unbound".to_string());
                (None, None, None, Some(resolved))
            }
        };
        let key: Key = (
            record.agent_id.as_str().to_string(),
            day.clone().unwrap_or_default(),
            model.clone().unwrap_or_default(),
            harness.clone().unwrap_or_default(),
            profile.clone().unwrap_or_default(),
        );
        let bucket = buckets.entry(key).or_insert_with(|| UsageBucket {
            agent_id: record.agent_id.clone(),
            agent_name: agent_names.get(&record.agent_id).cloned(),
            day: day.clone(),
            model: model.clone(),
            harness: harness.clone(),
            profile: profile.clone(),
            totals: UsageTotals::default(),
        });
        bucket.totals.add_record(record);
        if let Some(ref prof_name) = profile {
            let entry = profile_rollups.entry(prof_name.clone()).or_default();
            entry
                .0
                .entry(record.agent_id.as_str().to_string())
                .or_insert_with(|| record.agent_id.clone());
            entry.1.add_record(record);
        }
        totals.add_record(record);
    }
    let bucket_values: Vec<UsageBucket> = buckets.into_values().collect();
    let profiles: Vec<ProfileUsageBucket> = profile_rollups
        .into_iter()
        .map(|(profile, (ids, prof_totals))| {
            let agent_ids: Vec<AgentId> = ids.into_values().collect();
            let agent_names: Vec<String> = agent_ids
                .iter()
                .map(|id| {
                    agent_names
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| id.to_string())
                })
                .collect();
            ProfileUsageBucket {
                profile,
                agent_ids,
                agent_names,
                totals: prof_totals,
            }
        })
        .collect();
    // Fold the grand total from the buckets so the two can never diverge.
    let mut folded = UsageTotals::default();
    for bucket in &bucket_values {
        folded.add_totals(&bucket.totals);
    }
    debug_assert_eq!(totals, folded);
    Ok(UsageReport {
        group_by: filter.group_by,
        agent_ids: agent_ids.to_vec(),
        totals,
        buckets: bucket_values,
        profiles,
    })
}

/// Why a usage query was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UsageAccessError {
    /// The caller may not see usage for the requested target.
    Forbidden(String),
    /// The request itself is invalid (unknown agent, bad parameters).
    Invalid(String),
}

impl std::fmt::Display for UsageAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden(msg) | Self::Invalid(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for UsageAccessError {}

/// All causal descendants of `root` (excluding `root` itself), following
/// `parent_id` links over `agents`.
pub fn usage_descendants(root: &AgentId, agents: &[Agent]) -> Vec<AgentId> {
    let mut set = std::collections::HashSet::from([root.clone()]);
    let mut changed = true;
    while changed {
        changed = false;
        for agent in agents {
            if let Some(parent) = &agent.parent_id
                && set.contains(parent)
                && set.insert(agent.id.clone())
            {
                changed = true;
            }
        }
    }
    set.remove(root);
    let mut out: Vec<AgentId> = set.into_iter().collect();
    out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    out
}

/// Whether `caller` may query usage across the whole workspace: the local
/// operator (`None`), a root caller (`parent_id` is `None`), or a caller
/// whose scope is `public`.
pub fn usage_caller_is_privileged(caller: Option<&Agent>) -> bool {
    match caller {
        None => true,
        Some(agent) => agent.parent_id.is_none() || agent.scope() == "public",
    }
}

fn find_agent<'a>(agents: &'a [Agent], target: &str) -> Option<&'a Agent> {
    agents
        .iter()
        .find(|a| a.name.eq_ignore_ascii_case(target) || a.id.as_str() == target)
}

/// Resolve the exact agent set a usage query may read. This is the causal
/// authorization boundary and it fails closed:
///
/// - privileged callers (operator, root, `public` scope) may read every
///   agent, or an explicit target with optional descendants;
/// - any other caller may read only itself and its causal descendants. An
///   omitted target means "my subtree"; naming a parent, sibling or any
///   unrelated agent, or asking for `all`, is forbidden and returns no data.
///
/// `include_descendants` defaults to `false` for an explicitly named target
/// and to `true` when the target is omitted for a non-privileged caller.
pub fn resolve_usage_agents(
    caller: Option<&Agent>,
    agents: &[Agent],
    target: Option<&str>,
    include_descendants: Option<bool>,
) -> Result<Vec<AgentId>, UsageAccessError> {
    let all_ids: Vec<AgentId> = agents.iter().map(|a| a.id.clone()).collect();
    let target = target.map(str::trim).filter(|t| !t.is_empty());
    let privileged = usage_caller_is_privileged(caller);

    if target.is_none_or(|t| t == "all" || t == "*") {
        if privileged {
            return Ok(all_ids);
        }
        if target.is_some() {
            return Err(UsageAccessError::Forbidden(
                "usage query for all agents requires a public or root caller".to_string(),
            ));
        }
        let caller = caller.expect("a non-privileged caller is an agent");
        let mut ids = vec![caller.id.clone()];
        ids.extend(usage_descendants(&caller.id, agents));
        return Ok(ids);
    }

    let target = target.expect("target checked above");
    let target_agent = if target.eq_ignore_ascii_case("self") {
        match caller {
            Some(caller) => agents.iter().find(|a| a.id == caller.id),
            None => None,
        }
        .ok_or_else(|| {
            UsageAccessError::Invalid("agent `self` requires an agent caller".to_string())
        })?
    } else {
        find_agent(agents, target)
            .ok_or_else(|| UsageAccessError::Invalid(format!("unknown agent `{target}`")))?
    };

    if !privileged {
        let caller = caller.expect("a non-privileged caller is an agent");
        let mut visible = std::collections::HashSet::from([caller.id.clone()]);
        visible.extend(usage_descendants(&caller.id, agents));
        if !visible.contains(&target_agent.id) {
            return Err(UsageAccessError::Forbidden(format!(
                "agent `{}` cannot query usage for `{}`",
                caller.name, target_agent.name
            )));
        }
    }

    let mut ids = vec![target_agent.id.clone()];
    if include_descendants.unwrap_or(false) {
        ids.extend(usage_descendants(&target_agent.id, agents));
    }
    Ok(ids)
}
