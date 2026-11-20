//! Durable, component-scoped runtime issue samples and occurrence counts.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{Store, now_ms};
use crate::db::{DbBackend, DbExec as _, DbExecExt as _, DbRow};
use crate::{AgentId, Error, db_params, masking::mask_known_secrets};

const MAX_SUMMARY_BYTES: usize = 512;
const MAX_DETAIL_BYTES: usize = 8 * 1024;

macro_rules! issue_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            fn from_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
                match row.get::<String>(index)?.as_str() {
                    $($value => Ok(Self::$variant)),+,
                    _ => Err(Error::Message(concat!("invalid stored ", stringify!($name)).into())),
                }
            }
        }
    };
}

issue_enum!(RuntimeComponent {
    Coordinator => "coordinator",
    Sandbox => "sandbox",
    Harness => "harness",
    McpTool => "mcp_tool",
});
issue_enum!(RuntimeIssueSource {
    AgentReport => "agent_report",
    AutoCapture => "auto_capture",
});
issue_enum!(RuntimeIssueStatus {
    Open => "open",
    Resolved => "resolved",
});

/// A report to persist. Pass only diagnostic text appropriate for durable
/// storage; credential masking is best-effort, not a general privacy filter.
#[derive(Clone, Copy)]
pub struct RuntimeIssueReport<'a> {
    pub component: RuntimeComponent,
    pub summary: &'a str,
    pub exact_error: &'a str,
    pub repro_steps: &'a str,
    pub source: RuntimeIssueSource,
    pub sample_agent_id: Option<&'a AgentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIssue {
    pub id: String,
    pub component: RuntimeComponent,
    pub error_fingerprint: String,
    pub summary: String,
    pub exact_error: String,
    pub repro_steps: String,
    pub source: RuntimeIssueSource,
    pub status: RuntimeIssueStatus,
    pub sample_agent_id: Option<AgentId>,
    pub occurrence_count: i64,
    /// UTC epoch milliseconds at first insertion.
    pub first_seen_at: i64,
    /// Latest UTC epoch milliseconds observed, never moved backwards.
    pub last_seen_at: i64,
}

impl Store {
    /// Insert an open issue or add an occurrence to the existing issue and
    /// return its id. The first report wins: duplicates never change the sample
    /// (even a missing agent id), its source, first timestamp, or issue status.
    ///
    /// Reject blank, NUL-containing or oversized text (512 UTF-8 bytes for the
    /// summary, 8 KiB each for the error and repro steps). Mask known credential
    /// patterns and registered values before storage and fingerprinting.
    pub fn record_runtime_issue(&self, report: RuntimeIssueReport<'_>) -> Result<String, Error> {
        let summary = issue_text("summary", report.summary, MAX_SUMMARY_BYTES)?;
        let exact_error = issue_text("exact_error", report.exact_error, MAX_DETAIL_BYTES)?;
        let repro_steps = issue_text("repro_steps", report.repro_steps, MAX_DETAIL_BYTES)?;
        let sample = report.sample_agent_id.map(AgentId::as_str);
        // Serde can construct an unchecked AgentId. Never store arbitrary text
        // as a sample id, and don't include rejected text in validation errors.
        if sample.is_some_and(|id| AgentId::parse(id).is_err()) {
            return Err(Error::Message(
                "invalid runtime issue sample_agent_id".into(),
            ));
        }
        let fingerprint = error_fingerprint(&exact_error);
        let id = format!("rti_{}", uuid::Uuid::now_v7().simple());
        self.write_tx(|tx| {
            let table = issue_table(tx.backend());
            tx.db_query_one(
                &format!(
                    "INSERT INTO {table} AS issue (
                        id, component, error_fingerprint, summary, exact_error, repro_steps,
                        source, status, sample_agent_id, occurrence_count, first_seen_at, last_seen_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'open', ?8, 1, ?9, ?9)
                     ON CONFLICT (component, error_fingerprint) DO UPDATE SET
                        occurrence_count = issue.occurrence_count + 1,
                        last_seen_at = CASE WHEN excluded.last_seen_at > issue.last_seen_at
                                            THEN excluded.last_seen_at ELSE issue.last_seen_at END
                     RETURNING id"
                ),
                &db_params![
                    id, report.component.as_str(), fingerprint, summary, exact_error, repro_steps,
                    report.source.as_str(), sample, now_ms()
                ],
                |row| row.get(0),
            )
        })
    }

    /// Return up to 100 matching issues, latest occurrence first, then id
    /// descending for timestamps that tie. Both filters are optional.
    pub fn runtime_issues(
        &self,
        status: Option<RuntimeIssueStatus>,
        component: Option<RuntimeComponent>,
    ) -> Result<Vec<RuntimeIssue>, Error> {
        let (filter, params) = match (status, component) {
            (None, None) => ("", vec![]),
            (Some(s), None) => ("WHERE status = ?1", db_params![s.as_str()]),
            (None, Some(c)) => ("WHERE component = ?1", db_params![c.as_str()]),
            (Some(s), Some(c)) => (
                "WHERE status = ?1 AND component = ?2",
                db_params![s.as_str(), c.as_str()],
            ),
        };
        let conn = self.read_conn();
        let table = issue_table(conn.backend());
        conn.db_query_vec(
            &format!(
                "SELECT id, component, error_fingerprint, summary, exact_error, repro_steps,
                        source, status, sample_agent_id, occurrence_count, first_seen_at, last_seen_at
                 FROM {table} {filter} ORDER BY last_seen_at DESC, id DESC LIMIT 100"
            ),
            &params,
            |row| {
                Ok(RuntimeIssue {
                    id: row.get(0)?,
                    component: RuntimeComponent::from_row(row, 1)?,
                    error_fingerprint: row.get(2)?,
                    summary: row.get(3)?,
                    exact_error: row.get(4)?,
                    repro_steps: row.get(5)?,
                    source: RuntimeIssueSource::from_row(row, 6)?,
                    status: RuntimeIssueStatus::from_row(row, 7)?,
                    sample_agent_id: row.get(8)?,
                    occurrence_count: row.get(9)?,
                    first_seen_at: row.get(10)?,
                    last_seen_at: row.get(11)?,
                })
            },
        )
    }
}

fn issue_table(backend: DbBackend) -> &'static str {
    match backend {
        DbBackend::Sqlite => "main.runtime_issues",
        DbBackend::Postgres => "public.runtime_issues",
    }
}

fn issue_text<'a>(field: &str, text: &'a str, max_bytes: usize) -> Result<Cow<'a, str>, Error> {
    let too_long = || Error::Message(format!("runtime issue {field} exceeds {max_bytes} bytes"));
    if text.len() > max_bytes {
        return Err(too_long());
    }
    if text.contains('\0') || text.trim().is_empty() {
        return Err(Error::Message(format!(
            "runtime issue {field} must be nonblank and contain no NULs"
        )));
    }
    let masked = mask_known_secrets(text);
    if masked.len() > max_bytes {
        return Err(too_long());
    }
    Ok(masked)
}

/// SHA-256 of the masked error with Unicode whitespace runs collapsed to one
/// ASCII space and lowercased. Digits and punctuation are kept.
fn error_fingerprint(masked_error: &str) -> String {
    let canonical = masked_error
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    format!("{:x}", Sha256::digest(canonical.to_lowercase().as_bytes()))
}

#[cfg(test)]
#[path = "runtime_issues_tests.rs"]
mod tests;
