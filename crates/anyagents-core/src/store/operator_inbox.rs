//! The operator's inbox: defects an agent reports, through `send` to
//! `operator`, to whoever runs this deployment.
//!
//! Agents write and only the operator reads; the server decides who may do
//! which. So that a looping agent, or a defect that hits the whole fleet,
//! cannot flood the inbox:
//!
//! - the same body from any sender within an hour of a report is that report,
//!   and the repeat is counted on it;
//! - each sender has a small burst and an hourly cap, and so does the fleet;
//! - reports older than the retention window are pruned.
//!
//! The decision and the write share one write transaction, so concurrent
//! sends cannot both take the last slot.

use rusqlite::{Connection, OptionalExtension, Row, params};

use super::{Error, MessageId, Store};
use crate::migrate::now_ms;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
/// Reports one sender may store in any minute.
pub const OPERATOR_BURST: i64 = 3;
/// Reports one sender may store in any hour.
pub const OPERATOR_HOURLY_CAP: i64 = 10;
/// Reports all senders together may store in any minute.
pub const OPERATOR_FLEET_BURST: i64 = 60;
/// Reports all senders together may store in any hour.
pub const OPERATOR_FLEET_HOURLY_CAP: i64 = 500;
/// The same body within this window of a report is that report.
pub const OPERATOR_DEDUPE_WINDOW_MS: i64 = HOUR_MS;
/// Reports older than this are pruned.
pub const OPERATOR_RETENTION_MS: i64 = 30 * 24 * HOUR_MS;
/// Longer output belongs in a file whose path the report names.
pub const OPERATOR_REPORT_MAX_BYTES: usize = 16 * 1024;

/// One report to store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorReport {
    /// The authenticated author: `agent:<agent id>` or `operator`. It keys the
    /// per-sender rate limit.
    pub sender: String,
    /// The author's display name when it wrote.
    pub sender_name: Option<String>,
    pub body: String,
    /// Overrides the clock so tests can age a report without sleeping.
    pub received_at_ms: Option<i64>,
}

/// One stored report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorMessage {
    pub id: MessageId,
    pub seq: i64,
    /// Who sent it first.
    pub sender: String,
    pub sender_name: Option<String>,
    pub body: String,
    pub received_at_ms: i64,
    /// How many times the same body arrived again, from any sender, within
    /// the window.
    pub repeats: i64,
    pub last_repeated_at_ms: Option<i64>,
}

/// What a report came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperatorReceipt {
    New(OperatorMessage),
    /// The same body arrived within the window; that report stands and counts
    /// this one.
    Duplicate(OperatorMessage),
    /// The sender or the fleet is over its burst or hourly limit; nothing was
    /// stored.
    Limited {
        retry_after_ms: i64,
    },
}

const COLS: &str =
    "id, seq, sender, sender_name, body, received_at_ms, repeats, last_repeated_at_ms";

fn from_row(row: &Row<'_>) -> Result<OperatorMessage, Error> {
    Ok(OperatorMessage {
        id: MessageId::parse(&row.get::<_, String>(0)?)?,
        seq: row.get(1)?,
        sender: row.get(2)?,
        sender_name: row.get(3)?,
        body: row.get(4)?,
        received_at_ms: row.get(5)?,
        repeats: row.get(6)?,
        last_repeated_at_ms: row.get(7)?,
    })
}

fn digest(body: &str) -> String {
    crate::learned_memory::sha256(body.as_bytes())
}

/// The report this body repeats, if one arrived within the window.
fn original(conn: &Connection, body: &str, now: i64) -> Result<Option<OperatorMessage>, Error> {
    conn.prepare_cached(&format!(
        "SELECT {COLS} FROM operator_messages
         WHERE body_sha256 = ?1 AND received_at_ms > ?2 AND body = ?3
         ORDER BY seq DESC LIMIT 1"
    ))?
    .query_row(
        params![digest(body), now - OPERATOR_DEDUPE_WINDOW_MS, body],
        |row| Ok(from_row(row)),
    )
    .optional()?
    .transpose()
}

/// When `sender` (every sender, for `None`) may store another report, if it
/// is over a limit now.
fn limited_until(conn: &Connection, sender: Option<&str>, now: i64) -> Result<Option<i64>, Error> {
    let limits = match sender {
        Some(_) => [(MINUTE_MS, OPERATOR_BURST), (HOUR_MS, OPERATOR_HOURLY_CAP)],
        None => [
            (MINUTE_MS, OPERATOR_FLEET_BURST),
            (HOUR_MS, OPERATOR_FLEET_HOURLY_CAP),
        ],
    };
    let mut open_at = None;
    for (window, cap) in limits {
        // The cap-th newest report in the window: once it leaves the window,
        // fewer than `cap` remain and a report may be stored again.
        let pivot: Option<i64> = conn
            .prepare_cached(
                "SELECT received_at_ms FROM operator_messages
                 WHERE (?1 IS NULL OR sender = ?1) AND received_at_ms > ?2
                 ORDER BY received_at_ms DESC LIMIT 1 OFFSET ?3",
            )?
            .query_row(params![sender, now - window, cap - 1], |row| row.get(0))
            .optional()?;
        if let Some(pivot) = pivot {
            open_at = open_at.max(Some(pivot + window));
        }
    }
    Ok(open_at)
}

/// The answer a report gets without being stored, or `None` when it would be
/// stored. A repeat is answered first, even while a limit holds, so a loop
/// keeps getting its receipt instead of an error.
fn decide(
    conn: &Connection,
    sender: &str,
    body: &str,
    now: i64,
) -> Result<Option<OperatorReceipt>, Error> {
    if let Some(existing) = original(conn, body, now)? {
        return Ok(Some(OperatorReceipt::Duplicate(existing)));
    }
    let open_at = limited_until(conn, Some(sender), now)?.max(limited_until(conn, None, now)?);
    Ok(open_at.map(|at| OperatorReceipt::Limited {
        retry_after_ms: (at - now).max(1),
    }))
}

fn validate(sender: &str, body: &str) -> Result<(), Error> {
    if sender.trim().is_empty() {
        return Err(Error::Message("an operator report needs a sender".into()));
    }
    if body.trim().is_empty() {
        return Err(Error::Message("an operator report needs text".into()));
    }
    if body.len() > OPERATOR_REPORT_MAX_BYTES {
        return Err(Error::Message(format!(
            "an operator report is at most {} KiB; put longer output in a file and give its path",
            OPERATOR_REPORT_MAX_BYTES / 1024
        )));
    }
    Ok(())
}

impl Store {
    /// What [`Store::report_to_operator`] would answer, without writing:
    /// `None` when the report would be stored.
    pub fn check_operator_report(
        &self,
        sender: &str,
        body: &str,
    ) -> Result<Option<OperatorReceipt>, Error> {
        let body = body.trim();
        validate(sender, body)?;
        decide(&self.read_conn(), sender, body, now_ms())
    }

    /// Store one report in the operator's inbox, count it on the report it
    /// repeats, or refuse it when a limit holds.
    pub fn report_to_operator(&self, report: OperatorReport) -> Result<OperatorReceipt, Error> {
        let report = OperatorReport {
            body: report.body.trim().to_string(),
            ..report
        };
        validate(&report.sender, &report.body)?;
        let now = report.received_at_ms.unwrap_or_else(now_ms);
        self.write_tx(|tx| {
            match decide(tx, &report.sender, &report.body, now)? {
                Some(OperatorReceipt::Duplicate(existing)) => {
                    tx.execute(
                        "UPDATE operator_messages
                         SET repeats = repeats + 1, last_repeated_at_ms = ?2 WHERE seq = ?1",
                        params![existing.seq, now],
                    )?;
                    return Ok(OperatorReceipt::Duplicate(OperatorMessage {
                        repeats: existing.repeats + 1,
                        last_repeated_at_ms: Some(now),
                        ..existing
                    }));
                }
                Some(answer) => return Ok(answer),
                None => {}
            }
            tx.execute(
                "DELETE FROM operator_messages WHERE received_at_ms < ?1",
                [now - OPERATOR_RETENTION_MS],
            )?;
            let id = MessageId::generate();
            tx.execute(
                "INSERT INTO operator_messages
                     (id, sender, sender_name, body, body_sha256, received_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.as_str(),
                    report.sender,
                    report.sender_name,
                    report.body,
                    digest(&report.body),
                    now
                ],
            )?;
            Ok(OperatorReceipt::New(OperatorMessage {
                id,
                seq: tx.last_insert_rowid(),
                sender: report.sender,
                sender_name: report.sender_name,
                body: report.body,
                received_at_ms: now,
                repeats: 0,
                last_repeated_at_ms: None,
            }))
        })
    }

    /// The latest `limit` reports after `since`, in sequence order, like an
    /// agent's inbox.
    pub fn operator_inbox(
        &self,
        since: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<OperatorMessage>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLS} FROM operator_messages WHERE seq > ?1 ORDER BY seq DESC LIMIT ?2"
        ))?;
        let mut rows = stmt.query(params![
            since.unwrap_or(i64::MIN),
            limit
                .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
                .unwrap_or(-1)
        ])?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next()? {
            messages.push(from_row(row)?);
        }
        messages.reverse();
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(sender: &str, body: &str, at: i64) -> OperatorReport {
        OperatorReport {
            sender: sender.into(),
            sender_name: Some(sender.trim_start_matches("agent:").into()),
            body: body.into(),
            received_at_ms: Some(at),
        }
    }

    fn stored(receipt: OperatorReceipt) -> OperatorMessage {
        match receipt {
            OperatorReceipt::New(message) => message,
            other => panic!("expected a new report, got {other:?}"),
        }
    }

    fn open() -> (tempfile::TempDir, Store) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    #[test]
    fn reports_are_stored_in_order_and_read_like_an_inbox() {
        let (_dir, store) = open();
        let t = 1_000_000;
        let first = stored(
            store
                .report_to_operator(report("agent:a", "  disk full  ", t))
                .unwrap(),
        );
        assert_eq!(first.body, "disk full");
        assert_eq!(first.sender_name.as_deref(), Some("a"));
        assert_eq!(first.repeats, 0);
        let second = stored(
            store
                .report_to_operator(report("agent:b", "socket refused", t + 1))
                .unwrap(),
        );
        let third = stored(
            store
                .report_to_operator(report("agent:a", "harness hang", t + 2))
                .unwrap(),
        );
        assert!(first.seq < second.seq && second.seq < third.seq);
        let all = store.operator_inbox(None, None).unwrap();
        assert_eq!(all, vec![first.clone(), second.clone(), third.clone()]);
        assert_eq!(
            store.operator_inbox(Some(first.seq), None).unwrap(),
            vec![second.clone(), third.clone()]
        );
        assert_eq!(store.operator_inbox(None, Some(1)).unwrap(), vec![third]);
        for (sender, body) in [("agent:a", " "), ("", "text")] {
            assert!(store.report_to_operator(report(sender, body, t)).is_err());
        }
        let long = "x".repeat(OPERATOR_REPORT_MAX_BYTES + 1);
        assert!(
            store
                .report_to_operator(report("agent:a", &long, t))
                .is_err()
        );
        assert!(store.check_operator_report("agent:a", &long).is_err());
        let exact = "y".repeat(OPERATOR_REPORT_MAX_BYTES);
        stored(
            store
                .report_to_operator(report("agent:a", &exact, t + 3))
                .unwrap(),
        );
        assert_eq!(store.operator_inbox(None, None).unwrap().len(), 4);
    }

    #[test]
    fn the_same_body_from_any_sender_within_the_window_is_counted_on_one_report() {
        let (_dir, store) = open();
        // Recent, because the check below reads the real clock.
        let t = now_ms() - 10_000;
        let first = stored(
            store
                .report_to_operator(report("agent:a", "same error", t))
                .unwrap(),
        );
        let OperatorReceipt::Duplicate(again) = store
            .report_to_operator(report("agent:a", "same error ", t + 1_000))
            .unwrap()
        else {
            panic!("a repeat is a duplicate");
        };
        assert_eq!((again.seq, again.repeats), (first.seq, 1));
        // Another sender's identical report is the same defect.
        let OperatorReceipt::Duplicate(other) = store
            .report_to_operator(report("agent:b", "same error", t + 2_000))
            .unwrap()
        else {
            panic!("an identical body from another sender is a duplicate");
        };
        assert_eq!(other.sender, "agent:a");
        assert_eq!(
            (other.repeats, other.last_repeated_at_ms),
            (2, Some(t + 2_000))
        );
        // A check counts nothing.
        assert!(matches!(
            store.check_operator_report("agent:c", "same error"),
            Ok(Some(OperatorReceipt::Duplicate(_)))
        ));
        let rows = store.operator_inbox(None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].repeats, 2);
        // After the window the defect is reported again.
        stored(
            store
                .report_to_operator(report(
                    "agent:b",
                    "same error",
                    t + OPERATOR_DEDUPE_WINDOW_MS + 1,
                ))
                .unwrap(),
        );
        assert_eq!(store.operator_inbox(None, None).unwrap().len(), 2);
    }

    #[test]
    fn each_sender_has_a_small_burst_and_an_hourly_cap() {
        let (_dir, store) = open();
        let t = 100_000_000;
        for i in 0..OPERATOR_BURST {
            stored(
                store
                    .report_to_operator(report("agent:a", &format!("burst {i}"), t + i))
                    .unwrap(),
            );
        }
        assert_eq!(
            store
                .report_to_operator(report("agent:a", "one more", t + 10))
                .unwrap(),
            OperatorReceipt::Limited {
                retry_after_ms: MINUTE_MS - 10
            }
        );
        // A repeat is still answered with its receipt while limited.
        assert!(matches!(
            store
                .report_to_operator(report("agent:a", "burst 0", t + 11))
                .unwrap(),
            OperatorReceipt::Duplicate(_)
        ));
        // The limit is per sender.
        stored(
            store
                .report_to_operator(report("agent:b", "one more", t + 12))
                .unwrap(),
        );
        // Spaced past the burst window, the hourly cap is what stops it.
        let mut at = t;
        for i in OPERATOR_BURST..OPERATOR_HOURLY_CAP {
            at += MINUTE_MS;
            stored(
                store
                    .report_to_operator(report("agent:a", &format!("spaced {i}"), at))
                    .unwrap(),
            );
        }
        at += MINUTE_MS;
        assert_eq!(
            store
                .report_to_operator(report("agent:a", "over the cap", at))
                .unwrap(),
            OperatorReceipt::Limited {
                retry_after_ms: t + HOUR_MS - at
            }
        );
        stored(
            store
                .report_to_operator(report("agent:a", "over the cap", t + HOUR_MS))
                .unwrap(),
        );
        let from_a = store
            .operator_inbox(None, None)
            .unwrap()
            .into_iter()
            .filter(|m| m.sender == "agent:a")
            .count();
        assert_eq!(from_a, usize::try_from(OPERATOR_HOURLY_CAP).unwrap() + 1);
    }

    #[test]
    fn the_fleet_has_its_own_burst_and_hourly_cap() {
        let (_dir, store) = open();
        let t = 200_000_000;
        // One report from each of many senders, all inside one minute.
        for i in 0..OPERATOR_FLEET_BURST {
            stored(
                store
                    .report_to_operator(report(&format!("agent:m{i}"), &format!("m{i}"), t + i))
                    .unwrap(),
            );
        }
        assert_eq!(
            store
                .report_to_operator(report("agent:late", "late", t + 100))
                .unwrap(),
            OperatorReceipt::Limited {
                retry_after_ms: MINUTE_MS - 100
            }
        );
        // One a second stays under the fleet burst; the hourly cap stops it.
        let (_dir, store) = open();
        for i in 0..OPERATOR_FLEET_HOURLY_CAP {
            stored(
                store
                    .report_to_operator(report(
                        &format!("agent:h{i}"),
                        &format!("h{i}"),
                        t + i * 1_000,
                    ))
                    .unwrap(),
            );
        }
        let at = t + OPERATOR_FLEET_HOURLY_CAP * 1_000;
        assert_eq!(
            store
                .report_to_operator(report("agent:late", "late", at))
                .unwrap(),
            OperatorReceipt::Limited {
                retry_after_ms: t + HOUR_MS - at
            }
        );
    }

    #[test]
    fn no_agent_may_be_created_or_renamed_as_operator() {
        let (_dir, store) = open();
        for name in ["operator", "Operator"] {
            assert!(
                store
                    .create_agent(crate::NewAgent::new(name, "codex"))
                    .is_err()
            );
        }
        let agent = store
            .create_agent(crate::NewAgent::new("operator-helper", "codex"))
            .unwrap();
        for name in ["operator", "agent:operator", "OPERATOR"] {
            assert!(store.rename_agent(&agent.id, name).is_err());
        }
        assert_eq!(store.agent(&agent.id).unwrap().name, "operator-helper");
    }

    #[test]
    fn reports_past_the_retention_window_are_pruned_and_seq_is_not_reused() {
        let (_dir, store) = open();
        let t = 300_000_000;
        let old = stored(
            store
                .report_to_operator(report("agent:a", "old", t))
                .unwrap(),
        );
        let later = t + OPERATOR_RETENTION_MS + 1;
        let new = stored(
            store
                .report_to_operator(report("agent:a", "new", later))
                .unwrap(),
        );
        assert!(new.seq > old.seq);
        assert_eq!(store.operator_inbox(None, None).unwrap(), vec![new]);
    }
}
