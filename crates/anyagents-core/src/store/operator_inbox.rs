//! The operator's inbox: defects an agent reports, through `send` to
//! `operator`, to whoever runs this deployment.
//!
//! Agents write and only the operator reads; the server decides who may do
//! which. Each sender has a small burst and an hourly cap, and the same body
//! from the same sender within an hour is the report already stored, so a
//! looping agent cannot flood the inbox. The decision and the insert share one
//! write transaction, so concurrent sends cannot both take the last slot.

use rusqlite::{Connection, OptionalExtension, Row, params};

use super::{Error, MessageId, Store};
use crate::migrate::now_ms;

/// Reports one sender may store in any [`OPERATOR_BURST_WINDOW_MS`].
pub const OPERATOR_BURST: i64 = 3;
pub const OPERATOR_BURST_WINDOW_MS: i64 = 60_000;
/// Reports one sender may store in any hour.
pub const OPERATOR_HOURLY_CAP: i64 = 10;
const HOUR_MS: i64 = 3_600_000;
/// The same body from the same sender within this window is not stored again.
pub const OPERATOR_DEDUPE_WINDOW_MS: i64 = HOUR_MS;
/// Longer output belongs in a file whose path the report names.
pub const OPERATOR_REPORT_MAX_BYTES: usize = 16 * 1024;

/// One report to store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorReport {
    /// The authenticated author: `agent:<agent id>` or `operator`. It keys the
    /// rate limit and deduplication.
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
    pub sender: String,
    pub sender_name: Option<String>,
    pub body: String,
    pub received_at_ms: i64,
}

/// What a report came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperatorReceipt {
    New(OperatorMessage),
    /// The sender sent this body within the window; that report stands.
    Duplicate(OperatorMessage),
    /// The sender is over its burst or hourly limit; nothing was stored.
    Limited {
        retry_after_ms: i64,
    },
}

const COLS: &str = "id, seq, sender, sender_name, body, received_at_ms";

fn from_row(row: &Row<'_>) -> Result<OperatorMessage, Error> {
    Ok(OperatorMessage {
        id: MessageId::parse(&row.get::<_, String>(0)?)?,
        seq: row.get(1)?,
        sender: row.get(2)?,
        sender_name: row.get(3)?,
        body: row.get(4)?,
        received_at_ms: row.get(5)?,
    })
}

/// The answer a report gets without being stored, or `None` when it would be
/// stored.
fn decide(
    conn: &Connection,
    sender: &str,
    body: &str,
    now: i64,
) -> Result<Option<OperatorReceipt>, Error> {
    // A duplicate is answered even when the sender is limited, so a loop that
    // repeats one report keeps getting its receipt instead of an error.
    let duplicate = conn
        .prepare_cached(&format!(
            "SELECT {COLS} FROM operator_messages
             WHERE sender = ?1 AND received_at_ms > ?2 AND body = ?3
             ORDER BY seq DESC LIMIT 1"
        ))?
        .query_row(
            params![sender, now - OPERATOR_DEDUPE_WINDOW_MS, body],
            |row| Ok(from_row(row)),
        )
        .optional()?
        .transpose()?;
    if let Some(existing) = duplicate {
        return Ok(Some(OperatorReceipt::Duplicate(existing)));
    }
    let mut open_at = None;
    for (window, cap) in [
        (OPERATOR_BURST_WINDOW_MS, OPERATOR_BURST),
        (HOUR_MS, OPERATOR_HOURLY_CAP),
    ] {
        // The cap-th newest report in the window: once it leaves the window,
        // fewer than `cap` remain and the sender may write again.
        let pivot: Option<i64> = conn
            .prepare_cached(
                "SELECT received_at_ms FROM operator_messages
                 WHERE sender = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms DESC LIMIT 1 OFFSET ?3",
            )?
            .query_row(params![sender, now - window, cap - 1], |row| row.get(0))
            .optional()?;
        if let Some(pivot) = pivot {
            open_at = open_at.max(Some(pivot + window));
        }
    }
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

    /// Store one report in the operator's inbox, unless the sender sent the
    /// same body within the window or is over its limit.
    pub fn report_to_operator(&self, report: OperatorReport) -> Result<OperatorReceipt, Error> {
        let report = OperatorReport {
            body: report.body.trim().to_string(),
            ..report
        };
        validate(&report.sender, &report.body)?;
        let now = report.received_at_ms.unwrap_or_else(now_ms);
        self.write_tx(|tx| {
            if let Some(answer) = decide(tx, &report.sender, &report.body, now)? {
                return Ok(answer);
            }
            let id = MessageId::generate();
            tx.execute(
                "INSERT INTO operator_messages (id, sender, sender_name, body, received_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    id.as_str(),
                    report.sender,
                    report.sender_name,
                    report.body,
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

    #[test]
    fn reports_are_stored_in_order_and_read_like_an_inbox() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let t = 1_000_000;
        let first = stored(
            store
                .report_to_operator(report("agent:a", "  disk full  ", t))
                .unwrap(),
        );
        assert_eq!(first.body, "disk full");
        assert_eq!(first.sender_name.as_deref(), Some("a"));
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
        assert_eq!(store.operator_inbox(None, None).unwrap().len(), 3);
    }

    #[test]
    fn the_same_body_from_the_same_sender_within_the_window_is_one_report() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let t = 10_000_000;
        let first = stored(
            store
                .report_to_operator(report("agent:a", "same error", t))
                .unwrap(),
        );
        assert_eq!(
            store
                .report_to_operator(report("agent:a", "same error ", t + 1_000))
                .unwrap(),
            OperatorReceipt::Duplicate(first.clone())
        );
        // Another sender's identical report is its own.
        stored(
            store
                .report_to_operator(report("agent:b", "same error", t + 2_000))
                .unwrap(),
        );
        // After the window the defect is reported again.
        stored(
            store
                .report_to_operator(report(
                    "agent:a",
                    "same error",
                    t + OPERATOR_DEDUPE_WINDOW_MS + 1,
                ))
                .unwrap(),
        );
        assert_eq!(store.operator_inbox(None, None).unwrap().len(), 3);
    }

    #[test]
    fn each_sender_has_a_small_burst_and_an_hourly_cap() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let t = 100_000_000;
        for i in 0..OPERATOR_BURST {
            stored(
                store
                    .report_to_operator(report("agent:a", &format!("burst {i}"), t + i))
                    .unwrap(),
            );
        }
        let limited = store
            .report_to_operator(report("agent:a", "one more", t + 10))
            .unwrap();
        assert_eq!(
            limited,
            OperatorReceipt::Limited {
                retry_after_ms: OPERATOR_BURST_WINDOW_MS - 10
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
            at += OPERATOR_BURST_WINDOW_MS;
            stored(
                store
                    .report_to_operator(report("agent:a", &format!("spaced {i}"), at))
                    .unwrap(),
            );
        }
        at += OPERATOR_BURST_WINDOW_MS;
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
}
