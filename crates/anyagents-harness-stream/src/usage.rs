//! Normalize result counters before they enter the per-turn ledger.

use std::path::PathBuf;

use anyagents_harness::{TurnUsage, atomic_write_file};
use serde_json::Value;

use crate::{LineProtocol, State};

pub(super) fn turn_usage(
    state: &mut State,
    protocol: LineProtocol,
    result: &Value,
    reported: TurnUsage,
) -> TurnUsage {
    match protocol {
        LineProtocol::CumulativeResult => {
            state.cumulative.delta(state.session_id.as_deref(), result)
        }
        _ => reported,
    }
}

pub(super) fn reported_usage(protocol: LineProtocol, result: &Value) -> TurnUsage {
    let mut usage = tokens(counters(result).map(Option::unwrap_or_default));
    if protocol == LineProtocol::AcknowledgedResult {
        usage.reported_cost_micros = result.get("cost_micros").and_then(Value::as_i64);
    }
    usage
}

fn counters(result: &Value) -> [Option<u64>; 4] {
    let usage = result
        .get("usage")
        .or_else(|| result.pointer("/result/usage"))
        .or_else(|| result.pointer("/message/usage"));
    let count = |keys: &[&str]| {
        keys.iter()
            .filter_map(|key| usage?.get(*key)?.as_u64())
            .max()
    };
    [
        count(&["input_tokens", "input"]),
        count(&["output_tokens", "output"]),
        count(&["cache_read_tokens", "cacheRead", "cache_read_input_tokens"]),
        count(&[
            "cache_write_tokens",
            "cacheWrite",
            "cache_creation_input_tokens",
        ]),
    ]
}

fn tokens(
    [
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
    ]: [u64; 4],
) -> TurnUsage {
    TurnUsage {
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
        reported_cost_micros: None,
    }
}

#[derive(Default)]
pub(super) struct CumulativeUsage {
    dir: Option<PathBuf>,
    session: Option<String>,
    previous: [u64; 4],
}

impl CumulativeUsage {
    pub(super) fn new(dir: Option<PathBuf>) -> Self {
        Self {
            dir,
            ..Self::default()
        }
    }

    pub(super) fn delta(&mut self, session: Option<&str>, result: &Value) -> TurnUsage {
        let reported = counters(result);
        if reported.iter().all(Option::is_none)
            || (reported.iter().flatten().all(|count| *count == 0) && failed(result))
        {
            // Missing telemetry and zero-filled failure results are not resets.
            return TurnUsage::default();
        }
        let path = self.checkpoint_path(session);
        if self.session.as_deref() != session {
            self.session = session.map(str::to_owned);
            self.previous = path
                .as_ref()
                .and_then(|path| std::fs::read(path).ok())
                .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                .unwrap_or_default();
        }
        if reported
            .iter()
            .zip(self.previous)
            .any(|(now, before)| now.is_some_and(|now| now < before))
        {
            // A decrease marks a new counter epoch (e.g. compaction).
            self.previous = [0; 4];
        }
        let delta = std::array::from_fn(|i| match reported[i] {
            Some(now) => now.saturating_sub(std::mem::replace(&mut self.previous[i], now)),
            None => 0,
        });
        if let Some(path) = path {
            // This is a snapshot of the wire counters, not the served ledger;
            // a detached worker must begin with an empty served ledger.
            let _ = atomic_write_file(
                &path,
                &serde_json::to_vec(&self.previous).expect("four token counters serialize"),
                0o600,
            );
        }
        tokens(delta)
    }

    fn checkpoint_path(&self, session: Option<&str>) -> Option<PathBuf> {
        let session = session.filter(|id| !id.is_empty() && id.len() <= 96)?;
        // Encode opaque identifiers as a single safe filename component.
        let name: String = session.bytes().map(|byte| format!("{byte:02x}")).collect();
        Some(self.dir.as_ref()?.join(format!("{name}.json")))
    }
}

fn failed(result: &Value) -> bool {
    result.get("is_error").and_then(Value::as_bool) == Some(true)
        || result
            .pointer("/result/status")
            .and_then(Value::as_str)
            .is_some_and(|s| s.eq_ignore_ascii_case("error"))
        || [result.get("error"), result.pointer("/result/error")]
            .into_iter()
            .flatten()
            .any(|error| !error.is_null())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn per_turn_results_pass_through_even_when_the_counts_grow() {
        for protocol in [LineProtocol::Result, LineProtocol::AcknowledgedResult] {
            let mut state = State::new(Some("session"));
            for input in [1_000, 2_500, 3_100] {
                let result = json!({"usage": {"input_tokens": input}, "cost_micros": 5});
                let reported = reported_usage(protocol, &result);
                let usage = turn_usage(&mut state, protocol, &result, reported);
                assert_eq!(usage.input_tokens, input);
                assert_eq!(
                    usage.reported_cost_micros,
                    (protocol == LineProtocol::AcknowledgedResult).then_some(5)
                );
            }
        }
    }
}
