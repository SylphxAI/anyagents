//! The catalog entry and the printed line, decided without a process. The child
//! itself is covered by each adapter's scripted test.

use super::{LineProtocol, StreamSpec};
use serde_json::json;

#[test]
fn each_catalog_entry_starts_and_resumes_with_its_own_arguments() {
    let agy = StreamSpec::for_program("agy");
    assert_eq!(agy.name, "anyagents-agy");
    assert_eq!(
        agy.resume_args(Some("s1")),
        ["--output-format", "stream-json", "--resume", "s1"]
    );
    assert_eq!(agy.resume_args(None), ["--output-format", "stream-json"]);

    let opencode = StreamSpec::for_program("opencode");
    assert_eq!(
        opencode.resume_args(Some("s1")),
        ["serve", "--session", "s1"]
    );

    let omp = StreamSpec::for_program("omp");
    assert_eq!(omp.name, "anyagents-omp");
    assert_eq!(omp.protocol, LineProtocol::Done);
    assert_eq!(omp.resume_args(Some("s1")), ["--session", "s1"]);
    assert_eq!(omp.resume_args(None), Vec::<String>::new());
}

#[test]
fn the_closing_line_decides_the_turn_and_carries_its_usage() {
    let done = json!({
        "type": "done",
        "session_id": "sess-1",
        "usage": {"input_tokens": 11, "output_tokens": 7, "cache_read_tokens": 3, "cache_write_tokens": 2}
    });
    let effect = StreamSpec::for_program("omp").effect(&done);
    assert!(effect.finishes_turn);
    let usage = effect.usage.expect("usage on the closing line");
    assert_eq!((usage.input_tokens, usage.output_tokens), (11, 7));
    assert_eq!((usage.cache_read_tokens, usage.cache_write_tokens), (3, 2));
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(effect.session_id.as_deref(), Some("sess-1"));
    assert_eq!(effect.text, None);

    // `result` does not close a harness that ends on `done`.
    let other = StreamSpec::for_program("omp").effect(&json!({"type": "result"}));
    assert!(!other.finishes_turn);
    // A result-only harness ignores `done`.
    let agy = StreamSpec::for_program("agy").effect(&json!({"type": "done"}));
    assert!(!agy.finishes_turn);
}

#[test]
fn a_cost_is_kept_only_where_the_vendor_reports_one() {
    let result = json!({
        "type": "result",
        "cost_micros": 250_000,
        "usage": {"input_tokens": 1, "output_tokens": 2, "cache_read_tokens": 3, "cache_write_tokens": 4}
    });
    let with_cost = StreamSpec::for_program("opencode").effect(&result);
    let usage = with_cost.usage.expect("usage");
    assert_eq!(usage.reported_cost_micros, Some(250_000));
    let without = StreamSpec::for_program("agy").effect(&result);
    assert_eq!(without.usage.expect("usage").reported_cost_micros, None);
}

#[test]
fn answer_text_is_read_only_from_a_streaming_vendor() {
    let text = json!({"type": "text", "text": "pong"});
    let opencode = StreamSpec::for_program("opencode").effect(&text);
    assert!(!opencode.finishes_turn);
    assert_eq!(opencode.text.as_deref(), Some("pong"));
    // An empty or absent text is no answer.
    let empty = StreamSpec::for_program("opencode").effect(&json!({"type": "text", "text": ""}));
    assert_eq!(empty.text, None);
    // The result-only harness never takes text.
    assert_eq!(StreamSpec::for_program("agy").effect(&text).text, None);
}

#[test]
fn a_rate_limit_anywhere_is_a_wait_with_its_reset_time() {
    for program in ["agy", "omp", "opencode"] {
        let effect = StreamSpec::for_program(program)
            .effect(&json!({"type": "rate_limit", "resets_at_ms": 1_700_000_000_000i64}));
        assert_eq!(effect.quota_reset_at_ms, Some(1_700_000_000_000));
        assert!(!effect.finishes_turn);
    }
    let done = StreamSpec::for_program("omp").effect(&json!({"type": "done"}));
    assert_eq!(done.quota_reset_at_ms, None);
}
