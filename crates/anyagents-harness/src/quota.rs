//! When a vendor's quota refusal says the quota comes back.
//!
//! Vendors state the reset in their error text, either as an instant
//! (`resets at 2026-11-16T00:00:00Z`) or as a wait from when they answered
//! (`Resets in 86h43m28s`, `resets in 3d 14h`). One parser reads both, so every
//! adapter, persisted fault record and view derives the same reset time.

use serde_json::Value;

/// The error text a vendor line or persisted record carries: a string, or an
/// object's `message`.
pub fn error_text(error: &Value) -> Option<&str> {
    error
        .as_str()
        .or_else(|| error.get("message").and_then(Value::as_str))
}

/// The reset instant (epoch ms) `text` states, a wait counted from
/// `observed_at_ms` (when the vendor answered). `None` when the text names no
/// reset, or names one that cannot be read exactly.
pub fn reset_at_ms(text: &str, observed_at_ms: i64) -> Option<i64> {
    resets_at_ms(text)
        .or_else(|| resets_in_ms(text).map(|wait| observed_at_ms.saturating_add(wait)))
}

/// The reset a persisted fault record (`{error, quota_reset_at_ms,
/// timestamp_ms}`) names: its recorded reset, else the one its error text
/// states, counted from when it was recorded.
pub fn recorded_reset_at_ms(record: &Value) -> Option<i64> {
    if let Some(reset) = record.get("quota_reset_at_ms").and_then(Value::as_i64) {
        return Some(reset);
    }
    let recorded_at = record.get("timestamp_ms").and_then(Value::as_i64)?;
    let text = record.get("error").and_then(error_text)?;
    reset_at_ms(text, recorded_at)
}

/// `resets at <RFC 3339 instant>`.
pub fn resets_at_ms(text: &str) -> Option<i64> {
    let after = after_phrase(text, "resets at ")?;
    let token = after
        .split_whitespace()
        .next()?
        .trim_end_matches(['.', ',', ';', ')', ']', '"']);
    parse_rfc3339_utc_ms(token)
}

/// `resets in <duration>`, in milliseconds.
pub fn resets_in_ms(text: &str) -> Option<i64> {
    let after = after_phrase(text, "resets in ")?;
    // The duration runs up to the first character that is neither a digit,
    // a unit letter nor a space between components ("3d 14h").
    let end = after
        .char_indices()
        .find(|&(_, ch)| !(ch.is_ascii_alphanumeric() || ch == ' '))
        .map_or(after.len(), |(idx, _)| idx);
    let mut duration = String::new();
    for word in after[..end].split_whitespace() {
        if parse_duration_ms(word).is_none() {
            break;
        }
        duration.push_str(word);
    }
    parse_duration_ms(&duration).filter(|&ms| ms > 0)
}

/// A compact duration of `d`, `h`, `m` and `s` components (`86h43m28s`,
/// `3d14h`, `45m12s`, `90s`), in milliseconds. Any component may exceed its
/// usual range. `None` unless the whole token is such a duration.
pub fn parse_duration_ms(token: &str) -> Option<i64> {
    let mut total: i64 = 0;
    let mut num: i64 = 0;
    let mut has_digits = false;
    let mut matched_unit = false;
    for ch in token.chars() {
        if let Some(digit) = ch.to_digit(10) {
            num = num.checked_mul(10)?.checked_add(i64::from(digit))?;
            has_digits = true;
            continue;
        }
        if !has_digits {
            return None;
        }
        let factor = match ch.to_ascii_lowercase() {
            'd' => 86_400_000,
            'h' => 3_600_000,
            'm' => 60_000,
            's' => 1_000,
            _ => return None,
        };
        total = total.checked_add(num.checked_mul(factor)?)?;
        num = 0;
        has_digits = false;
        matched_unit = true;
    }
    (matched_unit && !has_digits).then_some(total)
}

/// The text after the first case-insensitive `phrase` (ASCII).
fn after_phrase<'a>(text: &'a str, phrase: &str) -> Option<&'a str> {
    let idx = text.to_ascii_lowercase().find(phrase)?;
    Some(text[idx + phrase.len()..].trim_start())
}

/// An RFC 3339 instant (`Z` or `±hh:mm` offset, optional fraction) in epoch ms.
pub fn parse_rfc3339_utc_ms(ts: &str) -> Option<i64> {
    let (date_part, rest) = ts.split_once(['T', 't'])?;
    let mut date_it = date_part.split('-');
    let year: i64 = date_it.next()?.parse().ok()?;
    let month: u32 = date_it.next()?.parse().ok()?;
    let day: u32 = date_it.next()?.parse().ok()?;
    if date_it.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let (time_core, offset_secs): (&str, i64) = if let Some(core) = rest.strip_suffix(['Z', 'z']) {
        (core, 0)
    } else {
        let idx = rest.rfind(['+', '-'])?;
        let (core, off) = rest.split_at(idx);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let mut off_it = off[1..].split(':');
        let oh: i64 = off_it.next()?.parse().ok()?;
        let om: i64 = off_it.next()?.parse().ok()?;
        if off_it.next().is_some() || !(0..=23).contains(&oh) || !(0..=59).contains(&om) {
            return None;
        }
        (core, sign * (oh * 3600 + om * 60))
    };

    let (hms, frac_ms) = match time_core.split_once('.') {
        Some((hms, frac)) => {
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let padded = format!("{frac:0<3}");
            let ms: i64 = padded[..3].parse().ok()?;
            (hms, ms)
        }
        None => (time_core, 0),
    };
    let mut hms_it = hms.split(':');
    let hour: i64 = hms_it.next()?.parse().ok()?;
    let min: i64 = hms_it.next()?.parse().ok()?;
    let sec: i64 = hms_it.next()?.parse().ok()?;
    if hms_it.next().is_some()
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&min)
        || !(0..=59).contains(&sec)
    {
        return None;
    }

    // Howard Hinnant days-from-civil (1970-01-01 = 0).
    let y = year - if month <= 2 { 1 } else { 0 };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = i64::from(month);
    let d = i64::from(day);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3600 + min * 60 + sec)?
        .checked_sub(offset_secs)?;
    secs.checked_mul(1_000)?.checked_add(frac_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const H: i64 = 3_600_000;
    const M: i64 = 60_000;
    const S: i64 = 1_000;
    const D: i64 = 24 * H;

    #[test]
    fn durations_of_any_size_parse_exactly() {
        assert_eq!(
            parse_duration_ms("86h43m28s"),
            Some(86 * H + 43 * M + 28 * S)
        );
        assert_eq!(parse_duration_ms("3d14h"), Some(3 * D + 14 * H));
        assert_eq!(parse_duration_ms("45m12s"), Some(45 * M + 12 * S));
        assert_eq!(parse_duration_ms("30h59m1s"), Some(30 * H + 59 * M + S));
        assert_eq!(parse_duration_ms("2d"), Some(2 * D));
        assert_eq!(parse_duration_ms("90s"), Some(90 * S));
        assert_eq!(parse_duration_ms("400h"), Some(400 * H));
        assert_eq!(parse_duration_ms("1D2H"), Some(D + 2 * H));
        for refused in [
            "",
            "h",
            "86",
            "86h43",
            "5x",
            "1h-2m",
            "1.5h",
            "99999999999999999999d",
        ] {
            assert_eq!(parse_duration_ms(refused), None, "{refused}");
        }
    }

    #[test]
    fn resets_in_reads_the_wait_from_vendor_text() {
        let agy = "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 86h43m28s.";
        assert_eq!(resets_in_ms(agy), Some(86 * H + 43 * M + 28 * S));
        assert_eq!(resets_in_ms("Resets in 3d14h."), Some(3 * D + 14 * H));
        assert_eq!(resets_in_ms("resets in 45m12s"), Some(45 * M + 12 * S));
        assert_eq!(
            resets_in_ms("Quota resets in 3d 14h 5m, then retry"),
            Some(3 * D + 14 * H + 5 * M)
        );
        assert_eq!(
            resets_in_ms("RESOURCE_EXHAUSTED: resets in 5h12m40s\")"),
            Some(5 * H + 12 * M + 40 * S)
        );
        assert_eq!(
            resets_in_ms("Resets in 2h when your plan renews"),
            Some(2 * H)
        );
        for refused in [
            "Resets in soon.",
            "Resets in 0s.",
            "quota reached",
            "Resets in .",
        ] {
            assert_eq!(resets_in_ms(refused), None, "{refused}");
        }
    }

    #[test]
    fn reset_at_counts_a_wait_from_when_the_vendor_answered() {
        let observed = 1_794_513_170_296;
        assert_eq!(
            reset_at_ms("Resets in 86h43m28s.", observed),
            Some(observed + 86 * H + 43 * M + 28 * S)
        );
        assert_eq!(
            reset_at_ms(
                "Your usage window resets at 2026-11-16T00:00:00Z.",
                observed
            ),
            Some(1_794_787_200_000)
        );
        assert_eq!(reset_at_ms("Quota exhausted.", observed), None);
    }

    #[test]
    fn a_persisted_record_names_its_reset_from_its_text_when_none_was_stored() {
        let recorded_at = 1_794_513_170_296;
        let stored =
            json!({"error": "Resets in 1h.", "quota_reset_at_ms": 42, "timestamp_ms": recorded_at});
        assert_eq!(recorded_reset_at_ms(&stored), Some(42));
        let null = json!({"error": "Individual quota reached. Resets in 86h43m28s.", "quota_reset_at_ms": null, "timestamp_ms": recorded_at});
        assert_eq!(
            recorded_reset_at_ms(&null),
            Some(recorded_at + 86 * H + 43 * M + 28 * S)
        );
        let object = json!({"error": {"message": "Resets in 3d14h"}, "timestamp_ms": recorded_at});
        assert_eq!(
            recorded_reset_at_ms(&object),
            Some(recorded_at + 3 * D + 14 * H)
        );
        let absolute = json!({"error": "quota exhausted; resets at 2026-11-16T00:00:00Z.", "timestamp_ms": recorded_at});
        assert_eq!(recorded_reset_at_ms(&absolute), Some(1_794_787_200_000));
        let untimed = json!({"error": "Resets in 1h."});
        assert_eq!(recorded_reset_at_ms(&untimed), None);
        let plain = json!({"error": "quota exhausted", "timestamp_ms": recorded_at});
        assert_eq!(recorded_reset_at_ms(&plain), None);
    }

    #[test]
    fn rfc3339_instants_parse_with_offsets_and_fractions() {
        assert_eq!(
            parse_rfc3339_utc_ms("2026-11-16T00:00:00Z"),
            Some(1_794_787_200_000)
        );
        assert_eq!(
            parse_rfc3339_utc_ms("2026-11-15T19:00:00.250-05:00"),
            Some(1_794_787_200_250)
        );
        assert_eq!(parse_rfc3339_utc_ms("2026-13-01T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_utc_ms("not a time"), None);
    }
}
