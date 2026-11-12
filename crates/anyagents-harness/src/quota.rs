//! Unit-explicit quota timing. Protocol adapters select fields at their boundary;
//! this module deliberately has no JSON traversal or prose parser.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaResetInput {
    UnixMilliseconds(i64),
    UnixSeconds(i64),
    AfterMilliseconds(i64),
    AfterSeconds(i64),
}

/// Invalid or overflowing provider values are unknown, never wrapped deadlines.
pub fn extract_quota_reset(input: QuotaResetInput, now_ms: i64) -> Option<i64> {
    let (value, relative, seconds) = match input {
        QuotaResetInput::UnixMilliseconds(n) => (n, false, false),
        QuotaResetInput::UnixSeconds(n) => (n, false, true),
        QuotaResetInput::AfterMilliseconds(n) => (n, true, false),
        QuotaResetInput::AfterSeconds(n) => (n, true, true),
    };
    if value < 0 || (!relative && value == 0) { return None; }
    let ms = if seconds { value.checked_mul(1000)? } else { value };
    if relative { now_ms.checked_add(ms) } else { Some(ms) }
}

/// Unknown reset is distinct from a provider deadline for operator visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaRetry {
    Known { retry_at_ms: i64 },
    ResetUnknown { retry_at_ms: i64 },
}

impl QuotaRetry {
    pub fn retry_at_ms(self) -> i64 {
        match self {
            Self::Known { retry_at_ms } | Self::ResetUnknown { retry_at_ms } => retry_at_ms,
        }
    }
}

/// Called only after a protocol has positively identified a quota rejection.
/// Unknown reset retries grow from one minute to fifteen minutes, without ever
/// admitting the failed delivery as successful or spinning on a past deadline.
pub fn quota_retry(reset_at_ms: Option<i64>, now_ms: i64, attempt: u32) -> QuotaRetry {
    match reset_at_ms.filter(|reset| *reset > now_ms) {
        Some(retry_at_ms) => QuotaRetry::Known { retry_at_ms },
        None => QuotaRetry::ResetUnknown {
            retry_at_ms: now_ms.saturating_add((60_000_i64 << attempt.min(4)).min(900_000)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_are_explicit_not_guessed_by_magnitude() {
        assert_eq!(extract_quota_reset(QuotaResetInput::UnixMilliseconds(100), 1), Some(100));
        assert_eq!(extract_quota_reset(QuotaResetInput::UnixSeconds(100), 1), Some(100_000));
        assert_eq!(extract_quota_reset(QuotaResetInput::AfterSeconds(30), 1000), Some(31_000));
        assert_eq!(extract_quota_reset(QuotaResetInput::AfterMilliseconds(30), 1000), Some(1030));
    }

    #[test]
    fn invalid_and_overflowing_values_stay_unknown() {
        assert_eq!(extract_quota_reset(QuotaResetInput::AfterSeconds(-1), 1000), None);
        assert_eq!(extract_quota_reset(QuotaResetInput::UnixSeconds(i64::MAX), 1000), None);
        assert_eq!(extract_quota_reset(QuotaResetInput::AfterMilliseconds(1), i64::MAX), None);
        assert_eq!(extract_quota_reset(QuotaResetInput::UnixMilliseconds(0), 1000), None);
    }

    #[test]
    fn known_deadline_is_preserved_and_unknown_backoff_is_bounded() {
        assert_eq!(quota_retry(Some(2000), 1000, 99), QuotaRetry::Known { retry_at_ms: 2000 });
        assert_eq!(quota_retry(None, 1000, 0), QuotaRetry::ResetUnknown { retry_at_ms: 61_000 });
        assert_eq!(quota_retry(None, 1000, u32::MAX), QuotaRetry::ResetUnknown { retry_at_ms: 901_000 });
        assert_eq!(quota_retry(Some(999), 1000, 1).retry_at_ms(), 121_000);
    }
}
