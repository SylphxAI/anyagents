//! Canonical harness selectors, separate from model provider/profile names.

/// Resolve legacy first-party harness names without changing provider names.
/// Call this only for harness selectors, never model IDs or provider profiles.
pub fn normalize_harness_name(harness: &str) -> &str {
    match harness.trim() {
        "sylphx" | "sylphx-agent" | "sylphx_agent" | "sylphx/default" => "spiron",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_first_party_harness_names_are_canonical() {
        for harness in [
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
            " sylphx ",
        ] {
            assert_eq!(normalize_harness_name(harness), "spiron", "{harness}");
        }
        for other in [
            "claude-code",
            "codex",
            "opencode",
            "sylphx/openai/gpt-6.1-sol",
        ] {
            assert_eq!(normalize_harness_name(other), other);
        }
    }
}
