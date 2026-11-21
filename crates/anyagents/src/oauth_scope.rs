//! OAuth 2.0 scope syntax (RFC 6749 §3.3). Tokens are case-sensitive.

use std::collections::BTreeSet;

pub(crate) const ADMIN: &str = "admin:*";
pub(crate) const KNOWLEDGE_READ: &str = "knowledge:read";
pub(crate) const KNOWLEDGE_WRITE: &str = "knowledge:write";
pub(crate) const KNOWLEDGE_GRANT: &str = "knowledge:grant";

pub(crate) fn valid_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| matches!(b, b'!' | b'#'..=b'[' | b']'..=b'~'))
}

pub(crate) fn parse(raw: &str) -> Option<BTreeSet<&str>> {
    let scopes: BTreeSet<_> = raw.split(' ').collect();
    scopes.iter().all(|s| valid_token(s)).then_some(scopes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_use_exact_space_separated_scope_tokens() {
        for invalid in [
            "",
            " admin:*",
            "admin:* ",
            "admin:*\\",
            "admin:*\topenid",
            "admin:*  openid",
            "\"admin:*\"",
            "é",
        ] {
            assert_eq!(parse(invalid), None);
        }
        for other in ["ADMIN:*", "admin:read", "knowledge:*", "admin:*,openid"] {
            assert!(!parse(other).unwrap().contains(ADMIN));
        }
        assert!(parse("openid admin:*").unwrap().contains(ADMIN));
    }
}
