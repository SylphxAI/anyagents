//! Address grammar for `send`.
//!
//! `agent:<name>` and a bare name are an agent. `group:<name>` is a named
//! group. `telegram:chat:<id>[:<thread>]` and `telegram:user:<id>[:<message>]`
//! are channel targets: a forum topic, and the message a reply answers.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One destination. A bare name is an agent; its canonical form is `agent:<name>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    Agent(String),
    Group(String),
    /// A chat, and the forum topic inside it when the address names one.
    TelegramChat { chat: String, thread: Option<String> },
    /// A person, and the message being answered when the address names one.
    TelegramUser { user: String, message: Option<String> },
}

/// Why an address string was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressError {
    pub input: String,
    pub reason: &'static str,
}

impl std::fmt::Display for AddressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid address '{}': {}", self.input, self.reason)
    }
}

impl std::error::Error for AddressError {}

impl Address {
    /// Parse one address. Whitespace is rejected rather than trimmed.
    pub fn parse(input: &str) -> Result<Self, AddressError> {
        let err = |reason: &'static str| AddressError {
            input: input.to_string(),
            reason,
        };
        if input.is_empty() || input.chars().any(char::is_whitespace) {
            return Err(err("address is empty or contains whitespace"));
        }
        if let Some(name) = input.strip_prefix("agent:") {
            return name_token(name)
                .map(Self::Agent)
                .ok_or_else(|| err("empty agent name"));
        }
        if let Some(name) = input.strip_prefix("group:") {
            return name_token(name)
                .map(Self::Group)
                .ok_or_else(|| err("empty group name"));
        }
        if let Some(rest) = input.strip_prefix("telegram:chat:") {
            return match split_optional(rest) {
                (Some(chat), thread) => Ok(Self::TelegramChat { chat, thread }),
                (None, _) => Err(err("empty or malformed telegram chat id")),
            };
        }
        if let Some(rest) = input.strip_prefix("telegram:user:") {
            return match split_optional(rest) {
                (Some(user), message) => Ok(Self::TelegramUser { user, message }),
                (None, _) => Err(err("empty or malformed telegram user id")),
            };
        }
        if input.contains(':') {
            return Err(err("unknown address scheme"));
        }
        name_token(input)
            .map(Self::Agent)
            .ok_or_else(|| err("empty agent name"))
    }

    /// Stable string form. Bare names are written as `agent:<name>`.
    pub fn canonical(&self) -> String {
        match self {
            Self::Agent(name) => format!("agent:{name}"),
            Self::Group(name) => format!("group:{name}"),
            Self::TelegramChat { chat, thread } => match thread {
                Some(thread) => format!("telegram:chat:{chat}:{thread}"),
                None => format!("telegram:chat:{chat}"),
            },
            Self::TelegramUser { user, message } => match message {
                Some(message) => format!("telegram:user:{user}:{message}"),
                None => format!("telegram:user:{user}"),
            },
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.canonical())
    }
}

impl Serialize for Address {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.canonical())
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// Agent and group names cannot contain a colon, so the scheme stays unambiguous.
fn name_token(value: &str) -> Option<String> {
    if value.is_empty() || value.contains(':') || value.chars().any(char::is_whitespace) {
        None
    } else {
        Some(value.to_string())
    }
}

/// `a` or `a:b`. A first part that is empty or malformed is `None`, and so is a
/// second part that is wrong.
fn split_optional(value: &str) -> (Option<String>, Option<String>) {
    match value.split_once(':') {
        Some((first, rest)) => match id_token(rest) {
            Some(rest) => (id_token(first), Some(rest)),
            None => (None, None),
        },
        None => (id_token(value), None),
    }
}

fn id_token(value: &str) -> Option<String> {
    if value.is_empty() || value.contains(':') || value.chars().any(char::is_whitespace) {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_form() {
        assert_eq!(
            Address::parse("alpha").unwrap(),
            Address::Agent("alpha".into())
        );
        assert_eq!(
            Address::parse("agent:alpha").unwrap(),
            Address::Agent("alpha".into())
        );
        assert_eq!(Address::parse("alpha").unwrap().canonical(), "agent:alpha");
        assert_eq!(
            Address::parse("group:crew").unwrap(),
            Address::Group("crew".into())
        );
        assert_eq!(
            Address::parse("telegram:chat:-100123").unwrap(),
            Address::TelegramChat {
                chat: "-100123".into(),
                thread: None
            }
        );
        assert_eq!(
            Address::parse("telegram:user:42").unwrap(),
            Address::TelegramUser {
                user: "42".into(),
                message: None
            }
        );
        assert_eq!(
            Address::parse("telegram:chat:-100123:7").unwrap().canonical(),
            "telegram:chat:-100123:7"
        );
        assert_eq!(
            Address::parse("telegram:user:42:9").unwrap().canonical(),
            "telegram:user:42:9"
        );
    }

    #[test]
    fn rejects_unknown_and_empty() {
        assert!(Address::parse("").is_err());
        assert!(Address::parse("has space").is_err());
        assert!(Address::parse("agent:").is_err());
        assert!(Address::parse("agent:a:b").is_err());
        assert!(Address::parse("group:").is_err());
        assert!(Address::parse("telegram:chat:").is_err());
        assert!(Address::parse("telegram:user:").is_err());
        assert!(Address::parse("telegram:chat:1:").is_err());
        assert!(Address::parse("telegram:user:1::2").is_err());
        assert!(Address::parse("telegram:room:1").is_err());
        assert!(Address::parse("email:a@b.c").is_err());
    }

    #[test]
    fn serde_uses_the_canonical_string() {
        let address = Address::parse("alpha").unwrap();
        let json = serde_json::to_string(&address).unwrap();
        assert_eq!(json, "\"agent:alpha\"");
        let back: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(back, address);
    }
}
