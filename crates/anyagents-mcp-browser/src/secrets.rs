//! In-memory secret resolution (`op://<vault>/<item>/<field>`, `env:<VAR>`).
//!
//! Resolved values live in [`Secret`] (redacted `Debug`, zeroized on drop) and
//! are never logged, echoed in errors, or returned in MCP tool responses.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use agent_workstation::Secret;

use crate::chromium::is_executable_file;
use crate::session::BrowserError;

const RESERVED: &str = "ANYAGENTS_AGENT_ ANYAGENTS_CAPABILITY ANYAGENTS_SESSION_ \
ANYAGENTS_MCP_ ANYAGENTS_OPERATOR_ ANYAGENTS_MASTER_ ANYAGENTS_ADMIN_ ANYAGENTS_WEBHOOK_ \
ANYAGENTS_CALLBACK_ ANYAGENTS_GIT_ ANYAGENTS_S3_ OP_SERVICE_ACCOUNT_ OP_CONNECT_TOKEN";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeInput {
    Plain(String),
    SecretRef(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretSource {
    OpRead(String),
    Env(String),
}

fn res_err(m: impl Into<String>) -> BrowserError {
    BrowserError::new("SECRET_RESOLUTION_ERROR", m)
}

pub fn parse_secret_ref(raw: &str) -> Result<SecretSource, BrowserError> {
    let s = raw.trim();
    if s.is_empty() || s.bytes().any(|b| matches!(b, 0 | b'\r' | b'\n')) {
        return Err(BrowserError::invalid_input("invalid `secret_ref`"));
    }
    if let Some(rest) = s.strip_prefix("op://") {
        if rest.split('/').filter(|p| !p.is_empty()).count() < 3 {
            return Err(BrowserError::invalid_input(
                "expected `op://<vault>/<item>/<field>`",
            ));
        }
        return Ok(SecretSource::OpRead(s.into()));
    }
    if let Some(rest) = s.strip_prefix("env://").or_else(|| s.strip_prefix("env:")) {
        let v = rest.trim();
        let ok = v.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !ok {
            return Err(BrowserError::invalid_input("expected `env:<VAR_NAME>`"));
        }
        let up = v.to_ascii_uppercase();
        if RESERVED.split_whitespace().any(|p| up.starts_with(p)) {
            return Err(BrowserError::new(
                "REFUSED",
                "reserved credential env var refused",
            ));
        }
        return Ok(SecretSource::Env(v.into()));
    }
    Err(BrowserError::invalid_input(
        "expected `op://<vault>/<item>/<field>` or `env:<VAR>`",
    ))
}

pub fn classify_type_input(
    text: Option<&str>,
    secret_ref: Option<&str>,
) -> Result<TypeInput, BrowserError> {
    if let Some(r) = secret_ref.map(str::trim) {
        if text.is_some_and(|t| !t.is_empty()) {
            return Err(BrowserError::invalid_input(
                "pass `text` or `secret_ref`, not both",
            ));
        }
        parse_secret_ref(r)?;
        return Ok(TypeInput::SecretRef(r.into()));
    }
    let raw = text.ok_or_else(|| BrowserError::invalid_input("requires `text` or `secret_ref`"))?;
    let t = raw.trim();
    if t.starts_with("op://") || t.starts_with("env:") {
        parse_secret_ref(t)?;
        return Ok(TypeInput::SecretRef(t.into()));
    }
    Ok(TypeInput::Plain(raw.into()))
}

pub fn resolve_secret(secret_ref: &str) -> Result<Secret, BrowserError> {
    resolve_secret_with(secret_ref, None, |k| std::env::var(k).ok())
}

pub fn resolve_secret_with<F>(
    secret_ref: &str,
    op_bin: Option<&Path>,
    env: F,
) -> Result<Secret, BrowserError>
where
    F: Fn(&str) -> Option<String>,
{
    match parse_secret_ref(secret_ref)? {
        SecretSource::OpRead(r) => run_op_read(op_bin, &r).map(Secret::new),
        SecretSource::Env(var) => env(&var)
            .filter(|v| !v.is_empty())
            .map(Secret::new)
            .ok_or_else(|| res_err("environment secret reference is unset or empty")),
    }
}

pub fn sanitize_secret_fill_error(err: BrowserError) -> BrowserError {
    const SAFE: &str = "STALE_REF STALE_TARGET NOT_FOUND HUMAN_IN_CONTROL DESKTOP_IN_USE \
    TIMEOUT INVALID_INPUT SECRET_UNAVAILABLE SECRET_RESOLUTION_ERROR REFUSED";
    if SAFE.split_whitespace().any(|c| c == err.code) {
        err
    } else {
        BrowserError::new(err.code, "secret credential fill failed (details redacted)")
    }
}

fn run_op_read(explicit: Option<&Path>, secret_ref: &str) -> Result<String, BrowserError> {
    let bin = explicit
        .map(Path::to_path_buf)
        .filter(|p| is_executable_file(p))
        .or_else(|| {
            if explicit.is_some() {
                return None;
            }
            let home = std::env::var_os("HOME").filter(|h| !h.is_empty());
            "/usr/local/bin/op /usr/bin/op /opt/homebrew/bin/op /bin/op /snap/bin/op"
                .split_whitespace()
                .map(PathBuf::from)
                .chain(home.map(|h| PathBuf::from(h).join(".local/bin/op")))
                .find(|p| is_executable_file(p))
        })
        .ok_or_else(|| BrowserError::new("SECRET_UNAVAILABLE", "1Password CLI (`op`) missing"))?;
    let out = Command::new(&bin)
        .args(["read", "--no-newline", secret_ref])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("DESK_SECRET_READ", "1")
        .output()
        .map_err(|e| res_err(format!("failed executing `op`: {e}")))?;
    if !out.status.success() {
        let code = out.status.code().unwrap_or(-1);
        return Err(res_err(format!("1Password failed (exit code {code})")));
    }
    let val = String::from_utf8_lossy(&out.stdout)
        .trim_matches(['\r', '\n'])
        .to_string();
    if val.trim().is_empty() {
        return Err(res_err("1Password returned an empty value"));
    }
    Ok(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_ref_parse_env_redaction_and_op_cli() {
        assert!(parse_secret_ref("op://V/I/p").is_ok());
        assert_eq!(
            parse_secret_ref("env:PASS").unwrap(),
            SecretSource::Env("PASS".into())
        );
        for bad in ["", "op://a/b", "op://a/b/c\n", "env:1BAD", "plain"] {
            assert!(parse_secret_ref(bad).is_err(), "{bad}");
        }
        let blk = "env:ANYAGENTS_AGENT_TOKEN env:ANYAGENTS_OPERATOR_TOKEN \
            env:ANYAGENTS_S3_SECRET_ACCESS_KEY env:OP_SERVICE_ACCOUNT_TOKEN";
        for b in blk.split_whitespace() {
            assert_eq!(parse_secret_ref(b).unwrap_err().code, "REFUSED");
        }
        let s = resolve_secret_with("env:P", None, |k| (k == "P").then(|| "s3cr3t".into()));
        assert_eq!(format!("{:?}", s.unwrap()), "Secret(<redacted>)");
        assert!(resolve_secret_with("env:P", None, |_| None).is_err());
        assert!(resolve_secret_with("env:P", None, |_| Some("".into())).is_err());
        assert!(classify_type_input(Some("op://v/i/f"), None).is_ok());
        assert!(classify_type_input(Some("plain"), Some("op://v/i/f")).is_err());
        let red = sanitize_secret_fill_error(BrowserError::new("PROTOCOL_ERROR", "leaked secret"));
        assert!(!red.message.contains("leaked secret") && red.message.contains("redacted"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = tempfile::tempdir().unwrap();
            let shim = dir.path().join("fake-op");
            let s = "#!/bin/sh\n[ \"$DESK_SECRET_READ\" = 1 ] || exit 11\ncase \"$*\" in\n 'read --no-newline op://v/i/p') printf 'top-secret';;\n 'read --no-newline op://v/i/e') printf ' \\n';;\n *) echo LEAK_ERR >&2; echo LEAK_OUT; exit 7;;\nesac\n";
            std::fs::write(&shim, s).unwrap();
            std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o700)).unwrap();
            let run = |r| resolve_secret_with(r, Some(&shim), |_| None);
            let ok = run("op://v/i/p").unwrap();
            assert_eq!(format!("{ok:?}"), "Secret(<redacted>)");
            assert!(run("op://v/i/e").unwrap_err().message.contains("empty"));
            let e = run("op://v/i/x").unwrap_err().message;
            assert!(
                e.contains("exit code 7") && !e.contains("LEAK_ERR") && !e.contains("LEAK_OUT")
            );
        }
    }
}
