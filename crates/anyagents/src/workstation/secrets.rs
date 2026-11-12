//! Secret references for credential fills: `op://<vault>/<item>/<field>`,
//! `op-otp://<vault>/<item>` (live TOTP), and `secret://<key>` (the local
//! credential store). Values are resolved in memory at the moment of
//! insertion and never returned, logged, or echoed in errors.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyagents_core::Error;

/// Whether a fill names a secret: an explicit `secret_ref`, or `text` that is
/// itself a secret reference.
pub fn is_secret_reference(text: &str) -> bool {
    let s = text.trim();
    s.starts_with("op://") || s.starts_with("op-otp://") || s.starts_with("secret://")
}

/// The resolver handed to the desktop session for one fill.
#[derive(Debug, Clone)]
pub struct SecretRefs {
    data: PathBuf,
}

impl SecretRefs {
    /// Resolve against the data directory's credential store and the `op` CLI.
    pub fn new(data: &Path) -> Self {
        Self {
            data: data.to_path_buf(),
        }
    }
}

#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl agent_workstation::SecretResolver for SecretRefs {
    async fn resolve(
        &self,
        secret_ref: &str,
    ) -> agent_workstation::Result<agent_workstation::Secret> {
        let data = self.data.clone();
        let secret_ref = secret_ref.to_string();
        let resolved = tokio::task::spawn_blocking(move || {
            resolve_fill_value(&data, None, Some(&secret_ref)).map(|(value, _)| value)
        })
        .await
        .map_err(|_| agent_workstation::Error::Refused("secret resolution stopped".into()))?;
        resolved
            .map(agent_workstation::Secret::new)
            .map_err(|e| agent_workstation::Error::Refused(e.to_string()))
    }
}

/// Inherit the provider's documented HOME, OP_CONFIG_DIR and
/// OP_SERVICE_ACCOUNT_TOKEN settings while marking the child as an in-memory
/// daemon secret read (`DESK_SECRET_READ=1`) with piped stdio so output-guard
/// shims permit consumed pipe capture without exposing plaintext.
fn configure_op_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("DESK_SECRET_READ", "1");
}

fn build_op_secret_command(op_program: &std::ffi::OsStr, args: &[&str]) -> Command {
    let mut cmd = Command::new(op_program);
    cmd.args(args);
    configure_op_command(&mut cmd);
    cmd
}

/// Resolve a secret reference (`op://...`, `op-otp://...`, `secret://...`) in memory
/// without ever echoing the plaintext secret in errors, logs, or responses.
pub fn resolve_fill_value(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
) -> Result<(String, Option<String>), Error> {
    resolve_fill_value_with_op(data, raw_text, secret_ref, std::ffi::OsStr::new("op"))
}

fn resolve_fill_value_with_op(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
    op_program: &std::ffi::OsStr,
) -> Result<(String, Option<String>), Error> {
    let candidate = secret_ref
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| raw_text.map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or("");

    if candidate.starts_with("op://") {
        let parts: Vec<&str> = candidate
            .trim_start_matches("op://")
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() < 3 {
            return Err(Error::Message(
                "invalid 1Password secret reference format (expected op://<vault>/<item>/<field>)"
                    .into(),
            ));
        }
        let mut cmd = build_op_secret_command(op_program, &["read", "--no-newline", candidate]);
        let out = cmd
            .output()
            .map_err(|_| Error::Message("failed to invoke `op` CLI for secret reference".into()))?;
        if !out.status.success() {
            return Err(Error::Message(format!(
                "1Password secret resolution failed (exit code {})",
                out.status.code().unwrap_or(-1)
            )));
        }
        let secret = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if secret.is_empty() {
            return Err(Error::Message(
                "1Password secret resolution returned an empty value".into(),
            ));
        }
        return Ok((secret, Some(candidate.to_string())));
    }

    if let Some(rest) = candidate.strip_prefix("op-otp://") {
        let trimmed = rest.trim_matches('/');
        if trimmed.is_empty() {
            return Err(Error::Message(
                "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)".into(),
            ));
        }
        let mut cmd = if let Some((vault, item)) = trimmed.split_once('/') {
            let vault = vault.trim().trim_matches('/');
            let item = item.trim().trim_matches('/');
            if vault.is_empty() || item.is_empty() {
                return Err(Error::Message(
                    "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)"
                        .into(),
                ));
            }
            build_op_secret_command(
                op_program,
                &["item", "get", item, "--vault", vault, "--otp"],
            )
        } else {
            build_op_secret_command(op_program, &["item", "get", trimmed, "--otp"])
        };
        let out = cmd
            .output()
            .map_err(|_| Error::Message("failed to invoke `op` CLI for OTP reference".into()))?;
        if !out.status.success() {
            return Err(Error::Message(format!(
                "1Password OTP resolution failed (exit code {})",
                out.status.code().unwrap_or(-1)
            )));
        }
        let otp = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if otp.is_empty() {
            return Err(Error::Message(
                "1Password OTP resolution returned an empty value".into(),
            ));
        }
        return Ok((otp, Some(candidate.to_string())));
    }

    if let Some(secret_key) = candidate.strip_prefix("secret://") {
        use anyagents_core::{CredentialStore, FileCredentialStore};
        let store = FileCredentialStore::new(data);
        let secret = store.read(secret_key.trim())?;
        let val = secret
            .as_str()
            .map(|s| s.trim().to_string())
            .map_err(|_| Error::Message("stored secret is not valid UTF-8 text".into()))?;
        return Ok((val, Some(candidate.to_string())));
    }

    if secret_ref.is_some() {
        return Err(Error::Message(
            "secret_ref must start with `op://`, `op-otp://`, or `secret://`".into(),
        ));
    }

    Ok((raw_text.unwrap_or("").to_string(), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_configuration_preserves_documented_operator_settings() {
        let mut command = Command::new("env");
        command
            .env_clear()
            .env("HOME", "/synthetic/home")
            .env("OP_CONFIG_DIR", "/synthetic/config")
            .env("OP_SERVICE_ACCOUNT_TOKEN", "synthetic-not-a-credential");
        configure_op_command(&mut command);
        let envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> = command.get_envs().collect();
        assert!(envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));
        let output = command.output().unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let mut settings = stdout.lines().collect::<Vec<_>>();
        settings.sort();
        assert_eq!(
            settings,
            [
                "DESK_SECRET_READ=1",
                "HOME=/synthetic/home",
                "OP_CONFIG_DIR=/synthetic/config",
                "OP_SERVICE_ACCOUNT_TOKEN=synthetic-not-a-credential",
            ]
        );
    }

    #[test]
    fn op_secret_and_otp_commands_include_desk_secret_read_env() {
        let read_cmd = build_op_secret_command(
            std::ffi::OsStr::new("op"),
            &["read", "--no-newline", "op://vault/item/password"],
        );
        let read_args: Vec<&std::ffi::OsStr> = read_cmd.get_args().collect();
        assert_eq!(
            read_args,
            [
                std::ffi::OsStr::new("read"),
                std::ffi::OsStr::new("--no-newline"),
                std::ffi::OsStr::new("op://vault/item/password"),
            ]
        );
        let read_envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> =
            read_cmd.get_envs().collect();
        assert!(read_envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));

        let otp_cmd = build_op_secret_command(
            std::ffi::OsStr::new("op"),
            &["item", "get", "item", "--vault", "vault", "--otp"],
        );
        let otp_args: Vec<&std::ffi::OsStr> = otp_cmd.get_args().collect();
        assert_eq!(
            otp_args,
            [
                std::ffi::OsStr::new("item"),
                std::ffi::OsStr::new("get"),
                std::ffi::OsStr::new("item"),
                std::ffi::OsStr::new("--vault"),
                std::ffi::OsStr::new("vault"),
                std::ffi::OsStr::new("--otp"),
            ]
        );
        let otp_envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> =
            otp_cmd.get_envs().collect();
        assert!(otp_envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));
    }

    /// Write an executable from a child process. A file this process held
    /// open for writing could be inherited by a sibling test's fork and make
    /// exec fail with "Text file busy"; the child's descriptor never is.
    #[cfg(unix)]
    fn write_executable(path: &Path, text: &str) {
        use std::io::Write as _;
        let mut child = Command::new("/bin/sh")
            .args(["-c", "cat > \"$1\" && chmod 700 \"$1\"", "sh"])
            .arg(path)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success());
    }

    #[cfg(unix)]
    #[test]
    fn op_secret_and_otp_resolution_sets_desk_secret_read_and_never_leaks_values() {
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("fake-op");
        write_executable(
            &shim,
            r#"#!/bin/sh
if [ "${DESK_SECRET_READ:-}" != "1" ]; then
  echo "desk-tool-guard: refusing to print a 1Password secret value" >&2
  exit 2
fi
case "$*" in
  "read --no-newline op://vault-alpha/item-beta/password")
    printf 'in-memory-secret-value
'
    exit 0
    ;;
  "read --no-newline op://vault-alpha/item-beta/otp?attribute=otp")
    printf '987654
'
    exit 0
    ;;
  "item get item-beta --vault vault-alpha --otp")
    printf '654321
'
    exit 0
    ;;
  "item get standalone-item --otp")
    printf '112233
'
    exit 0
    ;;
  "read --no-newline op://vault-alpha/partial-leak/password" | "item get partial-leak --vault vault-alpha --otp")
    printf 'partial-stdout-secret-bytes'
    printf 'partial-stderr-secret-bytes op://vault-alpha/partial-leak/password
' >&2
    exit 2
    ;;
  "read --no-newline op://vault-alpha/empty-item/password" | "item get empty-item --vault vault-alpha --otp")
    printf '   
'
    exit 0
    ;;
  *)
    printf 'unexpected-args-secret-fragment
' >&2
    exit 1
    ;;
esac
"#,
        );

        // Counterfactual check: without DESK_SECRET_READ=1, the guard shim refuses with exit code 2.
        let unguarded = Command::new(&shim)
            .env_remove("DESK_SECRET_READ")
            .args([
                "read",
                "--no-newline",
                "op://vault-alpha/item-beta/password",
            ])
            .output()
            .unwrap();
        assert_eq!(unguarded.status.code(), Some(2));

        // Positive checks: op:// and op-otp:// pass DESK_SECRET_READ=1 and resolve in memory.
        let (secret, src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op://vault-alpha/item-beta/password"),
            shim.as_os_str(),
        )
        .unwrap();
        assert_eq!(secret, "in-memory-secret-value");
        assert_eq!(src.as_deref(), Some("op://vault-alpha/item-beta/password"));

        let (attr_otp, attr_src) = resolve_fill_value_with_op(
            dir.path(),
            Some("op://vault-alpha/item-beta/otp?attribute=otp"),
            None,
            shim.as_os_str(),
        )
        .unwrap();
        assert_eq!(attr_otp, "987654");
        assert_eq!(
            attr_src.as_deref(),
            Some("op://vault-alpha/item-beta/otp?attribute=otp")
        );

        let (otp, otp_src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op-otp://vault-alpha/item-beta"),
            shim.as_os_str(),
        )
        .unwrap();
        assert_eq!(otp, "654321");
        assert_eq!(otp_src.as_deref(), Some("op-otp://vault-alpha/item-beta"));

        let (single_otp, single_src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op-otp://standalone-item"),
            shim.as_os_str(),
        )
        .unwrap();
        assert_eq!(single_otp, "112233");
        assert_eq!(single_src.as_deref(), Some("op-otp://standalone-item"));

        // Negative checks: partial stdout/stderr on non-zero exit must never appear in errors.
        for failing_ref in [
            "op://vault-alpha/partial-leak/password",
            "op-otp://vault-alpha/partial-leak",
        ] {
            let err =
                resolve_fill_value_with_op(dir.path(), None, Some(failing_ref), shim.as_os_str())
                    .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("exit code 2"), "{msg}");
            for forbidden in [
                "partial-stdout-secret-bytes",
                "partial-stderr-secret-bytes",
                "vault-alpha",
                "partial-leak",
                "password",
            ] {
                assert!(!msg.contains(forbidden), "leaked {forbidden} in {msg}");
            }

            let public_err = crate::workstation::ToolError {
                code: "REFUSED",
                message: msg.clone(),
                hint: "",
                retryable: false,
            }
            .into_error(true)
            .to_string();
            assert!(public_err.contains("REFUSED"), "{public_err}");
            assert!(
                public_err.contains("browser secret input failed (provider/page details withheld)"),
                "{public_err}"
            );
            for forbidden in [
                "partial-stdout-secret-bytes",
                "partial-stderr-secret-bytes",
                "vault-alpha",
                "partial-leak",
                "password",
            ] {
                assert!(
                    !public_err.contains(forbidden),
                    "leaked {forbidden} in {public_err}"
                );
            }
        }

        // Empty stdout, missing binary, and malformed references never echo secret or ref fragments.
        for empty_ref in [
            "op://vault-alpha/empty-item/password",
            "op-otp://vault-alpha/empty-item",
        ] {
            let msg =
                resolve_fill_value_with_op(dir.path(), None, Some(empty_ref), shim.as_os_str())
                    .unwrap_err()
                    .to_string();
            assert!(msg.contains("empty value"), "{msg}");
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("empty-item"), "{msg}");
        }

        let missing_bin = dir.path().join("nonexistent-op");
        for valid_ref in [
            "op://vault-alpha/item-beta/password",
            "op-otp://vault-alpha/item-beta",
        ] {
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(valid_ref),
                missing_bin.as_os_str(),
            )
            .unwrap_err()
            .to_string();
            assert!(msg.contains("failed to invoke `op` CLI"), "{msg}");
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("item-beta"), "{msg}");
        }

        for malformed_ref in [
            "op://vault-alpha/only-two",
            "op-otp:///",
            "op-otp://vault-alpha/   ",
            "unsupported://vault-alpha/item-beta",
        ] {
            let msg =
                resolve_fill_value_with_op(dir.path(), None, Some(malformed_ref), shim.as_os_str())
                    .unwrap_err()
                    .to_string();
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("item-beta"), "{msg}");
            assert!(!msg.contains("only-two"), "{msg}");
        }
    }
}
