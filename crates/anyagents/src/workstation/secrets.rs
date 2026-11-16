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

const OP_CONNECT_TOKEN_FILES_ENV: &str = "OP_CONNECT_TOKEN_FILES";
const OP_CONNECT_TOKEN_ENV: &str = "OP_CONNECT_TOKEN";

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
    resolve_fill_value_with_connect(data, raw_text, secret_ref, None, None)
}

/// The `op` CLI: `op_program` in tests, otherwise the toolchain's declared
/// tool (capability `browser.secret_ref`).
fn resolve_op_program(op_program: Option<&std::ffi::OsStr>) -> Result<std::ffi::OsString, Error> {
    match op_program {
        Some(bin) => Ok(bin.to_os_string()),
        None => anyagents_toolchain::tool("op", "browser.secret_ref")
            .map(|tool| tool.path.into_os_string())
            .map_err(|err| Error::Message(err.to_string())),
    }
}

fn parse_connect_token_files(raw: &std::ffi::OsStr) -> Vec<PathBuf> {
    std::env::split_paths(raw)
        .filter(|path| !path.as_os_str().is_empty())
        .collect()
}

fn configured_connect_token_files_from_env() -> Option<Vec<PathBuf>> {
    let raw = std::env::var_os(OP_CONNECT_TOKEN_FILES_ENV)?;
    let paths = parse_connect_token_files(&raw);
    if paths.is_empty() { None } else { Some(paths) }
}

fn read_connect_token_file(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn load_connect_tokens(paths: &[PathBuf]) -> Vec<String> {
    let mut tokens = Vec::new();
    for path in paths {
        if let Some(token) = read_connect_token_file(path)
            && !tokens.contains(&token)
        {
            tokens.push(token);
        }
    }
    tokens
}

fn is_connect_vault_refusal(out: &std::process::Output) -> bool {
    let stderr = String::from_utf8_lossy(&out.stderr).to_ascii_lowercase();
    let stdout = String::from_utf8_lossy(&out.stdout).to_ascii_lowercase();
    let combined = format!("{stderr}\n{stdout}");
    combined.contains("reason=ungranted")
        || combined.contains("ungranted")
        || combined.contains("403")
        || combined.contains("forbidden")
        || combined.contains("outside connect scope")
        || combined.contains("in connect grants")
}

enum OpAttemptError {
    VaultRefused(i32),
    Fatal(Error),
}

fn run_op_secret_attempt(
    op_bin: &std::ffi::OsStr,
    args: &[&str],
    connect_token: Option<&str>,
    label: &str,
) -> Result<String, OpAttemptError> {
    let mut cmd = build_op_secret_command(op_bin, args);
    if let Some(token) = connect_token {
        cmd.env(OP_CONNECT_TOKEN_ENV, token);
        // Scope each attempt to one token so an underlying wrapper does not re-iterate.
        cmd.env_remove(OP_CONNECT_TOKEN_FILES_ENV);
    }
    let out = cmd.output().map_err(|_| {
        OpAttemptError::Fatal(Error::Message(format!(
            "failed to invoke `op` CLI for {label} reference"
        )))
    })?;
    if !out.status.success() {
        let code = out.status.code().unwrap_or(-1);
        if is_connect_vault_refusal(&out) {
            return Err(OpAttemptError::VaultRefused(code));
        }
        return Err(OpAttemptError::Fatal(Error::Message(format!(
            "1Password {label} resolution failed (exit code {code})"
        ))));
    }
    let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if value.is_empty() {
        return Err(OpAttemptError::Fatal(Error::Message(format!(
            "1Password {label} resolution returned an empty value"
        ))));
    }
    Ok(value)
}

fn run_op_secret_with_connect(
    op_program: Option<&std::ffi::OsStr>,
    args: &[&str],
    connect_token_files: Option<&[PathBuf]>,
    label: &str,
) -> Result<String, Error> {
    let configured_from_env;
    let token_files = match connect_token_files {
        Some(files) => Some(files),
        None => {
            configured_from_env = configured_connect_token_files_from_env();
            configured_from_env.as_deref()
        }
    };

    let Some(paths) = token_files else {
        let op_bin = resolve_op_program(op_program)?;
        return match run_op_secret_attempt(&op_bin, args, None, label) {
            Ok(value) => Ok(value),
            Err(OpAttemptError::VaultRefused(code)) => Err(Error::Message(format!(
                "1Password {label} resolution failed (exit code {code})"
            ))),
            Err(OpAttemptError::Fatal(err)) => Err(err),
        };
    };

    let tokens = load_connect_tokens(paths);
    if tokens.is_empty() {
        return Err(Error::Message(format!(
            "1Password {label} resolution failed: no readable Connect token files"
        )));
    }

    let op_bin = resolve_op_program(op_program)?;
    let mut last_refusal_code = -1;
    for token in tokens {
        match run_op_secret_attempt(&op_bin, args, Some(&token), label) {
            Ok(value) => return Ok(value),
            Err(OpAttemptError::VaultRefused(code)) => {
                last_refusal_code = code;
            }
            Err(OpAttemptError::Fatal(err)) => return Err(err),
        }
    }

    Err(Error::Message(format!(
        "1Password {label} resolution failed (exit code {last_refusal_code})"
    )))
}

#[cfg(test)]
fn resolve_fill_value_with_op(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
    op_program: Option<&std::ffi::OsStr>,
) -> Result<(String, Option<String>), Error> {
    resolve_fill_value_with_connect(data, raw_text, secret_ref, op_program, None)
}

fn resolve_fill_value_with_connect(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
    op_program: Option<&std::ffi::OsStr>,
    connect_token_files: Option<&[PathBuf]>,
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
        let secret = run_op_secret_with_connect(
            op_program,
            &["read", "--no-newline", candidate],
            connect_token_files,
            "secret",
        )?;
        return Ok((secret, Some(candidate.to_string())));
    }

    if let Some(rest) = candidate.strip_prefix("op-otp://") {
        let trimmed = rest.trim_matches('/');
        if trimmed.is_empty() {
            return Err(Error::Message(
                "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)".into(),
            ));
        }
        let otp = if let Some((vault, item)) = trimmed.split_once('/') {
            let vault = vault.trim().trim_matches('/');
            let item = item.trim().trim_matches('/');
            if vault.is_empty() || item.is_empty() {
                return Err(Error::Message(
                    "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)"
                        .into(),
                ));
            }
            run_op_secret_with_connect(
                op_program,
                &["item", "get", item, "--vault", vault, "--otp"],
                connect_token_files,
                "OTP",
            )?
        } else {
            run_op_secret_with_connect(
                op_program,
                &["item", "get", trimmed, "--otp"],
                connect_token_files,
                "OTP",
            )?
        };
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
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(secret, "in-memory-secret-value");
        assert_eq!(src.as_deref(), Some("op://vault-alpha/item-beta/password"));

        let (attr_otp, attr_src) = resolve_fill_value_with_op(
            dir.path(),
            Some("op://vault-alpha/item-beta/otp?attribute=otp"),
            None,
            Some(shim.as_os_str()),
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
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(otp, "654321");
        assert_eq!(otp_src.as_deref(), Some("op-otp://vault-alpha/item-beta"));

        let (single_otp, single_src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op-otp://standalone-item"),
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(single_otp, "112233");
        assert_eq!(single_src.as_deref(), Some("op-otp://standalone-item"));

        // Negative checks: partial stdout/stderr on non-zero exit must never appear in errors.
        for failing_ref in [
            "op://vault-alpha/partial-leak/password",
            "op-otp://vault-alpha/partial-leak",
        ] {
            let err = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(failing_ref),
                Some(shim.as_os_str()),
            )
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
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(empty_ref),
                Some(shim.as_os_str()),
            )
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
                Some(missing_bin.as_os_str()),
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
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(malformed_ref),
                Some(shim.as_os_str()),
            )
            .unwrap_err()
            .to_string();
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("item-beta"), "{msg}");
            assert!(!msg.contains("only-two"), "{msg}");
        }
    }

    #[test]
    fn connect_token_file_parsing_and_loading_skip_empty_unreadable_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("connect-token");
        let blank = dir.path().join("connect-token-empty");
        let subdir = dir.path().join("subdir");
        let dup = dir.path().join("connect-token-dup");
        let second = dir.path().join("connect-token-secondary");
        let missing = dir.path().join("connect-token-missing");

        std::fs::write(&first, "  tok-first\n").unwrap();
        std::fs::write(&blank, "   \n\t  ").unwrap();
        std::fs::create_dir(&subdir).unwrap();
        std::fs::write(&dup, "tok-first").unwrap();
        std::fs::write(&second, "tok-second").unwrap();

        let joined = std::env::join_paths([
            Path::new(""),
            &missing,
            &blank,
            &subdir,
            &first,
            &dup,
            &second,
            Path::new(""),
        ])
        .unwrap();
        let parsed = parse_connect_token_files(&joined);
        assert_eq!(
            parsed,
            vec![
                missing.clone(),
                blank.clone(),
                subdir.clone(),
                first.clone(),
                dup.clone(),
                second.clone(),
            ]
        );
        assert!(parse_connect_token_files(std::ffi::OsStr::new("")).is_empty());

        let loaded = load_connect_tokens(&parsed);
        assert_eq!(
            loaded,
            vec!["tok-first".to_string(), "tok-second".to_string()]
        );

        let err = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op://vault-alpha/item-beta/password"),
            None,
            Some(&[missing, blank, subdir]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("no readable Connect token files"), "{err}");
        assert!(!err.contains("connect-token-missing"), "{err}");
        assert!(!err.contains("vault-alpha"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn connect_token_files_fall_through_on_ungranted_or_403_and_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("attempts.log");
        let shim = dir.path().join("fake-op-connect");
        let script = format!(
            r##"#!/bin/sh
if [ -n "$OP_CONNECT_TOKEN_FILES" ]; then
  echo "nested OP_CONNECT_TOKEN_FILES should be cleared per attempt" >&2
  exit 9
fi
printf '%s|%s\n' "$OP_CONNECT_TOKEN" "$*" >> '{log}'
case "$OP_CONNECT_TOKEN:$*" in
  tok-ungranted:*vault-beta*|tok-ungranted:*StandaloneBeta*)
    echo "op-connect-read-refused token=tok-ungranted reason=ungranted" >&2
    exit 2
    ;;
  tok-403:*vault-beta*)
    echo "HTTP 403 Forbidden for vault-beta" >&2
    exit 1
    ;;
  tok-403:*StandaloneBeta*)
    echo "OTP item not found or ambiguous in Connect grants" >&2
    exit 1
    ;;
  tok-granted-b:read\ --no-newline\ op://vault-beta/item-beta/password)
    printf 'secret-from-third-token\n'
    exit 0
    ;;
  tok-granted-b:item\ get\ item-beta\ --vault\ vault-beta\ --otp)
    printf '445566\n'
    exit 0
    ;;
  tok-granted-b:item\ get\ StandaloneBeta\ --otp)
    printf '778899\n'
    exit 0
    ;;
  tok-fatal:*)
    echo "network connection refused tok-fatal" >&2
    exit 3
    ;;
  *)
    echo "vault is outside Connect scope" >&2
    exit 2
    ;;
esac
"##,
            log = log.display(),
        );
        write_executable(&shim, &script);

        let missing = dir.path().join("missing-token");
        let empty = dir.path().join("empty-token");
        let tok_ungranted = dir.path().join("token-1");
        let tok_403 = dir.path().join("token-2");
        let tok_granted_b = dir.path().join("token-3");
        let tok_fatal = dir.path().join("token-fatal");
        std::fs::write(&empty, " \n").unwrap();
        std::fs::write(&tok_ungranted, "tok-ungranted\n").unwrap();
        std::fs::write(&tok_403, "tok-403\n").unwrap();
        std::fs::write(&tok_granted_b, "tok-granted-b\n").unwrap();
        std::fs::write(&tok_fatal, "tok-fatal\n").unwrap();

        let ordered = [
            missing,
            empty,
            tok_ungranted.clone(),
            tok_403.clone(),
            tok_granted_b.clone(),
            tok_fatal.clone(),
        ];

        let (secret, src) = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op://vault-beta/item-beta/password"),
            Some(shim.as_os_str()),
            Some(&ordered),
        )
        .unwrap();
        assert_eq!(secret, "secret-from-third-token");
        assert_eq!(src.as_deref(), Some("op://vault-beta/item-beta/password"));

        let (otp, _) = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op-otp://vault-beta/item-beta"),
            Some(shim.as_os_str()),
            Some(&ordered),
        )
        .unwrap();
        assert_eq!(otp, "445566");

        let (standalone_otp, _) = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op-otp://StandaloneBeta"),
            Some(shim.as_os_str()),
            Some(&ordered),
        )
        .unwrap();
        assert_eq!(standalone_otp, "778899");

        let attempts = std::fs::read_to_string(&log).unwrap();
        assert!(!attempts.contains("tok-fatal"), "{attempts}");
        let lines: Vec<&str> = attempts.lines().collect();
        assert_eq!(lines.len(), 9, "{attempts}");
        assert!(lines[0].starts_with("tok-ungranted|"));
        assert!(lines[1].starts_with("tok-403|"));
        assert!(lines[2].starts_with("tok-granted-b|"));

        // Fail closed when all configured token files refuse the target vault/item.
        let err = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op://vault-gamma/item-beta/password"),
            Some(shim.as_os_str()),
            Some(&[tok_ungranted.clone(), tok_403.clone()]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("exit code 2"), "{err}");
        for forbidden in ["tok-ungranted", "tok-403", "vault-gamma", "item-beta"] {
            assert!(!err.contains(forbidden), "leaked {forbidden} in {err}");
        }

        // Non-refusal failures stop immediately without falling through to later tokens.
        let before_fatal = std::fs::read_to_string(&log).unwrap().lines().count();
        let fatal_err = resolve_fill_value_with_connect(
            dir.path(),
            None,
            Some("op://vault-beta/item-beta/password"),
            Some(shim.as_os_str()),
            Some(&[tok_fatal, tok_granted_b]),
        )
        .unwrap_err()
        .to_string();
        assert!(fatal_err.contains("exit code 3"), "{fatal_err}");
        assert!(!fatal_err.contains("tok-fatal"), "{fatal_err}");
        let after_fatal = std::fs::read_to_string(&log).unwrap().lines().count();
        assert_eq!(after_fatal, before_fatal + 1);
    }
}
