//! Operator-selected identity, independent of vendor HOME and ambient tokens.
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::HarnessError;
use serde::Deserialize;

pub const CONFIG_ENV: &str = "ANYAGENTS_LAUNCH_IDENTITY";
const TOKEN_ENV: &[&str] = &[
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    gh_program: PathBuf,
    gh_config_dir: PathBuf,
    github_host: String,
    github_login: String,
    git_author_name: String,
    git_author_email: String,
}

fn refused(reason: &str) -> HarnessError {
    HarnessError::Failed(format!("launch identity: {reason}"))
}

impl Identity {
    fn read(path: &Path) -> Result<Self, HarnessError> {
        if !path.is_absolute() {
            return Err(refused("configuration path must be absolute"));
        }
        let bytes =
            std::fs::read(path).map_err(|_| refused("cannot read operator configuration"))?;
        let identity: Self = serde_json::from_slice(&bytes)
            .map_err(|_| refused("invalid operator configuration"))?;
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<(), HarnessError> {
        let identifier = |s: &str| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        };
        let text = |s: &str| {
            !s.trim().is_empty()
                && s == s.trim()
                && !s.chars().any(|c| c.is_control() || c == '<' || c == '>')
        };
        if !self.gh_program.is_absolute()
            || !self.gh_config_dir.is_absolute()
            || self.gh_program.to_str().is_none()
            || self.gh_config_dir.to_str().is_none()
            || !identifier(&self.github_host)
            || !identifier(&self.github_login)
            || !text(&self.git_author_name)
            || !text(&self.git_author_email)
            || self.git_author_email.chars().any(char::is_whitespace)
        {
            return Err(refused(
                "absolute credential/program paths, host, login and complete author are required",
            ));
        }
        Ok(())
    }

    fn environment(&self, environment: &[(String, String)]) -> Vec<(String, String)> {
        let mut env: Vec<_> = environment
            .iter()
            .filter(|(key, _)| {
                !TOKEN_ENV.contains(&key.as_str())
                    && key != CONFIG_ENV
                    && key != "GIT_CONFIG_PARAMETERS"
            })
            .cloned()
            .collect();
        let gh = self.gh_program.to_str().expect("validated program path");
        let helper = format!(
            "!'{}' auth git-credential",
            gh.replace(char::from(39), "'\\''")
        );
        env.extend([
            (
                "GH_CONFIG_DIR".into(),
                self.gh_config_dir.to_string_lossy().into_owned(),
            ),
            ("GH_HOST".into(), self.github_host.clone()),
            ("GH_PROMPT_DISABLED".into(), "1".into()),
            ("GH_HTTP_TIMEOUT".into(), "10".into()),
            ("GIT_TERMINAL_PROMPT".into(), "0".into()),
            ("GIT_AUTHOR_NAME".into(), self.git_author_name.clone()),
            ("GIT_AUTHOR_EMAIL".into(), self.git_author_email.clone()),
            ("GIT_COMMITTER_NAME".into(), self.git_author_name.clone()),
            ("GIT_COMMITTER_EMAIL".into(), self.git_author_email.clone()),
            ("EMAIL".into(), self.git_author_email.clone()),
            ("GIT_CONFIG_COUNT".into(), "3".into()),
            ("GIT_CONFIG_KEY_0".into(), "credential.helper".into()),
            ("GIT_CONFIG_VALUE_0".into(), String::new()),
            (
                "GIT_CONFIG_KEY_1".into(),
                format!("credential.https://{}.helper", self.github_host),
            ),
            ("GIT_CONFIG_VALUE_1".into(), String::new()),
            (
                "GIT_CONFIG_KEY_2".into(),
                format!("credential.https://{}.helper", self.github_host),
            ),
            ("GIT_CONFIG_VALUE_2".into(), helper),
        ]);
        env
    }

    fn verify(&self, env: &[(String, String)]) -> Result<(), HarnessError> {
        // Probe with the actual seat admission, without recursive resolution.
        // Vendor stderr may contain secrets and must not reach launch errors.
        let mut status = super::spawn_command_unchecked(&self.gh_program, env)?;
        for key in TOKEN_ENV.iter().copied().chain(["GIT_CONFIG_PARAMETERS"]) {
            status.env_remove(key);
        }
        status.args(["auth", "status", "--hostname", &self.github_host]);
        run_probe(status)?;
        let mut user = super::spawn_command_unchecked(&self.gh_program, env)?;
        for key in TOKEN_ENV.iter().copied().chain(["GIT_CONFIG_PARAMETERS"]) {
            user.env_remove(key);
        }
        user.args([
            "api",
            "--hostname",
            &self.github_host,
            "user",
            "--jq",
            "[.login, .type] | @tsv",
        ]);
        let output = run_probe(user)?;
        if output.trim() != format!("{}\tUser", self.github_login) {
            return Err(refused(
                "configured human GitHub identity did not match; refusing fallback",
            ));
        }
        Ok(())
    }
}

fn run_probe(mut command: Command) -> Result<String, HarnessError> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = command
        .spawn()
        .map_err(|_| refused("cannot execute credential probe"))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(refused(
                    "credential probe timed out or could not be observed",
                ));
            }
        }
    }
    let output = child
        .wait_with_output()
        .map_err(|_| refused("cannot read credential probe"))?;
    if !output.status.success() {
        return Err(refused(
            "GitHub authentication failed as the launch principal; check credential access",
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| refused("invalid credential probe response"))
}

pub fn require_configuration() -> Result<(), HarnessError> {
    let path = std::env::var_os(CONFIG_ENV)
        .ok_or_else(|| refused("operator configuration is required (ANYAGENTS_LAUNCH_IDENTITY)"))?;
    Identity::read(Path::new(&path))?;
    Ok(())
}

pub fn command(program: &Path, environment: &[(String, String)]) -> Result<Command, HarnessError> {
    // Policy is selected only by the serving operator, never an agent launch_env.
    match std::env::var_os(CONFIG_ENV) {
        Some(path) => command_with_config(program, environment, Path::new(&path)),
        None if environment
            .iter()
            .any(|(key, _)| key == super::SEAT_LAUNCH_ENV) =>
        {
            Err(refused(
                "operator configuration is required (ANYAGENTS_LAUNCH_IDENTITY)",
            ))
        }
        // Standalone adapter/protocol users have no managed seat or serving policy.
        // The managed vendor dispatcher requires configuration for all real launches.
        None => super::spawn_command_unchecked(program, environment),
    }
}

fn command_with_config(
    program: &Path,
    environment: &[(String, String)],
    path: &Path,
) -> Result<Command, HarnessError> {
    let identity = Identity::read(path)?;
    let environment = identity.environment(environment);
    identity.verify(&environment)?;
    let mut command = super::spawn_command_unchecked(program, &environment)?;
    for key in TOKEN_ENV {
        command.env_remove(key);
    }
    command.env_remove("GIT_CONFIG_PARAMETERS");
    command.env_remove(CONFIG_ENV);
    Ok(command)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fixture(dir: &Path, login: &str, kind: &str) -> PathBuf {
        let gh = dir.join("gh'fixture");
        std::fs::write(
            &gh,
            format!(
                r#"#!/bin/sh
set -eu
[ -z "${{GH_TOKEN-}}${{GITHUB_TOKEN-}}${{GH_ENTERPRISE_TOKEN-}}${{GITHUB_ENTERPRISE_TOKEN-}}" ]
[ -r "$GH_CONFIG_DIR/hosts.yml" ]
case "$1 $2" in
  'auth status') exit 0 ;;
  'api --hostname') printf '%s\t%s\n' '{login}' '{kind}' ;;
  'auth git-credential') printf 'username={login}\npassword=fixture-only\n' ;;
  *) exit 2 ;;
esac
"#
            ),
        )
        .unwrap();
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config_dir = dir.join("credential-source");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join("hosts.yml"), "synthetic credentials").unwrap();
        let config = dir.join("launch-identity.json");
        std::fs::write(
            &config,
            serde_json::json!({
                "gh_program": gh, "gh_config_dir": config_dir,
                "github_host": "git.example.test", "github_login": "operator-user",
                "git_author_name": "Example Author", "git_author_email": "author@example.test"
            })
            .to_string(),
        )
        .unwrap();
        config
    }

    #[test]
    fn configured_identity_overrides_child_auth_author_and_git_helpers() {
        let dir = tempfile::tempdir().unwrap();
        let config = fixture(dir.path(), "operator-user", "User");
        let home = dir.path().join("isolated-home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(
            home.join(".gitconfig"),
            "[credential \"https://git.example.test/repo\"]\n helper = !exit 99\n",
        )
        .unwrap();
        let env = vec![
            ("HOME".into(), home.to_str().unwrap().into()),
            ("GH_TOKEN".into(), "wrong-fixture-identity".into()),
            ("GITHUB_TOKEN".into(), "wrong-fixture-identity".into()),
            ("GH_CONFIG_DIR".into(), "/nonexistent".into()),
            ("GIT_AUTHOR_NAME".into(), "Wrong Author".into()),
            ("GIT_AUTHOR_EMAIL".into(), "wrong@example.test".into()),
            ("GIT_CONFIG_PARAMETERS".into(), "invalid".into()),
            (CONFIG_ENV.into(), "/agent-selected-policy".into()),
        ];
        let mut cmd = command_with_config(Path::new("/bin/sh"), &env, &config).unwrap();
        cmd.args(["-c", "set -eu; test -z \"${GH_TOKEN-}${GITHUB_TOKEN-}${ANYAGENTS_LAUNCH_IDENTITY-}\"; git var GIT_AUTHOR_IDENT; printf 'protocol=https\\nhost=git.example.test\\npath=repo\\n\\n' | git credential fill"]);
        let output = cmd.current_dir(dir.path()).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(
            text.starts_with("Example Author <author@example.test> "),
            "{text}"
        );
        assert!(text.contains("username=operator-user"), "{text}");
    }

    #[test]
    fn wrong_user_and_bot_identity_refuse_before_starting_harness() {
        for (login, kind) in [("other-user", "User"), ("operator-user", "Bot")] {
            let dir = tempfile::tempdir().unwrap();
            let config = fixture(dir.path(), login, kind);
            let error = command_with_config(Path::new("/bin/true"), &[], &config).unwrap_err();
            assert!(error.to_string().contains("refusing fallback"));
        }
    }

    #[test]
    fn unavailable_credentials_refuse_without_leaking_provider_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let config = fixture(dir.path(), "operator-user", "User");
        std::fs::remove_file(dir.path().join("credential-source/hosts.yml")).unwrap();
        let error = command_with_config(Path::new("/bin/true"), &[], &config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("authentication failed as the launch principal")
        );
        assert!(!error.to_string().contains("fixture-only"));
    }

    #[test]
    fn invalid_missing_relative_and_partial_configuration_refuse() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Identity::read(Path::new("relative.json")).is_err());
        assert!(Identity::read(&dir.path().join("missing.json")).is_err());
        let config = fixture(dir.path(), "operator-user", "User");
        for json in ["{}", "not json", r#"{"git_author_name":"Only Name"}"#] {
            std::fs::write(&config, json).unwrap();
            assert!(command_with_config(Path::new("/bin/true"), &[], &config).is_err());
        }
    }

    #[test]
    fn complete_author_and_absolute_credential_source_are_required() {
        let dir = tempfile::tempdir().unwrap();
        let config = fixture(dir.path(), "operator-user", "User");
        let original: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        for (field, value) in [
            ("git_author_name", ""),
            ("git_author_email", "bad\naddress"),
            ("gh_config_dir", "relative"),
            ("github_login", ""),
            ("github_host", "bad/host"),
        ] {
            let mut json = original.clone();
            json[field] = value.into();
            std::fs::write(&config, json.to_string()).unwrap();
            assert!(Identity::read(&config).is_err(), "{field}");
        }
    }
}
