//! External checkout provider contract. The explicitly configured executable accepts
//! `fetch <source> <ref>` and `worktree <source> <target> <branch> [base]`.
//! No host-specific provider or install location is selected implicitly.
use std::path::{Path, PathBuf};
use std::process::Command;

use anyagents_core::Error;

pub(crate) struct Consumer {
    executable: PathBuf,
}

impl Consumer {
    pub(crate) fn configured() -> Self {
        Self {
            executable: std::env::var_os("ANYAGENTS_CHECKOUT_PROVIDER_BIN")
                .map(PathBuf::from)
                .unwrap_or_default(),
        }
    }

    fn run(&self, repo: &str, args: &[&std::ffi::OsStr]) -> Result<(), Error> {
        if self.executable.as_os_str().is_empty() {
            return Err(Error::WorktreeProvisioning {
                repo: repo.into(),
                message: "configure ANYAGENTS_CHECKOUT_PROVIDER_BIN with a checkout provider"
                    .into(),
            });
        }
        let out = Command::new(&self.executable)
            .args(args)
            .output()
            .map_err(|error| Error::WorktreeProvisioning {
                repo: repo.into(),
                message: format!("{}: {error}", self.executable.display()),
            })?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let stdout = String::from_utf8_lossy(&out.stdout);
            return Err(Error::WorktreeProvisioning {
                repo: repo.into(),
                message: format!(
                    "{}: {}",
                    out.status,
                    if stderr.trim().is_empty() {
                        stdout.trim()
                    } else {
                        stderr.trim()
                    }
                ),
            });
        }
        Ok(())
    }

    fn fetch(&self, repo: &str, base: &str) -> Result<(), Error> {
        self.run(repo, &["fetch".as_ref(), repo.as_ref(), base.as_ref()])
    }

    fn checkout(
        &self,
        repo: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        let mut args = vec![
            "worktree".as_ref(),
            repo.as_ref(),
            target.as_os_str(),
            branch.as_ref(),
        ];
        if let Some(base) = base {
            args.push(base.as_ref());
        }
        self.run(repo, &args)
    }

    pub(crate) fn remote(
        &self,
        raw: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        let repo = github_repo(raw).unwrap_or_else(|| raw.to_string());
        if repo.trim().is_empty() {
            return Err(Error::WorktreeSourceUnpublished {
                source_repo: raw.into(),
                message: "repository source is empty".into(),
            });
        }
        let start = base.map(|base| {
            if base == "HEAD"
                || base.starts_with("origin/")
                || is_commit_id(base)
                || base.starts_with("refs/tags/")
            {
                base.to_string()
            } else if let Some(branch) = base.strip_prefix("refs/heads/") {
                format!("origin/{branch}")
            } else {
                format!("origin/{base}")
            }
        });
        if let Some(base) = base.filter(|b| *b != "HEAD" && *b != "origin/HEAD" && !is_commit_id(b))
        {
            self.fetch(&repo, base.strip_prefix("origin/").unwrap_or(base))?;
        }
        self.checkout(&repo, target, branch, start.as_deref())
    }

    pub(crate) fn local(
        &self,
        source: &Path,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        crate::worktree_access::validate_git_config(source)?;
        let refuse = |message: String| Error::WorktreeSourceUnpublished {
            source_repo: source.display().to_string(),
            message,
        };
        let git = |args: &[&str]| -> Result<String, Error> {
            let out = Command::new("git")
                .args([
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                ])
                .arg("-C")
                .arg(source)
                .args(args)
                .output()
                .map_err(|e| refuse(e.to_string()))?;
            if !out.status.success() {
                return Err(refuse(
                    String::from_utf8_lossy(&out.stderr).trim().to_string(),
                ));
            }
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        };
        let mut remotes: Vec<_> = git(&["remote"])?.lines().map(str::to_string).collect();
        remotes.sort_by_key(|name| (name != "origin", name.clone()));
        let (remote, repo) = remotes
            .into_iter()
            .find_map(|name| {
                let url = git(&["config", "--get", &format!("remote.{name}.url")]).ok()?;
                (!url.trim().is_empty()).then(|| (name, github_remote_repo(&url).unwrap_or(url)))
            })
            .ok_or_else(|| refuse("no remote is configured".into()))?;
        let head = git(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        let selected = git(&[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{}^{{commit}}", base.unwrap_or("HEAD")),
        ])?;
        // Read live advertised refs, not potentially stale origin/* tracking refs.
        let refs = git(&["ls-remote", "--heads", "--tags", &remote])?;
        let reachable_ref = |commit: &str| -> Option<String> {
            refs.lines()
                .filter_map(|line| line.split_once('\t'))
                .find_map(|(tip, name)| {
                    if name.ends_with("^{}") {
                        return None;
                    }
                    let reachable = tip == commit
                        || Command::new("git")
                            .args([
                                "-c",
                                "core.fsmonitor=false",
                                "-c",
                                "core.hooksPath=/dev/null",
                            ])
                            .arg("-C")
                            .arg(source)
                            .args(["merge-base", "--is-ancestor", commit, tip])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status()
                            .is_ok_and(|s| s.success());
                    reachable.then(|| name.to_string())
                })
        };
        reachable_ref(&head).ok_or_else(|| refuse("HEAD is not reachable from the remote's advertised refs; push HEAD first (fetch the remote if local history is stale)".into()))?;
        let reference = reachable_ref(&selected).ok_or_else(|| {
            refuse(
                "requested base is not reachable from the remote's advertised refs; push it first"
                    .into(),
            )
        })?;
        // A published feature commit may not be in the hub's default branch yet.
        self.fetch(&repo, &reference)?;
        self.checkout(&repo, target, branch, Some(&selected))
    }
}

fn is_commit_id(raw: &str) -> bool {
    matches!(raw.len(), 40 | 64) && raw.bytes().all(|b| b.is_ascii_hexdigit())
}

fn github_remote_repo(raw: &str) -> Option<String> {
    [
        "https://github.com/",
        "http://github.com/",
        "git@github.com:",
        "ssh://git@github.com/",
    ]
    .iter()
    .any(|prefix| raw.starts_with(prefix))
    .then(|| github_repo(raw))
    .flatten()
}

fn github_repo(raw: &str) -> Option<String> {
    let raw = raw.trim().trim_end_matches('/');
    let slug = raw
        .strip_prefix("https://github.com/")
        .or_else(|| raw.strip_prefix("http://github.com/"))
        .or_else(|| raw.strip_prefix("git@github.com:"))
        .or_else(|| raw.strip_prefix("ssh://git@github.com/"))
        .unwrap_or(raw)
        .trim_end_matches(".git");
    let parts: Vec<_> = slug.split('/').collect();
    (parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty()
                && !part.contains("..")
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        }))
    .then(|| slug.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    pub(crate) struct Fixture {
        pub consumer: Consumer,
        pub source: PathBuf,
        pub log: PathBuf,
        pub fail: PathBuf,
    }

    pub(crate) fn git(source: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(source)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().into()
    }

    /// The test parent must never hold a writable executable descriptor: an
    /// unrelated parallel fork can inherit it until exec and cause ETXTBSY even
    /// after std::fs::write returns. A waited writer has no surviving children.
    pub(crate) fn write_executable(path: &Path, body: &str) {
        assert!(
            Command::new("/bin/sh")
                .args(["-c", "printf '%s' \"$2\" > \"$1\"", "fixture-writer"])
                .arg(path)
                .arg(body)
                .status()
                .unwrap()
                .success()
        );
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    impl Fixture {
        pub(crate) fn new(dir: &Path) -> Self {
            let source = dir.join("source");
            let remote = dir.join("remote.git");
            let executable = dir.join("consumer");
            let log = dir.join("consumer.log");
            let fail = dir.join("fail");
            std::fs::create_dir_all(source.join("src")).unwrap();
            std::fs::create_dir_all(source.join("excluded")).unwrap();
            git(&source, &["init", "-b", "main"]);
            git(&source, &["config", "user.name", "Example Author"]);
            git(&source, &["config", "user.email", "author@example.test"]);
            std::fs::write(source.join("README.md"), "# Demo Repo\n").unwrap();
            std::fs::write(source.join("src/keep"), "included\n").unwrap();
            std::fs::write(source.join("excluded/large"), "excluded\n").unwrap();
            git(&source, &["add", "."]);
            git(&source, &["commit", "-m", "Initial commit"]);
            git(
                &source,
                &[
                    "clone",
                    "--bare",
                    "--no-hardlinks",
                    ".",
                    remote.to_str().unwrap(),
                ],
            );
            git(
                &source,
                &[
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/ExampleOrg/demo.git",
                ],
            );
            git(
                &source,
                &[
                    "config",
                    &format!("url.file://{}.insteadOf", remote.display()),
                    "https://github.com/ExampleOrg/demo.git",
                ],
            );
            git(&source, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
            // The authority fixture deliberately owns sparse checkout and hooks;
            // the consumer must not reimplement either policy or recover on failure.
            write_executable(
                &executable,
                &format!(
                    r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> {log:?}
if [ -f {fail:?} ] && [ "$(cat {fail:?})" = "$1" ]; then
  echo 'checkout-provider: injected policy refusal' >&2
  exit 42
fi
case "$1" in
  fetch) exit 0 ;;
  worktree)
    git -C {source:?} worktree add -b "$4" "$3" "${{5:-HEAD}}" >&2
    git -C "$3" sparse-checkout set src
    hooks=$(git -C "$3" rev-parse --git-path hooks)
    mkdir -p "$hooks"
    printf '#!/bin/sh\nexit 0\n' > "$hooks/pre-commit"
    printf '#!/bin/sh\nexit 0\n' > "$hooks/pre-push"
    chmod +x "$hooks/pre-commit" "$hooks/pre-push"
    ;;
  *) exit 2 ;;
esac
"#
                ),
            );
            Self {
                consumer: Consumer { executable },
                source,
                log,
                fail,
            }
        }

        pub(crate) fn assert_checkout(&self, target: &Path) {
            assert_eq!(
                git(target, &["config", "--get", "core.sparseCheckout"]),
                "true"
            );
            assert!(target.join("src/keep").is_file());
            assert!(!target.join("excluded/large").exists());
            for name in ["pre-commit", "pre-push"] {
                let hook = PathBuf::from(git(
                    target,
                    &["rev-parse", "--git-path", &format!("hooks/{name}")],
                ));
                assert_ne!(
                    std::fs::metadata(hook).unwrap().permissions().mode() & 0o111,
                    0
                );
            }
        }
    }

    #[test]
    fn remote_and_local_checkouts_use_consumer_policy() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let remote = dir.path().join("remote-work");
        f.consumer
            .remote(
                "https://github.com/ExampleOrg/demo.git",
                &remote,
                "remote-work",
                Some("main"),
            )
            .unwrap();
        f.assert_checkout(&remote);
        let local = dir.path().join("local-work");
        f.consumer
            .local(&f.source, &local, "local-work", None)
            .unwrap();
        f.assert_checkout(&local);
        assert_eq!(
            git(&local, &["rev-parse", "HEAD"]),
            git(&f.source, &["rev-parse", "HEAD"])
        );
        let log = std::fs::read_to_string(&f.log).unwrap();
        assert!(log.contains("fetch ExampleOrg/demo main"));
        assert!(log.contains("fetch ExampleOrg/demo refs/heads/main"));
        assert_eq!(
            log.lines().filter(|l| l.starts_with("worktree ")).count(),
            2
        );
    }

    #[test]
    fn unpushed_head_and_stale_tracking_refs_are_refused_before_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["commit", "--allow-empty", "-m", "Not pushed"]);
        git(
            &f.source,
            &["update-ref", "refs/remotes/origin/main", "HEAD"],
        );
        let target = dir.path().join("work");
        let err = f
            .consumer
            .local(&f.source, &target, "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. } if message.contains("push HEAD first")),
            "{err}"
        );
        assert!(!target.exists());
        assert!(!f.log.exists());
        // Publishing the same commit makes the previously refused request valid.
        git(&f.source, &["push", "origin", "HEAD:main"]);
        f.consumer.local(&f.source, &target, "work", None).unwrap();
        f.assert_checkout(&target);
    }

    #[test]
    fn missing_remote_refuses_before_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["remote", "remove", "origin"]);
        let err = f
            .consumer
            .local(&f.source, &dir.path().join("work"), "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. } if message.contains("no remote")),
            "{err}"
        );
        assert!(!f.log.exists());
    }

    #[test]
    fn fetch_and_checkout_failures_keep_tool_message_without_fallback() {
        for operation in ["fetch", "worktree"] {
            let dir = tempfile::tempdir().unwrap();
            let f = Fixture::new(dir.path());
            std::fs::write(&f.fail, operation).unwrap();
            let target = dir.path().join("work");
            let err = f
                .consumer
                .remote("ExampleOrg/demo", &target, "work", Some("main"))
                .unwrap_err();
            assert!(
                matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("injected policy refusal") && message.contains("42")),
                "{err}"
            );
            assert!(!target.exists());
            let log = std::fs::read_to_string(&f.log).unwrap();
            assert_eq!(
                log.lines().count(),
                if operation == "fetch" { 1 } else { 2 }
            );
        }
    }

    #[test]
    fn github_urls_are_normalized_but_other_hosts_and_local_remotes_are_rejected() {
        for raw in [
            "https://github.com/ExampleOrg/demo.git",
            "git@github.com:ExampleOrg/demo.git",
            "ssh://git@github.com/ExampleOrg/demo",
        ] {
            assert_eq!(github_remote_repo(raw).as_deref(), Some("ExampleOrg/demo"));
        }
        for raw in [
            "ExampleOrg/demo",
            "../demo",
            "/tmp/demo",
            "https://example.com/ExampleOrg/demo",
            "https://github.com/ExampleOrg/../demo",
        ] {
            assert!(github_remote_repo(raw).is_none(), "{raw}");
        }
    }

    #[test]
    fn published_ancestor_and_non_origin_github_remote_are_supported() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let selected = git(&f.source, &["rev-parse", "HEAD"]);
        git(
            &f.source,
            &["commit", "--allow-empty", "-m", "Published descendant"],
        );
        git(&f.source, &["push", "origin", "HEAD:main"]);
        git(&f.source, &["checkout", "--detach", &selected]);
        git(&f.source, &["remote", "rename", "origin", "upstream"]);
        let target = dir.path().join("work");
        f.consumer.local(&f.source, &target, "work", None).unwrap();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), selected);
        f.assert_checkout(&target);
    }

    #[test]
    fn absent_consumer_does_not_enable_a_non_desk_mode() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let consumer = Consumer {
            executable: dir.path().join("absent-consumer"),
        };
        let target = dir.path().join("work");
        let err = consumer
            .local(&f.source, &target, "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("absent-consumer")),
            "{err}"
        );
        assert!(!target.exists());
    }
    #[test]
    fn unconfigured_checkout_provider_refuses_and_generic_remote_sources_are_forwarded() {
        let dir = tempfile::tempdir().unwrap();
        let unconfigured = Consumer {
            executable: PathBuf::new(),
        };
        assert!(
            unconfigured
                .remote(
                    "https://git.example.test/team/repo.git",
                    &dir.path().join("target"),
                    "branch",
                    None
                )
                .is_err()
        );
        let executable = dir.path().join("provider");
        let receipt = dir.path().join("args");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n",
                receipt.display()
            ),
        );
        let configured = Consumer { executable };
        configured
            .remote(
                "https://git.example.test/team/repo.git",
                &dir.path().join("target"),
                "branch",
                Some("main"),
            )
            .unwrap();
        let args = std::fs::read_to_string(receipt).unwrap();
        assert!(args.contains("https://git.example.test/team/repo.git"));
        assert!(args.contains("worktree"));
    }
}
