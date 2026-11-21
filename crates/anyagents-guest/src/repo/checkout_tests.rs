use super::*;

struct Fixture {
    temp: tempfile::TempDir,
    state: PathBuf,
    work: PathBuf,
    spec: GuestRepoSpec,
    head: String,
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let origin = root.join("origin.git");
        let seed = root.join("seed");
        let state = root.join("state");
        let work = root.join("workspace");
        fs::create_dir(&origin).unwrap();
        fs::create_dir(&seed).unwrap();
        fs::create_dir(&state).unwrap();
        git(&origin, &["init", "--bare", "--initial-branch=main"]);
        git(&seed, &["init", "--initial-branch=main"]);
        fs::write(seed.join("tracked"), b"base\n").unwrap();
        fs::write(seed.join("second"), b"other\n").unwrap();
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-m", "base"]);
        git(&seed, &["push", origin.to_str().unwrap(), "main"]);
        let head = git(&seed, &["rev-parse", "HEAD"]);
        let spec = GuestRepoSpec {
            repo: origin.to_str().unwrap().to_string(),
            branch: Some("main".into()),
            base: None,
        };
        Self {
            temp,
            state,
            work,
            spec,
            head,
        }
    }

    fn boot(&self) -> Result<(), String> {
        ensure_repo_checked_out(&self.work, &self.state, &self.spec, Some(Path::new("git")))
    }

    fn assert_checkout(&self, branch: &str) {
        assert_eq!(
            git(&self.work, &["symbolic-ref", "--short", "HEAD"]),
            branch
        );
        assert_eq!(git(&self.work, &["rev-parse", "HEAD"]), self.head);
        assert_eq!(git(&self.work, &["status", "--porcelain"]), "");
        assert_eq!(fs::read(self.work.join("tracked")).unwrap(), b"base\n");
        assert!(!self.state.join("clone-attempt.json").exists());
        for path in [self.temp.path(), &self.work] {
            assert!(fs::read_dir(path).unwrap().all(|e| {
                !e.unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".clone-staging-")
            }));
        }
    }
}

#[test]
fn cold_checkout_is_clean_for_absent_and_precreated_targets() {
    for precreated in [false, true] {
        let fixture = Fixture::new();
        if precreated {
            fs::create_dir(&fixture.work).unwrap();
        }
        fixture.boot().unwrap();
        fixture.assert_checkout("main");
    }
}

#[test]
fn missing_remote_branch_starts_at_the_requested_base() {
    let mut fixture = Fixture::new();
    let origin = Path::new(&fixture.spec.repo);
    git(origin, &["update-ref", "refs/heads/base", &fixture.head]);
    let seed = fixture.temp.path().join("seed");
    git(&seed, &["commit", "--allow-empty", "-m", "different main"]);
    git(&seed, &["push", &fixture.spec.repo, "main"]);
    fixture.spec.branch = Some("topic/new".into());
    fixture.spec.base = Some("base".into());
    fixture.boot().unwrap();
    fixture.assert_checkout("topic/new");
}

#[test]
fn reaps_only_the_abandoned_checkout_named_by_the_journal() {
    let fixture = Fixture::new();
    fs::create_dir(&fixture.work).unwrap();
    let work = fixture.work.canonicalize().unwrap();
    let slot = staging::Slot::lock(&fixture.state).unwrap();
    let attempt = slot.begin(&work, false).unwrap();
    let checkout = attempt.checkout(&work);
    fs::create_dir_all(checkout.join(".git/objects")).unwrap();
    fs::write(checkout.join(".git/objects/incomplete"), b"partial").unwrap();
    let lookalike = fixture.temp.path().join(".clone-staging-unrelated");
    fs::write(&lookalike, b"preserve").unwrap();
    assert!(fixture.boot().is_err(), "must not reap a live transaction");
    assert!(checkout.join(".git/objects/incomplete").is_file());
    drop(slot);
    fixture.boot().unwrap();
    assert_eq!(fs::read(&lookalike).unwrap(), b"preserve");
    fs::remove_file(lookalike).unwrap();
    fixture.assert_checkout("main");
}

#[test]
fn preserves_a_clean_checkout_unpushed_history_and_a_dirty_checkout() {
    let mut fixture = Fixture::new();
    fixture.boot().unwrap();
    fixture.spec.branch = Some("do-not-switch".into());
    fixture.boot().unwrap();
    fixture.assert_checkout("main");
    git(
        &fixture.work,
        &["commit", "--allow-empty", "-m", "local only"],
    );
    fs::write(fixture.work.join("tracked"), b"edited\n").unwrap();
    fs::write(fixture.work.join("untracked"), b"keep\n").unwrap();
    let head = git(&fixture.work, &["rev-parse", "HEAD"]);
    let status = git(&fixture.work, &["status", "--porcelain"]);
    fixture.boot().unwrap();
    assert_eq!(git(&fixture.work, &["rev-parse", "HEAD"]), head);
    assert_eq!(git(&fixture.work, &["status", "--porcelain"]), status);
    assert_eq!(fs::read(fixture.work.join("tracked")).unwrap(), b"edited\n");
    assert_eq!(fs::read(fixture.work.join("untracked")).unwrap(), b"keep\n");
}

#[test]
fn refuses_unowned_user_files_and_unmarked_partial_git_metadata() {
    let fixture = Fixture::new();
    fs::create_dir(&fixture.work).unwrap();
    let data = fixture.work.join("user-file");
    fs::write(&data, b"keep\n").unwrap();
    assert!(
        fixture
            .boot()
            .unwrap_err()
            .contains("workspace is not empty")
    );
    fs::create_dir(fixture.work.join(".git")).unwrap();
    let partial = fixture.work.join(".git/partial");
    fs::write(&partial, b"unknown origin").unwrap();
    assert!(
        fixture
            .boot()
            .unwrap_err()
            .contains("existing Git metadata is unusable")
    );
    assert_eq!(fs::read(&data).unwrap(), b"keep\n");
    assert_eq!(fs::read(&partial).unwrap(), b"unknown origin");
}

#[test]
fn empty_remote_does_not_publish_unborn_head() {
    let fixture = Fixture::new();
    let origin = Path::new(&fixture.spec.repo);
    git(origin, &["update-ref", "-d", "refs/heads/main"]);
    let mut fixture = fixture;
    fixture.spec.branch = None;
    let error = fixture.boot().unwrap_err();
    assert!(error.contains("HEAD^{commit}"), "{error}");
    assert_eq!(fs::read_dir(&fixture.work).unwrap().count(), 0);
    assert!(!fixture.state.join("clone-attempt.json").exists());
}
