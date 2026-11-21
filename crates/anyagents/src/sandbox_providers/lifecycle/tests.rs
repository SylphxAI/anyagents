use std::path::Path;
use std::process::{Command, Output};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{StatusCode, Uri};

use super::PROBE;

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .current_dir(dir)
        .args(["-c", "user.name=Test", "-c", "user.email=test@example.test"])
        .args(args)
        .output()
        .unwrap();
    assert!(out.status.success(), "{args:?}: {out:?}");
}

fn probe(dir: &Path) -> Output {
    Command::new("/bin/sh")
        .current_dir(dir)
        .args(["-c", PROBE, "workspace-probe"])
        .arg(dir.canonicalize().unwrap())
        .output()
        .unwrap()
}

async fn serve_repo(State(root): State<std::path::PathBuf>, uri: Uri) -> (StatusCode, Bytes) {
    let Some(path) = uri.path().strip_prefix("/repo.git/") else {
        return (StatusCode::NOT_FOUND, Bytes::new());
    };
    assert!(path.split('/').all(|seg| !matches!(seg, "." | "..")));
    match std::fs::read(root.join(path)) {
        Ok(body) => (StatusCode::OK, Bytes::from(body)),
        Err(_) => (StatusCode::NOT_FOUND, Bytes::new()),
    }
}

#[tokio::test]
async fn workspace_probe_requires_live_publication_and_a_clean_worktree() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("repo.git");
    let work = tmp.path().join("work");
    std::fs::create_dir(&remote).unwrap();
    std::fs::create_dir(&work).unwrap();
    git(&remote, &["init", "--bare", "--initial-branch=main"]);
    let app = Router::new()
        .fallback(serve_repo)
        .with_state(remote.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/repo.git", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    tokio::task::spawn_blocking(move || {
        assert_eq!(probe(&work).stdout, b"no repository\n");
        git(&work, &["init", "--initial-branch=main"]);
        std::fs::write(work.join("tracked"), "tracked\n").unwrap();
        git(&work, &["add", "tracked"]);
        git(&work, &["commit", "-m", "initial"]);
        git(
            &work,
            &["push", remote.to_str().unwrap(), "HEAD:refs/heads/main"],
        );
        git(&remote, &["update-server-info"]);
        git(&work, &["remote", "add", "origin", &endpoint]);
        let out = probe(&work);
        assert_eq!(out.stdout, b"clean and pushed\n", "{out:?}");
        std::fs::write(work.join("untracked"), "unsaved\n").unwrap();
        assert!(!probe(&work).status.success());
        std::fs::remove_file(work.join("untracked")).unwrap();
        git(&work, &["commit", "--allow-empty", "-m", "not pushed"]);
        assert!(!probe(&work).status.success());
        git(&work, &["reset", "--hard", "HEAD~1"]);
        assert!(
            !probe(&work).status.success(),
            "reflog contains unpublished work"
        );
        git(&work, &["reflog", "expire", "--expire=now", "--all"]);
        assert!(probe(&work).status.success());
        let commit = std::fs::read_to_string(remote.join("refs/heads/main")).unwrap();
        git(
            &work,
            &["update-ref", "refs/remotes/origin/main", commit.trim()],
        );
        git(&remote, &["update-ref", "-d", "refs/heads/main"]);
        git(&remote, &["update-server-info"]);
        assert!(
            !probe(&work).status.success(),
            "cached origin ref is not publication proof"
        );
    })
    .await
    .unwrap();
    server.abort();
}

#[test]
fn workspace_probe_does_not_treat_nested_or_broken_checkouts_as_no_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let nested = tmp.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    git(&nested, &["init"]);
    assert!(!probe(tmp.path()).status.success());
    std::fs::write(tmp.path().join(".git"), "gitdir: not-available\n").unwrap();
    assert!(!probe(tmp.path()).status.success());
}
