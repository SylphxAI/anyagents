use super::super::agents_lifecycle::workdirs_overlap;
use super::super::*;
use super::archive::archived_reap_fixture;
use super::helpers::env_lock;
use std::path::PathBuf;

#[test]
fn cleanup_preserves_unpushed_head_and_other_branch_then_removes_pushed() {
    use crate::repo::tests::git;
    fn snapshot(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        fn walk(root: &Path, path: &Path, out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                let meta = std::fs::symlink_metadata(&path).unwrap();
                if meta.is_dir() {
                    walk(root, &path, out);
                } else if meta.is_file() {
                    out.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(&path).unwrap(),
                    );
                } else {
                    panic!("unexpected fixture object: {}", path.display());
                }
            }
        }
        let mut out = std::collections::BTreeMap::new();
        walk(root, root, &mut out);
        out
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let commit = |args: &[&str]| {
        let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
        all.extend_from_slice(args);
        git(&work, &all)
    };
    assert!(
        is_git_worktree_safe_to_reap(&work, None),
        "published baseline must be eligible"
    );
    std::fs::write(work.join("src/unique"), b"unique committed source\n").unwrap();
    git(&work, &["add", "src/unique"]);
    commit(&["commit", "-m", "Unpushed source"]);
    // A child-controlled marker must not turn an unpushed HEAD into a GC proof.
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::write(
        cfg.join(".anyagents-managed-worktree.json"),
        serde_json::to_vec(
            &serde_json::json!({"path": work, "initial_head": git(&work, &["rev-parse", "HEAD"])}),
        )
        .unwrap(),
    )
    .unwrap();
    let before_work = snapshot(&work);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(snapshot(&work), before_work);
    // A checkout whose .git is gone or replaced by a pointer elsewhere is
    // never judged through it, nor treated as a plain directory.
    let own = work.join(".git");
    let aside = dir.path().join("aside.git");
    std::fs::rename(&own, &aside).unwrap();
    for pointer in [None, Some(b"gitdir: /nonexistent/broken\n".as_slice())] {
        if let Some(bytes) = pointer {
            std::fs::write(&own, bytes).unwrap();
        }
        std::fs::create_dir_all(work.join("target")).unwrap();
        std::fs::write(work.join("target/uncertain-report"), b"retain\n").unwrap();
        let missing_work = snapshot(&work);
        assert!(!is_git_worktree_safe_to_reap(&work, None));
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert_eq!(snapshot(&work), missing_work, "lost .git deleted source");
        let _ = std::fs::remove_file(&own);
    }
    std::fs::rename(&aside, &own).unwrap();
    std::fs::remove_dir_all(work.join("target")).unwrap();
    git(&work, &["push", "-q", "origin", "HEAD:refs/heads/worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    let pushed_head = git(&work, &["rev-parse", "HEAD"]);
    git(&work, &["checkout", "-q", "-b", "unpublished-side"]);
    commit(&["commit", "-q", "--allow-empty", "-m", "Side branch only"]);
    git(&work, &["checkout", "-q", "--detach", &pushed_head]);
    let before_work = snapshot(&work);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "published HEAD does not cover another unpushed local branch"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(snapshot(&work), before_work);
    git(
        &work,
        &["push", "-q", "origin", "refs/heads/unpublished-side"],
    );
    assert!(is_git_worktree_safe_to_reap(&work, None));
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(!work.exists());
}

/// Archive keeps anything only the checkout holds: a stash, a tag or a
/// note the remote lacks. What the remote advertises is the proof, so a
/// branch published by URL (no remote-tracking ref moves) is reaped.
/// A clean checkout of a fresh pool clone whose every commit is on the
/// remote: the baseline each refusal below changes one thing in.
fn published_checkout(dir: &Path) -> (PathBuf, PathBuf) {
    let fixture = dir.join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    let work = dir.join("work");
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None), "baseline");
    (work, remote)
}

#[test]
fn cleanup_preserves_detached_reflog_and_in_progress_git_state() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    git(&work, &["checkout", "--detach"]);
    git_as_author(&work, &["commit", "--allow-empty", "-m", "detached-only"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None), "detached HEAD");
    git(&work, &["checkout", "worker"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "reflog-only commit"
    );
    git(&work, &["reflog", "expire", "--expire=now", "--all"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for name in [
        "index.lock",
        "HEAD.lock",
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "rebase-apply",
        "rebase-merge",
        "sequencer",
        "BISECT_START",
        "AUTO_MERGE",
    ] {
        let path = work.join(".git").join(name);
        std::fs::write(&path, "recovery state").unwrap();
        assert!(!is_git_worktree_safe_to_reap(&work, None), "{name}");
        std::fs::remove_file(path).unwrap();
    }
    assert!(is_git_worktree_safe_to_reap(&work, None));
}

fn git_as_author(dir: &Path, args: &[&str]) -> String {
    let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
    all.extend_from_slice(args);
    crate::repo::tests::git(dir, &all)
}

/// Ignored files are often the only copy of something (`.env`, data, a
/// gitignored memory file). Build-like names do not prove disposability.
#[test]
fn cleanup_keeps_all_ignored_files_including_build_names() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    std::fs::create_dir_all(work.join(".git/info")).unwrap();
    std::fs::write(
        work.join(".git/info/exclude"),
        "node_modules/\ntarget/\n.env\nmemory/\nbench/results/\n",
    )
    .unwrap();
    for dir in ["node_modules/pkg", "target/debug", "crates/x/target/debug"] {
        std::fs::create_dir_all(work.join(dir)).unwrap();
    }
    std::fs::write(work.join("node_modules/pkg/index.js"), "x").unwrap();
    std::fs::write(
        work.join("target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::fs::write(work.join("target/debug/bin"), "x").unwrap();
    std::fs::write(
        work.join("crates/x/target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "build output may hide unique work"
    );
    for path in ["node_modules", "target", "crates"] {
        std::fs::remove_dir_all(work.join(path)).unwrap();
    }
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for (path, body) in [
        (".env", "SECRET=only-copy\n"),
        ("memory/MEMORY.md", "# the agent's only memory\n"),
        ("bench/results/run.json", "{}\n"),
    ] {
        let file = work.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, body).unwrap();
        assert!(
            !is_git_worktree_safe_to_reap(&work, None),
            "ignored {path} was judged disposable"
        );
        std::fs::remove_file(&file).unwrap();
    }
    // A directory merely named target is not build output.
    std::fs::create_dir_all(work.join("notes/target")).unwrap();
    std::fs::write(work.join("notes/target/plan.md"), "x").unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an untagged target/"
    );
}

/// An edit to a tracked file is never incidental, whatever the file is
/// called; and AnyAgents writes no instruction file into a workdir, so an
/// untracked one is the agent's too.
#[test]
fn cleanup_keeps_tracked_and_untracked_instruction_file_edits() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    for path in ["AGENTS.md", "CLAUDE.md", ".claude/settings.json"] {
        let file = work.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "committed\n").unwrap();
    }
    crate::repo::tests::git(&work, &["add", "."]);
    git_as_author(&work, &["commit", "-qm", "instructions"]);
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for path in ["AGENTS.md", "CLAUDE.md", ".claude/settings.json"] {
        std::fs::write(work.join(path), "an edit\n").unwrap();
        assert!(
            !is_git_worktree_safe_to_reap(&work, None),
            "tracked {path} edit"
        );
        crate::repo::tests::git(&work, &["checkout", "-q", "--", path]);
    }
    std::fs::write(work.join("GEMINI.md"), "written by the agent\n").unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "untracked GEMINI.md"
    );
}

/// An orphan HEAD has no commit, so nothing about it is proven.
#[test]
fn cleanup_keeps_an_unborn_head() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    crate::repo::tests::git(&work, &["switch", "-q", "--orphan", "scratch"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
}

/// A submodule's own Git directory, absorbed or embedded, is history the
/// check cannot read: keep.
#[test]
fn cleanup_keeps_submodules_with_their_own_git_directory() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    let library = dir.path().join("library");
    std::fs::create_dir_all(&library).unwrap();
    crate::repo::tests::git(&library, &["init", "-q", "-b", "main"]);
    git_as_author(
        &library,
        &["commit", "-q", "--allow-empty", "-m", "library"],
    );
    // Embedded: a repository inside the checkout, staged as a gitlink.
    let embedded = work.join("vendor/embedded");
    std::fs::create_dir_all(&embedded).unwrap();
    crate::repo::tests::git(&embedded, &["init", "-q", "-b", "main"]);
    git_as_author(
        &embedded,
        &["commit", "-q", "--allow-empty", "-m", "unpushed"],
    );
    crate::repo::tests::git(&work, &["add", "vendor/embedded"]);
    git_as_author(&work, &["commit", "-qm", "gitlink"]);
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "embedded gitlink"
    );
    // Absorbed: `.git/modules` holds the submodule's repository.
    git_as_author(
        &work,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            library.to_str().unwrap(),
            "lib",
        ],
    );
    assert!(work.join(".git/modules").is_dir());
    assert!(!is_git_worktree_safe_to_reap(&work, None), ".git/modules");
}

/// Every ref namespace counts, not only branches: a filter-branch backup
/// in `refs/original` is the only copy of the history it names.
#[test]
fn cleanup_counts_every_ref_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    // A commit no branch, tag or reflog names yet.
    let only_copy = git_as_author(
        &work,
        &[
            "commit-tree",
            "HEAD^{tree}",
            "-p",
            "HEAD",
            "-m",
            "rewritten away",
        ],
    );
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for name in [
        "refs/original/refs/heads/worker",
        "refs/anyagents/backup",
        "refs/pull/1/head",
    ] {
        crate::repo::tests::git(&work, &["update-ref", name, &only_copy]);
        assert!(!is_git_worktree_safe_to_reap(&work, None), "{name}");
        crate::repo::tests::git(&work, &["update-ref", "-d", name]);
    }
}

/// A live agent working inside an archived agent's directory, or in a
/// directory holding it, keeps that directory whole.
#[test]
fn cleanup_keeps_a_directory_a_live_agent_works_in_or_around() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let archived = archived_reap_fixture(&store, "old-worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &archived.id);
    let inside = work.join("closure/live-worker");
    std::fs::create_dir_all(&inside).unwrap();
    std::fs::write(inside.join("progress.md"), "live work\n").unwrap();
    let live = store
        .create_agent(anyagents_core::NewAgent::new("live-worker", "codex"))
        .unwrap();
    store
        .set_agent_running(&live.id, &inside.display().to_string())
        .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &archived.id));
    assert_eq!(
        std::fs::read_to_string(inside.join("progress.md")).unwrap(),
        "live work\n",
        "a live agent's workdir inside the reaped one was deleted"
    );
    // The other way round: the live agent works in a directory that holds it.
    store
        .set_agent_running(&live.id, &work.parent().unwrap().display().to_string())
        .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &archived.id));
    assert!(inside.join("progress.md").is_file());
    assert!(workdirs_overlap(&work, &inside) && workdirs_overlap(&inside, &work));
    assert!(!workdirs_overlap(
        &work,
        &dir.path().join("agents/other/work")
    ));
    assert!(!workdirs_overlap(
        &work,
        &work.with_file_name(format!("{}2", work.file_name().unwrap().to_string_lossy()))
    ));
    assert!(!workdirs_overlap(&work, Path::new("")));
}

#[test]
fn cleanup_keeps_a_stash_and_unpublished_tags_or_notes() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let as_author = |args: &[&str]| {
        let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
        all.extend_from_slice(args);
        git(&work, &all)
    };
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A stash is the only copy of an edit.
    std::fs::write(work.join("README.md"), "precious edit\n").unwrap();
    as_author(&["stash", "-q"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "a stash was judged disposable"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(git(&work, &["stash", "list"]).lines().count(), 1);
    git(&work, &["stash", "drop", "-q"]);
    // A tag the remote lacks, then one it has.
    git(&work, &["tag", "local-only"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an unpublished tag"
    );
    git(&work, &["push", "-q", "origin", "refs/tags/local-only"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A note the remote lacks, then one it has.
    as_author(&["notes", "add", "-m", "remember this"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an unpublished note"
    );
    git(&work, &["push", "-q", "origin", "refs/notes/commits"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A commit published by URL, as a publishing tool does: no
    // remote-tracking ref records it, the remote still has it.
    as_author(&["commit", "-q", "--allow-empty", "-m", "Published by URL"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    git(
        &work,
        &[
            "push",
            "-q",
            remote.to_str().unwrap(),
            "HEAD:refs/heads/published",
        ],
    );
    assert!(git(&work, &["for-each-ref", "refs/remotes/origin/published"]).is_empty());
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // With no answer from the remote, nothing is proven.
    git(
        &work,
        &[
            "remote",
            "set-url",
            "origin",
            "https://example.invalid/x.git",
        ],
    );
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    git(
        &work,
        &["remote", "set-url", "origin", remote.to_str().unwrap()],
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(!work.exists());
}

/// An archived agent's linked worktree from before per-agent repositories
/// is converted, then judged like any checkout: dirty work stays, and the
/// shared store no longer registers it.
#[test]
fn cleanup_converts_a_legacy_worktree_and_keeps_its_work() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let dirty = archived_reap_fixture(&store, "dirty", &parent);
    let clean = archived_reap_fixture(&store, "clean", &parent);
    let fixture = dir.path().join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    let hub = fixture.join("hub");
    git(
        &fixture,
        &[
            "clone",
            "-q",
            remote.to_str().unwrap(),
            hub.to_str().unwrap(),
        ],
    );
    git(&hub, &["config", "gc.pruneExpire", "never"]);
    git(&hub, &["config", "gc.cruftPacks", "true"]);
    let mut checkouts = Vec::new();
    for agent in [&dirty, &clean] {
        let work = anyagents_core::agent_work_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(work.parent().unwrap()).unwrap();
        git(
            &hub,
            &["worktree", "add", "-q", "--detach", work.to_str().unwrap()],
        );
        checkouts.push(work);
    }
    std::fs::write(checkouts[0].join("README.md"), "work in progress\n").unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &dirty.id));
    assert!(checkouts[0].join(".git").is_dir(), "not converted");
    assert_eq!(
        std::fs::read_to_string(checkouts[0].join("README.md")).unwrap(),
        "work in progress\n"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &clean.id));
    assert!(!checkouts[1].exists(), "a clean converted checkout stays");
    let registered = git(&hub, &["worktree", "list", "--porcelain"]);
    assert!(
        !registered.contains(checkouts[0].to_str().unwrap())
            && !registered.contains(checkouts[1].to_str().unwrap()),
        "{registered}"
    );
    // Preserve both pool recovery references, even after successful reaping.
    // Cleanup must not drop recovery protection before directory removal.
    let keeps = git(
        &hub,
        &["for-each-ref", "--format=%(refname)", "refs/anyagents/keep"],
    );
    assert_eq!(keeps.lines().count(), 2, "{keeps}");
}

#[test]
fn archive_reap_preserves_unpushed_and_external_worktrees() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    std::fs::create_dir_all(&work).unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "-c",
                "user.name=Example Author",
                "-c",
                "user.email=author@example.org",
                "-C",
            ])
            .arg(&work)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    std::fs::write(work.join("source.rs"), b"unique committed source").unwrap();
    std::fs::write(work.join(".gitignore"), b"/target/\n").unwrap();
    git(&["add", "source.rs", ".gitignore"]);
    git(&["commit", "-qm", "Initial source"]);
    std::fs::create_dir_all(work.join("target/debug")).unwrap();
    std::fs::write(work.join("target/debug/report"), b"unique report").unwrap();
    // A clean commit with no remote is not safe to delete.
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(work.join("source.rs").exists());
    assert!(work.join(".git").exists());
    assert_eq!(
        std::fs::read(work.join("target/debug/report")).unwrap(),
        b"unique report"
    );
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    assert!(
        cfg.join(".anyagents-reaped").exists(),
        "retained source is not pending cleanup"
    );
    let external = dir.path().join("borrowed-worktree");
    std::fs::create_dir_all(external.join("target/debug")).unwrap();
    std::fs::write(external.join("source.rs"), b"borrowed source").unwrap();
    std::fs::write(
        cfg.join(".anyagents-managed-worktree.json"),
        serde_json::to_vec(&serde_json::json!({"path": external})).unwrap(),
    )
    .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(external.join("source.rs").exists());
    assert!(external.join("target/debug").exists());
}
