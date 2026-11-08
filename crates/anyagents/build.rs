use std::path::PathBuf;
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    // A source archive extracted inside another repository must not claim that
    // unrelated parent's commit. This crate's Git metadata belongs to its workspace.
    if !PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.git")
        .exists()
    {
        return None;
    }
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // HEAD and its branch ref can live outside the source tree in a worktree.
    // Track both so a cached build never carries the previous commit's identity.
    for name in ["HEAD".to_owned(), "packed-refs".to_owned()]
        .into_iter()
        .chain(git(&["symbolic-ref", "-q", "HEAD"]))
    {
        if let Some(path) = git(&["rev-parse", "--path-format=absolute", "--git-path", &name]) {
            let path = PathBuf::from(path);
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    // Source archives without Git metadata cannot attest a commit. Do not
    // substitute a runtime environment variable or the installer's revision.
    let sha = git(&["rev-parse", "HEAD"])
        .filter(|sha| sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=ANYAGENTS_SOURCE_SHA={sha}");
}
