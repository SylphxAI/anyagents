use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
    println!("cargo:rerun-if-changed=../../.git/packed-refs");
    if let Ok(output) = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()
        && output.status.success()
    {
        println!(
            "cargo:rerun-if-changed=../../.git/{}",
            String::from_utf8_lossy(&output.stdout).trim()
        );
    }
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=ANYAGENTS_COMMIT={commit}");
    println!(
        "cargo:rustc-env=ANYAGENTS_VERSION={} ({commit})",
        env!("CARGO_PKG_VERSION")
    );
}
