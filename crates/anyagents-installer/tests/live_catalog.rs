//! Opt-in vendor acceptance: downloads real artifacts and runs only credential-free doctor commands.
//! Run: cargo test -p anyagents-installer --test live_catalog -- --ignored --nocapture

use std::sync::Arc;

use anyagents_installer::fetch::Fetch;
use anyagents_installer::platform::Platform;
use anyagents_installer::receipt::ReceiptState;
use anyagents_installer::source::{Source, resolve};
use anyagents_installer::{InstallError, Lifecycle, NoLeases};

// Reject rather than mock successful network I/O: any resolver fetch is a test failure.
struct RejectFetch;

impl Fetch for RejectFetch {
    fn get(&self, _url: &str, _limit: u64) -> Result<Vec<u8>, InstallError> {
        panic!("unconfigured source attempted to fetch metadata");
    }

    fn download(&self, _url: &str, _out: &mut dyn std::io::Write) -> Result<u64, InstallError> {
        panic!("unconfigured source attempted to download an artifact");
    }
}

#[test]
fn unconfigured_source_refuses_before_fetch_or_install_state() {
    // Exercise the source mechanism, not a policy that particular vendors must
    // remain unconfigured. Resolution has no install root and creates no
    // lifecycle/store state. Lifecycle::ensure may separately acquire disk locks.
    for (os, arch) in [
        ("linux", "x86_64"),
        ("linux", "aarch64"),
        ("macos", "x86_64"),
        ("macos", "aarch64"),
    ] {
        let platform = Platform::from_parts(os, arch).unwrap();
        match resolve(
            "unconfigured-fixture",
            &Source::Unconfigured,
            &platform,
            &RejectFetch,
        ) {
            Err(InstallError::UnverifiableSource { harness, reason }) => {
                assert_eq!(harness, "unconfigured-fixture");
                assert_eq!(
                    reason,
                    "no verifiable release source; declare a pinned, local or vendor-manifest source in caller configuration"
                );
            }
            other => panic!("unconfigured source must refuse resolution, got {other:?}"),
        }
    }
}

#[test]
#[ignore = "downloads and executes current vendor releases; requires network access"]
fn configured_vendor_releases_install_and_survive_reload() {
    let data = tempfile::tempdir().unwrap();
    let lifecycle = Lifecycle::production(data.path(), Arc::new(NoLeases)).unwrap();
    let mut failures = Vec::new();
    for id in [
        "node",
        "bun",
        "claude-code",
        "codex",
        "opencode",
        "omp",
        "dsh",
    ] {
        let result = (|| {
            let entry = lifecycle.catalog.get(id).unwrap();
            let expected = resolve(
                id,
                &entry.source,
                &lifecycle.platform,
                lifecycle.fetch.as_ref(),
            )?;
            let installed = lifecycle.ensure(id)?;
            assert_eq!(
                installed.version, expected.version,
                "{id}: changed during install"
            );
            assert!(installed.launcher.is_file(), "{id}: missing launcher");
            let reloaded = Lifecycle::production(data.path(), Arc::new(NoLeases))?;
            assert_eq!(
                reloaded.ensure(id)?,
                installed,
                "{id}: changed after reload"
            );
            let status = reloaded.status(id)?;
            assert_eq!(status.current.unwrap().version, expected.version);
            assert!(status.bad.is_empty(), "{id}: failed doctor");
            let receipt = status
                .receipts
                .iter()
                .find(|r| r.version == expected.version)
                .unwrap();
            assert_eq!(receipt.state, ReceiptState::Ok);
            assert!(receipt.doctor.as_ref().unwrap().ok);
            eprintln!(
                "{id}@{}: verified artifact, doctor, receipt and reload",
                installed.version
            );
            Ok::<_, anyagents_installer::InstallError>(())
        })();
        if let Err(error) = result {
            eprintln!("{id}: {error}");
            failures.push(format!("{id}: {error}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
