use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use sha2::{Digest as _, Sha256, Sha512};

use super::*;
use crate::catalog::{DoctorSpec, InstallMethod, SubscriptionTerms};
use crate::source::{PinnedArtifact, Source};
use crate::unpack::Format;
use crate::unpack::tests::{tar_gz, zip};
use crate::verify::to_hex;

struct FixtureRegistry {
    addr: String,
    routes: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FixtureRegistry {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let routes = Arc::new(RwLock::new(BTreeMap::<String, Vec<u8>>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let routes_bg = Arc::clone(&routes);
        let stop_bg = Arc::clone(&stop);

        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stop_bg.load(Ordering::Acquire) {
                    break;
                }
                let Ok(mut stream) = stream else {
                    break;
                };
                let routes_req = Arc::clone(&routes_bg);
                std::thread::spawn(move || serve_one(&mut stream, &routes_req));
            }
        });

        Self {
            addr: format!("http://{addr}"),
            routes,
            stop,
            handle: Some(handle),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.addr, path)
    }

    fn put(&self, path: impl Into<String>, body: impl Into<Vec<u8>>) {
        self.routes
            .write()
            .unwrap()
            .insert(path.into(), body.into());
    }

    fn put_json(&self, path: impl Into<String>, value: &serde_json::Value) {
        self.put(path, serde_json::to_vec(value).unwrap());
    }
}

impl Drop for FixtureRegistry {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let addr = self.addr.trim_start_matches("http://");
        let _ = TcpStream::connect(addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve_one(stream: &mut TcpStream, routes: &RwLock<BTreeMap<String, Vec<u8>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let body = routes.read().unwrap().get(path).cloned();
    match body {
        Some(bytes) => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&bytes);
        }
        None => {
            let head = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(head.as_bytes());
        }
    }
    let _ = stream.flush();
}

fn sri_sha512(bytes: &[u8]) -> String {
    let digest = Sha512::digest(bytes);
    format!(
        "sha512-{}",
        base64::engine::general_purpose::STANDARD.encode(digest)
    )
}

fn hex_sha256(bytes: &[u8]) -> String {
    to_hex(&Sha256::digest(bytes))
}

fn fake_script(version: &str, exit_code: i32) -> Vec<u8> {
    format!("#!/bin/sh\necho \"{version}\"\nexit {exit_code}\n").into_bytes()
}

fn test_platform() -> Platform {
    Platform::host().unwrap()
}

fn make_entry(
    id: &str,
    source: Source,
    launcher: &str,
    runtime: Option<&str>,
    sign_in_command: Vec<&str>,
    self_update_off: &[(&str, &str)],
) -> CatalogEntry {
    CatalogEntry {
        id: id.to_string(),
        install: InstallMethod::Vendored,
        sign_in_command: sign_in_command.into_iter().map(str::to_string).collect(),
        api_key_variable: None,
        subscription: SubscriptionTerms {
            models: Vec::new(),
            source: String::new(),
        },
        source,
        launcher: launcher.to_string(),
        runtime: runtime.map(str::to_string),
        doctor: DoctorSpec {
            version_args: vec!["--version".to_string()],
            handshake: Vec::new(),
        },
        config_var: crate::catalog::config_dir_variable(id).map(str::to_string),
        self_update_off: self_update_off
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    }
}

struct MockLeases {
    result: Mutex<Result<BTreeSet<String>, String>>,
}

impl MockLeases {
    fn with(versions: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(Ok(versions.iter().map(|s| (*s).to_string()).collect())),
        })
    }

    fn set(&self, versions: &[&str]) {
        *self.result.lock().unwrap() = Ok(versions.iter().map(|s| (*s).to_string()).collect());
    }

    fn set_err(&self, msg: &str) {
        *self.result.lock().unwrap() = Err(msg.to_string());
    }
}

impl LeaseView for MockLeases {
    fn leased_versions(&self, _: &str) -> Result<BTreeSet<String>, String> {
        self.result.lock().unwrap().clone()
    }
}

fn publish_npm_platform_release(
    reg: &FixtureRegistry,
    pkg: &str,
    platform_pkg: &str,
    version: &str,
    launcher_rel_in_pkg: &str,
    script: &[u8],
) {
    let tgz_path = format!("/tarballs/{platform_pkg}-{version}.tgz");
    let tgz = tar_gz(&[(&format!("package/{launcher_rel_in_pkg}"), script, 0o755)]);
    let integrity = sri_sha512(&tgz);
    reg.put(tgz_path.clone(), tgz);

    reg.put_json(
        format!("/{platform_pkg}/{version}"),
        &serde_json::json!({
            "name": platform_pkg,
            "version": version,
            "dist": {
                "tarball": reg.url(&tgz_path),
                "integrity": integrity,
            }
        }),
    );

    reg.put_json(
        format!("/{pkg}/latest"),
        &serde_json::json!({
            "name": pkg,
            "version": version,
            "optionalDependencies": {
                platform_pkg: version,
            }
        }),
    );
}

fn test_lifecycle(
    root: &Path,
    entries: Vec<CatalogEntry>,
    runtimes: Vec<CatalogEntry>,
    leases: Arc<dyn LeaseView>,
) -> Lifecycle {
    Lifecycle::new(
        root.join("harnesses"),
        Catalog::new(entries, runtimes),
        Arc::new(HttpFetch::new()),
        Arc::new(ProcessExec),
        leases,
        test_platform(),
    )
}

#[test]
fn invariant_6_and_11_npm_install_and_auto_update_promote() {
    let reg = FixtureRegistry::start();
    let platform = test_platform();
    let plat_pkg = format!("@acme/claude-{}-{}", platform.os, platform.cpu);
    publish_npm_platform_release(
        &reg,
        "@acme/claude",
        &plat_pkg,
        "1.0.0",
        "claude",
        &fake_script("1.0.0", 0),
    );

    let root = tempfile::tempdir().unwrap();
    let entry = make_entry(
        "claude-code",
        Source::Npm {
            package: "@acme/claude".to_string(),
            platform_package: Some("@acme/claude-{os}-{cpu}".to_string()),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@acme/claude-{os}-{cpu}/claude",
        None,
        vec!["claude", "auth", "login"],
        &[("DISABLE_AUTOUPDATER", "1")],
    );
    let lc = test_lifecycle(root.path(), vec![entry], vec![], Arc::new(NoLeases));

    let installed = lc.ensure("claude-code").unwrap();
    assert_eq!(installed.version, "1.0.0");
    assert!(
        installed
            .launcher
            .starts_with(root.path().join("harnesses/claude-code/1.0.0"))
    );
    assert!(installed.launcher.is_file());
    assert_eq!(
        installed.env.get("DISABLE_AUTOUPDATER").map(String::as_str),
        Some("1")
    );

    // Tick when unchanged returns Unchanged.
    assert_eq!(
        lc.tick("claude-code").unwrap(),
        Tick::Unchanged {
            version: "1.0.0".to_string()
        }
    );

    // Publish 1.1.0 and tick -> Promoted, then ensure returns 1.1.0.
    publish_npm_platform_release(
        &reg,
        "@acme/claude",
        &plat_pkg,
        "1.1.0",
        "claude",
        &fake_script("1.1.0", 0),
    );
    assert_eq!(
        lc.tick("claude-code").unwrap(),
        Tick::Promoted {
            from: Some("1.0.0".to_string()),
            to: "1.1.0".to_string(),
        }
    );
    let after = lc.ensure("claude-code").unwrap();
    assert_eq!(after.version, "1.1.0");
    assert!(
        after
            .launcher
            .starts_with(root.path().join("harnesses/claude-code/1.1.0"))
    );
}

#[test]
fn invariant_7_tampered_tarball_leaves_no_version_or_staging_dir() {
    let reg = FixtureRegistry::start();
    let tgz = tar_gz(&[("package/bin/tool", &fake_script("1.0.0", 0), 0o755)]);
    let wrong_digest = sri_sha512(b"expected-something-else");
    reg.put("/pkg-1.0.0.tgz", tgz);
    reg.put_json(
        "/tool/latest",
        &serde_json::json!({
            "name": "tool",
            "version": "1.0.0",
            "dist": {
                "tarball": reg.url("/pkg-1.0.0.tgz"),
                "integrity": wrong_digest,
            }
        }),
    );

    let root = tempfile::tempdir().unwrap();
    let entry = make_entry(
        "tool",
        Source::Npm {
            package: "tool".to_string(),
            platform_package: None,
            registry: Some(reg.addr.clone()),
        },
        "node_modules/tool/bin/tool",
        None,
        vec!["tool"],
        &[],
    );
    let lc = test_lifecycle(root.path(), vec![entry], vec![], Arc::new(NoLeases));

    let err = lc.ensure("tool").unwrap_err();
    assert!(
        matches!(err, InstallError::ChecksumMismatch { .. }),
        "{err:?}"
    );

    let entry_path = root.path().join("harnesses/tool");
    assert!(!entry_path.join("1.0.0").exists());
    for item in std::fs::read_dir(&entry_path).unwrap() {
        let name = item.unwrap().file_name().to_string_lossy().into_owned();
        assert_eq!(name, ".lock", "leftover artifact {name}");
    }
}

#[test]
fn invariant_8_leases_protect_old_versions_from_gc_and_err_removes_nothing() {
    let reg = FixtureRegistry::start();
    let platform = test_platform();
    let plat_pkg = format!("@acme/codex-{}-{}", platform.os, platform.cpu);
    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "1.0.0",
        "codex",
        &fake_script("1.0.0", 0),
    );

    let root = tempfile::tempdir().unwrap();
    let leases = MockLeases::with(&["1.0.0"]);
    let entry = make_entry(
        "codex",
        Source::Npm {
            package: "@acme/codex".to_string(),
            platform_package: Some("@acme/codex-{os}-{cpu}".to_string()),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@acme/codex-{os}-{cpu}/codex",
        None,
        vec!["codex", "login"],
        &[],
    );
    let lc = test_lifecycle(
        root.path(),
        vec![entry],
        vec![],
        Arc::clone(&leases) as Arc<dyn LeaseView>,
    );

    assert_eq!(lc.ensure("codex").unwrap().version, "1.0.0");

    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "2.0.0",
        "codex",
        &fake_script("2.0.0", 0),
    );
    assert!(matches!(lc.tick("codex").unwrap(), Tick::Promoted { .. }));

    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "3.0.0",
        "codex",
        &fake_script("3.0.0", 0),
    );
    assert!(matches!(lc.tick("codex").unwrap(), Tick::Promoted { .. }));

    // 1.0.0 is leased, 2.0.0 is previous, 3.0.0 is current -> all 3 kept!
    let base = root.path().join("harnesses/codex");
    assert!(base.join("1.0.0").is_dir());
    assert!(base.join("2.0.0").is_dir());
    assert!(base.join("3.0.0").is_dir());

    // An unreceipted dir and seat-bin/unpinned/legacy must never be touched by gc.
    std::fs::create_dir_all(base.join("seat-bin")).unwrap();
    std::fs::create_dir_all(base.join("unpinned")).unwrap();
    std::fs::create_dir_all(base.join("manual-dir")).unwrap();
    std::fs::create_dir_all(base.join(".staging-stale")).unwrap();

    // Release 1.0.0 lease, but inject a lease-read error first -> GC fails closed.
    leases.set_err("database locked");
    assert!(matches!(lc.gc("codex"), Err(InstallError::Leases(_))));
    assert!(base.join("1.0.0").is_dir());

    // Now lease read succeeds with no leases -> 1.0.0 is collected, 2.0.0 + 3.0.0 remain.
    leases.set(&[]);
    assert_eq!(lc.gc("codex").unwrap(), 1);
    assert!(!base.join("1.0.0").exists());
    assert!(base.join("2.0.0").is_dir());
    assert!(base.join("3.0.0").is_dir());
    assert!(base.join("seat-bin").is_dir());
    assert!(base.join("unpinned").is_dir());
    assert!(base.join("manual-dir").is_dir());
    assert!(!base.join(".staging-stale").exists());
}

#[test]
fn invariant_9_doctor_failure_refuses_and_confirm_failure_rolls_back() {
    let reg = FixtureRegistry::start();
    let platform = test_platform();
    let plat_pkg = format!("@acme/opencode-{}-{}", platform.os, platform.cpu);
    publish_npm_platform_release(
        &reg,
        "@acme/opencode",
        &plat_pkg,
        "1.0.0",
        "bin/opencode",
        &fake_script("1.0.0", 0),
    );

    let root = tempfile::tempdir().unwrap();
    let entry = make_entry(
        "opencode",
        Source::Npm {
            package: "@acme/opencode".to_string(),
            platform_package: Some("@acme/opencode-{os}-{cpu}".to_string()),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@acme/opencode-{os}-{cpu}/bin/opencode",
        None,
        vec!["opencode", "auth", "login"],
        &[],
    );
    let lc = test_lifecycle(root.path(), vec![entry], vec![], Arc::new(NoLeases));

    assert_eq!(lc.ensure("opencode").unwrap().version, "1.0.0");
    assert!(lc.confirm("opencode", "1.0.0", true).unwrap());
    // Confirmed version is never rolled back on a subsequent turn failure.
    assert!(!lc.confirm("opencode", "1.0.0", false).unwrap());

    // 2.0.0 exits 1 during doctor -> Refused, current stays 1.0.0, never re-promoted.
    publish_npm_platform_release(
        &reg,
        "@acme/opencode",
        &plat_pkg,
        "2.0.0",
        "bin/opencode",
        &fake_script("2.0.0", 1),
    );
    let res = lc.tick("opencode").unwrap();
    assert!(matches!(res, Tick::Refused { ref version, .. } if version == "2.0.0"));
    assert_eq!(lc.ensure("opencode").unwrap().version, "1.0.0");
    assert_eq!(
        lc.tick("opencode").unwrap(),
        Tick::SkippedBad {
            version: "2.0.0".to_string()
        }
    );

    // 3.0.0 passes doctor, promotes, then fails its first real turn -> rolls back to 1.0.0.
    publish_npm_platform_release(
        &reg,
        "@acme/opencode",
        &plat_pkg,
        "3.0.0",
        "bin/opencode",
        &fake_script("3.0.0", 0),
    );
    assert_eq!(
        lc.tick("opencode").unwrap(),
        Tick::Promoted {
            from: Some("1.0.0".to_string()),
            to: "3.0.0".to_string()
        }
    );
    assert_eq!(lc.ensure("opencode").unwrap().version, "3.0.0");

    assert!(lc.confirm("opencode", "3.0.0", false).unwrap());
    assert_eq!(lc.ensure("opencode").unwrap().version, "1.0.0");
    assert_eq!(
        lc.tick("opencode").unwrap(),
        Tick::SkippedBad {
            version: "3.0.0".to_string()
        }
    );
    let status = lc.status("opencode").unwrap();
    assert_eq!(status.current.unwrap().version, "1.0.0");
    assert_eq!(status.bad.len(), 2);
}

#[test]
fn adopts_newest_legacy_dir_and_leaves_others_untouched() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("harnesses/claude-code");
    let platform = test_platform();
    let rel = format!(
        "node_modules/@anthropic-ai/claude-code-{}-{}/claude",
        platform.os, platform.cpu
    );

    for (ver, delay) in [("2.1.320", false), ("2.1.323", true)] {
        if delay {
            std::thread::sleep(Duration::from_millis(30));
        }
        let launcher = base.join(ver).join(&rel);
        std::fs::create_dir_all(launcher.parent().unwrap()).unwrap();
        std::fs::write(&launcher, fake_script(ver, 0)).unwrap();
        std::fs::write(base.join(ver).join(".installed"), ver).unwrap();
    }
    std::fs::create_dir_all(base.join("seat-bin")).unwrap();
    std::fs::create_dir_all(base.join("unpinned")).unwrap();

    let entry = make_entry(
        "claude-code",
        Source::Unconfigured,
        "node_modules/@anthropic-ai/claude-code-{os}-{cpu}/claude",
        None,
        vec!["claude", "auth", "login"],
        &[],
    );
    let lc = test_lifecycle(root.path(), vec![entry], vec![], Arc::new(NoLeases));

    let installed = lc.ensure("claude-code").unwrap();
    assert_eq!(installed.version, "2.1.323");

    let receipt = crate::receipt::read_receipt(&lc.harnesses, "claude-code", "2.1.323")
        .unwrap()
        .unwrap();
    assert_eq!(receipt.source, "adopted");
    assert!(receipt.digest.starts_with("tree-sha256:"));
    assert!(base.join("2.1.320").is_dir());
    assert!(base.join("seat-bin").is_dir());
    assert!(base.join("unpinned").is_dir());

    // Confirming an adopted version works and sets confirmed_at.
    assert!(lc.confirm("claude-code", "2.1.323", true).unwrap());
    let receipt = crate::receipt::read_receipt(&lc.harnesses, "claude-code", "2.1.323")
        .unwrap()
        .unwrap();
    assert!(receipt.confirmed_at.is_some());
    let _ = SystemTime::now();
}

#[test]
fn runtime_managed_npm_tree_github_release_vendor_manifest_local_and_sign_in() {
    let reg = FixtureRegistry::start();
    let platform = test_platform();

    // Fake node tarball: bin/node handles --version and npm-cli.js install --prefix <dir>.
    let node_script = br#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "v26.13.1"
  exit 0
fi
case "$1" in
  *npm-cli.js) ;;
  *) exec /bin/sh "$@" ;;
esac
shift # npm-cli.js
shift # install
prefix=""
while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) prefix="$2"; shift 2 ;;
    *) shift ;;
  esac
done
/bin/mkdir -p "$prefix/node_modules/@deepseek-ai/dsh/lib"
echo 'echo "1.2.3"' > "$prefix/node_modules/@deepseek-ai/dsh/lib/bin.js"
exit 0
"#;
    let node_tgz = tar_gz(&[
        ("node-v26.13.1/bin/node", node_script, 0o755),
        (
            "node-v26.13.1/lib/node_modules/npm/bin/npm-cli.js",
            b"// npm",
            0o644,
        ),
    ]);
    let node_sha = hex_sha256(&node_tgz);
    reg.put("/node.tar.gz", node_tgz);

    let node_runtime = make_entry(
        "node",
        Source::Pinned {
            version: "26.13.1".to_string(),
            artifacts: BTreeMap::from([(
                platform.key(),
                PinnedArtifact {
                    url: reg.url("/node.tar.gz"),
                    sha256: node_sha,
                },
            )]),
            format: Format::TarGz,
            strip: 1,
        },
        "bin/node",
        None,
        vec![],
        &[],
    );

    // dsh uses NpmTree + node runtime
    let dsh_tgz = tar_gz(&[("package/lib/bin.js", b"console.log('1.2.3')", 0o644)]);
    reg.put_json(
        "/@deepseek-ai/dsh/latest",
        &serde_json::json!({
            "name": "@deepseek-ai/dsh",
            "version": "1.2.3",
            "dist": {
                "tarball": reg.url("/dsh.tgz"),
                "integrity": sri_sha512(&dsh_tgz),
            }
        }),
    );
    let dsh = make_entry(
        "dsh",
        Source::NpmTree {
            package: "@deepseek-ai/dsh".to_string(),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@deepseek-ai/dsh/lib/bin.js",
        Some("node"),
        vec!["dsh", "--profile", "web"],
        &[("DSH_NO_UPDATE", "1")],
    );

    // GithubRelease + zip for bun
    let bun_zip = zip(&[("bun-pkg/bun", &fake_script("1.4.0", 0), 0o755)]);
    let bun_sum = format!("{}  bun-{}.zip\n", hex_sha256(&bun_zip), platform.key());
    reg.put(
        "/api/repos/oven/bun/releases/latest",
        br#"{"tag_name":"bun-v1.4.0"}"#.to_vec(),
    );
    reg.put(
        format!(
            "/dl/oven/bun/releases/download/bun-v1.4.0/bun-{}.zip",
            platform.key()
        ),
        bun_zip,
    );
    reg.put(
        "/dl/oven/bun/releases/download/bun-v1.4.0/SHASUMS256.txt",
        bun_sum.into_bytes(),
    );
    let bun_rt = make_entry(
        "bun",
        Source::GithubRelease {
            repo: "oven/bun".to_string(),
            tag_prefix: "bun-v".to_string(),
            assets: BTreeMap::from([(platform.key(), "bun-{os}-{cpu}.zip".to_string())]),
            checksums: "SHASUMS256.txt".to_string(),
            format: Format::Zip,
            strip: 1,
            api: Some(reg.url("/api")),
            download: Some(reg.url("/dl")),
        },
        "bun",
        None,
        vec![],
        &[],
    );

    // VendorManifest + Raw binary for grok
    let grok_raw = fake_script("4.2.0", 0);
    reg.put("/grok-bin", grok_raw.clone());
    reg.put_json(
        "/grok-manifest.json",
        &serde_json::json!({
            "version": "4.2.0",
            "platforms": {
                platform.key(): {
                    "url": reg.url("/grok-bin"),
                    "sha256": hex_sha256(&grok_raw),
                }
            }
        }),
    );
    let grok = make_entry(
        "grok",
        Source::VendorManifest {
            url: reg.url("/grok-manifest.json"),
            format: Format::Raw,
            strip: 0,
        },
        "grok",
        None,
        vec!["grok", "login"],
        &[],
    );

    // Local binary for spiron
    let root = tempfile::tempdir().unwrap();
    let local_bin = root.path().join("host-spiron");
    let spiron_bytes = fake_script("0.9.0", 0);
    std::fs::write(&local_bin, &spiron_bytes).unwrap();
    let spiron = make_entry(
        "spiron",
        Source::Local {
            version: "0.9.0".to_string(),
            path: local_bin,
            sha256: hex_sha256(&spiron_bytes),
        },
        "spiron",
        None,
        vec!["spiron", "login"],
        &[],
    );

    let cloud = make_entry(
        "claude-cloud",
        Source::Builtin,
        "",
        None,
        vec!["claude", "auth", "login"],
        &[],
    );

    let lc = test_lifecycle(
        root.path(),
        vec![dsh, grok, spiron, cloud],
        vec![node_runtime, bun_rt],
        Arc::new(NoLeases),
    );

    let dsh_inst = lc.ensure("dsh").unwrap();
    assert_eq!(dsh_inst.version, "1.2.3");
    assert_eq!(
        dsh_inst.runtime.as_deref(),
        Some(
            root.path()
                .join("harnesses/node/26.13.1/bin/node")
                .as_path()
        )
    );
    assert_eq!(lc.ensure("bun").unwrap().version, "1.4.0");
    assert_eq!(lc.ensure("grok").unwrap().version, "4.2.0");
    assert_eq!(lc.ensure("spiron").unwrap().version, "0.9.0");

    // Sign-in invocation for runtime-backed harness uses runtime as program.
    let staging = root.path().join("login-staging");
    let inv = lc.sign_in_invocation("dsh", &staging).unwrap();
    assert_eq!(inv.program, dsh_inst.runtime.unwrap());
    assert_eq!(
        inv.args,
        vec![
            dsh_inst.launcher.display().to_string(),
            "--profile".to_string(),
            "web".to_string()
        ]
    );
    assert_eq!(inv.env.get("HOME"), Some(&staging.display().to_string()));
    assert_eq!(inv.env.get("DSH_NO_UPDATE"), Some(&"1".to_string()));

    // Sign-in invocation for native binary uses launcher directly.
    let grok_inv = lc.sign_in_invocation("grok", &staging).unwrap();
    assert_eq!(
        grok_inv.program,
        root.path().join("harnesses/grok/4.2.0/grok")
    );
    assert_eq!(grok_inv.args, vec!["login"]);

    // Builtin harness is not managed.
    assert!(matches!(
        lc.ensure("claude-cloud"),
        Err(InstallError::NotManaged(_))
    ));
    assert!(matches!(
        lc.sign_in_invocation("claude-cloud", &staging),
        Err(InstallError::NotManaged(_))
    ));
}

#[test]
fn corrupt_records_and_legacy_same_name_dir_fail_safe() {
    let reg = FixtureRegistry::start();
    let platform = test_platform();
    let plat_pkg = format!("@acme/codex-{}-{}", platform.os, platform.cpu);
    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "1.0.0",
        "codex",
        &fake_script("1.0.0", 0),
    );

    let root = tempfile::tempdir().unwrap();
    let entry = make_entry(
        "codex",
        Source::Npm {
            package: "@acme/codex".to_string(),
            platform_package: Some("@acme/codex-{os}-{cpu}".to_string()),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@acme/codex-{os}-{cpu}/codex",
        None,
        vec!["codex", "login"],
        &[],
    );
    let lc = test_lifecycle(root.path(), vec![entry], vec![], Arc::new(NoLeases));
    assert_eq!(lc.ensure("codex").unwrap().version, "1.0.0");

    // Pre-create a legacy 2.0.0 dir with no receipt; tick moves it to .legacy-* and promotes.
    let unreceipted = root.path().join("harnesses/codex/2.0.0");
    std::fs::create_dir_all(&unreceipted).unwrap();
    std::fs::write(unreceipted.join("old-junk"), b"old").unwrap();
    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "2.0.0",
        "codex",
        &fake_script("2.0.0", 0),
    );
    assert_eq!(
        lc.tick("codex").unwrap(),
        Tick::Promoted {
            from: Some("1.0.0".to_string()),
            to: "2.0.0".to_string()
        }
    );
    assert!(
        std::fs::read_dir(root.path().join("harnesses/codex"))
            .unwrap()
            .filter_map(Result::ok)
            .any(|e| e
                .file_name()
                .to_string_lossy()
                .starts_with(".legacy-2.0.0-"))
    );

    // Corrupt current.json fails closed.
    std::fs::write(root.path().join("harnesses/codex/current.json"), "{broken").unwrap();
    assert!(matches!(lc.ensure("codex"), Err(InstallError::Corrupt(_))));
}

#[test]
fn store_harness_leases_protect_leased_versions() {
    use anyagents_core::{NewAgent, ProcessIdentity, Store};

    let reg = FixtureRegistry::start();
    let platform = test_platform();
    let plat_pkg = format!("@acme/codex-{}-{}", platform.os, platform.cpu);
    publish_npm_platform_release(
        &reg,
        "@acme/codex",
        &plat_pkg,
        "1.0.0",
        "codex",
        &fake_script("1.0.0", 0),
    );

    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("worker", "codex"))
        .unwrap();
    let holder = ProcessIdentity {
        pid: 4242,
        start_time: 99,
    };

    let entry = make_entry(
        "codex",
        Source::Npm {
            package: "@acme/codex".to_string(),
            platform_package: Some("@acme/codex-{os}-{cpu}".to_string()),
            registry: Some(reg.addr.clone()),
        },
        "node_modules/@acme/codex-{os}-{cpu}/codex",
        None,
        vec!["codex", "login"],
        &[],
    );
    let lc = test_lifecycle(
        root.path(),
        vec![entry],
        vec![],
        Arc::clone(&store) as Arc<dyn LeaseView>,
    );

    assert_eq!(lc.ensure("codex").unwrap().version, "1.0.0");
    store
        .acquire_harness_lease(&agent.id, "codex", "1.0.0", holder)
        .unwrap();

    for ver in ["2.0.0", "3.0.0"] {
        publish_npm_platform_release(
            &reg,
            "@acme/codex",
            &plat_pkg,
            ver,
            "codex",
            &fake_script(ver, 0),
        );
        assert!(matches!(lc.tick("codex").unwrap(), Tick::Promoted { .. }));
    }

    let base = root.path().join("harnesses/codex");
    assert_eq!(lc.gc("codex").unwrap(), 0);
    assert!(base.join("1.0.0").is_dir());
    assert!(base.join("2.0.0").is_dir());
    assert!(base.join("3.0.0").is_dir());

    assert!(
        store
            .release_harness_lease(&agent.id, "codex", &holder)
            .unwrap()
    );
    assert_eq!(lc.gc("codex").unwrap(), 1);
    assert!(!base.join("1.0.0").exists());
    assert!(base.join("2.0.0").is_dir());
    assert!(base.join("3.0.0").is_dir());
}
