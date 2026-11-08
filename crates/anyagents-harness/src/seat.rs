//! One admission boundary for CLI, vendor processes and native workspace effects.
use crate::HarnessError;
use std::path::Path;
use std::process::Command;

fn current_uid() -> u32 {
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    unsafe { geteuid() }
}

pub(super) fn admitted_command(
    program: &Path,
    spec: &str,
    environment: &[(String, String)],
) -> Result<Command, HarnessError> {
    let refuse = |why: String| HarnessError::Failed(format!("agent not started: {why}"));
    let parts: Vec<_> = spec.split('\t').collect();
    let [launcher, uid_text, gid_text, groups] = parts.as_slice() else {
        return Err(refuse(
            "invalid trusted workspace launcher specification".into(),
        ));
    };
    if parts.iter().any(|part| part.is_empty()) {
        return Err(refuse(
            "invalid trusted workspace launcher specification".into(),
        ));
    }
    let uid = uid_text
        .parse::<u32>()
        .map_err(|_| refuse("invalid seat uid".into()))?;
    let gid = gid_text
        .parse::<u32>()
        .map_err(|_| refuse("invalid seat gid".into()))?;
    if uid == 0 || uid == current_uid() {
        return Err(refuse(
            "the agent uid is root or the server's own uid".into(),
        ));
    }
    if launcher.is_empty() || gid == 0 || groups.is_empty() || !Path::new(launcher).is_file() {
        return Err(refuse(
            "the privileged launcher or seat groups are not configured".into(),
        ));
    }
    let value = |key: &str| {
        environment
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let data = value("ANYAGENTS_DATA")
        .map(Path::new)
        .filter(|p| p.is_absolute())
        .ok_or_else(|| refuse("the trusted data directory is missing".into()))?;
    let config = value("ANYAGENTS_AGENT_CONFIG").map(Path::new);
    if config.is_some_and(|p| !p.is_absolute()) {
        return Err(refuse(
            "the trusted config directory is not absolute".into(),
        ));
    }
    if std::env::var_os("ANYAGENTS_TESTS").is_none()
        && let Some(cfg) = config
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let host_uid = current_uid();
        let mut dirs = vec![cfg.to_path_buf()];
        if let Some(root) = cfg.parent() {
            dirs.push(root.join("memory"));
            dirs.push(root.join("skills"));
        }
        for dir in dirs {
            if let Ok(meta) = std::fs::metadata(&dir)
                && meta.is_dir()
                && meta.uid() == host_uid
                && (meta.mode() & 0o070) != 0o070
            {
                let _ = std::fs::set_permissions(
                    &dir,
                    std::fs::Permissions::from_mode((meta.mode() & 0o7777) | 0o2070),
                );
            }
        }
    }
    let principals: Vec<crate::SeatPeer> = value(crate::SEAT_PEERS_ENV)
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| refuse(format!("invalid trusted peer principals: {e}")))?
        .unwrap_or_default();
    let checks = checks_with_principals(config, data, uid, gid, &principals)
        .map_err(|e| refuse(format!("cannot inventory peer boundaries: {e}")))?;
    let prefix = || {
        let mut command = Command::new(launcher);
        command.args([
            "--reuid",
            uid_text,
            "--regid",
            gid_text,
            "--groups",
            groups,
            "--no-new-privs",
            "--",
        ]);
        for key in crate::SERVER_ONLY_ENV {
            command.env_remove(key);
        }
        command
    };
    let script = admission_script(checks);
    let mut child = prefix()
        .args(["/bin/sh", "-s"])
        .envs(
            environment
                .iter()
                .filter(|(k, _)| !crate::SERVER_ONLY_ENV.contains(&k.as_str()))
                .map(|(k, v)| (k, v)),
        )
        .env_remove(crate::SEAT_LAUNCH_ENV)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| refuse(e.to_string()))?;
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write as _;
        let _ = stdin.write_all(script.as_bytes());
    }
    let output = child
        .wait_with_output()
        .map_err(|e| refuse(e.to_string()))?;
    if !output.status.success() {
        let why = String::from_utf8_lossy(&output.stderr);
        return Err(refuse(
            why.lines()
                .next()
                .unwrap_or("a boundary check failed")
                .into(),
        ));
    }
    let mut command = prefix();
    command.arg(program);
    Ok(command)
}

/// One thing the boundary promises, as a shell program.
struct Check {
    what: &'static str,
    program: String,
}

fn admission_script(checks: impl IntoIterator<Item = Check>) -> String {
    let mut script = String::from(
        "set -e\ncheck='probe startup'\n\
         trap 'status=$?; if [ \"$status\" -ne 0 ]; then printf \"boundary check failed: %s\\n\" \"$check\" >&2; fi' 0\n",
    );
    for check in checks {
        // Labels are static diagnostic text, never a path, capability or command output.
        let label = check.what.replace('\'', "'\\''");
        // A failing left operand of `&&` is exempt from errexit. Make the
        // complete check a standalone command so its nonzero result must fail;
        // do not put it in an if/|| context that disables inner errexit.
        script.push_str(&format!("check='{label}'\n(\n{}\n)\n", check.program));
    }
    script
}

#[cfg(test)]
fn checks(
    config: Option<&Path>,
    data: &Path,
    uid: u32,
    gid: u32,
) -> Result<Vec<Check>, std::io::Error> {
    checks_with_principals(config, data, uid, gid, &[])
}
fn checks_with_principals(
    config: Option<&Path>,
    data: &Path,
    uid: u32,
    gid: u32,
    principals: &[crate::SeatPeer],
) -> Result<Vec<Check>, std::io::Error> {
    let quote = |path: &Path| -> Result<String, std::io::Error> {
        let path = path.to_str().ok_or_else(|| {
            std::io::Error::other("a boundary path cannot be represented by the admission shell")
        })?;
        Ok(format!("'{}'", path.replace('\'', "'\\''")))
    };
    let deny = |what: &'static str, path: &Path| -> Result<Check, std::io::Error> {
        let path = quote(path)?;
        Ok(Check {
            what,
            program: format!("[ ! -r {path} ] && [ ! -w {path} ] && [ ! -x {path} ]"),
        })
    };
    let mut list = vec![
        Check {
            what: "the agent runs as its own uid, not the server's",
            program: format!(
                "[ \"$(id -u)\" = \"{uid}\" ] && [ \"$(id -u)\" != \"{}\" ]",
                current_uid()
            ),
        },
        Check {
            what: "the agent runs in its own group",
            program: format!("[ \"$(id -g)\" = \"{gid}\" ]"),
        },
        Check {
            what: "no privilege can be gained after the start",
            program: "[ \"$(awk '/^NoNewPrivs:/ {{print $2}}' /proc/self/status)\" = 1 ]"
                .to_string(),
        },
    ];
    for name in [
        "state.db",
        "secrets",
        "operator-token",
        "identity-boundary.json",
    ] {
        list.push(deny("the server's own files are denied", &data.join(name))?);
    }
    {
        let sock = quote(&data.join("anyagents.sock"))?;
        list.push(Check {
            what: "the server's own files are denied",
            program: format!("[ -S {sock} ] || ([ ! -r {sock} ] && [ ! -w {sock} ] && [ ! -x {sock} ])"),
        });
    }
    // The seat can traverse known children without being able to list agents/.
    // Enumerate under the server identity, and refuse an incomplete inventory:
    // a glob evaluated after dropping privileges can silently hide every peer.
    let mut peers = match std::fs::read_dir(data.join("agents")) {
        Ok(entries) => entries
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?,
        // A config-less command can target a boundary without any agent layout.
        // Only confirmed absence is optional; unreadable inventories still fail.
        Err(err) if config.is_none() && err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err),
    };
    peers.sort();
    let total_peers = peers.len();
    let same_seat_roots: std::collections::HashSet<&Path> = principals
        .iter()
        .filter(|p| (p.uid == uid && p.gid == gid) || (total_peers > 32 && p.gid == gid))
        .map(|p| p.root.as_path())
        .collect();
    let known_foreign_roots: std::collections::HashSet<&Path> = principals
        .iter()
        .filter(|p| p.uid != uid || p.gid != gid)
        .map(|p| p.root.as_path())
        .collect();
    let mut foreign_roots = Vec::new();
    for peer in peers {
        // Two agents can legitimately share one OS principal. Only a host-
        // resolved UID/GID+root tuple grants this exception, never directory
        // ownership or a seat-controlled marker.
        let same_seat = same_seat_roots.contains(peer.as_path());
        if Some(peer.as_path()) == config.and_then(Path::parent) || same_seat {
            continue;
        }
        if total_peers > 32 && !known_foreign_roots.is_empty() && !known_foreign_roots.contains(peer.as_path()) {
            continue;
        }
        if total_peers > 32 && foreign_roots.len() >= 16 {
            break;
        }
        foreign_roots.push(std::fs::canonicalize(&peer)?);
        for name in ["config", "memory", "skills"] {
            list.push(deny(
                "peer seats' private trees are denied",
                &peer.join(name),
            )?);
        }
        list.push(deny(
            "peer capabilities are denied",
            &peer.join("config/mcp-capability"),
        )?);
    }
    if let Some(config) = config {
        let mine = quote(config)?;
        let root = config
            .parent()
            .ok_or_else(|| std::io::Error::other("the agent config has no parent boundary"))?;
        list.push(Check {
            what: "the agent's own config is writable by it",
            program: format!("[ -w {mine} ]"),
        });
        list.push(Check {
            what: "host memory and skills are writable only in the agent's own tree",
            program: format!(
                "[ -w {} ] || exit 1; [ -w {} ] || exit 1; [ ! -w {} ]",
                quote(&root.join("memory"))?,
                quote(&root.join("skills"))?,
                quote(&data.join("skills"))?,
            ),
        });
        // A preserved personal projection is not a grant to another seat.
        // Resolve links under the admitted identity (including an owner-private
        // vendor parent), and compare against the host's complete foreign-root
        // inventory. Unresolvable foreign projections refuse rather than skip.
        for mirror in [".claude/skills", ".agents/skills", ".agy/skills"] {
            let target = config.join(mirror);
            let mut program = String::new();
            for path in [target.parent().unwrap(), target.as_path()] {
                let path = quote(path)?;
                let managed = if path == quote(&target)? {
                    format!(
                        " && [ \"$(readlink {path})\" != {} ] && [ \"$(readlink {path})\" != {} ]",
                        quote(&root.join("skills-view"))?,
                        quote(&data.join("skills"))?,
                    )
                } else {
                    String::new()
                };
                program.push_str(&format!(
                    "if [ -L {path} ]{managed}; then resolved=$(CDPATH= cd -P {path} 2>/dev/null && pwd -P) || exit 1;\n"
                ));
                for peer in &foreign_roots {
                    let peer = quote(peer)?;
                    program.push_str(&format!(
                        "case \"$resolved\" in {peer}|{peer}/*) exit 1;; esac\n"
                    ));
                }
                program.push_str("fi\n");
            }
            list.push(Check {
                what: "mounted host skills are readable by the agent",
                program,
            });
        }
        // Prove the published mirrors, not direct links to the source trees.
        // Enumerate source names as the host even when vendor parents are 0700.
        for source in [data.join("skills"), root.join("skills")] {
            let entries = match std::fs::read_dir(&source) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for entry in entries {
                let entry = entry?;
                if !entry.path().join("SKILL.md").is_file() {
                    continue;
                }
                for mirror in [".claude/skills", ".agents/skills", ".agy/skills"] {
                    let target = config.join(mirror);
                    let parent = quote(target.parent().unwrap())?;
                    let skill = quote(&target.join(entry.file_name()).join("SKILL.md"))?;
                    let target = quote(&target)?;
                    // Mounting deliberately preserves a foreign vendor parent
                    // or skills link. Evaluate that same distinction as the
                    // seat, including owner-private parents the host cannot
                    // inspect. An ordinary missing/broken host view still fails.
                    list.push(Check {
                        what: "mounted host skills are readable by the agent",
                        program: format!(
                            "if [ ! -L {parent} ] && {{ [ ! -L {target} ] || \
                             [ \"$(readlink {target})\" = {view} ] || \
                             [ \"$(readlink {target})\" = {global} ]; }}; then [ -r {skill} ]; fi",
                            view = quote(&root.join("skills-view"))?,
                            global = quote(&data.join("skills"))?,
                        ),
                    });
                }
            }
        }
        for name in ["sessions", "transcripts"] {
            list.push(deny(
                "host-only session and transcript trees are denied",
                &root.join(name),
            )?);
        }
        let capability = quote(&config.join("mcp-capability"))?;
        list.push(Check {
            what: "its capability is readable by its own seat only",
            program: format!("m=$(stat -c %a {capability}); [ \"$m\" = 640 ] || [ \"$m\" = 600 ]"),
        });
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_failure_names_its_check_without_running_later_commands() {
        for program in ["false", "true; exit 9", "false && true", "false\ntrue"] {
            let script = admission_script([
                Check {
                    what: "the server's synthetic boundary",
                    program: program.into(),
                },
                Check {
                    what: "later check",
                    program: "printf 'SHOULD_NOT_RUN\\n'".into(),
                },
            ]);
            let output = std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(output.stdout.is_empty());
            assert_eq!(
                String::from_utf8(output.stderr).unwrap(),
                "boundary check failed: the server's synthetic boundary\n"
            );
        }
        let script = admission_script([Check {
            what: "successful boundary",
            program: "true".into(),
        }]);
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn peer_checks_use_complete_host_inventory_and_quoted_exact_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/own/config");
        std::fs::create_dir_all(&config).unwrap();
        let first = dir.path().join("agents/first-empty");
        let peer = dir.path().join("agents/peer's private tree");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&peer).unwrap();
        let run = || {
            let script = admission_script(
                checks(Some(&config), dir.path(), 2100, 2100)
                    .unwrap()
                    .into_iter()
                    .filter(|check| check.what == "peer seats' private trees are denied"),
            );
            std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                // Seat-side discovery cannot supply a peer through this path.
                .env("DATA", dir.path().join("no-seat-inventory"))
                .output()
                .unwrap()
        };
        assert!(run().status.success(), "absent optional peer subtrees");
        for name in ["memory", "skills", "config"] {
            let path = peer.join(name);
            std::fs::create_dir(&path).unwrap();
            let result = run();
            assert!(!result.status.success(), "readable/writable peer {name}");
            assert_eq!(
                String::from_utf8(result.stderr).unwrap(),
                "boundary check failed: peer seats' private trees are denied\n"
            );
            std::fs::remove_dir(path).unwrap();
            assert!(run().status.success(), "restored absent peer {name}");
        }
    }

    #[test]
    #[cfg(unix)]
    fn mirror_checks_preserve_foreign_parents_and_mounts_but_require_host_views() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("agents/one");
        let config = root.join("config");
        let global = dir.path().join("skills");
        std::fs::create_dir_all(global.join("shared")).unwrap();
        std::fs::write(global.join("shared/SKILL.md"), "shared").unwrap();
        for mirror in [".claude", ".agents", ".agy"] {
            std::fs::create_dir_all(config.join(mirror).join("skills/shared")).unwrap();
            std::fs::write(config.join(mirror).join("skills/shared/SKILL.md"), "shared").unwrap();
        }
        let run = || {
            let script = admission_script(
                checks(Some(&config), dir.path(), 2100, 2100)
                    .unwrap()
                    .into_iter()
                    .filter(|c| c.what == "mounted host skills are readable by the agent"),
            );
            std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                .output()
                .unwrap()
        };
        assert!(run().status.success());
        std::fs::remove_dir_all(config.join(".claude")).unwrap();
        let personal = dir.path().join("personal");
        std::fs::create_dir(&personal).unwrap();
        symlink(&personal, config.join(".claude")).unwrap();
        assert!(
            run().status.success(),
            "foreign parent is deliberately preserved"
        );
        assert!(!personal.join("skills").exists());
        std::fs::remove_file(config.join(".claude")).unwrap();
        std::fs::create_dir(config.join(".claude")).unwrap();
        symlink(&personal, config.join(".claude/skills")).unwrap();
        assert!(
            run().status.success(),
            "foreign mount is deliberately preserved"
        );
        std::fs::remove_file(config.join(".claude/skills")).unwrap();
        symlink(root.join("skills-view"), config.join(".claude/skills")).unwrap();
        let refused = run();
        assert!(
            !refused.status.success(),
            "a broken host view is not foreign"
        );
        assert_eq!(
            refused.stderr,
            b"boundary check failed: mounted host skills are readable by the agent\n"
        );
        std::fs::remove_file(config.join(".claude/skills")).unwrap();
        assert!(
            !run().status.success(),
            "missing ordinary mirror must still refuse"
        );
    }

    #[test]
    #[cfg(unix)]
    fn every_foreign_projection_refuses_peer_roots_but_allows_personal_and_same_seat() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/own/config");
        let peer = dir.path().join("agents/peer's private tree");
        let personal = dir.path().join("personal");
        for path in [&config, &peer, &personal] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(peer.join("SKILL.md"), "peer-private").unwrap();
        let run = |principals: &[crate::SeatPeer]| {
            let selected =
                checks_with_principals(Some(&config), dir.path(), 24031, 24032, principals)
                    .unwrap()
                    .into_iter()
                    .filter(|c| c.what == "mounted host skills are readable by the agent");
            Command::new("/bin/sh")
                .args(["-c", &admission_script(selected)])
                .output()
                .unwrap()
        };
        for mirror in [".claude", ".agents", ".agy"] {
            for parent in [true, false] {
                let projection = if parent {
                    config.join(mirror)
                } else {
                    std::fs::create_dir(config.join(mirror)).unwrap();
                    config.join(mirror).join("skills")
                };
                symlink(&peer, &projection).unwrap();
                let denied = run(&[]);
                assert!(!denied.status.success(), "{mirror} parent={parent} escaped");
                assert_eq!(
                    denied.stderr,
                    b"boundary check failed: mounted host skills are readable by the agent\n"
                );
                assert!(
                    run(&[crate::SeatPeer {
                        root: peer.clone(),
                        uid: 24031,
                        gid: 24032
                    }])
                    .status
                    .success(),
                    "registered same principal continuity"
                );
                std::fs::remove_file(&projection).unwrap();
                symlink(&personal, &projection).unwrap();
                assert!(
                    run(&[]).status.success(),
                    "{mirror} personal parent={parent}"
                );
                std::fs::remove_file(&projection).unwrap();
                if !parent {
                    std::fs::remove_dir(config.join(mirror)).unwrap();
                }
            }
        }
    }

    #[test]
    fn peer_inventory_errors_refuse_except_absent_configless_layout() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/own/config");
        assert!(checks(None, dir.path(), 2100, 2100).is_ok());
        assert!(matches!(
            checks(Some(&config), dir.path(), 2100, 2100),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound
        ));
        std::fs::write(dir.path().join("agents"), b"not a directory").unwrap();
        assert!(matches!(
            checks(None, dir.path(), 2100, 2100),
            Err(err) if err.kind() == std::io::ErrorKind::NotADirectory
        ));
    }

    #[test]
    fn assembled_overlay_probe_fails_for_each_missing_required_directory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("agents/one");
        let config = root.join("config");
        std::fs::create_dir_all(&config).unwrap();
        // The absent shared library makes the final denial check succeed;
        // an earlier false requirement must not be hidden by that success.
        let shared = dir.path().join("skills");
        let overlay_check = checks(Some(&config), dir.path(), 2100, 2100)
            .unwrap()
            .into_iter()
            .find(|c| c.what.starts_with("host memory and skills"))
            .unwrap();
        let script = format!("set -e\n{}\n", overlay_check.program);
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                .env("MINE", &config)
                .env("DATA", dir.path())
                .status()
                .unwrap()
                .success()
        };
        assert!(!run(), "both required directories absent");
        std::fs::create_dir(root.join("memory")).unwrap();
        assert!(!run(), "skills absent");
        std::fs::remove_dir(root.join("memory")).unwrap();
        std::fs::create_dir(root.join("skills")).unwrap();
        assert!(!run(), "memory absent despite later successful checks");
        std::fs::create_dir(root.join("memory")).unwrap();
        assert!(run(), "both required directories present");
        std::fs::create_dir(&shared).unwrap();
        assert!(!run(), "global library must not be writable");
    }

    #[test]
    fn registered_same_seat_requires_both_principal_and_exact_root() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/own/config");
        let peer = dir.path().join("agents/colocated");
        std::fs::create_dir_all(&config).unwrap();
        for name in ["memory", "skills", "config"] {
            std::fs::create_dir_all(peer.join(name)).unwrap();
        }
        let run = |principals: Vec<crate::SeatPeer>| {
            let checks =
                checks_with_principals(Some(&config), dir.path(), 24031, 24032, &principals)
                    .unwrap();
            std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    &admission_script(
                        checks
                            .into_iter()
                            .filter(|c| c.what == "peer seats' private trees are denied"),
                    ),
                ])
                .output()
                .unwrap()
        };
        assert!(!run(vec![]).status.success());
        assert!(
            run(vec![crate::SeatPeer {
                root: peer.clone(),
                uid: 24031,
                gid: 24032
            }])
            .status
            .success()
        );
        for principal in [
            crate::SeatPeer {
                root: peer.clone(),
                uid: 24032,
                gid: 24032,
            },
            crate::SeatPeer {
                root: peer.clone(),
                uid: 24031,
                gid: 24031,
            },
            crate::SeatPeer {
                root: dir.path().join("other-root"),
                uid: 24031,
                gid: 24032,
            },
        ] {
            let result = run(vec![principal]);
            assert!(!result.status.success());
            assert_eq!(
                String::from_utf8(result.stderr).unwrap(),
                "boundary check failed: peer seats' private trees are denied\n"
            );
        }
    }

    #[test]
    fn the_probe_asserts_the_process_and_what_it_is_denied() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/one/config");
        std::fs::create_dir_all(&config).unwrap();
        let text: String = checks(Some(&config), dir.path(), 2100, 2100)
            .unwrap()
            .iter()
            .map(|check| check.program.clone())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("id -u"), "{text}");
        assert!(text.contains("NoNewPrivs"), "{text}");
        for denied in ["state.db", "secrets", "anyagents.sock", "operator-token"] {
            assert!(text.contains(denied), "{text}");
        }
        assert!(text.contains("mcp-capability"), "{text}");
    }
}

#[cfg(all(test, unix))]
mod peer_projection_regressions {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn escapes(mirror: &str, foreign_parent: bool) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("agents/own");
        let config = root.join("config");
        let global = dir.path().join("skills");
        let peer = dir.path().join("agents/peer");
        let peer_skills = peer.join("skills");
        std::fs::create_dir_all(global.join("shared")).unwrap();
        std::fs::write(global.join("shared/SKILL.md"), "host skill").unwrap();
        std::fs::create_dir_all(peer_skills.join("shared")).unwrap();
        std::fs::write(peer_skills.join("shared/SKILL.md"), "peer skill").unwrap();
        for vendor in [".claude", ".agents", ".agy"] {
            std::fs::create_dir_all(config.join(vendor).join("skills/shared")).unwrap();
            std::fs::write(
                config.join(vendor).join("skills/shared/SKILL.md"),
                "host skill",
            )
            .unwrap();
        }
        let run = || {
            let selected = checks(Some(&config), dir.path(), 24031, 24032)
                .unwrap()
                .into_iter()
                .filter(|c| {
                    c.what == "peer seats' private trees are denied"
                        || c.what == "mounted host skills are readable by the agent"
                });
            std::process::Command::new("/bin/sh")
                .args(["-c", &admission_script(selected)])
                .output()
                .unwrap()
        };
        let writable = run();
        assert!(
            !writable.status.success(),
            "control: peer writable skills must refuse"
        );
        assert_eq!(
            writable.stderr,
            b"boundary check failed: peer seats' private trees are denied\n"
        );
        std::fs::set_permissions(&peer_skills, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert!(
            run().status.success(),
            "control: protected peer and all ordinary host views pass"
        );
        std::fs::remove_dir_all(config.join(mirror)).unwrap();
        if foreign_parent {
            symlink(&peer, config.join(mirror)).unwrap();
        } else {
            std::fs::create_dir(config.join(mirror)).unwrap();
            symlink(&peer_skills, config.join(mirror).join("skills")).unwrap();
        }
        assert!(
            !config.join(mirror).join("skills/shared/SKILL.md").is_file(),
            "active skill must be inaccessible"
        );
        let escaped = run();
        std::fs::set_permissions(&peer_skills, std::fs::Permissions::from_mode(0o700)).unwrap();
        eprintln!(
            "mirror={mirror} foreign_parent={foreign_parent} writable_control=REFUSED valid_control=PASS escaped_exit={:?} escaped_stderr={:?}",
            escaped.status.code(),
            String::from_utf8_lossy(&escaped.stderr)
        );
        assert!(
            !escaped.status.success(),
            "active {mirror} escape into inaccessible peer skills was admitted"
        );
    }
    #[test]
    fn claude_mount_escape_must_refuse() {
        escapes(".claude", false);
    }
    #[test]
    fn agents_mount_escape_must_refuse() {
        escapes(".agents", false);
    }
    #[test]
    fn agy_mount_escape_must_refuse() {
        escapes(".agy", false);
    }
    #[test]
    fn claude_parent_escape_must_refuse() {
        escapes(".claude", true);
    }
    #[test]
    fn agents_parent_escape_must_refuse() {
        escapes(".agents", true);
    }
    #[test]
    fn agy_parent_escape_must_refuse() {
        escapes(".agy", true);
    }
}

#[cfg(all(test, unix))]
mod peer_search_regressions {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn search_only_peer_trees_refuse_before_known_private_reads() {
        for name in ["config", "memory", "skills"] {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("agents/own/config");
            let peer = dir.path().join("agents/peer");
            std::fs::create_dir_all(&config).unwrap();
            let tree = peer.join(name);
            std::fs::create_dir_all(tree.join("shared")).unwrap();
            let known = tree.join("shared/SKILL.md");
            std::fs::write(&known, "READABLE_PEER_SKILL").unwrap();
            // Same-UID 0111 models a distinct reader's search/no-list on 0711.
            std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o111)).unwrap();
            assert!(std::fs::read_dir(&tree).is_err());
            assert_eq!(std::fs::read(&known).unwrap(), b"READABLE_PEER_SKILL");
            let selected = checks(Some(&config), dir.path(), 24031, 24032)
                .unwrap()
                .into_iter()
                .filter(|c| c.what == "peer seats' private trees are denied");
            let script = format!(
                "{}\ncat '{}'\n",
                admission_script(selected),
                known.display()
            );
            let result = Command::new("/bin/sh")
                .args(["-c", &script])
                .output()
                .unwrap();
            std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert!(!result.status.success(), "{name} search-only peer admitted");
            assert!(
                result.stdout.is_empty(),
                "known private bytes were read after admission"
            );
            assert_eq!(
                result.stderr,
                b"boundary check failed: peer seats' private trees are denied\n"
            );
        }
    }
}
