// Included in server::tests so these exercise the authenticated send path and
// use the same environment lock as other in-process server fixtures.
fn a2a_media_fixture(data: &Path, native: bool) {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let store = Store::open(data.join("state.db")).unwrap();
    if native {
        std::fs::set_permissions(data.join("state.db"), std::fs::Permissions::from_mode(0o600))
            .unwrap();
    }
    let agents: Vec<_> = ["author", "reader", "bystander"]
        .into_iter()
        .map(|name| {
            store
                .create_agent(anyagents_core::NewAgent::new(name, "opencode"))
                .unwrap()
        })
        .collect();
    for agent in &agents {
        store.set_agent_state(&agent.id, "running").unwrap();
    }
    let author = &agents[0];
    let reader = &agents[1];
    let bystander = &agents[2];
    let work = data.join("agents").join(author.id.as_str());
    std::fs::create_dir_all(&work).unwrap();
    let source = work.join("source.png");
    let bytes = b"\0exact attachment bytes\xff";
    std::fs::write(&source, bytes).unwrap();
    let secret = data.join("protected");
    std::fs::write(&secret, b"protected bytes").unwrap();
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::os::unix::fs::symlink(&secret, work.join("link.png")).unwrap();
    struct RestoreOwner(PathBuf);
    impl Drop for RestoreOwner {
        fn drop(&mut self) {
            let path = std::ffi::CString::new(self.0.as_os_str().as_encoded_bytes()).unwrap();
            // Restore fixture ownership so root without DAC override can clean it.
            unsafe {
                libc::chown(
                    path.as_ptr(),
                    crate::sandbox::server_uid(),
                    crate::sandbox::server_gid(),
                );
            }
        }
    }
    let _restore_work = native.then(|| RestoreOwner(work.clone()));
    if native {
        let registry: serde_json::Map<String, serde_json::Value> = agents.iter().enumerate().map(|(i, a)| {
            let uid = 24211 + i as u32;
            (a.id.to_string(), serde_json::json!({"user":format!("media-{i}"),"uid":uid,"gid":24220,"socket_gid":24220}))
        }).collect();
        // SAFETY: caller holds env_lock, restores the registry after the test.
        unsafe {
            std::env::set_var(
                "ANYAGENTS_PRINCIPALS",
                serde_json::Value::Object(registry).to_string(),
            );
        }
        // Restrict and chown the file before locking its parent directory,
        // so root without DAC override/read-search can still traverse `work`.
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
        let source_c = std::ffi::CString::new(source.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(source_c.as_ptr(), 24211, 24220) }, 0);
        std::fs::set_permissions(&work, std::fs::Permissions::from_mode(0o700)).unwrap();
        let work_c = std::ffi::CString::new(work.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(work_c.as_ptr(), 24211, 24220) }, 0);
        // Known path is readable only by the sender before send.
        assert_eq!(
            crate::agent_file::FileAuthority::of_agent_id(&store, &author.id)
                .read(&source, 100)
                .unwrap(),
            bytes
        );
        assert!(
            crate::agent_file::FileAuthority::of_agent_id(&store, &reader.id)
                .read(&source, 100)
                .is_err()
        );
    }
    let actor = Actor::Agent(author.name.clone());
    let request = |path: &Path, to: &str| SendRequest {
        sender: Some(actor.as_address()),
        to: vec![to.into()],
        // An authored attachment-like string must not be rewritten.
        text: format!("caption preserves {}", source.display()),
        media: Some(crate::api::SendMedia {
            kind: "photo".into(),
            source: path.to_string_lossy().into_owned(),
            title: None,
            performer: None,
        }),
        quote: Some(anyagents_core::Quote {
            text: "quoted source".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let send = |body: SendRequest, key: &str| {
        let mut plan =
            SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
        plan.principal_id = Some(author.id.clone());
        send_sync(&store, &actor, body, plan, Some(key.into()))
    };
    for (i, path) in [
        secret.clone(),
        work.join("link.png"),
        work.join("../../../protected"),
        data.join("state.db"),
    ]
    .iter()
    .enumerate()
    {
        assert!(send(request(path, &reader.name), &format!("deny-{i}")).is_err());
        assert!(store.inbox(&reader.id, None).unwrap().is_empty());
        assert!(!data.join("inbound-media").exists());
        assert_eq!(std::fs::read(&secret).unwrap(), b"protected bytes");
    }
    let staged_path = |recipient: &anyagents_core::Agent| {
        let inbox = store.inbox(&recipient.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        let text = &inbox[0].text;
        assert!(text.contains("quoted source"), "{text}");
        assert!(text.contains(&format!("caption preserves {}", source.display())));
        assert_eq!(
            text.matches("[Local media (photo) file: ").count(),
            1,
            "{text}"
        );
        assert!(!text.contains("[Attachment (photo)"), "{text}");
        let p = text
            .split("[Local media (photo) file: ")
            .nth(1)
            .unwrap()
            .split(']')
            .next()
            .unwrap();
        let p = PathBuf::from(p);
        assert!(p.starts_with(data.join("inbound-media").join(recipient.id.as_str())));
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        let authority = crate::agent_file::FileAuthority::of_agent_id(&store, &recipient.id);
        assert_eq!(authority.read(&p, 100).unwrap(), bytes);
        assert!(authority.write(&p, b"changed").is_err());
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        p
    };
    let _ =
        send(request(&source, &reader.name), "direct").unwrap_or_else(|e| panic!("{}", e.message));
    let direct = staged_path(reader);
    assert!(
        crate::agent_file::FileAuthority::of_agent_id(&store, &bystander.id)
            .read(&direct, 100)
            .is_err()
    );
    if native {
        assert_eq!(
            std::fs::metadata(&direct).unwrap().uid(),
            crate::sandbox::server_uid()
        );
    }
    // Whole-call replay returns its receipt without reopening a vanished source.
    if native {
        let path = std::ffi::CString::new(work.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(
            unsafe {
                libc::chown(
                    path.as_ptr(),
                    crate::sandbox::server_uid(),
                    crate::sandbox::server_gid(),
                )
            },
            0
        );
    }
    std::fs::remove_file(&source).unwrap();
    let _ =
        send(request(&source, &reader.name), "direct").unwrap_or_else(|e| panic!("{}", e.message));
    assert_eq!(
        std::fs::read_dir(direct.parent().unwrap()).unwrap().count(),
        1
    );
    std::fs::write(&source, bytes).unwrap();
    if native {
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
        let p = std::ffi::CString::new(source.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(p.as_ptr(), 24211, 24220) }, 0);
        let p = std::ffi::CString::new(work.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(p.as_ptr(), 24211, 24220) }, 0);
    }
    for a in &agents {
        store.join_group("media-room", &a.id).unwrap();
    }
    // A partial fanout failure retains the committed first copy, and removes
    // the failed recipient's staged file. Retrying must not copy the first twice.
    let db = rusqlite::Connection::open(data.join("state.db")).unwrap();
    db.execute_batch(&format!("CREATE TRIGGER media_failure BEFORE INSERT ON messages WHEN NEW.agent_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected media failure'); END;", bystander.id)).unwrap();
    assert!(send(request(&source, "group:media-room"), "room").is_err());
    let failed_dir = data.join("inbound-media").join(bystander.id.as_str());
    if failed_dir.exists() {
        assert_eq!(std::fs::read_dir(&failed_dir).unwrap().count(), 0);
    }
    db.execute_batch("DROP TRIGGER media_failure;").unwrap();
    let _ = send(request(&source, "group:media-room"), "room")
        .unwrap_or_else(|e| panic!("{}", e.message));
    assert_eq!(store.inbox(&reader.id, None).unwrap().len(), 2);
    assert_eq!(
        std::fs::read_dir(direct.parent().unwrap()).unwrap().count(),
        2
    );
    let room = staged_path(bystander);
    assert_ne!(room, direct);
    assert!(
        crate::agent_file::FileAuthority::of_agent_id(&store, &reader.id)
            .read(&room, 100)
            .is_err()
    );
    assert_eq!(std::fs::read(&secret).unwrap(), b"protected bytes");
}

#[test]
fn a2a_private_media_send_and_rollback() {
    let _env = env_lock();
    let _restore = SingleUidEnvironment::new();
    let dir = crate::test_tempdir();
    a2a_media_fixture(dir.path(), false);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires the explicit privileged cross-seat media CI lane"]
fn privileged_a2a_private_media_cross_uid() {
    use std::os::unix::fs::PermissionsExt;
    let _env = env_lock();
    let _restore = SingleUidEnvironment::new();
    assert_eq!(
        std::env::var("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").as_deref(),
        Ok("1")
    );
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "requires authorized root executor"
    );
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let caps = u64::from_str_radix(
        status
            .lines()
            .find_map(|l| l.strip_prefix("CapEff:\t"))
            .unwrap(),
        16,
    )
    .unwrap();
    assert_eq!(
        caps & 6,
        0,
        "drop DAC override/read-search to make negatives real"
    );
    assert_eq!(
        caps & ((1 << 0) | (1 << 6) | (1 << 7)),
        (1 << 0) | (1 << 6) | (1 << 7)
    );
    let old = std::env::var_os("ANYAGENTS_LAUNCHER");
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            unsafe {
                match &self.0 {
                    Some(v) => std::env::set_var("ANYAGENTS_LAUNCHER", v),
                    None => std::env::remove_var("ANYAGENTS_LAUNCHER"),
                }
            }
        }
    }
    let _launcher = Restore(old);
    unsafe {
        std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv");
    }
    let dir = tempfile::Builder::new()
        .prefix("a2a-media-")
        .tempdir_in("/var/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o711)).unwrap();
    a2a_media_fixture(dir.path(), true);
    println!("A2A_PRIVATE_MEDIA_CROSS_UID_EXECUTED");
}
