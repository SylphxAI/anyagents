//! A vendor child must not inherit the server's open files.

#[cfg(target_os = "linux")]
#[test]
fn a_child_starts_without_the_listening_socket() {
    // A listening socket stands for the server socket a child used to inherit.
    // The child lists its own descriptors, which is the only way to see what it
    // holds; the socket's must not be there.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = std::process::Command::new("sh");
    command
        .arg("-c")
        .arg("for f in /proc/self/fd/*; do readlink $f; done");
    anyagents_harness_stream::close_inherited_for_test(&mut command);
    let out = command.output().unwrap();
    let held = String::from_utf8_lossy(&out.stdout);
    assert!(!held.contains("socket:"), "the child kept a socket: {held}");
    assert!(listener.local_addr().is_ok());
}
