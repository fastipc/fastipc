//! The examples (examples/*.rs, which `cargo test` builds) run in pairs, as the README and the website run them: a
//! server, then its client, each in a process of its own.

use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// The example's executable next to this test's (target/<profile>/examples), if Cargo built it.
fn example(name: &str) -> Option<PathBuf> {
    let deps = env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let path = deps.parent().unwrap().join("examples").join(format!("{name}{}", env::consts::EXE_SUFFIX));
    path.exists().then_some(path)
}

/// Runs `server`, then `client`; both must succeed. Their outputs.
fn run_pair(server: &str, client: &str) -> Option<(String, String)> {
    let (Some(server_exe), Some(client_exe)) = (example(server), example(client)) else {
        eprintln!("examples: skipped {server} and {client}, not built (run `cargo test` without a target filter)");
        return None;
    };
    let server = Command::new(server_exe).stdout(Stdio::piped()).spawn().unwrap();
    let client = Command::new(client_exe).output().unwrap();
    let server = server.wait_with_output().unwrap();
    assert!(client.status.success(), "{}", String::from_utf8_lossy(&client.stderr));
    assert!(server.status.success(), "{}", String::from_utf8_lossy(&server.stderr));
    Some((String::from_utf8(server.stdout).unwrap(), String::from_utf8(client.stdout).unwrap()))
}

/// One test, so that no two pairs use a name at once.
#[test]
fn examples_run_in_pairs() {
    if let Some((_, client)) = run_pair("server", "client") {
        assert_eq!(client.trim_end(), "PING");
    }
    if let Some((server, client)) = run_pair("echo_server", "echo_client") {
        assert_eq!(client.lines().collect::<Vec<_>>(), ["hello", "shared", "memory"]);
        assert_eq!(server.trim_end(), "client gone");
    }
    if let Some((server, client)) = run_pair("echo_server", "game_loop") {
        assert!(client.trim_end().ends_with("echoes in 60 frames"), "{client}");
        let echoes: u32 = client.split_whitespace().next().unwrap().parse().unwrap();
        assert!((50..=60).contains(&echoes), "{client}");
        assert_eq!(server.trim_end(), "client gone");
    }
    if let Some((_, client)) = run_pair("rpc_server", "rpc_client") {
        assert_eq!(client.trim_end(), "0 PING");
    }
    // The raw FFI's pair (fipc-sys's README), alone and against the safe API's
    for (server, client) in [("sys_server", "sys_client"), ("server", "sys_client"), ("sys_server", "client")] {
        if let Some((_, output)) = run_pair(server, client) {
            assert_eq!(output.trim_end(), "PING", "{server} and {client}");
        }
    }
}
