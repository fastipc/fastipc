//! The examples (examples/*.rs, which `cargo test` builds) run in pairs, as the README and the website run them: a
//! server, then its client (a publisher and its subscriber, a producer and its consumer, a service's server and its
//! client, in either order), each in a process of its own.

use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// The example's executable next to this test's (target/<profile>/examples), if Cargo built it.
fn example(name: &str) -> Option<PathBuf> {
    let deps = env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let path = deps.parent().unwrap().join("examples").join(format!("{name}{}", env::consts::EXE_SUFFIX));
    path.exists().then_some(path)
}

/// Runs `server`, then `client`; both must succeed. Their outputs.
fn run_pair(server: &str, client: &str) -> Option<(String, String)> {
    run_pair_in_order(server, client, false)
}

/// Runs `server` and `client`, the client first (half a second ahead) if `client_first`; both must succeed. Their
/// outputs.
fn run_pair_in_order(server: &str, client: &str, client_first: bool) -> Option<(String, String)> {
    let (Some(server_exe), Some(client_exe)) = (example(server), example(client)) else {
        eprintln!("examples: skipped {server} and {client}, not built (run `cargo test` without a target filter)");
        return None;
    };
    let (server, client) = if client_first {
        let client = Command::new(client_exe).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        thread::sleep(Duration::from_millis(500));
        let server = Command::new(server_exe).stdout(Stdio::piped()).spawn().unwrap();
        (server.wait_with_output().unwrap(), client.wait_with_output().unwrap())
    } else {
        let server = Command::new(server_exe).stdout(Stdio::piped()).spawn().unwrap();
        let client = Command::new(client_exe).output().unwrap();
        (server.wait_with_output().unwrap(), client)
    };
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
    // The broadcast's pair, in either order: the publisher waits for the subscriber, and the join for the publisher
    for subscriber_first in [false, true] {
        if let Some((_, subscriber)) = run_pair_in_order("publisher", "subscriber", subscriber_first) {
            assert_eq!(subscriber.lines().collect::<Vec<_>>(), ["tick 1", "tick 2", "tick 3"], "{subscriber_first}");
        }
    }
    // The work queue's pair, in either order: the consumer's join waits for the owner, and the jobs wait in the queue
    // (in any order: the queue promises none)
    for consumer_first in [false, true] {
        if let Some((_, consumer)) = run_pair_in_order("producer", "consumer", consumer_first) {
            let mut jobs: Vec<_> = consumer.lines().collect();
            jobs.sort_unstable();
            assert_eq!(jobs, ["job 1", "job 2", "job 3"], "{consumer_first}");
        }
    }
    // The service's pair, in either order: the client's join waits for the owner; the replies come in the order of the
    // requests
    for client_first in [false, true] {
        if let Some((_, client)) = run_pair_in_order("service_server", "service_client", client_first) {
            assert_eq!(client.lines().collect::<Vec<_>>(), ["PING 1", "PING 2", "PING 3"], "{client_first}");
        }
    }
    // The raw FFI's pair (fipc-sys's README), alone and against the safe API's
    for (server, client) in [("sys_server", "sys_client"), ("server", "sys_client"), ("sys_server", "client")] {
        if let Some((_, output)) = run_pair(server, client) {
            assert_eq!(output.trim_end(), "PING", "{server} and {client}");
        }
    }
}
