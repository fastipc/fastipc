//! Peers in other processes: this test binary run again as a peer (the test `peer`, which does nothing unless the
//! environment names a part for it), echoing, exiting, killed, serving RPC.

mod common;

use std::env;
use std::process::{self, Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::{TEN_SECONDS, pattern, unique_name};
use fipc::{Connection, Error, Listener, RpcKind};

const PEER: &str = "FIPC_RUST_TEST_PEER";
const PEER_START: Duration = Duration::from_secs(30);

/// The peer's part, when this binary runs as one: `<mode>:<name>`.
#[test]
fn peer() {
    let Ok(spec) = env::var(PEER) else {
        return;
    };
    let (mode, name) = spec.split_once(':').unwrap();
    match mode {
        // Echoes every message until the server's end
        "echo" => {
            let mut conn = Connection::connect(name, PEER_START).unwrap();
            loop {
                match conn.receive(None) {
                    Ok(message) => conn.send(&message, None).unwrap(),
                    Err(Error::Disconnected) => break,
                    Err(e) => panic!("echo: {e}"),
                }
            }
        }
        // Sends one message, then the process ends without dropping the connection
        "exit" => {
            let mut conn = Connection::connect(name, PEER_START).unwrap();
            conn.send(b"bye", None).unwrap();
            process::exit(0);
        }
        // Says it is ready, then waits to be killed
        "hang" => {
            let mut conn = Connection::connect(name, PEER_START).unwrap();
            conn.send(b"ready", None).unwrap();
            thread::sleep(Duration::from_secs(600));
        }
        // An RPC server: answers each request with its payload reversed and status = its length, until the end
        "rpc_server" => {
            let mut listener = Listener::listen(name, 1 << 16).unwrap();
            let mut conn = listener.accept(PEER_START).unwrap();
            loop {
                match conn.rpc_receive(None) {
                    Ok(request) => {
                        let reply: Vec<u8> = request.payload.iter().rev().copied().collect();
                        conn.rpc_respond(request.id, request.opcode, reply.len() as i32, &reply, None).unwrap();
                    }
                    Err(Error::Disconnected) => break,
                    Err(e) => panic!("rpc_server: {e}"),
                }
            }
        }
        _ => panic!("unknown peer mode {mode}"),
    }
}

/// This test binary as a peer of `mode` on `name`.
fn start_peer(mode: &str, name: &str) -> Child {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", "peer", "--nocapture", "--test-threads=1", "--quiet"])
        .env(PEER, format!("{mode}:{name}"))
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

/// The peer's exit, waiting up to 30 seconds.
fn wait_for(peer: &mut Child) -> process::ExitStatus {
    let start = Instant::now();
    loop {
        if let Some(status) = peer.try_wait().unwrap() {
            return status;
        }
        if start.elapsed() > Duration::from_secs(30) {
            peer.kill().unwrap();
            panic!("the peer didn't exit");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn an_echo_peer() {
    let name = unique_name("echo");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
    let mut peer = start_peer("echo", &name);
    let mut conn = listener.accept(PEER_START).unwrap();
    for size in [1, 100, 65_472, 70_000, 1_000_000] {
        let message = pattern(size);
        // A message longer than the ring needs the echo to run at once: send from a scoped thread
        let (tx, rx) = conn.split();
        let echoed = thread::scope(|s| {
            s.spawn(|| tx.send(&message, TEN_SECONDS).unwrap());
            rx.receive(TEN_SECONDS).unwrap()
        });
        assert_eq!(echoed, message, "size {size}");
    }
    drop(conn);
    assert!(wait_for(&mut peer).success());
}

#[test]
fn a_peer_that_exits() {
    let name = unique_name("exit");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
    let mut peer = start_peer("exit", &name);
    let mut conn = listener.accept(PEER_START).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"bye");
    assert_eq!(conn.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert!(wait_for(&mut peer).success());
}

#[test]
fn a_killed_peer() {
    let name = unique_name("killed");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
    let mut peer = start_peer("hang", &name);
    let mut conn = listener.accept(PEER_START).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"ready");
    peer.kill().unwrap();
    assert_eq!(conn.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(conn.send(b"anyone?", None), Err(Error::Disconnected));
    let _ = peer.wait();
}

#[test]
fn an_rpc_server_peer() {
    let name = unique_name("rpc_server");
    let mut peer = start_peer("rpc_server", &name);
    let mut conn = Connection::connect(&name, PEER_START).unwrap();
    let ids: Vec<u64> = (0..10u32).map(|i| conn.rpc_submit(i, &pattern(i as usize * 1000), None).unwrap()).collect();
    assert_eq!(ids, (1..=10).collect::<Vec<u64>>());
    for (i, id) in ids.into_iter().enumerate() {
        let response = conn.rpc_receive(TEN_SECONDS).unwrap();
        let expected: Vec<u8> = pattern(i * 1000).into_iter().rev().collect();
        assert_eq!((response.id, response.kind, response.opcode), (id, RpcKind::Response, i as u32));
        assert_eq!(response.status, expected.len() as i32);
        assert_eq!(response.payload, expected);
    }
    drop(conn);
    assert!(wait_for(&mut peer).success());
}
