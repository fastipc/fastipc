//! Cross-language: this binding against the Python binding (bindings/python, `fipc`) in another process, both ways, on
//! a stream, a broadcast, a work queue and a service (the Python binding's tests/broadcast_peer.py, tests/queue_peer.py
//! and tests/service_peer.py). The
//! interpreter is `FIPC_TEST_PYTHON`, else the repository's venv, else `python`; it needs cffi. Without one the tests
//! say so and pass, unless `FIPC_REQUIRE_INTEROP` is set.

mod common;

use std::env;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::{
    BROADCAST_MESSAGES, BROADCAST_STEP, QUEUE_CAPACITY, QUEUE_JOBS, QUEUE_MEMBERS, QUEUE_STEP, TEN_SECONDS,
    answer_until_the_end, check_jobs, check_subscription, join_and_call, own_and_call, pattern, publish_messages,
    receive_until_the_end, repository, send_jobs, serve, unique_name,
};
use fipc::{Connection, Error, NO_WAIT, QueueRole, ServiceRole, SubscriberRole};

const PEER_START: Duration = Duration::from_secs(30);

/// The Python peer: `peer.py client|server <name> <bindings/python>`.
const PYTHON_PEER: &str = r#"
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import FipcError, Result, stream_connect, stream_create

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A stream's client: echo each message reversed until the server's end
    with stream_connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
    # A second connection: ask the Rust server, then answer its question
    with stream_connect(name, timeout_ms=20000) as conn:
        conn.send("hello from Python".encode(), timeout_ms=20000)
        reply = conn.recv(timeout_ms=20000)
        assert reply == b"HELLO FROM PYTHON", reply
        question = conn.recv(timeout_ms=20000)
        conn.send(question * 2, timeout_ms=20000)
        try:
            conn.recv(timeout_ms=20000)
            raise AssertionError("expected the server's end")
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                message = conn.recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.send(message.upper(), timeout_ms=20000)
"#;

/// A Python interpreter with cffi, or None (the test is then skipped, unless FIPC_REQUIRE_INTEROP is set).
fn python() -> Option<PathBuf> {
    let venv = repository().join("venv");
    let candidates = [
        env::var_os("FIPC_TEST_PYTHON").map(PathBuf::from),
        Some(venv.join(if cfg!(windows) { "Scripts/python.exe" } else { "bin/python" })),
        Some(PathBuf::from(if cfg!(windows) { "python" } else { "python3" })),
    ];
    for python in candidates.into_iter().flatten() {
        let probe = Command::new(&python).args(["-c", "import cffi"]).stderr(Stdio::null()).status();
        if probe.is_ok_and(|s| s.success()) {
            return Some(python);
        }
    }
    assert!(env::var_os("FIPC_REQUIRE_INTEROP").is_none(), "no Python with cffi (set FIPC_TEST_PYTHON)");
    eprintln!("interop: skipped, no Python with cffi (set FIPC_TEST_PYTHON)");
    None
}

fn start_python(python: &PathBuf, mode: &str, name: &str) -> Child {
    let dir = env::temp_dir().join(format!("{name}_peer"));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("peer.py");
    std::fs::write(&script, PYTHON_PEER).unwrap();
    let bindings = repository().join("bindings").join("python");
    Command::new(python).arg(&script).args([mode, name]).arg(bindings).spawn().unwrap()
}

/// A Rust server and a Python client: echoed messages, then a second connection that asks in both directions.
#[test]
fn rust_server_python_client() {
    let Some(python) = python() else { return };
    let name = unique_name("py_client");
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
    let mut peer = start_python(&python, "client", &name);

    let mut conn = listener.accept(PEER_START).unwrap();
    for size in [1, 100, 70_000, 200_000] {
        let message = pattern(size);
        let (tx, rx) = conn.split();
        let reversed = std::thread::scope(|s| {
            s.spawn(|| tx.send(&message, TEN_SECONDS).unwrap()); // longer than the ring: send while receiving
            rx.receive(TEN_SECONDS).unwrap()
        });
        assert_eq!(reversed, message.iter().rev().copied().collect::<Vec<u8>>(), "size {size}");
    }
    drop(conn);

    let mut conn = listener.accept(PEER_START).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"hello from Python");
    conn.send(b"HELLO FROM PYTHON", TEN_SECONDS).unwrap();

    conn.send(b"Rust", TEN_SECONDS).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"RustRust");
    drop(conn);

    assert!(peer.wait().unwrap().success(), "the Python peer failed");
}

/// A Python server that answers each message in upper case and a Rust client, its messages pipelined, one longer than
/// the ring; the Rust client's end ends the Python server.
#[test]
fn python_server_rust_client() {
    let Some(python) = python() else { return };
    let name = unique_name("py_server");
    let mut peer = start_python(&python, "server", &name);

    let mut conn = fipc::stream::connect(&name, PEER_START).unwrap();
    conn.send(b"shared memory", TEN_SECONDS).unwrap();
    let (tx, rx) = conn.split();
    let big = pattern(300_000);
    let two = std::thread::scope(|s| {
        s.spawn(|| tx.send(&big, TEN_SECONDS).unwrap());
        assert_eq!(rx.receive(TEN_SECONDS).unwrap(), b"SHARED MEMORY");
        rx.receive(TEN_SECONDS).unwrap()
    });
    assert_eq!(two, big.to_ascii_uppercase());
    assert_eq!(conn.receive(NO_WAIT), Err(Error::Timeout));
    drop(conn);

    assert!(peer.wait().unwrap().success(), "the Python peer failed");
}

/// bindings/python/tests/broadcast_peer.py with `args`.
fn start_broadcast_peer(python: &PathBuf, args: &[&str]) -> Child {
    let script = repository().join("bindings").join("python").join("tests").join("broadcast_peer.py");
    Command::new(python).arg(script).args(args).spawn().unwrap()
}

/// A Python publisher; a gated and a lossy Rust subscriber, each on a thread of its own.
#[test]
fn python_publisher_rust_subscribers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_publisher");
    let count = BROADCAST_MESSAGES.to_string();
    let mut peer = start_broadcast_peer(&python, &["publisher", &name, &count, "2"]);
    let mut gated = fipc::broadcast::join(&name, SubscriberRole::Gated, PEER_START).unwrap();
    let mut lossy = fipc::broadcast::join(&name, SubscriberRole::Lossy, PEER_START).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| check_subscription(&mut lossy, false));
        check_subscription(&mut gated, true);
    });
    assert!(peer.wait().unwrap().success(), "the Python publisher failed");
}

/// A Rust publisher; a gated and a lossy Python subscriber.
#[test]
fn rust_publisher_python_subscribers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_subscribers");
    let mut publisher = fipc::broadcast::create(&name, 4096, 8, BROADCAST_STEP).unwrap();
    let count = BROADCAST_MESSAGES.to_string();
    let mut peers = [
        start_broadcast_peer(&python, &["subscriber", &name, "gated", &count]),
        start_broadcast_peer(&python, &["subscriber", &name, "lossy", &count]),
    ];
    publish_messages(&mut publisher, 2, PEER_START);
    drop(publisher); // ends the broadcast
    for peer in &mut peers {
        assert!(peer.wait().unwrap().success(), "a Python subscriber failed");
    }
}

/// bindings/python/tests/queue_peer.py with `args`.
fn start_queue_peer(python: &PathBuf, args: &[&str]) -> Child {
    let script = repository().join("bindings").join("python").join("tests").join("queue_peer.py");
    Command::new(python).arg(script).args(args).spawn().unwrap()
}

/// A Python owner and producer; two Rust consumers, each on a thread of its own, share its jobs.
#[test]
fn python_producer_rust_consumers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_q_producer");
    let count = QUEUE_JOBS.to_string();
    let mut peer = start_queue_peer(&python, &["producer", &name, &count, "2"]);
    let consumers: Vec<Connection> =
        (0..2).map(|_| fipc::queue::join(&name, QueueRole::Consumer, PEER_START).unwrap()).collect();
    let received: Vec<Vec<Vec<u8>>> = std::thread::scope(|s| {
        let threads: Vec<_> =
            consumers.into_iter().map(|mut consumer| s.spawn(move || receive_until_the_end(&mut consumer))).collect();
        threads.into_iter().map(|thread| thread.join().unwrap()).collect()
    });
    check_jobs(&received.concat(), 1, QUEUE_JOBS);
    assert!(peer.wait().unwrap().success(), "the Python producer failed");
}

/// A Rust owner and producer, as the examples' producer: sends the jobs, waits for the Python consumer, which checks
/// them, and ends the queue.
#[test]
fn rust_producer_python_consumer() {
    let Some(python) = python() else { return };
    let name = unique_name("py_q_consumer");
    let queue = fipc::queue::create(&name, QUEUE_CAPACITY, QUEUE_MEMBERS, QUEUE_MEMBERS).unwrap();
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    let mut peer = start_queue_peer(&python, &["consumer", &name, &QUEUE_JOBS.to_string()]);
    send_jobs(&mut producer, QUEUE_JOBS);
    assert_eq!(queue.wait_for_members(QueueRole::Consumer, 1, PEER_START), Ok(1));
    drop(producer);
    queue.close(); // ends the queue: the consumer receives what is left, then Disconnected
    assert!(peer.wait().unwrap().success(), "the Python consumer failed");
}

/// A Rust owner and consumer; two Python producers join.
#[test]
fn rust_consumer_python_producers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_q_producers");
    let queue = fipc::queue::create(&name, QUEUE_CAPACITY, QUEUE_MEMBERS, QUEUE_MEMBERS).unwrap();
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap();
    let count = QUEUE_JOBS.to_string();
    let mut peers = [
        start_queue_peer(&python, &["joined-producer", &name, &count]),
        start_queue_peer(&python, &["joined-producer", &name, &count]),
    ];
    let received: Vec<Vec<u8>> = (0..2 * QUEUE_JOBS).map(|_| consumer.receive(QUEUE_STEP).unwrap()).collect();
    check_jobs(&received, 2, QUEUE_JOBS);
    for peer in &mut peers {
        assert!(peer.wait().unwrap().success(), "a Python producer failed");
    }
    assert_eq!(consumer.receive(NO_WAIT), Err(Error::Timeout)); // the producers left; the queue goes on
    assert_eq!(queue.wait_for_members(QueueRole::Consumer, 0, NO_WAIT), Ok(1));
}

/// A Python owner and consumer; two Rust producers join, each on a thread of its own.
#[test]
fn python_consumer_rust_producers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_q_consumer_owner");
    let count = QUEUE_JOBS.to_string();
    let mut peer = start_queue_peer(&python, &["owner-consumer", &name, &count, "2"]);
    std::thread::scope(|s| {
        for _ in 0..2 {
            s.spawn(|| {
                let mut producer = fipc::queue::join(&name, QueueRole::Producer, PEER_START).unwrap();
                send_jobs(&mut producer, QUEUE_JOBS);
            });
        }
    });
    assert!(peer.wait().unwrap().success(), "the Python consumer failed");
}

/// How many requests each client calls in the service's tests.
const SERVICE_CALLS: u64 = 300;

/// bindings/python/tests/service_peer.py with `args`.
fn service_peer(python: &PathBuf, args: &[&str]) -> Command {
    let script = repository().join("bindings").join("python").join("tests").join("service_peer.py");
    let mut command = Command::new(python);
    command.arg(script).args(args);
    command
}

fn start_service_peer(python: &PathBuf, args: &[&str]) -> Child {
    service_peer(python, args).spawn().unwrap()
}

/// A Python owner and server; two Rust clients, each on a thread of its own, call it.
#[test]
fn python_service_server_rust_clients() {
    let Some(python) = python() else { return };
    let name = unique_name("py_s_server");
    let calls = SERVICE_CALLS.to_string();
    let mut peer = start_service_peer(&python, &["server", &name, &calls, "2"]);
    std::thread::scope(|s| {
        for _ in 0..2 {
            s.spawn(|| join_and_call(&name, SERVICE_CALLS, PEER_START));
        }
    });
    assert!(peer.wait().unwrap().success(), "the Python server failed");
}

/// A Rust owner and server, as the examples' server; two Python clients call it.
#[test]
fn rust_service_server_python_clients() {
    let Some(python) = python() else { return };
    let name = unique_name("py_s_clients");
    let calls = SERVICE_CALLS.to_string();
    let mut peers = [
        start_service_peer(&python, &["client", &name, &calls]),
        start_service_peer(&python, &["client", &name, &calls]),
    ];
    serve(&name, SERVICE_CALLS, 2, PEER_START);
    for peer in &mut peers {
        assert!(peer.wait().unwrap().success(), "a Python client failed");
    }
}

/// A Python owner and client; two Rust servers join it and answer until its end.
#[test]
fn python_service_client_rust_servers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_s_owner_client");
    let mut peer = start_service_peer(&python, &["owner-client", &name, &SERVICE_CALLS.to_string(), "2"]);
    let answered: u64 = std::thread::scope(|s| {
        let servers: Vec<_> = (0..2)
            .map(|_| {
                s.spawn(|| {
                    let mut server = fipc::service::join(&name, ServiceRole::Server, PEER_START).unwrap();
                    answer_until_the_end(&mut server)
                })
            })
            .collect();
        servers.into_iter().map(|server| server.join().unwrap()).sum()
    });
    assert_eq!(answered, SERVICE_CALLS);
    assert!(peer.wait().unwrap().success(), "the Python client failed");
}

/// A Rust owner and client; two Python servers join it and answer until its end.
#[test]
fn rust_service_client_python_servers() {
    let Some(python) = python() else { return };
    let name = unique_name("py_s_joined");
    let mut peers = [
        start_service_peer(&python, &["joined-server", &name]),
        start_service_peer(&python, &["joined-server", &name]),
    ];
    own_and_call(&name, SERVICE_CALLS, 2, PEER_START);
    for peer in &mut peers {
        assert!(peer.wait().unwrap().success(), "a Python server failed");
    }
}

/// A Python server killed while it holds a Rust client's request: the request is answered Abandoned, and the next
/// call is answered by a Rust server.
#[test]
fn a_python_server_killed_holding_a_request_abandons_it() {
    let Some(python) = python() else { return };
    let name = unique_name("py_s_killed");
    let _service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut peer = service_peer(&python, &["holding-server", &name]).stdout(Stdio::piped()).spawn().unwrap();
    client.send(b"doomed", TEN_SECONDS).unwrap();
    let mut lines = BufReader::new(peer.stdout.take().unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), "holding");
    peer.kill().unwrap();
    let _ = peer.wait();
    assert_eq!(client.receive(PEER_START), Err(Error::Abandoned)); // once the owner's process has seen its end
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            let request = server.receive(TEN_SECONDS).unwrap();
            server.send(&request.to_ascii_uppercase(), None).unwrap();
        });
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT");
    });
}
