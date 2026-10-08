//! Cross-language: this binding against the Python binding (bindings/python, `fipc`) in another process, both
//! ways, with plain messages and RPC. The interpreter is `FIPC_TEST_PYTHON`, else the repository's venv, else
//! `python`; it needs cffi. Without one the tests say so and pass, unless `FIPC_REQUIRE_INTEROP` is set.

mod common;

use std::env;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::{TEN_SECONDS, pattern, repository, unique_name};
use fipc::{Connection, Error, Listener, NO_WAIT, RpcKind};

const PEER_START: Duration = Duration::from_secs(30);

/// The Python peer: `peer.py client|server <name> <bindings/python>`.
const PYTHON_PEER: &str = r#"
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A plain connection: echo each message reversed until the server's end
    with Conn.connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
    # An RPC connection: call the Rust server, then answer its call
    with Conn.connect(name, timeout_ms=20000) as conn:
        request_id = conn.rpc_submit(7, "hello from Python".encode(), timeout_ms=20000)
        reply = conn.rpc_recv(timeout_ms=20000)
        assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
        request = conn.rpc_recv(timeout_ms=20000)
        assert request.kind == RPC_REQUEST and request.opcode == 9, request
        conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2, timeout_ms=20000)
        try:
            conn.rpc_recv(timeout_ms=20000)
            raise AssertionError("expected the server's end")
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                request = conn.rpc_recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(), timeout_ms=20000)
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

/// A Rust server and a Python client: plain messages, then RPC in both directions.
#[test]
fn rust_server_python_client() {
    let Some(python) = python() else { return };
    let name = unique_name("py_client");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
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
    let request = conn.rpc_receive(TEN_SECONDS).unwrap();
    assert_eq!((request.kind, request.opcode), (RpcKind::Request, 7));
    assert_eq!(request.payload, b"hello from Python");
    conn.rpc_respond(request.id, 7, 42, b"HELLO FROM PYTHON", TEN_SECONDS).unwrap();

    let id = conn.rpc_submit(9, b"Rust", TEN_SECONDS).unwrap();
    let reply = conn.rpc_receive(TEN_SECONDS).unwrap();
    assert_eq!((reply.kind, reply.id, reply.status), (RpcKind::Response, id, 4));
    assert_eq!(reply.payload, b"RustRust");
    drop(conn);

    assert!(peer.wait().unwrap().success(), "the Python peer failed");
}

/// A Python server and a Rust client over RPC; the Rust client's end ends the Python server.
#[test]
fn python_server_rust_client() {
    let Some(python) = python() else { return };
    let name = unique_name("py_server");
    let mut peer = start_python(&python, "server", &name);

    let mut conn = Connection::connect(&name, PEER_START).unwrap();
    let first = conn.rpc_submit(1, b"shared memory", TEN_SECONDS).unwrap();
    let (tx, rx) = conn.split();
    let big = pattern(300_000);
    let (second, two) = std::thread::scope(|s| {
        let sender = s.spawn(|| tx.rpc_submit(2, &big, TEN_SECONDS).unwrap());
        let one = rx.rpc_receive(TEN_SECONDS).unwrap();
        assert_eq!((one.id, one.status), (first, -1));
        assert_eq!(one.payload, b"SHARED MEMORY");
        let two = rx.rpc_receive(TEN_SECONDS).unwrap();
        (sender.join().unwrap(), two)
    });
    assert_eq!(two.id, second);
    assert_eq!(two.payload, big.to_ascii_uppercase());
    assert_eq!(conn.rpc_receive(NO_WAIT), Err(Error::Timeout));
    drop(conn);

    assert!(peer.wait().unwrap().success(), "the Python peer failed");
}
