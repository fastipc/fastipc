//! What the integration tests share: unique names, test data, a connected pair, the repository.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fipc::{Connection, Listener};

pub const RING: usize = 1 << 20;
pub const TEN_SECONDS: Duration = Duration::from_secs(10);

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A name no other test (or run) uses.
pub fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    format!(
        "fipc_rust_{prefix}_{}_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        nanos % 1_000_000
    )
}

/// `len` bytes of a pattern that differs at every offset of a ring's frame.
pub fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(31) + 7) as u8).collect()
}

/// A listener and a server and client connection on one name. The client connects on another thread: a connect waits
/// for the listener's accept, so a client and its server in one process run on different threads.
pub struct Pair {
    pub listener: Listener,
    pub client: Connection,
    pub server: Connection,
}

impl Pair {
    pub fn new(prefix: &str) -> Pair {
        Pair::with_ring(prefix, RING)
    }

    pub fn with_ring(prefix: &str, ring: usize) -> Pair {
        let name = unique_name(prefix);
        let mut listener = Listener::listen(&name, ring).expect("listen");
        let connecting = connect_aside(&name);
        let server = listener.accept(TEN_SECONDS).expect("accept");
        let client = connecting.join().unwrap().expect("connect");
        Pair { listener, client, server }
    }
}

/// Connects to `name` on a thread of its own, within ten seconds, while the caller accepts.
pub fn connect_aside(name: &str) -> std::thread::JoinHandle<fipc::Result<Connection>> {
    let name = name.to_owned();
    std::thread::spawn(move || Connection::connect(&name, TEN_SECONDS))
}

/// The repository's root (this crate is bindings/rust/fipc).
pub fn repository() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("..").join("..")
}
