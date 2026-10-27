//! What the integration tests share: unique names, test data, a connected pair, a broadcast's messages, a work
//! queue's jobs and a service's calls, the repository.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fipc::{Connection, Error, Listener, ServiceRole};

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

/// A stream's listener and a server and client connection on one name. The client connects on another thread: a connect waits
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
        let mut listener = fipc::stream::create(&name, ring).expect("create");
        let connecting = connect_aside(&name);
        let server = listener.accept(TEN_SECONDS).expect("accept");
        let client = connecting.join().unwrap().expect("connect");
        Pair { listener, client, server }
    }
}

/// Connects to `name` on a thread of its own, within ten seconds, while the caller accepts.
pub fn connect_aside(name: &str) -> std::thread::JoinHandle<fipc::Result<Connection>> {
    let name = name.to_owned();
    std::thread::spawn(move || fipc::stream::connect(&name, TEN_SECONDS))
}

/// The repository's root (this crate is bindings/rust/fipc).
pub fn repository() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("..").join("..")
}

/// How many messages a broadcast's publisher sends in the tests across processes, and how long each step may wait (as
/// bindings/python/tests/broadcast_peer.py, whose messages these are).
pub const BROADCAST_MESSAGES: u64 = 100;
pub const BROADCAST_STEP: Duration = Duration::from_secs(20);

/// Message `i` of a broadcast's publisher: `message <i>`.
pub fn broadcast_message(i: u64) -> Vec<u8> {
    format!("message {i}").into_bytes()
}

/// Waits for `subscribers` to join, then sends the broadcast's messages.
pub fn publish_messages(publisher: &mut Connection, subscribers: u32, wait: Duration) {
    let joined = publisher.wait_for_members(subscribers, wait).expect("the subscribers didn't join");
    assert!(joined >= subscribers, "{joined} subscribers joined");
    for i in 0..BROADCAST_MESSAGES {
        publisher.send(&broadcast_message(i), BROADCAST_STEP).unwrap();
    }
}

/// Receives the broadcast's messages until its end. A gated subscriber gets every one in order; a lossy one gets each
/// after the last, or after as many as an overrun between them reported lost, and received and lost add up.
pub fn check_subscription(subscriber: &mut Connection, gated: bool) {
    let mut expected = 0; // the index of the next message
    let mut lost = 0;
    loop {
        match subscriber.receive(BROADCAST_STEP) {
            Ok(message) => {
                assert_eq!(message, broadcast_message(expected), "gated: {gated}");
                expected += 1;
            }
            Err(Error::Overrun { lost: count }) if !gated => {
                assert!(count >= 1);
                lost += count;
                expected += count;
            }
            Err(Error::Disconnected) => break,
            Err(e) => panic!("gated: {gated}: {e}"),
        }
    }
    assert_eq!(expected, BROADCAST_MESSAGES, "gated: {gated}: received {} and lost {lost}", expected - lost);
}

/// How many jobs a work queue's producer sends in the tests across processes, and how long each step may wait (as
/// bindings/python/tests/queue_peer.py, whose jobs these are, and whose queue has rings of 4096 bytes and room for 8
/// producers and 8 consumers).
pub const QUEUE_JOBS: u64 = 100;
pub const QUEUE_STEP: Duration = Duration::from_secs(20);
pub const QUEUE_CAPACITY: usize = 4096;
pub const QUEUE_MEMBERS: u32 = 8;

/// A producer's job `i`: `job <i>`.
pub fn job(i: u64) -> Vec<u8> {
    format!("job {i}").into_bytes()
}

/// Sends job 1 to job `count`.
pub fn send_jobs(producer: &mut Connection, count: u64) {
    for i in 1..=count {
        producer.send(&job(i), QUEUE_STEP).unwrap();
    }
}

/// Receives until the queue's end (`Error::Disconnected`).
pub fn receive_until_the_end(consumer: &mut Connection) -> Vec<Vec<u8>> {
    let mut received = Vec::new();
    loop {
        match consumer.receive(QUEUE_STEP) {
            Ok(message) => received.push(message),
            Err(Error::Disconnected) => return received,
            Err(e) => panic!("consumer: {e}"),
        }
    }
}

/// That `received` is the jobs of `producers` producers, each sending job 1 to job `count`, in any order (the queue
/// promises none): each job exactly `producers` times.
pub fn check_jobs(received: &[Vec<u8>], producers: usize, count: u64) {
    let mut times: HashMap<&[u8], usize> = HashMap::new();
    for message in received {
        *times.entry(message.as_slice()).or_default() += 1;
    }
    let jobs: Vec<Vec<u8>> = (1..=count).map(job).collect();
    for job in &jobs {
        let n = times.remove(job.as_slice()).unwrap_or(0);
        assert_eq!(n, producers, "{} received {n} times", String::from_utf8_lossy(job));
    }
    let others: Vec<_> = times.keys().map(|m| String::from_utf8_lossy(m)).collect();
    assert!(others.is_empty(), "received messages that aren't jobs: {others:?}");
}

/// How a service's peers across processes are set up, and how long each step may wait (as
/// bindings/python/tests/service_peer.py, whose service has rings of 4096 bytes, requests and replies of up to 256
/// bytes, and room for 8 clients and 8 servers).
pub const SERVICE_CAPACITY: usize = 4096;
pub const SERVICE_MAX_MESSAGE: usize = 256;
pub const SERVICE_MEMBERS: u32 = 8;
pub const SERVICE_STEP: Duration = Duration::from_secs(20);

/// A client's request `i`: `ping <i>`; its reply is its uppercase.
pub fn request(i: u64) -> Vec<u8> {
    format!("ping {i}").into_bytes()
}

/// Calls `count` times, request 1 to request `count`, and checks that each reply is its request's uppercase.
pub fn call(client: &mut Connection, count: u64) {
    for i in 1..=count {
        let reply = client.call(&request(i), SERVICE_STEP).unwrap();
        assert_eq!(reply, request(i).to_ascii_uppercase(), "the reply to request {i}");
    }
}

/// Answers `count` requests, each with its uppercase.
pub fn answer(server: &mut Connection, count: u64) {
    for _ in 0..count {
        let request = server.receive(SERVICE_STEP).unwrap();
        server.send(&request.to_ascii_uppercase(), None).unwrap();
    }
}

/// Answers requests with their uppercase until the service ends, and returns how many it answered.
pub fn answer_until_the_end(server: &mut Connection) -> u64 {
    let mut answered = 0;
    loop {
        let request = match server.receive(SERVICE_STEP) {
            Ok(request) => request,
            Err(Error::Disconnected) => return answered,
            Err(e) => panic!("server: {e}"),
        };
        match server.send(&request.to_ascii_uppercase(), None) {
            Ok(()) => answered += 1,
            Err(Error::Disconnected) => return answered, // the service ended between the request and its reply
            Err(e) => panic!("server: {e}"),
        }
    }
}

/// The service's owner and server, as the examples' server: creates the service on `name`, joins it as a server,
/// answers `count` requests of each of `clients` clients, then ends the service.
pub fn serve(name: &str, count: u64, clients: u64, wait: Duration) {
    let service =
        fipc::service::create(name, SERVICE_CAPACITY, SERVICE_MAX_MESSAGE, SERVICE_MEMBERS, SERVICE_MEMBERS).unwrap();
    let mut server = fipc::service::join(name, ServiceRole::Server, wait).unwrap();
    answer(&mut server, count * clients);
    service.close();
}

/// A client: joins `name` within `wait`, calls `count` times and checks each reply.
pub fn join_and_call(name: &str, count: u64, wait: Duration) {
    let mut client = fipc::service::join(name, ServiceRole::Client, wait).unwrap();
    call(&mut client, count);
}

/// The service's owner and client: creates the service on `name`, joins it as a client, waits for `servers` servers,
/// calls `count` times and checks each reply, then ends the service.
pub fn own_and_call(name: &str, count: u64, servers: u32, wait: Duration) {
    let service =
        fipc::service::create(name, SERVICE_CAPACITY, SERVICE_MAX_MESSAGE, SERVICE_MEMBERS, SERVICE_MEMBERS).unwrap();
    let mut client = fipc::service::join(name, ServiceRole::Client, wait).unwrap();
    let joined = service.wait_for_members(ServiceRole::Server, servers, wait).expect("the servers didn't join");
    assert!(joined >= servers, "{joined} servers joined");
    call(&mut client, count);
    service.close();
}
