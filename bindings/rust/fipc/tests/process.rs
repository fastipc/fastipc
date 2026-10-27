//! Peers in other processes: this test binary run again as a peer (the test `peer`, which does nothing unless the
//! environment names a part for it), echoing, exiting, killed, serving, publishing on a broadcast or subscribing
//! to one, owning a work queue as its producer or consuming from one, owning a service as its server or client,
//! joining one as a server or a client, or holding a request until it is killed.

mod common;

use std::env;
use std::io::{BufRead, BufReader};
use std::process::{self, Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::{
    BROADCAST_STEP, QUEUE_CAPACITY, QUEUE_JOBS, QUEUE_MEMBERS, TEN_SECONDS, answer_until_the_end, check_jobs,
    check_subscription, join_and_call, own_and_call, pattern, publish_messages, receive_until_the_end, send_jobs,
    serve, unique_name,
};
use fipc::{Error, QueueRole, ServiceRole, SubscriberRole};

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
            let mut conn = fipc::stream::connect(name, PEER_START).unwrap();
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
            let mut conn = fipc::stream::connect(name, PEER_START).unwrap();
            conn.send(b"bye", None).unwrap();
            process::exit(0);
        }
        // Says it is ready, then waits to be killed
        "hang" => {
            let mut conn = fipc::stream::connect(name, PEER_START).unwrap();
            conn.send(b"ready", None).unwrap();
            thread::sleep(Duration::from_secs(600));
        }
        // A server: answers each message with it reversed, until the end
        "reverse_server" => {
            let mut listener = fipc::stream::create(name, 1 << 16).unwrap();
            let mut conn = listener.accept(PEER_START).unwrap();
            loop {
                match conn.receive(None) {
                    Ok(message) => {
                        let reply: Vec<u8> = message.into_iter().rev().collect();
                        conn.send(&reply, None).unwrap();
                    }
                    Err(Error::Disconnected) => break,
                    Err(e) => panic!("reverse_server: {e}"),
                }
            }
        }
        // A broadcast's publisher: sends the broadcast's messages once two subscribers joined, then ends it
        "publisher" => {
            let mut publisher = fipc::broadcast::create(name, 4096, 8, BROADCAST_STEP).unwrap();
            publish_messages(&mut publisher, 2, PEER_START);
        }
        // A broadcast's subscriber: checks the messages it receives until the broadcast's end
        "gated" | "lossy" => {
            let role = if mode == "gated" { SubscriberRole::Gated } else { SubscriberRole::Lossy };
            let mut subscriber = fipc::broadcast::join(name, role, PEER_START).unwrap();
            check_subscription(&mut subscriber, role == SubscriberRole::Gated);
        }
        "queue_producer" => own_and_produce(name),
        // A work queue's consumer: checks the jobs it receives until the queue's end
        "queue_consumer" => {
            let mut consumer = fipc::queue::join(name, QueueRole::Consumer, PEER_START).unwrap();
            check_jobs(&receive_until_the_end(&mut consumer), 1, QUEUE_JOBS);
        }
        // A service's owner and server: answers SERVICE_CALLS requests of each of two clients, then ends the service
        "service_server" => serve(name, SERVICE_CALLS, 2, PEER_START),
        // A service's client: calls SERVICE_CALLS times and checks each reply
        "service_client" => join_and_call(name, SERVICE_CALLS, PEER_START),
        // A service's server: answers requests until the service's end
        "joined_server" => {
            let mut server = fipc::service::join(name, ServiceRole::Server, PEER_START).unwrap();
            answer_until_the_end(&mut server);
        }
        // A service's server: takes one request, says so on stdout and waits, never replying, until it is killed
        "holding_server" => {
            let mut server = fipc::service::join(name, ServiceRole::Server, PEER_START).unwrap();
            let _request = server.receive_request(PEER_START).unwrap();
            println!("holding");
            thread::sleep(Duration::from_secs(600));
        }
        _ => panic!("unknown peer mode {mode}"),
    }
}

/// How many requests a service's client peer calls.
const SERVICE_CALLS: u64 = 300;

/// A work queue's owner and producer, as the examples' producer: creates the queue, joins it as its producer, sends
/// the jobs, waits for a consumer, then ends the queue (the consumer receives what is left, then Disconnected).
fn own_and_produce(name: &str) {
    let queue = fipc::queue::create(name, QUEUE_CAPACITY, QUEUE_MEMBERS, QUEUE_MEMBERS).unwrap();
    let mut producer = fipc::queue::join(name, QueueRole::Producer, TEN_SECONDS).unwrap();
    send_jobs(&mut producer, QUEUE_JOBS);
    assert_eq!(queue.wait_for_members(QueueRole::Consumer, 1, PEER_START), Ok(1));
    drop(producer);
    queue.close();
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

/// This test binary as a peer of `mode` on `name`, its stdout piped.
fn start_peer_piped(mode: &str, name: &str) -> Child {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", "peer", "--nocapture", "--test-threads=1", "--quiet"])
        .env(PEER, format!("{mode}:{name}"))
        .stdout(Stdio::piped())
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
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
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
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
    let mut peer = start_peer("exit", &name);
    let mut conn = listener.accept(PEER_START).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"bye");
    assert_eq!(conn.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert!(wait_for(&mut peer).success());
}

#[test]
fn a_killed_peer() {
    let name = unique_name("killed");
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
    let mut peer = start_peer("hang", &name);
    let mut conn = listener.accept(PEER_START).unwrap();
    assert_eq!(conn.receive(TEN_SECONDS).unwrap(), b"ready");
    peer.kill().unwrap();
    assert_eq!(conn.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(conn.send(b"anyone?", None), Err(Error::Disconnected));
    let _ = peer.wait();
}

/// A server in another process; this client's messages pipelined, the answers in order.
#[test]
fn a_server_peer() {
    let name = unique_name("reverse_server");
    let mut peer = start_peer("reverse_server", &name);
    let mut conn = fipc::stream::connect(&name, PEER_START).unwrap();
    for i in 1..=10 {
        conn.send(&pattern(i * 1000), None).unwrap();
    }
    for i in 1..=10 {
        let expected: Vec<u8> = pattern(i * 1000).into_iter().rev().collect();
        assert_eq!(conn.receive(TEN_SECONDS).unwrap(), expected);
    }
    drop(conn);
    assert!(wait_for(&mut peer).success());
}

/// A publisher in another process; a gated and a lossy subscriber here, each on a thread of its own.
#[test]
fn a_publisher_peer() {
    let name = unique_name("bc_publisher");
    let mut peer = start_peer("publisher", &name);
    let mut gated = fipc::broadcast::join(&name, SubscriberRole::Gated, PEER_START).unwrap();
    let mut lossy = fipc::broadcast::join(&name, SubscriberRole::Lossy, PEER_START).unwrap();
    thread::scope(|s| {
        s.spawn(|| check_subscription(&mut lossy, false));
        check_subscription(&mut gated, true);
    });
    assert!(wait_for(&mut peer).success());
}

/// A publisher here; a gated and a lossy subscriber in other processes.
#[test]
fn subscriber_peers() {
    let name = unique_name("bc_subscribers");
    let mut publisher = fipc::broadcast::create(&name, 4096, 8, BROADCAST_STEP).unwrap();
    let mut peers = [start_peer("gated", &name), start_peer("lossy", &name)];
    publish_messages(&mut publisher, 2, PEER_START);
    drop(publisher); // ends the broadcast
    for peer in &mut peers {
        assert!(wait_for(peer).success());
    }
}

/// A work queue's owner and producer in another process; its consumer here.
#[test]
fn a_queue_producer_peer() {
    let name = unique_name("q_producer");
    let mut peer = start_peer("queue_producer", &name);
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, PEER_START).unwrap();
    check_jobs(&receive_until_the_end(&mut consumer), 1, QUEUE_JOBS);
    assert!(wait_for(&mut peer).success());
}

/// A work queue's owner and producer here; its consumer in another process.
#[test]
fn a_queue_consumer_peer() {
    let name = unique_name("q_consumer");
    let mut peer = start_peer("queue_consumer", &name); // joins within its timeout, before or after the queue exists
    own_and_produce(&name);
    assert!(wait_for(&mut peer).success());
}

/// A service's owner and server in another process; two clients here, each on a thread of its own.
#[test]
fn a_service_server_peer() {
    let name = unique_name("s_server");
    let mut peer = start_peer("service_server", &name);
    thread::scope(|s| {
        for _ in 0..2 {
            s.spawn(|| join_and_call(&name, SERVICE_CALLS, PEER_START));
        }
    });
    assert!(wait_for(&mut peer).success());
}

/// A service's owner and server here; two clients in other processes.
#[test]
fn service_client_peers() {
    let name = unique_name("s_clients");
    let mut peers = [start_peer("service_client", &name), start_peer("service_client", &name)];
    serve(&name, SERVICE_CALLS, 2, PEER_START);
    for peer in &mut peers {
        assert!(wait_for(peer).success());
    }
}

/// A service's owner and client here; two servers in other processes join it and answer until its end.
#[test]
fn joined_server_peers() {
    let name = unique_name("s_joined");
    let mut peers = [start_peer("joined_server", &name), start_peer("joined_server", &name)];
    own_and_call(&name, SERVICE_CALLS, 2, PEER_START);
    for peer in &mut peers {
        assert!(wait_for(peer).success());
    }
}

/// A server killed while it holds a request: its client's receive returns Abandoned once the owner's process has seen
/// its end, and the next call is answered by another server.
#[test]
fn a_server_peer_killed_holding_a_request_abandons_it() {
    let name = unique_name("s_killed");
    let _service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut peer = start_peer_piped("holding_server", &name);
    client.send(b"doomed", TEN_SECONDS).unwrap();
    let mut lines = BufReader::new(peer.stdout.take().unwrap()).lines();
    assert!(lines.any(|line| line.unwrap() == "holding"), "the peer didn't take the request");
    peer.kill().unwrap();
    let _ = peer.wait();
    assert_eq!(client.receive(PEER_START), Err(Error::Abandoned));
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    thread::scope(|s| {
        s.spawn(|| {
            let request = server.receive(TEN_SECONDS).unwrap();
            server.send(&request.to_ascii_uppercase(), None).unwrap();
        });
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT");
    });
}
