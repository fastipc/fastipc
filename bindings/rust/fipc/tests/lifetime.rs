//! Threads, cancels and drops: what ends a call, a connection and a listener.

mod common;

use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::{Pair, TEN_SECONDS, connect_aside, unique_name};
use fipc::{Connection, Error, Listener, NO_WAIT};

#[test]
fn a_canceler_stops_a_receive_on_another_thread() {
    let Pair { listener: _listener, mut client, server } = Pair::new("cancel_receive");
    let canceler = server.canceler();
    let reader = thread::spawn(move || {
        let mut server = server;
        (server.receive(None), server)
    });
    thread::sleep(Duration::from_millis(50)); // let it wait
    canceler.cancel();
    let (result, mut server) = reader.join().unwrap();
    assert_eq!(result, Err(Error::Cancelled));

    // Final: later waits are cancelled too, while calls that needn't wait still work, and the peer sees nothing
    assert_eq!(server.receive(Duration::from_secs(1)), Err(Error::Cancelled));
    client.send(b"queued", None).unwrap();
    assert_eq!(server.receive(None).unwrap(), b"queued");
    server.send(b"still up", NO_WAIT).unwrap();
    assert_eq!(client.receive(None).unwrap(), b"still up");
    canceler.cancel(); // again: nothing more
}

#[test]
fn a_canceler_stops_an_accept_on_another_thread() {
    let name = unique_name("cancel_accept");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
    let canceler = listener.canceler();
    let server = thread::spawn(move || {
        let result = listener.accept(None).map(|_| ());
        (result, listener)
    });
    thread::sleep(Duration::from_millis(50));
    canceler.cancel();
    let (result, mut listener) = server.join().unwrap();
    assert_eq!(result, Err(Error::Cancelled));
    assert_eq!(listener.accept(NO_WAIT).unwrap_err(), Error::Cancelled);
    // A cancelled listener sets no new client up
    assert_eq!(Connection::connect(&name, Duration::from_millis(100)).unwrap_err(), Error::Timeout);
}

#[test]
fn a_canceler_outlives_its_handle() {
    let pair = Pair::new("canceler_after_drop");
    let canceler = pair.server.canceler();
    let listener_canceler = pair.listener.canceler();
    drop(pair);
    canceler.cancel(); // the connection is closed: nothing to do
    listener_canceler.cancel();
}

#[test]
fn cancelling_while_the_handle_is_dropped_on_another_thread() {
    // The canceler's call may hold the last reference: the close then runs on its thread, after the cancel
    for _ in 0..200 {
        let pair = Pair::with_ring("cancel_race", 1024);
        let canceler = pair.client.canceler();
        let barrier = Arc::new(Barrier::new(2));
        let wait = Arc::clone(&barrier);
        let dropper = thread::spawn(move || {
            wait.wait();
            drop(pair);
        });
        barrier.wait();
        canceler.cancel();
        dropper.join().unwrap();
    }
}

#[test]
fn dropping_a_connection_ends_it_after_its_messages() {
    let Pair { listener: _listener, mut client, mut server } = Pair::new("drop");
    for i in 0..3u8 {
        client.send(&[i], None).unwrap();
    }
    drop(client);
    for i in 0..3u8 {
        assert_eq!(server.receive(None).unwrap(), [i]); // every message the peer completed comes first
    }
    assert_eq!(server.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(server.receive(NO_WAIT), Err(Error::Disconnected)); // final
    assert_eq!(server.send(b"x", NO_WAIT), Err(Error::Disconnected));
    assert_eq!(server.rpc_submit(1, b"x", NO_WAIT), Err(Error::Disconnected));
}

#[test]
fn a_split_connection_closes_with_its_last_half() {
    let Pair { listener: _listener, client, mut server } = Pair::new("split_drop");
    let (tx, mut rx) = client.into_split();
    assert_eq!(tx.max_piece(), rx.max_piece());
    assert_eq!(tx.name(), rx.name());
    drop(tx);
    server.send(b"to the receiver", None).unwrap();
    assert_eq!(rx.receive(None).unwrap(), b"to the receiver");
    assert_eq!(server.receive(Duration::from_millis(50)), Err(Error::Timeout)); // still open
    drop(rx);
    assert_eq!(server.receive(TEN_SECONDS), Err(Error::Disconnected));
}

#[test]
fn halves_send_and_receive_at_once() {
    const COUNT: u32 = 20_000;
    let Pair { listener: _listener, client, server } = Pair::with_ring("halves", 4096);
    // The server echoes on two threads; the client sends on one and receives on another
    let (mut server_tx, mut server_rx) = server.into_split();
    let (to_echo_tx, to_echo_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let echo_in = thread::spawn(move || {
        while let Ok(message) = server_rx.receive(TEN_SECONDS) {
            if to_echo_tx.send(message).is_err() {
                break;
            }
        }
    });
    let echo_out = thread::spawn(move || {
        for message in to_echo_rx {
            server_tx.send(&message, TEN_SECONDS).unwrap();
        }
    });
    let (mut client_tx, mut client_rx) = client.into_split();
    let sender = thread::spawn(move || {
        for i in 0..COUNT {
            client_tx.send(&i.to_le_bytes(), TEN_SECONDS).unwrap();
        }
        client_tx
    });
    let mut buf = [0u8; 4];
    for i in 0..COUNT {
        assert_eq!(client_rx.receive_into(&mut buf, TEN_SECONDS), Ok(4));
        assert_eq!(u32::from_le_bytes(buf), i);
    }
    let client_tx = sender.join().unwrap();
    drop((client_tx, client_rx)); // the echo threads see the end
    echo_in.join().unwrap();
    echo_out.join().unwrap();
}

#[test]
fn a_borrowed_split_in_a_scope() {
    let mut pair = Pair::new("scope");
    let (tx, rx) = pair.server.split();
    thread::scope(|s| {
        s.spawn(|| {
            for i in 0..100u8 {
                tx.send(&[i], None).unwrap();
            }
        });
        s.spawn(|| {
            for i in 0..100u8 {
                assert_eq!(rx.receive(None).unwrap(), [i]);
            }
        });
        for i in 0..100u8 {
            assert_eq!(pair.client.receive(None).unwrap(), [i]);
            pair.client.send(&[i], None).unwrap();
        }
    });
    // A slot borrows its half: the other half goes on meanwhile
    let (tx, rx) = pair.server.split();
    let mut slot = tx.send_acquire(4, None).unwrap();
    pair.client.send(b"in", None).unwrap();
    assert_eq!(rx.receive(None).unwrap(), b"in");
    slot.copy_from_slice(b"out!");
    slot.commit(4).unwrap();
    assert_eq!(pair.client.receive(None).unwrap(), b"out!");
}

#[test]
fn a_connection_shared_between_threads() {
    // As in an application that keeps it in shared state: a Mutex serializes the calls
    let Pair { listener: _listener, client, mut server } = Pair::new("shared");
    let shared = Arc::new(Mutex::new(client));
    let threads: Vec<_> = (0..4u8)
        .map(|t| {
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                for _ in 0..50 {
                    shared.lock().unwrap().send(&[t], None).unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let mut counts = [0; 4];
    for _ in 0..200 {
        counts[server.receive(None).unwrap()[0] as usize] += 1;
    }
    assert_eq!(counts, [50; 4]);
}

#[test]
fn dropping_the_listener_keeps_its_connections() {
    let Pair { listener, mut client, mut server } = Pair::new("listener_drop");
    let name = listener.name().to_owned();
    drop(listener);
    client.send(b"still here", None).unwrap();
    assert_eq!(server.receive(None).unwrap(), b"still here");
    // The name is free again (on Windows, once the accepted connections are gone)
    drop((client, server));
    let start = Instant::now();
    loop {
        match Listener::listen(&name, 1 << 16) {
            Ok(_) => break,
            Err(Error::AddrInUse) if start.elapsed() < TEN_SECONDS => thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("listen again: {e}"),
        }
    }
}

// An accept that polls (NO_WAIT) sends SEGMENT and times out; the client answers READY and its connect returns. The
// listener is dropped before another accept takes the connection: the client gets Error::Disconnected.
#[test]
fn a_listener_dropped_before_its_accept_returns_disconnects_the_client() {
    let name = unique_name("unaccepted");
    let mut listener = Listener::listen(&name, 1 << 16).unwrap();
    let connecting = connect_aside(&name);
    let mut accepted = None;
    while !connecting.is_finished() {
        if accepted.is_none() {
            accepted = listener.accept(NO_WAIT).ok(); // READY may come within the call that sent SEGMENT
        }
        thread::sleep(Duration::from_millis(20));
    }
    let mut client = connecting.join().unwrap().unwrap();
    drop(accepted);
    drop(listener);
    assert_eq!(client.receive(TEN_SECONDS), Err(Error::Disconnected));
}
