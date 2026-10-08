//! The code fragments of the documentation (the READMEs, the website), each run here inside the code it needs. The
//! test `docs` checks that every Rust block of the documentation is in this file or in examples/.

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::{Pair, connect_aside, unique_name};
use fipc::{Error, Listener};

fn handle(message: &[u8]) {
    assert!(!message.is_empty());
}

#[test]
fn poll_with_a_timeout() -> fipc::Result<()> {
    let mut pair = Pair::new("snippet_poll");
    let conn = &mut pair.server;
    pair.client.send(b"one", None)?;
    for _ in 0..2 {
        match conn.receive(Duration::from_millis(100)) {
            Ok(message) => handle(&message),
            Err(Error::Timeout) => {} // nothing yet
            Err(e) => return Err(e),  // Error::Disconnected: the peer is gone
        }
    }
    Ok(())
}

#[test]
fn zero_copy_in_place() -> fipc::Result<()> {
    let mut pair = Pair::new("snippet_zero_copy");
    {
        let conn = &mut pair.client;
        let mut slot = conn.send_acquire(5, None)?; // room in the ring
        slot.copy_from_slice(b"hello");
        slot.commit(5)?;
    }
    {
        let conn = &mut pair.server;
        let message = conn.receive_acquire(None)?; // read-only, in the ring
        handle(&message);
        drop(message); // frees its room
    }
    Ok(())
}

#[test]
fn stop_a_reader_thread() {
    let Pair { listener: _listener, server: mut conn, client: mut peer } = Pair::new("snippet_reader");
    peer.send(b"before the cancel", None).unwrap();
    let canceler = conn.canceler();
    let reader = std::thread::spawn(move || {
        // until Error::Cancelled (or Error::Disconnected)
        while let Ok(message) = conn.receive(None) {
            handle(&message);
        }
    });
    // ...
    canceler.cancel(); // wakes the reader: Cancelled
    reader.join().unwrap();
}

#[test]
fn a_reader_thread_feeds_a_channel() -> fipc::Result<()> {
    let name = unique_name("snippet_channel");
    let mut listener = Listener::listen(&name, 1 << 16)?;
    let connecting = connect_aside(&name);
    let conn = listener.accept(None)?;
    let mut peer = connecting.join().unwrap()?;
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    // Sending on this thread, receiving on another: the halves of the connection
    let (mut sender, mut receiver) = conn.into_split();
    let reader = std::thread::spawn(move || {
        while let Ok(message) = receiver.receive(None) {
            if tx.send(message).is_err() {
                break; // the frame loop is gone
            }
        }
    });
    // Each frame: whatever has arrived, without waiting
    sender.send(b"update", None)?;
    let echo = peer.receive(None)?;
    peer.send(&echo, None)?;
    let echoed = rx.recv().unwrap();
    while let Ok(message) = rx.try_recv() {
        handle(&message);
    }
    assert_eq!(echoed, b"update");
    drop(peer); // the reader sees Error::Disconnected and ends
    reader.join().unwrap();
    Ok(())
}
