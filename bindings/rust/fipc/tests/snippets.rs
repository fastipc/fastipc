//! The code fragments of the documentation (the READMEs, the website), each run here inside the code it needs. The
//! test `docs` checks that every Rust block of the documentation is in this file or in examples/.

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::{Pair, TEN_SECONDS, connect_aside, unique_name};
use fipc::{Error, NO_WAIT, ServiceRole, SubscriberRole};

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
    let mut listener = fipc::stream::create(&name, 1 << 16)?;
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

#[test]
fn a_lossy_subscriber_that_falls_behind() -> fipc::Result<()> {
    let name = unique_name("snippet_overrun");
    let mut publisher = fipc::broadcast::create(&name, 4096, 1, None)?;
    let mut subscriber = fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS)?;
    assert_eq!(publisher.wait_for_members(1, TEN_SECONDS)?, 1);
    for _ in 0..100 {
        publisher.send(b"update", NO_WAIT)?; // laps the broadcast's 64 messages
    }
    for _ in 0..2 {
        match subscriber.receive(None) {
            Ok(message) => handle(&message),
            // Fell a lap behind: the next receive goes on from a recent message
            Err(Error::Overrun { lost }) => println!("{lost} messages lost"),
            Err(e) => return Err(e), // Error::Disconnected: the broadcast ended
        }
    }
    Ok(())
}

#[test]
fn a_server_replies_in_place() -> fipc::Result<()> {
    let name = unique_name("snippet_in_place");
    let _service = fipc::service::create(&name, 4096, 256, 1, 1)?;
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS)?;
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS)?;
    client.send(b"in place", None)?;
    let request = server.receive_request(None)?; // held in place until the reply
    let len = request.len();
    request.reply_in_place(len, |request, reply| {
        reply.copy_from_slice(request); // the reply's room, apart from the request: zero-copy both ways
        reply.make_ascii_uppercase();
        len
    })?;
    assert_eq!(client.receive(None)?, b"IN PLACE");
    Ok(())
}

#[test]
fn an_abandoned_call() -> fipc::Result<()> {
    let name = unique_name("snippet_abandoned");
    let _service = fipc::service::create(&name, 4096, 256, 1, 1)?;
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS)?;
    let quitter = std::thread::spawn({
        let name = name.clone();
        move || -> fipc::Result<()> {
            let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS)?;
            server.receive(None)?; // and leaves without replying: Abandoned
            Ok(())
        }
    });
    // The server never replies, so the call can't return Ok: past the match, it returned Abandoned
    match client.call(b"charge 42", Duration::from_secs(5)) {
        Ok(reply) => handle(&reply),
        // Its server ended before it replied: the charge may or may not have been made
        Err(Error::Abandoned) => println!("ask the ledger before trying again"),
        Err(e) => return Err(e), // Error::Disconnected: the service ended
    }
    quitter.join().unwrap()?;
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // answered once: nothing more is owed
    Ok(())
}

#[test]
fn a_batch_each_way() -> fipc::Result<()> {
    let Pair { listener: _listener, client: mut conn, server: mut peer } = Pair::new("snippet_batch");
    let batch: [&[u8]; 3] = [b"one", b"two", b"three"];
    let sent = conn.send_many(&batch, None)?; // published together, at most one wake
    let mut buf = vec![0; 1 << 16];
    let mut lens = [0; 64];
    let count = peer.receive_many(&mut buf, &mut lens, None)?; // every message already there
    for message in fipc::batch_messages(&buf, &lens[..count]) {
        handle(message);
    }
    assert_eq!((sent, count), (3, 3));
    Ok(())
}
