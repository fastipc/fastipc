//! Copied messages, zero-copy, timeouts and error results, between two connections in this process.

mod common;

use std::io;
use std::thread;
use std::time::{Duration, Instant};

use common::{Pair, TEN_SECONDS, connect_aside, pattern, unique_name};
use fipc::{Error, NO_WAIT};

#[test]
fn messages_of_every_size_arrive_whole() {
    let Pair { listener: _listener, client, mut server } = Pair::with_ring("sizes", 1 << 16);
    let piece = server.max_piece();
    assert_eq!(piece, (1 << 16) - 64);
    let sizes = [1, 15, 16, 17, 1000, piece - 1, piece, piece + 1, 200_000, 3_000_000];
    // A message longer than the ring needs a receiver at the same time: the client sends from a thread
    let sender = thread::spawn(move || {
        let mut client = client;
        for size in sizes {
            client.send(&pattern(size), TEN_SECONDS).unwrap();
        }
        client
    });
    for size in sizes {
        assert_eq!(server.receive(TEN_SECONDS).unwrap(), pattern(size), "size {size}");
    }
    let _client = sender.join().unwrap();
}

#[test]
fn receive_into_reports_the_room_a_message_needs() {
    let mut pair = Pair::new("into");
    pair.client.send(&pattern(300), None).unwrap();
    let mut small = [0u8; 100];
    assert_eq!(pair.server.receive_into(&mut small, None), Err(Error::TooLarge { len: 300 }));
    assert_eq!(pair.server.receive_into(&mut [], None), Err(Error::TooLarge { len: 300 })); // still queued
    let mut buf = vec![0u8; 512];
    assert_eq!(pair.server.receive_into(&mut buf, None), Ok(300));
    assert_eq!(&buf[..300], &pattern(300)[..]);
    assert_eq!(pair.server.receive_into(&mut buf, NO_WAIT), Err(Error::Timeout));
}

#[test]
fn an_empty_message_is_invalid() {
    let mut pair = Pair::new("empty");
    assert_eq!(pair.client.send(&[], None), Err(Error::Invalid));
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn messages_arrive_in_order_both_ways() {
    let mut pair = Pair::new("order");
    for i in 0..1000u32 {
        pair.client.send(&i.to_le_bytes(), None).unwrap();
        pair.server.send(&(i + 1).to_le_bytes(), None).unwrap();
    }
    for i in 0..1000u32 {
        assert_eq!(pair.server.receive(None).unwrap(), i.to_le_bytes());
        assert_eq!(pair.client.receive(None).unwrap(), (i + 1).to_le_bytes());
    }
}

#[test]
fn zero_copy_round_trip() {
    let Pair { listener: _listener, client, mut server } = Pair::new("zerocopy");
    let sizes = [1, 16, 100, 4096, client.max_piece(), 3, client.max_piece()];
    // A frame never wraps: one that doesn't fit before the ring's end waits for the receiver to pass the end, so the
    // receiver runs at the same time
    let sender = thread::spawn(move || {
        let mut client = client;
        for size in sizes {
            let mut slot = client.send_acquire(size, TEN_SECONDS).unwrap();
            assert_eq!(slot.len(), size);
            assert_eq!(slot.as_ptr() as usize % 16, 0, "16-byte aligned");
            slot.copy_from_slice(&pattern(size));
            slot.commit(size).unwrap();
        }
        client
    });
    for size in sizes {
        let message = server.receive_acquire(TEN_SECONDS).unwrap();
        assert_eq!(message.as_ptr() as usize % 16, 0, "16-byte aligned");
        assert_eq!(&*message, &pattern(size)[..], "size {size}");
        message.release();
    }
    let mut pair = Pair { listener: _listener, client: sender.join().unwrap(), server };
    // A commit of part of the slot sends that part; the copying calls see zero-copy messages and vice versa
    let mut slot = pair.client.send_acquire(64, None).unwrap();
    slot[..5].copy_from_slice(b"hello");
    slot.commit(5).unwrap();
    pair.client.send(b"copied", None).unwrap();
    assert_eq!(pair.server.receive(None).unwrap(), b"hello");
    assert_eq!(&*pair.server.receive_acquire(None).unwrap(), b"copied");
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_dropped_send_slot_sends_nothing() {
    let mut pair = Pair::new("abandon");
    let mut slot = pair.client.send_acquire(32, None).unwrap();
    slot.fill(b'X');
    drop(slot);
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
    pair.client.send(b"after", None).unwrap();
    assert_eq!(pair.server.receive(None).unwrap(), b"after");
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_commit_out_of_range_sends_nothing() {
    let mut pair = Pair::new("commit");
    assert_eq!(pair.client.send_acquire(8, None).unwrap().commit(0), Err(Error::Invalid));
    assert_eq!(pair.client.send_acquire(8, None).unwrap().commit(9), Err(Error::Invalid));
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
    pair.client.send_acquire(8, None).unwrap().commit(8).unwrap();
    assert_eq!(pair.server.receive(None).unwrap().len(), 8);
}

#[test]
fn zero_copy_takes_one_piece() {
    let Pair { listener: _listener, mut client, mut server } = Pair::with_ring("zc_large", 1 << 16);
    let piece = client.max_piece();
    assert_eq!(client.send_acquire(piece + 1, NO_WAIT).unwrap_err(), Error::TooLarge { len: piece + 1 });
    assert_eq!(client.send_acquire(0, NO_WAIT).unwrap_err(), Error::Invalid);
    let sender = thread::spawn(move || {
        client.send(&pattern(100_000), None).unwrap();
        client
    });
    // A message in several pieces stays queued for the copying calls
    assert_eq!(server.receive_acquire(TEN_SECONDS).unwrap_err(), Error::TooLarge { len: 100_000 });
    assert_eq!(server.receive(None).unwrap(), pattern(100_000));
    let _client = sender.join().unwrap();
}

#[test]
fn timeouts() {
    let mut pair = Pair::with_ring("timeouts", 1024);
    assert_eq!(pair.server.receive(NO_WAIT), Err(Error::Timeout));
    assert_eq!(pair.server.receive_acquire(NO_WAIT).unwrap_err(), Error::Timeout);
    assert_eq!(pair.listener.accept(NO_WAIT).unwrap_err(), Error::Invalid); // its last connection is open

    let start = Instant::now();
    assert_eq!(pair.server.receive(Duration::from_millis(50)), Err(Error::Timeout));
    assert!(start.elapsed() >= Duration::from_millis(45), "{:?}", start.elapsed());

    // A full ring: the sender times out and changes nothing
    while pair.client.send(&[1; 100], NO_WAIT).is_ok() {}
    assert_eq!(pair.client.send(&[1; 100], Duration::from_millis(20)), Err(Error::Timeout));
    assert_eq!(pair.client.send_acquire(100, NO_WAIT).unwrap_err(), Error::Timeout);
    assert_eq!(pair.server.receive(None).unwrap(), [1; 100]);
    pair.client.send(&[2; 100], NO_WAIT).unwrap();

    let nobody = unique_name("nobody");
    assert_eq!(fipc::stream::connect(&nobody, NO_WAIT).unwrap_err(), Error::Timeout);
    assert_eq!(fipc::stream::connect(&nobody, Duration::from_millis(30)).unwrap_err(), Error::Timeout);
}

#[test]
fn create_errors() {
    for bad in ["", ".hidden", "-dash", "a b", "slash/name", "nul\0name", &"x".repeat(246)] {
        assert_eq!(fipc::stream::create(bad, 1 << 16).unwrap_err(), Error::Invalid, "{bad:?}");
    }
    let name = unique_name("errors");
    for bad in [0, 1000, 1023, 1025, 3 << 20] {
        assert_eq!(fipc::stream::create(&name, bad).unwrap_err(), Error::Invalid, "capacity {bad}");
    }
    let listener = fipc::stream::create(&name, 1024).unwrap();
    assert_eq!(listener.name(), name);
    assert_eq!(fipc::stream::create(&name, 1024).unwrap_err(), Error::AddrInUse);
    drop(listener);
    fipc::stream::create(&name, 1024).unwrap(); // free again
    assert_eq!(fipc::stream::connect("bad name", NO_WAIT).unwrap_err(), Error::Invalid);
}

#[test]
fn one_client_at_a_time() {
    let name = unique_name("one_at_a_time");
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
    let connecting = connect_aside(&name);
    let server = listener.accept(TEN_SECONDS).unwrap();
    let first = connecting.join().unwrap().unwrap();
    assert_eq!(server.name(), name);
    assert_eq!(first.name(), name);
    assert_eq!(listener.accept(NO_WAIT).unwrap_err(), Error::Invalid);
    drop(server);
    drop(first);
    let connecting = connect_aside(&name);
    let _server = listener.accept(TEN_SECONDS).unwrap();
    let second = connecting.join().unwrap().unwrap();
    assert_eq!(second.max_piece(), (1 << 16) - 64);
}

#[test]
fn errors_name_their_result() {
    let cases = [
        (Error::Timeout, 1, "FIPC_TIMEOUT", io::ErrorKind::TimedOut),
        (Error::Disconnected, 2, "FIPC_DISCONNECTED", io::ErrorKind::ConnectionReset),
        (Error::Cancelled, 3, "FIPC_CANCELLED", io::ErrorKind::Interrupted),
        (Error::TooLarge { len: 7 }, 4, "FIPC_TOO_LARGE", io::ErrorKind::InvalidInput),
        (Error::Invalid, 5, "FIPC_INVALID", io::ErrorKind::InvalidInput),
        (Error::NoMemory, 6, "FIPC_NO_MEMORY", io::ErrorKind::OutOfMemory),
        (Error::AddrInUse, 7, "FIPC_ADDR_IN_USE", io::ErrorKind::AddrInUse),
    ];
    for (error, code, name, kind) in cases {
        assert_eq!(error.code(), code);
        assert_eq!(error.name(), name);
        assert!(error.to_string().starts_with(&format!("{name}: ")), "{error}");
        let io_error = io::Error::from(error);
        assert_eq!(io_error.kind(), kind);
        assert_eq!(io_error.get_ref().unwrap().downcast_ref::<Error>(), Some(&error));
    }
    assert_eq!(Error::TooLarge { len: 7 }.to_string(), "FIPC_TOO_LARGE: the message (7 bytes) doesn't fit");
}

#[test]
fn debug_output_names_the_handles() {
    let mut pair = Pair::new("debug");
    assert!(format!("{:?}", pair.listener).contains("fipc_rust_debug_"));
    assert!(format!("{:?}", pair.client).contains("max_piece"));
    assert!(format!("{:?}", pair.client.canceler()).contains("connection"));
    let slot = pair.client.send_acquire(4, None).unwrap();
    assert!(format!("{slot:?}").contains("len: 4"));
}
