//! The broadcast in one process: a publisher and its lossy and gated subscribers, the overrun's lost count, the
//! subscriber count and its wait, an unsubscribed gated subscriber, a broadcast's misuses. Publishers and subscribers
//! in other processes are in process.rs and interop.rs.

mod common;

use std::time::Duration;

use common::{TEN_SECONDS, unique_name};
use fipc::{Error, MembersError, NO_WAIT, QueueRole, SubscriberRole};

/// `len` bytes that start with `i`, little-endian.
fn numbered(i: u64, len: usize) -> Vec<u8> {
    let mut message = vec![0; len];
    message[..8].copy_from_slice(&i.to_le_bytes());
    message
}

fn number(message: &[u8]) -> u64 {
    u64::from_le_bytes(message[..8].try_into().unwrap())
}

#[test]
fn lossy_and_gated_subscribers_receive_every_message_then_disconnected() {
    let name = unique_name("bc_roles");
    let mut publisher = fipc::broadcast::create(&name, 1 << 16, 4, None).unwrap();
    let mut lossy = fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS).unwrap();
    let mut gated = fipc::broadcast::join(&name, SubscriberRole::Gated, TEN_SECONDS).unwrap();
    assert_eq!(publisher.wait_for_members(2, TEN_SECONDS), Ok(2));
    assert_eq!(publisher.max_piece(), (1 << 16) / 4);
    assert_eq!((lossy.max_piece(), lossy.name()), (publisher.max_piece(), name.as_str()));

    let words = [b"hello".to_vec(), b"shared".to_vec(), b"memory".repeat(1000)];
    for word in &words {
        publisher.send(word, TEN_SECONDS).unwrap();
    }
    for word in &words {
        assert_eq!(&lossy.receive(TEN_SECONDS).unwrap(), word);
    }
    let mut buf = [0; 16];
    assert_eq!(gated.receive_into(&mut buf, TEN_SECONDS), Ok(5));
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(gated.receive_acquire(NO_WAIT).unwrap_err(), Error::Invalid); // in place on a gated one: unsafe
    // SAFETY: the broadcast's max_stall is None: nobody is unsubscribed
    let held = unsafe { gated.receive_acquire_gated(TEN_SECONDS) }.unwrap(); // holds the publisher back until dropped
    assert_eq!(&*held, b"shared");
    drop(held);
    assert_eq!(gated.receive(TEN_SECONDS).unwrap(), words[2]);

    // Zero-copy publishing; a broadcast's misuses
    let mut slot = publisher.send_acquire(4, TEN_SECONDS).unwrap();
    slot.copy_from_slice(b"tick");
    slot.commit(4).unwrap();
    assert_eq!(lossy.receive(TEN_SECONDS).unwrap(), b"tick");
    assert_eq!(gated.receive(TEN_SECONDS).unwrap(), b"tick");
    assert_eq!(lossy.receive(NO_WAIT), Err(Error::Timeout));
    assert_eq!(lossy.receive_acquire(NO_WAIT).unwrap_err(), Error::Invalid); // a lossy subscriber copies
    // SAFETY: a lossy subscriber's memory is never revoked; the library refuses the call anyway
    assert_eq!(unsafe { lossy.receive_acquire_gated(NO_WAIT) }.unwrap_err(), Error::Invalid);
    assert_eq!(publisher.receive(NO_WAIT), Err(Error::Invalid));
    assert_eq!(gated.send(b"x", NO_WAIT), Err(Error::Invalid));
    let not_the_publisher = lossy.wait_for_members(0, NO_WAIT);
    assert_eq!(not_the_publisher, Err(MembersError { error: Error::Invalid, count: 0 }));
    let too_long = vec![1; publisher.max_piece() + 1];
    assert_eq!(publisher.send(&too_long, NO_WAIT), Err(Error::TooLarge { len: too_long.len() }));

    // The publisher's drop ends the broadcast: what it sent is received first
    publisher.send(b"last", TEN_SECONDS).unwrap();
    drop(publisher);
    for subscriber in [&mut lossy, &mut gated] {
        assert_eq!(subscriber.receive(TEN_SECONDS).unwrap(), b"last");
        assert_eq!(subscriber.receive(TEN_SECONDS), Err(Error::Disconnected));
    }
}

/// The library admits subscribers itself: eight join one after the other, with no call of the publisher's meanwhile.
#[test]
fn subscribers_join_with_no_call_of_the_publishers() {
    let name = unique_name("bc_admission");
    let mut publisher = fipc::broadcast::create(&name, 4096, 8, None).unwrap();
    let subscribers: Vec<_> =
        (0..8).map(|_| fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS).unwrap()).collect();
    assert_eq!(publisher.wait_for_members(8, TEN_SECONDS), Ok(8));
    publisher.send(b"to all", NO_WAIT).unwrap();
    for mut subscriber in subscribers {
        assert_eq!(subscriber.receive(TEN_SECONDS).unwrap(), b"to all");
    }
}

#[test]
fn a_lapped_lossy_subscriber_gets_overrun_with_the_number_lost() {
    let name = unique_name("bc_overrun");
    let mut publisher = fipc::broadcast::create(&name, 4096, 1, None).unwrap();
    let mut subscriber = fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS).unwrap();
    assert_eq!(publisher.wait_for_members(1, TEN_SECONDS), Ok(1));
    // 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
    // (docs/protocol.md §9.6), so 0 to 139 are lost
    for i in 0..144 {
        publisher.send(&numbered(i, 48), NO_WAIT).unwrap();
    }
    let overrun = subscriber.receive(TEN_SECONDS).unwrap_err();
    assert_eq!(overrun, Error::Overrun { lost: 140 });
    assert!(overrun.to_string().starts_with("FIPC_OVERRUN: 140 messages lost"), "{overrun}");
    assert_eq!(number(&subscriber.receive(TEN_SECONDS).unwrap()), 140);
    let mut buf = [0; 64];
    for i in 141..144 {
        assert_eq!(subscriber.receive_into(&mut buf, TEN_SECONDS), Ok(48));
        assert_eq!(number(&buf), i);
    }
    assert_eq!(subscriber.receive(NO_WAIT), Err(Error::Timeout));

    // A receive into the caller's buffer reports it too
    for i in 0..144 {
        publisher.send(&numbered(i, 48), NO_WAIT).unwrap();
    }
    assert_eq!(subscriber.receive_into(&mut buf, TEN_SECONDS), Err(Error::Overrun { lost: 140 }));
    assert_eq!(subscriber.receive_into(&mut buf, TEN_SECONDS), Ok(48));
    assert_eq!(number(&buf), 140);
}

#[test]
fn the_subscriber_count_and_its_wait() {
    let name = unique_name("bc_count");
    let publisher = fipc::broadcast::create(&name, 4096, 2, None).unwrap();
    assert_eq!(publisher.wait_for_members(0, NO_WAIT), Ok(0));
    let timeout = publisher.wait_for_members(1, NO_WAIT).unwrap_err();
    assert_eq!(timeout, MembersError { error: Error::Timeout, count: 0 });
    assert!(timeout.to_string().ends_with("(0 members joined)"), "{timeout}");

    std::thread::scope(|s| {
        let waiting = s.spawn(|| publisher.wait_for_members(1, TEN_SECONDS)); // any thread, through &publisher
        let _subscriber = fipc::broadcast::join(&name, SubscriberRole::Gated, TEN_SECONDS).unwrap();
        assert_eq!(waiting.join().unwrap(), Ok(1));
        assert_eq!(publisher.wait_for_members(0, NO_WAIT), Ok(1));
        let short = publisher.wait_for_members(2, Duration::from_millis(50));
        assert_eq!(short, Err(MembersError { error: Error::Timeout, count: 1 }));

        let cancelled = s.spawn(|| publisher.wait_for_members(2, TEN_SECONDS));
        publisher.cancel(); // the wait, now or about to start, returns Cancelled
        assert_eq!(cancelled.join().unwrap(), Err(MembersError { error: Error::Cancelled, count: 1 }));
    });
    let over = publisher.wait_for_members(3, NO_WAIT); // over max_subscribers
    assert_eq!(over, Err(MembersError { error: Error::Invalid, count: 0 }));
    assert_eq!(Error::from(over.unwrap_err()), Error::Invalid);
    // A count that needn't wait works after the cancel too, on either half
    let (sender, receiver) = publisher.into_split();
    assert!(sender.wait_for_members(0, NO_WAIT).is_ok() && receiver.wait_for_members(0, NO_WAIT).is_ok());
}

#[test]
fn a_gated_subscriber_that_holds_the_publisher_back_is_unsubscribed() {
    let name = unique_name("bc_stall");
    let mut publisher = fipc::broadcast::create(&name, 4096, 1, Duration::from_millis(100)).unwrap();
    let mut subscriber = fipc::broadcast::join(&name, SubscriberRole::Gated, TEN_SECONDS).unwrap();
    assert_eq!(publisher.wait_for_members(1, TEN_SECONDS), Ok(1));
    // The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed, and the publisher goes
    // on
    for _ in 0..100 {
        publisher.send(&[0; 48], TEN_SECONDS).unwrap();
    }
    assert_eq!(publisher.wait_for_members(0, NO_WAIT), Ok(0));
    assert_eq!(subscriber.receive(NO_WAIT), Err(Error::Unsubscribed));
    assert_eq!(subscriber.receive(NO_WAIT), Err(Error::Unsubscribed)); // final
}

#[test]
fn a_broadcast_that_refuses_gated_subscribers_and_misuses() {
    let name = unique_name("bc_no_gated");
    let _publisher = fipc::broadcast::create(&name, 4096, 1, NO_WAIT).unwrap();
    assert_eq!(fipc::broadcast::join(&name, SubscriberRole::Gated, TEN_SECONDS).unwrap_err(), Error::Invalid);
    assert_eq!(fipc::broadcast::create(&name, 4096, 1, None).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::queue::create(&name, 4096, 1, 1).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::stream::create(&name, 4096).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::stream::connect(&name, TEN_SECONDS).unwrap_err(), Error::Invalid); // a broadcast isn't a server
    assert_eq!(fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap_err(), Error::Invalid);
    for (capacity, max_subscribers) in [(1000, 1), (2048, 1), (4096, 0), (4096, 1025)] {
        let bad = fipc::broadcast::create(&unique_name("bc_bad"), capacity, max_subscribers, None);
        assert_eq!(bad.unwrap_err(), Error::Invalid, "{capacity} {max_subscribers}");
    }
    assert_eq!(fipc::broadcast::join("bad name", SubscriberRole::Lossy, NO_WAIT).unwrap_err(), Error::Invalid);
}

/// The name of a stream whose server accepts: a subscriber learns the kind from the server's accept and gets Invalid,
/// and the accept goes on waiting.
#[test]
fn a_subscriber_refuses_a_streams_name() {
    let name = unique_name("bc_stream");
    let mut listener = fipc::stream::create(&name, 1 << 16).unwrap();
    std::thread::scope(|s| {
        let accepting = s.spawn(|| listener.accept(Duration::from_secs(2)).map(|_| ()));
        assert_eq!(fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS).unwrap_err(), Error::Invalid);
        assert_eq!(accepting.join().unwrap(), Err(Error::Timeout));
    });
}
