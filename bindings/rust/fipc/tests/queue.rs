//! The work queue in one process: producers and consumers on threads, each message delivered once; the owner as a
//! member; the queue's end at its close or drop; the member count, its wait and its cancel; consumers racing for
//! messages longer than their receive's first buffer; a queue's misuses. Producers and consumers in other processes
//! are in process.rs, interop.rs and examples.rs.

mod common;

use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use common::{TEN_SECONDS, check_jobs, job, pattern, receive_until_the_end, unique_name};
use fipc::{Connection, Error, MembersError, NO_WAIT, QueueRole, SubscriberRole};

/// Producer `producer`'s message `i`, `len` bytes (at least 16): the two numbers, little-endian, then a pattern.
fn numbered(producer: u64, i: u64, len: usize) -> Vec<u8> {
    let mut message = pattern(len);
    message[..8].copy_from_slice(&producer.to_le_bytes());
    message[8..16].copy_from_slice(&i.to_le_bytes());
    message
}

/// A message's producer and number.
fn numbers(message: &[u8]) -> (u64, u64) {
    let producer = u64::from_le_bytes(message[..8].try_into().unwrap());
    (producer, u64::from_le_bytes(message[8..16].try_into().unwrap()))
}

/// That the consumers received each of `producers` x `count` messages exactly once and whole (message `i` is
/// `length(i)` bytes long), in any order: the queue promises none.
fn check_each_once(received: &[Vec<Vec<u8>>], producers: u64, count: u64, length: impl Fn(u64) -> usize) {
    let mut seen = HashSet::new();
    for message in received.iter().flatten() {
        let (producer, i) = numbers(message);
        assert!(producer < producers && i < count, "producer {producer}, message {i}");
        assert_eq!(*message, numbered(producer, i, length(i)), "producer {producer}, message {i}");
        assert!(seen.insert((producer, i)), "message {i} of producer {producer} received twice");
    }
    assert_eq!(seen.len() as u64, producers * count, "messages lost");
}

/// `count` members of `role` of the queue on `name`, joined one after the other.
fn join(name: &str, role: QueueRole, count: usize) -> Vec<Connection> {
    (0..count).map(|_| fipc::queue::join(name, role, TEN_SECONDS).unwrap()).collect()
}

#[test]
fn producers_and_consumers_on_threads_receive_each_message_once() {
    const PRODUCERS: u64 = 4;
    const COUNT: u64 = 2000;
    let name = unique_name("q_threads");
    let queue = fipc::queue::create(&name, 1 << 16, PRODUCERS as u32, 4).unwrap();
    let length = |i: u64| 16 + (i as usize * 7) % 200;
    let consumers = join(&name, QueueRole::Consumer, 4);
    let received: Vec<Vec<Vec<u8>>> = thread::scope(|s| {
        let consumers: Vec<_> = consumers
            .into_iter()
            .enumerate()
            .map(|(k, mut consumer)| {
                s.spawn(move || {
                    let mut messages = Vec::new();
                    let mut buf = vec![0; consumer.max_piece()];
                    loop {
                        // Each consumer receives its own way: into a new Vec, into its buffer, or in place
                        let message = match k % 3 {
                            0 => consumer.receive(TEN_SECONDS),
                            1 => consumer.receive_into(&mut buf, TEN_SECONDS).map(|len| buf[..len].to_vec()),
                            _ => consumer.receive_acquire(TEN_SECONDS).map(|slot| slot.to_vec()),
                        };
                        match message {
                            Ok(message) => messages.push(message),
                            Err(Error::Disconnected) => return messages,
                            Err(e) => panic!("consumer {k}: {e}"),
                        }
                    }
                })
            })
            .collect();
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let name = &name;
                s.spawn(move || {
                    let mut producer = fipc::queue::join(name, QueueRole::Producer, TEN_SECONDS).unwrap();
                    for i in 0..COUNT {
                        producer.send(&numbered(p, i, length(i)), TEN_SECONDS).unwrap();
                    }
                }) // the producer leaves the queue; its messages stay for the consumers
            })
            .collect();
        for producer in producers {
            producer.join().unwrap();
        }
        queue.close(); // ends the queue: the consumers receive what is left, then Disconnected
        consumers.into_iter().map(|consumer| consumer.join().unwrap()).collect()
    });
    check_each_once(&received, PRODUCERS, COUNT, length);
}

#[test]
fn the_owner_as_a_consumer_and_a_producer_in_one_thread() {
    let name = unique_name("q_roles");
    let queue = fipc::queue::create(&name, 4096, 1, 1).unwrap();
    assert_eq!(queue.name(), name);
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap();
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    assert_eq!((producer.max_piece(), consumer.max_piece()), (1024, 1024)); // a quarter of the capacity
    assert_eq!((producer.name(), consumer.name()), (name.as_str(), name.as_str()));
    assert_eq!(consumer.receive(NO_WAIT), Err(Error::Timeout));

    producer.send(b"hello", TEN_SECONDS).unwrap();
    let mut slot = producer.send_acquire(9, TEN_SECONDS).unwrap();
    slot.copy_from_slice(b"zero-copy");
    slot.commit(9).unwrap();
    producer.send(&[3; 1000], TEN_SECONDS).unwrap();
    let mut received = vec![consumer.receive(TEN_SECONDS).unwrap()];
    let held = consumer.receive_acquire(TEN_SECONDS).unwrap(); // in place, until dropped
    received.push(held.to_vec());
    drop(held);
    let mut buf = [0; 1000];
    let len = consumer.receive_into(&mut buf, TEN_SECONDS).unwrap();
    received.push(buf[..len].to_vec());
    received.sort();
    assert_eq!(received, [vec![3; 1000], b"hello".to_vec(), b"zero-copy".to_vec()]);
    producer.send(&[4; 1000], TEN_SECONDS).unwrap();
    assert_eq!(consumer.receive_into(&mut [0; 16], TEN_SECONDS), Err(Error::TooLarge { len: 1000 })); // left queued
    assert_eq!(consumer.receive(TEN_SECONDS).unwrap(), [4; 1000]);

    // What each role refuses
    let too_long = vec![1; producer.max_piece() + 1];
    assert_eq!(producer.send(&too_long, NO_WAIT), Err(Error::TooLarge { len: too_long.len() }));
    assert_eq!(producer.send_acquire(too_long.len(), NO_WAIT).unwrap_err(), Error::TooLarge { len: too_long.len() });
    assert_eq!(producer.receive(NO_WAIT), Err(Error::Invalid));
    assert_eq!(producer.receive_acquire(NO_WAIT).unwrap_err(), Error::Invalid);
    assert_eq!(consumer.send(b"x", NO_WAIT), Err(Error::Invalid));
    assert_eq!(consumer.send_acquire(1, NO_WAIT).unwrap_err(), Error::Invalid);
    for member in [&producer, &consumer] {
        assert_eq!(member.wait_for_members(0, NO_WAIT), Err(MembersError { error: Error::Invalid, count: 0 }));
    }

    // The queue's close ends it; the consumer receives what is left first
    producer.send(b"last", TEN_SECONDS).unwrap();
    queue.close();
    assert_eq!(producer.send(b"after the end", TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(consumer.receive(TEN_SECONDS).unwrap(), b"last");
    assert_eq!(consumer.receive(TEN_SECONDS), Err(Error::Disconnected));
}

/// The examples' exchange in one thread: the owner's producer sends before any consumer has joined, the owner waits
/// for a consumer, and the producer's and the queue's drops end the queue.
#[test]
fn the_owners_producer_sends_before_a_consumer_joins() {
    let name = unique_name("q_exchange");
    let queue = fipc::queue::create(&name, 4096, 1, 1).unwrap();
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    for i in 1..=3 {
        producer.send(&job(i), NO_WAIT).unwrap(); // waits in the queue
    }
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap();
    assert_eq!(queue.wait_for_members(QueueRole::Consumer, 1, TEN_SECONDS), Ok(1));
    drop(producer);
    drop(queue);
    check_jobs(&receive_until_the_end(&mut consumer), 1, 3);
}

#[test]
fn the_member_count_its_wait_and_its_cancel() {
    let name = unique_name("q_count");
    let queue = fipc::queue::create(&name, 4096, 2, 3).unwrap();
    assert_eq!(queue.wait_for_members(None, 0, NO_WAIT), Ok(0));
    let timeout = queue.wait_for_members(QueueRole::Producer, 1, NO_WAIT).unwrap_err();
    assert_eq!(timeout, MembersError { error: Error::Timeout, count: 0 });
    assert!(timeout.to_string().ends_with("(0 members joined)"), "{timeout}");

    let _producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    let consumers = join(&name, QueueRole::Consumer, 2);
    assert_eq!(queue.wait_for_members(QueueRole::Producer, 1, TEN_SECONDS), Ok(1));
    assert_eq!(queue.wait_for_members(QueueRole::Consumer, 2, TEN_SECONDS), Ok(2));
    assert_eq!(queue.wait_for_members(None, 3, TEN_SECONDS), Ok(3)); // both roles
    assert_eq!(queue.wait_for_members(Some(QueueRole::Consumer), 0, NO_WAIT), Ok(2));
    let short = queue.wait_for_members(QueueRole::Consumer, 3, Duration::from_millis(50));
    assert_eq!(short, Err(MembersError { error: Error::Timeout, count: 2 }));

    // Over the role's maximum (or both maxima together)
    for (role, at_least) in [(Some(QueueRole::Producer), 3), (Some(QueueRole::Consumer), 4), (None, 6)] {
        let over = queue.wait_for_members(role, at_least, NO_WAIT);
        assert_eq!(over, Err(MembersError { error: Error::Invalid, count: 0 }), "{role:?}");
    }

    // A member that leaves is counted until the owner's process has seen its end
    drop(consumers);
    let mut left = Ok(2);
    for _ in 0..1000 {
        left = queue.wait_for_members(QueueRole::Consumer, 0, NO_WAIT);
        if left == Ok(0) {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(left, Ok(0));

    // The cancel, from another thread, ends the wait; the queue goes on
    let canceler = queue.canceler();
    thread::scope(|s| {
        let waiting = s.spawn(|| queue.wait_for_members(QueueRole::Consumer, 1, TEN_SECONDS)); // through &queue
        canceler.cancel();
        assert_eq!(waiting.join().unwrap(), Err(MembersError { error: Error::Cancelled, count: 0 }));
    });
    assert_eq!(queue.wait_for_members(None, 2, TEN_SECONDS), Err(MembersError { error: Error::Cancelled, count: 1 }));
    assert_eq!(queue.wait_for_members(None, 0, NO_WAIT), Ok(1)); // a count that needn't wait still works
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap(); // joins go on
    assert_eq!(consumer.receive(NO_WAIT), Err(Error::Timeout));
}

/// The queue's handle is its one owner: dropping it ends the queue at a known place (no garbage collector), and a
/// canceler doesn't keep it alive. A producer kept beyond the queue's scope gets Disconnected.
#[test]
fn the_queues_drop_ends_it_and_frees_the_name() {
    let name = unique_name("q_drop");
    let (mut producer, canceler) = {
        let queue = fipc::queue::create(&name, 4096, 1, 1).unwrap();
        assert_eq!(fipc::queue::create(&name, 4096, 1, 1).unwrap_err(), Error::AddrInUse);
        let producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
        (producer, queue.canceler())
    }; // the queue's drop
    assert_eq!(producer.send(b"after the end", TEN_SECONDS), Err(Error::Disconnected));
    canceler.cancel(); // does nothing: the queue is closed
    let queue = fipc::queue::create(&name, 4096, 1, 1).unwrap(); // the name is free again
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    producer.send(b"a new queue", TEN_SECONDS).unwrap();
    assert_eq!(format!("{queue:?}"), format!("Queue {{ name: {name:?}, .. }}"));
}

/// Every message once, whole, through the receive's growing path under contention: every receive's first buffer is
/// empty, so it asks for the message's length, then takes it into room of that length; another consumer may take the
/// message between the two calls, and the receive then takes the next one, growing again for a longer one, and
/// returns exactly its length. That a retry waits only for what is left of the timeout is the unit test
/// `a_receive_that_grows_its_buffer_waits_only_for_what_is_left_of_its_timeout` (src/connection.rs): a race can't
/// show it reliably.
#[test]
fn two_consumers_race_for_messages_longer_than_their_first_buffer() {
    const COUNT: u64 = 3000;
    let name = unique_name("q_race");
    let queue = fipc::queue::create(&name, 1 << 16, 1, 2).unwrap();
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    let max_piece = producer.max_piece();
    let length = |i: u64| 16 + (i as usize * 997) % (max_piece - 15); // 16 to max_piece
    let consumers = join(&name, QueueRole::Consumer, 2);
    let received: Vec<Vec<Vec<u8>>> = thread::scope(|s| {
        let consumers: Vec<_> =
            consumers.into_iter().map(|mut consumer| s.spawn(move || receive_until_the_end(&mut consumer))).collect();
        for i in 0..COUNT {
            producer.send(&numbered(0, i, length(i)), TEN_SECONDS).unwrap();
        }
        queue.close();
        consumers.into_iter().map(|consumer| consumer.join().unwrap()).collect()
    });
    check_each_once(&received, 1, COUNT, length);
}

#[test]
fn a_queues_misuses() {
    let name = unique_name("q_misuse");
    let _queue = fipc::queue::create(&name, 4096, 1, 1).unwrap();
    assert_eq!(fipc::queue::create(&name, 4096, 1, 1).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::broadcast::create(&name, 4096, 1, None).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::stream::create(&name, 4096).unwrap_err(), Error::AddrInUse);
    for role in [SubscriberRole::Lossy, SubscriberRole::Gated] {
        assert_eq!(fipc::broadcast::join(&name, role, TEN_SECONDS).unwrap_err(), Error::Invalid, "{role:?}");
    }
    assert_eq!(fipc::stream::connect(&name, TEN_SECONDS).unwrap_err(), Error::Invalid); // a queue isn't a server

    let broadcast = unique_name("q_broadcast");
    let _publisher = fipc::broadcast::create(&broadcast, 4096, 1, None).unwrap();
    for role in [QueueRole::Producer, QueueRole::Consumer] {
        assert_eq!(fipc::queue::join(&broadcast, role, TEN_SECONDS).unwrap_err(), Error::Invalid, "{role:?}");
    }

    let bad_sizes = [(2048, 1, 1), (5000, 1, 1), (4096, 0, 1), (4096, 65, 1), (4096, 1, 0), (4096, 1, 65)];
    for (capacity, producers, consumers) in bad_sizes {
        let bad = fipc::queue::create(&unique_name("q_bad"), capacity, producers, consumers);
        assert_eq!(bad.unwrap_err(), Error::Invalid, "{capacity} {producers} {consumers}");
    }
    assert_eq!(fipc::queue::create("bad name", 4096, 1, 1).unwrap_err(), Error::Invalid);
    assert_eq!(fipc::queue::join("bad name", QueueRole::Producer, NO_WAIT).unwrap_err(), Error::Invalid);
}
