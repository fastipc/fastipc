//! Batches (send_many, receive_many): on a stream, in order and packed back to back, a buffer that stops a batch and a
//! first message too long for it, NO_WAIT's partial send with how many went, an empty message refused before any goes,
//! the split halves; a broadcast's and a work queue's batches, and TooLarge over max_piece before any goes; a service
//! client's batch of requests, owed as its sends are, with their replies in order, and a server's batches refused.

mod common;

use std::thread;

use common::{Pair, TEN_SECONDS, pattern, unique_name};
use fipc::{Connection, Error, NO_WAIT, QueueRole, SendManyError, ServiceRole, SubscriberRole};

/// `count` messages, received in batches (a broadcast's subscriber may take fewer than are there).
fn receive_all(conn: &mut Connection, count: usize) -> Vec<Vec<u8>> {
    let mut buf = vec![0; 1 << 20];
    let mut lens = [0; 16];
    let mut received = Vec::new();
    while received.len() < count {
        let taken = conn.receive_many(&mut buf, &mut lens, TEN_SECONDS).unwrap();
        assert!((1..=16).contains(&taken), "a batch of {taken}");
        received.extend(fipc::batch_messages(&buf, &lens[..taken]).map(<[u8]>::to_vec));
    }
    received
}

#[test]
fn a_streams_batches_come_in_order_packed_back_to_back() {
    let Pair { listener: _listener, mut client, mut server } = Pair::with_ring("batch", 1024);
    let messages: Vec<Vec<u8>> = (0..6).map(|i| pattern(10 + i * 20)).collect();
    assert_eq!(client.send_many(&messages, TEN_SECONDS), Ok(6));
    let mut buf = [0u8; 4096];
    let mut lens = [0usize; 16];
    assert_eq!(server.receive_many(&mut buf, &mut lens, TEN_SECONDS), Ok(6));
    assert!(fipc::batch_messages(&buf, &lens[..6]).eq(messages.iter().map(Vec::as_slice)));

    // A buffer that stops the batch; a first message too long for it stays queued, its length reported
    assert_eq!(client.send_many(&messages, TEN_SECONDS), Ok(6));
    assert_eq!(server.receive_many(&mut buf[..10 + 30], &mut lens, TEN_SECONDS), Ok(2));
    assert_eq!(server.receive_many(&mut buf[..10], &mut lens, TEN_SECONDS), Err(Error::TooLarge { len: 50 }));
    assert_eq!(server.receive_many(&mut buf, &mut lens[..3], TEN_SECONDS), Ok(3)); // at most lens.len()
    assert_eq!(server.receive_many(&mut buf, &mut lens, TEN_SECONDS), Ok(1));
    assert_eq!(&buf[..lens[0]], &messages[5][..]);

    // A message longer than one piece goes in pieces and arrives whole; the halves batch too
    let long = pattern(100_000);
    let batch = [&messages[0][..], &long, &messages[1]];
    let (tx, _) = client.split();
    let received = thread::scope(|s| {
        let receiving = s.spawn(|| receive_all(&mut server, 3));
        assert_eq!(tx.send_many(&batch, TEN_SECONDS), Ok(3));
        receiving.join().unwrap()
    });
    assert_eq!(received, batch);
    assert_eq!(server.receive_many(&mut buf, &mut lens, NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_partial_send_says_how_many_went_and_an_empty_message_sends_none() {
    let Pair { listener: _listener, mut client, mut server } = Pair::with_ring("partial", 1024);
    let hundred = pattern(100);
    let many = vec![&hundred[..]; 100];
    // Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
    let partial = client.send_many(&many, NO_WAIT).unwrap_err();
    assert_eq!(partial.error, Error::Timeout);
    assert!((1..=8).contains(&partial.sent), "{partial}");
    assert!(receive_all(&mut server, partial.sent).iter().all(|m| *m == hundred));

    let received = thread::scope(|s| {
        let receiving = s.spawn(|| receive_all(&mut server, 100));
        assert_eq!(client.send_many(&many, TEN_SECONDS), Ok(100)); // sent while it is received
        receiving.join().unwrap()
    });
    assert_eq!(received, many);

    let with_empty: [&[u8]; 2] = [&hundred, &[]];
    assert_eq!(client.send_many(&with_empty, NO_WAIT), Err(SendManyError { error: Error::Invalid, sent: 0 }));
    let none: [&[u8]; 0] = [];
    assert_eq!(client.send_many(&none, NO_WAIT), Err(SendManyError { error: Error::Invalid, sent: 0 }));
    assert_eq!(server.receive_many(&mut [0; 16], &mut [], NO_WAIT), Err(Error::Invalid));
    assert_eq!(server.receive_many(&mut [0; 16], &mut [0; 4], NO_WAIT), Err(Error::Timeout)); // nothing went
    let error: Error = client.send_many(&with_empty, NO_WAIT).unwrap_err().into(); // `?` into fipc::Result
    assert_eq!(error, Error::Invalid);
}

#[test]
fn a_broadcasts_batches_reach_every_subscriber() {
    let name = unique_name("bc_batch");
    let mut publisher = fipc::broadcast::create(&name, 1 << 16, 4, None).unwrap();
    let mut lossy = fipc::broadcast::join(&name, SubscriberRole::Lossy, TEN_SECONDS).unwrap();
    let mut gated = fipc::broadcast::join(&name, SubscriberRole::Gated, TEN_SECONDS).unwrap();
    assert_eq!(publisher.wait_for_members(2, TEN_SECONDS), Ok(2));
    let ticks: Vec<Vec<u8>> = (0..40).map(|i| format!("tick {i}").into_bytes()).collect();
    assert_eq!(publisher.send_many(&ticks, TEN_SECONDS), Ok(40));
    let too_large = vec![0u8; publisher.max_piece() + 1];
    assert_eq!(
        publisher.send_many(&[&ticks[0][..], &too_large], NO_WAIT),
        Err(SendManyError { error: Error::TooLarge { len: too_large.len() }, sent: 0 })
    );
    assert_eq!(publisher.receive_many(&mut [0; 64], &mut [0; 4], NO_WAIT), Err(Error::Invalid));
    assert_eq!(gated.send_many(&ticks, NO_WAIT).unwrap_err().error, Error::Invalid);
    drop(publisher); // what it sent is received first
    for subscriber in [&mut lossy, &mut gated] {
        assert_eq!(receive_all(subscriber, 40), ticks);
        assert_eq!(subscriber.receive_many(&mut [0; 64], &mut [0; 4], TEN_SECONDS), Err(Error::Disconnected));
    }
}

#[test]
fn a_queues_batches_deliver_every_message_once() {
    let name = unique_name("q_batch");
    let queue = fipc::queue::create(&name, 4096, 1, 1).unwrap();
    let mut producer = fipc::queue::join(&name, QueueRole::Producer, TEN_SECONDS).unwrap();
    let mut consumer = fipc::queue::join(&name, QueueRole::Consumer, TEN_SECONDS).unwrap();
    let jobs: Vec<Vec<u8>> = (0..200).map(|i| format!("job {i}").into_bytes()).collect();
    let received = thread::scope(|s| {
        let receiving = s.spawn(|| receive_all(&mut consumer, 200));
        for chunk in jobs.chunks(50) {
            assert_eq!(producer.send_many(chunk, TEN_SECONDS), Ok(50));
        }
        receiving.join().unwrap()
    });
    assert_eq!(received, jobs); // one producer: its messages in order
    let too_large = vec![0u8; producer.max_piece() + 1];
    assert_eq!(
        producer.send_many(&[&jobs[0][..], &too_large], NO_WAIT).unwrap_err().error,
        Error::TooLarge { len: too_large.len() }
    );
    assert_eq!(consumer.send_many(&jobs, NO_WAIT).unwrap_err().error, Error::Invalid);
    assert_eq!(producer.receive_many(&mut [0; 64], &mut [0; 4], NO_WAIT), Err(Error::Invalid));
    drop(queue);
    assert_eq!(consumer.receive_many(&mut [0; 64], &mut [0; 4], TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(producer.send_many(&jobs, TEN_SECONDS).unwrap_err().error, Error::Disconnected);
}

/// A server's answer to the next request: the request with " done".
fn answer(server: &mut Connection) {
    let mut request = server.receive(TEN_SECONDS).unwrap();
    request.extend_from_slice(b" done");
    server.send(&request, NO_WAIT).unwrap();
}

#[test]
fn a_service_clients_batch_is_owed_as_its_sends_are_and_a_servers_batches_are_invalid() {
    let name = unique_name("s_batch");
    let _service = fipc::service::create(&name, 1 << 14, 64, 1, 1).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let requests: [&[u8]; 3] = [b"a", b"bb", b"ccc"];
    assert_eq!(client.send_many(&requests, TEN_SECONDS), Ok(3));
    assert_eq!(client.call(b"d", NO_WAIT), Err(Error::Invalid)); // those replies are the caller's
    for request in requests {
        assert_eq!(server.receive(TEN_SECONDS).unwrap(), request);
        let mut reply = request.to_vec();
        reply.extend_from_slice(b" done");
        server.send(&reply, NO_WAIT).unwrap();
    }
    assert_eq!(receive_all(&mut client, 3), [&b"a done"[..], b"bb done", b"ccc done"]);

    thread::scope(|s| {
        s.spawn(|| answer(&mut server));
        assert_eq!(client.call(b"e", TEN_SECONDS).unwrap(), b"e done"); // nothing owed now
    });
    assert_eq!(server.send_many(&requests, NO_WAIT).unwrap_err().error, Error::Invalid);
    assert_eq!(server.receive_many(&mut [0; 64], &mut [0; 4], NO_WAIT), Err(Error::Invalid));

    // A batch whose requests don't all fit counts only those that went: once their replies are received, a call goes
    // again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
    let partial = client.send_many(&[&b"f"[..]; 200], NO_WAIT).unwrap_err();
    assert_eq!(partial.error, Error::Timeout);
    assert!((1..=128).contains(&partial.sent), "{partial}");
    for _ in 0..partial.sent {
        let request = server.receive(TEN_SECONDS).unwrap();
        server.send(&request, NO_WAIT).unwrap();
    }
    assert_eq!(receive_all(&mut client, partial.sent).len(), partial.sent);
    thread::scope(|s| {
        s.spawn(|| answer(&mut server));
        assert_eq!(client.call(b"g", TEN_SECONDS).unwrap(), b"g done");
    });
}
