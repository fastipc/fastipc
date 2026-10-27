//! The service in one process: many clients and one server, a pool of server threads that hold requests at once, each
//! client's replies in the order of its requests, Abandoned when a server is dropped holding a request, the calls each
//! role refuses, the zero-copy calls on both roles (a request held in place and replied to in place), a request replied
//! to from another thread, timeouts and cancels (a client's call that gives up and the next one, which drops the late
//! reply; a call after a cancel, which sends nothing), the owner's wait for members and its cancel, the end at the
//! service's close or drop, requests longer than a receive's first buffer, a service's misuses. Clients and servers in
//! other processes are in process.rs, interop.rs and examples.rs.

mod common;

use std::collections::HashMap;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use common::{TEN_SECONDS, answer_until_the_end, unique_name};
use fipc::{Connection, Error, FOREVER, MembersError, NO_WAIT, QueueRole, ServiceRole, SubscriberRole};

/// `count` members of `role` of the service on `name`, joined one after the other.
fn join(name: &str, role: ServiceRole, count: usize) -> Vec<Connection> {
    (0..count).map(|_| fipc::service::join(name, role, TEN_SECONDS).unwrap()).collect()
}

/// Answers the next request with its uppercase.
fn answer_one(server: &mut Connection) {
    let request = server.receive(TEN_SECONDS).unwrap();
    server.send(&request.to_ascii_uppercase(), None).unwrap();
}

/// Answers each request with its uppercase until the service ends: copied (way 0), into a buffer (1), or in place
/// (2, replying in place too); returns the requests it answered.
fn answer_each_way(server: &mut Connection, way: usize) -> Vec<Vec<u8>> {
    let mut answered = Vec::new();
    let mut buf = vec![0; server.max_piece()];
    loop {
        let result = match way {
            0 => server.receive(TEN_SECONDS).and_then(|request| {
                server.send(&request.to_ascii_uppercase(), None)?;
                Ok(request)
            }),
            1 => server.receive_into(&mut buf, TEN_SECONDS).and_then(|len| {
                let request = buf[..len].to_vec();
                server.send(&request.to_ascii_uppercase(), None)?;
                Ok(request)
            }),
            _ => server.receive_request(TEN_SECONDS).and_then(|request| {
                let copy = request.to_vec();
                request.reply_in_place(copy.len(), |request, reply| {
                    reply.copy_from_slice(request); // the request in place stays valid until the reply's commit
                    reply.make_ascii_uppercase();
                    request.len()
                })?;
                Ok(copy)
            }),
        };
        match result {
            Ok(request) => answered.push(request),
            Err(Error::Disconnected) => return answered,
            Err(e) => panic!("server (way {way}): {e}"),
        }
    }
}

#[test]
fn many_clients_and_one_server_each_reply_to_its_own_client_in_order() {
    const CLIENTS: usize = 8;
    const COUNT: usize = 300;
    let name = unique_name("s_many");
    let service = fipc::service::create(&name, 4096, 64, CLIENTS as u32, 1).unwrap();
    assert_eq!(service.name(), name);
    let clients = join(&name, ServiceRole::Client, CLIENTS);
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(service.wait_for_members(None, CLIENTS as u32 + 1, TEN_SECONDS), Ok(CLIENTS as u32 + 1));
    assert!(clients.iter().chain([&server]).all(|conn| conn.max_piece() == 64 && conn.name() == name));
    let answered = thread::scope(|s| {
        let answering = s.spawn(|| answer_until_the_end(&mut server));
        let asking: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(k, mut client)| {
                s.spawn(move || {
                    for i in 0..COUNT {
                        let reply = client.call(format!("client {k} request {i}").as_bytes(), TEN_SECONDS).unwrap();
                        assert_eq!(reply, format!("CLIENT {k} REQUEST {i}").as_bytes());
                    }
                })
            })
            .collect();
        for client in asking {
            client.join().unwrap();
        }
        service.close(); // ends the service: the server's receive returns Disconnected
        answering.join().unwrap()
    });
    assert_eq!(answered, (CLIENTS * COUNT) as u64);
}

/// Four servers on threads and four clients that send at once: the servers each take a request and wait at a barrier
/// before replying, so all four hold one at the same time; then they answer the rest, each its own way (copied, into a
/// buffer, in place), each request once, and each client receives its replies in the order of its requests (each
/// sends several before it receives, from one thread, then receives them on another: its halves).
#[test]
fn a_pool_of_server_threads_holds_requests_at_once_and_answers_each_once() {
    const COUNT: usize = 100;
    let name = unique_name("s_pool");
    let service = fipc::service::create(&name, 1 << 14, 64, 4, 4).unwrap();
    let servers = join(&name, ServiceRole::Server, 4);
    let clients = join(&name, ServiceRole::Client, 4);
    assert_eq!(service.wait_for_members(ServiceRole::Server, 4, TEN_SECONDS), Ok(4));
    let barrier = Barrier::new(4);
    let answered: Vec<Vec<Vec<u8>>> = thread::scope(|s| {
        let answering: Vec<_> = servers
            .into_iter()
            .enumerate()
            .map(|(k, mut server)| {
                let barrier = &barrier;
                s.spawn(move || {
                    let request = server.receive(TEN_SECONDS).unwrap();
                    barrier.wait(); // every server holds a request
                    server.send(&request.to_ascii_uppercase(), None).unwrap();
                    let mut answered = answer_each_way(&mut server, k % 3);
                    answered.push(request);
                    answered
                })
            })
            .collect();
        let asking: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(k, client)| {
                s.spawn(move || {
                    let (mut tx, mut rx) = client.into_split();
                    for start in (0..COUNT).step_by(10) {
                        // ten requests in flight, sent on this thread, then their ten replies, in order, on another
                        for i in start..start + 10 {
                            tx.send(format!("{k} {i}").as_bytes(), TEN_SECONDS).unwrap();
                        }
                        thread::scope(|s| {
                            s.spawn(|| {
                                for i in start..start + 10 {
                                    assert_eq!(rx.receive(TEN_SECONDS).unwrap(), format!("{k} {i}").as_bytes());
                                }
                            });
                        });
                    }
                })
            })
            .collect();
        for client in asking {
            client.join().unwrap();
        }
        service.close();
        answering.into_iter().map(|server| server.join().unwrap()).collect()
    });
    assert!(answered.iter().all(|requests| !requests.is_empty()), "a server answered nothing");
    let mut times: HashMap<Vec<u8>, usize> = HashMap::new();
    for request in answered.into_iter().flatten() {
        *times.entry(request).or_default() += 1;
    }
    assert_eq!(times.len(), 4 * COUNT, "requests lost");
    assert!(times.values().all(|&n| n == 1), "a request answered twice");
    assert!((0..4).all(|k| (0..COUNT).all(|i| times.contains_key(format!("{k} {i}").as_bytes()))));
}

#[test]
fn a_clients_replies_come_in_the_order_of_its_requests_whoever_replies_first() {
    let name = unique_name("s_order");
    let _service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut slow = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut fast = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    client.send(b"first", TEN_SECONDS).unwrap();
    client.send(b"second", TEN_SECONDS).unwrap();
    assert_eq!(slow.receive(TEN_SECONDS).unwrap(), b"first"); // each client's requests are taken in the order sent
    assert_eq!(fast.receive(TEN_SECONDS).unwrap(), b"second");
    fast.send(b"SECOND", None).unwrap();
    assert_eq!(client.receive(Duration::from_millis(50)), Err(Error::Timeout)); // the first request's reply comes first
    slow.send(b"FIRST", None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"FIRST");
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"SECOND");
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_server_dropped_holding_a_request_abandons_it_and_the_next_request_is_answered() {
    let name = unique_name("s_abandoned");
    let _service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut other = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut quitter = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    client.send(b"one", TEN_SECONDS).unwrap();
    assert_eq!(quitter.receive(TEN_SECONDS).unwrap(), b"one");
    drop(quitter); // holding it: the request is answered Abandoned
    assert_eq!(client.receive(TEN_SECONDS), Err(Error::Abandoned));
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // answered once: nothing more is owed

    // A request held in place is abandoned the same way, and call returns Abandoned too: the request counts as
    // answered, and the next call gets its own reply
    let mut quitter = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    thread::scope(|s| {
        let calling = s.spawn(|| client.call(b"two", TEN_SECONDS));
        let request = quitter.receive_request(TEN_SECONDS).unwrap();
        assert_eq!(&*request, b"two");
        drop(request); // still held, until the connection's next send or its drop
        drop(quitter);
        assert_eq!(calling.join().unwrap(), Err(Error::Abandoned));
    });
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut other));
        assert_eq!(client.call(b"three", TEN_SECONDS).unwrap(), b"THREE");
    });
}

#[test]
fn each_roles_calls_what_it_cant_use_is_invalid_and_max_piece_is_max_message() {
    let name = unique_name("s_roles");
    let service = fipc::service::create(&name, 1 << 14, 1000, 1, 1).unwrap();
    // The owner's thread joins both: the library admits them
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!((client.max_piece(), server.max_piece()), (1000, 1000));
    assert_eq!(client.send(&[0; 1001], NO_WAIT), Err(Error::TooLarge { len: 1001 }));
    assert_eq!(client.send(b"", NO_WAIT), Err(Error::Invalid));
    assert_eq!(client.send_acquire(1001, NO_WAIT).unwrap_err(), Error::TooLarge { len: 1001 });
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // no reply to come
    assert_eq!(client.receive_request(NO_WAIT).unwrap_err(), Error::Invalid); // a server's
    assert_eq!(server.send(b"x", NO_WAIT), Err(Error::Invalid)); // no request held
    assert_eq!(server.send_acquire(1, NO_WAIT).unwrap_err(), Error::Invalid);
    assert_eq!(server.receive(NO_WAIT), Err(Error::Timeout));
    assert_eq!(server.call(b"x", NO_WAIT), Err(Error::Invalid)); // a client's
    for member in [&client, &server] {
        assert_eq!(member.wait_for_members(0, NO_WAIT), Err(MembersError { error: Error::Invalid, count: 0 }));
    }
    assert_eq!(fipc::service::create(&name, 4096, 64, 1, 1).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::queue::create(&name, 4096, 1, 1).unwrap_err(), Error::AddrInUse);

    // A server holds one request: its receives are Invalid until it replies; a slot's drop doesn't let it go
    client.send(&[7; 1000], TEN_SECONDS).unwrap();
    assert_eq!(server.receive_into(&mut [0; 999], TEN_SECONDS), Err(Error::TooLarge { len: 1000 })); // takes nothing
    let slot = server.receive_acquire(TEN_SECONDS).unwrap();
    assert_eq!(&*slot, [7; 1000]);
    drop(slot); // the request is still held
    assert_eq!(server.receive(NO_WAIT), Err(Error::Invalid));
    assert_eq!(server.receive_acquire(NO_WAIT).unwrap_err(), Error::Invalid);
    assert_eq!(server.receive_request(NO_WAIT).unwrap_err(), Error::Invalid);
    assert_eq!(server.send(&[0; 1001], None), Err(Error::TooLarge { len: 1001 }));
    let mut reply = server.send_acquire(1000, NO_WAIT).unwrap();
    reply.fill(3);
    assert_eq!(reply.commit(1001), Err(Error::Invalid)); // the reservation is dropped with the slot: nothing sent
    server.send(&[3; 1000], None).unwrap(); // replies to the held request
    assert_eq!(server.send(b"x", None), Err(Error::Invalid)); // nothing held any more
    assert_eq!(client.receive_into(&mut [0; 10], TEN_SECONDS), Err(Error::TooLarge { len: 1000 })); // takes nothing
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), [3; 1000]);

    // In place both ways: a request committed in place, held in place by the server, which replies in place reading
    // it, and its reply received in place
    let mut slot = client.send_acquire(5, TEN_SECONDS).unwrap();
    slot.copy_from_slice(b"hello");
    slot.commit(5).unwrap();
    let request = server.receive_request(TEN_SECONDS).unwrap();
    assert_eq!(format!("{request:?}"), "Request { len: 5, .. }");
    request
        .reply_in_place(5, |request, reply| {
            reply.copy_from_slice(request);
            reply.make_ascii_uppercase();
            assert_eq!(request, b"hello"); // whole until the commit
            5
        })
        .unwrap();
    let reply = client.receive_acquire(TEN_SECONDS).unwrap();
    assert_eq!(&*reply, b"HELLO");
    drop(reply);

    // A reply in place of a length out of range sends nothing, and the request stays held
    client.send(b"again", TEN_SECONDS).unwrap();
    let request = server.receive_request(TEN_SECONDS).unwrap();
    assert_eq!(request.reply_in_place(5, |_, _| 6), Err(Error::Invalid));
    assert_eq!(server.receive_request(NO_WAIT).unwrap_err(), Error::Invalid); // still held
    server.send(b"AGAIN", None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"AGAIN");
    client.send(b"x", TEN_SECONDS).unwrap();
    let request = server.receive_request(TEN_SECONDS).unwrap();
    assert_eq!(request.reply(&[0; 1001]), Err(Error::TooLarge { len: 1001 })); // nothing sent: still held
    server.send(b"X", None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"X");

    // A server split into its halves receives copies only: its Sender could reply while a slot lives
    let (tx, rx) = server.split();
    assert_eq!(rx.receive_acquire(NO_WAIT).unwrap_err(), Error::Invalid);
    // SAFETY: not a gated subscriber; the call is refused before it receives.
    assert_eq!(unsafe { rx.receive_acquire_gated(NO_WAIT) }.unwrap_err(), Error::Invalid);
    client.send(b"split", TEN_SECONDS).unwrap();
    let request = rx.receive(TEN_SECONDS).unwrap();
    tx.send(&request.to_ascii_uppercase(), None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"SPLIT");
    assert_eq!(service.wait_for_members(ServiceRole::Client, 0, NO_WAIT), Ok(1));
    assert_eq!(service.wait_for_members(ServiceRole::Server, 0, NO_WAIT), Ok(1));
}

/// A server receives a request in place and hands it to another thread, which replies; a server split into its
/// halves receives on one thread and replies on another.
#[test]
fn a_request_replied_to_from_another_thread() {
    let name = unique_name("s_threads");
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    client.send(b"in place", TEN_SECONDS).unwrap();
    thread::scope(|s| {
        let request = server.receive_request(TEN_SECONDS).unwrap();
        s.spawn(move || {
            let reply = request.to_ascii_uppercase();
            request.reply(&reply).unwrap();
        });
    });
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"IN PLACE");

    // The receiving thread's next receive waits for the reply: until then, the server holds the request, and its
    // receives return Invalid
    let (mut tx, mut rx) = server.into_split();
    let (to_reply, requests) = std::sync::mpsc::channel::<Vec<u8>>();
    let (to_receive, replied) = std::sync::mpsc::channel::<()>();
    let replying = thread::spawn(move || {
        for request in requests {
            tx.send(&request.to_ascii_uppercase(), None).unwrap();
            to_receive.send(()).unwrap();
        }
    });
    let receiving = thread::spawn(move || {
        for _ in 0..3 {
            to_reply.send(rx.receive(TEN_SECONDS).unwrap()).unwrap();
            replied.recv().unwrap();
        }
    });
    for i in 0..3 {
        let request = format!("request {i}");
        assert_eq!(client.call(request.as_bytes(), TEN_SECONDS).unwrap(), request.to_ascii_uppercase().as_bytes());
    }
    receiving.join().unwrap();
    replying.join().unwrap();
}

/// A call whose receive timed out or was cancelled leaves its reply to come; the next call receives and drops it
/// (within its own timeout) before it sends, so it returns the reply to its own request. A call refuses (Invalid) while
/// a request sent with send or a slot's commit still has its reply to be received.
#[test]
fn a_call_that_gives_up_leaves_its_reply_owed_and_the_next_call_drops_it() {
    let name = unique_name("s_owed");
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"late", Duration::from_millis(50)), Err(Error::Timeout)); // went: no reply in time
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"late");
    server.send(b"LATE", None).unwrap(); // the late reply, owed to the call that gave up

    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT"); // not LATE
    });
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // nothing more owed

    // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
    assert_eq!(client.call(b"a", NO_WAIT), Err(Error::Timeout)); // a went; its reply is owed
    assert_eq!(client.call(b"b", Duration::from_millis(50)), Err(Error::Timeout)); // a's reply isn't there: no b
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"a");
    server.send(b"A", None).unwrap();
    assert_eq!(server.receive(NO_WAIT), Err(Error::Timeout)); // no b
    thread::scope(|s| {
        let answering = s.spawn(|| server.receive(TEN_SECONDS));
        assert_eq!(client.call(b"c", Duration::from_millis(200)), Err(Error::Timeout)); // drops A, sends c, gives up
        assert_eq!(answering.join().unwrap().unwrap(), b"c");
    });
    server.send(b"C", None).unwrap(); // owed to the call that gave up
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"C"); // a receive of the caller's own takes it, in request order
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout));

    // A request of send whose reply hasn't been received: call refuses, and leaves it for the caller's receive
    client.send(b"mine", TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Invalid));
    answer_one(&mut server);
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"MINE");
    // ... and so does one of a slot's commit, received in place
    let mut slot = client.send_acquire(4, TEN_SECONDS).unwrap();
    slot.copy_from_slice(b"slot");
    slot.commit(4).unwrap();
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Invalid));
    answer_one(&mut server);
    assert_eq!(&*client.receive_acquire(TEN_SECONDS).unwrap(), b"SLOT");
    // A send that fails owes nothing
    assert_eq!(client.send(b"", NO_WAIT), Err(Error::Invalid));
    assert_eq!(client.call(b"", NO_WAIT), Err(Error::Invalid)); // an empty request: refused, nothing owed
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"x", TEN_SECONDS).unwrap(), b"X");
    });
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_call_whose_timeout_runs_out_while_it_drops_an_owed_reply_never_sends_its_request() {
    let name = unique_name("s_deadline");
    let _service = fipc::service::create(&name, 65536, 8192, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"late", Duration::from_millis(50)), Err(Error::Timeout)); // went: no reply in time
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"late");
    server.send(&[b'L'; 8192], None).unwrap(); // the late reply has come once the send returns
    // A timeout of 1 ns: the call drops the owed reply (there, so its receive needn't wait), and by then the timeout
    // has run out, so its request isn't sent
    assert_eq!(client.call(b"never", Duration::from_nanos(1)), Err(Error::Timeout));
    assert_eq!(server.receive(Duration::from_millis(50)), Err(Error::Timeout)); // the server never saw it
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // nothing is owed: the late reply was dropped
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT");
    });

    // A zero timeout waits at no step: with the owed reply there, the call drops it and sends
    assert_eq!(client.call(b"a", NO_WAIT), Err(Error::Timeout)); // a went; its reply is owed
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"a");
    server.send(b"A", None).unwrap();
    assert_eq!(client.call(b"b", NO_WAIT), Err(Error::Timeout)); // drops A, sends b, its reply not there yet
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"b");
    server.send(b"B", None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"B"); // a plain receive takes the late reply first
}

#[test]
fn a_call_checks_its_request_at_once_even_while_a_reply_is_owed() {
    let name = unique_name("s_check");
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"held", Duration::from_millis(50)), Err(Error::Timeout)); // its reply is owed
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"held"); // held, not replied to: the owed reply doesn't come
    let started = Instant::now();
    assert_eq!(client.call(b"", FOREVER), Err(Error::Invalid));
    let too_long = vec![b'x'; client.max_piece() + 1];
    assert_eq!(client.call(&too_long, FOREVER), Err(Error::TooLarge { len: too_long.len() }));
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed()); // neither waited
    server.send(b"HELD", None).unwrap(); // still owed: the next call drops it and returns its own reply
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT");
    });
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout));
}

#[test]
fn a_call_that_gave_up_on_a_request_its_server_abandons_leaves_an_abandoned_owed_and_the_next_call_drops_it() {
    let name = unique_name("s_owed_abandoned");
    let _service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut quitter = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"lost", Duration::from_millis(50)), Err(Error::Timeout)); // went: no reply in time
    assert_eq!(quitter.receive(TEN_SECONDS).unwrap(), b"lost");
    drop(quitter); // holding it: the request is answered Abandoned, owed to the call that gave up
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"next", TEN_SECONDS).unwrap(), b"NEXT"); // drops the Abandoned, gets its own
    });
    assert_eq!(client.receive(NO_WAIT), Err(Error::Timeout)); // nothing more owed
}

#[test]
fn a_call_after_the_services_end() {
    // Disconnected from the drop of an owed reply that never came
    let name = unique_name("s_end_owed");
    let service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    assert_eq!(client.call(b"held", Duration::from_millis(50)), Err(Error::Timeout)); // its reply is owed
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"held");
    service.close();
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Disconnected));

    // Disconnected from the send, with nothing owed
    let name = unique_name("s_end_send");
    let service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let _server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    service.close();
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(client.call(b"", TEN_SECONDS), Err(Error::Invalid)); // the request is checked first, end or not

    // Invalid with a reply owed to a request of send's
    let name = unique_name("s_end_sent");
    let service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let _server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    client.send(b"mine", TEN_SECONDS).unwrap();
    service.close();
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Invalid));
    assert_eq!(client.receive(TEN_SECONDS), Err(Error::Disconnected)); // that reply never comes
}

#[test]
fn a_cancel_ends_a_clients_and_a_servers_wait_and_a_cancelled_call_leaves_its_reply_owed() {
    let name = unique_name("s_cancel");
    let _service = fipc::service::create(&name, 4096, 64, 2, 1).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let canceler = client.canceler();
    thread::scope(|s| {
        let calling = s.spawn(|| client.call(b"cancelled", FOREVER));
        assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"cancelled");
        canceler.cancel(); // from another thread: the call's wait for its reply returns Cancelled
        assert_eq!(calling.join().unwrap(), Err(Error::Cancelled));
    });
    server.send(b"CANCELLED", None).unwrap(); // owed to the cancelled call
    assert_eq!(client.receive(NO_WAIT).unwrap(), b"CANCELLED"); // a receive that needn't wait still works
    assert_eq!(client.receive(TEN_SECONDS), Err(Error::Cancelled)); // final
    drop(client);

    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap(); // the server goes on
    let canceler = server.canceler();
    thread::scope(|s| {
        let waiting = s.spawn(|| server.receive(FOREVER));
        canceler.cancel();
        assert_eq!(waiting.join().unwrap(), Err(Error::Cancelled));
    });
    client.send(b"after", TEN_SECONDS).unwrap();
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), b"after"); // a request there: the receive needn't wait
    server.send(b"AFTER", None).unwrap(); // a reply never waits
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"AFTER");
    assert_eq!(server.receive(TEN_SECONDS), Err(Error::Cancelled)); // final
}

#[test]
fn a_call_after_a_cancel_returns_cancelled_and_sends_nothing() {
    let name = unique_name("s_call_cancelled");
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    client.cancel();
    assert_eq!(client.call(b"", TEN_SECONDS), Err(Error::Invalid)); // the request's checks come first
    assert_eq!(client.call(&[0; 65], TEN_SECONDS), Err(Error::TooLarge { len: 65 }));
    for timeout in [FOREVER, NO_WAIT, Some(TEN_SECONDS)] {
        assert_eq!(client.call(b"never sent", timeout), Err(Error::Cancelled), "timeout {timeout:?}");
    }
    assert_eq!(server.receive(Duration::from_millis(200)), Err(Error::Timeout)); // no request went
}

#[test]
fn the_owners_wait_for_members_and_its_cancel() {
    let name = unique_name("s_members");
    let service = fipc::service::create(&name, 4096, 64, 2, 3).unwrap();
    assert_eq!(service.wait_for_members(None, 0, NO_WAIT), Ok(0));
    assert_eq!(service.wait_for_members(ServiceRole::Client, 0, NO_WAIT), Ok(0));
    let timeout = service.wait_for_members(None, 1, NO_WAIT).unwrap_err();
    assert_eq!(timeout, MembersError { error: Error::Timeout, count: 0 });

    let members = thread::scope(|s| {
        let waiting = s.spawn(|| service.wait_for_members(ServiceRole::Server, 1, TEN_SECONDS)); // while one joins
        let members = [ServiceRole::Server, ServiceRole::Client].map(|role| join(&name, role, 1));
        assert_eq!(waiting.join().unwrap(), Ok(1));
        members
    });
    assert_eq!(service.wait_for_members(None, 2, TEN_SECONDS), Ok(2));
    assert_eq!(service.wait_for_members(Some(ServiceRole::Client), 0, NO_WAIT), Ok(1));
    let short = service.wait_for_members(ServiceRole::Client, 2, Duration::from_millis(50));
    assert_eq!(short, Err(MembersError { error: Error::Timeout, count: 1 }));

    // Over the role's maximum (max_clients, max_servers, or their sum for both)
    for (role, at_least) in [(Some(ServiceRole::Client), 3), (Some(ServiceRole::Server), 4), (None, 6)] {
        let over = service.wait_for_members(role, at_least, NO_WAIT);
        assert_eq!(over, Err(MembersError { error: Error::Invalid, count: 0 }), "{role:?}");
    }

    // A member that leaves is counted until the owner's process has seen its end
    drop(members);
    let mut left = Ok(2);
    for _ in 0..1000 {
        left = service.wait_for_members(None, 0, NO_WAIT);
        if left == Ok(0) {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(left, Ok(0));

    // The cancel, from another thread, ends the wait; the service goes on
    let canceler = service.canceler();
    assert_eq!(format!("{canceler:?}"), "Canceler { target: \"service\" }");
    thread::scope(|s| {
        let waiting = s.spawn(|| service.wait_for_members(ServiceRole::Server, 1, TEN_SECONDS)); // through &service
        canceler.cancel();
        assert_eq!(waiting.join().unwrap(), Err(MembersError { error: Error::Cancelled, count: 0 }));
    });
    assert_eq!(service.wait_for_members(None, 1, TEN_SECONDS), Err(MembersError { error: Error::Cancelled, count: 0 }));
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap(); // joins go on
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    thread::scope(|s| {
        s.spawn(|| answer_one(&mut server));
        assert_eq!(client.call(b"after the cancel", TEN_SECONDS).unwrap(), b"AFTER THE CANCEL");
    });
    assert_eq!(service.wait_for_members(None, 0, NO_WAIT), Ok(2)); // a count that needn't wait still works
}

#[test]
fn the_services_close_ends_it_for_clients_and_servers() {
    let name = unique_name("s_end");
    let service = fipc::service::create(&name, 4096, 64, 1, 2).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let mut holder = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    client.send(b"done", TEN_SECONDS).unwrap();
    client.send(b"held", TEN_SECONDS).unwrap();
    answer_one(&mut server);
    let held = holder.receive_request(TEN_SECONDS).unwrap();
    assert_eq!(&*held, b"held");
    service.close();
    assert_eq!(client.send(b"after", TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(server.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), b"DONE"); // the replies that came before the end, then the end
    assert_eq!(client.receive(TEN_SECONDS), Err(Error::Disconnected));
    assert_eq!(held.reply(b"HELD"), Err(Error::Disconnected)); // a reply after the end
    // The reply owed to the request of send that held took never comes: call refuses, as while any reply owed to a
    // request of send's is to come
    assert_eq!(client.call(b"x", TEN_SECONDS), Err(Error::Invalid));
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap(); // the name is free again at once
}

/// The service's handle is its one owner: dropping it ends the service at a known place (no garbage collector), and a
/// canceler doesn't keep it alive. A client kept beyond the service's scope gets Disconnected.
#[test]
fn the_services_drop_ends_it_and_frees_the_name() {
    let name = unique_name("s_drop");
    let (mut client, canceler) = {
        let service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
        assert_eq!(fipc::service::create(&name, 4096, 64, 1, 1).unwrap_err(), Error::AddrInUse);
        let client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
        (client, service.canceler())
    }; // the service's drop
    assert_eq!(client.send(b"after the end", TEN_SECONDS), Err(Error::Disconnected));
    canceler.cancel(); // does nothing: the service is closed
    let service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap(); // the name is free again
    assert_eq!(format!("{service:?}"), format!("Service {{ name: {name:?}, .. }}"));
    assert!(!service.as_raw().is_null());
}

/// A receive's first buffer is empty, so it asks for the message's length, then takes it into room of that length:
/// requests and replies longer than the ring's first piece arrive whole on both sides.
#[test]
fn requests_longer_than_the_receive_buffer_are_received_whole_on_both_sides() {
    let name = unique_name("s_large");
    let _service = fipc::service::create(&name, 1 << 16, 1 << 14, 1, 1).unwrap();
    let mut client = fipc::service::join(&name, ServiceRole::Client, TEN_SECONDS).unwrap();
    let mut server = fipc::service::join(&name, ServiceRole::Server, TEN_SECONDS).unwrap();
    let request = common::pattern(1 << 14);
    client.send(&request, TEN_SECONDS).unwrap();
    assert_eq!(server.receive(TEN_SECONDS).unwrap(), request);
    let reply: Vec<u8> = request.iter().rev().copied().collect();
    server.send(&reply, None).unwrap();
    assert_eq!(client.receive(TEN_SECONDS).unwrap(), reply);
}

#[test]
fn a_services_misuses() {
    let name = unique_name("s_misuse");
    for (capacity, max_message, clients, servers) in
        [(1000, 64, 1, 1), (4096, 0, 1, 1), (4096, 1025, 1, 1), (4096, 64, 0, 1), (4096, 64, 65, 1), (4096, 64, 1, 0)]
    {
        let bad = fipc::service::create(&name, capacity, max_message, clients, servers);
        assert_eq!(bad.unwrap_err(), Error::Invalid, "{capacity} {max_message} {clients} {servers}");
    }
    assert_eq!(fipc::service::create(&name, 4096, 64, 1, 65).unwrap_err(), Error::Invalid);
    let _service = fipc::service::create(&name, 4096, 64, 1, 1).unwrap();
    assert_eq!(fipc::broadcast::create(&name, 4096, 1, None).unwrap_err(), Error::AddrInUse);
    assert_eq!(fipc::stream::create(&name, 4096).unwrap_err(), Error::AddrInUse);
    for role in [SubscriberRole::Lossy, SubscriberRole::Gated] {
        assert_eq!(fipc::broadcast::join(&name, role, TEN_SECONDS).unwrap_err(), Error::Invalid, "{role:?}");
    }
    for role in [QueueRole::Producer, QueueRole::Consumer] {
        assert_eq!(fipc::queue::join(&name, role, TEN_SECONDS).unwrap_err(), Error::Invalid, "{role:?}");
    }
    assert_eq!(fipc::stream::connect(&name, TEN_SECONDS).unwrap_err(), Error::Invalid); // a service isn't a server
    // The other kinds' roles, through the C API (the binding's ServiceRole has none of them)
    let c_name = std::ffi::CString::new(name.as_str()).unwrap();
    for role in [fipc::sys::FIPC_SUBSCRIBER_GATED, fipc::sys::FIPC_PRODUCER, fipc::sys::FIPC_CONSUMER, 7] {
        let mut out = std::ptr::null_mut();
        // SAFETY: the name is NUL-terminated; out is valid for a write (and stays null: the join fails).
        let code = unsafe { fipc::sys::fipc_service_join(c_name.as_ptr(), role, &mut out, 0) };
        assert_eq!(code, fipc::sys::FIPC_INVALID, "role {role}");
    }

    let queue = unique_name("s_queue");
    let _queue = fipc::queue::create(&queue, 4096, 1, 1).unwrap();
    for role in [ServiceRole::Client, ServiceRole::Server] {
        assert_eq!(fipc::service::join(&queue, role, TEN_SECONDS).unwrap_err(), Error::Invalid, "{role:?}");
    }
    assert_eq!(fipc::service::create("bad name", 4096, 64, 1, 1).unwrap_err(), Error::Invalid);
    assert_eq!(fipc::service::join("bad name", ServiceRole::Client, NO_WAIT).unwrap_err(), Error::Invalid);

    // A stream's connection isn't a service's: call and receive_request refuse
    let mut pair = common::Pair::new("s_stream");
    assert_eq!(pair.client.call(b"x", NO_WAIT), Err(Error::Invalid));
    assert_eq!(pair.server.receive_request(NO_WAIT).unwrap_err(), Error::Invalid);
}
