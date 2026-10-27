// The service in one process: createService, its ranges and the other kinds' roles; eight clients and one server, a
// pool of servers holding requests at once, each client's replies in the order of its requests; FIPC_ABANDONED when a
// server closes holding a request; each role's refused calls and the zero-copy calls on both roles (a server's request
// view lasting until a reply gives the request up, past a reply refused with it held); call's owed replies after a
// timeout or a cancel, and the reply of a send; a call whose time runs out before its send, a call's checks of its
// request, an FIPC_ABANDONED owed to a call and a call after the end whose owed reply never came; a call on a cancelled
// connection, which sends nothing; concurrent calls, run one at a time, and a queued call's timeout, cancel and close;
// the owner's count of members and its cancel; the end at the service's close; and the examples' exchange on one
// thread. The owner's process admits the members by itself and a join waits on a thread of its own, so an owner and its
// members can share a process, and a thread (joinServiceSync). Other processes and the Python binding:
// tests/processes.test.mjs and tests/interop.test.mjs; a service the program drops (the garbage collector never closes
// it) and worker threads: tests/lifetime.test.mjs.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  CLIENT,
  CONSUMER,
  FOREVER,
  FipcError,
  NO_WAIT,
  PRODUCER,
  SERVER,
  SUBSCRIBER_GATED,
  SUBSCRIBER_LOSSY,
  Service,
  TEN_SECONDS,
  closeAll,
  connectStream,
  createBroadcast,
  createQueue,
  createService,
  createStream,
  joinBroadcast,
  joinQueue,
  joinService,
  joinServiceSync,
  pair,
  rejectsWith,
  sleep,
  throwsWith,
  uniqueName,
} from './support.mjs';
import { answerUntilTheEnd, joinAndCall, upper } from './service.mjs';

const SLOW = { timeout: 120000 };

/** Keeps this thread busy for `ms` milliseconds: what completes meanwhile is seen only after */
function busyWait(ms) {
  const end = performance.now() + ms;
  while (performance.now() < end);
}

/** Answers one request with its uppercase */
async function answerOne(server) {
  await server.send(upper(await server.receive(TEN_SECONDS)));
}

/** Asserts that `fn` throws (or `promise` rejects with) an Error of the code ERR_FIPC_BUSY */
function throwsBusy(fn) {
  assert.throws(fn, { code: 'ERR_FIPC_BUSY' });
}

async function rejectsBusy(promise) {
  await assert.rejects(promise, { code: 'ERR_FIPC_BUSY' });
}

/** Asserts that every call of `calls` fails with FIPC_CANCELLED (all of them settled first: none rejects unhandled) */
async function allCancelled(calls) {
  for (const outcome of await Promise.allSettled(calls)) {
    assert.equal(outcome.status, 'rejected');
    assert.ok(outcome.reason instanceof FipcError, `expected a FipcError, got ${outcome.reason}`);
    assert.equal(outcome.reason.code, 'FIPC_CANCELLED');
    assert.equal(outcome.reason.call, 'call');
  }
}

describe('service', () => {
  test('createService\'s ranges, the name, and the other kinds\' roles and names', async () => {
    const name = uniqueName('s_api');
    for (const args of [[1000, 64, 1, 1], [4096, 0, 1, 1], [4096, 1025, 1, 1], [4096, 64, 0, 1], [4096, 64, 65, 1],
      [4096, 64, 1, 0], [4096, 64, 1, 65]]) {
      const error = throwsWith(assert, () => createService(name, ...args), 'FIPC_INVALID');
      assert.equal(error.call, 'createService');
    }
    assert.throws(() => createService(name, 4096, '64', 1, 1), TypeError);
    assert.throws(() => createService(name, 4096, 64, 1.5, 1), RangeError);
    assert.throws(() => createService(42, 4096, 64, 1, 1), TypeError);
    assert.equal(CLIENT, 5);
    assert.equal(SERVER, 6);

    const service = createService(name, 4096, 64, 1, 1);
    assert.ok(service instanceof Service);
    assert.equal(service.name, name);
    assert.equal(service.closed, false);
    for (const create of [() => createService(name, 4096, 64, 1, 1), () => createQueue(name, 4096, 1, 1),
      () => createBroadcast(name, 4096, 1), () => createStream(name, 4096)]) {
      throwsWith(assert, create, 'FIPC_ADDR_IN_USE');
    }
    for (const role of [SUBSCRIBER_LOSSY, SUBSCRIBER_GATED, PRODUCER, CONSUMER, 7]) { // no role of a service's
      throwsWith(assert, () => joinServiceSync(name, role, NO_WAIT), 'FIPC_INVALID');
    }
    await rejectsWith(assert, joinService(name, PRODUCER, TEN_SECONDS), 'FIPC_INVALID');
    assert.throws(() => joinServiceSync(name, 'client'), TypeError);
    // A service's name refuses the other kinds' joins, its roles included; the owner's admission goes on
    await rejectsWith(assert, joinBroadcast(name, SUBSCRIBER_GATED, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinQueue(name, CONSUMER, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinQueue(name, CLIENT, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, connectStream(name, TEN_SECONDS), 'FIPC_INVALID');
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    await rejectsWith(assert, joinService(name, CLIENT, 100), 'FIPC_TIMEOUT'); // the one client's place is held
    closeAll(client, service);
    assert.equal(service.closed, true);
    service.close(); // again: nothing
    assert.throws(() => service.waitForMembersSync(), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => service.cancel(), { code: 'ERR_FIPC_CLOSED' });
    createService(name, 4096, 64, 1, 1).close(); // the name is free again at once

    // A queue's and a broadcast's names refuse a service's roles
    const queue = createQueue(uniqueName('s_api_queue'), 4096, 1, 1);
    const broadcastName = uniqueName('s_api_bc');
    const publisher = createBroadcast(broadcastName, 4096, 1);
    for (const role of [CLIENT, SERVER]) {
      await rejectsWith(assert, joinService(queue.name, role, TEN_SECONDS), 'FIPC_INVALID');
      await rejectsWith(assert, joinService(broadcastName, role, TEN_SECONDS), 'FIPC_INVALID');
    }
    await rejectsWith(assert, joinBroadcast(broadcastName, CLIENT, TEN_SECONDS), 'FIPC_INVALID');
    closeAll(queue, publisher);
  });

  test('eight clients and one server: each reply goes to its own client, in order', SLOW, async () => {
    const name = uniqueName('s_many');
    const COUNT = 300;
    const service = createService(name, 4096, 64, 8, 1);
    const clients = await Promise.all([...Array(8)].map(() => joinService(name, CLIENT, TEN_SECONDS)));
    const server = joinServiceSync(name, SERVER, TEN_SECONDS);
    assert.equal(await service.waitForMembers(undefined, 9, TEN_SECONDS), 9);
    for (const conn of [...clients, server]) assert.equal(conn.maxPiece, 64);
    const answering = answerUntilTheEnd(server);
    try {
      await Promise.all(clients.map(async (client, k) => {
        for (let i = 0; i < COUNT; i++) {
          const text = `client ${k} request ${i}`;
          assert.equal((await client.call(text, TEN_SECONDS)).toString(), text.toUpperCase());
        }
      }));
    } finally {
      service.close(); // the server answers until the end, whatever the calls came to (as in the pool's test)
      await Promise.allSettled([answering]);
      closeAll(server, ...clients);
    }
    assert.equal(await answering, 8 * COUNT);
  });

  test('a pool of four servers holds requests at once, and answers each once', SLOW, async () => {
    const name = uniqueName('s_pool');
    const service = createService(name, 4096, 64, 1, 4);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const servers = await Promise.all([...Array(4)].map(() => joinService(name, SERVER, TEN_SECONDS)));
    // Each server takes one request and holds it: four requests are held at once, on four threads
    const taking = servers.map((server) => server.receive(TEN_SECONDS));
    for (let i = 0; i < 4; i++) await client.send(`request ${i}`, TEN_SECONDS);
    const held = (await Promise.all(taking)).map(String);
    assert.deepEqual([...held].sort(), ['request 0', 'request 1', 'request 2', 'request 3']);
    for (const server of servers) await rejectsWith(assert, server.receive(NO_WAIT), 'FIPC_INVALID'); // holding
    for (let s = 3; s >= 0; s--) servers[s].sendSync(held[s].toUpperCase()); // the replies, last server first
    for (let i = 0; i < 4; i++) assert.equal((await client.receive(TEN_SECONDS)).toString(), `REQUEST ${i}`);
    await rejectsWith(assert, client.receive(NO_WAIT), 'FIPC_TIMEOUT');

    // Then a stream of calls, the pool sharing them: each answered once
    const counts = servers.map(() => 0);
    const answering = servers.map(async (server, s) => (counts[s] = await answerUntilTheEnd(server)));
    try {
      for (let i = 0; i < 400; i++) assert.equal((await client.call(`job ${i}`, TEN_SECONDS)).toString(), `JOB ${i}`);
    } finally {
      // The servers answer until the end, whatever the calls came to: none of their receives is left to fail later,
      // unhandled, once this test has failed
      service.close();
      await Promise.allSettled(answering);
      closeAll(client, ...servers);
    }
    await Promise.all(answering); // a server's error, if one failed
    assert.equal(counts.reduce((a, b) => a + b), 400);
  });

  test('a client\'s replies come in the order of its requests, whichever server replies first', async () => {
    const name = uniqueName('s_order');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const slow = await joinService(name, SERVER, TEN_SECONDS);
    const fast = await joinService(name, SERVER, TEN_SECONDS);
    await client.send('first', TEN_SECONDS);
    assert.equal((await slow.receive(TEN_SECONDS)).toString(), 'first');
    await client.send('second', TEN_SECONDS);
    assert.equal((await fast.receive(TEN_SECONDS)).toString(), 'second');
    await fast.send('SECOND');
    await rejectsWith(assert, client.receive(50), 'FIPC_TIMEOUT'); // the reply to the first comes first
    await slow.send('FIRST');
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'FIRST');
    assert.equal(client.receiveSync(TEN_SECONDS).toString(), 'SECOND');
    throwsWith(assert, () => client.receiveSync(NO_WAIT), 'FIPC_TIMEOUT');
    closeAll(client, slow, fast, service);
  });

  test('a server that closes holding a request abandons it; the next request is answered', async () => {
    const name = uniqueName('s_abandoned');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const other = await joinService(name, SERVER, TEN_SECONDS);
    // Each way of receiving gets FIPC_ABANDONED, with no length, and the request counts as answered
    const receives = [
      (timeout) => client.receive(timeout),
      (timeout) => client.receiveInto(Buffer.alloc(64), timeout),
      (timeout) => client.receiveAcquire(timeout),
      async (timeout) => client.receiveSync(timeout),
    ];
    for (const receive of receives) {
      const quitter = await joinService(name, SERVER, TEN_SECONDS);
      await client.send('one', TEN_SECONDS);
      assert.equal((await quitter.receive(TEN_SECONDS)).toString(), 'one');
      quitter.close(); // holding it: the request is answered FIPC_ABANDONED
      const error = await rejectsWith(assert, receive(TEN_SECONDS), 'FIPC_ABANDONED');
      assert.equal('length' in error || 'lost' in error || 'count' in error, false);
      await rejectsWith(assert, client.receive(NO_WAIT), 'FIPC_TIMEOUT'); // answered once: nothing more is owed
    }

    // call() fails with FIPC_ABANDONED too, and the request counts as answered: the next call gets its own reply
    const quitter = await joinService(name, SERVER, TEN_SECONDS);
    const calling = client.call('two', TEN_SECONDS);
    assert.equal((await quitter.receive(TEN_SECONDS)).toString(), 'two');
    quitter.close();
    assert.equal((await rejectsWith(assert, calling, 'FIPC_ABANDONED')).call, 'call');
    const answering = answerOne(other);
    assert.equal((await client.call('three', TEN_SECONDS)).toString(), 'THREE');
    await answering;
    closeAll(client, other, service);
  });

  test('each role\'s calls: what it can\'t use is FIPC_INVALID; maxPiece is maxMessage; zero-copy both ways',
    async () => {
      const name = uniqueName('s_roles');
      const service = createService(name, 1 << 14, 1000, 1, 1);
      const client = joinServiceSync(name, CLIENT, TEN_SECONDS); // the owner's thread joins both: the library admits
      const server = joinServiceSync(name, SERVER, TEN_SECONDS);
      assert.equal(client.maxPiece, 1000);
      assert.equal(server.maxPiece, 1000);
      throwsWith(assert, () => client.sendSync(Buffer.alloc(1001)), 'FIPC_TOO_LARGE');
      await rejectsWith(assert, client.send(Buffer.alloc(1001)), 'FIPC_TOO_LARGE');
      throwsWith(assert, () => client.sendSync(''), 'FIPC_INVALID'); // an empty request
      throwsWith(assert, () => client.sendAcquireSync(1001), 'FIPC_TOO_LARGE');
      throwsWith(assert, () => client.sendCommit(1), 'FIPC_INVALID'); // no reservation
      throwsWith(assert, () => client.receiveSync(NO_WAIT), 'FIPC_TIMEOUT'); // no reply to come: nothing was counted
      throwsWith(assert, () => client.waitForMembersSync(), 'FIPC_INVALID'); // a broadcast publisher's call
      client.receiveRelease(); // nothing held: does nothing
      throwsWith(assert, () => server.sendSync('x'), 'FIPC_INVALID'); // no request held
      throwsWith(assert, () => server.sendAcquireSync(1), 'FIPC_INVALID');
      throwsWith(assert, () => server.sendCommit(1), 'FIPC_INVALID');
      throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_TIMEOUT');
      throwsWith(assert, () => server.waitForMembersSync(), 'FIPC_INVALID');
      await rejectsWith(assert, server.call('x'), 'FIPC_INVALID'); // call is a client's
      throwsWith(assert, () => server.callSync('x'), 'FIPC_INVALID');

      // A server holds one request: its receives are FIPC_INVALID until it replies, and its view lasts until then
      await client.send(Buffer.alloc(1000, 7), TEN_SECONDS);
      const tooLarge = await rejectsWith(assert, server.receiveInto(Buffer.alloc(999), TEN_SECONDS), 'FIPC_TOO_LARGE');
      assert.equal(tooLarge.length, 1000); // it takes nothing
      const view = await server.receiveAcquire(TEN_SECONDS);
      assert.equal(view.length, 1000);
      throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_INVALID');
      await rejectsWith(assert, server.receiveAcquire(NO_WAIT), 'FIPC_INVALID');
      server.receiveRelease(); // does nothing: the request is still held, its view still valid
      assert.equal(view.length, 1000);
      throwsWith(assert, () => server.sendSync(Buffer.alloc(1001)), 'FIPC_TOO_LARGE');
      const room = server.sendAcquireSync(1000);
      room.fill(3);
      assert.ok(Buffer.from(view).equals(Buffer.alloc(1000, 7)), 'the request in place stays whole until the commit');
      throwsWith(assert, () => server.sendCommit(1001), 'FIPC_INVALID');
      server.sendCommit(1000);
      assert.equal(view.length, 0, 'the reply ends the request\'s view');
      assert.equal(room.length, 0);
      throwsWith(assert, () => server.sendSync('x'), 'FIPC_INVALID'); // nothing held any more
      const short = throwsWith(assert, () => client.receiveIntoSync(Buffer.alloc(10), TEN_SECONDS), 'FIPC_TOO_LARGE');
      assert.equal(short.length, 1000); // it takes nothing
      assert.ok((await client.receive(TEN_SECONDS)).equals(Buffer.alloc(1000, 3)));

      // The zero-copy calls the other way: a request committed in place, its reply received in place; a copied reply
      // ends the request's view too
      const slot = await client.sendAcquire(5, TEN_SECONDS);
      slot.set(Buffer.from('hello'));
      client.sendCommit(5);
      const request = server.receiveAcquireSync(TEN_SECONDS);
      assert.equal(Buffer.from(request).toString(), 'hello');
      await server.send(Buffer.from(request).toString().toUpperCase());
      assert.equal(request.length, 0);
      const reply = await client.receiveAcquire(TEN_SECONDS);
      assert.equal(Buffer.from(reply).toString(), 'HELLO');
      client.receiveRelease();
      assert.equal(reply.length, 0);

      // A server's reply while its async receive is pending would answer a request it hasn't seen: ERR_FIPC_BUSY
      const receiving = server.receive(TEN_SECONDS);
      throwsBusy(() => server.sendSync('x'));
      await rejectsBusy(server.send('x'));
      throwsBusy(() => server.sendCommit(1));
      await client.send('late', TEN_SECONDS);
      assert.equal((await receiving).toString(), 'late');
      await server.send('LATE');
      assert.equal((await client.receive(TEN_SECONDS)).toString(), 'LATE');

      assert.equal(service.waitForMembersSync(CLIENT), 1);
      assert.equal(service.waitForMembersSync(SERVER), 1);
      closeAll(client, server, service);
    });

  test('a server\'s request view lasts while a refused reply leaves the request held, until a reply gives it up',
    async () => {
      const name = uniqueName('s_view');
      const service = createService(name, 4096, 64, 1, 1);
      const client = await joinService(name, CLIENT, TEN_SECONDS);
      const server = await joinService(name, SERVER, TEN_SECONDS);
      const holds = (view, text) => assert.equal(Buffer.from(view).toString(), text, 'the request\'s view is whole');

      // Replies the library refuses with the request still held: empty, too large, a commit past its reservation
      await client.send('first', TEN_SECONDS);
      const first = await server.receiveAcquire(TEN_SECONDS);
      await rejectsWith(assert, server.send(''), 'FIPC_INVALID');
      holds(first, 'first');
      throwsWith(assert, () => server.sendSync(new Uint8Array(0)), 'FIPC_INVALID');
      holds(first, 'first');
      await rejectsWith(assert, server.send(Buffer.alloc(65)), 'FIPC_TOO_LARGE');
      throwsWith(assert, () => server.sendSync(Buffer.alloc(65)), 'FIPC_TOO_LARGE');
      holds(first, 'first');
      const room = server.sendAcquireSync(5);
      throwsWith(assert, () => server.sendCommit(6), 'FIPC_INVALID'); // the reservation stays
      holds(first, 'first');
      room.set(Buffer.from('FIRST'));
      server.sendCommit(5); // the reply gives the request up
      assert.equal(first.length, 0);
      assert.equal(room.length, 0);
      assert.equal((await client.receive(TEN_SECONDS)).toString(), 'FIRST');

      // After the service's end a reply is refused before it answers: the request stays held, its view whole, until
      // the close; a commit's reservation is used up, though
      await client.send('second', TEN_SECONDS);
      const second = server.receiveAcquireSync(TEN_SECONDS);
      const late = server.sendAcquireSync(6);
      service.close();
      throwsWith(assert, () => server.sendCommit(6), 'FIPC_DISCONNECTED');
      assert.equal(late.length, 0);
      holds(second, 'second');
      await rejectsWith(assert, server.send('SECOND'), 'FIPC_DISCONNECTED');
      throwsWith(assert, () => server.sendSync('SECOND'), 'FIPC_DISCONNECTED');
      holds(second, 'second');
      server.close();
      assert.equal(second.length, 0);
      closeAll(client);
    });

  test('a call that gives up leaves its reply owed, and the next call drops it', async () => {
    const name = uniqueName('s_owed');
    const service = createService(name, 4096, 64, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const timedOut = await rejectsWith(assert, client.call('late', 50), 'FIPC_TIMEOUT'); // no server replied in time
    assert.equal(timedOut.call, 'call');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'late');
    await server.send('LATE'); // the late reply, owed to the call that gave up
    let answering = answerOne(server);
    assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT'); // not LATE
    await answering;
    await rejectsWith(assert, client.receive(NO_WAIT), 'FIPC_TIMEOUT'); // nothing more owed

    // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
    await rejectsWith(assert, client.call('a', NO_WAIT), 'FIPC_TIMEOUT'); // a went; its reply is owed
    await rejectsWith(assert, client.call('b', 50), 'FIPC_TIMEOUT'); // a's reply hasn't come: b never goes
    assert.equal(server.receiveSync(TEN_SECONDS).toString(), 'a');
    server.sendSync('A');
    throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_TIMEOUT'); // no b
    const taking = server.receive(TEN_SECONDS);
    await rejectsWith(assert, client.call('c', 200), 'FIPC_TIMEOUT'); // drops A, sends c, gives up on its reply
    assert.equal((await taking).toString(), 'c');
    server.sendSync('C'); // owed to the call that gave up
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'C'); // the caller's own receive takes it, in order
    await rejectsWith(assert, client.receive(NO_WAIT), 'FIPC_TIMEOUT');

    // The same with callSync: it drops the reply owed to a call that gave up
    throwsWith(assert, () => client.callSync('d', NO_WAIT), 'FIPC_TIMEOUT');
    assert.equal(server.receiveSync(TEN_SECONDS).toString(), 'd');
    server.sendSync('D');
    const error = throwsWith(assert, () => client.callSync('e', 50), 'FIPC_TIMEOUT'); // drops D, sends e, gives up
    assert.equal(error.call, 'callSync');
    assert.equal(server.receiveSync(TEN_SECONDS).toString(), 'e');
    server.sendSync('E');
    answering = answerOne(server);
    assert.equal((await client.call('f', TEN_SECONDS)).toString(), 'F'); // drops E
    await answering;

    // A request of send's (or sendCommit's) whose reply hasn't been received: call refuses, and leaves it to receive
    await client.send('mine', TEN_SECONDS);
    await rejectsWith(assert, client.call('x', TEN_SECONDS), 'FIPC_INVALID');
    throwsWith(assert, () => client.callSync('x', TEN_SECONDS), 'FIPC_INVALID');
    await answerOne(server);
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'MINE');
    const slot = client.sendAcquireSync(4);
    slot.set(Buffer.from('also'));
    client.sendCommit(4);
    await rejectsWith(assert, client.call('x', TEN_SECONDS), 'FIPC_INVALID');
    await answerOne(server);
    assert.equal(client.receiveSync(TEN_SECONDS).toString(), 'ALSO');

    // A send that fails counts nothing; a call whose send fails leaves nothing owed
    await rejectsWith(assert, client.send(Buffer.alloc(65)), 'FIPC_TOO_LARGE');
    throwsWith(assert, () => client.sendSync(''), 'FIPC_INVALID');
    await rejectsWith(assert, client.call(Buffer.alloc(65), TEN_SECONDS), 'FIPC_TOO_LARGE');
    throwsWith(assert, () => client.callSync('', TEN_SECONDS), 'FIPC_INVALID');
    await assert.rejects(client.call(42), TypeError);
    answering = answerOne(server);
    assert.equal((await client.call('x', TEN_SECONDS)).toString(), 'X');
    await answering;
    closeAll(client, server, service);
  });

  test('a call whose time runs out before its send never sends its request', async () => {
    const name = uniqueName('s_spent');
    const service = createService(name, 1 << 16, 4096, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    await rejectsWith(assert, client.call('late', 50), 'FIPC_TIMEOUT');
    assert.equal(server.receiveSync(TEN_SECONDS).toString(), 'late');
    // The discard's receive gets the owed reply at once, but this thread sees it only after the call's deadline
    const dropping = client.call('never', 100);
    server.sendSync(Buffer.alloc(4096, 'L'));
    busyWait(250);
    assert.equal((await rejectsWith(assert, dropping, 'FIPC_TIMEOUT')).call, 'call');
    throwsWith(assert, () => server.receiveSync(50), 'FIPC_TIMEOUT'); // the server never saw the request
    let answering = answerOne(server);
    assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT'); // the owed reply was dropped
    await answering;

    // A call waiting for its turn whose time runs out: by its timer, or at its turn, late; either way nothing goes
    const first = client.call('first', FOREVER);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'first');
    const queued = client.call('queued', 100);
    server.sendSync('FIRST');
    busyWait(250);
    assert.equal((await first).toString(), 'FIRST');
    await rejectsWith(assert, queued, 'FIPC_TIMEOUT');
    throwsWith(assert, () => server.receiveSync(50), 'FIPC_TIMEOUT');
    answering = answerOne(server);
    assert.equal((await client.call('after', TEN_SECONDS)).toString(), 'AFTER');
    await answering;
    closeAll(client, server, service);
  });

  test('a call checks its request before it waits for anything, even with a reply owed', async () => {
    const name = uniqueName('s_check');
    const service = createService(name, 4096, 64, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    await rejectsWith(assert, client.call('owed', 50), 'FIPC_TIMEOUT');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'owed'); // held: its reply hasn't come
    const started = performance.now();
    assert.equal((await rejectsWith(assert, client.call('', FOREVER), 'FIPC_INVALID')).call, 'call');
    await rejectsWith(assert, client.call(new Uint8Array(0), FOREVER), 'FIPC_INVALID');
    await rejectsWith(assert, client.call(Buffer.alloc(65), FOREVER), 'FIPC_TOO_LARGE');
    await rejectsWith(assert, client.call('\u00e9'.repeat(33), FOREVER), 'FIPC_TOO_LARGE'); // 66 bytes of UTF-8
    assert.equal(throwsWith(assert, () => client.callSync('', FOREVER), 'FIPC_INVALID').call, 'callSync');
    throwsWith(assert, () => client.callSync(new ArrayBuffer(65), FOREVER), 'FIPC_TOO_LARGE');
    await assert.rejects(client.call(42, FOREVER), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => client.callSync({}, FOREVER), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    for (const timeout of [NaN, '5', {}]) { // and its timeout, as every other call checks one
      await assert.rejects(client.call('x', timeout), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
      assert.throws(() => client.callSync('x', timeout), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    }
    const waited = performance.now() - started;
    assert.ok(waited < 1000, `the checks took ${waited} ms`);
    // A send of another type counts nothing either: the calls after it aren't refused
    await assert.rejects(async () => client.send(42, TEN_SECONDS), TypeError);
    assert.throws(() => client.sendSync(42, TEN_SECONDS), TypeError);
    server.sendSync('OWED');
    const answering = answerOne(server);
    assert.equal((await client.call('own', TEN_SECONDS)).toString(), 'OWN'); // OWED, still owed, was dropped first
    await answering;
    closeAll(client, server, service);
  });

  test('an FIPC_ABANDONED owed to a call that gave up counts: the next call drops it', async () => {
    const name = uniqueName('s_owed_abandoned');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const other = await joinService(name, SERVER, TEN_SECONDS);
    const quitter = await joinService(name, SERVER, TEN_SECONDS);
    const calling = client.call('gives up', 200);
    assert.equal((await quitter.receive(TEN_SECONDS)).toString(), 'gives up');
    await rejectsWith(assert, calling, 'FIPC_TIMEOUT');
    quitter.close(); // holding it: FIPC_ABANDONED, owed to the call that gave up
    const answering = answerOne(other);
    assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT');
    await answering;
    await rejectsWith(assert, client.receive(NO_WAIT), 'FIPC_TIMEOUT'); // nothing more owed
    closeAll(client, other, service);
  });

  test('after the service\'s end, a call whose owed reply never came fails with FIPC_DISCONNECTED', async () => {
    const name = uniqueName('s_call_end');
    const service = createService(name, 4096, 64, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    await rejectsWith(assert, client.call('unanswered', 50), 'FIPC_TIMEOUT');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'unanswered');
    service.close();
    await rejectsWith(assert, client.call('x', TEN_SECONDS), 'FIPC_DISCONNECTED'); // from its discard
    throwsWith(assert, () => client.callSync('x', TEN_SECONDS), 'FIPC_DISCONNECTED');
    closeAll(client, server);
  });

  test('a cancel ends a client\'s and a server\'s wait; a cancelled call leaves its reply owed', async () => {
    const name = uniqueName('s_cancel');
    const service = createService(name, 4096, 64, 2, 1);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    let client = await joinService(name, CLIENT, TEN_SECONDS);
    const calling = client.call('cancelled', FOREVER);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'cancelled');
    client.cancel(); // the call's wait for its reply fails with FIPC_CANCELLED
    await rejectsWith(assert, calling, 'FIPC_CANCELLED');
    await server.send('CANCELLED'); // owed to the cancelled call
    assert.equal((await client.receive(NO_WAIT)).toString(), 'CANCELLED'); // a receive that needn't wait still works
    await rejectsWith(assert, client.receive(TEN_SECONDS), 'FIPC_CANCELLED'); // final
    client.close();

    client = await joinService(name, CLIENT, TEN_SECONDS); // the server goes on
    const waiting = server.receive(FOREVER);
    server.cancel();
    await rejectsWith(assert, waiting, 'FIPC_CANCELLED');
    await client.send('after', TEN_SECONDS);
    assert.equal(server.receiveSync(TEN_SECONDS).toString(), 'after'); // a request there: the receive needn't wait
    server.sendSync('AFTER'); // a reply never waits
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'AFTER');
    await rejectsWith(assert, server.receive(TEN_SECONDS), 'FIPC_CANCELLED'); // final
    closeAll(client, server, service);
  });

  test('a call on a cancelled connection fails with FIPC_CANCELLED at once and sends nothing', async () => {
    const name = uniqueName('s_call_cancelled');
    const service = createService(name, 4096, 64, 1, 1);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    client.cancel();
    await rejectsWith(assert, client.call('', TEN_SECONDS), 'FIPC_INVALID'); // the request's checks come first
    await rejectsWith(assert, client.call(Buffer.alloc(65), TEN_SECONDS), 'FIPC_TOO_LARGE');
    throwsWith(assert, () => client.callSync('', TEN_SECONDS), 'FIPC_INVALID');
    for (const timeout of [FOREVER, NO_WAIT, TEN_SECONDS]) {
      const error = await rejectsWith(assert, client.call('never sent', timeout), 'FIPC_CANCELLED');
      assert.equal(error.call, 'call');
      assert.equal(throwsWith(assert, () => client.callSync('never sent', timeout), 'FIPC_CANCELLED').call, 'callSync');
    }
    await rejectsWith(assert, server.receive(200), 'FIPC_TIMEOUT'); // no request went
    closeAll(client, server, service);
  });

  test('calls run one at a time, in order; meanwhile the client\'s other calls are ERR_FIPC_BUSY', async () => {
    const name = uniqueName('s_concurrent');
    const service = createService(name, 4096, 64, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const answering = answerUntilTheEnd(server);
    const replies = await Promise.all([...Array(20)].map((_, i) => client.call(`call ${i}`, TEN_SECONDS)));
    assert.deepEqual(replies.map(String), [...Array(20)].map((_, i) => `CALL ${i}`));

    // While a call is pending, its client's sends, receives, commits, releases and callSync are ERR_FIPC_BUSY
    const calling = client.call('pending', TEN_SECONDS);
    await rejectsBusy(client.send('x'));
    throwsBusy(() => client.sendSync('x'));
    await rejectsBusy(client.sendAcquire(1));
    throwsBusy(() => client.sendAcquireSync(1));
    throwsBusy(() => client.sendCommit(1));
    await rejectsBusy(client.receive());
    throwsBusy(() => client.receiveSync(NO_WAIT));
    await rejectsBusy(client.receiveInto(Buffer.alloc(8)));
    throwsBusy(() => client.receiveIntoSync(Buffer.alloc(8), NO_WAIT));
    await rejectsBusy(client.receiveAcquire());
    throwsBusy(() => client.receiveAcquireSync(NO_WAIT));
    throwsBusy(() => client.receiveRelease());
    throwsBusy(() => client.callSync('x'));
    assert.equal(client.maxPiece, 64); // what doesn't send or receive still works
    assert.equal((await calling).toString(), 'PENDING');
    assert.equal((await client.call('then', TEN_SECONDS)).toString(), 'THEN'); // all of them work again

    // A call while an async receive of the caller's is pending would race it for the reply: ERR_FIPC_BUSY
    await client.send('sent', TEN_SECONDS);
    const receiving = client.receive(TEN_SECONDS);
    await rejectsBusy(client.call('x'));
    throwsBusy(() => client.callSync('x'));
    assert.equal((await receiving).toString(), 'SENT');

    // A call that fails doesn't stop the one queued behind it
    const first = client.call(Buffer.alloc(65), TEN_SECONDS); // FIPC_TOO_LARGE, at once
    const second = client.call('second', TEN_SECONDS);
    await rejectsWith(assert, first, 'FIPC_TOO_LARGE');
    assert.equal((await second).toString(), 'SECOND');

    // Closed: ERR_FIPC_CLOSED
    service.close();
    assert.equal(await answering, 24);
    client.close();
    await assert.rejects(client.call('x'), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => client.callSync('x'), { code: 'ERR_FIPC_CLOSED' });
    server.close();

    // call on another kind's connection is FIPC_INVALID
    const { listener, server: streamServer, client: streamClient } = await pair(4096, 's_call_stream');
    await rejectsWith(assert, streamClient.call('x'), 'FIPC_INVALID');
    throwsWith(assert, () => streamServer.callSync('x'), 'FIPC_INVALID');
    closeAll(streamServer, streamClient, listener);
  });

  test('a call waiting for its turn times out on time, and a cancel or a close ends its wait', async () => {
    const name = uniqueName('s_turns');
    const service = createService(name, 4096, 64, 2, 1);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    let client = await joinService(name, CLIENT, TEN_SECONDS);

    // The first call holds the turn while the server holds its request: the queued call's own timeout ends its wait
    const first = client.call('first', TEN_SECONDS);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'first');
    const started = performance.now();
    const short = client.call('short', 100);
    const third = client.call('third', TEN_SECONDS);
    const timedOut = await rejectsWith(assert, short, 'FIPC_TIMEOUT');
    assert.equal(timedOut.call, 'call');
    const waited = performance.now() - started;
    assert.ok(waited >= 90 && waited < 5000, `the queued call gave up after ${waited} ms`);
    await rejectsWith(assert, client.call('now', NO_WAIT), 'FIPC_TIMEOUT'); // queued, it can't wait: at once
    // A timeout that isn't one is refused at once, before the call would wait for its turn without a limit
    await assert.rejects(client.call('nan', NaN), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    await assert.rejects(client.call('text', '100'), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    await server.send('FIRST');
    assert.equal((await first).toString(), 'FIRST');
    // The call behind the one that gave up keeps its place: its request is the next the server sees
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'third');
    await server.send('THIRD');
    assert.equal((await third).toString(), 'THIRD');
    await client.send('free', TEN_SECONDS); // nothing pending is left: the client's other calls work again
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'free');
    await server.send('FREE');
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'FREE');

    // A cancel ends the wait of every call waiting for its turn, and of those made later while one is pending
    const holding = client.call('holding', FOREVER);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'holding');
    const queued = [client.call('q1', FOREVER), client.call('q2', TEN_SECONDS)];
    client.cancel(); // the holder's wait for its reply, as before, and the queued calls' waits for their turn
    queued.push(client.call('later', FOREVER)); // made after the cancel, behind the holder: at once
    await allCancelled([holding, ...queued]);
    await server.send('HOLDING'); // owed to the cancelled call
    client.close();

    // A close ends them too
    client = await joinService(name, CLIENT, TEN_SECONDS);
    const pending = client.call('pending', FOREVER);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'pending');
    const behind = client.call('behind', FOREVER);
    client.close();
    await allCancelled([pending, behind]);
    await server.send('PENDING');
    closeAll(server, service);
  });

  test('the owner\'s count of members and its cancel', async () => {
    const name = uniqueName('s_members');
    const service = createService(name, 4096, 64, 2, 3);
    assert.equal(service.waitForMembersSync(), 0);
    assert.equal(await service.waitForMembers(), 0);
    assert.equal(service.waitForMembersSync(CLIENT), 0);
    assert.equal(service.waitForMembersSync(0, 0, NO_WAIT), 0); // 0 counts both roles, as undefined does
    const timeout = throwsWith(assert, () => service.waitForMembersSync(undefined, 1, NO_WAIT), 'FIPC_TIMEOUT');
    assert.equal(timeout.count, 0);
    assert.equal(timeout.call, 'waitForMembersSync');

    const waiting = service.waitForMembers(SERVER, 1, TEN_SECONDS); // on the service's thread, while one joins
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const client = joinServiceSync(name, CLIENT, TEN_SECONDS);
    assert.ok(await waiting >= 1);
    assert.equal(await service.waitForMembers(undefined, 2, TEN_SECONDS), 2);
    assert.equal(service.waitForMembersSync(CLIENT), 1);
    assert.equal(service.waitForMembersSync(SERVER), 1);
    const started = performance.now();
    assert.equal((await rejectsWith(assert, service.waitForMembers(CLIENT, 2, 50), 'FIPC_TIMEOUT')).count, 1);
    assert.ok(performance.now() - started >= 40, `waited ${performance.now() - started} ms`);
    // Over the role's maximum (maxClients, maxServers, their sum for both), or another role
    for (const [role, atLeast] of [[CLIENT, 3], [SERVER, 4], [undefined, 6], [PRODUCER, 1]]) {
      const over = throwsWith(assert, () => service.waitForMembersSync(role, atLeast), 'FIPC_INVALID');
      assert.equal('count' in over, false);
    }
    assert.throws(() => service.waitForMembersSync('client'), TypeError);
    assert.throws(() => service.waitForMembersSync(undefined, -1), RangeError);

    const counting = service.waitForMembers(SERVER, 2);
    service.cancel(); // the wait, now or later, fails with FIPC_CANCELLED and the count
    assert.equal((await rejectsWith(assert, counting, 'FIPC_CANCELLED')).count, 1);
    const cancelled = throwsWith(assert, () => service.waitForMembersSync(undefined, 3, FOREVER), 'FIPC_CANCELLED');
    assert.equal(cancelled.count, 2);

    // The service goes on after its cancel: members join, and a call goes through
    const other = await joinService(name, CLIENT, TEN_SECONDS);
    const answering = answerOne(server);
    assert.equal((await other.call('after the cancel', TEN_SECONDS)).toString(), 'AFTER THE CANCEL');
    await answering;
    closeAll(other, client, server);
    service.close();
    assert.throws(() => service.waitForMembersSync(), { code: 'ERR_FIPC_CLOSED' });
  });

  test('the service\'s close ends it for clients and servers', async () => {
    const name = uniqueName('s_end');
    const service = createService(name, 4096, 64, 2, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const holder = await joinService(name, SERVER, TEN_SECONDS);
    await client.send('done', TEN_SECONDS);
    await client.send('held', TEN_SECONDS);
    await answerOne(server);
    assert.equal((await holder.receive(TEN_SECONDS)).toString(), 'held');
    const waiting = server.receive(TEN_SECONDS); // a receive that waits ends at the close
    const counting = service.waitForMembers(CLIENT, 2); // the close cancels it
    service.close();
    service.close(); // again: nothing
    await rejectsWith(assert, counting, 'FIPC_CANCELLED');
    await rejectsWith(assert, waiting, 'FIPC_DISCONNECTED');
    assert.equal(server.ended, true);
    await rejectsWith(assert, client.send('after', TEN_SECONDS), 'FIPC_DISCONNECTED');
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'DONE'); // the replies that came before the end
    await rejectsWith(assert, client.receive(TEN_SECONDS), 'FIPC_DISCONNECTED'); // then the end
    assert.equal(client.ended, true);
    await rejectsWith(assert, holder.send('HELD'), 'FIPC_DISCONNECTED'); // a reply after the end
    // After the end with a send's reply owed (held's, never to come), call refuses, as in every binding
    await rejectsWith(assert, client.call('x', TEN_SECONDS), 'FIPC_INVALID');
    createService(name, 4096, 64, 1, 1).close(); // the name is free again at once
    closeAll(client, server, holder);

    // A call after the end, with nothing owed: FIPC_DISCONNECTED
    const other = createService(uniqueName('s_end_call'), 4096, 64, 1, 1);
    const caller = await joinService(other.name, CLIENT, TEN_SECONDS);
    other.close();
    await rejectsWith(assert, caller.call('x', TEN_SECONDS), 'FIPC_DISCONNECTED');
    caller.close();
  });

  test('requests and replies of maxMessage bytes, through every way of sending and receiving', async () => {
    const name = uniqueName('s_large');
    const service = createService(name, 1 << 16, 1 << 14, 1, 1);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const request = Buffer.alloc(1 << 14);
    for (let i = 0; i < request.length; i++) request[i] = i & 0xff;
    await client.send(request, TEN_SECONDS);
    assert.ok((await server.receive(TEN_SECONDS)).equals(request));
    await server.send(Buffer.from(request).reverse());
    assert.ok((await client.receive(TEN_SECONDS)).equals(Buffer.from(request).reverse()));
    const answering = (async () => {
      const buffer = Buffer.alloc(1 << 14);
      const length = await server.receiveInto(buffer, TEN_SECONDS);
      await server.send(buffer.subarray(0, length).reverse());
    })();
    assert.ok((await client.call(request, TEN_SECONDS)).equals(Buffer.from(request).reverse()));
    await answering;
    closeAll(client, server, service);
  });

  test('the examples\' exchange on one thread, with the Sync calls only', () => {
    const name = uniqueName('s_sync');
    const service = createService(name, 4096, 256, 1, 1);
    const server = joinServiceSync(name, SERVER, 5000);
    const client = joinServiceSync(name, CLIENT, 5000);
    for (let i = 1; i <= 3; i++) client.sendSync(`ping ${i}`); // the requests wait for the server
    for (let i = 0; i < 3; i++) server.sendSync(server.receiveSync().toString().toUpperCase());
    server.close();
    service.close();
    const replies = [];
    for (let i = 0; i < 3; i++) replies.push(client.receiveSync().toString());
    assert.deepEqual(replies, ['PING 1', 'PING 2', 'PING 3']);
    throwsWith(assert, () => client.receiveSync(), 'FIPC_DISCONNECTED');
    client.close();
  });

  test('the shared example\'s exchange, the client joining first', async () => {
    const name = uniqueName('s_example');
    const calling = joinAndCall(name, 3); // its join waits for the service
    await sleep(200);
    const service = createService(name, 4096, 256, 1, 1);
    const server = await joinService(name, SERVER, 5000);
    for (let i = 0; i < 3; i++) {
      const request = await server.receive();
      await server.send(request.toString().toUpperCase());
    }
    await calling;
    closeAll(server, service);
  });
});
