// The work queue in one process: createQueue, the count of members and what each role refuses, the end at the queue's
// close, producers and consumers interleaved, a broadcast and a queue refusing each other's roles, consumers racing
// for messages of every length, and the shared example's exchange on one thread. The owner's process admits the
// members by itself and a join waits on a thread of its own, so an owner and its members can share a process, and a
// thread (joinQueueSync). The queue promises no order: the checks are that each message came exactly once. Other
// processes and the Python binding: tests/processes.test.mjs and tests/interop.test.mjs; a queue the program drops
// (the garbage collector never closes it): tests/lifetime.test.mjs.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  CONSUMER,
  FOREVER,
  NO_WAIT,
  PRODUCER,
  SUBSCRIBER_GATED,
  SUBSCRIBER_LOSSY,
  TEN_SECONDS,
  closeAll,
  connectStream,
  createBroadcast,
  createQueue,
  createStream,
  joinBroadcast,
  joinQueue,
  joinQueueSync,
  rejectsWith,
  throwsWith,
  uniqueName,
} from './support.mjs';
import { SIZES, TIMEOUT, checkJobs, checkSplit, job, jobNumber } from './queue.mjs';

const SLOW = { timeout: 120000 };

/** Lets the event loop run: an async call's settling, another loop's turn */
function yieldToLoop() {
  return new Promise((resolve) => setImmediate(resolve));
}

/** Waits until `queue` counts `count` members of `role` (undefined: both), within ten seconds */
async function countDropsTo(queue, role, count) {
  const deadline = performance.now() + TEN_SECONDS;
  while (queue.waitForMembersSync(role) !== count) {
    assert.ok(performance.now() < deadline, `the count didn't drop to ${count} within 10 s`);
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

describe('work queue', () => {
  test('createQueue, the count of members, and what the roles refuse', async () => {
    const name = uniqueName('q_api');
    for (const [capacity, producers, consumers] of [[2048, 1, 1], [5000, 1, 1], [4096, 0, 1], [4096, 65, 1],
      [4096, 1, 0], [4096, 1, 65]]) {
      const error = throwsWith(assert, () => createQueue(name, capacity, producers, consumers), 'FIPC_INVALID');
      assert.equal(error.call, 'createQueue');
    }
    assert.throws(() => createQueue(name, 4096, '1', 1), TypeError);
    assert.throws(() => createQueue(name, 4096, 1.5, 1), RangeError);
    assert.throws(() => createQueue(42, 4096, 1, 1), TypeError);
    assert.equal(PRODUCER, 3);
    assert.equal(CONSUMER, 4);

    const queue = createQueue(name, 4096, 1, 2);
    assert.equal(queue.name, name);
    assert.equal(queue.closed, false);
    throwsWith(assert, () => createQueue(name, 4096, 1, 1), 'FIPC_ADDR_IN_USE');
    throwsWith(assert, () => createBroadcast(name, 4096, 1), 'FIPC_ADDR_IN_USE');
    throwsWith(assert, () => createStream(name, 4096), 'FIPC_ADDR_IN_USE');

    // The count: at once, a poll, a wait that times out, over a role's maximum
    assert.equal(queue.waitForMembersSync(), 0);
    assert.equal(await queue.waitForMembers(), 0);
    assert.equal(queue.waitForMembersSync(PRODUCER), 0);
    assert.equal(queue.waitForMembersSync(0, 0, NO_WAIT), 0); // 0 counts both roles, as undefined does
    const timeout = throwsWith(assert, () => queue.waitForMembersSync(CONSUMER, 1, NO_WAIT), 'FIPC_TIMEOUT');
    assert.equal(timeout.count, 0);
    assert.equal(timeout.call, 'waitForMembersSync');
    const started = performance.now();
    assert.equal((await rejectsWith(assert, queue.waitForMembers(undefined, 1, 50), 'FIPC_TIMEOUT')).count, 0);
    assert.ok(performance.now() - started >= 40, `waited ${performance.now() - started} ms`);
    throwsWith(assert, () => queue.waitForMembersSync(PRODUCER, 2), 'FIPC_INVALID');
    throwsWith(assert, () => queue.waitForMembersSync(CONSUMER, 3), 'FIPC_INVALID');
    const over = throwsWith(assert, () => queue.waitForMembersSync(undefined, 4), 'FIPC_INVALID');
    assert.equal('count' in over, false);
    throwsWith(assert, () => queue.waitForMembersSync(undefined, 3, NO_WAIT), 'FIPC_TIMEOUT'); // both: up to 1 + 2
    throwsWith(assert, () => queue.waitForMembersSync(SUBSCRIBER_GATED, 0), 'FIPC_INVALID');
    assert.throws(() => queue.waitForMembersSync('producer'), TypeError);
    assert.throws(() => queue.waitForMembersSync(undefined, -1), RangeError);

    // The roles: a broadcast's and unknown ones are refused
    throwsWith(assert, () => joinQueueSync(name, 5, NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, joinQueue(name, SUBSCRIBER_LOSSY, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinQueue(name, SUBSCRIBER_GATED, TEN_SECONDS), 'FIPC_INVALID');
    assert.throws(() => joinQueueSync(name, 'producer'), TypeError);

    // The owner's producer, joined from this thread: it sends, nothing else
    const producer = joinQueueSync(name, PRODUCER, TEN_SECONDS);
    assert.equal(await queue.waitForMembers(PRODUCER, 1, TEN_SECONDS), 1);
    await rejectsWith(assert, joinQueue(name, PRODUCER, 100), 'FIPC_TIMEOUT'); // the one producer's place is held
    assert.equal(producer.maxPiece, 1024);
    throwsWith(assert, () => producer.receiveSync(NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, producer.receive(NO_WAIT), 'FIPC_INVALID');
    throwsWith(assert, () => producer.receiveIntoSync(Buffer.alloc(8), NO_WAIT), 'FIPC_INVALID');
    throwsWith(assert, () => producer.receiveAcquireSync(NO_WAIT), 'FIPC_INVALID');
    producer.receiveRelease(); // does nothing
    throwsWith(assert, () => producer.waitForMembersSync(), 'FIPC_INVALID');
    throwsWith(assert, () => producer.sendSync(Buffer.alloc(1025)), 'FIPC_TOO_LARGE');
    await rejectsWith(assert, producer.send(Buffer.alloc(1025)), 'FIPC_TOO_LARGE');
    producer.sendSync('job 1', NO_WAIT);
    const room = await producer.sendAcquire(5, NO_WAIT);
    room.set(Buffer.from('job 2'));
    producer.sendCommit(5);
    let sent = 2;
    for (;;) { // until the ring is full, as no consumer takes anything
      try {
        producer.sendSync(`job ${sent + 1}`, NO_WAIT);
      } catch (error) {
        assert.equal(error.code, 'FIPC_TIMEOUT');
        break;
      }
      sent += 1;
    }
    assert.equal(sent, 64, 'the messages a ring of 4096 bytes holds');
    const waiting = producer.send('job 65'); // waits for room, on the producer's thread
    producer.cancel();
    await rejectsWith(assert, waiting, 'FIPC_CANCELLED');

    // A consumer receives; nothing else
    const consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
    assert.equal(await queue.waitForMembers(undefined, 2, TEN_SECONDS), 2);
    assert.equal(consumer.maxPiece, 1024);
    await rejectsWith(assert, consumer.send('x'), 'FIPC_INVALID');
    throwsWith(assert, () => consumer.sendAcquireSync(1), 'FIPC_INVALID');
    throwsWith(assert, () => consumer.waitForMembersSync(), 'FIPC_INVALID');
    const received = [];
    for (let i = 0; i < 64; i++) received.push(jobNumber(await consumer.receive(TEN_SECONDS)));
    checkJobs(received, 1, 64);
    throwsWith(assert, () => consumer.receiveSync(NO_WAIT), 'FIPC_TIMEOUT');
    const waited = performance.now();
    await rejectsWith(assert, consumer.receive(50), 'FIPC_TIMEOUT'); // nothing left: it waits
    assert.ok(performance.now() - waited >= 40, `waited ${performance.now() - waited} ms`);
    throwsWith(assert, () => consumer.receiveAcquireSync(NO_WAIT), 'FIPC_TIMEOUT');

    // The cancel: a wait, now or later, fails with FIPC_CANCELLED and the count; the queue goes on
    const counting = queue.waitForMembers(CONSUMER, 2);
    queue.cancel();
    assert.equal((await rejectsWith(assert, counting, 'FIPC_CANCELLED')).count, 1);
    assert.equal(throwsWith(assert, () => queue.waitForMembersSync(undefined, 3, FOREVER), 'FIPC_CANCELLED').count, 2);
    closeAll(producer, consumer);
    joinQueueSync(name, CONSUMER, TEN_SECONDS).close(); // joins still work
    queue.close();
    queue.close(); // again: nothing
    assert.equal(queue.closed, true);
    assert.throws(() => queue.waitForMembersSync(), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => queue.cancel(), { code: 'ERR_FIPC_CLOSED' });
    createQueue(name, 4096, 1, 1).close(); // the name is free again at once
  });

  test('the queue\'s close ends it; a member\'s close doesn\'t', async () => {
    const name = uniqueName('q_end');
    const queue = createQueue(name, 4096, 2, 1);
    const producer = await joinQueue(name, PRODUCER, TEN_SECONDS);
    let consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
    producer.sendSync('job 1', NO_WAIT);
    // A member's close leaves the queue; it goes on, and the member's place is free again
    consumer.close();
    await countDropsTo(queue, CONSUMER, 0);
    consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
    for (let i = 2; i <= 4; i++) producer.sendSync(job(i), NO_WAIT);

    // The queue's close: producers' sends fail, consumers receive what is left (each its own way), then the end
    const counting = queue.waitForMembers(PRODUCER, 2); // the close cancels it
    queue.close();
    await rejectsWith(assert, counting, 'FIPC_CANCELLED');
    await rejectsWith(assert, producer.send('job 5', TEN_SECONDS), 'FIPC_DISCONNECTED');
    assert.equal(producer.ended, true);
    producer.close();
    const received = [jobNumber(await consumer.receive(TEN_SECONDS))];
    const buffer = Buffer.alloc(16);
    received.push(jobNumber(buffer.subarray(0, await consumer.receiveInto(buffer, TEN_SECONDS))));
    const view = await consumer.receiveAcquire(TEN_SECONDS);
    received.push(jobNumber(Buffer.from(view)));
    consumer.receiveRelease();
    assert.equal(view.length, 0, 'the view is detached at the release');
    received.push(jobNumber(consumer.receiveSync(TEN_SECONDS)));
    checkJobs(received, 1, 4);
    throwsWith(assert, () => consumer.receiveSync(TEN_SECONDS), 'FIPC_DISCONNECTED');
    assert.equal(consumer.ended, true);
    consumer.close();

    // A receive that waits ends at the close
    const other = createQueue(uniqueName('q_end_wait'), 4096, 1, 1);
    const waiter = await joinQueue(other.name, CONSUMER, TEN_SECONDS);
    const receiving = waiter.receive(TEN_SECONDS);
    other.close();
    await rejectsWith(assert, receiving, 'FIPC_DISCONNECTED');
    waiter.close();
  });

  test('three producers and three consumers, each its own way: every message once', SLOW, async () => {
    const name = uniqueName('q_mix');
    const queue = createQueue(name, 4096, 8, 8);
    const producers = [await joinQueue(name, PRODUCER, TEN_SECONDS), await joinQueue(name, PRODUCER, TEN_SECONDS),
      joinQueueSync(name, PRODUCER, TEN_SECONDS)];
    const consumers = [];
    for (let c = 0; c < 3; c++) consumers.push(await joinQueue(name, CONSUMER, TEN_SECONDS));
    assert.equal(await queue.waitForMembers(undefined, 6, TEN_SECONDS), 6);
    assert.equal(producers[2].maxPiece, 1024);
    assert.equal(consumers[0].maxPiece, 1024);

    const COUNT = 200; // each producer's: more than its ring holds (64), so sends wait for room
    const message = (p, i) => `producer ${p} job ${i}`;
    const sends = [
      async (producer, text) => producer.send(text, TIMEOUT),
      async (producer, text) => { // in place
        const room = await producer.sendAcquire(text.length, TIMEOUT);
        room.set(Buffer.from(text));
        producer.sendCommit(text.length);
      },
      async (producer, text) => { // polled with the Sync call
        for (;;) {
          try {
            return producer.sendSync(text, NO_WAIT);
          } catch (error) {
            assert.equal(error.code, 'FIPC_TIMEOUT');
          }
          await yieldToLoop();
        }
      },
    ];
    const buffer = Buffer.alloc(64);
    const receives = [
      async (consumer) => (await consumer.receive(TIMEOUT)).toString(),
      async (consumer) => buffer.toString('utf8', 0, await consumer.receiveInto(buffer, TIMEOUT)),
      async (consumer) => { // in place
        const text = Buffer.from(await consumer.receiveAcquire(TIMEOUT)).toString();
        consumer.receiveRelease();
        return text;
      },
    ];
    const producing = producers.map(async (producer, p) => {
      for (let i = 1; i <= COUNT; i++) await sends[p](producer, message(p, i));
    });
    const lists = consumers.map(async (consumer, c) => {
      const list = [];
      for (;;) {
        try {
          list.push(await receives[c](consumer));
        } catch (error) {
          if (error.code === 'FIPC_DISCONNECTED') return list;
          throw error;
        }
      }
    });
    await Promise.all(producing);
    // The queue's close ends it: producers' sends fail, consumers receive what is left, then the end
    queue.close();
    for (const producer of producers) {
      await rejectsWith(assert, producer.send('after the end', TEN_SECONDS), 'FIPC_DISCONNECTED');
      producer.close();
    }

    const seen = new Set();
    for (const list of await Promise.all(lists)) {
      for (const text of list) {
        assert.match(text, /^producer \d job \d+$/, 'a message of no producer');
        assert.ok(!seen.has(text), `${text} went to two consumers`);
        seen.add(text);
      }
    }
    for (let p = 0; p < 3; p++) {
      for (let i = 1; i <= COUNT; i++) assert.ok(seen.has(message(p, i)), `${message(p, i)} was never received`);
    }
    closeAll(...consumers);
  });

  test('a broadcast and a queue refuse each other\'s roles and names; a queue is no stream', async () => {
    const queueName = uniqueName('q_kind');
    const queue = createQueue(queueName, 4096, 1, 1);
    await rejectsWith(assert, joinBroadcast(queueName, SUBSCRIBER_LOSSY, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinBroadcast(queueName, PRODUCER, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, connectStream(queueName, TEN_SECONDS), 'FIPC_INVALID');
    const producer = await joinQueue(queueName, PRODUCER, TEN_SECONDS); // the owner's admission went on
    const broadcastName = uniqueName('q_kind_bc');
    const publisher = createBroadcast(broadcastName, 4096, 1);
    await rejectsWith(assert, joinQueue(broadcastName, CONSUMER, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinQueue(broadcastName, SUBSCRIBER_GATED, TEN_SECONDS), 'FIPC_INVALID');
    const subscriber = await joinBroadcast(broadcastName, SUBSCRIBER_LOSSY, TEN_SECONDS);
    closeAll(subscriber, publisher, producer, queue);
  });

  test('consumers race for messages of every length: each received once and whole', SLOW, async () => {
    // A receive asks for the next message's length, then receives it into a Buffer of that length; meanwhile another
    // consumer may take that message, and the next is longer or shorter. Two consumers receive on their threads (the
    // async calls), two on this one (the Sync call): one polls, one waits a little, so it retries within a timeout.
    const name = uniqueName('q_race');
    const queue = createQueue(name, 64 * 1024, 1, 4);
    const producer = await joinQueue(name, PRODUCER, TEN_SECONDS);
    const consumers = await Promise.all([0, 1, 2, 3].map(() => joinQueue(name, CONSUMER, TEN_SECONDS)));
    const COUNT = 2000;
    const size = SIZES.long;
    const lists = consumers.map(async (consumer, c) => {
      const list = [];
      for (;;) {
        let data;
        try {
          if (c < 2) {
            data = await consumer.receive(TIMEOUT);
          } else {
            data = consumer.receiveSync(c === 2 ? NO_WAIT : 5); // the async sends go on, on the producer's thread
          }
        } catch (error) {
          if (error.code === 'FIPC_DISCONNECTED') return list;
          assert.ok(c >= 2 && error.code === 'FIPC_TIMEOUT', `consumer ${c}: ${error.code}`);
          await yieldToLoop();
          continue;
        }
        list.push(jobNumber(data, size));
        if (c >= 2) await yieldToLoop();
      }
    });
    for (let i = 1; i <= COUNT; i++) await producer.send(job(i, size(i)), TIMEOUT);
    producer.close();
    queue.close();
    checkSplit(await Promise.all(lists), COUNT);
    closeAll(...consumers);
  });

  test('the shared example\'s exchange on one thread, with the Sync calls only', () => {
    const name = uniqueName('q_sync');
    const queue = createQueue(name, 4096, 1, 1);
    const producer = joinQueueSync(name, PRODUCER, 5000);
    for (let i = 1; i <= 3; i++) producer.sendSync(`job ${i}`); // waits in the queue until a consumer takes it
    const consumer = joinQueueSync(name, CONSUMER, 5000);
    assert.equal(queue.waitForMembersSync(CONSUMER, 1), 1);
    producer.close();
    queue.close();
    const jobs = [];
    for (;;) {
      try {
        jobs.push(consumer.receiveSync().toString());
      } catch (error) {
        if (error.code === 'FIPC_DISCONNECTED') break;
        throw error;
      }
    }
    assert.deepEqual(jobs.sort(), ['job 1', 'job 2', 'job 3']);
    consumer.close();
  });
});
