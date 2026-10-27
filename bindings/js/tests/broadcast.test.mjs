// The broadcast in one process: createBroadcast, the publisher's calls and a broadcast's misuses, a lossy and a gated
// subscriber, the overrun's lost count, the count of members and its wait, an unsubscribed gated subscriber. The
// publisher's process admits its subscribers by itself and a join waits on a thread of its own, so a publisher and its
// subscribers can share a process, and a thread (joinBroadcastSync). Other processes and the Python binding:
// tests/processes.test.mjs and tests/interop.test.mjs.
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
  joinBroadcastSync,
  rejectsWith,
  throwsWith,
  uniqueName,
} from './support.mjs';

/** A message of `size` bytes that starts with `i` */
function numbered(i, size = 48) {
  const bytes = Buffer.alloc(size);
  bytes.writeUInt32LE(i);
  return bytes;
}

/** Waits until `publisher` counts `count` subscribers, within ten seconds */
async function countDropsTo(publisher, count) {
  const deadline = performance.now() + TEN_SECONDS;
  while (publisher.waitForMembersSync() !== count) {
    assert.ok(performance.now() < deadline, `the count didn't drop to ${count} within 10 s`);
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

describe('broadcast', () => {
  test('createBroadcast, and the publisher without subscribers', async () => {
    const name = uniqueName('bc_api');
    for (const [capacity, maxSubscribers] of [[2048, 1], [4096, 0], [4096, 1025], [5000, 1]]) {
      const error = throwsWith(assert, () => createBroadcast(name, capacity, maxSubscribers), 'FIPC_INVALID');
      assert.equal(error.call, 'createBroadcast');
    }
    assert.throws(() => createBroadcast(name, 4096, '8'), TypeError);
    assert.throws(() => createBroadcast(42, 4096, 8), TypeError);
    const publisher = createBroadcast(name, 4096, 2);
    throwsWith(assert, () => createBroadcast(name, 4096, 2), 'FIPC_ADDR_IN_USE');
    throwsWith(assert, () => createStream(name, 4096), 'FIPC_ADDR_IN_USE');
    throwsWith(assert, () => createQueue(name, 4096, 1, 1), 'FIPC_ADDR_IN_USE');
    assert.equal(publisher.maxPiece, 1024);

    // The count: at once, a poll, a wait that times out, over max_subscribers
    assert.equal(publisher.waitForMembersSync(), 0);
    assert.equal(await publisher.waitForMembers(), 0);
    const timeout = throwsWith(assert, () => publisher.waitForMembersSync(1, NO_WAIT), 'FIPC_TIMEOUT');
    assert.equal(timeout.count, 0);
    assert.equal(timeout.message, 'waitForMembersSync: FIPC_TIMEOUT (0 joined)');
    const started = performance.now();
    assert.equal((await rejectsWith(assert, publisher.waitForMembers(2, 50), 'FIPC_TIMEOUT')).count, 0);
    assert.ok(performance.now() - started >= 40, `waited ${performance.now() - started} ms`);
    const over = throwsWith(assert, () => publisher.waitForMembersSync(3), 'FIPC_INVALID'); // over max_subscribers
    assert.equal('count' in over, false);
    assert.throws(() => publisher.waitForMembersSync(-1), RangeError);

    // What a publisher can't do
    throwsWith(assert, () => publisher.receiveSync(NO_WAIT), 'FIPC_INVALID');
    throwsWith(assert, () => publisher.sendSync(Buffer.alloc(1025)), 'FIPC_TOO_LARGE');

    // The roles: a queue's and unknown ones are refused
    throwsWith(assert, () => joinBroadcastSync(name, 5, NO_WAIT), 'FIPC_INVALID'); // 5 is no role
    await rejectsWith(assert, joinBroadcast(name, PRODUCER, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, joinBroadcast(name, CONSUMER, TEN_SECONDS), 'FIPC_INVALID');
    assert.throws(() => joinBroadcastSync(name, 'gated'), TypeError);

    // The cancel: a wait, now or later, fails with FIPC_CANCELLED, the count with it
    const counting = publisher.waitForMembers(1);
    publisher.cancel();
    assert.equal((await rejectsWith(assert, counting, 'FIPC_CANCELLED')).count, 0);
    assert.equal(throwsWith(assert, () => publisher.waitForMembersSync(1, FOREVER), 'FIPC_CANCELLED').count, 0);
    publisher.close();
    assert.throws(() => publisher.waitForMembersSync(), { code: 'ERR_FIPC_CLOSED' });

    // The publisher's close frees the name at once
    createBroadcast(name, 4096, 1).close();
  });

  test('a lossy and a gated subscriber receive every message, then FIPC_DISCONNECTED', async () => {
    const name = uniqueName('bc_roles');
    const publisher = createBroadcast(name, 1 << 16, 4, FOREVER);
    const lossy = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    const gated = await joinBroadcast(name, SUBSCRIBER_GATED, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(2, TEN_SECONDS), 2);
    assert.equal(lossy.maxPiece, (1 << 16) / 4);
    assert.equal(gated.maxPiece, publisher.maxPiece);

    const words = ['hello', 'shared', 'memory'.repeat(1000)];
    for (const word of words) await publisher.send(word, TEN_SECONDS);
    for (const word of words) assert.equal((await lossy.receive(TEN_SECONDS)).toString(), word);
    const buffer = Buffer.alloc(16);
    assert.equal(await gated.receiveInto(buffer, TEN_SECONDS), 5);
    assert.equal(buffer.subarray(0, 5).toString(), 'hello');
    // Copies only: a gated subscriber's message could become inaccessible while a view of it is read
    await rejectsWith(assert, gated.receiveAcquire(TEN_SECONDS), 'FIPC_INVALID');
    throwsWith(assert, () => lossy.receiveAcquireSync(NO_WAIT), 'FIPC_INVALID');
    assert.equal(gated.receiveSync(TEN_SECONDS).toString(), 'shared');
    assert.equal(gated.receiveSync(TEN_SECONDS).toString(), words[2]);

    // Zero-copy publishing; a subscriber's misuses
    const room = await publisher.sendAcquire(4, TEN_SECONDS);
    room.set(Buffer.from('tick'));
    publisher.sendCommit(4);
    assert.equal((await lossy.receive(TEN_SECONDS)).toString(), 'tick');
    assert.equal((await gated.receive(TEN_SECONDS)).toString(), 'tick');
    await rejectsWith(assert, lossy.receive(NO_WAIT), 'FIPC_TIMEOUT');
    await rejectsWith(assert, gated.send('x'), 'FIPC_INVALID');
    throwsWith(assert, () => gated.waitForMembersSync(), 'FIPC_INVALID');
    await rejectsWith(assert, publisher.send(Buffer.alloc(publisher.maxPiece + 1)), 'FIPC_TOO_LARGE');

    // The publisher's close ends the broadcast: what it sent is received first
    await publisher.send('last', TEN_SECONDS);
    publisher.close();
    for (const subscriber of [lossy, gated]) {
      assert.equal((await subscriber.receive(TEN_SECONDS)).toString(), 'last');
      await rejectsWith(assert, subscriber.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
      assert.equal(subscriber.ended, true);
    }
    closeAll(lossy, gated);
  });

  test('a lapped lossy subscriber gets FIPC_OVERRUN with the number of messages lost', async () => {
    const name = uniqueName('bc_overrun');
    const publisher = createBroadcast(name, 4096, 1, FOREVER);
    const subscriber = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(1, TEN_SECONDS), 1);
    // 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
    // (docs/protocol.md §9.6), so 0 to 139 are lost
    for (let i = 0; i < 144; i++) publisher.sendSync(numbered(i), NO_WAIT);
    const overrun = await rejectsWith(assert, subscriber.receive(TEN_SECONDS), 'FIPC_OVERRUN');
    assert.equal(overrun.lost, 140);
    assert.equal(typeof overrun.lost, 'number');
    assert.equal(overrun.message, 'receive: FIPC_OVERRUN (140 lost)');
    assert.equal('count' in overrun, false);
    assert.equal((await subscriber.receive(TEN_SECONDS)).readUInt32LE(), 140);
    const buffer = Buffer.alloc(64);
    for (let i = 141; i < 144; i++) {
      assert.equal(await subscriber.receiveInto(buffer, TEN_SECONDS), 48);
      assert.equal(buffer.readUInt32LE(), i);
    }
    assert.equal(overrun.lost, 140, 'the count, after the receives'); // a value of its own
    assert.equal('lost' in await rejectsWith(assert, subscriber.receive(NO_WAIT), 'FIPC_TIMEOUT'), false);

    // The Sync receives report it too, into the caller's buffer and into a new one
    for (let i = 0; i < 144; i++) publisher.sendSync(numbered(i), NO_WAIT);
    assert.equal(throwsWith(assert, () => subscriber.receiveIntoSync(buffer, TEN_SECONDS), 'FIPC_OVERRUN').lost, 140);
    assert.equal(subscriber.receiveIntoSync(buffer, TEN_SECONDS), 48);
    assert.equal(buffer.readUInt32LE(), 140);
    for (let i = 0; i < 3; i++) subscriber.receiveSync(TEN_SECONDS);
    for (let i = 0; i < 144; i++) publisher.sendSync(numbered(i), NO_WAIT);
    assert.equal(throwsWith(assert, () => subscriber.receiveSync(TEN_SECONDS), 'FIPC_OVERRUN').lost, 140);
    assert.equal(subscriber.receiveSync(TEN_SECONDS).readUInt32LE(), 140);
    closeAll(subscriber, publisher);
  });

  test('the count of members and its wait; joins in either form, on the publisher\'s thread too', async () => {
    const name = uniqueName('bc_count');
    const publisher = createBroadcast(name, 4096, 2, FOREVER);
    const waiting = publisher.waitForMembers(1, TEN_SECONDS); // on the publisher's thread, while one joins
    // A Sync join on this thread, the publisher's: its process admits the subscriber by itself
    const first = joinBroadcastSync(name, SUBSCRIBER_GATED, TEN_SECONDS);
    assert.equal(await waiting, 1);
    assert.equal(publisher.waitForMembersSync(), 1);
    assert.equal((await rejectsWith(assert, publisher.waitForMembers(2, 50), 'FIPC_TIMEOUT')).count, 1);

    // The count runs beside a count that waits, in either form
    const both = publisher.waitForMembers(2, TEN_SECONDS);
    assert.equal(publisher.waitForMembersSync(), 1);
    assert.equal(await publisher.waitForMembers(), 1);
    const second = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    assert.equal(await both, 2);
    await rejectsWith(assert, joinBroadcast(name, SUBSCRIBER_LOSSY, 100), 'FIPC_TIMEOUT'); // every slot is held

    // A subscriber that leaves is counted until the publisher has seen its end, soon after; its slot is free then
    second.close();
    await countDropsTo(publisher, 1);
    const third = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(2, TEN_SECONDS), 2);
    await publisher.send('to both', TEN_SECONDS);
    assert.equal((await first.receive(TEN_SECONDS)).toString(), 'to both');
    assert.equal((await third.receive(TEN_SECONDS)).toString(), 'to both');
    third.close();

    // Closing the publisher cancels a count that waits, which reports the count
    await countDropsTo(publisher, 1);
    const pending = publisher.waitForMembers(2);
    publisher.close();
    assert.equal((await rejectsWith(assert, pending, 'FIPC_CANCELLED')).count, 1);
    await rejectsWith(assert, first.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
    first.close();
  });

  test('a gated subscriber that holds the publisher back is unsubscribed', async () => {
    const name = uniqueName('bc_stall');
    const publisher = createBroadcast(name, 4096, 1, 100);
    const subscriber = await joinBroadcast(name, SUBSCRIBER_GATED, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(1, TEN_SECONDS), 1);
    // The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed and the publisher goes on
    for (let i = 0; i < 100; i++) await publisher.send(numbered(i), TEN_SECONDS);
    assert.equal(publisher.waitForMembersSync(), 0);
    await rejectsWith(assert, subscriber.receive(NO_WAIT), 'FIPC_UNSUBSCRIBED');
    throwsWith(assert, () => subscriber.receiveSync(NO_WAIT), 'FIPC_UNSUBSCRIBED'); // final
    closeAll(subscriber, publisher);
  });

  test('a broadcast that refuses gated subscribers; a broadcast is no stream', async () => {
    const name = uniqueName('bc_no_gated');
    const publisher = createBroadcast(name, 4096, 1, NO_WAIT);
    await rejectsWith(assert, joinBroadcast(name, SUBSCRIBER_GATED, TEN_SECONDS), 'FIPC_INVALID');
    await rejectsWith(assert, connectStream(name, TEN_SECONDS), 'FIPC_INVALID');
    const lossy = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(1, TEN_SECONDS), 1);
    closeAll(lossy, publisher);
  });

  test('the shared example\'s exchange on one thread, with the Sync calls only', () => {
    const name = uniqueName('bc_sync');
    const publisher = createBroadcast(name, 1 << 16, 8, 5000);
    const subscriber = joinBroadcastSync(name, SUBSCRIBER_GATED, 5000);
    assert.equal(publisher.waitForMembersSync(1), 1);
    for (let i = 1; i <= 3; i++) publisher.sendSync(`tick ${i}`);
    publisher.close();
    const ticks = [];
    for (;;) {
      try {
        ticks.push(subscriber.receiveSync().toString());
      } catch (error) {
        if (error.code === 'FIPC_DISCONNECTED') break;
        throw error;
      }
    }
    assert.deepEqual(ticks, ['tick 1', 'tick 2', 'tick 3']);
    subscriber.close();
  });
});
