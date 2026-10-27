// Batches (sendMany, receiveMany), async and Sync: on a stream, in order and whole, every kind of payload, maxCount, a
// first message longer than the batch's 64 KiB, NO_WAIT's partial send with how many went, an empty message refused
// before any goes; a broadcast's and a work queue's batches, and FIPC_TOO_LARGE over maxPiece before any goes; a
// service client's batch of requests, owed as its sends are, with their replies in order, and a server's batches
// refused.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  CLIENT,
  CONSUMER,
  FOREVER,
  NO_WAIT,
  PRODUCER,
  SERVER,
  SUBSCRIBER_GATED,
  SUBSCRIBER_LOSSY,
  TEN_SECONDS,
  closeAll,
  createBroadcast,
  createQueue,
  createService,
  joinBroadcast,
  joinQueue,
  joinService,
  pair,
  pattern,
  rejectsWith,
  throwsWith,
  uniqueName,
} from './support.mjs';

/** `count` messages, received in batches (a broadcast's subscriber may take fewer than are there) */
async function receiveAll(conn, count, maxCount = 16) {
  const received = [];
  while (received.length < count) {
    const batch = await conn.receiveMany(maxCount, TEN_SECONDS);
    assert.ok(batch.length >= 1 && batch.length <= maxCount, `a batch of ${batch.length}`);
    received.push(...batch);
  }
  return received;
}

describe('batches', () => {
  test('a stream\'s batches come in order and whole, async and Sync, of every kind of payload', async () => {
    const { listener, server, client } = await pair(1024);
    const messages = Array.from({ length: 6 }, (_, i) => pattern(10 + i * 20, i));
    assert.equal(await client.sendMany(messages, TEN_SECONDS), 6);
    assert.deepEqual(await server.receiveMany(16, TEN_SECONDS), messages);
    assert.equal(client.sendManySync(messages, TEN_SECONDS), 6);
    assert.deepEqual(server.receiveManySync(2, TEN_SECONDS), messages.slice(0, 2)); // maxCount
    assert.deepEqual(server.receiveManySync(undefined, TEN_SECONDS), messages.slice(2)); // 16
    const bytes = pattern(32);
    const payloads = ['héllo ✓', bytes, new Uint8Array(bytes.buffer, bytes.byteOffset + 8, 8),
      new DataView(bytes.buffer, bytes.byteOffset, 4), new Uint8Array([9, 8, 7]).buffer];
    assert.equal(await client.sendMany(payloads, TEN_SECONDS), 5);
    assert.deepEqual(await receiveAll(server, 5), [Buffer.from('héllo ✓'), bytes, bytes.subarray(8, 16),
      bytes.subarray(0, 4), Buffer.from([9, 8, 7])]);

    // A message longer than one piece goes in pieces (the send's lane makes the call), and one longer than 64 KiB
    // comes alone, in a batch of its own
    const long = pattern(100000, 5);
    const receiving = receiveAll(server, 3);
    assert.equal(await client.sendMany(['a', long, 'b'], TEN_SECONDS), 3);
    assert.deepEqual(await receiving, [Buffer.from('a'), long, Buffer.from('b')]);
    await rejectsWith(assert, server.receiveMany(16, NO_WAIT), 'FIPC_TIMEOUT');
    closeAll(server, client, listener);
  });

  test('a partial send says how many went, and an empty message sends none', async () => {
    const { listener, server, client } = await pair(1024);
    const hundred = pattern(100);
    const many = Array(100).fill(hundred);
    // Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
    const partial = await rejectsWith(assert, client.sendMany(many, NO_WAIT), 'FIPC_TIMEOUT');
    assert.ok(partial.sent >= 1 && partial.sent <= 8, partial.message);
    assert.match(partial.message, /sendMany: FIPC_TIMEOUT \(\d+ sent\)/);
    assert.deepEqual(await receiveAll(server, partial.sent), Array(partial.sent).fill(hundred));
    const partialSync = throwsWith(assert, () => client.sendManySync(many, NO_WAIT), 'FIPC_TIMEOUT');
    assert.ok(partialSync.sent >= 1 && partialSync.sent <= 8);
    let received = 0;
    while (received < partialSync.sent) received += server.receiveManySync(16, TEN_SECONDS).length;
    assert.equal(received, partialSync.sent);

    // More than the ring holds, sent while it is received: what didn't go at once goes from the send's lane
    const receiving = receiveAll(server, 100);
    assert.equal(await client.sendMany(many, TEN_SECONDS), 100);
    assert.deepEqual(await receiving, many);

    const empty = await rejectsWith(assert, client.sendMany([hundred, ''], NO_WAIT), 'FIPC_INVALID');
    assert.equal(empty.sent, 0);
    await rejectsWith(assert, client.sendMany([], NO_WAIT), 'FIPC_INVALID');
    throwsWith(assert, () => client.sendManySync([new Uint8Array(0)], NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, server.receiveMany(0, NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, server.receiveMany(16, NO_WAIT), 'FIPC_TIMEOUT'); // nothing went
    await assert.rejects(client.sendMany('not an array'), TypeError);
    await assert.rejects(client.sendMany([1]), TypeError);
    assert.throws(() => server.receiveManySync(-1), RangeError);
    closeAll(server, client, listener);
  });

  test('a broadcast\'s batches reach every subscriber', async () => {
    const name = uniqueName('bc_batch');
    const publisher = createBroadcast(name, 64 * 1024, 4, FOREVER);
    const lossy = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS);
    const gated = await joinBroadcast(name, SUBSCRIBER_GATED, TEN_SECONDS);
    assert.equal(await publisher.waitForMembers(2, TEN_SECONDS), 2);
    const ticks = Array.from({ length: 40 }, (_, i) => Buffer.from(`tick ${i}`));
    assert.equal(await publisher.sendMany(ticks, TEN_SECONDS), 40);
    const tooLarge = await rejectsWith(assert,
      publisher.sendMany([ticks[0], Buffer.alloc(publisher.maxPiece + 1)], NO_WAIT), 'FIPC_TOO_LARGE');
    assert.equal(tooLarge.sent, 0);
    await rejectsWith(assert, publisher.receiveMany(16, NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, gated.sendMany(ticks, NO_WAIT), 'FIPC_INVALID');
    publisher.close(); // what it sent is received first
    for (const subscriber of [lossy, gated]) {
      assert.deepEqual(await receiveAll(subscriber, 40), ticks);
      await rejectsWith(assert, subscriber.receiveMany(16, TEN_SECONDS), 'FIPC_DISCONNECTED');
    }
    closeAll(lossy, gated);
  });

  test('a work queue\'s batches deliver every message once', async () => {
    const name = uniqueName('q_batch');
    const queue = createQueue(name, 4096, 1, 1);
    const producer = await joinQueue(name, PRODUCER, TEN_SECONDS);
    const consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
    const jobs = Array.from({ length: 200 }, (_, i) => Buffer.from(`job ${i}`));
    const receiving = receiveAll(consumer, 200);
    for (let at = 0; at < 200; at += 50) assert.equal(await producer.sendMany(jobs.slice(at, at + 50), TEN_SECONDS), 50);
    assert.deepEqual(await receiving, jobs); // one producer: its messages in order
    await rejectsWith(assert, producer.sendMany([jobs[0], Buffer.alloc(producer.maxPiece + 1)], NO_WAIT),
      'FIPC_TOO_LARGE');
    await rejectsWith(assert, consumer.sendMany(jobs, NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, producer.receiveMany(16, NO_WAIT), 'FIPC_INVALID');
    queue.close();
    await rejectsWith(assert, consumer.receiveMany(16, TEN_SECONDS), 'FIPC_DISCONNECTED');
    await rejectsWith(assert, producer.sendMany(jobs, TEN_SECONDS), 'FIPC_DISCONNECTED');
    closeAll(producer, consumer);
  });

  test('a service client\'s batch is owed as its sends are, and a server\'s batches are invalid', async () => {
    const name = uniqueName('s_batch');
    const service = createService(name, 16 * 1024, 64, 1, 1);
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const answer = async () => {
      const request = await server.receive(TEN_SECONDS);
      await server.send(`${request} done`, NO_WAIT);
    };
    assert.equal(await client.sendMany(['a', 'bb', 'ccc'], TEN_SECONDS), 3);
    await rejectsWith(assert, client.call('d', NO_WAIT), 'FIPC_INVALID'); // those replies are the caller's
    for (let i = 0; i < 3; i++) await answer();
    assert.deepEqual((await receiveAll(client, 3)).map(String), ['a done', 'bb done', 'ccc done']);
    const [reply] = await Promise.all([client.call('e', TEN_SECONDS), answer()]); // nothing owed now
    assert.equal(String(reply), 'e done');
    await rejectsWith(assert, server.sendMany(['x'], NO_WAIT), 'FIPC_INVALID');
    await rejectsWith(assert, server.receiveMany(16, NO_WAIT), 'FIPC_INVALID');

    // A batch whose requests don't all fit counts only those that went: once their replies are received, a call goes
    // again. Each request holds 64 + 64 bytes of the 16 KiB ring: at most 128 fit
    const partial = throwsWith(assert, () => client.sendManySync(Array(200).fill('f'), NO_WAIT), 'FIPC_TIMEOUT');
    assert.ok(partial.sent >= 1 && partial.sent <= 128, partial.message);
    throwsWith(assert, () => client.callSync('g', NO_WAIT), 'FIPC_INVALID'); // their replies are owed
    for (let i = 0; i < partial.sent; i++) server.sendSync(server.receiveSync(TEN_SECONDS), NO_WAIT);
    assert.equal(client.receiveManySync(256, TEN_SECONDS).length, partial.sent);
    const [last] = await Promise.all([client.call('h', TEN_SECONDS), answer()]);
    assert.equal(String(last), 'h done');
    closeAll(server, client, service);
  });
});
