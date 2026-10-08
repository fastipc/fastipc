// Plain messages: the async and Sync forms, every kind of payload, messages of several pieces, order, receiveInto,
// cancel, close and the peer's end, and the event loop while a call waits.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  NO_WAIT,
  TEN_SECONDS,
  closeAll,
  pair,
  pattern,
  rejectsWith,
  sleep,
  throwsWith,
} from './support.mjs';

describe('plain messages', () => {
  test('send and receive, async and Sync', async () => {
    const { listener, server, client } = await pair();
    await client.send('hello');
    assert.deepEqual(await server.receive(TEN_SECONDS), Buffer.from('hello'));
    client.sendSync('sync');
    const message = server.receiveSync(TEN_SECONDS);
    assert.ok(Buffer.isBuffer(message));
    assert.equal(message.toString(), 'sync');
    await server.send(pattern(1000));
    assert.deepEqual(client.receiveSync(TEN_SECONDS), pattern(1000));
    closeAll(server, client, listener);
  });

  test('every kind of payload: strings as UTF-8, TypedArrays (with offsets), DataViews, ArrayBuffers', async () => {
    const { listener, server, client } = await pair();
    const bytes = pattern(64);
    const payloads = [
      ['a string', 'héllo wörld ✓', Buffer.from('héllo wörld ✓', 'utf8')],
      ['a Buffer', bytes, bytes],
      ['a Uint8Array', new Uint8Array(bytes), bytes],
      ['a Uint8Array at an offset', new Uint8Array(bytes.buffer, bytes.byteOffset + 8, 16), bytes.subarray(8, 24)],
      ['an Int32Array', new Int32Array([1, -2, 3]), Buffer.from(new Int32Array([1, -2, 3]).buffer)],
      ['a Float64Array', new Float64Array([1.5, -0.25]), Buffer.from(new Float64Array([1.5, -0.25]).buffer)],
      ['a BigUint64Array', new BigUint64Array([2n ** 63n]), Buffer.from(new BigUint64Array([2n ** 63n]).buffer)],
      ['a DataView', new DataView(bytes.buffer, bytes.byteOffset + 4, 10), bytes.subarray(4, 14)],
      ['an ArrayBuffer', new Uint8Array([9, 8, 7]).buffer, Buffer.from([9, 8, 7])],
    ];
    for (const [what, payload, expected] of payloads) {
      await client.send(payload);
      assert.deepEqual(await server.receive(TEN_SECONDS), expected, what);
      client.sendSync(payload);
      assert.deepEqual(server.receiveSync(TEN_SECONDS), expected, `${what}, Sync`);
    }
    if (typeof SharedArrayBuffer === 'function') {
      const shared = new Uint8Array(new SharedArrayBuffer(4));
      shared.set([1, 2, 3, 4]);
      await client.send(shared);
      assert.deepEqual(await server.receive(TEN_SECONDS), Buffer.from([1, 2, 3, 4]));
    }
    if (typeof Float16Array === 'function') {
      const halves = new Float16Array([1.5, -2, 0.25]);
      await client.send(halves);
      assert.deepEqual(await server.receive(TEN_SECONDS), Buffer.from(halves.buffer));
      await client.send(pattern(8));
      assert.equal(await server.receiveInto(new Float16Array(4), TEN_SECONDS), 8);
    }
    closeAll(server, client, listener);
  });

  test('every kind of payload and receive buffer on calls queued behind a long one (the thread makes them)', async () => {
    const { listener, server, client } = await pair(4096);
    const long = pattern(100000);
    const backing = new Uint8Array(64);
    const intos = [backing.subarray(8, 24), new DataView(backing.buffer, 32, 16), new ArrayBuffer(16)];
    // Receives queued behind one that waits for a message; sends behind a long one, which waits for its pieces to go
    const receives = [server.receive(TEN_SECONDS), ...intos.map((into) => server.receiveInto(into, TEN_SECONDS))];
    const sends = [
      client.send(long, TEN_SECONDS),
      client.send('abc', TEN_SECONDS),
      client.send(pattern(16, 1), TEN_SECONDS),
      client.send(new DataView(new Uint8Array([1, 2, 3, 4, 5, 6]).buffer, 1, 3), TEN_SECONDS),
      client.send(new Uint8Array([9, 8]).buffer, TEN_SECONDS),
    ];
    const empties = ['', new Uint8Array(0)].map((empty) => rejectsWith(assert, client.send(empty, TEN_SECONDS), 'FIPC_INVALID'));
    receives.push(server.receive(TEN_SECONDS));
    const [first, ...lengths] = await Promise.all(receives);
    await Promise.all(sends);
    await Promise.all(empties);
    assert.ok(first.equals(long));
    assert.deepEqual(lengths.slice(0, 3), [3, 16, 3]);
    assert.equal(Buffer.from(backing.subarray(8, 11)).toString(), 'abc');
    assert.ok(Buffer.from(backing.subarray(32, 48)).equals(pattern(16, 1)));
    assert.deepEqual([...new Uint8Array(intos[2], 0, 3)], [2, 3, 4]);
    assert.deepEqual(lengths[3], Buffer.from([9, 8]));
    closeAll(server, client, listener);
  });

  test('an async timeout is never cut short: a receive of 2 ms waits them out', async () => {
    const { listener, server, client } = await pair();
    let shortest = Infinity;
    for (let i = 0; i < 200; i++) {
      const started = performance.now();
      await rejectsWith(assert, server.receive(2), 'FIPC_TIMEOUT');
      shortest = Math.min(shortest, performance.now() - started);
    }
    // Deadlines are whole milliseconds: one may come up to 1 ms early, never more
    assert.ok(shortest >= 1, `a receive of 2 ms timed out after ${shortest.toFixed(3)} ms`);
    closeAll(server, client, listener);
  });

  test('an empty message is FIPC_INVALID', async () => {
    const { listener, server, client } = await pair();
    await rejectsWith(assert, client.send(''), 'FIPC_INVALID');
    await rejectsWith(assert, client.send(new Uint8Array(0)), 'FIPC_INVALID');
    throwsWith(assert, () => client.sendSync(Buffer.alloc(0)), 'FIPC_INVALID');
    await client.send('after');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'after');
    closeAll(server, client, listener);
  });

  test('messages of several pieces, async both ways, while the other side receives as they come', async () => {
    const { listener, server, client } = await pair(4096);
    for (const size of [4096 - 64, 4096 - 63, 10000, 100000, 1 << 20]) {
      const message = pattern(size, size);
      const [, received] = await Promise.all([client.send(message, TEN_SECONDS), server.receive(TEN_SECONDS)]);
      assert.equal(received.length, size);
      assert.ok(received.equals(message), `size ${size}`);
    }
    closeAll(server, client, listener);
  });

  test('a long message with Sync on one side and async on the other', async () => {
    const { listener, server, client } = await pair(4096);
    const message = pattern(300000, 7);
    const receiving = server.receive(TEN_SECONDS);
    const sending = client.send(message, TEN_SECONDS); // a thread sends it as the receiver makes room
    assert.ok((await receiving).equals(message));
    await sending;
    const back = server.send(message, TEN_SECONDS);
    assert.ok(client.receiveSync(TEN_SECONDS).equals(message)); // this thread receives as the server's thread sends
    await back;
    closeAll(server, client, listener);
  });

  test('order: queued async sends and receives keep the order they were made in', async () => {
    const { listener, server, client } = await pair(4096);
    const count = 2000;
    const sizes = Array.from({ length: count }, (_, i) => (i % 50 === 0 ? 9000 : 1 + (i % 300)));
    const sends = sizes.map((size, i) => client.send(pattern(size, i), TEN_SECONDS)); // not awaited one by one
    const receives = sizes.map(() => server.receive(TEN_SECONDS));
    const received = await Promise.all(receives);
    await Promise.all(sends);
    received.forEach((message, i) => assert.ok(message.equals(pattern(sizes[i], i)), `message ${i}`));
    closeAll(server, client, listener);
  });

  test('a full ring: a send waits for room (Sync: FIPC_TIMEOUT with NO_WAIT), and goes once there is', async () => {
    const { listener, server, client } = await pair(4096);
    let sent = 0;
    for (;;) {
      try {
        client.sendSync(pattern(500, sent), NO_WAIT);
        sent += 1;
      } catch (error) {
        assert.equal(error.code, 'FIPC_TIMEOUT');
        break;
      }
    }
    assert.ok(sent >= 5, `${sent} messages fit`);
    await rejectsWith(assert, client.send('x', NO_WAIT), 'FIPC_TIMEOUT');
    await rejectsWith(assert, client.send('x', 30), 'FIPC_TIMEOUT');
    const waiting = client.send('last', TEN_SECONDS);
    await sleep(20);
    for (let i = 0; i < sent; i++) assert.ok((await server.receive(TEN_SECONDS)).equals(pattern(500, i)));
    await waiting;
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'last');
    closeAll(server, client, listener);
  });

  test('receiveInto: the length; a message longer than the buffer stays queued (FIPC_TOO_LARGE, its length)', async () => {
    const { listener, server, client } = await pair(4096);
    const buffer = new Uint8Array(100);
    await client.send('twelve bytes');
    assert.equal(await server.receiveInto(buffer, TEN_SECONDS), 12);
    assert.equal(Buffer.from(buffer.subarray(0, 12)).toString(), 'twelve bytes');
    await client.send(pattern(150));
    const error = await rejectsWith(assert, server.receiveInto(buffer, TEN_SECONDS), 'FIPC_TOO_LARGE');
    assert.equal(error.length, 150);
    assert.equal(throwsWith(assert, () => server.receiveIntoSync(buffer, TEN_SECONDS), 'FIPC_TOO_LARGE').length, 150);
    const big = new ArrayBuffer(200);
    assert.equal(server.receiveIntoSync(big, TEN_SECONDS), 150);
    assert.ok(Buffer.from(big, 0, 150).equals(pattern(150)));
    // Into a view at an offset, and a DataView; a message of several pieces into a buffer that holds it
    const backing = new Uint8Array(64);
    await client.send('abc');
    assert.equal(await server.receiveInto(backing.subarray(10, 20), TEN_SECONDS), 3);
    assert.equal(Buffer.from(backing.subarray(10, 13)).toString(), 'abc');
    await client.send('def');
    assert.equal(server.receiveIntoSync(new DataView(backing.buffer, 30, 5), TEN_SECONDS), 3);
    assert.equal(Buffer.from(backing.subarray(30, 33)).toString(), 'def');
    const long = pattern(50000, 3);
    const into = Buffer.alloc(60000);
    const [length] = await Promise.all([server.receiveInto(into, TEN_SECONDS), client.send(long, TEN_SECONDS)]);
    assert.equal(length, 50000);
    assert.ok(into.subarray(0, length).equals(long));
    closeAll(server, client, listener);
  });

  test('the event loop runs while a receive waits, and while a long message moves', async () => {
    const { listener, server, client } = await pair(4096);
    let ticks = 0;
    const timer = setInterval(() => (ticks += 1), 5);
    const receiving = server.receive(TEN_SECONDS);
    await sleep(200);
    assert.ok(ticks >= 10, `${ticks} timer ticks while the receive waited`);
    await client.send('wake');
    assert.equal((await receiving).toString(), 'wake');
    ticks = 0;
    const message = pattern(4 << 20);
    const moving = Promise.all([client.send(message, TEN_SECONDS), server.receive(TEN_SECONDS)]);
    const [, received] = await moving;
    assert.ok(received.equals(message));
    clearInterval(timer);
    closeAll(server, client, listener);
  });

  test('cancel: pending and later calls that wait fail with FIPC_CANCELLED; calls that needn\'t wait still work', async () => {
    const { listener, server, client } = await pair(4096);
    const pending = server.receive();
    const queued = server.receive();
    await sleep(20);
    server.cancel();
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    await rejectsWith(assert, queued, 'FIPC_CANCELLED');
    await rejectsWith(assert, server.receive(), 'FIPC_CANCELLED');
    throwsWith(assert, () => server.receiveSync(), 'FIPC_CANCELLED');
    await server.send('a send with room needn\'t wait');
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'a send with room needn\'t wait');
    await client.send('queued before');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'queued before'); // a message there: no wait
    closeAll(server, client, listener);
  });

  test('close with calls pending: they fail with FIPC_CANCELLED, the peer sees FIPC_DISCONNECTED', async () => {
    const { listener, server, client } = await pair(4096);
    const receives = [server.receive(), server.receive(TEN_SECONDS)];
    const filling = [];
    for (let i = 0; i < 20; i++) filling.push(server.send(pattern(1000), TEN_SECONDS));
    const settled = Promise.allSettled(filling); // handled from now on: some reject when the close cancels them
    await sleep(20);
    server.close();
    await rejectsWith(assert, receives[0], 'FIPC_CANCELLED');
    await rejectsWith(assert, receives[1], 'FIPC_CANCELLED');
    const outcomes = await settled;
    assert.ok(outcomes.some((o) => o.status === 'rejected' && o.reason.code === 'FIPC_CANCELLED'));
    // The client gets what the server completed, then the end
    let delivered = 0;
    for (;;) {
      try {
        await client.receive(TEN_SECONDS);
        delivered += 1;
      } catch (error) {
        assert.equal(error.code, 'FIPC_DISCONNECTED');
        break;
      }
    }
    assert.equal(delivered, outcomes.filter((o) => o.status === 'fulfilled').length);
    assert.equal(client.ended, true);
    closeAll(client, listener);
  });

  test('the peer\'s end: messages first, then FIPC_DISCONNECTED for every call, final', async () => {
    const { listener, server, client } = await pair(4096);
    await client.send('one');
    client.sendSync('two');
    const waiting = server.receive(TEN_SECONDS);
    assert.equal((await waiting).toString(), 'one');
    const end = server.receive(TEN_SECONDS);
    const second = server.receive(TEN_SECONDS);
    client.close();
    assert.equal((await end).toString(), 'two');
    await rejectsWith(assert, second, 'FIPC_DISCONNECTED');
    throwsWith(assert, () => server.receiveSync(), 'FIPC_DISCONNECTED');
    await rejectsWith(assert, server.send('x'), 'FIPC_DISCONNECTED');
    throwsWith(assert, () => server.sendSync('x'), 'FIPC_DISCONNECTED');
    assert.equal(server.ended, true);
    closeAll(server, listener);
  });

  test('a Sync call beside a pending async call of the same direction is ERR_FIPC_BUSY; the other direction is free', async () => {
    const { listener, server, client } = await pair(4096);
    const pending = server.receive(TEN_SECONDS);
    assert.throws(() => server.receiveSync(0), { code: 'ERR_FIPC_BUSY' });
    assert.throws(() => server.receiveIntoSync(new Uint8Array(4), 0), { code: 'ERR_FIPC_BUSY' });
    assert.throws(() => server.rpcReceiveSync(0), { code: 'ERR_FIPC_BUSY' });
    assert.throws(() => server.receiveRelease(), { code: 'ERR_FIPC_BUSY' });
    await assert.rejects(server.receiveAcquire(0), { code: 'ERR_FIPC_BUSY' });
    server.sendSync('the send direction is free');
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'the send direction is free');
    await client.send('done');
    assert.equal((await pending).toString(), 'done');
    throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_TIMEOUT'); // settled: Sync calls work again
    closeAll(server, client, listener);
  });
});
