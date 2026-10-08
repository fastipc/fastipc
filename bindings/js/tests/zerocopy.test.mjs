// Zero-copy: acquire, commit and release, async and Sync, and the views' lifetimes (detached when their bytes end).
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { NO_WAIT, TEN_SECONDS, closeAll, pair, pattern, rejectsWith, sleep, throwsWith } from './support.mjs';

describe('zero-copy', () => {
  test('sendAcquire + sendCommit, receiveAcquire + receiveRelease, async and Sync', async () => {
    const { listener, server, client } = await pair(4096);
    const slot = await client.sendAcquire(5, TEN_SECONDS);
    assert.ok(slot instanceof Uint8Array);
    assert.equal(slot.length, 5);
    slot.set(Buffer.from('hello'));
    client.sendCommit(5);
    assert.equal(slot.length, 0, 'the commit detaches the view');
    const view = await server.receiveAcquire(TEN_SECONDS);
    assert.ok(view instanceof Uint8Array);
    assert.equal(Buffer.from(view).toString(), 'hello');
    server.receiveRelease();
    assert.equal(view.length, 0, 'the release detaches the view');
    assert.equal(view.buffer.byteLength, 0);

    const syncSlot = client.sendAcquireSync(100, TEN_SECONDS);
    syncSlot.set(pattern(100));
    client.sendCommit(60); // the first 60 bytes of the room
    const syncView = server.receiveAcquireSync(TEN_SECONDS);
    assert.ok(Buffer.from(syncView).equals(pattern(60)));
    server.receiveRelease();
    server.receiveRelease(); // without a message: nothing
    closeAll(server, client, listener);
  });

  test('the views are 16-byte aligned, as the ring hands them out', async () => {
    const { listener, server, client } = await pair(4096);
    for (const length of [1, 3, 17, 100]) {
      const slot = client.sendAcquireSync(length, TEN_SECONDS);
      assert.equal(slot.byteOffset, 0);
      slot.fill(length);
      client.sendCommit(length);
      const view = server.receiveAcquireSync(TEN_SECONDS);
      assert.equal(view.length, length);
      assert.ok(view.every((b) => b === length));
      server.receiveRelease();
    }
    closeAll(server, client, listener);
  });

  test('the lane\'s next call detaches the view: a receive releases the message, a send drops the reservation', async () => {
    const { listener, server, client } = await pair(4096);
    await client.send('first');
    await client.send('second');
    const view = server.receiveAcquireSync(TEN_SECONDS);
    assert.equal(Buffer.from(view).toString(), 'first');
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'second'); // releases 'first' first
    assert.equal(view.length, 0);

    const dropped = client.sendAcquireSync(10, TEN_SECONDS);
    dropped.fill(1);
    await client.send('instead'); // the reservation is dropped, never sent
    assert.equal(dropped.length, 0);
    assert.equal((await server.receive(TEN_SECONDS)).toString(), 'instead');
    throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_TIMEOUT');

    // A Uint8Array made over the view's buffer is detached with it
    await client.send('again');
    const another = server.receiveAcquireSync(TEN_SECONDS);
    const alias = new Uint8Array(another.buffer);
    assert.equal(alias.length, 5);
    server.receiveRelease();
    assert.equal(alias.length, 0);
    closeAll(server, client, listener);
  });

  test('close detaches every view; the views keep nothing alive after it', async () => {
    const { listener, server, client } = await pair(4096);
    await client.send('held');
    const view = server.receiveAcquireSync(TEN_SECONDS);
    const slot = client.sendAcquireSync(8, TEN_SECONDS);
    server.close();
    client.close();
    assert.equal(view.length, 0);
    assert.equal(slot.length, 0);
    assert.equal(view[0], undefined);
    slot[0] = 1; // writes nowhere
    closeAll(listener);
  });

  test('sendCommit without a reservation, or with a bad length, is FIPC_INVALID (the reservation stays)', async () => {
    const { listener, server, client } = await pair(4096);
    throwsWith(assert, () => client.sendCommit(1), 'FIPC_INVALID');
    const slot = client.sendAcquireSync(4, TEN_SECONDS);
    throwsWith(assert, () => client.sendCommit(5), 'FIPC_INVALID');
    throwsWith(assert, () => client.sendCommit(0), 'FIPC_INVALID');
    assert.equal(slot.length, 4, 'a failed commit keeps the view');
    slot.set([1, 2, 3, 4]);
    client.sendCommit(4);
    assert.deepEqual([...(await server.receive(TEN_SECONDS))], [1, 2, 3, 4]);
    closeAll(server, client, listener);
  });

  test('acquire limits: over maxPiece is FIPC_TOO_LARGE; a message of several pieces stays queued for receive', async () => {
    const { listener, server, client } = await pair(4096);
    assert.equal(client.maxPiece, 4032);
    await rejectsWith(assert, client.sendAcquire(4033), 'FIPC_TOO_LARGE');
    throwsWith(assert, () => client.sendAcquireSync(0), 'FIPC_INVALID');
    const full = client.sendAcquireSync(4032, TEN_SECONDS);
    full.fill(7);
    client.sendCommit(4032);
    const message = pattern(10000);
    const sending = client.send(message, TEN_SECONDS);
    assert.equal((await server.receiveAcquire(TEN_SECONDS)).length, 4032);
    server.receiveRelease();
    const error = await rejectsWith(assert, server.receiveAcquire(TEN_SECONDS), 'FIPC_TOO_LARGE');
    assert.equal(error.length, 10000);
    assert.ok((await server.receive(TEN_SECONDS)).equals(message));
    await sending;
    closeAll(server, client, listener);
  });

  test('an async acquire waits for room or a message, without blocking the event loop', async () => {
    const { listener, server, client } = await pair(4096);
    let ticks = 0;
    const timer = setInterval(() => (ticks += 1), 5);
    const waiting = server.receiveAcquire(TEN_SECONDS);
    await sleep(100);
    assert.ok(ticks >= 5);
    client.sendSync('zero-copy');
    const view = await waiting;
    assert.equal(Buffer.from(view).toString(), 'zero-copy');
    server.receiveRelease();
    // Fill the ring, then acquire room: it comes once the server receives
    while (true) {
      try {
        client.sendSync(pattern(1000), NO_WAIT);
      } catch (error) {
        assert.equal(error.code, 'FIPC_TIMEOUT');
        break;
      }
    }
    await rejectsWith(assert, client.sendAcquire(1000, NO_WAIT), 'FIPC_TIMEOUT');
    const room = client.sendAcquire(1000, TEN_SECONDS);
    await sleep(20);
    while (true) {
      try {
        server.receiveSync(NO_WAIT);
      } catch (error) {
        assert.equal(error.code, 'FIPC_TIMEOUT');
        break;
      }
    }
    const slot = await room;
    slot.fill(3);
    client.sendCommit(1000);
    assert.ok((await server.receive(TEN_SECONDS)).every((b) => b === 3));
    clearInterval(timer);
    closeAll(server, client, listener);
  });

  test('an async acquire runs alone on its direction: anything beside it is ERR_FIPC_BUSY', async () => {
    const { listener, server, client } = await pair(4096);
    const pendingReceive = server.receive(TEN_SECONDS);
    await assert.rejects(server.receiveAcquire(TEN_SECONDS), { code: 'ERR_FIPC_BUSY' }); // behind a pending call
    client.sendSync('a');
    assert.equal((await pendingReceive).toString(), 'a');
    const acquiring = server.receiveAcquire(TEN_SECONDS);
    await assert.rejects(server.receive(TEN_SECONDS), { code: 'ERR_FIPC_BUSY' });
    await assert.rejects(server.rpcReceive(TEN_SECONDS), { code: 'ERR_FIPC_BUSY' });
    assert.throws(() => server.receiveAcquireSync(0), { code: 'ERR_FIPC_BUSY' });
    assert.throws(() => server.receiveRelease(), { code: 'ERR_FIPC_BUSY' });
    client.sendSync('b');
    const view = await acquiring;
    assert.equal(Buffer.from(view).toString(), 'b');
    server.receiveRelease();
    // Sends: an acquire and a commit
    const slotting = client.sendAcquire(4, TEN_SECONDS);
    const slot = await slotting;
    slot.set([1, 2, 3, 4]);
    client.sendCommit(4);
    assert.deepEqual([...(await server.receive(TEN_SECONDS))], [1, 2, 3, 4]);
    closeAll(server, client, listener);
  });

  test('cancel and close end a pending acquire with FIPC_CANCELLED', async () => {
    const { listener, server, client } = await pair(4096);
    const acquiring = server.receiveAcquire();
    await sleep(20);
    server.cancel();
    await rejectsWith(assert, acquiring, 'FIPC_CANCELLED');
    const other = client.receiveAcquire();
    await sleep(20);
    client.close();
    await rejectsWith(assert, other, 'FIPC_CANCELLED');
    closeAll(server, listener);
  });
});
