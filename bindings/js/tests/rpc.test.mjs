// RPC: requests and responses, ids, kinds, statuses, empty and long payloads, receiving into a buffer.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  NO_WAIT,
  RPC_REQUEST,
  RPC_RESPONSE,
  TEN_SECONDS,
  closeAll,
  pair,
  pattern,
  rejectsWith,
  throwsWith,
} from './support.mjs';

describe('RPC', () => {
  test('a request and its response, async and Sync; ids count from 1', async () => {
    const { listener, server, client } = await pair();
    const id = await client.rpcSubmit(7, 'ping');
    assert.equal(id, 1);
    const request = await server.rpcReceive(TEN_SECONDS);
    assert.deepEqual(Object.keys(request), ['id', 'kind', 'opcode', 'status', 'payload']);
    assert.equal(request.id, 1);
    assert.equal(request.kind, RPC_REQUEST);
    assert.equal(request.opcode, 7);
    assert.equal(request.status, 0);
    assert.ok(Buffer.isBuffer(request.payload));
    assert.equal(request.payload.toString(), 'ping');
    await server.rpcRespond(request.id, request.opcode, 42, request.payload.toString().toUpperCase());
    const response = client.rpcReceiveSync(TEN_SECONDS);
    assert.equal(response.id, 1);
    assert.equal(response.kind, RPC_RESPONSE);
    assert.equal(response.status, 42);
    assert.equal(response.payload.toString(), 'PING');

    assert.equal(client.rpcSubmitSync(8, pattern(10)), 2);
    const second = server.rpcReceiveSync(TEN_SECONDS);
    server.rpcRespondSync(second.id, 8, 0, second.payload);
    const reply = await client.rpcReceive(TEN_SECONDS);
    assert.equal(reply.id, 2);
    assert.ok(reply.payload.equals(pattern(10)));
    closeAll(server, client, listener);
  });

  test('empty payloads: undefined, null, an empty string or buffer', async () => {
    const { listener, server, client } = await pair();
    for (const empty of [undefined, null, '', new Uint8Array(0)]) {
      const id = await client.rpcSubmit(1, empty);
      const request = await server.rpcReceive(TEN_SECONDS);
      assert.equal(request.id, id);
      assert.equal(request.payload.length, 0);
      server.rpcRespondSync(id, 1, 0, empty);
      const response = client.rpcReceiveSync(TEN_SECONDS);
      assert.equal(response.payload.length, 0);
    }
    await client.rpcSubmit(3); // no payload argument at all
    assert.equal((await server.rpcReceive(TEN_SECONDS)).opcode, 3);
    closeAll(server, client, listener);
  });

  test('every kind of payload, empty ones too, on calls queued behind a long one (the thread makes them)', async () => {
    const { listener, server, client } = await pair(4096);
    const long = pattern(100000);
    const payloads = [undefined, null, '', 'text', new Uint8Array(0), pattern(10), new DataView(new ArrayBuffer(3)), new ArrayBuffer(5)];
    const lengths = [0, 0, 0, 4, 0, 10, 3, 5, 0];
    // The long request waits for the server to take its pieces: every request after it waits behind it
    const first = client.rpcSubmit(9, long, TEN_SECONDS);
    const queued = [...payloads.map((payload) => client.rpcSubmit(1, payload, TEN_SECONDS)), client.rpcSubmit(2)];
    const requests = [];
    for (let i = 0; i <= queued.length; i++) requests.push(await server.rpcReceive(TEN_SECONDS));
    const ids = await Promise.all([first, ...queued]);
    assert.ok(requests[0].payload.equals(long));
    requests.slice(1).forEach((request, i) => {
      assert.equal(request.id, ids[i + 1]);
      assert.equal(request.payload.length, lengths[i], `request ${i}`);
    });

    const responding = [server.rpcRespond(ids[0], 9, 0, long, TEN_SECONDS)];
    payloads.forEach((payload, i) => responding.push(server.rpcRespond(ids[i + 1], 1, i, payload, TEN_SECONDS)));
    responding.push(server.rpcRespond(ids.at(-1), 2, 0));
    const responses = [];
    for (let i = 0; i < responding.length; i++) responses.push(await client.rpcReceive(TEN_SECONDS));
    await Promise.all(responding);
    assert.ok(responses[0].payload.equals(long));
    responses.slice(1).forEach((response, i) => {
      assert.equal(response.id, ids[i + 1]);
      assert.equal(response.payload.length, lengths[i], `response ${i}`);
    });
    closeAll(server, client, listener);
  });

  test('opcodes and statuses at their limits; a bigint id', async () => {
    const { listener, server, client } = await pair();
    const id = await client.rpcSubmit(0xffffffff, 'x');
    const request = await server.rpcReceive(TEN_SECONDS);
    assert.equal(request.opcode, 0xffffffff);
    for (const status of [0, -1, 2147483647, -2147483648]) {
      await server.rpcRespond(BigInt(id), 0, status);
      const response = await client.rpcReceive(TEN_SECONDS);
      assert.equal(response.status, status);
      assert.equal(response.id, id);
    }
    closeAll(server, client, listener);
  });

  test('payloads of several pieces, both ways at once', async () => {
    const { listener, server, client } = await pair(4096);
    for (const size of [4096 - 64 - 32, 4096 - 64 - 31, 100000]) {
      const payload = pattern(size, size);
      const [id, request] = await Promise.all([client.rpcSubmit(5, payload, TEN_SECONDS), server.rpcReceive(TEN_SECONDS)]);
      assert.equal(request.id, id);
      assert.ok(request.payload.equals(payload), `size ${size}`);
      const [, response] = await Promise.all([
        server.rpcRespond(id, 5, 1, request.payload, TEN_SECONDS),
        client.rpcReceive(TEN_SECONDS),
      ]);
      assert.ok(response.payload.equals(payload));
    }
    closeAll(server, client, listener);
  });

  test('many requests in flight, answered in order; ids unique and increasing', async () => {
    const { listener, server, client } = await pair(4096);
    const count = 500;
    // More than the rings hold: the client receives responses while its requests still go out
    const submitted = Array.from({ length: count }, (_, i) => client.rpcSubmit(i % 7, `request ${i}`, TEN_SECONDS));
    const serving = (async () => {
      for (let i = 0; i < count; i++) {
        const request = await server.rpcReceive(TEN_SECONDS);
        await server.rpcRespond(request.id, request.opcode, i, request.payload, TEN_SECONDS);
      }
    })();
    const responses = [];
    for (let i = 0; i < count; i++) responses.push(await client.rpcReceive(TEN_SECONDS));
    const ids = await Promise.all(submitted);
    await serving;
    // A submit that finds the ring full waits on the connection's thread; only a request that was sent takes an id, so
    // the ids are 1, 2, 3, ... with no gap
    ids.forEach((id, i) => assert.equal(id, i + 1));
    responses.forEach((response, i) => {
      assert.equal(response.id, ids[i]);
      assert.equal(response.status, i);
      assert.equal(response.payload.toString(), `request ${i}`);
    });
    closeAll(server, client, listener);
  });

  test('rpcReceiveInto: the header with the length; a longer payload stays queued (FIPC_TOO_LARGE)', async () => {
    const { listener, server, client } = await pair(4096);
    const buffer = Buffer.alloc(16);
    const id = await client.rpcSubmit(9, 'payload');
    const header = await server.rpcReceiveInto(buffer, TEN_SECONDS);
    assert.deepEqual(header, { id, kind: RPC_REQUEST, opcode: 9, status: 0, length: 7 });
    assert.equal(buffer.subarray(0, 7).toString(), 'payload');
    await client.rpcSubmit(9, pattern(40));
    const error = await rejectsWith(assert, server.rpcReceiveInto(buffer, TEN_SECONDS), 'FIPC_TOO_LARGE');
    assert.equal(error.length, 40);
    assert.equal(throwsWith(assert, () => server.rpcReceiveIntoSync(buffer, 0), 'FIPC_TOO_LARGE').length, 40);
    const big = new Uint8Array(64);
    assert.equal(server.rpcReceiveIntoSync(big, 0).length, 40);
    assert.ok(Buffer.from(big.subarray(0, 40)).equals(pattern(40)));
    await client.rpcSubmit(2);
    assert.equal((await server.rpcReceiveInto(new Uint8Array(0), TEN_SECONDS)).length, 0); // empty: fits anything
    const long = pattern(20000, 9);
    const into = Buffer.alloc(20000);
    const [, longHeader] = await Promise.all([client.rpcSubmit(4, long, TEN_SECONDS), server.rpcReceiveInto(into, TEN_SECONDS)]);
    assert.equal(longHeader.length, 20000);
    assert.ok(into.equals(long));
    closeAll(server, client, listener);
  });

  test('a plain message on an RPC receive is FIPC_INVALID, and dropped', async () => {
    const { listener, server, client } = await pair();
    await client.send('not an RPC message');
    await rejectsWith(assert, server.rpcReceive(TEN_SECONDS), 'FIPC_INVALID');
    throwsWith(assert, () => server.rpcReceiveSync(NO_WAIT), 'FIPC_TIMEOUT');
    await client.rpcSubmit(1, 'fine');
    assert.equal((await server.rpcReceive(TEN_SECONDS)).payload.toString(), 'fine');
    closeAll(server, client, listener);
  });

  test('timeouts, cancel and the peer\'s end on RPC calls', async () => {
    const { listener, server, client } = await pair();
    await rejectsWith(assert, server.rpcReceive(NO_WAIT), 'FIPC_TIMEOUT');
    await rejectsWith(assert, server.rpcReceive(30), 'FIPC_TIMEOUT');
    const pending = server.rpcReceive();
    server.cancel();
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    const waiting = client.rpcReceive(TEN_SECONDS);
    server.close();
    await rejectsWith(assert, waiting, 'FIPC_DISCONNECTED');
    await rejectsWith(assert, client.rpcSubmit(1, 'x'), 'FIPC_DISCONNECTED');
    throwsWith(assert, () => client.rpcRespondSync(1, 1, 0, 'x'), 'FIPC_DISCONNECTED');
    closeAll(client, listener);
  });
});
