// The API's surface: the exports, the results and errors, arguments, listeners and handles' states.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, test } from 'node:test';

import * as esm from '../index.mjs';
import {
  Connection,
  FipcError,
  Listener,
  NO_WAIT,
  Queue,
  Service,
  binding,
  closeAll,
  connectStream,
  connectStreamSync,
  createStream,
  fipc,
  pair,
  rejectsWith,
  throwsWith,
  uniqueName,
} from './support.mjs';

const require = createRequire(import.meta.url);

describe('the module', () => {
  test('ES module and CommonJS entries export the same binding', () => {
    const cjs = require('../index.cjs');
    const names = ['createStream', 'connectStream', 'connectStreamSync', 'createBroadcast', 'joinBroadcast',
      'joinBroadcastSync', 'createQueue', 'joinQueue', 'joinQueueSync', 'createService', 'joinService',
      'joinServiceSync', 'Listener', 'Queue', 'Service', 'Connection', 'FipcError', 'Result', 'resultStr', 'NO_WAIT',
      'FOREVER', 'SUBSCRIBER_LOSSY', 'SUBSCRIBER_GATED', 'PRODUCER', 'CONSUMER', 'CLIENT', 'SERVER', 'version',
      'library', 'addon'];
    for (const name of names) assert.equal(esm[name], cjs[name], name);
    assert.deepEqual(Object.keys(cjs).sort(), [...names].sort(), 'the exports, and nothing else');
    assert.equal(esm.default, cjs);
  });

  test('constants, version and paths', () => {
    assert.equal(fipc.NO_WAIT, 0);
    assert.equal(fipc.FOREVER, -1);
    assert.equal(fipc.SUBSCRIBER_LOSSY, 1);
    assert.equal(fipc.SUBSCRIBER_GATED, 2);
    assert.equal(fipc.PRODUCER, 3);
    assert.equal(fipc.CONSUMER, 4);
    assert.equal(fipc.CLIENT, 5);
    assert.equal(fipc.SERVER, 6);
    const pkg = JSON.parse(readFileSync(path.join(binding, 'package.json'), 'utf8'));
    assert.equal(fipc.version, pkg.version);
    assert.match(path.basename(fipc.addon), /^fipc\.node$/);
    assert.match(path.basename(fipc.library), /^(fastipc\.dll|libfastipc\.so|libfastipc\.dylib)$/);
  });

  test('results and their names', () => {
    assert.deepEqual(Object.entries(fipc.Result), [['OK', 0], ['TIMEOUT', 1], ['DISCONNECTED', 2], ['CANCELLED', 3],
      ['TOO_LARGE', 4], ['INVALID', 5], ['NO_MEMORY', 6], ['ADDR_IN_USE', 7], ['OVERRUN', 8], ['UNSUBSCRIBED', 9],
      ['ABANDONED', 10]]);
    assert.ok(Object.isFrozen(fipc.Result));
    assert.equal(fipc.resultStr(0), 'FIPC_OK');
    assert.equal(fipc.resultStr(7), 'FIPC_ADDR_IN_USE');
    assert.equal(fipc.resultStr(9), 'FIPC_UNSUBSCRIBED');
    assert.equal(fipc.resultStr(10), 'FIPC_ABANDONED');
    assert.equal(fipc.resultStr(11), 'FIPC_UNKNOWN');
    assert.equal(fipc.resultStr(-1), 'FIPC_UNKNOWN');
    assert.equal(fipc.resultStr('1'), 'FIPC_UNKNOWN');
  });

  test('a FipcError names its result and its call', () => {
    const error = new FipcError(1, 'receive');
    assert.ok(error instanceof Error);
    assert.equal(error.name, 'FipcError');
    assert.equal(error.code, 'FIPC_TIMEOUT');
    assert.equal(error.result, 1);
    assert.equal(error.call, 'receive');
    assert.equal(error.message, 'receive: FIPC_TIMEOUT');
    assert.equal('length' in error, false);
    assert.equal(new FipcError(4, 'receiveInto', 100).length, 100);
  });

  test('listeners, queues, services and connections are made by the kinds\' functions only', () => {
    assert.throws(() => new Listener(), TypeError);
    assert.throws(() => new Queue(), TypeError);
    assert.throws(() => new Service(), TypeError);
    assert.throws(() => new Connection(), TypeError);
  });
});

describe('listeners', () => {
  test('createStream claims a name; a second listener gets FIPC_ADDR_IN_USE until the first closes', () => {
    const name = uniqueName('in_use');
    const listener = createStream(name, 4096);
    assert.equal(listener.name, name);
    assert.equal(listener.closed, false);
    const error = throwsWith(assert, () => createStream(name, 4096), 'FIPC_ADDR_IN_USE');
    assert.equal(error.call, 'createStream');
    listener.close();
    assert.equal(listener.closed, true);
    listener.close(); // again: nothing
    createStream(name, 4096).close();
  });

  test('a bad name or capacity is FIPC_INVALID; a wrong type is a TypeError', () => {
    throwsWith(assert, () => createStream('', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => createStream('.hidden', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => createStream('a/b', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => createStream('x'.repeat(246), 4096), 'FIPC_INVALID');
    throwsWith(assert, () => createStream('a\0b', 4096), 'FIPC_INVALID'); // not 'a': a NUL is in no name
    throwsWith(assert, () => createStream(uniqueName('cap'), 1000), 'FIPC_INVALID'); // not a power of two
    throwsWith(assert, () => createStream(uniqueName('cap'), 512), 'FIPC_INVALID'); // under 1 KiB
    assert.throws(() => createStream(42, 4096), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => createStream('x', '4096'), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => createStream('x', 4096.5), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' });
    assert.throws(() => createStream('x', -1), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' });
  });

  test('accept times out; acceptSync with NO_WAIT polls', async () => {
    const listener = createStream(uniqueName('accept_timeout'), 4096);
    const started = performance.now();
    const error = await rejectsWith(assert, listener.accept(50), 'FIPC_TIMEOUT');
    assert.equal(error.call, 'accept');
    assert.ok(performance.now() - started >= 40);
    assert.equal(throwsWith(assert, () => listener.acceptSync(NO_WAIT), 'FIPC_TIMEOUT').call, 'acceptSync');
    listener.close();
  });

  test('cancel ends a pending accept and every later one', async () => {
    const listener = createStream(uniqueName('accept_cancel'), 4096);
    const pending = listener.accept();
    setTimeout(() => listener.cancel(), 20);
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    await rejectsWith(assert, listener.accept(), 'FIPC_CANCELLED');
    throwsWith(assert, () => listener.acceptSync(), 'FIPC_CANCELLED');
    listener.close();
  });

  test('close ends a pending accept with FIPC_CANCELLED; calls on a closed listener fail with ERR_FIPC_CLOSED', async () => {
    const listener = createStream(uniqueName('accept_close'), 4096);
    const pending = listener.accept();
    listener.close();
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    await assert.rejects(listener.accept(), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => listener.acceptSync(0), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => listener.cancel(), { code: 'ERR_FIPC_CLOSED' });
  });

  test('one client at a time: accept fails with FIPC_INVALID while the last connection is open', async () => {
    const { listener, server, client } = await pair(4096, 'one_client');
    await rejectsWith(assert, listener.accept(NO_WAIT), 'FIPC_INVALID');
    server.close();
    const [second, secondClient] = await Promise.all([listener.accept(5000), connectStream(listener.name, 5000)]);
    closeAll(second, secondClient, client, listener);
  });

  test('an acceptSync beside a pending accept is ERR_FIPC_BUSY; a second accept queues behind it', async () => {
    const listener = createStream(uniqueName('accept_busy'), 4096);
    const pending = listener.accept();
    const queued = listener.accept();
    assert.throws(() => listener.acceptSync(0), { code: 'ERR_FIPC_BUSY' });
    listener.close();
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    await rejectsWith(assert, queued, 'FIPC_CANCELLED');
  });

  test('the connections a listener accepted stay open after it closes', async () => {
    const { listener, server, client } = await pair(4096, 'outlive');
    listener.close();
    await client.send('still here');
    assert.equal((await server.receive(1000)).toString(), 'still here');
    closeAll(server, client);
  });

  test('Symbol.dispose closes', { skip: typeof Symbol.dispose !== 'symbol' }, () => {
    const listener = createStream(uniqueName('dispose'), 4096);
    listener[Symbol.dispose]();
    assert.equal(listener.closed, true);
  });
});

describe('connections', () => {
  test('connectStream times out without a server; connectStreamSync too', async () => {
    const name = uniqueName('no_server');
    const error = await rejectsWith(assert, connectStream(name, 50), 'FIPC_TIMEOUT');
    assert.equal(error.call, 'connectStream');
    assert.equal(throwsWith(assert, () => connectStreamSync(name, 0), 'FIPC_TIMEOUT').call, 'connectStreamSync');
    await rejectsWith(assert, connectStream(`${name}\0x`, 50), 'FIPC_INVALID'); // not name: a NUL is in no name
    throwsWith(assert, () => connectStreamSync(`${name}\0x`, 0), 'FIPC_INVALID');
    await assert.rejects(connectStream(42), { code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => connectStreamSync(name, 'soon'), { code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => connectStreamSync(name, Number.NaN), { code: 'ERR_INVALID_ARG_TYPE' });
  });

  test('a client that connects first waits for the server to listen', async () => {
    const name = uniqueName('client_first');
    const connecting = connectStream(name, 10000);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const listener = createStream(name, 4096);
    const [server, client] = await Promise.all([listener.accept(10000), connecting]);
    closeAll(server, client, listener);
  });

  test('connectStreamSync on this thread against an async accept', async () => {
    const name = uniqueName('connect_sync');
    const listener = createStream(name, 4096);
    const accepting = listener.accept(10000);
    const client = connectStreamSync(name, 10000);
    const server = await accepting;
    assert.equal(client.maxPiece, 4096 - 64);
    assert.equal(server.maxPiece, 4096 - 64);
    closeAll(server, client, listener);
  });

  test('closed, ended, close twice, and calls on a closed connection', async () => {
    const { listener, server, client } = await pair(4096, 'closed');
    assert.equal(client.closed, false);
    assert.equal(client.ended, false);
    client.close();
    client.close();
    assert.equal(client.closed, true);
    assert.equal(client.maxPiece, 4096 - 64); // still known
    await assert.rejects(client.receive(), { code: 'ERR_FIPC_CLOSED' });
    await assert.rejects(client.send('x'), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => client.sendSync('x'), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => client.receiveRelease(), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => client.sendCommit(1), { code: 'ERR_FIPC_CLOSED' });
    assert.throws(() => client.cancel(), { code: 'ERR_FIPC_CLOSED' });
    await rejectsWith(assert, server.receive(1000), 'FIPC_DISCONNECTED');
    assert.equal(server.ended, true);
    await rejectsWith(assert, server.send('x'), 'FIPC_DISCONNECTED');
    closeAll(server, listener);
  });

  test('wrong argument types', async () => {
    const { listener, server, client } = await pair(4096, 'types');
    await assert.rejects(client.send(42), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    await assert.rejects(client.send({}), { name: 'TypeError' });
    assert.throws(() => client.sendSync(null), { name: 'TypeError' });
    await assert.rejects(client.receive('soon'), { name: 'TypeError' });
    await assert.rejects(client.receiveInto('not a buffer'), { name: 'TypeError' });
    await assert.rejects(client.sendAcquire(-1), { name: 'RangeError' });
    // Called on another object, a method fails before reaching the addon
    assert.throws(() => Connection.prototype.sendSync.call({}, 'x'), TypeError);
    closeAll(server, client, listener);
  });

  test('timeouts: NO_WAIT, a fraction of a millisecond, a negative number and undefined', async () => {
    const { listener, server, client } = await pair(4096, 'timeouts');
    throwsWith(assert, () => server.receiveSync(NO_WAIT), 'FIPC_TIMEOUT');
    await rejectsWith(assert, server.receive(NO_WAIT), 'FIPC_TIMEOUT');
    await rejectsWith(assert, server.receive(0.2), 'FIPC_TIMEOUT');
    let started = performance.now();
    await rejectsWith(assert, server.receive(100), 'FIPC_TIMEOUT');
    const waited = performance.now() - started;
    assert.ok(waited >= 90 && waited < 5000, `waited ${waited} ms`);
    started = performance.now();
    throwsWith(assert, () => server.receiveSync(60), 'FIPC_TIMEOUT');
    const waitedSync = performance.now() - started;
    assert.ok(waitedSync >= 50, `waited ${waitedSync} ms`);
    const forever = server.receive(-5);
    const alsoForever = server.receive(); // queued behind it
    await client.send('one');
    await client.send('two');
    assert.equal((await forever).toString(), 'one');
    assert.equal((await alsoForever).toString(), 'two');
    closeAll(server, client, listener);
  });
});
