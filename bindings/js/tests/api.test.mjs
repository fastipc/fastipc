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
  binding,
  closeAll,
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
    for (const name of ['Listener', 'Connection', 'FipcError', 'Result', 'resultStr', 'NO_WAIT', 'FOREVER',
      'RPC_REQUEST', 'RPC_RESPONSE', 'version', 'library', 'addon']) {
      assert.equal(esm[name], cjs[name], name);
    }
    assert.equal(esm.default, cjs);
  });

  test('constants, version and paths', () => {
    assert.equal(fipc.NO_WAIT, 0);
    assert.equal(fipc.FOREVER, -1);
    assert.equal(fipc.RPC_REQUEST, 1);
    assert.equal(fipc.RPC_RESPONSE, 2);
    const pkg = JSON.parse(readFileSync(path.join(binding, 'package.json'), 'utf8'));
    assert.equal(fipc.version, pkg.version);
    assert.match(path.basename(fipc.addon), /^fipc\.node$/);
    assert.match(path.basename(fipc.library), /^(fastipc\.dll|libfastipc\.so|libfastipc\.dylib)$/);
  });

  test('results and their names', () => {
    assert.deepEqual(Object.entries(fipc.Result), [['OK', 0], ['TIMEOUT', 1], ['DISCONNECTED', 2], ['CANCELLED', 3],
      ['TOO_LARGE', 4], ['INVALID', 5], ['NO_MEMORY', 6], ['ADDR_IN_USE', 7]]);
    assert.ok(Object.isFrozen(fipc.Result));
    assert.equal(fipc.resultStr(0), 'FIPC_OK');
    assert.equal(fipc.resultStr(7), 'FIPC_ADDR_IN_USE');
    assert.equal(fipc.resultStr(8), 'FIPC_UNKNOWN');
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

  test('listeners and connections are made by listen, accept and connect only', () => {
    assert.throws(() => new Listener(), TypeError);
    assert.throws(() => new Connection(), TypeError);
  });
});

describe('listeners', () => {
  test('listen claims a name; a second listener gets FIPC_ADDR_IN_USE until the first closes', () => {
    const name = uniqueName('in_use');
    const listener = Listener.listen(name, 4096);
    assert.equal(listener.name, name);
    assert.equal(listener.closed, false);
    const error = throwsWith(assert, () => Listener.listen(name, 4096), 'FIPC_ADDR_IN_USE');
    assert.equal(error.call, 'listen');
    listener.close();
    assert.equal(listener.closed, true);
    listener.close(); // again: nothing
    Listener.listen(name, 4096).close();
  });

  test('a bad name or capacity is FIPC_INVALID; a wrong type is a TypeError', () => {
    throwsWith(assert, () => Listener.listen('', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => Listener.listen('.hidden', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => Listener.listen('a/b', 4096), 'FIPC_INVALID');
    throwsWith(assert, () => Listener.listen('x'.repeat(246), 4096), 'FIPC_INVALID');
    throwsWith(assert, () => Listener.listen('a\0b', 4096), 'FIPC_INVALID'); // not 'a': a NUL is in no name
    throwsWith(assert, () => Listener.listen(uniqueName('cap'), 1000), 'FIPC_INVALID'); // not a power of two
    throwsWith(assert, () => Listener.listen(uniqueName('cap'), 512), 'FIPC_INVALID'); // under 1 KiB
    assert.throws(() => Listener.listen(42, 4096), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => Listener.listen('x', '4096'), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => Listener.listen('x', 4096.5), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' });
    assert.throws(() => Listener.listen('x', -1), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' });
  });

  test('accept times out; acceptSync with NO_WAIT polls', async () => {
    const listener = Listener.listen(uniqueName('accept_timeout'), 4096);
    const started = Date.now();
    const error = await rejectsWith(assert, listener.accept(50), 'FIPC_TIMEOUT');
    assert.equal(error.call, 'accept');
    assert.ok(Date.now() - started >= 40);
    assert.equal(throwsWith(assert, () => listener.acceptSync(NO_WAIT), 'FIPC_TIMEOUT').call, 'acceptSync');
    listener.close();
  });

  test('cancel ends a pending accept and every later one', async () => {
    const listener = Listener.listen(uniqueName('accept_cancel'), 4096);
    const pending = listener.accept();
    setTimeout(() => listener.cancel(), 20);
    await rejectsWith(assert, pending, 'FIPC_CANCELLED');
    await rejectsWith(assert, listener.accept(), 'FIPC_CANCELLED');
    throwsWith(assert, () => listener.acceptSync(), 'FIPC_CANCELLED');
    listener.close();
  });

  test('close ends a pending accept with FIPC_CANCELLED; calls on a closed listener fail with ERR_FIPC_CLOSED', async () => {
    const listener = Listener.listen(uniqueName('accept_close'), 4096);
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
    const [second, secondClient] = await Promise.all([listener.accept(5000), Connection.connect(listener.name, 5000)]);
    closeAll(second, secondClient, client, listener);
  });

  test('an acceptSync beside a pending accept is ERR_FIPC_BUSY; a second accept queues behind it', async () => {
    const listener = Listener.listen(uniqueName('accept_busy'), 4096);
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
    const listener = Listener.listen(uniqueName('dispose'), 4096);
    listener[Symbol.dispose]();
    assert.equal(listener.closed, true);
  });
});

describe('connections', () => {
  test('connect times out without a server; connectSync too', async () => {
    const name = uniqueName('no_server');
    const error = await rejectsWith(assert, Connection.connect(name, 50), 'FIPC_TIMEOUT');
    assert.equal(error.call, 'connect');
    assert.equal(throwsWith(assert, () => Connection.connectSync(name, 0), 'FIPC_TIMEOUT').call, 'connectSync');
    await rejectsWith(assert, Connection.connect(`${name}\0x`, 50), 'FIPC_INVALID'); // not name: a NUL is in no name
    throwsWith(assert, () => Connection.connectSync(`${name}\0x`, 0), 'FIPC_INVALID');
    await assert.rejects(Connection.connect(42), { code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => Connection.connectSync(name, 'soon'), { code: 'ERR_INVALID_ARG_TYPE' });
    assert.throws(() => Connection.connectSync(name, Number.NaN), { code: 'ERR_INVALID_ARG_TYPE' });
  });

  test('a client that connects first waits for the server to listen', async () => {
    const name = uniqueName('client_first');
    const connecting = Connection.connect(name, 10000);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const listener = Listener.listen(name, 4096);
    const [server, client] = await Promise.all([listener.accept(10000), connecting]);
    closeAll(server, client, listener);
  });

  test('connectSync on this thread against an async accept', async () => {
    const name = uniqueName('connect_sync');
    const listener = Listener.listen(name, 4096);
    const accepting = listener.accept(10000);
    const client = Connection.connectSync(name, 10000);
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
    await assert.rejects(client.rpcSubmit(-1, 'x'), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' });
    await assert.rejects(client.rpcSubmit(2 ** 32, 'x'), { name: 'RangeError' });
    await assert.rejects(client.rpcSubmit('1', 'x'), { name: 'TypeError' });
    await assert.rejects(client.rpcRespond(1, 1, 2 ** 31, 'x'), { name: 'RangeError' });
    await assert.rejects(client.rpcRespond(-1n, 1, 0, 'x'), { name: 'RangeError' });
    await assert.rejects(client.rpcRespond(1.5, 1, 0, 'x'), { name: 'RangeError' });
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
    let started = Date.now();
    await rejectsWith(assert, server.receive(100), 'FIPC_TIMEOUT');
    const waited = Date.now() - started;
    assert.ok(waited >= 90 && waited < 5000, `waited ${waited} ms`);
    started = Date.now();
    throwsWith(assert, () => server.receiveSync(60), 'FIPC_TIMEOUT');
    assert.ok(Date.now() - started >= 50);
    const forever = server.receive(-5);
    const alsoForever = server.receive(); // queued behind it
    await client.send('one');
    await client.send('two');
    assert.equal((await forever).toString(), 'one');
    assert.equal((await alsoForever).toString(), 'two');
    closeAll(server, client, listener);
  });
});
