// Lifetimes: what keeps a process alive, the garbage collector closing dropped handles (and views keeping theirs),
// worker threads and their teardown.
import assert from 'node:assert/strict';
import path from 'node:path';
import { describe, test } from 'node:test';
import { Worker } from 'node:worker_threads';

import {
  Connection,
  Listener,
  TEN_SECONDS,
  closeAll,
  exitCode,
  here,
  pair,
  rejectsWith,
  scriptCommand,
  sleep,
  start,
  startPeer,
  uniqueName,
} from './support.mjs';

const SLOW = { timeout: 120000 };

// node --expose-gc, deno --v8-flags=--expose-gc, or Bun's own
const collect = typeof globalThis.gc === 'function' ? () => globalThis.gc()
  : typeof globalThis.Bun?.gc === 'function' ? () => globalThis.Bun.gc(true) : null;
const NO_GC = { skip: collect ? false : 'no garbage collector to call (node --expose-gc)', timeout: 60000 };

/** Collects garbage until `done()` resolves truthy (finalizers may run a turn of the loop later); false after 5 s. */
async function collectUntil(done) {
  for (const started = Date.now(); Date.now() - started < 5000;) {
    collect();
    await sleep(20);
    if (await done()) return true;
  }
  return false;
}

/** Whether the peer has ended `conn`: its receive fails with FIPC_DISCONNECTED (a short wait otherwise) */
async function peerEnded(conn) {
  try {
    await conn.receive(10);
    return false;
  } catch (error) {
    if (error.code === 'FIPC_TIMEOUT') return false;
    assert.equal(error.code, 'FIPC_DISCONNECTED');
    return true;
  }
}

describe('lifetimes', () => {
  test('an idle connection doesn\'t keep a process alive; a pending call does, until it settles', SLOW, async () => {
    const name = uniqueName('idle');
    const listener = Listener.listen(name, 4096);
    const idle = startPeer('idle_exit', name);
    let conn = await listener.accept(30000);
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'hi');
    await conn.send('bye');
    assert.equal(await exitCode(idle, 15000), 0, idle.output()); // ends with its connection open
    await rejectsWith(assert, conn.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
    conn.close();

    const pending = startPeer('pending', name);
    conn = await listener.accept(30000);
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'ready');
    const exited = await Promise.race([pending.exit.then(() => true), sleep(500).then(() => false)]);
    assert.equal(exited, false, 'the process waits for its pending receive');
    await conn.send('go');
    assert.equal(await exitCode(pending, 15000), 0, pending.output());
    assert.equal(pending.output().trim(), 'go');
    closeAll(conn, listener);
  });

  test('the garbage collector closes a connection the program dropped', NO_GC, async () => {
    const { listener, client } = await (async () => {
      const { listener: l, server, client: c } = await pair(4096, 'gc_conn');
      await server.send('before'); // the server's Connection is out of reach once this returns
      return { listener: l, client: c };
    })();
    assert.equal((await client.receive(TEN_SECONDS)).toString(), 'before');
    assert.ok(await collectUntil(() => peerEnded(client)), 'the dropped connection was never closed');
    closeAll(client, listener);
  });

  test('the garbage collector closes a listener the program dropped: its name is free again', NO_GC, async () => {
    const name = uniqueName('gc_listener');
    (() => Listener.listen(name, 4096))();
    const freed = await collectUntil(() => {
      try {
        Listener.listen(name, 4096).close();
        return true;
      } catch (error) {
        assert.equal(error.code, 'FIPC_ADDR_IN_USE');
        return false;
      }
    });
    assert.ok(freed, 'the dropped listener kept its name');
  });

  test('a zero-copy view keeps its connection open; once it is dropped too, the connection closes', NO_GC, async () => {
    const held = {};
    const { listener, client } = await (async () => {
      const { listener: l, server, client: c } = await pair(4096, 'gc_view');
      await c.send('in the ring');
      held.view = await server.receiveAcquire(TEN_SECONDS);
      return { listener: l, client: c };
    })();
    assert.equal(await collectUntil(() => peerEnded(client)), false, 'the connection closed under its view');
    assert.equal(Buffer.from(held.view).toString(), 'in the ring'); // still mapped
    delete held.view;
    assert.ok(await collectUntil(() => peerEnded(client)), 'the connection outlived its view');
    closeAll(client, listener);
  });

  test('a worker thread connects and calls; a terminated worker\'s connection ends', SLOW, async () => {
    const name = uniqueName('worker');
    const listener = Listener.listen(name, 4096);
    const script = path.join(here, 'worker.mjs');

    const worker = new Worker(script, { workerData: { name, role: 'rpc' } });
    const result = new Promise((resolve, reject) => {
      worker.once('message', resolve);
      worker.once('error', reject);
    });
    let conn = await listener.accept(30000);
    const request = await conn.rpcReceive(TEN_SECONDS);
    await conn.rpcRespond(request.id, request.opcode, 0, request.payload.toString().toUpperCase());
    assert.deepEqual(await result, { id: request.id, response: 'FROM A WORKER' });
    await rejectsWith(assert, conn.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
    conn.close();

    const hanging = new Worker(script, { workerData: { name, role: 'hang' } });
    const ready = new Promise((resolve, reject) => {
      hanging.once('message', resolve);
      hanging.once('error', reject);
    });
    conn = await listener.accept(30000);
    assert.equal(await ready, 'ready');
    await hanging.terminate(); // its environment's teardown closes the connection it left open, mid-receive
    await rejectsWith(assert, conn.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
    closeAll(conn, listener);
    await sleep(50);
    assert.ok(Connection); // this process goes on
  });

  test('a worker terminated while it connects: the connect ends later, without its environment', SLOW, async () => {
    const worker = new Worker(path.join(here, 'worker.mjs'), { workerData: { name: uniqueName('nobody'), role: 'connect' } });
    const ready = new Promise((resolve, reject) => {
      worker.once('message', resolve);
      worker.once('error', reject);
    });
    assert.equal(await ready, 'ready');
    await worker.terminate();
    await sleep(1000); // past the connect's timeout: its thread finishes after the worker's teardown
    assert.ok(Connection); // this process goes on

    // In a process that never loaded the binding on its own thread, the worker's environment was the binding's last
    const host = start(scriptCommand(path.join(here, 'worker_host.mjs'), [uniqueName('nobody'), 'connect']));
    assert.equal(await exitCode(host), 0, host.output());
    assert.match(host.output(), /lived on/);
  });
});
