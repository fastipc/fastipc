// Lifetimes: what keeps a process alive, the garbage collector closing dropped handles (and views keeping theirs; a
// service's server dropped holding a request, which its close abandons), a work queue and a service it never closes,
// worker threads and their teardown.
import assert from 'node:assert/strict';
import path from 'node:path';
import { describe, test } from 'node:test';
import { Worker } from 'node:worker_threads';

import {
  CLIENT,
  CONSUMER,
  Connection,
  PRODUCER,
  SERVER,
  TEN_SECONDS,
  closeAll,
  createQueue,
  createService,
  createStream,
  exitCode,
  here,
  pair,
  joinQueue,
  joinService,
  rejectsWith,
  scriptCommand,
  sleep,
  start,
  startPeer,
  throwsWith,
  uniqueName,
} from './support.mjs';

const SLOW = { timeout: 120000 };

// node --expose-gc, deno --v8-flags=--expose-gc, or Bun's own
const collect = typeof globalThis.gc === 'function' ? () => globalThis.gc()
  : typeof globalThis.Bun?.gc === 'function' ? () => globalThis.Bun.gc(true) : null;
const NO_GC = { skip: collect ? false : 'no garbage collector to call (node --expose-gc)', timeout: 60000 };

/** Collects garbage until `done()` resolves truthy (finalizers may run a turn of the loop later); false after 5 s. */
async function collectUntil(done) {
  for (const started = performance.now(); performance.now() - started < 5000;) {
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

/** A worker's first message (its error, if it fails first) */
function workerMessage(worker) {
  return new Promise((resolve, reject) => {
    worker.once('message', resolve);
    worker.once('error', reject);
  });
}

describe('lifetimes', () => {
  test('an idle connection doesn\'t keep a process alive; a pending call does, until it settles', SLOW, async () => {
    const name = uniqueName('idle');
    const listener = createStream(name, 4096);
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

  test('a service\'s server dropped holding a request: the garbage collector closes it, which abandons the request',
    NO_GC, async () => {
      const name = uniqueName('gc_server');
      const service = createService(name, 4096, 64, 1, 1);
      const client = await joinService(name, CLIENT, TEN_SECONDS);
      await (async () => {
        const server = await joinService(name, SERVER, TEN_SECONDS);
        await client.send('held', TEN_SECONDS);
        // The server's Connection is out of reach once this returns, holding the request: no reply ever comes
        assert.equal((await server.receive(TEN_SECONDS)).toString(), 'held');
      })();
      const abandoned = async () => {
        try {
          await client.receive(10);
        } catch (error) {
          if (error.code === 'FIPC_TIMEOUT') return false;
          assert.equal(error.code, 'FIPC_ABANDONED');
          return true;
        }
        assert.fail('a reply came from a server that was dropped');
      };
      assert.ok(await collectUntil(abandoned), 'the dropped server was never closed');
      // Its place is free again: the next server answers the next call
      const next = await joinService(name, SERVER, TEN_SECONDS);
      const answering = next.receive(TEN_SECONDS).then((request) => next.send(request.toString().toUpperCase()));
      assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT');
      await answering;
      closeAll(next, client, service);
    });

  test('the garbage collector closes a listener the program dropped: its name is free again', NO_GC, async () => {
    const name = uniqueName('gc_listener');
    (() => createStream(name, 4096))();
    const freed = await collectUntil(() => {
      try {
        createStream(name, 4096).close();
        return true;
      } catch (error) {
        assert.equal(error.code, 'FIPC_ADDR_IN_USE');
        return false;
      }
    });
    assert.ok(freed, 'the dropped listener kept its name');
  });

  test('a work queue the program dropped goes on: the garbage collector never closes it', NO_GC, async () => {
    const name = uniqueName('gc_queue');
    const { producer, dropped } = await (async () => {
      const queue = createQueue(name, 4096, 1, 1); // dropped once this returns: only its producer is kept
      return { producer: await joinQueue(name, PRODUCER, TEN_SECONDS), dropped: new WeakRef(queue) };
    })();
    // A deref that finds the Queue keeps it until the end of the task: collect at a task's start, then look
    let collected = false;
    for (const started = performance.now(); !collected && performance.now() - started < 5000;) {
      await sleep(20);
      collect();
      collected = dropped.deref() === undefined;
    }
    assert.ok(collected, 'the dropped Queue was never collected');
    for (let i = 0; i < 5; i++) {
      collect();
      await sleep(20); // finalizers may run a turn of the loop later
    }
    await producer.send('after the collections', TEN_SECONDS);
    const consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
    assert.equal((await consumer.receive(TEN_SECONDS)).toString(), 'after the collections');
    throwsWith(assert, () => createQueue(name, 4096, 1, 1), 'FIPC_ADDR_IN_USE'); // the queue still holds its name
    closeAll(producer, consumer);
  });

  test('a service the program dropped goes on: the garbage collector never closes it', NO_GC, async () => {
    const name = uniqueName('gc_service');
    const { server, dropped } = await (async () => {
      const service = createService(name, 4096, 64, 1, 1); // dropped once this returns: only its server is kept
      return { server: await joinService(name, SERVER, TEN_SECONDS), dropped: new WeakRef(service) };
    })();
    let collected = false;
    for (const started = performance.now(); !collected && performance.now() - started < 5000;) {
      await sleep(20);
      collect();
      collected = dropped.deref() === undefined;
    }
    assert.ok(collected, 'the dropped Service was never collected');
    for (let i = 0; i < 5; i++) {
      collect();
      await sleep(20); // finalizers may run a turn of the loop later
    }
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const answering = (async () => server.send((await server.receive(TEN_SECONDS)).toString().toUpperCase()))();
    assert.equal((await client.call('still here', TEN_SECONDS)).toString(), 'STILL HERE');
    await answering;
    throwsWith(assert, () => createService(name, 4096, 64, 1, 1), 'FIPC_ADDR_IN_USE'); // it still holds its name
    closeAll(server, client);
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
    const listener = createStream(name, 4096);
    const script = path.join(here, 'worker.mjs');

    const worker = new Worker(script, { workerData: { name, role: 'call' } });
    const result = new Promise((resolve, reject) => {
      worker.once('message', resolve);
      worker.once('error', reject);
    });
    let conn = await listener.accept(30000);
    const request = await conn.receive(TEN_SECONDS);
    await conn.send(request.toString().toUpperCase());
    assert.equal(await result, 'FROM A WORKER');
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

  test('a terminated worker\'s queue ends with its environment: its consumer gets the end, the name is free', SLOW,
    async () => {
      const name = uniqueName('worker_queue');
      const worker = new Worker(path.join(here, 'worker.mjs'), { workerData: { name, role: 'queue' } });
      const ready = new Promise((resolve, reject) => {
        worker.once('message', resolve);
        worker.once('error', reject);
      });
      assert.equal(await ready, 'ready');
      const consumer = await joinQueue(name, CONSUMER, TEN_SECONDS);
      assert.equal((await consumer.receive(TEN_SECONDS)).toString(), 'from a worker');
      await worker.terminate(); // its environment's teardown closes the queue, its waitForMembers in progress
      await rejectsWith(assert, consumer.receive(TEN_SECONDS), 'FIPC_DISCONNECTED');
      consumer.close();
      createQueue(name, 4096, 1, 1).close();
    });

  test('a terminated worker\'s service ends with its environment: its client gets the end, the name is free', SLOW,
    async () => {
      const name = uniqueName('worker_service');
      const worker = new Worker(path.join(here, 'worker.mjs'), { workerData: { name, role: 'service' } });
      assert.equal(await workerMessage(worker), 'ready');
      const client = await joinService(name, CLIENT, TEN_SECONDS);
      assert.equal((await client.call('from main', TEN_SECONDS)).toString(), 'FROM MAIN');
      await worker.terminate(); // its environment's teardown closes the service and its server, mid-receive
      // The name is free once the teardown has closed the service (Deno's terminate may settle before that)
      for (const started = performance.now(); ;) {
        try {
          createService(name, 4096, 64, 1, 1).close();
          break;
        } catch (error) {
          assert.equal(error.code, 'FIPC_ADDR_IN_USE');
          assert.ok(performance.now() - started < TEN_SECONDS, 'the terminated worker\'s service kept its name');
          await sleep(10);
        }
      }
      await rejectsWith(assert, client.call('after', TEN_SECONDS), 'FIPC_DISCONNECTED');
      client.close();
    });

  test('a terminated worker\'s server abandons the request it holds; the service goes on', SLOW, async () => {
    const name = uniqueName('worker_server');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const worker = new Worker(path.join(here, 'worker.mjs'), { workerData: { name, role: 'holding_server' } });
    const holding = workerMessage(worker);
    assert.equal((await client.call('first', TEN_SECONDS)).toString(), 'FIRST');
    await client.send('held', TEN_SECONDS);
    assert.equal(await holding, 'holding');
    await worker.terminate(); // its environment's teardown closes its server, which holds the request
    await rejectsWith(assert, client.receive(TEN_SECONDS), 'FIPC_ABANDONED');
    const server = await joinService(name, SERVER, TEN_SECONDS);
    const answering = (async () => server.send((await server.receive(TEN_SECONDS)).toString().toUpperCase()))();
    assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT');
    await answering;
    closeAll(server, client, service);
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

    // In a process that never loads the binding on its own thread, the worker's environment is the binding's last:
    // its teardown must leave the process running
    const host = start(scriptCommand(path.join(here, 'worker_host.mjs'), [uniqueName('nobody'), 'connect']));
    assert.equal(await exitCode(host), 0, host.output());
    assert.match(host.output(), /lived on/);
  });
});
