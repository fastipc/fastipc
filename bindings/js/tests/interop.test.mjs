// Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both ways, on
// a stream, a broadcast (the Python binding's tests/broadcast_peer.py), a work queue (its tests/queue_peer.py) and a
// service (its tests/service_peer.py, a server killed holding a request too). The interpreter is FIPC_TEST_PYTHON,
// else the repository's venv, else python; it needs cffi. Without one the tests are skipped, unless
// FIPC_REQUIRE_INTEROP is set.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, test } from 'node:test';

import {
  CLIENT,
  NO_WAIT,
  SERVER,
  TEN_SECONDS,
  closeAll,
  connectStream,
  createService,
  createStream,
  exitCode,
  joinService,
  findPython,
  pattern,
  rejectsWith,
  repository,
  sleep,
  start,
  uniqueName,
} from './support.mjs';
import { publish, subscribe } from './broadcast.mjs';
import { checkSplit, consume, joinAndProduce, ownAndConsume, produce } from './queue.mjs';
import { joinAndAnswer, joinAndCall, ownAndCall, serve, upper } from './service.mjs';

const BROADCAST_PEER = path.join(repository, 'bindings', 'python', 'tests', 'broadcast_peer.py');
const QUEUE_PEER = path.join(repository, 'bindings', 'python', 'tests', 'queue_peer.py');
const SERVICE_PEER = path.join(repository, 'bindings', 'python', 'tests', 'service_peer.py');

// The Python peer: peer.py client|server <name> <bindings/python>
const PYTHON_PEER = `
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import FipcError, Result, stream_connect, stream_create

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A stream's client: echo each message reversed until the server's end
    with stream_connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    # Answer each message in upper case until the client's end
    with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                message = conn.recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.send(message.upper(), timeout_ms=20000)
`;

const SLOW = { timeout: 120000 };

describe('interop with Python', () => {
  let python;
  let folder;
  let script;

  before(async () => {
    python = await findPython();
    folder = mkdtempSync(path.join(os.tmpdir(), 'fipc-js-'));
    script = path.join(folder, 'peer.py');
    writeFileSync(script, PYTHON_PEER);
  });

  after(() => rmSync(folder, { recursive: true, force: true }));

  function startPython(mode, name) {
    return start([python, script, mode, name, path.join(repository, 'bindings', 'python')]);
  }

  test('a JavaScript server and a Python client: plain messages; the server\'s end ends the client', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_client');
    const listener = createStream(name, 64 * 1024);
    const peer = startPython('client', name);

    const conn = await listener.accept(30000);
    for (const size of [1, 100, 70000, 200000]) {
      const message = pattern(size, size);
      await conn.send(message, TEN_SECONDS); // longer than the ring: Python receives it as it comes
      assert.ok((await conn.receive(TEN_SECONDS)).equals(Buffer.from(message).reverse()), `size ${size}`);
    }
    closeAll(conn, listener);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python server and a JavaScript client; the client\'s end ends the server', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_server');
    const peer = startPython('server', name);
    const conn = await connectStream(name, 30000);
    await conn.send('shared memory', TEN_SECONDS);
    const big = pattern(300000);
    const sending = conn.send(big, TEN_SECONDS); // longer than the ring: it goes while the server answers
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'SHARED MEMORY');
    const two = await conn.receive(TEN_SECONDS);
    await sending;
    assert.ok(two.equals(big.map((b) => (b >= 0x61 && b <= 0x7a ? b - 32 : b)))); // bytes.upper()
    await rejectsWith(assert, conn.receive(NO_WAIT), 'FIPC_TIMEOUT');
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a JavaScript publisher; a gated and a lossy Python subscriber', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_bc_sub');
    const peers = ['gated', 'lossy'].map((role) => start([python, BROADCAST_PEER, 'subscriber', name, role, '300']));
    await publish(name, 300, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python publisher; a gated and a lossy JavaScript subscriber', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_bc_pub');
    const peer = start([python, BROADCAST_PEER, 'publisher', name, '300', '2']);
    await Promise.all([subscribe(name, true, 300), subscribe(name, false, 300)]);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  /** tests/queue_peer.py of the Python binding in another process: `mode` on `name` with `args` */
  function startQueuePeer(mode, name, ...args) {
    return start([python, QUEUE_PEER, mode, name, ...args.map(String)]);
  }

  test('a Python owner and producer; two JavaScript consumers split its jobs', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_q_prod');
    const peer = startQueuePeer('producer', name, 300, 2);
    checkSplit(await Promise.all([consume(name), consume(name)]), 300);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a JavaScript owner and producer; a Python consumer', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_q_cons');
    const peer = startQueuePeer('consumer', name, 300);
    await produce(name, 300, 1);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('two joined Python producers; the JavaScript owner\'s consumer', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_q_join');
    const peers = [startQueuePeer('joined-producer', name, 100), startQueuePeer('joined-producer', name, 100)];
    await ownAndConsume(name, 100, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python owner and consumer; two joined JavaScript producers', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_q_own');
    const peer = startQueuePeer('owner-consumer', name, 100, 2);
    await Promise.all([joinAndProduce(name, 100), joinAndProduce(name, 100)]);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  /** tests/service_peer.py of the Python binding in another process: `mode` on `name` with `args` */
  function startServicePeer(mode, name, ...args) {
    return start([python, SERVICE_PEER, mode, name, ...args.map(String)]);
  }

  test('a Python owner and server; a JavaScript client', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_s_server');
    const peer = startServicePeer('server', name, 300);
    await joinAndCall(name, 300);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a JavaScript owner and server; two Python clients', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_s_clients');
    const peers = [startServicePeer('client', name, 300), startServicePeer('client', name, 300)];
    await serve(name, 300, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('two joined Python servers; the JavaScript owner\'s client', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_s_joined');
    const peers = [startServicePeer('joined-server', name), startServicePeer('joined-server', name)];
    await ownAndCall(name, 300, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python owner and client; two joined JavaScript servers', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_s_owner');
    const peer = startServicePeer('owner-client', name, 300, 2);
    await Promise.all([joinAndAnswer(name), joinAndAnswer(name)]);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python server killed holding a JavaScript client\'s request abandons it', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_s_killed');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const peer = startServicePeer('holding-server', name);
    try {
      await client.send('doomed', TEN_SECONDS);
      for (const started = performance.now(); !peer.output().includes('holding');) {
        assert.ok(performance.now() - started < 30000, `the peer never held the request: ${peer.output()}`);
        await sleep(10);
      }
      peer.process.kill('SIGKILL');
      await exitCode(peer);
      await rejectsWith(assert, client.receive(TEN_SECONDS), 'FIPC_ABANDONED'); // once the owner has seen its end
      const server = await joinService(name, SERVER, TEN_SECONDS);
      const answering = (async () => server.send(upper(await server.receive(TEN_SECONDS))))();
      assert.equal((await client.call('next', TEN_SECONDS)).toString(), 'NEXT');
      await answering;
      server.close();
    } finally {
      peer.process.kill('SIGKILL');
      closeAll(client, service);
    }
  });
});
