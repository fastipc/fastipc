// Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both ways, with
// plain messages and RPC. The interpreter is FIPC_TEST_PYTHON, else the repository's venv, else python; it needs cffi.
// Without one the tests are skipped, unless FIPC_REQUIRE_INTEROP is set.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, test } from 'node:test';

import {
  Connection,
  Listener,
  NO_WAIT,
  RPC_REQUEST,
  RPC_RESPONSE,
  TEN_SECONDS,
  closeAll,
  exitCode,
  findPython,
  pattern,
  rejectsWith,
  repository,
  start,
  uniqueName,
} from './support.mjs';

// The Python peer: peer.py client|server <name> <bindings/python>
const PYTHON_PEER = `
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A plain connection: echo each message reversed until the server's end
    with Conn.connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
    # An RPC connection: call the JavaScript server, then answer its call
    with Conn.connect(name, timeout_ms=20000) as conn:
        request_id = conn.rpc_submit(7, "hello from Python".encode(), timeout_ms=20000)
        reply = conn.rpc_recv(timeout_ms=20000)
        assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
        request = conn.rpc_recv(timeout_ms=20000)
        assert request.kind == RPC_REQUEST and request.opcode == 9, request
        conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2, timeout_ms=20000)
        try:
            conn.rpc_recv(timeout_ms=20000)
            raise AssertionError("expected the server's end")
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                request = conn.rpc_recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(), timeout_ms=20000)
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

  test('a JavaScript server and a Python client: plain messages, then RPC both ways', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_client');
    const listener = Listener.listen(name, 64 * 1024);
    const peer = startPython('client', name);

    let conn = await listener.accept(30000);
    for (const size of [1, 100, 70000, 200000]) {
      const message = pattern(size, size);
      await conn.send(message, TEN_SECONDS); // longer than the ring: Python receives it as it comes
      assert.ok((await conn.receive(TEN_SECONDS)).equals(Buffer.from(message).reverse()), `size ${size}`);
    }
    conn.close();

    conn = await listener.accept(30000);
    const request = await conn.rpcReceive(TEN_SECONDS);
    assert.equal(request.kind, RPC_REQUEST);
    assert.equal(request.opcode, 7);
    assert.equal(request.payload.toString(), 'hello from Python');
    await conn.rpcRespond(request.id, 7, 42, 'HELLO FROM PYTHON', TEN_SECONDS);

    const id = await conn.rpcSubmit(9, 'JavaScript', TEN_SECONDS);
    const reply = await conn.rpcReceive(TEN_SECONDS);
    assert.equal(reply.kind, RPC_RESPONSE);
    assert.equal(reply.id, id);
    assert.equal(reply.status, 10);
    assert.equal(reply.payload.toString(), 'JavaScriptJavaScript');
    closeAll(conn, listener);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a Python server and a JavaScript client over RPC; the client\'s end ends the server', SLOW, async (t) => {
    if (!python) return t.skip('no Python with cffi (set FIPC_TEST_PYTHON)');
    const name = uniqueName('py_server');
    const peer = startPython('server', name);
    const conn = await Connection.connect(name, 30000);
    const first = await conn.rpcSubmit(1, 'shared memory', TEN_SECONDS);
    const big = pattern(300000);
    const sending = conn.rpcSubmit(2, big, TEN_SECONDS);
    const one = await conn.rpcReceive(TEN_SECONDS);
    assert.equal(one.id, first);
    assert.equal(one.status, -1);
    assert.equal(one.payload.toString(), 'SHARED MEMORY');
    const two = await conn.rpcReceive(TEN_SECONDS);
    assert.equal(two.id, await sending);
    assert.ok(two.payload.equals(big.map((b) => (b >= 0x61 && b <= 0x7a ? b - 32 : b)))); // bytes.upper()
    await rejectsWith(assert, conn.rpcReceive(NO_WAIT), 'FIPC_TIMEOUT');
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
  });
});
