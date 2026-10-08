// Other processes: peers in this runtime (tests/peer.mjs), killed, ended and restarted; the examples in pairs.
import assert from 'node:assert/strict';
import path from 'node:path';
import { describe, test } from 'node:test';

import {
  Connection,
  Listener,
  NO_WAIT,
  TEN_SECONDS,
  binding,
  closeAll,
  exitCode,
  pattern,
  rejectsWith,
  scriptCommand,
  sleep,
  start,
  startPeer,
  uniqueName,
} from './support.mjs';

const SLOW = { timeout: 120000 };

/** Receives until the peer's end; the messages received */
async function untilEnd(conn) {
  const messages = [];
  for (;;) {
    try {
      messages.push((await conn.receive(TEN_SECONDS)).toString());
    } catch (error) {
      assert.equal(error.code, 'FIPC_DISCONNECTED');
      return messages;
    }
  }
}

describe('other processes', () => {
  test('an echo peer: messages of every size, async', SLOW, async () => {
    const name = uniqueName('echo');
    const listener = Listener.listen(name, 64 * 1024);
    const peer = startPeer('echo', name);
    const conn = await listener.accept(30000);
    for (const size of [1, 100, 70000, 300000]) {
      const message = pattern(size, size);
      const [, echoed] = await Promise.all([conn.send(message, TEN_SECONDS), conn.receive(TEN_SECONDS)]);
      assert.ok(echoed.equals(message), `size ${size}`);
    }
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
    listener.close();
  });

  test('an echo peer that uses the Sync calls only', SLOW, async () => {
    const name = uniqueName('sync_echo');
    const listener = Listener.listen(name, 4096);
    const peer = startPeer('sync_echo', name);
    const conn = await listener.accept(30000);
    for (let i = 0; i < 200; i++) {
      const message = pattern(1 + (i * 97) % 9000, i);
      conn.sendSync(message, TEN_SECONDS);
      assert.ok((await conn.receive(TEN_SECONDS)).equals(message));
    }
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
    listener.close();
  });

  test('an RPC peer answers in order; requests pipelined', SLOW, async () => {
    const name = uniqueName('echo_rpc');
    const listener = Listener.listen(name, 64 * 1024);
    const peer = startPeer('echo_rpc', name);
    const conn = await listener.accept(30000);
    const payloads = Array.from({ length: 100 }, (_, i) => pattern(i * 1000 + 1, i));
    // Submitted at once and received meanwhile: the responses outgrow the ring before the last request goes
    const submitting = Promise.all(payloads.map((p, i) => conn.rpcSubmit(i, p, TEN_SECONDS)));
    const responses = [];
    for (let i = 0; i < payloads.length; i++) responses.push(await conn.rpcReceive(TEN_SECONDS));
    const ids = await submitting;
    for (let i = 0; i < payloads.length; i++) {
      const response = responses[i];
      assert.equal(response.id, ids[i]);
      assert.equal(response.opcode, i);
      assert.equal(response.status, payloads[i].length);
      assert.ok(response.payload.equals(Buffer.from(payloads[i]).reverse()));
    }
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
    listener.close();
  });

  test('a zero-copy echo peer', SLOW, async () => {
    const name = uniqueName('zerocopy_echo');
    const listener = Listener.listen(name, 8192);
    const peer = startPeer('zerocopy_echo', name);
    const conn = await listener.accept(30000);
    for (const size of [1, 64, 8128, 8129, 50000]) {
      const message = pattern(size, size);
      await conn.send(message, TEN_SECONDS);
      assert.ok((await conn.receive(TEN_SECONDS)).equals(message), `size ${size}`);
    }
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
    listener.close();
  });

  test('a peer that sends and exits: every message arrives, then FIPC_DISCONNECTED', SLOW, async () => {
    const name = uniqueName('send_and_exit');
    const listener = Listener.listen(name, 64 * 1024);
    const peer = startPeer('send_and_exit', name, 100);
    const conn = await listener.accept(30000);
    const messages = await untilEnd(conn);
    assert.deepEqual(messages, Array.from({ length: 100 }, (_, i) => `message ${i + 1}`));
    assert.equal(conn.ended, true);
    assert.equal(await exitCode(peer), 0, peer.output());
    closeAll(conn, listener);
  });

  test('a killed peer: a pending receive fails with FIPC_DISCONNECTED at once', SLOW, async () => {
    const name = uniqueName('killed');
    const listener = Listener.listen(name, 4096);
    const peer = startPeer('hang', name);
    const conn = await listener.accept(30000);
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'ready');
    const pending = conn.receive(); // no timeout: only the peer's end ends it
    await sleep(50);
    const killed = Date.now();
    peer.process.kill('SIGKILL');
    await rejectsWith(assert, pending, 'FIPC_DISCONNECTED');
    assert.ok(Date.now() - killed < 5000, `seen after ${Date.now() - killed} ms`);
    await exitCode(peer);
    closeAll(conn, listener);
  });

  test('the server\'s end reaches a peer that waits; the next client gets in after a restart', SLOW, async () => {
    const name = uniqueName('restart');
    let listener = Listener.listen(name, 4096);
    const peer = startPeer('expect_end', name);
    let conn = await listener.accept(30000);
    conn.close();
    listener.close();
    assert.equal(await exitCode(peer), 0, peer.output());

    listener = Listener.listen(name, 4096); // a new server on the same name
    const second = startPeer('echo', name);
    conn = await listener.accept(30000);
    await conn.send('again');
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'again');
    conn.close();
    assert.equal(await exitCode(second), 0, second.output());
    listener.close();
  });

  test('a server in another process; this process the client, connecting first', SLOW, async () => {
    const name = uniqueName('rpc_server');
    const connecting = Connection.connect(name, 30000);
    await sleep(100);
    const peer = startPeer('rpc_server', name);
    const conn = await connecting;
    const id = await conn.rpcSubmit(3, 'shared memory');
    const response = await conn.rpcReceive(TEN_SECONDS);
    assert.equal(response.id, id);
    assert.equal(response.status, -1);
    assert.equal(response.payload.toString(), 'SHARED MEMORY');
    await rejectsWith(assert, conn.rpcReceive(NO_WAIT), 'FIPC_TIMEOUT');
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  for (const [server, client, expected] of [
    ['server.mjs', 'client.mjs', ['PING']],
    ['echo_server.mjs', 'echo_client.mjs', ['hello', 'shared', 'memory']],
    ['rpc_server.mjs', 'rpc_client.mjs', ['true PING', 'true SHARED', 'true MEMORY']],
  ]) {
    test(`the examples ${server} and ${client}`, SLOW, async () => {
      const examples = path.join(binding, 'examples');
      const serving = start(scriptCommand(path.join(examples, server)), { cwd: examples });
      await sleep(300);
      const calling = start(scriptCommand(path.join(examples, client)), { cwd: examples });
      assert.equal(await exitCode(calling), 0, calling.output());
      assert.deepEqual(calling.output().trim().split(/\r?\n/), expected);
      assert.equal(await exitCode(serving), 0, serving.output());
    });
  }
});
