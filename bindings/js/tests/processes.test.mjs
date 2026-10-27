// Other processes: peers in this runtime (tests/peer.mjs), killed, ended and restarted; a broadcast's publisher and
// subscribers; a work queue's owner, producers and consumers; a service's owner, clients and servers, and a server
// killed holding a request; the examples in pairs, in both start orders.
import assert from 'node:assert/strict';
import path from 'node:path';
import { describe, test } from 'node:test';

import {
  CLIENT,
  NO_WAIT,
  SERVER,
  TEN_SECONDS,
  binding,
  closeAll,
  connectStream,
  createService,
  createStream,
  exitCode,
  joinService,
  pattern,
  rejectsWith,
  scriptCommand,
  sleep,
  start,
  startPeer,
  uniqueName,
} from './support.mjs';
import { publish, subscribe } from './broadcast.mjs';
import { SIZES, checkSplit, ownAndConsume, produce } from './queue.mjs';
import { joinAndCall, ownAndCall, serve, upper } from './service.mjs';

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
    const listener = createStream(name, 64 * 1024);
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
    const listener = createStream(name, 4096);
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

  test('an echo peer answers in order; messages pipelined', SLOW, async () => {
    const name = uniqueName('echo_pipelined');
    const listener = createStream(name, 64 * 1024);
    const peer = startPeer('echo', name);
    const conn = await listener.accept(30000);
    const payloads = Array.from({ length: 100 }, (_, i) => pattern(i * 1000 + 1, i));
    // Sent at once and received meanwhile: the echoes outgrow the ring before the last message goes
    const sending = Promise.all(payloads.map((p) => conn.send(p, TEN_SECONDS)));
    const echoes = [];
    for (let i = 0; i < payloads.length; i++) echoes.push(await conn.receive(TEN_SECONDS));
    await sending;
    for (let i = 0; i < payloads.length; i++) assert.ok(echoes[i].equals(payloads[i]), `message ${i}`);
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
    listener.close();
  });

  test('a zero-copy echo peer', SLOW, async () => {
    const name = uniqueName('zerocopy_echo');
    const listener = createStream(name, 8192);
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
    const listener = createStream(name, 64 * 1024);
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
    const listener = createStream(name, 4096);
    const peer = startPeer('hang', name);
    const conn = await listener.accept(30000);
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'ready');
    const pending = conn.receive(); // no timeout: only the peer's end ends it
    await sleep(50);
    const killed = performance.now();
    peer.process.kill('SIGKILL');
    await rejectsWith(assert, pending, 'FIPC_DISCONNECTED');
    assert.ok(performance.now() - killed < 5000, `seen after ${performance.now() - killed} ms`);
    await exitCode(peer);
    closeAll(conn, listener);
  });

  test('the server\'s end reaches a peer that waits; the next client gets in after a restart', SLOW, async () => {
    const name = uniqueName('restart');
    let listener = createStream(name, 4096);
    const peer = startPeer('expect_end', name);
    let conn = await listener.accept(30000);
    conn.close();
    listener.close();
    assert.equal(await exitCode(peer), 0, peer.output());

    listener = createStream(name, 4096); // a new server on the same name
    const second = startPeer('echo', name);
    conn = await listener.accept(30000);
    await conn.send('again');
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'again');
    conn.close();
    assert.equal(await exitCode(second), 0, second.output());
    listener.close();
  });

  test('a server in another process; this process the client, connecting first', SLOW, async () => {
    const name = uniqueName('upper_server');
    const connecting = connectStream(name, 30000);
    await sleep(100);
    const peer = startPeer('upper_server', name);
    const conn = await connecting;
    await conn.send('shared memory');
    assert.equal((await conn.receive(TEN_SECONDS)).toString(), 'SHARED MEMORY');
    await rejectsWith(assert, conn.receive(NO_WAIT), 'FIPC_TIMEOUT');
    conn.close();
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a publisher here; a gated and a lossy subscriber in other processes', SLOW, async () => {
    const name = uniqueName('bc_pub');
    const peers = [startPeer('subscriber', name, 'gated', 300), startPeer('subscriber', name, 'lossy', 300)];
    await publish(name, 300, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a publisher in another process; a gated and a lossy subscriber here', SLOW, async () => {
    const name = uniqueName('bc_sub');
    const peer = startPeer('publisher', name, 300, 2);
    await Promise.all([subscribe(name, true, 300), subscribe(name, false, 300)]);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  /** Two queue_consumer peers on `name`: the job numbers each received */
  async function consumerPeers(name, ...args) {
    const peers = [startPeer('queue_consumer', name, ...args), startPeer('queue_consumer', name, ...args)];
    return peers.map(async (peer) => {
      assert.equal(await exitCode(peer), 0, peer.output());
      return JSON.parse(peer.output().trim().split(/\r?\n/).at(-1));
    });
  }

  test('the owner\'s producer here; two consumers in other processes split its jobs', SLOW, async () => {
    const name = uniqueName('q_own_p');
    const lists = await consumerPeers(name);
    await produce(name, 300, 2);
    checkSplit(await Promise.all(lists), 300);
  });

  test('two consumers in other processes race for messages of every length: each received once and whole', SLOW,
    async () => {
      const name = uniqueName('q_race_p');
      const lists = await consumerPeers(name, 'long');
      await produce(name, 300, 2, { capacity: 64 * 1024, size: SIZES.long });
      checkSplit(await Promise.all(lists), 300);
    });

  test('the owner\'s consumer here; two joined producers in other processes', SLOW, async () => {
    const name = uniqueName('q_own_c');
    const peers = [startPeer('queue_joined_producer', name, 100), startPeer('queue_joined_producer', name, 100)];
    await ownAndConsume(name, 100, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a service\'s server in another process; its client here', SLOW, async () => {
    const name = uniqueName('s_server_p');
    const peer = startPeer('service_server', name, 300, 1);
    await joinAndCall(name, 300);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('the owner\'s server here; two clients in other processes', SLOW, async () => {
    const name = uniqueName('s_clients_p');
    const peers = [startPeer('service_client', name, 300), startPeer('service_client', name, 300)];
    await serve(name, 300, 2);
    for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('the owner\'s client here (async calls, then callSync); two joined servers in other processes', SLOW,
    async () => {
      for (const sync of [false, true]) {
        const name = uniqueName('s_joined_p');
        const peers = [startPeer('service_joined_server', name), startPeer('service_joined_server', name)];
        await ownAndCall(name, 300, 2, sync);
        for (const peer of peers) assert.equal(await exitCode(peer), 0, peer.output());
      }
    });

  test('the owner\'s server here; a client in another process that joins first', SLOW, async () => {
    const name = uniqueName('s_first_p');
    const peer = startPeer('service_client', name, 100);
    await sleep(300);
    await serve(name, 100, 1);
    assert.equal(await exitCode(peer), 0, peer.output());
  });

  test('a server process killed holding a request abandons it; the next call is answered', SLOW, async () => {
    const name = uniqueName('s_killed_p');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const peer = startPeer('service_holding_server', name);
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

  /**
   * Runs the examples `server` and `client`, `first` of them first, and checks what the client prints: in any order
   * with `unordered` (a queue's consumer: the queue promises no order)
   */
  async function examplePair(server, client, expected, first = server, unordered = false) {
    const examples = path.join(binding, 'examples');
    const [later] = [server, client].filter((example) => example !== first);
    const started = start(scriptCommand(path.join(examples, first)), { cwd: examples });
    await sleep(300);
    const other = start(scriptCommand(path.join(examples, later)), { cwd: examples });
    const [serving, calling] = first === server ? [started, other] : [other, started];
    assert.equal(await exitCode(calling), 0, calling.output());
    const lines = calling.output().trim().split(/\r?\n/);
    assert.deepEqual(unordered ? lines.sort() : lines, expected);
    assert.equal(await exitCode(serving), 0, serving.output());
  }

  const pairs = [
    ['server.mjs', 'client.mjs', ['PING']],
    ['publisher.mjs', 'subscriber.mjs', ['tick 1', 'tick 2', 'tick 3']],
    ['producer.mjs', 'consumer.mjs', ['job 1', 'job 2', 'job 3'], true],
    ['service_server.mjs', 'service_client.mjs', ['PING 1', 'PING 2', 'PING 3']],
    ['echo_server.mjs', 'echo_client.mjs', ['hello', 'shared', 'memory']],
  ];
  for (const [server, client, expected, unordered] of pairs) {
    test(`the examples ${server} and ${client}`, SLOW, () => examplePair(server, client, expected, server, unordered));
  }
  for (const [server, client, expected, unordered] of pairs.slice(0, 4)) {
    test(`the examples ${client} and ${server}, the ${client} first`, SLOW,
      () => examplePair(server, client, expected, client, unordered));
  }
});
