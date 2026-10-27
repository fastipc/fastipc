// The documentation's JavaScript blocks that aren't examples (examples/*.mjs), as code that runs: each block of the
// READMEs and of the website appears here (or in an example) line for line, which tests/docs.test.mjs checks.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { describe, test } from 'node:test';

import {
  CLIENT,
  NO_WAIT,
  SERVER,
  SUBSCRIBER_LOSSY,
  TEN_SECONDS,
  closeAll,
  createBroadcast,
  createService,
  joinBroadcast,
  joinService,
  pair,
  uniqueName,
} from './support.mjs';

describe('snippets', () => {
  test('the event loop runs while a receive waits', async () => {
    const { listener, server, client: conn } = await pair();
    const logged = [];
    const console = { log: (line) => logged.push(line) };
    setTimeout(() => server.sendSync('wake'), 250);
    const timer = setInterval(() => console.log("the event loop runs"), 100);
    const message = await conn.receive(); // waits without blocking the timer
    clearInterval(timer);
    assert.equal(message.toString(), 'wake');
    assert.ok(logged.length >= 1);
    closeAll(server, conn, listener);
  });

  test('errors and timeouts', async () => {
    const { listener, server, client: conn } = await pair();
    const handled = [];
    const handle = (message) => handled.push(message.toString());
    try {
      handle(await conn.receive(100)); // waits at most 100 ms
    } catch (error) {
      if (error.code !== "FIPC_TIMEOUT") throw error; // "FIPC_DISCONNECTED": the peer is gone
    }
    assert.deepEqual(handled, []);
    closeAll(server, conn, listener);
  });

  test('a buffer of your own: FIPC_TOO_LARGE says how much room', async () => {
    const { listener, server, client: conn } = await pair();
    await server.send(Buffer.alloc(5000, 1));
    let buffer = Buffer.alloc(4096);
    let length;
    try {
      length = await conn.receiveInto(buffer);
    } catch (error) {
      if (error.code !== "FIPC_TOO_LARGE") throw error;
      buffer = Buffer.alloc(error.length); // the message stays queued: make room, then take it
      length = await conn.receiveInto(buffer);
    }
    assert.equal(length, 5000);
    assert.equal(buffer.length, 5000);
    closeAll(server, conn, listener);
  });

  test('zero-copy: send and receive in place', async () => {
    const { listener, server, client: conn } = await pair();
    const slot = await conn.sendAcquire(5); // room in the ring: a Uint8Array
    slot.set(Buffer.from("hello"));
    conn.sendCommit(5); // sent; the view is detached
    assert.equal(slot.length, 0);
    const seen = [];
    const handle = (bytes) => seen.push(Buffer.from(bytes).toString());
    await (async (conn) => {
      const view = await conn.receiveAcquire(); // the message in the ring
      handle(view); // read it in place
      conn.receiveRelease(); // frees its room; view.length is 0 from now on
      assert.equal(view.length, 0);
    })(server);
    assert.deepEqual(seen, ['hello']);
    closeAll(server, conn, listener);
  });

  test('a batch each way', async () => {
    const { listener, server: peer, client: conn } = await pair();
    const handled = [];
    const handle = (message) => handled.push(message.toString());
    const sent = await conn.sendMany(["one", "two", "three"]); // published together
    for (const message of await peer.receiveMany(64)) { // every message already there, up to 64
      handle(message);
    }
    assert.equal(sent, 3);
    while (handled.length < 3) for (const message of await peer.receiveMany(64)) handle(message);
    assert.deepEqual(handled, ['one', 'two', 'three']);
    closeAll(peer, conn, listener);
  });

  test('polling with the Sync calls, once a frame', async () => {
    const { listener, server, client: conn } = await pair();
    await server.send('one');
    await server.send('two');
    const handled = [];
    const handle = (message) => handled.push(message.toString());
    // once a frame: take what has arrived, without waiting
    for (;;) {
      let message;
      try {
        message = conn.receiveSync(NO_WAIT);
      } catch (error) {
        if (error.code === "FIPC_TIMEOUT") break; // nothing more this frame
        throw error; // "FIPC_DISCONNECTED": the peer is gone
      }
      handle(message);
    }
    assert.deepEqual(handled, ['one', 'two']);
    closeAll(server, conn, listener);
  });

  test('a lossy subscriber that fell a lap behind', async () => {
    const name = uniqueName('snippet_overrun');
    const publisher = createBroadcast(name, 4096, 1);
    const subscriber = await joinBroadcast(name, SUBSCRIBER_LOSSY, TEN_SECONDS); // joined once it returns
    for (let i = 0; i < 144; i++) publisher.sendSync(Buffer.alloc(48, i), NO_WAIT); // two laps of 64 messages
    const handled = [];
    const handle = (message) => handled.push(message[0]);
    const logged = [];
    const console = { log: (line) => logged.push(line) };
    try {
      handle(await subscriber.receive());
    } catch (error) {
      if (error.code !== "FIPC_OVERRUN") throw error; // "FIPC_DISCONNECTED": the broadcast ended
      console.log(`${error.lost} messages lost`); // fell a lap behind: the next receive goes on from a recent message
    }
    assert.deepEqual(logged, ['140 messages lost']);
    handle(await subscriber.receive());
    assert.deepEqual(handled, [140]);
    closeAll(subscriber, publisher);
  });

  test('a call whose server ended before it replied, sent again', async () => {
    const name = uniqueName('snippet_abandoned');
    const service = createService(name, 4096, 64, 1, 2);
    const client = await joinService(name, CLIENT, TEN_SECONDS);
    const quitter = await joinService(name, SERVER, TEN_SECONDS);
    const quitting = (async () => {
      await quitter.receive(TEN_SECONDS); // takes the first try, and leaves holding it
      quitter.close();
      const server = await joinService(name, SERVER, TEN_SECONDS);
      await server.send((await server.receive(TEN_SECONDS)).toString().toUpperCase());
      return server;
    })();
    const request = 'idempotent';
    let reply;
    try {
      reply = await client.call(request);
    } catch (error) {
      if (error.code !== "FIPC_ABANDONED") throw error; // "FIPC_DISCONNECTED": the service ended
      reply = await client.call(request); // its server ended before it replied: send it again, if it may run twice
    }
    assert.equal(reply.toString(), 'IDEMPOTENT');
    closeAll(await quitting, client, service);
  });

  test('CommonJS', () => {
    const require = createRequire(import.meta.url);
    const { createStream, connectStream } = require("fipc");
    assert.equal(typeof createStream, 'function');
    assert.equal(typeof connectStream, 'function');
  });
});
