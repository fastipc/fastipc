// The documentation's JavaScript blocks that aren't examples (examples/*.mjs), as code that runs: each block of the
// READMEs and of the website appears here (or in an example) line for line, which tests/docs.test.mjs checks.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { describe, test } from 'node:test';

import { NO_WAIT, closeAll, pair } from './support.mjs';

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

  test('CommonJS', () => {
    const require = createRequire(import.meta.url);
    const { Listener, Connection } = require("fipc");
    assert.equal(typeof Listener.listen, 'function');
    assert.equal(typeof Connection.connect, 'function');
  });
});
