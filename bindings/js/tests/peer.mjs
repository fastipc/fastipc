/*
 * The tests' peer in another process, in the runtime that runs the tests: `peer.mjs <role> <name> [arguments]`. Exit
 * code 0 when the role went as expected; an error ends it with another code.
 *
 *   echo <name>               connects; sends back every message it receives until the peer's end
 *   echo_rpc <name>           connects; answers every request with its payload reversed, status = its length, until
 *                             the peer's end
 *   send_and_exit <name> <n>  connects; sends "message 1" ... "message n", then exits without closing
 *   hang <name>               connects, sends "ready", then waits until it is killed
 *   expect_end <name>         connects; receives until the peer's end (FIPC_DISCONNECTED), and nothing else may come
 *   rpc_server <name>         listens with 64 KiB rings; accepts one client and answers every request in upper case,
 *                             status -1, until its end
 *   zerocopy_echo <name>      connects; receives each message in place (receiveAcquire, or receive for one of several
 *                             pieces) and sends it back through sendAcquire (send for a long one)
 *   sync_echo <name>          echo with the Sync calls only
 *   idle_exit <name>          connects, sends "hi", receives one message, and returns without closing: the process
 *                             ends by itself (an idle connection doesn't keep the event loop alive)
 *   pending <name>            connects, sends "ready", then waits for one message with no timeout and prints it: the
 *                             pending call keeps the process alive until then
 */
import { Connection, FipcError, Listener, RPC_REQUEST } from '../index.mjs';

const [role, name, arg] = process.argv.slice(2);
const TIMEOUT = 20000;

function isEnd(error) {
  if (error instanceof FipcError && error.code === 'FIPC_DISCONNECTED') return true;
  throw error;
}

async function untilEnd(step) {
  for (;;) {
    try {
      await step();
    } catch (error) {
      if (isEnd(error)) return;
    }
  }
}

function reversed(bytes) {
  return Buffer.from(bytes).reverse();
}

const roles = {
  async echo() {
    const conn = await Connection.connect(name, TIMEOUT);
    await untilEnd(async () => conn.send(await conn.receive(TIMEOUT), TIMEOUT));
    conn.close();
  },
  async echo_rpc() {
    const conn = await Connection.connect(name, TIMEOUT);
    await untilEnd(async () => {
      const request = await conn.rpcReceive(TIMEOUT);
      if (request.kind !== RPC_REQUEST) throw new Error(`a request's kind is ${request.kind}`);
      await conn.rpcRespond(request.id, request.opcode, request.payload.length, reversed(request.payload), TIMEOUT);
    });
    conn.close();
  },
  async send_and_exit() {
    const conn = await Connection.connect(name, TIMEOUT);
    for (let i = 1; i <= Number(arg); i++) conn.sendSync(`message ${i}`, TIMEOUT);
    process.exit(0); // without close: the end of the process ends the connection
  },
  async hang() {
    const conn = await Connection.connect(name, TIMEOUT);
    await conn.send('ready', TIMEOUT);
    setInterval(() => conn.maxPiece, 1000); // keeps the connection in reach: a dropped one is closed when collected
  },
  async expect_end() {
    const conn = await Connection.connect(name, TIMEOUT);
    try {
      await conn.receive(TIMEOUT);
    } catch (error) {
      if (!(error instanceof FipcError) || error.code !== 'FIPC_DISCONNECTED') throw error;
      if (!conn.ended) throw new Error('ended is false after FIPC_DISCONNECTED');
      conn.close();
      return;
    }
    throw new Error('expected the peer\'s end, got a message');
  },
  async rpc_server() {
    const listener = Listener.listen(name, 64 * 1024);
    const conn = await listener.accept(TIMEOUT);
    await untilEnd(async () => {
      const request = await conn.rpcReceive(TIMEOUT);
      await conn.rpcRespond(request.id, request.opcode, -1, request.payload.toString('latin1').toUpperCase(), TIMEOUT);
    });
    conn.close();
    listener.close();
  },
  async zerocopy_echo() {
    const conn = await Connection.connect(name, TIMEOUT);
    const max = conn.maxPiece;
    await untilEnd(async () => {
      let message;
      try {
        message = Buffer.from(await conn.receiveAcquire(TIMEOUT)); // a copy: the view ends at the release
        conn.receiveRelease();
      } catch (error) {
        if (!(error instanceof FipcError) || error.code !== 'FIPC_TOO_LARGE') throw error;
        message = await conn.receive(TIMEOUT);
        if (message.length !== error.length) throw new Error(`length ${message.length}, announced ${error.length}`);
      }
      if (message.length <= max) {
        const slot = await conn.sendAcquire(message.length, TIMEOUT);
        slot.set(message);
        conn.sendCommit(message.length);
      } else {
        await conn.send(message, TIMEOUT);
      }
    });
    conn.close();
  },
  async sync_echo() {
    const conn = Connection.connectSync(name, TIMEOUT);
    for (;;) {
      let message;
      try {
        message = conn.receiveSync(TIMEOUT);
      } catch (error) {
        if (isEnd(error)) break;
      }
      conn.sendSync(message, TIMEOUT);
    }
    conn.close();
  },
  async idle_exit() {
    const conn = await Connection.connect(name, TIMEOUT);
    await conn.send('hi', TIMEOUT);
    await conn.receive(TIMEOUT);
  },
  async pending() {
    const conn = await Connection.connect(name, TIMEOUT);
    await conn.send('ready', TIMEOUT);
    console.log((await conn.receive()).toString());
  },
};

if (!roles[role]) {
  console.error(`unknown role ${role}`);
  process.exit(2);
}
await roles[role]();
