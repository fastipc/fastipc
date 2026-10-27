/*
 * The tests' peer in another process, in the runtime that runs the tests: `peer.mjs <role> <name> [arguments]`. Exit
 * code 0 when the role went as expected; an error ends it with another code.
 *
 *   echo <name>               connects; sends back every message it receives until the peer's end
 *   send_and_exit <name> <n>  connects; sends "message 1" ... "message n", then exits without closing
 *   hang <name>               connects, sends "ready", then waits until it is killed
 *   expect_end <name>         connects; receives until the peer's end (FIPC_DISCONNECTED), and nothing else may come
 *   upper_server <name>       listens with 64 KiB rings; accepts one client and answers every message in upper case,
 *                             until its end
 *   zerocopy_echo <name>      connects; receives each message in place (receiveAcquire, or receive for one of several
 *                             pieces) and sends it back through sendAcquire (send for a long one)
 *   sync_echo <name>          echo with the Sync calls only
 *   idle_exit <name>          connects, sends "hi", receives one message, and returns without closing: the process
 *                             ends by itself (an idle connection doesn't keep the event loop alive)
 *   pending <name>            connects, sends "ready", then waits for one message with no timeout and prints it: the
 *                             pending call keeps the process alive until then
 *   publisher <name> <count> <subscribers>
 *                             a broadcast's publisher: waits for the subscribers, sends count messages, closes
 *                             (tests/broadcast.mjs)
 *   subscriber <name> <gated|lossy> <count>
 *                             joins the broadcast and checks what it receives until its end (tests/broadcast.mjs)
 *   queue_producer <name> <count> <consumers> [long]
 *                             a work queue's owner and producer: sends count jobs (long ones: SIZES.long), waits for the
 *                             consumers, closes, which ends the queue (tests/queue.mjs)
 *   queue_consumer <name> [long]
 *                             joins the queue as a consumer, receives and checks jobs until its end, and prints their
 *                             numbers, a JSON array
 *   queue_owner_consumer <name> <count> <producers>
 *                             a work queue's owner and consumer: receives and checks count jobs of each producer,
 *                             closes
 *   queue_joined_producer <name> <count>
 *                             joins the queue as a producer, sends count jobs, closes
 *   service_server <name> <count> <clients>
 *                             a service's owner and server: answers count requests of each client, closes, which ends
 *                             the service (tests/service.mjs)
 *   service_client <name> <count>
 *                             joins the service as a client, calls count times and checks each reply, closes
 *   service_owner_client <name> <count> <servers>
 *                             a service's owner and client: waits for the servers, calls count times, closes
 *   service_joined_server <name>
 *                             joins the service as a server and answers requests until its end
 *   service_holding_server <name>
 *                             joins the service as a server, takes one request, prints `holding` and waits, never
 *                             replying, until it is killed
 */
import { FipcError, connectStream, connectStreamSync, createStream } from '../index.mjs';
import { publish, subscribe } from './broadcast.mjs';
import { SIZES, consume, joinAndProduce, ownAndConsume, produce } from './queue.mjs';
import { joinAndAnswer, joinAndCall, joinAndHold, ownAndCall, serve } from './service.mjs';

const [role, name, arg, arg2, arg3] = process.argv.slice(2);
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

const roles = {
  async echo() {
    const conn = await connectStream(name, TIMEOUT);
    await untilEnd(async () => conn.send(await conn.receive(TIMEOUT), TIMEOUT));
    conn.close();
  },
  async send_and_exit() {
    const conn = await connectStream(name, TIMEOUT);
    for (let i = 1; i <= Number(arg); i++) conn.sendSync(`message ${i}`, TIMEOUT);
    process.exit(0); // without close: the end of the process ends the connection
  },
  async hang() {
    const conn = await connectStream(name, TIMEOUT);
    await conn.send('ready', TIMEOUT);
    setInterval(() => conn.maxPiece, 1000); // keeps the connection in reach: a dropped one is closed when collected
  },
  async expect_end() {
    const conn = await connectStream(name, TIMEOUT);
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
  async upper_server() {
    const listener = createStream(name, 64 * 1024);
    const conn = await listener.accept(TIMEOUT);
    await untilEnd(async () => {
      const message = await conn.receive(TIMEOUT);
      await conn.send(message.toString('latin1').toUpperCase(), TIMEOUT);
    });
    conn.close();
    listener.close();
  },
  async zerocopy_echo() {
    const conn = await connectStream(name, TIMEOUT);
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
    const conn = connectStreamSync(name, TIMEOUT);
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
    const conn = await connectStream(name, TIMEOUT);
    await conn.send('hi', TIMEOUT);
    await conn.receive(TIMEOUT);
  },
  async pending() {
    const conn = await connectStream(name, TIMEOUT);
    await conn.send('ready', TIMEOUT);
    console.log((await conn.receive()).toString());
  },
  async publisher() {
    await publish(name, Number(arg), Number(arg2));
  },
  async subscriber() {
    if (arg !== 'gated' && arg !== 'lossy') throw new Error(`a subscriber is gated or lossy, not ${arg}`);
    await subscribe(name, arg === 'gated', Number(arg2));
  },
  async queue_producer() {
    await produce(name, Number(arg), Number(arg2), arg3 === 'long' ? { capacity: 64 * 1024, size: SIZES.long } : {});
  },
  async queue_consumer() {
    console.log(JSON.stringify(await consume(name, arg === 'long' ? SIZES.long : undefined)));
  },
  async queue_owner_consumer() {
    await ownAndConsume(name, Number(arg), Number(arg2));
  },
  async queue_joined_producer() {
    await joinAndProduce(name, Number(arg));
  },
  async service_server() {
    await serve(name, Number(arg), Number(arg2));
  },
  async service_client() {
    await joinAndCall(name, Number(arg));
  },
  async service_owner_client() {
    await ownAndCall(name, Number(arg), Number(arg2));
  },
  async service_joined_server() {
    await joinAndAnswer(name);
  },
  async service_holding_server() {
    await joinAndHold(name);
  },
};

if (!roles[role]) {
  console.error(`unknown role ${role}`);
  process.exit(2);
}
await roles[role]();
