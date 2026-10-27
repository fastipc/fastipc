// A worker thread for tests/lifetime.test.mjs: workerData { name, role }. 'call' connects, sends one message, posts
// the answer and closes; 'hang' connects, posts 'ready' and waits on a receive with no timeout until the worker is
// terminated; 'connect' starts a connect of half a second to a name nobody listens on, posts 'ready' and waits on it;
// 'queue' creates a queue, sends 'from a worker' as its producer, posts 'ready' and waits for three members with no
// timeout until the worker is terminated; 'service' creates a service, joins it as a server, posts 'ready' and
// answers requests with their uppercase until the worker is terminated; 'holding_server' joins the service as a
// server, answers one request, takes the next, posts 'holding' and holds it, never replying, until the worker is
// terminated.
import { parentPort, workerData } from 'node:worker_threads';

import { PRODUCER, SERVER, connectStream, createQueue, createService, joinQueue, joinService } from '../index.mjs';

if (workerData.role === 'service') {
  createService(workerData.name, 4096, 64, 1, 1);
  const server = await joinService(workerData.name, SERVER, 20000);
  parentPort.postMessage('ready');
  for (;;) await server.send((await server.receive()).toString().toUpperCase());
}
if (workerData.role === 'holding_server') {
  const server = await joinService(workerData.name, SERVER, 20000);
  await server.send((await server.receive(20000)).toString().toUpperCase());
  await server.receive(20000);
  parentPort.postMessage('holding');
  setInterval(() => server.maxPiece, 1000); // alive, and the connection in reach, until terminated
  await new Promise(() => {});
}

if (workerData.role === 'queue') {
  const queue = createQueue(workerData.name, 4096, 1, 2);
  const producer = await joinQueue(workerData.name, PRODUCER, 20000);
  await producer.send('from a worker', 20000);
  parentPort.postMessage('ready');
  await queue.waitForMembers(undefined, 3);
}
if (workerData.role === 'connect') {
  const connecting = connectStream(workerData.name, 500);
  parentPort.postMessage('ready');
  await connecting.catch(() => {});
  process.exit(0);
}
const conn = await connectStream(workerData.name, 20000);
if (workerData.role === 'call') {
  await conn.send('from a worker', 20000);
  parentPort.postMessage((await conn.receive(20000)).toString());
  conn.close();
} else {
  parentPort.postMessage('ready');
  await conn.receive();
}
