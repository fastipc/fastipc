// A worker thread for tests/lifetime.test.mjs: workerData { name, role }. 'rpc' connects, makes one call and closes;
// 'hang' connects, posts 'ready' and waits on a receive with no timeout until the worker is terminated; 'connect'
// starts a connect of half a second to a name nobody listens on, posts 'ready' and waits on it.
import { parentPort, workerData } from 'node:worker_threads';

import { Connection } from '../index.mjs';

if (workerData.role === 'connect') {
  const connecting = Connection.connect(workerData.name, 500);
  parentPort.postMessage('ready');
  await connecting.catch(() => {});
  process.exit(0);
}
const conn = await Connection.connect(workerData.name, 20000);
if (workerData.role === 'rpc') {
  const id = await conn.rpcSubmit(1, 'from a worker', 20000);
  const response = await conn.rpcReceive(20000);
  parentPort.postMessage({ id, response: response.payload.toString() });
  conn.close();
} else {
  parentPort.postMessage('ready');
  await conn.receive();
}
