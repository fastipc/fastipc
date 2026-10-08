// A process for tests/lifetime.test.mjs that never loads the binding on its own thread: it runs tests/worker.mjs as a
// worker (argv: name, role), terminates it once it is ready, and lives on for a second, then prints 'lived on'.
import path from 'node:path';
import { Worker } from 'node:worker_threads';

const [name, role] = process.argv.slice(2);
const worker = new Worker(path.join(import.meta.dirname, 'worker.mjs'), { workerData: { name, role } });
await new Promise((resolve, reject) => {
  worker.once('message', resolve);
  worker.once('error', reject);
});
await worker.terminate();
await new Promise((resolve) => setTimeout(resolve, 1000));
console.log('lived on');
