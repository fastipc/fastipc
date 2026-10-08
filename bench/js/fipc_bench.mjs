/*
 * FastIPC's JavaScript benchmark, through the binding (bindings/js, fipc) on Node.js, Bun or Deno: one-way
 * throughput between this process, a server that receives and times, and a client process (this script again, in the
 * same runtime) that sends. The same cases, output and checks as the C, C++, Python, C#, Java, Rust, Lua and Go
 * benchmarks.
 *
 *   node bench/js/fipc_bench.mjs copy|zerocopy|rpc [--sync]
 *   bun bench/js/fipc_bench.mjs ...;  deno run -A bench/js/fipc_bench.mjs ...
 *
 *   copy      conn.send(buffer) / conn.receiveInto a buffer the server reuses
 *   zerocopy  sendAcquire + set + sendCommit / receiveAcquire, copied out, + receiveRelease (a message of several
 *             pieces through the copying calls)
 *   rpc       rpcSubmit / rpcReceiveInto a buffer the server reuses
 *
 * Both sides use the async calls (each awaited: a message there is taken at once, without a thread); --sync uses the
 * Sync ones. The JIT compiles the code only after it has run a while, and a fresh process starts slowly, so each case
 * first sends messages untimed for half a second (the warm-up; at least its count), then a start marker, then its
 * count, and the server times from the last start marker to the end marker (see client). Each case prints "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare reads them).
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { Connection, FipcError, Listener } from '../../bindings/js/index.mjs';

const CONNECT_MS = 15000;
const DATA_OPCODE = 1;
const END_OPCODE = 0xfb000001;
const GO_OPCODE = 0xfb000002;
const END = Buffer.from('END');
const GO = Buffer.from('GO!');
const WARM_UP_NS = 500_000_000n;
const MARKER_EVERY = 1000;

const CASES = [
  { count: 2000000, ring: 512 * 1024, size: 16, name: 'Tiny messages (16B)' },
  { count: 2000000, ring: 512 * 1024, size: 64, name: 'Small messages (64B)' },
  { count: 1000000, ring: 512 * 1024, size: 256, name: 'Medium messages (256B)' },
  { count: 1000, ring: 512 * 1024, size: 64 * 1024, name: 'Large messages (64KB)' },
  { count: 1000, ring: 2 * 1024 * 1024, size: 512 * 1024, name: 'Large messages (512KB)' },
  { count: 10, ring: 512 * 1024, size: 1024 * 1024, name: 'Exceeds buffer (1MB msg, 512KB buffer)' },
];

const script = fileURLToPath(import.meta.url);
const runtime = typeof Deno !== 'undefined' ? [process.execPath, 'run', '-A'] : [process.execPath];

const AT_END = -1;
const AT_GO = -2;

/** AT_END or AT_GO for a marker, else 0 */
function marker(bytes, length) {
  if (length !== 3) return 0;
  if (bytes[0] === 0x45 && bytes[1] === 0x4e && bytes[2] === 0x44) return AT_END;
  return bytes[0] === 0x47 && bytes[1] === 0x4f && bytes[2] === 0x21 ? AT_GO : 0;
}

/** The server: skips the warm-up up to the last start marker, then receives until the end marker; { messages, bytes, seconds } */
async function serve(mode, conn, size, sync) {
  const buffer = Buffer.alloc(Math.max(size, 3));
  const sink = Buffer.alloc(Math.max(size, 3));
  let timing = false;
  let messages = 0;
  let bytes = 0;
  let started = 0n;
  for (;;) {
    let length;
    let mark = 0;
    if (mode === 'rpc') {
      const header = sync ? conn.rpcReceiveIntoSync(buffer) : await conn.rpcReceiveInto(buffer);
      mark = header.opcode === END_OPCODE ? AT_END : header.opcode === GO_OPCODE ? AT_GO : 0;
      length = header.length;
    } else if (mode === 'zerocopy') {
      let view;
      try {
        view = sync ? conn.receiveAcquireSync() : await conn.receiveAcquire();
      } catch (error) {
        if (!(error instanceof FipcError) || error.code !== 'FIPC_TOO_LARGE') throw error;
        length = sync ? conn.receiveIntoSync(buffer) : await conn.receiveInto(buffer); // several pieces
      }
      if (view) {
        length = view.length;
        mark = marker(view, length);
        if (mark === 0) sink.set(view); // a consumer copies the message out
        conn.receiveRelease();
      }
    } else {
      length = sync ? conn.receiveIntoSync(buffer) : await conn.receiveInto(buffer);
      mark = marker(buffer, length);
    }
    if (mark === AT_END) break;
    if (mark === AT_GO) { // the last one starts the timed messages
      timing = true;
      messages = 0;
      bytes = 0;
      started = process.hrtime.bigint();
    } else if (timing) {
      messages += 1;
      bytes += length;
    }
  }
  return { messages, bytes, seconds: Number(process.hrtime.bigint() - started) / 1e9 };
}

/** A start or end marker: an empty RPC of its opcode, else its 3 bytes through the copying send */
async function sendMarker(mode, conn, marker, opcode, sync) {
  if (mode === 'rpc') {
    if (sync) conn.rpcSubmitSync(opcode);
    else await conn.rpcSubmit(opcode);
  } else if (sync) conn.sendSync(marker);
  else await conn.send(marker);
}

/** One message */
async function sendOne(mode, conn, payload, onePiece, sync) {
  if (mode === 'rpc') {
    if (sync) conn.rpcSubmitSync(DATA_OPCODE, payload);
    else await conn.rpcSubmit(DATA_OPCODE, payload);
  } else if (mode === 'zerocopy' && onePiece) {
    const slot = sync ? conn.sendAcquireSync(payload.length) : await conn.sendAcquire(payload.length);
    slot.set(payload);
    conn.sendCommit(payload.length);
  } else if (sync) conn.sendSync(payload);
  else await conn.send(payload);
}

/**
 * The client process: connects; sends messages untimed for the warm-up (the count, and for at least WARM_UP_NS),
 * with a start marker every MARKER_EVERY messages, so that the real one takes no path the JIT hasn't seen; then the
 * start marker, `count` messages of `size` bytes and the end marker; and closes
 */
async function client(mode, name, size, count, sync) {
  const conn = await Connection.connect(name, CONNECT_MS);
  const payload = Buffer.alloc(size, 0x78);
  const onePiece = size <= conn.maxPiece;
  const warm = process.hrtime.bigint() + WARM_UP_NS;
  for (let i = 0; i < count || process.hrtime.bigint() < warm; i++) {
    if (i % MARKER_EVERY === 0) await sendMarker(mode, conn, GO, GO_OPCODE, sync);
    await sendOne(mode, conn, payload, onePiece, sync);
  }
  await sendMarker(mode, conn, GO, GO_OPCODE, sync);
  for (let i = 0; i < count; i++) await sendOne(mode, conn, payload, onePiece, sync);
  await sendMarker(mode, conn, END, END_OPCODE, sync);
  conn.close();
}

async function runCase(mode, testCase, index, sync) {
  const name = `jsbench_${process.pid}_${index}`;
  const listener = Listener.listen(name, testCase.ring);
  const args = [script, 'client', mode, name, String(testCase.size), String(testCase.count), sync ? '--sync' : ''];
  const child = spawn(runtime[0], [...runtime.slice(1), ...args], { stdio: ['ignore', 'ignore', 'inherit'] });
  const exited = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  let result;
  try {
    const conn = await listener.accept(CONNECT_MS);
    result = await serve(mode, conn, testCase.size, sync);
    conn.close();
  } catch (error) {
    console.error(`server: ${error.message}`);
    child.kill();
    listener.close();
    return false;
  }
  listener.close();
  const code = await exited;
  if (code !== 0) {
    console.error(`the client failed: exit code ${code}`);
    return false;
  }
  const { messages, bytes, seconds } = result;
  if (messages !== testCase.count || bytes !== testCase.count * testCase.size) {
    console.error(`expected ${testCase.count} messages of ${testCase.size} B, got ${messages} (${bytes} B)`);
    return false;
  }
  console.log(`Messages: ${testCase.count} | Ring: ${testCase.ring / 1024}KB | Size: ${testCase.size}B`);
  console.log(`Duration: ${seconds.toFixed(3)}s`);
  console.log(`Throughput: ${Math.round(messages / seconds)} messages/sec, ${(bytes / seconds / (1024 * 1024)).toFixed(1)} MB/sec`);
  return true;
}

const args = process.argv.slice(2);
const sync = args.includes('--sync');
const [mode = 'copy', ...rest] = args.filter((arg) => arg !== '--sync');
if (mode === 'client') {
  const [clientMode, name, size, count] = rest;
  await client(clientMode, name, Number(size), Number(count), sync);
  process.exit(0);
}
if (!['copy', 'zerocopy', 'rpc'].includes(mode)) {
  console.error('usage: fipc_bench.mjs copy|zerocopy|rpc [--sync]');
  process.exit(2);
}
const engine = typeof Deno !== 'undefined' ? `Deno ${Deno.version.deno}`
  : process.versions.bun ? `Bun ${process.versions.bun}` : `Node.js ${process.versions.node}`;
console.log(`FastIPC JavaScript benchmark: ${mode}${sync ? ' (Sync calls)' : ''}, ${engine}\n`);
let passed = 0;
for (let i = 0; i < CASES.length; i++) {
  console.log(`Test ${i + 1}/${CASES.length}: ${CASES[i].name}`);
  if (await runCase(mode, CASES[i], i + 1, sync)) passed += 1;
  else console.log('FAILED');
  console.log();
}
console.log(`Summary: ${passed}/${CASES.length} tests passed`);
process.exit(passed === CASES.length ? 0 : 1);
