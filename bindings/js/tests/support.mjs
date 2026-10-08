// The tests' helpers: names, payloads, other processes (this runtime running tests/peer.mjs, or Python), timing.
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';

import fipc from '../index.mjs';

export { fipc };
export const { Listener, Connection, FipcError, NO_WAIT, FOREVER, RPC_REQUEST, RPC_RESPONSE } = fipc;

export const here = import.meta.dirname;
export const binding = path.join(here, '..');
export const repository = path.join(here, '..', '..', '..');
export const windows = process.platform === 'win32';
export const TEN_SECONDS = 10000;

/** The runtime running the tests: 'node', 'bun' or 'deno'. */
export const runtime = typeof Deno !== 'undefined' ? 'deno' : process.versions.bun ? 'bun' : 'node';

let counter = 0;
/** A name no other test (or run) uses. */
export function uniqueName(tag) {
  counter += 1;
  return `fipc_js_${tag}_${process.pid}_${Date.now() % 100000}_${counter}`;
}

/** `size` bytes of a pattern that shows misplaced or missing bytes. */
export function pattern(size, seed = 0) {
  const bytes = Buffer.alloc(size);
  for (let i = 0; i < size; i++) bytes[i] = (i * 31 + seed + (i >> 8)) & 0xff;
  return bytes;
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A server's listener and a client's connection, in this process: an async accept and an async connect. */
export async function pair(capacity = 64 * 1024, tag = 'pair') {
  const name = uniqueName(tag);
  const listener = Listener.listen(name, capacity);
  const [server, client] = await Promise.all([listener.accept(TEN_SECONDS), Connection.connect(name, TEN_SECONDS)]);
  return { name, listener, server, client };
}

export function closeAll(...handles) {
  for (const handle of handles) handle?.close();
}

/** Asserts that `promise` rejects with a FipcError of `code`; the error. */
export async function rejectsWith(assert, promise, code) {
  try {
    await promise;
  } catch (error) {
    assert.ok(error instanceof FipcError, `expected a FipcError ${code}, got ${error}`);
    assert.equal(error.code, code);
    return error;
  }
  assert.fail(`expected ${code}, the call succeeded`);
}

/** Asserts that `fn` throws a FipcError of `code`; the error. */
export function throwsWith(assert, fn, code) {
  try {
    fn();
  } catch (error) {
    assert.ok(error instanceof FipcError, `expected a FipcError ${code}, got ${error}`);
    assert.equal(error.code, code);
    return error;
  }
  assert.fail(`expected ${code}, the call succeeded`);
}

/** The command that runs a script of this runtime. */
export function scriptCommand(script, args = []) {
  const prefix = runtime === 'deno' ? [process.execPath, 'run', '-A'] : [process.execPath];
  return [...prefix, script, ...args];
}

/** Another process: { process, exit (a promise of its exit code), output () => its stdout and stderr } */
export function start(argv, options = {}) {
  const child = spawn(argv[0], argv.slice(1), { stdio: ['ignore', 'pipe', 'pipe'], ...options });
  let output = '';
  child.stdout.on('data', (chunk) => (output += chunk));
  child.stderr.on('data', (chunk) => (output += chunk));
  const exit = new Promise((resolve) => child.on('close', (code, signal) => resolve(code ?? signal)));
  return { process: child, exit, output: () => output };
}

/** tests/peer.mjs in another process, in this runtime: `role` with `name` (peer.mjs lists the roles). */
export function startPeer(role, name, ...args) {
  return start(scriptCommand(path.join(here, 'peer.mjs'), [role, name, ...args.map(String)]));
}

/** Waits for a process's exit code, failing the test after `ms`. */
export async function exitCode(peer, ms = 30000) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`the peer didn't exit within ${ms} ms: ${peer.output()}`)), ms);
  });
  try {
    return await Promise.race([peer.exit, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

let python;
/**
 * A Python interpreter with cffi: FIPC_TEST_PYTHON, the repository's venv, or python; null if none (the interop tests
 * are then skipped, unless FIPC_REQUIRE_INTEROP is set).
 */
export async function findPython() {
  if (python !== undefined) return python;
  const venv = path.join(repository, 'venv', windows ? 'Scripts/python.exe' : 'bin/python');
  const candidates = [process.env.FIPC_TEST_PYTHON, existsSync(venv) ? venv : undefined, windows ? 'python' : 'python3'];
  python = null;
  for (const candidate of candidates.filter(Boolean)) {
    const probe = start([candidate, '-c', 'import cffi']);
    probe.process.on('error', () => {});
    const code = await Promise.race([probe.exit, sleep(15000).then(() => 'timeout')]);
    if (code === 0) {
      python = candidate;
      break;
    }
  }
  if (!python && process.env.FIPC_REQUIRE_INTEROP) throw new Error('no Python with cffi (set FIPC_TEST_PYTHON)');
  return python;
}
