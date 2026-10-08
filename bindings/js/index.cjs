'use strict';
/*
 * FastIPC for JavaScript: messages and RPC between two processes on one machine, through shared memory, in Node.js,
 * Bun and Deno. One Node-API addon (fipc.node) over the C API of include/fipc.h, the same file for all three runtimes.
 *
 *     const { Listener, Connection } = require('fipc');   // or: import { Listener, Connection } from 'fipc'
 *
 *     const listener = Listener.listen('my_channel', 1 << 20);           // the server: rings of 1 MiB each way
 *     const conn = await listener.accept();                              // waits for a client
 *     const conn = await Connection.connect('my_channel', 5000);         // the client, in another process
 *
 * Every call that may wait has two forms. `receive()` returns a Promise and doesn't block the event loop: it takes a
 * message at once if one is there, else a thread of the connection's own waits for it. `receiveSync()` waits on the
 * calling thread, which blocks the event loop meanwhile. Timeouts are milliseconds, the last argument: 0 (NO_WAIT)
 * doesn't wait, undefined or a negative number (FOREVER) waits as long as it takes.
 *
 * A call the library refuses rejects (or throws) a FipcError whose `code` names the result: 'FIPC_TIMEOUT',
 * 'FIPC_DISCONNECTED', ... A misuse throws (or rejects) a TypeError or RangeError (ERR_INVALID_ARG_TYPE,
 * ERR_OUT_OF_RANGE) or an Error with the code ERR_FIPC_CLOSED (a closed handle) or ERR_FIPC_BUSY (a Sync call, a
 * commit or a release while an async call of the same direction is pending, or anything beside a pending async
 * acquire).
 */

const fs = require('node:fs');
const path = require('node:path');

const VERSION = '1.0.0';

// platform-arch -> the library's file name; the package carries both in prebuilds/<platform-arch>/
const LIBRARIES = {
  'linux-x64': 'libfastipc.so',
  'linux-arm64': 'libfastipc.so',
  'win32-x64': 'fastipc.dll',
  'darwin-arm64': 'libfastipc.dylib',
};
const PLATFORM = `${process.platform}-${process.arch}`;
const LIBRARY = LIBRARIES[PLATFORM];
if (!LIBRARY) {
  throw new Error(`fipc runs on Linux (x64, arm64), Windows (x64) and macOS (arm64), not ${PLATFORM}`);
}
const ADDON = 'fipc.node';

function isFile(file) {
  try {
    return fs.statSync(file).isFile();
  } catch {
    return false;
  }
}

function environment(name) {
  try {
    return process.env[name] || undefined;
  } catch {
    return undefined; // Deno without --allow-env
  }
}

/*
 * Where the addon and the library are, in this order: the folder FASTIPC_LIB_DIR names (the library; and the addon
 * if it is there too); in a checkout of the repository (this file is bindings/js/index.cjs), its zig-out build; the
 * package's prebuilds/<platform-arch>/; for the library last, the system's search.
 */
function locate() {
  const found = { addon: undefined, library: undefined };
  const dir = environment('FASTIPC_LIB_DIR');
  if (dir) {
    found.library = path.join(dir, LIBRARY);
    if (!isFile(found.library)) {
      throw new Error(`fipc: FASTIPC_LIB_DIR names ${dir}, which holds no ${LIBRARY}`);
    }
    if (isFile(path.join(dir, ADDON))) found.addon = path.join(dir, ADDON);
  }
  const repository = path.join(__dirname, '..', '..');
  if (isFile(path.join(repository, 'build.zig')) && isFile(path.join(repository, 'include', 'fipc.h'))) {
    const out = path.join(repository, 'zig-out');
    if (!found.addon && isFile(path.join(out, 'lib', ADDON))) found.addon = path.join(out, 'lib', ADDON);
    for (const folder of ['lib', 'bin']) {
      if (!found.library && isFile(path.join(out, folder, LIBRARY))) found.library = path.join(out, folder, LIBRARY);
    }
  }
  const prebuilds = path.join(__dirname, 'prebuilds', PLATFORM);
  if (!found.addon && isFile(path.join(prebuilds, ADDON))) found.addon = path.join(prebuilds, ADDON);
  if (!found.library && isFile(path.join(prebuilds, LIBRARY))) found.library = path.join(prebuilds, LIBRARY);
  if (!found.addon) {
    throw new Error(`fipc: no ${ADDON} for ${PLATFORM} in ${prebuilds}` +
      " (in a checkout of the repository, build it: 'python devtool.py build')");
  }
  return { addon: found.addon, library: found.library || LIBRARY };
}

/** fipc_result_t */
const Result = Object.freeze({
  OK: 0,
  TIMEOUT: 1,
  DISCONNECTED: 2,
  CANCELLED: 3,
  TOO_LARGE: 4,
  INVALID: 5,
  NO_MEMORY: 6,
  ADDR_IN_USE: 7,
});
const NAMES = Object.keys(Result);

/** The library's name for a result: 'FIPC_OK', 'FIPC_TIMEOUT', ...; 'FIPC_UNKNOWN' for any other value. */
function resultStr(result) {
  return Number.isInteger(result) && NAMES[result] !== undefined ? `FIPC_${NAMES[result]}` : 'FIPC_UNKNOWN';
}

/** A call returned `result`, not OK. `code` is its name ('FIPC_TIMEOUT', ...). */
class FipcError extends Error {
  constructor(result, call, length) {
    super(`${call}: ${resultStr(result)}`);
    this.name = 'FipcError';
    this.code = resultStr(result);
    this.result = result;
    this.call = call;
    if (length !== undefined) this.length = length; // FIPC_TOO_LARGE of a receive: the message's length
  }
}

// The property under which a zero-copy view's ArrayBuffer holds its connection's handle
const HELD = Symbol('fipc.connection');

const located = locate();
const native = require(located.addon);
const library = native.init(
  (result, call, length) => new FipcError(result, call, length),
  (id, kind, opcode, status, payload, length) => (payload !== undefined
    ? { id, kind, opcode, status, payload }
    : { id, kind, opcode, status, length }),
  (arrayBuffer, handle) => {
    arrayBuffer[HELD] = handle; // a view that can be reached keeps its connection (and the ring) alive
    return new Uint8Array(arrayBuffer);
  },
  located.library,
);

// Only this module makes listeners and connections
const TOKEN = Symbol('fipc');

function connection(handle) {
  return new Connection(TOKEN, handle);
}

/** A server's name: it accepts one client at a time. Listener.listen makes one. */
class Listener {
  #handle;
  #name;

  constructor(token, handle, name) {
    if (token !== TOKEN) throw new TypeError('fipc: Listener.listen(name, capacity) makes a listener');
    this.#handle = handle;
    this.#name = name;
  }

  /**
   * Claims `name` (1-245 characters of [A-Za-z0-9_.-]) and listens on it, with rings of `capacity` bytes each way (a
   * power of two, 1 KiB to 2 GiB). Doesn't wait. FIPC_ADDR_IN_USE if another listener holds the name.
   */
  static listen(name, capacity) {
    return new Listener(TOKEN, native.listen(name, capacity), name);
  }

  /** The name it listens on. */
  get name() {
    return this.#name;
  }

  /** Whether close() has been called. */
  get closed() {
    return native.closed(this.#handle);
  }

  /**
   * Waits up to `timeout` ms for a client and sets its connection up, on a thread of the listener's own. One client
   * at a time: FIPC_INVALID while the connection it returned last is open.
   */
  accept(timeout) {
    return native.accept(this.#handle, timeout).then(connection);
  }

  /** accept() on this thread: a timeout of 0 polls (a client then takes one or two calls). */
  acceptSync(timeout) {
    return connection(native.acceptSync(this.#handle, timeout));
  }

  /** Makes every accept that waits, now or later, fail with FIPC_CANCELLED. Final. */
  cancel() {
    native.listenerCancel(this.#handle);
  }

  /** Stops listening (a pending accept fails with FIPC_CANCELLED); the connections it accepted stay open. */
  close() {
    native.listenerClose(this.#handle);
  }
}

/** One connection: two rings, one per direction. Connection.connect (a client) or Listener.accept (a server). */
class Connection {
  #handle;

  constructor(token, handle) {
    if (token !== TOKEN) {
      throw new TypeError('fipc: Connection.connect(name) or a listener\'s accept() makes a connection');
    }
    this.#handle = handle;
  }

  /**
   * Connects to the server listening on `name`, waiting up to `timeout` ms for it to listen and to accept, on a
   * thread of its own. The server runs in another process (or on another thread: a Worker, or an async accept).
   */
  static connect(name, timeout) {
    return native.connect(name, timeout).then(connection);
  }

  /** connect() on this thread. The server must not wait on this thread meanwhile (a Sync accept would). */
  static connectSync(name, timeout) {
    return connection(native.connectSync(name, timeout));
  }

  /** The longest message the zero-copy calls take: the ring's capacity less 64 bytes. */
  get maxPiece() {
    return native.maxPiece(this.#handle);
  }

  /** Whether close() has been called. */
  get closed() {
    return native.closed(this.#handle);
  }

  /** Whether a call has failed with FIPC_DISCONNECTED: the peer closed the connection or its process ended. Final. */
  get ended() {
    return native.ended(this.#handle);
  }

  /** Sends one message of any size, at least 1 byte: a string (as UTF-8), a TypedArray, a DataView or an ArrayBuffer. */
  send(data, timeout) {
    return native.send(this.#handle, data, timeout);
  }

  sendSync(data, timeout) {
    native.sendSync(this.#handle, data, timeout);
  }

  /** Receives one message of any size, as a new Buffer. */
  receive(timeout) {
    return native.receive(this.#handle, timeout);
  }

  receiveSync(timeout) {
    return native.receiveSync(this.#handle, timeout);
  }

  /**
   * Receives one message into `buffer` (a TypedArray, a DataView or an ArrayBuffer) and returns its length. A message
   * longer than the buffer stays queued: FIPC_TOO_LARGE, with its length in the error's `length`.
   */
  receiveInto(buffer, timeout) {
    return native.receiveInto(this.#handle, buffer, timeout);
  }

  receiveIntoSync(buffer, timeout) {
    return native.receiveIntoSync(this.#handle, buffer, timeout);
  }

  /**
   * Zero-copy send, step 1: room for `length` bytes (1 to maxPiece) in the ring, as a Uint8Array. Write the message
   * there, then sendCommit. The view is detached (its length becomes 0) at the commit, the next send call and close.
   */
  sendAcquire(length, timeout) {
    return native.sendAcquire(this.#handle, length, timeout);
  }

  sendAcquireSync(length, timeout) {
    return native.sendAcquireSync(this.#handle, length, timeout);
  }

  /** Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message. Doesn't wait. */
  sendCommit(length) {
    native.sendCommit(this.#handle, length);
  }

  /**
   * Zero-copy receive, step 1: the next message, as a Uint8Array over the ring (don't write to it). The view is
   * detached (its length becomes 0) at receiveRelease, the next receive call and close. A message of several pieces
   * stays queued for receive: FIPC_TOO_LARGE, with its length.
   */
  receiveAcquire(timeout) {
    return native.receiveAcquire(this.#handle, timeout);
  }

  receiveAcquireSync(timeout) {
    return native.receiveAcquireSync(this.#handle, timeout);
  }

  /** Zero-copy receive, step 2: frees the acquired message's room (without one, does nothing). */
  receiveRelease() {
    native.receiveRelease(this.#handle);
  }

  /** Sends a request with a payload of any size, possibly empty; resolves to its id (from 1, increasing). */
  rpcSubmit(opcode, data, timeout) {
    return native.rpcSubmit(this.#handle, opcode, data, timeout);
  }

  rpcSubmitSync(opcode, data, timeout) {
    return native.rpcSubmitSync(this.#handle, opcode, data, timeout);
  }

  /** Sends the response to request `id`, with the application's `status` (an int32) and a payload, possibly empty. */
  rpcRespond(id, opcode, status, data, timeout) {
    return native.rpcRespond(this.#handle, id, opcode, status, data, timeout);
  }

  rpcRespondSync(id, opcode, status, data, timeout) {
    native.rpcRespondSync(this.#handle, id, opcode, status, data, timeout);
  }

  /** Receives one request or response: { id, kind (RPC_REQUEST or RPC_RESPONSE), opcode, status, payload }. */
  rpcReceive(timeout) {
    return native.rpcReceive(this.#handle, timeout);
  }

  rpcReceiveSync(timeout) {
    return native.rpcReceiveSync(this.#handle, timeout);
  }

  /**
   * Receives one request or response with its payload in `buffer`: { id, kind, opcode, status, length }. A payload
   * longer than the buffer stays queued: FIPC_TOO_LARGE, with its length.
   */
  rpcReceiveInto(buffer, timeout) {
    return native.rpcReceiveInto(this.#handle, buffer, timeout);
  }

  rpcReceiveIntoSync(buffer, timeout) {
    return native.rpcReceiveIntoSync(this.#handle, buffer, timeout);
  }

  /** Makes every call that waits, now or later, fail with FIPC_CANCELLED; calls that needn't wait still work. Final. */
  cancel() {
    native.cancel(this.#handle);
  }

  /**
   * Ends the connection (the peer gets FIPC_DISCONNECTED once it has received what this side sent) and detaches every
   * zero-copy view. Pending async calls fail with FIPC_CANCELLED. Closing it again does nothing.
   */
  close() {
    native.close(this.#handle);
  }
}

if (typeof Symbol.dispose === 'symbol') {
  Listener.prototype[Symbol.dispose] = Listener.prototype.close;
  Connection.prototype[Symbol.dispose] = Connection.prototype.close;
}

module.exports = {
  Listener,
  Connection,
  FipcError,
  Result,
  resultStr,
  NO_WAIT: 0,
  FOREVER: -1,
  RPC_REQUEST: 1,
  RPC_RESPONSE: 2,
  version: VERSION,
  library,
  addon: located.addon,
};
