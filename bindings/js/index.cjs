'use strict';
/*
 * FastIPC for JavaScript, in Node.js, Bun and Deno, through shared memory: messages between two processes
 * (streams), broadcasts from one publisher to many subscribers, work queues from many producers to many consumers,
 * and services, whose clients' requests each go to one of a pool of servers, which replies. One Node-API addon
 * (fipc.node) over the C API of include/fipc.h, the same file for all three runtimes.
 *
 *     const fipc = require('fipc-js');   // or: import { createStream, connectStream, ... } from 'fipc-js'
 *
 *     const listener = fipc.createStream('my_channel', 1 << 20);        // the server: rings of 1 MiB each way
 *     const conn = await listener.accept();                             // waits for a client
 *     const conn = await fipc.connectStream('my_channel', 5000);        // the client, in another process
 *
 *     const publisher = fipc.createBroadcast('my_broadcast', 1 << 16, 8, 5000);   // a broadcast: its publisher
 *     const subscriber = await fipc.joinBroadcast('my_broadcast', fipc.SUBSCRIBER_GATED, 5000);
 *
 *     const queue = fipc.createQueue('my_queue', 4096, 8, 8);                      // a work queue: its owner
 *     const producer = await fipc.joinQueue('my_queue', fipc.PRODUCER, 5000);      // the owner's process too
 *     const consumer = await fipc.joinQueue('my_queue', fipc.CONSUMER, 5000);
 *
 *     const service = fipc.createService('my_service', 4096, 256, 8, 2);           // a service: its owner
 *     const server = await fipc.joinService('my_service', fipc.SERVER, 5000);      // the owner's process too
 *     const client = await fipc.joinService('my_service', fipc.CLIENT, 5000);
 *     const reply = await client.call('ping');                                     // the request, then its reply
 *
 * A broadcast's, a queue's or a service's owner admits its joiners by itself, on a thread of the library's: no call of
 * the owner's waits for them. Every call that may wait has two forms. `receive()` returns a Promise and doesn't block
 * the event loop: it takes a message at once if one is there, else a thread of the connection's own waits for it.
 * `receiveSync()` waits on the calling thread, which blocks the event loop meanwhile. Timeouts are milliseconds, the
 * last argument: 0 (NO_WAIT) doesn't wait, undefined or a negative number (FOREVER) waits as long as it takes.
 *
 * A call the library refuses rejects (or throws) a FipcError whose `code` names the result: 'FIPC_TIMEOUT',
 * 'FIPC_DISCONNECTED', ... A misuse throws (or rejects) a TypeError or RangeError (ERR_INVALID_ARG_TYPE,
 * ERR_OUT_OF_RANGE) or an Error with the code ERR_FIPC_CLOSED (a closed handle) or ERR_FIPC_BUSY (a Sync call, a
 * commit or a release while an async call of the same direction is pending, anything beside a pending async acquire,
 * a service client's other calls while its call is pending, or a service server's reply while its receive is).
 */

const fs = require('node:fs');
const path = require('node:path');

const VERSION = '1.0.0';

// platform-arch -> the library's file name; the package carries the addon and the library in prebuilds/<platform-arch>/
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
  OVERRUN: 8,
  UNSUBSCRIBED: 9,
  ABANDONED: 10,
});
const NAMES = Object.keys(Result);

/** The library's name for a result: 'FIPC_OK', 'FIPC_TIMEOUT', ...; 'FIPC_UNKNOWN' for any other value. */
function resultStr(result) {
  return Number.isInteger(result) && NAMES[result] !== undefined ? `FIPC_${NAMES[result]}` : 'FIPC_UNKNOWN';
}

/**
 * A call returned `result`, not OK. `code` is its name ('FIPC_TIMEOUT', ...). `value`, if given, is what the result
 * reports: a receive's FIPC_TOO_LARGE, the message's `length`; FIPC_OVERRUN, the number of messages `lost`; the
 * waitForMembers' other results, the `count`. `sent`, if given, is how many messages of a sendMany went.
 */
class FipcError extends Error {
  constructor(result, call, value, sent) {
    let property;
    if (value !== undefined) {
      property = result === Result.TOO_LARGE ? 'length' : result === Result.OVERRUN ? 'lost' : 'count';
    }
    const code = resultStr(result);
    super(property === 'lost' ? `${call}: ${code} (${value} lost)`
      : property === 'count' ? `${call}: ${code} (${value} joined)`
        : sent !== undefined ? `${call}: ${code} (${sent} sent)` : `${call}: ${code}`);
    this.name = 'FipcError';
    this.code = code;
    this.result = result;
    this.call = call;
    if (property) this[property] = value;
    if (sent !== undefined) this.sent = sent;
  }
}

// The property under which a zero-copy view's ArrayBuffer holds its connection's handle
const HELD = Symbol('fipc.connection');

const located = locate();
const native = require(located.addon);
const library = native.init(
  (result, call, value, sent) => new FipcError(result, call, value, sent),
  (arrayBuffer, handle) => {
    arrayBuffer[HELD] = handle; // a view that can be reached keeps its connection (and the ring) alive
    return new Uint8Array(arrayBuffer);
  },
  located.library,
  typeof Deno !== 'undefined', // Deno gets the addon's nudger (src/fipc_node.c, "Deno's nudges")
);

// Only this module makes listeners, connections, queues and services
const TOKEN = Symbol('fipc');

const CLIENT = 5;
const SERVER = 6;

/** A Connection over `handle`; `client` for a service's client, which keeps count of the replies it is owed */
function connection(handle, client = false) {
  return new Connection(TOKEN, handle, client);
}

function busy(call, why) {
  const error = new Error(`fipc: ${call} ${why}`);
  error.code = 'ERR_FIPC_BUSY';
  return error;
}

function closedConnection() {
  const error = new Error('fipc: the connection is closed');
  error.code = 'ERR_FIPC_CLOSED';
  return error;
}

/*
 * A service client's count of the replies it is owed, oldest first: `calls`, those owed to calls that gave up on them
 * (a call whose receive timed out or was cancelled; at most one, as every call drops it first), which the next call
 * receives and drops; then `sends`, those owed to send and sendCommit, which are the caller's to receive (a call
 * refuses while one is owed, so none of a call's comes after them). A reply, or FIPC_ABANDONED, answers the oldest. A
 * request is counted before its send (a receive may settle before the send's promise does) and uncounted if the send
 * fails. All of it is the JavaScript thread's: a connection belongs to one. `receiving` counts the caller's async
 * receives pending, `pending` the calls made and not settled (the one running and those waiting for their turn).
 * Calls take turns: `running` while one has it, and `waiting` holds those made meanwhile, oldest first, each waiting
 * for its turn within its own timeout; `cancelled` once the connection is cancelled or closed, which ends their waits,
 * and from then on a call sends nothing (it could never receive the reply).
 */
class Owed {
  calls = 0;
  sends = 0;
  receiving = 0;
  pending = 0;
  running = false;
  waiting = [];
  cancelled = false;
}

/** A call waiting for its turn gives up: FIPC_TIMEOUT or FIPC_CANCELLED */
function giveUp(owed, waiter, result) {
  clearTimeout(waiter.timer);
  owed.pending -= 1;
  waiter.reject(new FipcError(result, 'call'));
}

/** A call has settled: the oldest one waiting for its turn takes it */
function nextTurn(owed) {
  owed.running = false;
  const waiter = owed.waiting.shift();
  if (waiter === undefined) return;
  clearTimeout(waiter.timer);
  owed.running = true;
  waiter.start();
}

/** The connection is cancelled or closed: every call waiting for its turn, now or later, fails with FIPC_CANCELLED */
function cancelTurns(owed) {
  owed.cancelled = true;
  for (const waiter of owed.waiting.splice(0)) giveUp(owed, waiter, Result.CANCELLED);
}

/** A receive's outcome that answers the oldest request (a reply, or FIPC_ABANDONED) takes it off */
function answered(owed) {
  if (owed.calls > 0) owed.calls -= 1;
  else if (owed.sends > 0) owed.sends -= 1;
}

function abandoned(error) {
  return error instanceof FipcError && error.result === Result.ABANDONED;
}

/** An async receive's promise, counted: `mine` for the caller's own (not a call's) */
function answer(owed, promise, mine) {
  if (mine) owed.receiving += 1;
  return promise.then((value) => {
    if (mine) owed.receiving -= 1;
    answered(owed);
    return value;
  }, (error) => {
    if (mine) owed.receiving -= 1;
    if (abandoned(error)) answered(owed);
    throw error;
  });
}

/** A Sync receive, counted */
function answerSync(owed, receive) {
  let value;
  try {
    value = receive();
  } catch (error) {
    if (abandoned(error)) answered(owed);
    throw error;
  }
  answered(owed);
  return value;
}

/** An async batched receive's promise, counted: each reply, or a FIPC_ABANDONED, answers the oldest request */
function answerMany(owed, promise) {
  owed.receiving += 1;
  return promise.then((messages) => {
    owed.receiving -= 1;
    for (let i = 0; i < messages.length; i++) answered(owed);
    return messages;
  }, (error) => {
    owed.receiving -= 1;
    if (abandoned(error)) answered(owed);
    throw error;
  });
}

/** A batch's messages that didn't go, after its send failed with `error`: nothing is owed to them */
function unsent(owed, count, error) {
  owed.sends -= count - (error instanceof FipcError && error.sent !== undefined ? error.sent : 0);
}

/** A call's deadline (performance.now()'s), or null for no limit (and for a timeout the addon will refuse) */
function deadlineOf(timeout) {
  return typeof timeout === 'number' && timeout >= 0 ? performance.now() + timeout : null;
}

/** What is left of a call's timeout, for its next step */
function left(timeout, deadline) {
  return deadline === null ? timeout : Math.max(0, deadline - performance.now());
}

/** Whether a call's positive timeout has run out (a call that waited first then sends only with time left) */
function spent(timeout, deadline) {
  return deadline !== null && timeout > 0 && performance.now() >= deadline;
}

const encoder = new TextEncoder();

/**
 * A call's request, checked before the call waits for anything: its bytes (a string's UTF-8, else the TypedArray,
 * DataView or ArrayBuffer itself), or the error to fail with: a TypeError for another type, FIPC_INVALID for no bytes,
 * FIPC_TOO_LARGE over maxPiece.
 */
function checkedRequest(request, maxPiece, call) {
  let bytes = request;
  if (typeof request === 'string') {
    bytes = encoder.encode(request);
  } else if (!ArrayBuffer.isView(request) && Object.prototype.toString.call(request) !== '[object ArrayBuffer]') {
    const error = new TypeError(
      'fipc: a message is a TypedArray (a Buffer, a Uint8Array, ...), a DataView or an ArrayBuffer');
    error.code = 'ERR_INVALID_ARG_TYPE';
    return { error };
  }
  if (bytes.byteLength === 0) return { error: new FipcError(Result.INVALID, call) };
  if (bytes.byteLength > maxPiece) return { error: new FipcError(Result.TOO_LARGE, call) };
  return { bytes };
}

/**
 * A call's timeout, checked as the addon checks every other call's, before the call waits for anything: undefined,
 * null or a number other than NaN; else the TypeError to fail with. A call waiting for its turn has no other check.
 */
function timeoutError(timeout) {
  if (timeout === undefined || timeout === null || (typeof timeout === 'number' && !Number.isNaN(timeout))) {
    return null;
  }
  const error = new TypeError('fipc: a timeout is a number of milliseconds, or undefined (no limit)');
  error.code = 'ERR_INVALID_ARG_TYPE';
  return error;
}

/** A call's error, named for the call (a misuse's error as it is) */
function callError(error, call) {
  return error instanceof FipcError ? new FipcError(error.result, call) : error;
}

/**
 * A stream's server: claims `name` (1-245 characters of [A-Za-z0-9_.-]) and listens on it, with rings of `capacity`
 * bytes each way (a power of two, 1 KiB to 2 GiB), and returns its Listener, which accepts one client at a time.
 * Doesn't wait. FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
function createStream(name, capacity) {
  return new Listener(TOKEN, native.createStream(name, capacity), name);
}

/**
 * A stream's client: connects to the server of the stream on `name`, waiting up to `timeout` ms for it to listen and to
 * accept, on a thread of its own. The server runs in another process (or on another thread: a Worker, or an async
 * accept).
 */
function connectStream(name, timeout) {
  return native.connectStream(name, timeout).then(connection);
}

/** connectStream() on this thread. The server must not wait on this thread meanwhile (a Sync accept would). */
function connectStreamSync(name, timeout) {
  return connection(native.connectStreamSync(name, timeout));
}

/**
 * A broadcast's publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two, 4 KiB to
 * 2 GiB; messages of up to a quarter of it) and up to `maxSubscribers` subscribers (1 to 1024), and returns the
 * publisher's Connection, the broadcast's one handle: its sends go to every subscriber, and its close ends the
 * broadcast. Doesn't wait: subscribers may join from now on, and this process admits them by itself. A gated
 * subscriber that holds the publisher back for `maxStall` ms is unsubscribed; undefined or a negative number never
 * unsubscribes, 0 refuses gated subscribers. FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the
 * name.
 */
function createBroadcast(name, capacity, maxSubscribers, maxStall) {
  return connection(native.createBroadcast(name, capacity, maxSubscribers, maxStall));
}

/**
 * A subscriber: joins the broadcast on `name` as `role`, SUBSCRIBER_LOSSY (never holds the publisher back, and may
 * lose messages) or SUBSCRIBER_GATED (misses nothing: the publisher waits for it), waiting up to `timeout` ms for the
 * publisher to create the broadcast, for a free slot, and for the publisher's process to admit it, on a thread of its
 * own. It receives the messages sent from its admission on, with receive and receiveInto (copies), then
 * FIPC_DISCONNECTED once the publisher has closed. FIPC_INVALID for another role, a name a queue or a service holds, or
 * a gated subscriber on a broadcast that refuses them. On a name a stream holds, the join learns the kind when the
 * stream's server accepts it, which brings FIPC_INVALID; while no accept runs, it waits and fails with FIPC_TIMEOUT.
 */
function joinBroadcast(name, role, timeout) {
  return native.joinBroadcast(name, role, timeout).then(connection);
}

/** joinBroadcast() on this thread: the publisher's process admits it, so the publisher may be on this thread too. */
function joinBroadcastSync(name, role, timeout) {
  return connection(native.joinBroadcastSync(name, role, timeout));
}

/**
 * A work queue's owner: claims `name` for a work queue of up to `maxProducers` producers and `maxConsumers` consumers
 * (1 to 63 producers, 1 to 64 consumers), each producer with a ring of `capacity` bytes of messages (a power of two,
 * 4 KiB to 2 GiB; messages of up to a quarter of it), and returns the Queue. Doesn't wait: members may join from now
 * on, and this process admits them by itself; the owner joins with joinQueue too. A queue lives until close() or the
 * end of its process (of its worker thread, for one a Worker created); dropping the object doesn't end it.
 * FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
function createQueue(name, capacity, maxProducers, maxConsumers) {
  return new Queue(TOKEN, native.createQueue(name, capacity, maxProducers, maxConsumers), name);
}

/**
 * A producer or a consumer: joins the work queue on `name` as `role` (PRODUCER or CONSUMER), waiting up to `timeout`
 * ms for the owner to create the queue, for a free place of the role, and for the owner's process to admit it, on a
 * thread of its own. A consumer takes each message of the queue's producers that no other consumer took, in no
 * promised order (copied, or in place), then FIPC_DISCONNECTED once the queue has ended; a producer's sends fail with
 * FIPC_DISCONNECTED from then on. FIPC_INVALID for another role, or a name a broadcast or a service holds. On a name a
 * stream holds, the join learns the kind when the stream's server accepts it, which brings FIPC_INVALID; while no
 * accept runs, it waits and fails with FIPC_TIMEOUT.
 */
function joinQueue(name, role, timeout) {
  return native.joinQueue(name, role, timeout).then(connection);
}

/** joinQueue() on this thread: the owner's process admits it, so the owner may be on this thread too. */
function joinQueueSync(name, role, timeout) {
  return connection(native.joinQueueSync(name, role, timeout));
}

/**
 * A service's owner: claims `name` for a service of up to `maxClients` clients and `maxServers` servers (1 to 64
 * each), each client with a ring of `capacity` bytes (a power of two, 4 KiB to 2 GiB) for its requests and their
 * replies, each of 1 to `maxMessage` bytes (1 to a quarter of the capacity), and returns the Service. A request holds
 * its length rounded up to 64 bytes, plus maxMessage rounded up to 64, of its client's ring from its send until its
 * reply has been received. Doesn't wait: members may join from now on, and this process admits them by itself; the
 * owner joins with joinService too. A service lives until close() or the end of its process (of its worker thread,
 * for one a Worker created); dropping the object doesn't end it. FIPC_ADDR_IN_USE if another stream, broadcast, queue
 * or service holds the name.
 */
function createService(name, capacity, maxMessage, maxClients, maxServers) {
  return new Service(TOKEN, native.createService(name, capacity, maxMessage, maxClients, maxServers), name);
}

/**
 * A client or a server: joins the service on `name` as `role` (CLIENT or SERVER), waiting up to `timeout` ms for the
 * owner to create the service, for a free place of the role, and for the owner's process to admit it, on a thread of
 * its own. A client's send is a request, and its receive the reply to its oldest request whose reply it hasn't
 * received yet (or FIPC_ABANDONED, if the server that took it ended before it replied); call() does both. A server's
 * receive takes the next request of any client and holds it, and its send replies to it, never waiting. FIPC_INVALID
 * for another role, or a name a broadcast or a queue holds. On a name a stream holds, the join learns the kind when the
 * stream's server accepts it, which brings FIPC_INVALID; while no accept runs, it waits and fails with FIPC_TIMEOUT.
 */
function joinService(name, role, timeout) {
  return native.joinService(name, role, timeout).then((handle) => connection(handle, role === CLIENT));
}

/** joinService() on this thread: the owner's process admits it, so the owner may be on this thread too. */
function joinServiceSync(name, role, timeout) {
  return connection(native.joinServiceSync(name, role, timeout), role === CLIENT);
}

/** A stream's server, from createStream: it accepts one client at a time. */
class Listener {
  #handle;
  #name;

  constructor(token, handle, name) {
    if (token !== TOKEN) throw new TypeError('fipc: createStream(name, capacity) makes a listener');
    this.#handle = handle;
    this.#name = name;
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

  /** Stops listening (a pending accept fails with FIPC_CANCELLED) and frees the name; its connections stay. */
  close() {
    native.listenerClose(this.#handle);
  }
}

/**
 * A work queue, held by its owner (createQueue). Its producers and consumers, the owner's own included, join with
 * joinQueue. A queue lives until close() or the end of its process (of its worker thread, for one a Worker created);
 * dropping the object doesn't end it: the garbage collector never closes a queue, since its use lives in its members.
 */
class Queue {
  #handle;
  #name;

  constructor(token, handle, name) {
    if (token !== TOKEN) {
      throw new TypeError('fipc: createQueue(name, capacity, maxProducers, maxConsumers) makes a queue');
    }
    this.#handle = handle;
    this.#name = name;
  }

  /** The queue's name. */
  get name() {
    return this.#name;
  }

  /** Whether close() has been called. */
  get closed() {
    return native.closed(this.#handle);
  }

  /**
   * Resolves to the number of members of `role` (PRODUCER or CONSUMER; undefined or 0 counts both) joined once at
   * least `atLeast` (0 by default: at once) are, waiting up to `timeout` ms, on a thread of the queue's own. At
   * FIPC_TIMEOUT, FIPC_CANCELLED and FIPC_DISCONNECTED (the queue has ended) the error's `count` is the count;
   * FIPC_INVALID for another role, or for atLeast over the role's maximum (maxProducers, maxConsumers, or their sum).
   * It may run beside any call; an async count that must wait queues behind one already waiting, though, so a short
   * timeout can expire late.
   */
  waitForMembers(role, atLeast, timeout) {
    return native.queueWaitForMembers(this.#handle, role, atLeast, timeout);
  }

  /** waitForMembers() on this thread: the members' joins don't need it, so it may wait for a join of this thread's. */
  waitForMembersSync(role, atLeast, timeout) {
    return native.queueWaitForMembersSync(this.#handle, role, atLeast, timeout);
  }

  /** Makes every waitForMembers that waits, now or later, fail with FIPC_CANCELLED; the queue goes on. Final. */
  cancel() {
    native.queueCancel(this.#handle);
  }

  /**
   * Ends the queue and frees the name (a pending waitForMembers fails with FIPC_CANCELLED): producers' sends fail with
   * FIPC_DISCONNECTED; consumers receive what is left in it, then FIPC_DISCONNECTED. The members' connections, the
   * owner's own included, stay open until their close. Closing it again does nothing.
   */
  close() {
    native.queueClose(this.#handle);
  }
}

/**
 * A service, held by its owner (createService). Its clients and servers, the owner's own included, join with
 * joinService. A service lives until close() or the end of its process (of its worker thread, for one a Worker
 * created); dropping the object doesn't end it: the garbage collector never closes a service, since its use lives in
 * its members.
 */
class Service {
  #handle;
  #name;

  constructor(token, handle, name) {
    if (token !== TOKEN) {
      throw new TypeError('fipc: createService(name, capacity, maxMessage, maxClients, maxServers) makes a service');
    }
    this.#handle = handle;
    this.#name = name;
  }

  /** The service's name. */
  get name() {
    return this.#name;
  }

  /** Whether close() has been called. */
  get closed() {
    return native.closed(this.#handle);
  }

  /**
   * Resolves to the number of members of `role` (CLIENT or SERVER; undefined or 0 counts both) joined once at least
   * `atLeast` (0 by default: at once) are, waiting up to `timeout` ms, on a thread of the service's own. At
   * FIPC_TIMEOUT, FIPC_CANCELLED and FIPC_DISCONNECTED (the service has ended) the error's `count` is the count;
   * FIPC_INVALID for another role, or for atLeast over the role's maximum (maxClients, maxServers, or their sum). It
   * may run beside any call; an async count that must wait queues behind one already waiting, though, so a short
   * timeout can expire late.
   */
  waitForMembers(role, atLeast, timeout) {
    return native.serviceWaitForMembers(this.#handle, role, atLeast, timeout);
  }

  /** waitForMembers() on this thread: the members' joins don't need it, so it may wait for a join of this thread's. */
  waitForMembersSync(role, atLeast, timeout) {
    return native.serviceWaitForMembersSync(this.#handle, role, atLeast, timeout);
  }

  /** Makes every waitForMembers that waits, now or later, fail with FIPC_CANCELLED; the service goes on. Final. */
  cancel() {
    native.serviceCancel(this.#handle);
  }

  /**
   * Ends the service and frees the name (a pending waitForMembers fails with FIPC_CANCELLED): clients' sends and
   * servers' receives fail with FIPC_DISCONNECTED; a client receives the replies that came before the end, then
   * FIPC_DISCONNECTED, and a server's reply fails with FIPC_DISCONNECTED. The members' connections, the owner's own
   * included, stay open until their close. Closing it again does nothing.
   */
  close() {
    native.serviceClose(this.#handle);
  }
}

/**
 * One connection. A stream's: two rings, one per direction (connectStream for a client, a listener's accept for the
 * server). A broadcast's publisher (createBroadcast) or subscriber (joinBroadcast); a work queue's producer or
 * consumer (joinQueue); a service's client or server (joinService).
 */
class Connection {
  #handle;
  #owed; // a service client's Owed, else null

  constructor(token, handle, client = false) {
    if (token !== TOKEN) {
      throw new TypeError(
        'fipc: connectStream, a listener\'s accept, joinBroadcast, joinQueue or joinService makes a connection');
    }
    this.#handle = handle;
    this.#owed = client ? new Owed() : null;
  }

  /** A service client's call is pending: its other sends and receives would take the call's place */
  #callPending(owed, call) {
    if (owed.pending === 0) return null;
    return busy(call, 'while a call of this client is pending: await it first');
  }

  /**
   * The longest message the zero-copy calls take: the ring's capacity less 64 bytes (a broadcast's or a queue's: a
   * quarter of its capacity; a service's: its maxMessage).
   */
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

  /**
   * Sends one message of any size, at least 1 byte: a string (as UTF-8), a TypedArray, a DataView or an ArrayBuffer.
   * On a service's client: a request (of one piece, up to maxPiece), whose reply a receive returns; on a server: the
   * reply to the request it holds, which never waits (FIPC_INVALID with no request held).
   */
  send(data, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.send(this.#handle, data, timeout);
    const pending = this.#callPending(owed, 'send');
    if (pending) return Promise.reject(pending);
    owed.sends += 1;
    return native.send(this.#handle, data, timeout).catch((error) => { // a message of another type rejects too
      owed.sends -= 1;
      throw error;
    });
  }

  sendSync(data, timeout) {
    const owed = this.#owed;
    if (owed === null) {
      native.sendSync(this.#handle, data, timeout);
      return;
    }
    const pending = this.#callPending(owed, 'sendSync');
    if (pending) throw pending;
    owed.sends += 1;
    try {
      native.sendSync(this.#handle, data, timeout);
    } catch (error) {
      owed.sends -= 1;
      throw error;
    }
  }

  /**
   * Receives one message of any size, as a new Buffer. A lossy subscriber that fell a lap behind gets FIPC_OVERRUN
   * instead, with the number of messages it lost in the error's `lost`; its next receive goes on from a recent message.
   * A work queue's consumer receives the next message no other consumer took. A service's client receives the reply
   * to its oldest request whose reply it hasn't received yet, or FIPC_ABANDONED if the server that took that request
   * ended before it replied (the request counts as answered): after a call that gave up, that call's late reply first.
   * A server receives the next request of any client, which it holds until its reply (FIPC_INVALID while it holds
   * one).
   */
  receive(timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receive(this.#handle, timeout);
    const pending = this.#callPending(owed, 'receive');
    if (pending) return Promise.reject(pending);
    return answer(owed, native.receive(this.#handle, timeout), true);
  }

  receiveSync(timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveSync(this.#handle, timeout);
    const pending = this.#callPending(owed, 'receiveSync');
    if (pending) throw pending;
    return answerSync(owed, () => native.receiveSync(this.#handle, timeout));
  }

  /**
   * Receives one message into `buffer` (a TypedArray, a DataView or an ArrayBuffer) and returns its length. A message
   * longer than the buffer stays queued: FIPC_TOO_LARGE, with its length in the error's `length` (on a work queue,
   * another consumer may take it first).
   */
  receiveInto(buffer, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveInto(this.#handle, buffer, timeout);
    const pending = this.#callPending(owed, 'receiveInto');
    if (pending) return Promise.reject(pending);
    return answer(owed, native.receiveInto(this.#handle, buffer, timeout), true);
  }

  receiveIntoSync(buffer, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveIntoSync(this.#handle, buffer, timeout);
    const pending = this.#callPending(owed, 'receiveIntoSync');
    if (pending) throw pending;
    return answerSync(owed, () => native.receiveIntoSync(this.#handle, buffer, timeout));
  }

  /**
   * Sends `messages` (an array of at least one, each of at least 1 byte: strings, as UTF-8, TypedArrays, DataViews or
   * ArrayBuffers, copied when the call is made) in order and publishes them together, with at most one wake of the
   * receivers, and resolves to how many went: all of them. The timeout bounds the whole call (NO_WAIT: only what fits
   * now). A failure is the first message that didn't go, as send would fail, with how many went (all those before it)
   * in the error's `sent`. Every message is checked first, before any goes: FIPC_INVALID for an empty one or an empty
   * batch, FIPC_TOO_LARGE over maxPiece on a broadcast, a work queue or a service (a stream sends a longer message in
   * pieces). A service's client sends a batch of requests, whose replies come in their order and are owed as send's
   * are; a server's batch is FIPC_INVALID. Batch both ends: a receiver that takes one message per call falls behind a
   * batching sender.
   */
  sendMany(messages, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.sendMany(this.#handle, messages, timeout);
    const pending = this.#callPending(owed, 'sendMany');
    if (pending) return Promise.reject(pending);
    const count = Array.isArray(messages) ? messages.length : 0;
    owed.sends += count; // their replies are owed from before the send
    return native.sendMany(this.#handle, messages, timeout).catch((error) => {
      unsent(owed, count, error);
      throw error;
    });
  }

  sendManySync(messages, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.sendManySync(this.#handle, messages, timeout);
    const pending = this.#callPending(owed, 'sendManySync');
    if (pending) throw pending;
    const count = Array.isArray(messages) ? messages.length : 0;
    owed.sends += count;
    try {
      return native.sendManySync(this.#handle, messages, timeout);
    } catch (error) {
      unsent(owed, count, error);
      throw error;
    }
  }

  /**
   * Waits for the next message, as receive, then takes every further message already there, up to `maxCount` (16 if
   * undefined; at least 1) and as many as fit 64 KiB (a longer first message comes alone), and resolves to them, each a
   * new Buffer, in order. After the first it never waits, and stops before a message that isn't there yet, doesn't
   * fit, is in pieces, or would bring another result: the next receive returns it. A failure is the first message's, as
   * receive fails, with nothing else taken: FIPC_OVERRUN with the number lost in `lost`, a service client's
   * FIPC_ABANDONED, ...; FIPC_INVALID for a maxCount of 0, on a publisher, a producer and a service's server (which
   * holds one request at a time). A broadcast's subscriber takes, after its first, only the messages its publisher has
   * moved past (the newest come in the next calls); a work queue's consumer acknowledges each message before the call
   * returns; a service's client receives its replies in its requests' order.
   */
  receiveMany(maxCount, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveMany(this.#handle, maxCount, timeout);
    const pending = this.#callPending(owed, 'receiveMany');
    if (pending) return Promise.reject(pending);
    return answerMany(owed, native.receiveMany(this.#handle, maxCount, timeout));
  }

  receiveManySync(maxCount, timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveManySync(this.#handle, maxCount, timeout);
    const pending = this.#callPending(owed, 'receiveManySync');
    if (pending) throw pending;
    let messages;
    try {
      messages = native.receiveManySync(this.#handle, maxCount, timeout);
    } catch (error) {
      if (abandoned(error)) answered(owed);
      throw error;
    }
    for (let i = 0; i < messages.length; i++) answered(owed);
    return messages;
  }

  /**
   * Zero-copy send, step 1: room for `length` bytes (1 to maxPiece) in the ring, as a Uint8Array. Write the message
   * there, then sendCommit. The view is detached (its length becomes 0) at the commit, the next send call and close.
   */
  sendAcquire(length, timeout) {
    const pending = this.#owed && this.#callPending(this.#owed, 'sendAcquire');
    if (pending) return Promise.reject(pending);
    return native.sendAcquire(this.#handle, length, timeout);
  }

  sendAcquireSync(length, timeout) {
    const pending = this.#owed && this.#callPending(this.#owed, 'sendAcquireSync');
    if (pending) throw pending;
    return native.sendAcquireSync(this.#handle, length, timeout);
  }

  /**
   * Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message. Doesn't wait. On a
   * service's server it replies, and detaches the view of the request held in place once the reply gave it up.
   */
  sendCommit(length) {
    const owed = this.#owed;
    if (owed === null) {
      native.sendCommit(this.#handle, length);
      return;
    }
    const pending = this.#callPending(owed, 'sendCommit');
    if (pending) throw pending;
    owed.sends += 1;
    try {
      native.sendCommit(this.#handle, length);
    } catch (error) {
      owed.sends -= 1;
      throw error;
    }
  }

  /**
   * Zero-copy receive, step 1: the next message, as a Uint8Array over the ring (don't write to it). The view is
   * detached (its length becomes 0) at receiveRelease, the next receive call and close. A message of several pieces
   * stays queued for receive: FIPC_TOO_LARGE, with its length. FIPC_INVALID on a broadcast's subscriber,
   * which receives copies: a gated one's message can become inaccessible while a view is read. A service's client
   * takes its next reply in place (or FIPC_ABANDONED, as receive); a server the next request, whose view lasts until
   * a reply (its send or sendCommit) gives the request up, or close.
   */
  receiveAcquire(timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveAcquire(this.#handle, timeout);
    const pending = this.#callPending(owed, 'receiveAcquire');
    if (pending) return Promise.reject(pending);
    return answer(owed, native.receiveAcquire(this.#handle, timeout), true);
  }

  receiveAcquireSync(timeout) {
    const owed = this.#owed;
    if (owed === null) return native.receiveAcquireSync(this.#handle, timeout);
    const pending = this.#callPending(owed, 'receiveAcquireSync');
    if (pending) throw pending;
    return answerSync(owed, () => native.receiveAcquireSync(this.#handle, timeout));
  }

  /**
   * Zero-copy receive, step 2: frees the acquired message's room (without one, does nothing). On a service's server it
   * does nothing: the request stays held, and its view valid, until the reply.
   */
  receiveRelease() {
    const pending = this.#owed && this.#callPending(this.#owed, 'receiveRelease');
    if (pending) throw pending;
    native.receiveRelease(this.#handle);
  }

  /**
   * A service's client: sends `request` (as send takes it) and resolves to its reply, a new Buffer, waiting up to
   * `timeout` ms in all; FIPC_ABANDONED if the server that took it ended before it replied. A call checks its request
   * first: FIPC_INVALID for an empty one, FIPC_TOO_LARGE over maxPiece, a TypeError for another type, at once, even
   * while a reply is owed; and its timeout, as every call does (a TypeError for one that is neither a number nor
   * undefined, or is NaN), before it waits for its turn. Then a call on a cancelled connection fails with
   * FIPC_CANCELLED at once, whatever its timeout, and sends nothing: it could never receive the reply. A call never
   * resolves to another request's reply: one whose receive timed out or was cancelled leaves its reply owed, and the
   * next call first receives and drops that reply (at most one is owed, since every call drops it first; an
   * FIPC_ABANDONED in its place counts too), within its own timeout and before it sends. A call that had to drop an
   * owed reply or wait for its turn first sends its request only if its timeout has time left: once a positive timeout
   * has run out, it fails with FIPC_TIMEOUT without sending, so no server sees that request; with NO_WAIT no step
   * waits, and it sends if there is room. After a call that gave up, a plain receive returns that call's late reply (or
   * FIPC_ABANDONED) first: replies come in request order. FIPC_INVALID while a request sent with send (or sendCommit)
   * still has its reply to be received (that reply is the caller's: receive it first), and on any connection other than
   * a service's client. Calls run one at a time, in the order they were made: a call made while another is pending
   * waits for its turn, its timeout counting from when it was made, and fails with FIPC_TIMEOUT when that runs out
   * first (or FIPC_CANCELLED at a cancel or a close), the calls behind it keeping their order. While a call is pending,
   * the client's other sends and receives fail with ERR_FIPC_BUSY, and so does a call while an async receive of the
   * caller's is pending.
   */
  call(request, timeout) {
    const owed = this.#owed;
    if (native.closed(this.#handle)) return Promise.reject(closedConnection());
    if (owed === null) return Promise.reject(new FipcError(Result.INVALID, 'call'));
    const { bytes, error } = checkedRequest(request, native.maxPiece(this.#handle), 'call');
    if (error) return Promise.reject(error); // at once, before the call waits for anything
    const badTimeout = timeoutError(timeout);
    if (badTimeout) return Promise.reject(badTimeout);
    // A cancelled connection: the reply could never be received, so the request never goes
    if (owed.cancelled) return Promise.reject(new FipcError(Result.CANCELLED, 'call'));
    if (owed.receiving > 0) {
      return Promise.reject(busy('call', 'while an asynchronous receive of this client is pending: await it first'));
    }
    const deadline = deadlineOf(timeout);
    owed.pending += 1; // before the call runs: it may settle at once
    if (!owed.running) {
      owed.running = true;
      return this.#call(owed, bytes, timeout, deadline, false);
    }
    // Another call has the turn: this one waits for its own, in order, until its timeout or a cancel
    return new Promise((resolve, reject) => {
      const waiter = {
        start: () => resolve(this.#call(owed, bytes, timeout, deadline, true)),
        reject,
        timer: undefined,
      };
      if (deadline !== null) {
        if (timeout === 0) return giveUp(owed, waiter, Result.TIMEOUT);
        waiter.timer = setTimeout(() => {
          owed.waiting.splice(owed.waiting.indexOf(waiter), 1);
          giveUp(owed, waiter, Result.TIMEOUT);
        }, Math.min(Math.ceil(timeout), 0x7fffffff));
      }
      owed.waiting.push(waiter);
    });
  }

  /** A call's turn: `waited` if it waited for it */
  async #call(owed, request, timeout, deadline, waited) {
    const handle = this.#handle;
    try {
      if (owed.sends > 0) throw new FipcError(Result.INVALID, 'call'); // a reply of send's: the caller's to receive
      while (owed.calls > 0) { // the reply owed to a call that gave up on it: dropped
        try {
          await answer(owed, native.receive(handle, left(timeout, deadline)), false);
        } catch (error) {
          if (!abandoned(error)) throw error;
        }
        waited = true;
      }
      if (waited && spent(timeout, deadline)) throw new FipcError(Result.TIMEOUT, 'call'); // its request never goes
      if (owed.cancelled) throw new FipcError(Result.CANCELLED, 'call'); // cancelled while it dropped: it never goes
      owed.calls += 1; // its reply is owed until a receive answers it
      try {
        await native.send(handle, request, left(timeout, deadline));
      } catch (error) {
        owed.calls -= 1;
        throw error;
      }
      return await answer(owed, native.receive(handle, left(timeout, deadline)), false);
    } catch (error) {
      throw callError(error, 'call');
    } finally {
      owed.pending -= 1;
      nextTurn(owed);
    }
  }

  /**
   * call() on this thread. ERR_FIPC_BUSY while an async call, or an async receive of the caller's, is pending on this
   * client.
   */
  callSync(request, timeout) {
    const owed = this.#owed;
    if (native.closed(this.#handle)) throw closedConnection();
    if (owed === null) throw new FipcError(Result.INVALID, 'callSync');
    const { bytes, error } = checkedRequest(request, native.maxPiece(this.#handle), 'callSync');
    if (error) throw error; // at once, before the call waits for anything
    const badTimeout = timeoutError(timeout);
    if (badTimeout) throw badTimeout;
    if (owed.cancelled) throw new FipcError(Result.CANCELLED, 'callSync'); // as call's
    if (owed.pending > 0 || owed.receiving > 0) {
      throw busy('callSync', 'while an asynchronous call or receive of this client is pending: await it first');
    }
    const handle = this.#handle;
    const deadline = deadlineOf(timeout);
    try {
      if (owed.sends > 0) throw new FipcError(Result.INVALID, 'callSync');
      let waited = false;
      while (owed.calls > 0) {
        try {
          answerSync(owed, () => native.receiveSync(handle, left(timeout, deadline)));
        } catch (error) {
          if (!abandoned(error)) throw error;
        }
        waited = true;
      }
      if (waited && spent(timeout, deadline)) throw new FipcError(Result.TIMEOUT, 'callSync');
      owed.calls += 1;
      try {
        native.sendSync(handle, bytes, left(timeout, deadline));
      } catch (error) {
        owed.calls -= 1;
        throw error;
      }
      return answerSync(owed, () => native.receiveSync(handle, left(timeout, deadline)));
    } catch (error) {
      throw callError(error, 'callSync');
    }
  }

  /**
   * A broadcast's publisher: resolves to the number of subscribers joined once at least `atLeast` (0 by default: at
   * once) are, waiting up to `timeout` ms, on the connection's receiving thread (a publisher never receives). Wait for
   * the subscribers a send must reach before the first send. At FIPC_TIMEOUT and FIPC_CANCELLED the error's `count` is
   * the count; FIPC_INVALID on any other connection, or for atLeast over maxSubscribers. It may run beside any call of
   * the connection; an async count that must wait queues behind one already waiting, though, so a short timeout can
   * expire late.
   */
  waitForMembers(atLeast, timeout) {
    return native.waitForMembers(this.#handle, atLeast, timeout);
  }

  /** waitForMembers() on this thread: subscribers join without it, so it may wait for a join of this thread's. */
  waitForMembersSync(atLeast, timeout) {
    return native.waitForMembersSync(this.#handle, atLeast, timeout);
  }

  /**
   * Makes every call that waits, now or later, fail with FIPC_CANCELLED; calls that needn't wait still work. Final. A
   * service client's call, which waits for its reply, fails at once from then on, sending nothing.
   */
  cancel() {
    if (this.#owed !== null) cancelTurns(this.#owed); // first: a call that doesn't see it sent before the cancel
    native.cancel(this.#handle);
  }

  /**
   * Ends the connection (the peer gets FIPC_DISCONNECTED once it has received what this side sent) and detaches every
   * zero-copy view. Pending async calls fail with FIPC_CANCELLED. Closing it again does nothing.
   */
  close() {
    native.close(this.#handle);
    if (this.#owed !== null) cancelTurns(this.#owed);
  }
}

if (typeof Symbol.dispose === 'symbol') {
  Listener.prototype[Symbol.dispose] = Listener.prototype.close;
  Queue.prototype[Symbol.dispose] = Queue.prototype.close;
  Service.prototype[Symbol.dispose] = Service.prototype.close;
  Connection.prototype[Symbol.dispose] = Connection.prototype.close;
}

module.exports = {
  createStream,
  connectStream,
  connectStreamSync,
  createBroadcast,
  joinBroadcast,
  joinBroadcastSync,
  createQueue,
  joinQueue,
  joinQueueSync,
  createService,
  joinService,
  joinServiceSync,
  Listener,
  Queue,
  Service,
  Connection,
  FipcError,
  Result,
  resultStr,
  NO_WAIT: 0,
  FOREVER: -1,
  SUBSCRIBER_LOSSY: 1,
  SUBSCRIBER_GATED: 2,
  PRODUCER: 3,
  CONSUMER: 4,
  CLIENT,
  SERVER,
  version: VERSION,
  library,
  addon: located.addon,
};
