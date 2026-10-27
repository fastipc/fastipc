/**
 * FastIPC for JavaScript, in Node.js, Bun and Deno, through shared memory: messages between two processes
 * (streams: {@link createStream}, {@link connectStream}), broadcasts from one publisher to many subscribers
 * ({@link createBroadcast}, {@link joinBroadcast}), work queues that hand the messages of many producers each to
 * one of many consumers ({@link createQueue}, {@link joinQueue}), and services that hand the requests of many clients
 * each to one of a pool of servers, whose reply goes back to its client ({@link createService}, {@link joinService}).
 *
 * Every call that may wait has an async form, which returns a Promise and leaves the event loop running, and a Sync
 * form, which waits on the calling thread. Timeouts are milliseconds: 0 ({@link NO_WAIT}) doesn't wait, `undefined`
 * or a negative number ({@link FOREVER}) waits as long as it takes. A call the library refuses fails with a
 * {@link FipcError}.
 */

/** A message to send: a string (sent as UTF-8), or bytes in place. */
export type Data = string | ArrayBufferView | ArrayBuffer;

/** A buffer to receive into, in place. */
export type ReceiveBuffer = ArrayBufferView | ArrayBuffer;

/** Milliseconds: 0 doesn't wait; `undefined` or a negative number waits as long as it takes. */
export type Timeout = number | undefined;

/** Doesn't wait. */
export declare const NO_WAIT: 0;
/** Waits as long as it takes. */
export declare const FOREVER: -1;
/** A broadcast subscriber's role: lossy, it never holds the publisher back and may lose messages. */
export declare const SUBSCRIBER_LOSSY: 1;
/** A broadcast subscriber's role: gated, it misses nothing (the publisher waits for it). */
export declare const SUBSCRIBER_GATED: 2;
/** A work queue member's role: a producer, which sends. */
export declare const PRODUCER: 3;
/** A work queue member's role: a consumer, which receives each message no other consumer took. */
export declare const CONSUMER: 4;
/** A service member's role: a client, whose sends are requests and whose receives their replies, in order. */
export declare const CLIENT: 5;
/** A service member's role: a server, which receives the next request of any client and replies with a send. */
export declare const SERVER: 6;

/** {@link joinBroadcast}'s roles. */
export type SubscriberRole = typeof SUBSCRIBER_LOSSY | typeof SUBSCRIBER_GATED;
/** {@link joinQueue}'s roles, and the roles a queue's count of members counts. */
export type QueueRole = typeof PRODUCER | typeof CONSUMER;
/** {@link joinService}'s roles, and the roles a service's count of members counts. */
export type ServiceRole = typeof CLIENT | typeof SERVER;

/** The package's version. */
export declare const version: string;
/** The path the native library was loaded from (or its bare name, if the system's search found it). */
export declare const library: string;
/** The path of the Node-API addon. */
export declare const addon: string;

/** The C API's results (fipc_result_t). */
export declare const Result: Readonly<{
  OK: 0;
  TIMEOUT: 1;
  DISCONNECTED: 2;
  CANCELLED: 3;
  TOO_LARGE: 4;
  INVALID: 5;
  NO_MEMORY: 6;
  ADDR_IN_USE: 7;
  OVERRUN: 8;
  UNSUBSCRIBED: 9;
  ABANDONED: 10;
}>;

/** A result's name, as a FipcError's `code`. */
export type ResultCode =
  | 'FIPC_TIMEOUT'
  | 'FIPC_DISCONNECTED'
  | 'FIPC_CANCELLED'
  | 'FIPC_TOO_LARGE'
  | 'FIPC_INVALID'
  | 'FIPC_NO_MEMORY'
  | 'FIPC_ADDR_IN_USE'
  | 'FIPC_OVERRUN'
  | 'FIPC_UNSUBSCRIBED'
  | 'FIPC_ABANDONED';

/** The library's name for a result: 'FIPC_OK', 'FIPC_TIMEOUT', ...; 'FIPC_UNKNOWN' for any other value. */
export declare function resultStr(result: number): string;

/** A call returned a result other than OK. */
export declare class FipcError extends Error {
  readonly name: 'FipcError';
  /** The result's name: 'FIPC_TIMEOUT', 'FIPC_DISCONNECTED', ... */
  readonly code: ResultCode;
  /** The result's value ({@link Result}). */
  readonly result: number;
  /** The method that failed ('receive', 'acceptSync', 'call', ...). */
  readonly call: string;
  /** FIPC_TOO_LARGE of a receive: the message's length; the message stays queued. */
  readonly length?: number;
  /** FIPC_OVERRUN of a lossy subscriber's receive: the number of messages it lost, at least 1 (exact up to 2^53). */
  readonly lost?: number;
  /** waitForMembers' FIPC_TIMEOUT, FIPC_CANCELLED and (a queue's or a service's) FIPC_DISCONNECTED: the members. */
  readonly count?: number;
  /** sendMany's failure: how many of its messages went, all those before the one that didn't. */
  readonly sent?: number;
}

/**
 * A stream's server: claims `name` (1-245 characters of [A-Za-z0-9_.-], not starting with '.' or '-') and listens on
 * it, with rings of `capacity` bytes each way (a power of two, 1 KiB to 2 GiB). Doesn't wait. Fails with
 * FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
export declare function createStream(name: string, capacity: number): Listener;
/**
 * A stream's client: connects to the server of the stream on `name`, waiting for it to listen and to accept, on a
 * thread of its own.
 */
export declare function connectStream(name: string, timeout?: Timeout): Promise<Connection>;
/** connectStream() on the calling thread: the server must not wait on this thread meanwhile. */
export declare function connectStreamSync(name: string, timeout?: Timeout): Connection;

/**
 * A broadcast's publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two, 4 KiB to
 * 2 GiB; messages of up to a quarter of it) and up to `maxSubscribers` subscribers (1 to 1024), and returns the
 * publisher's connection, the broadcast's one handle: its sends go to every subscriber, and its close ends the
 * broadcast. Doesn't wait: this process admits the subscribers by itself. A gated subscriber that holds the publisher
 * back for `maxStall` ms is unsubscribed; `undefined` or a negative number never unsubscribes, 0 refuses gated
 * subscribers. Fails with FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
export declare function createBroadcast(
  name: string,
  capacity: number,
  maxSubscribers: number,
  maxStall?: Timeout,
): Connection;
/**
 * A subscriber: joins the broadcast on `name` as {@link SUBSCRIBER_LOSSY} or {@link SUBSCRIBER_GATED}, waiting for the
 * publisher to create the broadcast, for a free slot and for the publisher's process to admit it, on a thread of its
 * own. It receives (copies only) the messages sent from its admission on, then FIPC_DISCONNECTED once the publisher
 * has closed.
 */
export declare function joinBroadcast(name: string, role: SubscriberRole, timeout?: Timeout): Promise<Connection>;
/** joinBroadcast() on the calling thread: the publisher's process admits it, so the publisher may be on this thread. */
export declare function joinBroadcastSync(name: string, role: SubscriberRole, timeout?: Timeout): Connection;

/**
 * A work queue's owner: claims `name` for a work queue of up to `maxProducers` producers and `maxConsumers` consumers
 * (1 to 63 producers, 1 to 64 consumers), each producer with a ring of `capacity` bytes of messages (a power of two,
 * 4 KiB to 2 GiB; messages of up to a quarter of it). Doesn't wait: this process admits the members by itself; the
 * owner joins with {@link joinQueue} too. A queue lives until close() or the end of its process (of its worker thread,
 * for one a Worker created); dropping the object doesn't end it. Fails with FIPC_ADDR_IN_USE if another stream,
 * broadcast, queue or service holds the name.
 */
export declare function createQueue(name: string, capacity: number, maxProducers: number, maxConsumers: number): Queue;
/**
 * A producer or a consumer: joins the work queue on `name` as {@link PRODUCER} or {@link CONSUMER}, waiting for the
 * owner to create the queue, for a free place of the role and for the owner's process to admit it, on a thread of its
 * own. A consumer takes each message no other consumer took, in no promised order, then FIPC_DISCONNECTED once the
 * queue has ended; a producer's sends fail with FIPC_DISCONNECTED from then on.
 */
export declare function joinQueue(name: string, role: QueueRole, timeout?: Timeout): Promise<Connection>;
/** joinQueue() on the calling thread: the owner's process admits it, so the owner may be on this thread. */
export declare function joinQueueSync(name: string, role: QueueRole, timeout?: Timeout): Connection;

/**
 * A service's owner: claims `name` for a service of up to `maxClients` clients and `maxServers` servers (1 to 64
 * each), each client with a ring of `capacity` bytes (a power of two, 4 KiB to 2 GiB) for its requests and their
 * replies, each of 1 to `maxMessage` bytes (1 to a quarter of the capacity). A request holds its length rounded up to
 * 64 bytes, plus maxMessage rounded up to 64, of its client's ring from its send until its reply has been received.
 * Doesn't wait: this process admits the members by itself; the owner joins with {@link joinService} too. A service
 * lives until close() or the end of its process (of its worker thread, for one a Worker created); dropping the object
 * doesn't end it. Fails with FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
export declare function createService(
  name: string,
  capacity: number,
  maxMessage: number,
  maxClients: number,
  maxServers: number,
): Service;
/**
 * A client or a server: joins the service on `name` as {@link CLIENT} or {@link SERVER}, waiting for the owner to
 * create the service, for a free place of the role and for the owner's process to admit it, on a thread of its own.
 * A client's send is a request and its receive the reply to its oldest request whose reply it hasn't received yet, or
 * FIPC_ABANDONED if the server that took it ended before it replied; {@link Connection.call} does both. A server's
 * receive takes the next request of any client and holds it, and its send replies to it, never waiting. Once the
 * service has ended, clients' sends and servers' receives fail with FIPC_DISCONNECTED, and a client receives the
 * replies that came before the end, then FIPC_DISCONNECTED.
 */
export declare function joinService(name: string, role: ServiceRole, timeout?: Timeout): Promise<Connection>;
/** joinService() on the calling thread: the owner's process admits it, so the owner may be on this thread. */
export declare function joinServiceSync(name: string, role: ServiceRole, timeout?: Timeout): Connection;

/** A stream's server, from {@link createStream}: it accepts one client at a time. */
export declare class Listener {
  private constructor();
  /** The name it listens on. */
  readonly name: string;
  /** Whether close() has been called. */
  readonly closed: boolean;
  /**
   * Waits for a client and sets its connection up, on a thread of the listener's own. One client at a time:
   * FIPC_INVALID while the connection it returned last is open.
   */
  accept(timeout?: Timeout): Promise<Connection>;
  /** accept() on the calling thread: a timeout of 0 polls (a client then takes one or two calls). */
  acceptSync(timeout?: Timeout): Connection;
  /** Makes every accept that waits, now or later, fail with FIPC_CANCELLED. Final. */
  cancel(): void;
  /** Stops listening and frees the name; a pending accept fails with FIPC_CANCELLED. Its connections stay. */
  close(): void;
  [Symbol.dispose](): void;
}

/**
 * A work queue, held by its owner ({@link createQueue}). Its producers and consumers, the owner's own included, join
 * with {@link joinQueue}. A queue lives until close() or the end of its process (of its worker thread, for one a
 * Worker created); dropping the object doesn't end it: the garbage collector never closes a queue.
 */
export declare class Queue {
  private constructor();
  /** The queue's name. */
  readonly name: string;
  /** Whether close() has been called. */
  readonly closed: boolean;
  /**
   * The number of members of `role` ({@link PRODUCER} or {@link CONSUMER}; `undefined` or 0 counts both) joined, once
   * at least `atLeast` (0 by default: at once) are, waiting on a thread of the queue's own. At FIPC_TIMEOUT,
   * FIPC_CANCELLED and FIPC_DISCONNECTED (the queue has ended) the error's `count` is the count. It may run beside any
   * call; an async count that must wait queues behind one already waiting, though, so a short timeout can expire late.
   */
  waitForMembers(role?: QueueRole | 0, atLeast?: number, timeout?: Timeout): Promise<number>;
  /** waitForMembers() on the calling thread: members join without it, so it may wait for a join of this thread's. */
  waitForMembersSync(role?: QueueRole | 0, atLeast?: number, timeout?: Timeout): number;
  /** Makes every waitForMembers that waits, now or later, fail with FIPC_CANCELLED; the queue goes on. Final. */
  cancel(): void;
  /**
   * Ends the queue and frees the name: producers' sends fail with FIPC_DISCONNECTED; consumers receive what is left in
   * it, then FIPC_DISCONNECTED. The members' connections stay open until their close. Closing it again does nothing.
   */
  close(): void;
  [Symbol.dispose](): void;
}

/**
 * A service, held by its owner ({@link createService}). Its clients and servers, the owner's own included, join with
 * {@link joinService}. A service lives until close() or the end of its process (of its worker thread, for one a
 * Worker created); dropping the object doesn't end it: the garbage collector never closes a service.
 */
export declare class Service {
  private constructor();
  /** The service's name. */
  readonly name: string;
  /** Whether close() has been called. */
  readonly closed: boolean;
  /**
   * The number of members of `role` ({@link CLIENT} or {@link SERVER}; `undefined` or 0 counts both) joined, once at
   * least `atLeast` (0 by default: at once) are, waiting on a thread of the service's own. At FIPC_TIMEOUT,
   * FIPC_CANCELLED and FIPC_DISCONNECTED (the service has ended) the error's `count` is the count. It may run beside
   * any call; an async count that must wait queues behind one already waiting, though, so a short timeout can expire
   * late.
   */
  waitForMembers(role?: ServiceRole | 0, atLeast?: number, timeout?: Timeout): Promise<number>;
  /** waitForMembers() on the calling thread: members join without it, so it may wait for a join of this thread's. */
  waitForMembersSync(role?: ServiceRole | 0, atLeast?: number, timeout?: Timeout): number;
  /** Makes every waitForMembers that waits, now or later, fail with FIPC_CANCELLED; the service goes on. Final. */
  cancel(): void;
  /**
   * Ends the service and frees the name: clients' sends and servers' receives fail with FIPC_DISCONNECTED; a client
   * receives the replies that came before the end, then FIPC_DISCONNECTED, and a server's reply fails with
   * FIPC_DISCONNECTED. The members' connections stay open until their close. Closing it again does nothing.
   */
  close(): void;
  [Symbol.dispose](): void;
}

/**
 * One connection: a stream's (two rings, one per direction), a broadcast's publisher or subscriber, a work queue's
 * producer or consumer, or a service's client or server.
 */
export declare class Connection {
  private constructor();

  /**
   * The longest message the zero-copy calls take: the ring's capacity less 64 bytes (a broadcast's or a queue's: a
   * quarter of its capacity; a service's: its maxMessage).
   */
  readonly maxPiece: number;
  /** Whether close() has been called. */
  readonly closed: boolean;
  /** Whether a call has failed with FIPC_DISCONNECTED: the peer is gone. Final. */
  readonly ended: boolean;

  /**
   * Sends one message of any size, at least 1 byte. Don't change its bytes, or resize or transfer their ArrayBuffer,
   * until the promise settles. On a service's client: a request; on a server: the reply to the request it holds,
   * which never waits (FIPC_INVALID with none held).
   */
  send(data: Data, timeout?: Timeout): Promise<void>;
  sendSync(data: Data, timeout?: Timeout): void;

  /**
   * Receives one message of any size, as a new Buffer. A lossy subscriber that fell a lap behind fails with
   * FIPC_OVERRUN instead, the number of messages lost in the error's `lost`. A work queue's consumer receives the next
   * message no other consumer took. A service's client receives the reply to its oldest request whose reply it hasn't
   * received yet, or FIPC_ABANDONED (the request counts as answered): after a call that gave up, that call's late
   * reply first. A server receives the next request of any client, which it holds until its reply (FIPC_INVALID while
   * it holds one).
   */
  receive(timeout?: Timeout): Promise<Buffer>;
  receiveSync(timeout?: Timeout): Buffer;

  /**
   * Receives one message into `buffer` and resolves to its length. A longer message stays queued: FIPC_TOO_LARGE, with
   * its length in the error's `length` (on a work queue, another consumer may take it first). Don't use the buffer,
   * or resize or transfer its ArrayBuffer, until the promise settles.
   */
  receiveInto(buffer: ReceiveBuffer, timeout?: Timeout): Promise<number>;
  receiveIntoSync(buffer: ReceiveBuffer, timeout?: Timeout): number;

  /**
   * Sends `messages` (at least one, each of at least 1 byte; copied when the call is made) in order, published
   * together with at most one wake of the receivers, and resolves to how many went: all of them. The timeout bounds
   * the whole call (NO_WAIT: only what fits now). A failure is the first message that didn't go, as send would fail,
   * with how many went in the error's `sent`. Every message is checked before any goes. A service's client sends a
   * batch of requests, owed as send's are; a server's batch is FIPC_INVALID. Batch both ends: a receiver that takes one
   * message per call falls behind a batching sender.
   */
  sendMany(messages: readonly Data[], timeout?: Timeout): Promise<number>;
  sendManySync(messages: readonly Data[], timeout?: Timeout): number;

  /**
   * Waits for the next message, as receive, then takes every further message already there, up to `maxCount` (16 if
   * undefined) and as many as fit 64 KiB (a longer first message comes alone), each a new Buffer, in order. A failure
   * is the first message's, as receive fails. A broadcast's subscriber takes those its publisher has moved past; a
   * service's client receives its replies in its requests' order; FIPC_INVALID on a publisher, a producer and a
   * service's server.
   */
  receiveMany(maxCount?: number, timeout?: Timeout): Promise<Buffer[]>;
  receiveManySync(maxCount?: number, timeout?: Timeout): Buffer[];

  /**
   * Zero-copy send, step 1: room for `length` bytes (1 to maxPiece) in the ring. Write the message there, then
   * sendCommit. The view is detached (its length becomes 0) at the commit, the next send call and close.
   */
  sendAcquire(length: number, timeout?: Timeout): Promise<Uint8Array>;
  sendAcquireSync(length: number, timeout?: Timeout): Uint8Array;
  /**
   * Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message. Doesn't wait. On a
   * service's server it replies, and detaches the view of the request held in place once the reply gave it up.
   */
  sendCommit(length: number): void;

  /**
   * Zero-copy receive, step 1: the next message, in the ring (read it, don't write to it). The view is detached (its
   * length becomes 0) at receiveRelease, the next receive call and close. A message of several pieces stays queued:
   * FIPC_TOO_LARGE, with its length. FIPC_INVALID on a broadcast's subscriber, which receives copies. A service's
   * server's request view lasts until a reply (its send or sendCommit) gives the request up, or close.
   */
  receiveAcquire(timeout?: Timeout): Promise<Uint8Array>;
  receiveAcquireSync(timeout?: Timeout): Uint8Array;
  /** Zero-copy receive, step 2: frees the acquired message's room. On a service's server it does nothing. */
  receiveRelease(): void;

  /**
   * A service's client: sends `request` and resolves to its reply, waiting up to `timeout` ms in all; FIPC_ABANDONED if
   * the server that took it ended before it replied. A call checks its request first: FIPC_INVALID for an empty one,
   * FIPC_TOO_LARGE over maxPiece, a TypeError for another type, at once, even while a reply is owed; and its timeout (a
   * TypeError for one that is neither a number nor undefined, or is NaN), before it waits for its turn. Then a call on
   * a cancelled connection fails with FIPC_CANCELLED at once, whatever its timeout, and sends nothing: it could never
   * receive the reply. A call never resolves to another request's reply: one whose receive timed out or was cancelled
   * leaves its reply owed, and the next call first receives and drops that reply (at most one is owed, since every call
   * drops it first; an FIPC_ABANDONED in its place counts too), within its own timeout and before it sends. A call that
   * had to drop an owed reply or wait for its turn first sends its request only if its timeout has time left: once a
   * positive timeout has run out, it fails with FIPC_TIMEOUT without sending, so no server sees that request; with
   * NO_WAIT no step waits, and it sends if there is room. After a call that gave up, a plain receive returns that
   * call's late reply (or FIPC_ABANDONED) first: replies come in request order. FIPC_INVALID while a request sent with
   * send (or sendCommit) still has its reply to be received, and on any other connection. Calls run one at a time, in
   * the order they were made: a call made while another is pending waits for its turn within its own timeout, and fails
   * with FIPC_TIMEOUT when that runs out first (FIPC_CANCELLED at a cancel or a close), the calls behind it keeping
   * their order. While one is pending, the client's other sends and receives fail with ERR_FIPC_BUSY, and so does a
   * call while an async receive of this client is pending.
   */
  call(request: Data, timeout?: Timeout): Promise<Buffer>;
  /** call() on the calling thread: ERR_FIPC_BUSY while an async call or receive of this client is pending. */
  callSync(request: Data, timeout?: Timeout): Buffer;

  /**
   * A broadcast's publisher: the number of subscribers joined, once at least `atLeast` (0 by default: at once) are.
   * Wait for the subscribers a send must reach before the first send. At FIPC_TIMEOUT and FIPC_CANCELLED the error's
   * `count` is the count; FIPC_INVALID on any other connection. It may run beside any call of the connection; an async
   * count that must wait queues behind one already waiting, though, so a short timeout can expire late.
   */
  waitForMembers(atLeast?: number, timeout?: Timeout): Promise<number>;
  /** waitForMembers() on the calling thread: subscribers join without it, so it may wait for this thread's join. */
  waitForMembersSync(atLeast?: number, timeout?: Timeout): number;

  /**
   * Makes every call that waits, now or later, fail with FIPC_CANCELLED; calls that needn't wait still work. Final. A
   * service client's call, which waits for its reply, fails at once from then on, sending nothing.
   */
  cancel(): void;
  /** Ends the connection; pending async calls fail with FIPC_CANCELLED. Closing it again does nothing. */
  close(): void;
  [Symbol.dispose](): void;
}
