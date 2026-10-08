/**
 * FastIPC for JavaScript: messages and RPC between two processes on one machine, through shared memory, in Node.js,
 * Bun and Deno.
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
/** An RPC message's kind: a request. */
export declare const RPC_REQUEST: 1;
/** An RPC message's kind: a response. */
export declare const RPC_RESPONSE: 2;

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
}>;

/** A result's name, as a FipcError's `code`. */
export type ResultCode =
  | 'FIPC_TIMEOUT'
  | 'FIPC_DISCONNECTED'
  | 'FIPC_CANCELLED'
  | 'FIPC_TOO_LARGE'
  | 'FIPC_INVALID'
  | 'FIPC_NO_MEMORY'
  | 'FIPC_ADDR_IN_USE';

/** The library's name for a result: 'FIPC_OK', 'FIPC_TIMEOUT', ...; 'FIPC_UNKNOWN' for any other value. */
export declare function resultStr(result: number): string;

/** A call returned a result other than OK. */
export declare class FipcError extends Error {
  /** 'FipcError' */
  readonly name: 'FipcError';
  /** The result's name: 'FIPC_TIMEOUT', 'FIPC_DISCONNECTED', ... */
  readonly code: ResultCode;
  /** The result's value ({@link Result}). */
  readonly result: number;
  /** The method that failed ('receive', 'acceptSync', ...). */
  readonly call: string;
  /** FIPC_TOO_LARGE of a receive: the message's (or the RPC payload's) length; the message stays queued. */
  readonly length?: number;
}

/** A received request or response. */
export interface RpcMessage {
  /** The request's id, echoed in its response. */
  id: number;
  /** {@link RPC_REQUEST} or {@link RPC_RESPONSE}. */
  kind: number;
  /** The application's. */
  opcode: number;
  /** The application's; 0 in requests. */
  status: number;
  payload: Buffer;
}

/** A received request or response whose payload is in the caller's buffer. */
export interface RpcHeader {
  id: number;
  kind: number;
  opcode: number;
  status: number;
  /** The payload's length. */
  length: number;
}

/** A server's name: it accepts one client at a time. */
export declare class Listener {
  private constructor();
  /**
   * Claims `name` (1-245 characters of [A-Za-z0-9_.-], not starting with '.' or '-') and listens on it, with rings of
   * `capacity` bytes each way (a power of two, 1 KiB to 2 GiB). Doesn't wait. Fails with FIPC_ADDR_IN_USE if another
   * listener holds the name.
   */
  static listen(name: string, capacity: number): Listener;
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
  /** Stops listening; a pending accept fails with FIPC_CANCELLED. The connections it accepted stay open. */
  close(): void;
  [Symbol.dispose](): void;
}

/** One connection: two rings, one per direction. */
export declare class Connection {
  private constructor();
  /**
   * Connects to the server listening on `name`, waiting for it to listen and to accept, on a thread of its own.
   */
  static connect(name: string, timeout?: Timeout): Promise<Connection>;
  /** connect() on the calling thread: the server must not wait on this thread meanwhile. */
  static connectSync(name: string, timeout?: Timeout): Connection;

  /** The longest message the zero-copy calls take: the ring's capacity less 64 bytes. */
  readonly maxPiece: number;
  /** Whether close() has been called. */
  readonly closed: boolean;
  /** Whether a call has failed with FIPC_DISCONNECTED: the peer is gone. Final. */
  readonly ended: boolean;

  /**
   * Sends one message of any size, at least 1 byte. Don't change its bytes, or resize or transfer their ArrayBuffer,
   * until the promise settles.
   */
  send(data: Data, timeout?: Timeout): Promise<void>;
  sendSync(data: Data, timeout?: Timeout): void;

  /** Receives one message of any size, as a new Buffer. */
  receive(timeout?: Timeout): Promise<Buffer>;
  receiveSync(timeout?: Timeout): Buffer;

  /**
   * Receives one message into `buffer` and resolves to its length. A longer message stays queued: FIPC_TOO_LARGE, with
   * its length in the error's `length`. Don't use the buffer, or resize or transfer its ArrayBuffer, until the promise
   * settles.
   */
  receiveInto(buffer: ReceiveBuffer, timeout?: Timeout): Promise<number>;
  receiveIntoSync(buffer: ReceiveBuffer, timeout?: Timeout): number;

  /**
   * Zero-copy send, step 1: room for `length` bytes (1 to maxPiece) in the ring. Write the message there, then
   * sendCommit. The view is detached (its length becomes 0) at the commit, the next send call and close.
   */
  sendAcquire(length: number, timeout?: Timeout): Promise<Uint8Array>;
  sendAcquireSync(length: number, timeout?: Timeout): Uint8Array;
  /** Zero-copy send, step 2: sends the first `length` bytes of the acquired room as one message. Doesn't wait. */
  sendCommit(length: number): void;

  /**
   * Zero-copy receive, step 1: the next message, in the ring (read it, don't write to it). The view is detached (its
   * length becomes 0) at receiveRelease, the next receive call and close. A message of several pieces stays queued:
   * FIPC_TOO_LARGE, with its length.
   */
  receiveAcquire(timeout?: Timeout): Promise<Uint8Array>;
  receiveAcquireSync(timeout?: Timeout): Uint8Array;
  /** Zero-copy receive, step 2: frees the acquired message's room. */
  receiveRelease(): void;

  /** Sends a request with a payload of any size, possibly empty; resolves to its id (from 1, increasing). */
  rpcSubmit(opcode: number, data?: Data, timeout?: Timeout): Promise<number>;
  rpcSubmitSync(opcode: number, data?: Data, timeout?: Timeout): number;
  /** Sends the response to request `id`, with the application's `status` (an int32). */
  rpcRespond(id: number | bigint, opcode: number, status: number, data?: Data, timeout?: Timeout): Promise<void>;
  rpcRespondSync(id: number | bigint, opcode: number, status: number, data?: Data, timeout?: Timeout): void;
  /** Receives one request or response. */
  rpcReceive(timeout?: Timeout): Promise<RpcMessage>;
  rpcReceiveSync(timeout?: Timeout): RpcMessage;
  /** Receives one request or response with its payload in `buffer`. A longer payload stays queued: FIPC_TOO_LARGE. */
  rpcReceiveInto(buffer: ReceiveBuffer, timeout?: Timeout): Promise<RpcHeader>;
  rpcReceiveIntoSync(buffer: ReceiveBuffer, timeout?: Timeout): RpcHeader;

  /** Makes every call that waits, now or later, fail with FIPC_CANCELLED; calls that needn't wait still work. Final. */
  cancel(): void;
  /** Ends the connection; pending async calls fail with FIPC_CANCELLED. Closing it again does nothing. */
  close(): void;
  [Symbol.dispose](): void;
}
