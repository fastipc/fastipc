/*
 * fipc.h: the whole public API of FastIPC.
 *
 * FastIPC connects two processes of the same user on one machine through shared memory. A connection is two rings,
 * one per direction, each written by one side and read by the other; messages never leave shared memory. A server
 * listens on a name and accepts one client at a time; a client connects to the name. A local socket (Linux: abstract
 * AF_UNIX; macOS: a socket file in the user's private directory; Windows: a named pipe) sets each connection up, on the
 * threads that call fipc_accept and fipc_connect, and reports the end of the peer.
 *
 * Results
 *   Every call that can fail returns an fipc_result_t; fipc_result_str() names one. A NULL handle or a NULL required
 *   pointer is FIPC_INVALID.
 *
 * Timeouts
 *   int milliseconds, always a call's last parameter: 0 (FIPC_NO_WAIT) doesn't wait, and a negative value
 *   (FIPC_FOREVER) waits for ever. A call that would have to wait longer returns FIPC_TIMEOUT, having changed nothing.
 *
 * Names and capacity
 *   A name is 1-245 characters of [A-Za-z0-9_.-], not starting with '.' or '-'. It is local to the user (Linux, macOS)
 *   or to the desktop session (Windows). The capacity is the size of each ring in bytes, a power of two from 1024 to
 *   2^31: the server chooses it (fipc_listen), and a client's connection has the server's.
 *
 * Messages
 *   A plain message holds at least 1 byte; an RPC payload may be empty. Messages arrive in the order they were sent.
 *   The copying calls (fipc_send, fipc_recv, fipc_rpc_*) take messages of any size: a message longer than one piece
 *   (fipc_max_piece bytes) travels in pieces and arrives whole, and the library allocates nothing for it (a receive
 *   copies each piece straight into the caller's buffer). The zero-copy calls (fipc_send_acquire, fipc_recv_acquire)
 *   take messages of one piece.
 *
 * A message in pieces
 *   A copying call's timeout bounds its wait for the first piece (a send: for room for it). Once a piece has moved,
 *   the call goes on until the whole message has, however long that takes (past its timeout: a receive waits for a
 *   sender that is slow to send the rest, a send for a receiver slow to make room). Only fipc_cancel or the peer's end
 *   stops it, and the message is then dropped whole: the receiver never sees part of a message. A receive stopped in
 *   the middle returns FIPC_CANCELLED or FIPC_DISCONNECTED, and the next receive skips the message's other pieces. A
 *   send stopped in the middle returns FIPC_CANCELLED or FIPC_DISCONNECTED; its receiver drops the message when the
 *   sender's next message begins (a send that needn't wait still works after fipc_cancel), or reports
 *   FIPC_DISCONNECTED once the sender closes. The contents of the caller's buffer after a receive that didn't return
 *   FIPC_OK are unspecified.
 *
 * Threads
 *   On a connection, one thread at a time sends (fipc_send, fipc_send_acquire/_commit, fipc_rpc_submit,
 *   fipc_rpc_respond) and one thread at a time receives (fipc_recv, fipc_recv_acquire/_release, fipc_rpc_recv); the
 *   two may be different threads, running at once. On a listener, one thread at a time accepts. A client and its
 *   server run on different threads (of one process or two): fipc_connect waits for the server's fipc_accept.
 *   fipc_cancel, fipc_listener_cancel and fipc_max_piece may be called from any thread, at any time before the handle
 *   is closed. fipc_close and fipc_listener_close need the handle to themselves: no other thread may be inside any call
 *   on it, the cancels included. To stop a thread that waits: cancel, join the thread, then close. Separate handles are
 *   independent, a listener and the connections it accepted included.
 *
 * The peer's end: FIPC_DISCONNECTED
 *   The peer closed its connection, or its process ended, for any reason. There is no heartbeat and no timeout: a
 *   paused or hung peer is not reported, and a crashed one only once its process has ended (a crash handler or a
 *   debugger that holds the process delays it). Sends return FIPC_DISCONNECTED at once; receives return it only after
 *   they have delivered every message the peer completed. It is final: close the connection; to talk again, accept or
 *   connect a new one.
 *
 * fork (Linux, macOS)
 *   A child inherits no usable handle: its calls on inherited handles return FIPC_INVALID, except the closes, which
 *   free the child's copy without touching the parent's connection. The child's own new handles work normally.
 *
 * Unloading the library
 *   Never on Linux or macOS (their fork handlers stay registered); on Windows, only once every listener and connection
 *   is closed.
 */

#ifndef FIPC_H
#define FIPC_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32) && defined(FASTIPC_SHARED)
#define FIPC_API __declspec(dllimport)
#else
#define FIPC_API
#endif

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct fipc_listener fipc_listener_t; /* a name a server listens on */
typedef struct fipc_conn fipc_conn_t;         /* one connection: two rings */

typedef enum
{
    FIPC_OK = 0,
    FIPC_TIMEOUT = 1,      /* the timeout ran out (with timeout 0: nothing to do without waiting) */
    FIPC_DISCONNECTED = 2, /* the peer ended the connection (see above) */
    FIPC_CANCELLED = 3,    /* fipc_cancel / fipc_listener_cancel was called, and the call would have to wait */
    FIPC_TOO_LARGE = 4,    /* the message doesn't fit what the call can take: the caller's buffer, or one piece for
                              zero-copy. A receive reports the message's length and leaves the message queued. */
    FIPC_INVALID = 5,      /* a bad argument or handle, a call out of order, a peer with another version or user,
                              corrupt ring content */
    FIPC_NO_MEMORY = 6,    /* memory or another OS resource ran out (setting a connection up) */
    FIPC_ADDR_IN_USE = 7,  /* fipc_listen: another listener holds the name */
} fipc_result_t;

#define FIPC_NO_WAIT 0
#define FIPC_FOREVER (-1)

/* A static string naming `result` ("FIPC_OK", ...); "FIPC_UNKNOWN" for any other value. */
FIPC_API const char* fipc_result_str(fipc_result_t result);

/* === Connections === */

/*
 * Server: claims `name` and listens on it, with rings of `capacity` bytes for each connection. Doesn't wait: clients
 * may connect from now on, and fipc_accept sets each one up. FIPC_ADDR_IN_USE if another listener holds the name (it
 * is freed when that listener is closed or its process ends; on Windows, once the connections it accepted are closed
 * too).
 */
FIPC_API fipc_result_t fipc_listen(const char* name, size_t capacity, fipc_listener_t** out_listener);

/*
 * Server: waits up to timeout_ms for a client, sets its connection up (a new pair of rings) on the calling thread, and
 * returns it. A call that times out in the middle of a client's setup keeps it for the next call, so a timeout of 0
 * polls: a client then takes one or two calls. The connection may already hold the client's first messages.
 * One client at a time: FIPC_INVALID while the connection this listener returned last is open (close it first); a
 * client that connects meanwhile waits, within its own timeout. A client whose version differs, or which runs as
 * another user, is refused, and the call goes on waiting. FIPC_CANCELLED after fipc_listener_cancel.
 * FIPC_NO_MEMORY once the listener couldn't get the memory for a client's rings: the listener has failed for good,
 * and every later call returns it (close the listener, and listen again to go on).
 */
FIPC_API fipc_result_t fipc_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);

/*
 * Makes every fipc_accept of the listener that waits, now or later, return FIPC_CANCELLED; a client whose setup a
 * fipc_accept had begun is dropped (it connects again, within its own timeout, or gets FIPC_DISCONNECTED if its
 * fipc_connect returned). A client that connects afterwards waits, within its own timeout. Any thread. Final.
 */
FIPC_API void fipc_listener_cancel(fipc_listener_t* listener);

/*
 * Stops listening and frees the listener. The name is free again at once; on Windows, once the connections the
 * listener accepted are closed too. Those connections stay open. A client whose setup a fipc_accept began and didn't
 * finish is dropped (if its fipc_connect returned, it gets FIPC_DISCONNECTED). No other thread may be inside a call on
 * the listener. NULL is a no-op.
 */
FIPC_API void fipc_listener_close(fipc_listener_t* listener);

/*
 * Client: connects to the server listening on `name` and sets the connection up with it, on the calling thread,
 * waiting up to timeout_ms for a server to listen (it may start later, or be serving another client) and to call
 * fipc_accept. It returns once the server's fipc_accept has offered the rings, which may be before that fipc_accept
 * returns: messages sent meanwhile wait in the ring. The client and the server must run on different threads (of one
 * process or two). The connection's rings have the capacity the server chose. FIPC_INVALID if the server's version
 * differs, or it runs as another user; FIPC_NO_MEMORY if the rings can't be mapped. There is no handle to cancel until
 * it returns: use FIPC_FOREVER to wait for a server however late it starts, or a finite timeout to stay responsive.
 */
FIPC_API fipc_result_t fipc_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);

/*
 * The longest message the zero-copy calls take on the connection: its capacity less 64 bytes, the framing's share.
 * 0 for NULL. Any thread.
 */
FIPC_API size_t fipc_max_piece(const fipc_conn_t* conn);

/*
 * Makes every call of the connection that waits, now or later, return FIPC_CANCELLED; calls that needn't wait still
 * work. A message cancelled halfway is dropped whole. The connection stays up: the peer sees nothing until
 * fipc_close. Any thread, any time before fipc_close. Final.
 */
FIPC_API void fipc_cancel(fipc_conn_t* conn);

/*
 * Ends the connection (the peer gets FIPC_DISCONNECTED once it has received the messages this side completed),
 * unmaps its rings and frees the handle; every pointer the connection returned becomes invalid. No other thread may
 * be inside a call on the connection: fipc_cancel, join the threads that use it, then fipc_close. NULL is a no-op.
 */
FIPC_API void fipc_close(fipc_conn_t* conn);

/* === Messages === */

/*
 * Sends one message of `len` bytes (at least 1, any size): waits up to timeout_ms for room for its first piece, then
 * copies it in, in pieces if it is longer than one. Drops a zero-copy reservation that was never committed.
 */
FIPC_API fipc_result_t fipc_send(fipc_conn_t* conn, const void* data, size_t len, int timeout_ms);

/*
 * Receives one message of any size into `buf`: waits up to timeout_ms for its first piece, sets *out_len to its
 * length, copies it out and frees its room. FIPC_TOO_LARGE if buf_len < *out_len: nothing is taken, so allocate
 * *out_len bytes and call again (buf may be NULL when buf_len is 0, which asks for the next message's length only).
 */
FIPC_API fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);

/*
 * Zero-copy send, step 1: waits up to timeout_ms for `len` (at least 1) contiguous bytes in the ring and sets
 * *out_buf to them (16-byte aligned). Write the message there, then fipc_send_commit. The next send or acquire drops
 * a reservation that was never committed. FIPC_TOO_LARGE over fipc_max_piece(conn): send it with fipc_send.
 * The room is contiguous: a reservation that doesn't fit before the ring's end waits until the receiver has read past
 * the end, even in an empty ring, so a long one (near fipc_max_piece) can wait for the peer's next receive call.
 */
FIPC_API fipc_result_t fipc_send_acquire(fipc_conn_t* conn, size_t len, void** out_buf, int timeout_ms);

/*
 * Zero-copy send, step 2: publishes the first `len` bytes (1 to the acquired length) as one message. Doesn't wait.
 * FIPC_INVALID without a reservation, or for a length out of that range (the reservation stays for a valid one).
 */
FIPC_API fipc_result_t fipc_send_commit(fipc_conn_t* conn, size_t len);

/*
 * Zero-copy receive, step 1: waits up to timeout_ms for a message and sets *out_data (16-byte aligned; in the ring,
 * read-only) and *out_len. The bytes stay valid, and their room stays taken, until fipc_recv_release, the next
 * receive call of any kind (which releases them first) or fipc_close. FIPC_TOO_LARGE for a message in several
 * pieces: *out_len is its length, and it stays for fipc_recv.
 */
FIPC_API fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);

/* Zero-copy receive, step 2: frees the acquired message's room. Without one, a no-op. Doesn't wait. */
FIPC_API void fipc_recv_release(fipc_conn_t* conn);

/* === RPC: request and response messages over a connection === */

/*
 * Either side may submit requests and respond to the requests it receives; correlate responses by `id`. An RPC
 * message is a plain message with a 32-byte header in front: use a connection for one or the other. fipc_rpc_recv
 * drops a message that isn't a well-formed RPC message and returns FIPC_INVALID.
 */

#define FIPC_RPC_REQUEST 1
#define FIPC_RPC_RESPONSE 2

typedef struct
{
    uint64_t id;       /* the request's id, echoed in its response */
    uint32_t kind;     /* FIPC_RPC_REQUEST or FIPC_RPC_RESPONSE */
    uint32_t opcode;   /* the application's */
    int32_t status;    /* the application's; 0 in requests */
    uint32_t reserved; /* 0 */
    uint64_t len;      /* the payload's length */
} fipc_rpc_msg_t;

/*
 * Sends a request with a payload of any size, possibly empty (as fipc_send), and sets *out_id to its id: a
 * connection numbers its requests from 1.
 */
FIPC_API fipc_result_t
fipc_rpc_submit(fipc_conn_t* conn, uint32_t opcode, const void* data, size_t len, uint64_t* out_id, int timeout_ms);

/* Sends the response to request `id` (as fipc_send; the payload may be empty). */
FIPC_API fipc_result_t fipc_rpc_respond(
    fipc_conn_t* conn, uint64_t id, uint32_t opcode, int32_t status, const void* data, size_t len, int timeout_ms
);

/*
 * Receives one request or response (as fipc_recv): fills *msg and copies the payload into `buf`. FIPC_TOO_LARGE if
 * buf_len < msg->len: *msg is filled, nothing is taken, so allocate msg->len bytes and call again (buf may be NULL
 * when buf_len is 0).
 */
FIPC_API fipc_result_t fipc_rpc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, fipc_rpc_msg_t* msg, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* FIPC_H */
