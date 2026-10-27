/*
 * fipc.h: the whole public API of FastIPC.
 *
 * FastIPC moves messages between processes of the same user on one machine through shared memory; messages never
 * leave it. A name holds one of four kinds:
 *   stream     one connection between two processes: two rings, one per direction, each written by one side and read
 *              by the other. A server creates the name and accepts one client at a time; a client connects to it.
 *   broadcast  one publisher, many subscribers: every message goes to every subscriber. See its section.
 *   queue      many producers, many consumers: each message goes to one consumer. See its section.
 *   service    many clients, a pool of servers: each request to one server, its reply back to its client. See its
 *              section.
 * A local socket (Linux: abstract AF_UNIX; macOS: a socket file in the user's private directory; Windows: a named
 * pipe) sets each connection up and reports the end of the peer: a stream's on the threads that call
 * fipc_stream_accept and fipc_stream_connect; a broadcast's, a queue's or a service's on the joiner's thread and, in
 * the owner's process, on a thread of the library's.
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
 *   or to the desktop session (Windows), and one stream, broadcast, queue or service holds it at a time. The capacity
 *   is the size of each ring in bytes, a power of two: the name's creator chooses it (fipc_stream_create,
 *   fipc_broadcast_create, fipc_queue_create, fipc_service_create), and every connection to the name has it.
 *
 * Messages
 *   A message holds at least 1 byte. A stream's and a broadcast's messages arrive in the order they were sent; a queue
 *   promises no order; a service's replies arrive in the order of their requests. On a stream the copying calls
 *   (fipc_send, fipc_recv and their batches, fipc_send_many, fipc_recv_many) take messages of any size (a broadcast's,
 *   a queue's and a service's are of one piece: see their sections): a message longer than one piece (fipc_max_piece
 *   bytes) travels in pieces and arrives whole, and the library allocates nothing for it (a receive copies each piece
 *   straight into the caller's buffer). The zero-copy calls (fipc_send_acquire, fipc_recv_acquire) take messages of one
 *   piece.
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
 *   On a connection, one thread at a time sends (fipc_send, fipc_send_many, fipc_send_acquire/_commit) and one thread
 *   at a time receives (fipc_recv, fipc_recv_many, fipc_recv_acquire/_release); the two may be different threads,
 *   running at once. On a stream's listener, one thread at a time accepts. A stream's client and its server run on
 *   different threads (of one process or two): fipc_stream_connect waits for the server's fipc_stream_accept. A
 *   broadcast's, a queue's or a service's owner needs no thread for its joiners: its process admits them on a thread of
 *   the library's, so a joiner may run on any thread, the owner's included. fipc_cancel, fipc_listener_cancel,
 *   fipc_queue_cancel, fipc_service_cancel, fipc_max_piece, fipc_broadcast_wait_for_members,
 *   fipc_queue_wait_for_members and fipc_service_wait_for_members may be called from any thread, at any time before the
 *   handle is closed. fipc_close, fipc_listener_close, fipc_queue_close and fipc_service_close need the handle to
 *   themselves: no other thread may be inside any call on it, the cancels included. To stop a thread that waits:
 *   cancel, join the thread, then close. Separate handles are independent, a listener and the connections it accepted
 *   included.
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
 *   free the child's copy without touching the parent's connection, listener, queue or service. The child's own new
 *   handles work normally.
 *
 * Unloading the library
 *   Never on Linux or macOS (their fork handlers stay registered); on Windows, only once every handle is closed.
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

typedef struct fipc_listener fipc_listener_t; /* a stream's name, on which its server accepts clients */
typedef struct fipc_conn fipc_conn_t;         /* one connection: a stream's, a publisher, or a member */
typedef struct fipc_queue fipc_queue_t;       /* a work queue, held by its owner */
typedef struct fipc_service fipc_service_t;   /* a service, held by its owner */

typedef enum
{
    FIPC_OK = 0,
    FIPC_TIMEOUT = 1,      /* the timeout ran out (with timeout 0: nothing to do without waiting) */
    FIPC_DISCONNECTED = 2, /* the peer ended the connection (see above) */
    FIPC_CANCELLED = 3,    /* fipc_cancel, fipc_listener_cancel, fipc_queue_cancel or fipc_service_cancel was called,
                              and the call would have to wait */
    FIPC_TOO_LARGE = 4,    /* the message doesn't fit what the call can take: the caller's buffer, or one piece for
                              zero-copy and on a broadcast, a queue or a service. A receive reports the message's
                              length and leaves the message queued. */
    FIPC_INVALID = 5,      /* a bad argument or handle, a call out of order, a peer with another version or user,
                              corrupt ring content */
    FIPC_NO_MEMORY = 6,    /* memory or another OS resource ran out (setting a connection up) */
    FIPC_ADDR_IN_USE = 7,  /* fipc_stream_create, fipc_broadcast_create, fipc_queue_create, fipc_service_create:
                              another stream, broadcast, queue or service holds the name */
    FIPC_OVERRUN = 8,      /* a lossy subscriber fell a lap behind: messages were lost. The receive returns this
                              instead of a message and reports how many were lost (at least 1); the next goes on
                              from a recent message. */
    FIPC_UNSUBSCRIBED = 9, /* a gated subscriber held the publisher back for max_stall_ms and was dropped from the
                              broadcast: every later receive on its connection returns it (close it) */
    FIPC_ABANDONED = 10,   /* a service's client: the server that took the request ended before it replied. The
                              request is answered (the next receive returns the next reply); it may or may not have
                              been carried out. */
} fipc_result_t;

#define FIPC_NO_WAIT 0
#define FIPC_FOREVER (-1)

/* A static string naming `result` ("FIPC_OK", ...); "FIPC_UNKNOWN" for any other value. */
FIPC_API const char* fipc_result_str(fipc_result_t result);

/* === Streams: a server, and one client at a time === */

/*
 * Server: claims `name` for a stream and listens on it, with rings of `capacity` bytes (a power of two, 1024 to 2^31)
 * for each connection. Doesn't wait: clients may connect from now on, and fipc_stream_accept sets each one up.
 * FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name (it is freed when that one's handle
 * is closed or its process ends; a stream's, on Windows, once the connections its listener accepted are closed too).
 */
FIPC_API fipc_result_t fipc_stream_create(const char* name, size_t capacity, fipc_listener_t** out_listener);

/*
 * Server: waits up to timeout_ms for a client, sets its connection up (a new pair of rings) on the calling thread, and
 * returns it. A call that times out in the middle of a client's setup keeps it for the next call, so a timeout of 0
 * polls: a client then takes one or two calls. The connection may already hold the client's first messages.
 * One client at a time: FIPC_INVALID while the connection this listener returned last is open (close it first); a
 * client that connects meanwhile waits, within its own timeout. A client whose version differs, or which runs as
 * another user, is refused, and the call goes on waiting. FIPC_CANCELLED after fipc_listener_cancel.
 * FIPC_NO_MEMORY once the listener couldn't get the memory for a client's rings: the listener has failed for good,
 * and every later call returns it (close the listener, and create the stream again to go on).
 */
FIPC_API fipc_result_t fipc_stream_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);

/*
 * Makes every fipc_stream_accept of the listener that waits, now or later, return FIPC_CANCELLED; a client whose setup
 * had begun is dropped (it connects again, within its own timeout; a client whose fipc_stream_connect returned gets
 * FIPC_DISCONNECTED). Clients that come afterwards wait, within their own timeouts. Any thread. Final.
 */
FIPC_API void fipc_listener_cancel(fipc_listener_t* listener);

/*
 * Stops listening and frees the listener. The name is free again at once; on Windows, once the connections the
 * listener accepted are closed too. Those connections stay open. A client whose setup a fipc_stream_accept began and
 * didn't finish is dropped (if its fipc_stream_connect returned, it gets FIPC_DISCONNECTED). No other thread may be
 * inside a call on the listener. NULL is a no-op.
 */
FIPC_API void fipc_listener_close(fipc_listener_t* listener);

/*
 * Client: connects to the server of the stream on `name` and sets the connection up with it, on the calling thread,
 * waiting up to timeout_ms for a server to create the stream (it may start later, or be serving another client) and to
 * call fipc_stream_accept. It returns once the server's fipc_stream_accept has offered the rings, which may be before
 * that fipc_stream_accept returns: messages sent meanwhile wait in the ring. The client and the server must run on
 * different threads (of one process or two). The connection's rings have the capacity the server chose. FIPC_INVALID
 * if the server's version differs, or it runs as another user, or a broadcast, a queue or a service holds the name;
 * FIPC_NO_MEMORY if the rings can't be mapped. There is no handle to cancel until it returns: use FIPC_FOREVER to
 * wait for a server however late it starts, or a finite timeout to stay responsive.
 */
FIPC_API fipc_result_t fipc_stream_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);

/* === Connections === */

/*
 * The longest message the zero-copy calls take on the connection: its capacity less 64 bytes, the framing's share;
 * on a broadcast or a queue: a quarter of the capacity; on a service: its max_message. 0 for NULL. Any thread.
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
 * FIPC_OVERRUN (a lossy subscriber's): *out_len is the number of messages it lost. On a queue's consumer it
 * acknowledges the message before it returns (see the work queues' delivery). On a service's client: the next reply,
 * or FIPC_ABANDONED; on a server: the next request (see the services).
 */
FIPC_API fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);

/*
 * Sends `count` messages (at least 1) in order, message i being lens[i] bytes (at least 1) at datas[i], and publishes
 * them together, with at most one wake of the receivers for the batch (a broadcast, a queue and a service store their
 * index once; a stream stores its ring's head after each message); before any wait for room it publishes what it wrote.
 * timeout_ms bounds the whole call (FIPC_NO_WAIT: only what fits now). *out_sent is how many went, set on every return:
 * FIPC_OK only once all went; any other result is the first message that didn't go, as fipc_send would return it, and
 * every one before it went. Every message is checked first, before any goes: FIPC_INVALID for an empty one or a NULL
 * datas[i], FIPC_TOO_LARGE over fipc_max_piece on a broadcast, a queue or a service. On a stream a message longer than
 * fipc_max_piece goes in pieces, as fipc_send sends it, after the batch so far is published. It sends on a stream's
 * connection, a publisher, a producer and a service's client (a batch of requests, whose replies come in their order);
 * FIPC_INVALID on a service's server, which replies with fipc_send. A producer whose publish meets the queue's end
 * returns FIPC_DISCONNECTED, and *out_sent leaves out that publish's messages (they may still reach a consumer, as
 * fipc_send's may). Batch both ends: a receiver that takes one message per call falls behind a batching sender, and the
 * two run slower than without batches.
 */
FIPC_API fipc_result_t fipc_send_many(
    fipc_conn_t* conn, const void* const* datas, const size_t* lens, size_t count, size_t* out_sent, int timeout_ms
);

/*
 * Waits up to timeout_ms for the next message, as fipc_recv, then takes every further message already there that fits
 * the rest of `buf`, up to max_count (at least 1), packed back to back: message i is lens[i] bytes, at the sum of the
 * lengths before it. It frees their room together (on a stream, one index store for the call). *out_count is how many
 * it took, set on every return: 1 or more with FIPC_OK, 0 with any other result, which is the first message's, as
 * fipc_recv returns it: FIPC_TOO_LARGE (lens[0] is its length; nothing is taken), FIPC_OVERRUN (lens[0] is the number
 * lost), FIPC_ABANDONED (lens[0] is 0). After the first it never waits, and stops before a message that isn't there
 * yet, that doesn't fit, that is in pieces, or that would bring another result (a lossy subscriber's overwritten one,
 * an abandoned request): the next receive returns it. A broadcast's subscriber takes, after its first, only the
 * messages its publisher has moved past (all but those in the sixteenth of the capacity it writes now), so that it
 * never reads where the publisher writes; the newest come in the next calls. A queue's consumer acknowledges each
 * message before the call returns, as fipc_recv does. FIPC_INVALID on a publisher, a producer and a service's server,
 * which holds one request at a time.
 */
FIPC_API fipc_result_t fipc_recv_many(
    fipc_conn_t* conn, void* buf, size_t buf_len, size_t* lens, size_t max_count, size_t* out_count, int timeout_ms
);

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
 * pieces: *out_len is its length, and it stays for fipc_recv. FIPC_INVALID on a lossy subscriber. On a queue's
 * consumer the message is acknowledged at its release (see the work queues' delivery). On a service's client: the next
 * reply, or FIPC_ABANDONED; on a server: the next request (see the services).
 */
FIPC_API fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);

/*
 * Zero-copy receive, step 2: frees the acquired message's room; on a queue's consumer, acknowledges it (see the work
 * queues' delivery). Without one, a no-op. Doesn't wait.
 */
FIPC_API void fipc_recv_release(fipc_conn_t* conn);

/* === Broadcasts: one publisher, many subscribers === */

/*
 * A broadcast has one publisher and up to 1024 subscribers, in any processes of the user. Every message goes to every
 * subscriber, in one order, and is stored once, in memory every subscriber maps. The publisher creates the name with
 * fipc_broadcast_create, which returns the publisher's connection, the broadcast's one handle. A subscriber joins
 * with fipc_broadcast_join and receives on the connection it returns; the publisher's process admits it on a thread of
 * the library's. A subscriber receives the messages sent from its admission on, which comes before its
 * fipc_broadcast_join returns. A broadcast's messages are of one piece: 1 to fipc_max_piece bytes, a quarter of the
 * capacity.
 *
 * Each subscriber picks its role when it joins:
 *   FIPC_SUBSCRIBER_LOSSY  never holds the publisher back. One that falls a lap behind loses messages: a receive
 *                          returns FIPC_OVERRUN instead of a message and stores in *out_len how many messages it
 *                          skipped (at least 1), and the next goes on from a recent message (a message that a
 *                          FIPC_TOO_LARGE receive left queued can be lost the same way). A busy publisher can lap a
 *                          joiner before its first receive, which then returns FIPC_OVERRUN. It receives with
 *                          fipc_recv, which copies the message out and checks that the publisher didn't overwrite it
 *                          meanwhile; fipc_recv_acquire is FIPC_INVALID.
 *   FIPC_SUBSCRIBER_GATED  misses nothing: a send waits for room the subscriber still holds, from its admission on, so
 *                          max_stall_ms can unsubscribe one slow to make its first receive. It may receive in place
 *                          (fipc_recv_acquire): the message holds the publisher back until fipc_recv_release.
 *
 * The calls on a broadcast's connections:
 *   publisher   fipc_send and fipc_send_acquire/_commit publish to every subscriber (FIPC_TOO_LARGE over one piece;
 *               a send waits only for gated subscribers). Its sends never return FIPC_DISCONNECTED. fipc_recv*:
 *               FIPC_INVALID. fipc_close ends the broadcast and frees the name at once: subscribers get
 *               FIPC_DISCONNECTED once they have received what it published, and a joiner waits, within its own
 *               timeout, for a new broadcast on the name. fipc_broadcast_wait_for_members counts the joined
 *               subscribers, and waits for a number of them: before the first send, for those it must reach.
 *   subscriber  fipc_recv (gated: also fipc_recv_acquire/_release) receive. fipc_send*: FIPC_INVALID.
 *               FIPC_DISCONNECTED once the publisher closed or its process ended, after the messages it published. A
 *               gated subscriber whose publisher's process ended, or whose control connection failed (the publisher
 *               then drops it), receives them with fipc_recv only (copied and checked, as a lossy one does), and
 *               fipc_recv_acquire returns FIPC_DISCONNECTED: nothing protects them in place. fipc_close leaves the
 *               broadcast.
 *   fipc_max_piece and fipc_cancel work on both, as on a stream's connection. One thread at a time sends on the
 *   publisher and receives on a subscriber. fipc_send_many and fipc_recv_many batch the publisher's sends and a
 *   subscriber's receives.
 */

#define FIPC_SUBSCRIBER_LOSSY 1 /* fipc_broadcast_join's role: a lossy subscriber */
#define FIPC_SUBSCRIBER_GATED 2 /* fipc_broadcast_join's role: a gated subscriber */

/*
 * Publisher: claims `name` for a broadcast of `capacity` bytes of messages (a power of two, 4096 to 2^31) and room for
 * `max_subscribers` subscribers (1 to 1024), and returns the publisher's connection. Doesn't wait: subscribers may join
 * from now on. The broadcast holds up to capacity / 64 messages, each in its length rounded up to 64 bytes of the
 * capacity: fewer when longer ones fill the capacity first. A gated subscriber holds the publisher back, and a lossy
 * one is lapped, once that much is unread. Its memory, one segment every subscriber maps, is
 *   round4K(round64K(512 + capacity / 2) + round64K(capacity) + 1024 + 128 * max_subscribers) bytes,
 * where roundN rounds up to a multiple of N (for 4096 bytes and one subscriber 132 KiB; for 1 MiB and 64 subscribers
 * about 1.6 MiB). The publisher's process also keeps capacity / 16 bytes and up to 512 bytes per subscriber of its own,
 * and one thread, which admits the joiners and watches every joined subscriber.
 * A gated subscriber whose position stays still for max_stall_ms from the first send it held back (sends that timed
 * out meanwhile count too) is unsubscribed: its receives return FIPC_UNSUBSCRIBED from then on, the messages it holds
 * in place become inaccessible (reading them faults), and the publisher goes on. A negative max_stall_ms
 * (FIPC_FOREVER) never unsubscribes; 0 (FIPC_NO_WAIT) refuses gated subscribers. A subscriber whose whole process is
 * paused can't be unsubscribed: sends time out until it resumes or ends. A subscriber whose process ended is dropped at
 * once. FIPC_INVALID for a capacity or max_subscribers out of range; FIPC_NO_MEMORY if the memory for the whole
 * broadcast can't be had (it is set up now, not as subscribers join); FIPC_ADDR_IN_USE if another stream, broadcast,
 * queue or service holds the name.
 */
FIPC_API fipc_result_t fipc_broadcast_create(
    const char* name, size_t capacity, uint32_t max_subscribers, int max_stall_ms, fipc_conn_t** out_publisher
);

/*
 * Subscriber: joins the broadcast on `name` as `role` (FIPC_SUBSCRIBER_LOSSY or FIPC_SUBSCRIBER_GATED) and returns its
 * connection, waiting up to timeout_ms for the publisher to create the broadcast and to have a free slot (while
 * max_subscribers are joined, a joiner waits for one to leave), and for the publisher's process to admit it. No thread
 * of the publisher's need wait for it. FIPC_INVALID for another role (the queue and service roles included), a name a
 * queue or a service holds, a publisher of another version or user, or FIPC_SUBSCRIBER_GATED on a broadcast that
 * refuses gated subscribers; FIPC_NO_MEMORY if the broadcast can't be mapped. On a name a stream holds, the join learns
 * the kind when the stream's server accepts it, which brings FIPC_INVALID; while no fipc_stream_accept runs, it waits
 * and returns FIPC_TIMEOUT.
 */
FIPC_API fipc_result_t fipc_broadcast_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);

/*
 * Publisher: waits up to timeout_ms until at least `at_least` subscribers are joined, and stores in *out_count how
 * many are, at every return but FIPC_INVALID. Joined: admitted, and not yet left, dropped or unsubscribed; the
 * subscribers the publisher's sends reach. To send nothing before N subscribers are there, call it with N before the
 * first send. at_least 0 returns the count at once, and a timeout of 0 polls. A subscriber that leaves, or whose
 * process ends, is counted until the publisher's process has seen its end, soon after. FIPC_OK once the count is
 * reached; FIPC_TIMEOUT; FIPC_CANCELLED after fipc_cancel of the publisher; FIPC_INVALID on a connection other than a
 * publisher, or for at_least over the broadcast's max_subscribers. Any thread.
 */
FIPC_API fipc_result_t
fipc_broadcast_wait_for_members(fipc_conn_t* publisher, uint32_t at_least, uint32_t* out_count, int timeout_ms);

/* === Work queues: many producers, many consumers === */

/*
 * A work queue takes messages from up to 63 producers and hands each to one of up to 64 consumers, in any processes
 * of the user. Each producer has a ring of its own in the queue's memory, which every member maps. A consumer takes a
 * message of any producer: the queue promises no order. The owner creates the name with fipc_queue_create, which
 * returns the queue's handle. Producers and consumers join with fipc_queue_join (FIPC_PRODUCER, FIPC_CONSUMER), the
 * owner too: its process admits every joiner on a thread of the library's, so a member may join from any thread, the
 * owner's included. A queue's messages are of one piece: 1 to fipc_max_piece bytes, a quarter of the capacity. They
 * wait in the queue until a consumer takes them: a producer may send before any consumer has joined.
 *
 * Delivery: each message goes to one consumer; it goes again, to another consumer, only if the consumer that took it
 * ends (its process ends) before acknowledging it. When it acknowledges is the consumer's choice, not the queue's:
 *   fipc_recv                             acknowledges inside the call, before it returns: the application gets each
 *                                         message at most once (a consumer that ends inside the call never saw the
 *                                         message, which goes to another; one that ends after it returned has
 *                                         acknowledged it)
 *   fipc_recv_acquire, fipc_recv_release  acknowledges at the release (or the next receive, or fipc_close): released
 *                                         before the work, at most once; released after it, at least once, so the
 *                                         work must tolerate a duplicate (its consumer may end after the work and
 *                                         before the release)
 * Choose by the work, per consumer: work that may safely run twice (rendering, a computation, converting a file)
 * releases after it; work whose effects must never repeat (a payment, an email, a motor command) releases before it.
 * A message that goes again comes in no set order, after messages its producer sent later. Not covered: a consumer
 * that hangs while its process runs (there is no timeout); the owner's process (the queue ends with it), and a
 * reboot; a message whose consumer ends as the queue ends goes again only if the queue has room for it then. A
 * message that ends every consumer that takes it, acknowledged after the work, goes to each consumer in turn, with no
 * limit: acknowledge before the work where that can happen. A consumer holds one unacknowledged message at a time,
 * the one received in place, and a message held in place holds back its producer's room until released, however long
 * the work takes.
 *
 * The calls on a queue's connections:
 *   producer  fipc_send and fipc_send_acquire/_commit add a message to the producer's ring, waiting up to their timeout
 *             for room while consumers haven't taken (or, in place, released) the messages before it: a message held in
 *             place holds its producer back once the producer is a ring ahead of it. fipc_recv, fipc_recv_acquire,
 *             fipc_broadcast_wait_for_members: FIPC_INVALID; fipc_recv_release does nothing. fipc_close leaves the
 *             queue; the messages it sent stay for the consumers, and its place is free for another producer once they
 *             are all taken and released.
 *   consumer  fipc_recv takes a message, copies it out and acknowledges it before it returns. Its FIPC_TOO_LARGE takes
 *             nothing: *out_len is the length of the message it found, which stays in the queue, where another consumer
 *             may take it first (a buffer of fipc_max_piece bytes takes every message). fipc_recv_acquire takes a
 *             message in place, until fipc_recv_release, the next receive or fipc_close. fipc_send*,
 *             fipc_broadcast_wait_for_members: FIPC_INVALID. fipc_close leaves the queue (releasing a message held in
 *             place).
 *   fipc_max_piece and fipc_cancel work on both, as on a stream's connection. One thread at a time sends on a
 *   producer and receives on a consumer. fipc_send_many and fipc_recv_many batch a producer's sends and a consumer's
 *   receives.
 *
 * The queue ends at fipc_queue_close, or when the owner's process ends (a member's process that sees its control
 * connection to the owner fail takes that for the end). From then on a producer's sends return FIPC_DISCONNECTED (one
 * that returns it just as the queue ends may still have reached a consumer), and a consumer receives every message
 * still in the queue, then FIPC_DISCONNECTED. Nothing else ends it: a producer's or a consumer's close, or the end of
 * its process, doesn't, and consumers wait while no producer is joined.
 */

#define FIPC_PRODUCER 3 /* fipc_queue_join's and fipc_queue_wait_for_members' role: a work queue's producer */
#define FIPC_CONSUMER 4 /* fipc_queue_join's and fipc_queue_wait_for_members' role: a work queue's consumer */

/*
 * Owner: claims `name` for a work queue of up to `max_producers` producers (1 to 63) and `max_consumers` consumers
 * (1 to 64), each producer with a ring of `capacity` bytes of messages (a power of two, 4096 to 2^31), and returns the
 * queue's handle. Doesn't wait: members may join from now on. A ring holds up to capacity / 64 messages, each in its
 * length rounded up to 64 bytes of the capacity: fewer when longer ones fill it first. The queue's memory, one segment
 * every member maps, is
 *   round4K(512 + 128 * max_consumers) + (max_producers + 1) * (round4K(512 + capacity / 2) + capacity) bytes,
 * where round4K rounds up to a multiple of 4 KiB: one ring per producer and one return ring for the messages that go
 * again (for 4096 bytes, one producer and one consumer 20 KiB; for 1 MiB, 8 producers and 8 consumers about 13.5 MiB).
 * It is set up now, whole. Each producer's process also keeps capacity / 16 bytes of its own; the owner's process keeps
 * capacity / 16 bytes, up to 9 * capacity / 8 while messages that go again wait for room, and up to 512 bytes per
 * producer and consumer, and one thread, which admits the joiners, watches every joined member and returns the messages
 * that go again. FIPC_INVALID for a capacity, max_producers or max_consumers out of range; FIPC_NO_MEMORY if the memory
 * can't be had; FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
FIPC_API fipc_result_t fipc_queue_create(
    const char* name, size_t capacity, uint32_t max_producers, uint32_t max_consumers, fipc_queue_t** out_queue
);

/*
 * Producer or consumer: joins the queue on `name` as `role` (FIPC_PRODUCER or FIPC_CONSUMER) and returns its
 * connection, waiting up to timeout_ms for the owner to create the queue, for a free place of the role (a producer's
 * place is free again once every message its last producer sent was taken and released), and for the owner's process
 * to admit it. No thread of the owner's need wait for it. FIPC_INVALID for another role (the subscriber and service
 * roles included), a name a broadcast or a service holds, or an owner of another version or user; FIPC_NO_MEMORY if
 * the queue can't be mapped. On a name a stream holds, the join learns the kind when the stream's server accepts it,
 * which brings FIPC_INVALID; while no fipc_stream_accept runs, it waits and returns FIPC_TIMEOUT.
 */
FIPC_API fipc_result_t fipc_queue_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);

/*
 * Owner: waits up to timeout_ms until at least `at_least` members of `role` (FIPC_PRODUCER or FIPC_CONSUMER; 0 counts
 * both) are joined, and stores in *out_count how many are, at every return but FIPC_INVALID. Joined: admitted, and not
 * yet left. A member that leaves, or whose process ends, is counted until the owner's process has seen its end, soon
 * after. at_least 0 returns the count at once, and a timeout of 0 polls. FIPC_OK once the count is reached;
 * FIPC_TIMEOUT; FIPC_CANCELLED after fipc_queue_cancel; FIPC_DISCONNECTED once the queue has ended; FIPC_INVALID for
 * another role, or for at_least over the role's maximum (max_producers, max_consumers, or their sum for 0). Any
 * thread.
 */
FIPC_API fipc_result_t
fipc_queue_wait_for_members(fipc_queue_t* queue, int role, uint32_t at_least, uint32_t* out_count, int timeout_ms);

/*
 * Makes every fipc_queue_wait_for_members of the queue that waits, now or later, return FIPC_CANCELLED. The queue goes
 * on: its members and their joins are unaffected. Any thread, any time before fipc_queue_close. Final.
 */
FIPC_API void fipc_queue_cancel(fipc_queue_t* queue);

/*
 * Ends the queue (see above) and frees the handle; the name is free again at once. The members' connections, the
 * owner's own included, stay open until their fipc_close. No other thread may be inside a call on the queue. NULL is
 * a no-op.
 */
FIPC_API void fipc_queue_close(fipc_queue_t* queue);

/* === Services: many clients, a pool of servers, with replies === */

/*
 * A service takes requests from up to 64 clients and hands each one to one of up to 64 servers, in any processes of
 * the user. The server that took a request replies to it, and the reply goes back to the client that sent it. A server
 * waits for the requests of every client at once (one fipc_recv), and the servers share the load: each takes the next
 * request of any client. Each client has a ring of its own in the service's memory, which every member maps. A request
 * takes room there for itself and for its reply, so a server never waits for a client. The owner creates the name with
 * fipc_service_create, which returns the service's handle. Clients and servers join with fipc_service_join
 * (FIPC_CLIENT, FIPC_SERVER), and so may the owner: its process admits every joiner on a thread of the library's, so a
 * member may join from any thread, the owner's included. A service's requests and replies are of one piece, 1 to
 * fipc_max_piece bytes: max_message, the owner's choice.
 *
 * Order: a client's receive returns the reply to its oldest request whose reply it hasn't received yet, and waits for
 * that reply if it hasn't come. So the n-th reply a client receives answers its n-th request. A client may send several
 * requests before it receives (as many as its ring has room for), and the servers work on them at once. A receive that
 * times out or is cancelled leaves its reply to come: the next receive returns it. Servers take each client's requests
 * in the order it sent them, and the requests of different clients in no set order.
 *
 * A request is answered once: by its reply, or with FIPC_ABANDONED if the server that took it ended before it replied
 * (the server's process ended, or it closed its connection). An abandoned request may or may not have been carried
 * out. The service never sends it again: the client decides whether to. Not covered: a server that hangs while its
 * process runs, since there is no timeout (its client waits, within its own timeouts); and the owner's process, since
 * the service ends with it.
 *
 * The calls on a service's connections:
 *   client  fipc_send and fipc_send_acquire/_commit send a request. Each waits up to its timeout for room in the
 *           client's ring. A request holds its length rounded up to 64 bytes, plus max_message rounded up to 64, from
 *           its send until its reply has been received. A client that keeps sending without receiving waits for room
 *           that its own replies hold (with one thread, it times out). fipc_recv takes the next reply: FIPC_TOO_LARGE
 *           leaves it in place and reports its length, and a buffer of fipc_max_piece bytes takes every reply.
 *           fipc_recv_acquire takes the reply in place, until fipc_recv_release, the next receive or fipc_close. Both
 *           return FIPC_ABANDONED, with *out_len 0, for a request whose server ended: that request counts as
 *           answered. fipc_close leaves the service: requests no server took are dropped, and the replies to those
 *           already taken are lost.
 *   server  fipc_recv takes the next request of any client, copied; fipc_recv_acquire takes it in place. A server holds
 *           one request at a time: until it replies, its receives return FIPC_INVALID. fipc_send and
 *           fipc_send_acquire/_commit reply to the request it holds. They return FIPC_INVALID with no request held and
 *           FIPC_TOO_LARGE over fipc_max_piece, and a reply never waits. A request held in place stays valid until the
 *           reply's fipc_send or fipc_send_commit. fipc_recv_release does nothing. A reply to a client that has left
 *           is dropped and returns FIPC_OK. fipc_close leaves the service, and a request it still holds is
 *           FIPC_ABANDONED.
 *   fipc_max_piece and fipc_cancel work on both roles, as on a stream's connection. One thread at a time sends and one
 *   thread at a time receives on a connection; the two may be different threads (a server may receive on one thread
 *   and reply on another). fipc_broadcast_wait_for_members: FIPC_INVALID on both roles. fipc_send_many and
 *   fipc_recv_many batch a client's requests and its receives of their replies; on a server both are FIPC_INVALID.
 *
 * The service ends at fipc_service_close, or when the owner's process ends (a member's process that sees its control
 * connection to the owner fail takes that as the end). From then on:
 *   - clients' sends and servers' receives return FIPC_DISCONNECTED;
 *   - a client receives, in order, the replies that came before the end, then FIPC_DISCONNECTED at its first request
 *     without one;
 *   - a server's reply returns FIPC_DISCONNECTED. A reply that returns it just as the service ends may still have
 *     reached its client, but a reply that returned FIPC_OK is always received by a client that receives.
 * Nothing else ends a service: a member's close, or the end of its process, doesn't. Clients wait while no server is
 * joined, and servers wait while no client is.
 */

#define FIPC_CLIENT 5 /* fipc_service_join's and fipc_service_wait_for_members' role: a service's client */
#define FIPC_SERVER 6 /* fipc_service_join's and fipc_service_wait_for_members' role: a service's server */

/*
 * Owner: claims `name` for a service of up to `max_clients` clients (1 to 64) and `max_servers` servers (1 to 64), and
 * returns the service's handle. Each client gets a ring of `capacity` bytes (a power of two, 4096 to 2^31) for its
 * requests and their replies, each of 1 to `max_message` bytes (1 to capacity / 4). Doesn't wait: members may join from
 * now on. A request takes round64(its length) + round64(max_message) bytes of its client's ring, from its send until
 * its reply has been received, where round64 rounds up to a multiple of 64. A ring holds at most capacity / 64
 * requests. The service's memory, one segment every member maps, is
 *   round4K(512 + 128 * max_servers) + max_clients * (round4K(512 + capacity / 2) + capacity) bytes,
 * where round4K rounds up to a multiple of 4 KiB (for 4096 bytes, one client and one server: 12 KiB; for 64 KiB, 64
 * clients and 8 servers: about 6.3 MiB). It is set up now, whole. Each client's process also keeps capacity / 16 bytes
 * of its own. The owner's process keeps up to 512 bytes per client and per server, and one thread, which admits the
 * joiners, watches every joined member and answers the requests of the servers that end (FIPC_ABANDONED).
 * FIPC_INVALID for a capacity, max_message, max_clients or max_servers out of range; FIPC_NO_MEMORY if the memory can't
 * be had; FIPC_ADDR_IN_USE if another stream, broadcast, queue or service holds the name.
 */
FIPC_API fipc_result_t fipc_service_create(
    const char* name,
    size_t capacity,
    size_t max_message,
    uint32_t max_clients,
    uint32_t max_servers,
    fipc_service_t** out_service
);

/*
 * Client or server: joins the service on `name` as `role` (FIPC_CLIENT or FIPC_SERVER) and returns its connection.
 * It waits up to timeout_ms for the owner to create the service, for a free place of the role, and for the owner's
 * process to admit it. A client's place is free again once its last client has left and every request of it that a
 * server took has been answered (those no server took are dropped). No thread of the owner's need wait for it.
 * FIPC_INVALID for another role (the subscriber and queue roles included), a name a broadcast or a queue holds, or an
 * owner of another version or user; FIPC_NO_MEMORY if the service can't be mapped. On a name a stream holds, the join
 * learns the kind only when the stream's server accepts it, which brings FIPC_INVALID; while no fipc_stream_accept
 * runs, it waits and returns FIPC_TIMEOUT.
 */
FIPC_API fipc_result_t fipc_service_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);

/*
 * Owner: waits up to timeout_ms until at least `at_least` members of `role` (FIPC_CLIENT or FIPC_SERVER; 0 counts
 * both) are joined, and stores in *out_count how many are, at every return but FIPC_INVALID. Joined means admitted and
 * not yet left. A member that leaves, or whose process ends, is still counted until the owner's process has seen its
 * end, soon after. at_least 0 returns the count at once, and a timeout of 0 polls. Returns FIPC_OK once the count is
 * reached; FIPC_TIMEOUT; FIPC_CANCELLED after fipc_service_cancel; FIPC_DISCONNECTED once the service has ended;
 * FIPC_INVALID for another role, or for at_least over the role's maximum (max_clients, max_servers, or their sum for
 * 0). Any thread.
 */
FIPC_API fipc_result_t fipc_service_wait_for_members(
    fipc_service_t* service, int role, uint32_t at_least, uint32_t* out_count, int timeout_ms
);

/*
 * Makes every fipc_service_wait_for_members of the service that waits, now or later, return FIPC_CANCELLED. The
 * service goes on: its members and their joins are unaffected. Any thread, any time before fipc_service_close. Final.
 */
FIPC_API void fipc_service_cancel(fipc_service_t* service);

/*
 * Ends the service (see above) and frees the handle. The name is free again at once. The members' connections, the
 * owner's own included, stay open until their fipc_close. No other thread may be inside a call on the service. NULL
 * is a no-op.
 */
FIPC_API void fipc_service_close(fipc_service_t* service);

#ifdef __cplusplus
}
#endif

#endif /* FIPC_H */
