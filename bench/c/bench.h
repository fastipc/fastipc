/*
 * The C and C++ benchmark's two halves (bench/c): the harness (main.c: the cases, the client process, the timing and
 * the report) and the message loops it measures, written once against the C API (loops.c, the program c_bench) and
 * once against the C++ wrapper (loops.cpp, the program cpp_bench).
 */

#ifndef FIPC_BENCH_H
#define FIPC_BENCH_H

#include <stddef.h>

#include "fipc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* What the server measures: copy (fipc_send / fipc_recv into a buffer it reuses), zerocopy (fipc_send_acquire and
 * fipc_send_commit / fipc_recv_acquire, copied out, then fipc_recv_release; a message of several pieces through the
 * copying calls) or rpc (fipc_rpc_submit / fipc_rpc_recv into a buffer it reuses). */
enum bench_mode
{
    BENCH_COPY,
    BENCH_ZEROCOPY,
    BENCH_RPC,
};

/* The opcodes of the RPC end and start markers; the plain markers are the messages "END" and "GO!" */
#define BENCH_END_OPCODE 0xFB000001u
#define BENCH_GO_OPCODE 0xFB000002u

/* The warm-up: the client sends messages untimed for at least this many seconds (and at least the case's count),
 * with a start marker every BENCH_MARKER_EVERY of them, then the start marker the server times from */
#define BENCH_WARM_UP 0.2
#define BENCH_MARKER_EVERY 1000

/* What the server's loop received, and how long it took: result is FIPC_OK, or why the loop stopped */
typedef struct
{
    size_t messages;
    size_t bytes;
    double seconds;
    fipc_result_t result;
} bench_served;

/* The API the loops use: "C" or "C++" */
extern const char* const bench_api;

/* The server: receives on `conn` until the end marker, skipping the warm-up and timing from the last start marker to
 * the end marker, then closes `conn`. */
bench_served bench_serve(fipc_conn_t* conn, enum bench_mode mode, size_t size);

/* The client: sends the warm-up, the start marker, `count` messages of `size` bytes and the end marker on `conn`, then
 * closes it; FIPC_OK or the first failure. */
fipc_result_t bench_send(fipc_conn_t* conn, enum bench_mode mode, size_t size, size_t count);

/* Seconds on a monotonic clock (the harness's) */
double bench_now(void);

#ifdef __cplusplus
}
#endif

#endif /* FIPC_BENCH_H */
