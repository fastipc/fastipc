/*
 * The benchmark's message loops through the C API (include/fipc.h): the program c_bench.
 */

#include <stdlib.h>
#include <string.h>

#include "bench.h"

const char* const bench_api = "C";

/* What a received message is: data, or one of the markers */
enum
{
    DATA,
    AT_END,
    AT_GO,
};

static int marker(const void* data, size_t len)
{
    if (len != 3)
        return DATA;
    if (memcmp(data, "END", 3) == 0)
        return AT_END;
    return memcmp(data, "GO!", 3) == 0 ? AT_GO : DATA;
}

bench_served bench_serve(fipc_conn_t* conn, enum bench_mode mode, size_t size)
{
    bench_served served = {0, 0, 0.0, FIPC_OK};
    char* buf = malloc(size < 3 ? 3 : size);
    char* sink = malloc(size < 3 ? 3 : size);
    const size_t cap = size < 3 ? 3 : size;
    int timing = 0;
    double start = bench_now();
    for (;;)
    {
        size_t len = 0;
        int kind = DATA;
        fipc_result_t result;
        if (mode == BENCH_RPC)
        {
            fipc_rpc_msg_t msg = {0};
            result = fipc_rpc_recv(conn, buf, cap, &msg, FIPC_FOREVER);
            kind = msg.opcode == BENCH_END_OPCODE ? AT_END : msg.opcode == BENCH_GO_OPCODE ? AT_GO : DATA;
            len = (size_t) msg.len;
        }
        else if (mode == BENCH_ZEROCOPY)
        {
            const void* data;
            result = fipc_recv_acquire(conn, &data, &len, FIPC_FOREVER);
            if (result == FIPC_OK)
            {
                kind = marker(data, len);
                if (kind == DATA)
                {
                    memcpy(sink, data, len <= cap ? len : cap); /* a consumer copies the message out */
                    __asm__ volatile(""
                                     :
                                     : "r"(sink)
                                     : "memory"); /* and uses it: the compiler would drop a dead copy */
                }
                fipc_recv_release(conn);
            }
            else if (result == FIPC_TOO_LARGE) /* several pieces */
                result = fipc_recv(conn, buf, cap, &len, FIPC_FOREVER);
        }
        else
        {
            result = fipc_recv(conn, buf, cap, &len, FIPC_FOREVER);
            if (result == FIPC_OK)
                kind = marker(buf, len);
        }
        if (result != FIPC_OK)
        {
            served.result = result;
            break;
        }
        if (kind == AT_END)
            break;
        if (kind == AT_GO) /* the last one starts the timed messages */
        {
            timing = 1;
            served.messages = 0;
            served.bytes = 0;
            start = bench_now();
        }
        else if (timing)
        {
            served.messages++;
            served.bytes += len;
        }
    }
    served.seconds = bench_now() - start;
    fipc_close(conn);
    free(buf);
    free(sink);
    return served;
}

static fipc_result_t send_marker(fipc_conn_t* conn, enum bench_mode mode, uint32_t opcode, const char* text)
{
    uint64_t id;
    return mode == BENCH_RPC ? fipc_rpc_submit(conn, opcode, NULL, 0, &id, FIPC_FOREVER)
                             : fipc_send(conn, text, 3, FIPC_FOREVER);
}

static fipc_result_t send_one(fipc_conn_t* conn, enum bench_mode mode, const char* payload, size_t size, int one_piece)
{
    if (mode == BENCH_RPC)
    {
        uint64_t id;
        return fipc_rpc_submit(conn, 1, payload, size, &id, FIPC_FOREVER);
    }
    if (mode == BENCH_ZEROCOPY && one_piece)
    {
        void* slot;
        fipc_result_t result = fipc_send_acquire(conn, size, &slot, FIPC_FOREVER);
        if (result == FIPC_OK)
        {
            memcpy(slot, payload, size);
            result = fipc_send_commit(conn, size);
        }
        return result;
    }
    return fipc_send(conn, payload, size, FIPC_FOREVER);
}

fipc_result_t bench_send(fipc_conn_t* conn, enum bench_mode mode, size_t size, size_t count)
{
    char* payload = malloc(size);
    memset(payload, 'x', size);
    const int one_piece = size <= fipc_max_piece(conn);
    fipc_result_t result = FIPC_OK;
    const double warm = bench_now() + BENCH_WARM_UP;
    for (size_t i = 0; result == FIPC_OK && (i < count || bench_now() < warm); i++)
    {
        if (i % BENCH_MARKER_EVERY == 0)
            result = send_marker(conn, mode, BENCH_GO_OPCODE, "GO!");
        if (result == FIPC_OK)
            result = send_one(conn, mode, payload, size, one_piece);
    }
    if (result == FIPC_OK)
        result = send_marker(conn, mode, BENCH_GO_OPCODE, "GO!");
    for (size_t i = 0; i < count && result == FIPC_OK; i++)
        result = send_one(conn, mode, payload, size, one_piece);
    if (result == FIPC_OK)
        result = send_marker(conn, mode, BENCH_END_OPCODE, "END");
    fipc_close(conn);
    free(payload);
    return result;
}
