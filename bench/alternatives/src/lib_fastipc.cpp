// fastipc through its C API (fipc.h), with rings of --buffer bytes (512 KiB without it): the copying calls, fipc_send
// and fipc_recv ("fastipc"), or the zero-copy ones ("fastipc-zerocopy": the sender writes into fipc_send_acquire's room
// and commits; the receiver copies the message out of fipc_recv_acquire's and releases it). The zero-copy calls take a
// message of one piece, which every case's messages are (a stream's ring holds one of its capacity less 64 bytes).
// --batch N: the copying calls' batches, fipc_send_many of N messages and fipc_recv_many of up to N into a buffer of N
// messages (the zero-copy calls have none).
#include <fipc.h>

#include <cstring>
#include <vector>

#include "harness.hpp"

namespace alt
{
namespace
{

size_t capacity(const Params& p)
{
    return p.buffer ? p.buffer : 512 * 1024;
}

// A ring fipc.h takes: a power of two, 4096 to 2^31 (a stream's from 1024), with room for a message of one piece (a
// quarter of it; a stream sends a longer one in pieces).
bool takes(Pattern pattern, const Params& p)
{
    size_t ring = capacity(p);
    return (ring & (ring - 1)) == 0 && ring >= (pattern == Pattern::stream ? 1024 : 4096) && ring <= (size_t(1) << 31)
        && (pattern == Pattern::stream || p.size <= ring / 4);
}

void check(fipc_result_t result, const char* what)
{
    if (result != FIPC_OK)
        fail(what, fipc_result_str(result));
}

struct FastipcEndpoint : Endpoint
{
    fipc_listener_t* listener = nullptr;
    fipc_queue_t* queue = nullptr;
    fipc_service_t* service = nullptr;
    fipc_conn_t* conn = nullptr;
    std::vector<uint8_t> buffer;
    bool zero_copy;
    // --batch: each message's address and length for fipc_send_many, and their lengths from fipc_recv_many.
    std::vector<const void*> datas;
    std::vector<size_t> lens;

    FastipcEndpoint(const Params& p, bool zero_copy)
        : buffer(p.size * p.batch), zero_copy(zero_copy), datas(p.batch), lens(p.batch, p.size)
    {
    }

    ~FastipcEndpoint() override
    {
        fipc_close(conn);
        fipc_listener_close(listener);
        fipc_queue_close(queue);
        fipc_service_close(service);
    }

    // A server's reply after the service's owner ended: the run is over.
    void send(const uint8_t* data, size_t len) override
    {
        fipc_result_t result;
        if (zero_copy)
        {
            void* room = nullptr;
            result = fipc_send_acquire(conn, len, &room, FIPC_FOREVER);
            if (result == FIPC_OK)
            {
                std::memcpy(room, data, len);
                result = fipc_send_commit(conn, len);
            }
        }
        else
        {
            result = fipc_send(conn, data, len, FIPC_FOREVER);
        }
        if (result != FIPC_OK && result != FIPC_DISCONNECTED)
            fail("fipc_send", fipc_result_str(result));
    }

    void send_batch(const uint8_t* data, size_t len, int n) override
    {
        for (int k = 0; k < n; k++)
        {
            datas[k] = data + k * len;
            lens[k] = len;
        }
        size_t done = 0;
        while (done < static_cast<size_t>(n))
        {
            size_t sent = 0;
            fipc_result_t result =
                fipc_send_many(conn, datas.data() + done, lens.data() + done, n - done, &sent, FIPC_FOREVER);
            done += sent;
            if (result == FIPC_DISCONNECTED)
                return;
            if (result != FIPC_OK)
                fail("fipc_send_many", fipc_result_str(result));
        }
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        size_t len = 0;
        fipc_result_t result;
        if (lens.size() > 1)
        {
            size_t count = 0;
            result = fipc_recv_many(conn, buffer.data(), buffer.size(), lens.data(), lens.size(), &count, timeout_ms);
            if (result == FIPC_OK)
            {
                size_t at = 0;
                for (size_t k = 0; k < count; k++)
                {
                    sink.on(buffer.data() + at, lens[k]);
                    at += lens[k];
                }
                return static_cast<int>(count);
            }
        }
        else if (zero_copy)
        {
            const void* data = nullptr;
            result = fipc_recv_acquire(conn, &data, &len, timeout_ms);
            if (result == FIPC_OK)
            {
                std::memcpy(buffer.data(), data, len < buffer.size() ? len : buffer.size());
                fipc_recv_release(conn);
                sink.on(buffer.data(), len);
                return 1;
            }
        }
        else
        {
            result = fipc_recv(conn, buffer.data(), buffer.size(), &len, timeout_ms);
            if (result == FIPC_OK)
            {
                sink.on(buffer.data(), len);
                return 1;
            }
        }
        // A lossy subscriber that fell behind: the messages it lost show as gaps in their sequence numbers. The owner of
        // a queue or a service (its first consumer or server) may end before the others when the run is over.
        if (result == FIPC_TIMEOUT || result == FIPC_OVERRUN || result == FIPC_DISCONNECTED)
            return 0;
        fail("fipc_recv", fipc_result_str(result));
    }
};

bool supports(Pattern pattern, const Params& p)
{
    return takes(pattern, p);
}

// A lossy subscriber receives only with the copying call (it checks that the publisher didn't overwrite the message).
bool supports_zero_copy(Pattern pattern, const Params& p)
{
    return (pattern != Pattern::broadcast || !p.lossy) && takes(pattern, p)
        && (pattern != Pattern::stream || p.size <= capacity(p) - 64);
}

std::unique_ptr<Endpoint> open_with(bool zero_copy, Pattern pattern, Role role, const Params& p)
{
    auto endpoint = std::make_unique<FastipcEndpoint>(p, zero_copy);
    const char* name = p.name.c_str();
    switch (pattern)
    {
        case Pattern::stream:
            if (role == Role::rx || role == Role::echo)
            {
                check(fipc_stream_create(name, capacity(p), &endpoint->listener), "fipc_stream_create");
                check(fipc_stream_accept(endpoint->listener, &endpoint->conn, FIPC_FOREVER), "fipc_stream_accept");
            }
            else
            {
                check(fipc_stream_connect(name, &endpoint->conn, FIPC_FOREVER), "fipc_stream_connect");
            }
            break;
        case Pattern::broadcast:
            if (role == Role::pub)
            {
                check(
                    fipc_broadcast_create(name, capacity(p), p.subscribers, FIPC_FOREVER, &endpoint->conn),
                    "fipc_broadcast_create"
                );
                uint32_t joined = 0;
                check(
                    fipc_broadcast_wait_for_members(endpoint->conn, p.subscribers, &joined, FIPC_FOREVER),
                    "fipc_broadcast_wait_for_members"
                );
            }
            else
            {
                int mode = p.lossy ? FIPC_SUBSCRIBER_LOSSY : FIPC_SUBSCRIBER_GATED;
                check(fipc_broadcast_join(name, mode, &endpoint->conn, FIPC_FOREVER), "fipc_broadcast_join");
            }
            break;
        case Pattern::queue:
            // The first consumer owns the queue.
            if (role == Role::consumer && p.index == 0)
            {
                check(
                    fipc_queue_create(name, capacity(p), p.producers, p.consumers, &endpoint->queue),
                    "fipc_queue_create"
                );
            }
            check(
                fipc_queue_join(
                    name, role == Role::producer ? FIPC_PRODUCER : FIPC_CONSUMER, &endpoint->conn, FIPC_FOREVER
                ),
                "fipc_queue_join"
            );
            break;
        case Pattern::service:
            // The first server owns the service.
            if (role == Role::server && p.index == 0)
            {
                check(
                    fipc_service_create(name, capacity(p), p.size, p.clients, p.servers, &endpoint->service),
                    "fipc_service_create"
                );
            }
            check(
                fipc_service_join(
                    name, role == Role::client ? FIPC_CLIENT : FIPC_SERVER, &endpoint->conn, FIPC_FOREVER
                ),
                "fipc_service_join"
            );
            break;
    }
    return endpoint;
}

std::unique_ptr<Endpoint> open_copy(Pattern pattern, Role role, const Params& p)
{
    return open_with(false, pattern, role, p);
}

std::unique_ptr<Endpoint> open_zero_copy(Pattern pattern, Role role, const Params& p)
{
    return open_with(true, pattern, role, p);
}

}  // namespace

extern const Lib lib_fastipc{"fastipc", supports, open_copy, true};
extern const Lib lib_fastipc_zerocopy{"fastipc-zerocopy", supports_zero_copy, open_zero_copy};

}  // namespace alt
