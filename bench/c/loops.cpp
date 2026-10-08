// The benchmark's message loops through the C++ wrapper (include/fipc.hpp): the program cpp_bench. The same loops as
// loops.c, so that the two programs measure what the wrapper adds.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

#include <fipc.hpp>

#include "bench.h"

extern "C" const char* const bench_api = "C++";

namespace
{

// What a received message is: data, or one of the markers
enum class kind
{
    data,
    end,
    go,
};

kind marker(std::span<const std::byte> message)
{
    if (message.size() != 3)
        return kind::data;
    if (std::memcmp(message.data(), "END", 3) == 0)
        return kind::end;
    return std::memcmp(message.data(), "GO!", 3) == 0 ? kind::go : kind::data;
}

fipc::status send_marker(fipc::connection& conn, bench_mode mode, std::uint32_t opcode, std::string_view text)
{
    if (mode == BENCH_RPC)
    {
        auto id = conn.rpc_submit(opcode, std::string_view());
        return id ? fipc::status() : fipc::status(id.error());
    }
    return conn.send(text);
}

fipc::status send_one(fipc::connection& conn, bench_mode mode, std::span<const std::byte> payload, bool one_piece)
{
    if (mode == BENCH_RPC)
    {
        auto id = conn.rpc_submit(1, payload);
        return id ? fipc::status() : fipc::status(id.error());
    }
    if (mode == BENCH_ZEROCOPY && one_piece)
    {
        auto slot = conn.send_acquire(payload.size());
        if (!slot)
            return slot.error();
        std::memcpy(slot->data(), payload.data(), payload.size());
        return slot->commit();
    }
    return conn.send(payload);
}

}  // namespace

extern "C" bench_served bench_serve(fipc_conn_t* raw, bench_mode mode, size_t size)
{
    fipc::connection conn(raw);
    // Uninitialized, as loops.c's malloc: the first messages page them in
    const std::size_t cap = std::max<std::size_t>(size, 3);
    const auto buf_memory = std::make_unique_for_overwrite<std::byte[]>(cap);
    const auto sink = std::make_unique_for_overwrite<std::byte[]>(cap);
    const std::span<std::byte> buf(buf_memory.get(), cap);
    bench_served served{0, 0, 0.0, FIPC_OK};
    bool timing = false;
    double start = bench_now();
    for (;;)
    {
        std::size_t len = 0;
        kind received_kind = kind::data;
        if (mode == BENCH_RPC)
        {
            auto header = conn.rpc_receive_into(buf);
            if (!header)
            {
                served.result = static_cast<fipc_result_t>(header.error().code);
                break;
            }
            received_kind = header->opcode == BENCH_END_OPCODE ? kind::end
                          : header->opcode == BENCH_GO_OPCODE  ? kind::go
                                                               : kind::data;
            len = header->len;
        }
        else if (mode == BENCH_ZEROCOPY)
        {
            auto message = conn.receive_acquire();
            if (message)
            {
                len = message->size();
                received_kind = marker(message->bytes());
                if (received_kind == kind::data)
                {
                    std::memcpy(sink.get(), message->data(), std::min(len, cap));  // a consumer copies it out
                    __asm__ volatile(""
                                     :
                                     : "r"(sink.get())
                                     : "memory");  // and uses it: the compiler would drop a dead copy
                }
            }
            else if (message.error() == fipc::errc::too_large)  // several pieces
            {
                auto received = conn.receive_into(buf);
                if (!received)
                {
                    served.result = static_cast<fipc_result_t>(received.error().code);
                    break;
                }
                len = *received;
            }
            else
            {
                served.result = static_cast<fipc_result_t>(message.error().code);
                break;
            }
        }
        else
        {
            auto received = conn.receive_into(buf);
            if (!received)
            {
                served.result = static_cast<fipc_result_t>(received.error().code);
                break;
            }
            len = *received;
            received_kind = marker(buf.first(len));
        }
        if (received_kind == kind::end)
            break;
        if (received_kind == kind::go)  // the last one starts the timed messages
        {
            timing = true;
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
    return served;
}

extern "C" fipc_result_t bench_send(fipc_conn_t* raw, bench_mode mode, size_t size, size_t count)
{
    fipc::connection conn(raw);
    const std::vector<std::byte> payload(size, std::byte{'x'});
    const bool one_piece = size <= conn.max_piece();
    fipc::status sent;
    const double warm = bench_now() + BENCH_WARM_UP;
    for (size_t i = 0; sent && (i < count || bench_now() < warm); i++)
    {
        if (i % BENCH_MARKER_EVERY == 0)
            sent = send_marker(conn, mode, BENCH_GO_OPCODE, "GO!");
        if (sent)
            sent = send_one(conn, mode, payload, one_piece);
    }
    if (sent)
        sent = send_marker(conn, mode, BENCH_GO_OPCODE, "GO!");
    for (size_t i = 0; i < count && sent; i++)
        sent = send_one(conn, mode, payload, one_piece);
    if (sent)
        sent = send_marker(conn, mode, BENCH_END_OPCODE, "END");
    return sent ? FIPC_OK : static_cast<fipc_result_t>(sent.error().code);
}
