// cpp-ipc (libipc): ipc::route, one sender to many receivers in shared memory, default options. A stream is a route
// with one receiver (a round trip: one route each way), a broadcast a route with many. Its send() pushes over a
// receiver that hasn't made room within its timeout; this harness sends with try_send() in a loop, so no message is
// lost. No work queue (a route gives every message to every receiver), no service.
#include <cstring>
#include <string>
#include <vector>

#include <libipc/ipc.h>

#include "harness.hpp"

namespace alt
{
namespace
{

constexpr uint64_t wait_ms = 100;

struct CppIpcEndpoint : Endpoint
{
    ipc::route out, in;
    std::vector<uint8_t> buffer;

    explicit CppIpcEndpoint(size_t size) : buffer(size)
    {
    }

    void send(const uint8_t* data, size_t len) override
    {
        while (!out.try_send(data, len, wait_ms))
        {
        }
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        ipc::buff_t message = timeout_ms == 0 ? in.try_recv() : in.recv(static_cast<uint64_t>(timeout_ms));
        if (message.empty())
            return 0;
        std::memcpy(buffer.data(), message.data(), message.size() < buffer.size() ? message.size() : buffer.size());
        sink.on(buffer.data(), message.size());
        return 1;
    }
};

bool supports(Pattern pattern, const Params& p)
{
    return pattern == Pattern::stream || (pattern == Pattern::broadcast && !p.lossy);
}

std::unique_ptr<Endpoint> open(Pattern pattern, Role role, const Params& p)
{
    auto endpoint = std::make_unique<CppIpcEndpoint>(p.size);
    std::string forward = p.name + "-ab", back = p.name + "-ba";
    switch (role)
    {
        case Role::tx:
        case Role::pub:
            endpoint->out.connect(forward.c_str(), ipc::sender);
            if (!endpoint->out.wait_for_recv(pattern == Pattern::broadcast ? p.subscribers : 1))
                fail("wait_for_recv");
            break;
        case Role::rx:
        case Role::sub:
            endpoint->in.connect(forward.c_str(), ipc::receiver);
            break;
        case Role::ping:
            endpoint->in.connect(back.c_str(), ipc::receiver);
            endpoint->out.connect(forward.c_str(), ipc::sender);
            if (!endpoint->out.wait_for_recv(1))
                fail("wait_for_recv");
            break;
        case Role::echo:
            endpoint->in.connect(forward.c_str(), ipc::receiver);
            endpoint->out.connect(back.c_str(), ipc::sender);
            if (!endpoint->out.wait_for_recv(1))
                fail("wait_for_recv");
            break;
        default:
            fail("cpp-ipc: no such role");
    }
    return endpoint;
}

}  // namespace

extern const Lib lib_cppipc{"cpp-ipc", supports, open};

}  // namespace alt
