// Boost.Interprocess message_queue: a queue in shared memory guarded by an interprocess mutex and conditions, any
// number of senders and receivers, each message to one receiver. A stream is one queue (a round trip: one each way),
// a work queue one queue all its members share; its depth is 512 KiB of messages (8 to 1024 of them), or with
// --buffer its bytes of messages (at least 8). No broadcast, no service.
#include <memory>
#include <string>
#include <vector>

#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>

#include "harness.hpp"

namespace alt
{
namespace
{

namespace bip = boost::interprocess;

struct Queue
{
    std::string name;
    std::unique_ptr<bip::message_queue> queue;
    bool owner;

    Queue(const std::string& name, const Params& p, bool owner) : name(name), owner(owner)
    {
        queue = std::make_unique<bip::message_queue>(bip::open_or_create, name.c_str(), depth_for(p), p.size);
    }

    ~Queue()
    {
        queue.reset();
        if (owner)
            bip::message_queue::remove(name.c_str());
    }
};

struct BoostEndpoint : Endpoint
{
    std::unique_ptr<Queue> out, in;
    std::vector<uint8_t> buffer;

    explicit BoostEndpoint(size_t size) : buffer(size)
    {
    }

    void send(const uint8_t* data, size_t len) override
    {
        out->queue->send(data, len, 0);
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        bip::message_queue::size_type len = 0;
        unsigned int priority = 0;
        bool got;
        if (timeout_ms == 0)
        {
            got = in->queue->try_receive(buffer.data(), buffer.size(), len, priority);
        }
        else
        {
            auto deadline =
                boost::posix_time::microsec_clock::universal_time() + boost::posix_time::milliseconds(timeout_ms);
            got = in->queue->timed_receive(buffer.data(), buffer.size(), len, priority, deadline);
        }
        if (!got)
            return 0;
        sink.on(buffer.data(), len);
        return 1;
    }
};

bool supports(Pattern pattern, const Params&)
{
    return pattern == Pattern::stream || pattern == Pattern::queue;
}

std::unique_ptr<Endpoint> open(Pattern pattern, Role role, const Params& p)
{
    auto endpoint = std::make_unique<BoostEndpoint>(p.size);
    std::string forward = p.name + "-ab", back = p.name + "-ba";
    if (pattern == Pattern::queue)
    {
        if (role == Role::producer)
            endpoint->out = std::make_unique<Queue>(forward, p, false);
        else
            endpoint->in = std::make_unique<Queue>(forward, p, p.index == 0);
        return endpoint;
    }
    switch (role)
    {
        case Role::tx:
            endpoint->out = std::make_unique<Queue>(forward, p, false);
            break;
        case Role::rx:
            endpoint->in = std::make_unique<Queue>(forward, p, true);
            break;
        case Role::ping:
            endpoint->out = std::make_unique<Queue>(forward, p, false);
            endpoint->in = std::make_unique<Queue>(back, p, true);
            break;
        case Role::echo:
            endpoint->in = std::make_unique<Queue>(forward, p, true);
            endpoint->out = std::make_unique<Queue>(back, p, false);
            break;
        default:
            fail("boost: no such role");
    }
    return endpoint;
}

}  // namespace

extern const Lib lib_boost{"boost", supports, open};

}  // namespace alt
