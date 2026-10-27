// iceoryx2 through its C++ API: publish-subscribe and request-response services of byte slices, zero-copy (the
// sender writes into a loaned sample; the receiver copies the payload out of shared memory). Its receives don't wait:
// every receive polls, as iceoryx2's own benchmarks do.
//   stream     a publisher and one subscriber (a round trip: a service each way), buffers of 512 KiB of messages (8 to
//              1024 of them; with --buffer, its bytes of messages, at least 8), no overflow: a full subscriber holds
//              the publisher back (RetryUntilDelivered)
//   broadcast  a publisher and its subscribers, the same; lossy: with safe overflow (the oldest message is replaced)
//   service    request-response: clients and one server (iceoryx2 delivers each request to every server, so it has no
//              pool of servers that share the load)
// No work queue (a publisher's sample goes to every subscriber).
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <iox2/iceoryx2.hpp>

#include "harness.hpp"

namespace alt
{
namespace
{

using namespace iox2;
using Bytes = bb::Slice<uint8_t>;
constexpr ServiceType ipc_type = ServiceType::Ipc;
using NodeT = Node<ipc_type>;

auto make_node()
{
    return NodeBuilder().create<ipc_type>().value();
}

ServiceName service_name(const std::string& name)
{
    auto result = ServiceName::create(name.c_str());
    if (!result.has_value())
        fail("iceoryx2: a bad service name", name);
    return std::move(result.value());
}

auto make_pubsub(NodeT& node, const std::string& name, const Params& p, uint64_t subscribers)
{
    auto result = node.service_builder(service_name(name))
                      .publish_subscribe<Bytes>()
                      .max_publishers(1)
                      .max_subscribers(subscribers)
                      .history_size(0)
                      .subscriber_max_buffer_size(depth_for(p))
                      .enable_safe_overflow(p.lossy)
                      .open_or_create();
    if (!result.has_value())
        fail("iceoryx2: publish_subscribe open_or_create", name);
    return std::move(result.value());
}

auto make_reqres(NodeT& node, const std::string& name, const Params& p)
{
    auto result = node.service_builder(service_name(name))
                      .request_response<Bytes, Bytes>()
                      .max_clients(p.clients)
                      .max_servers(1)
                      .max_active_requests_per_client(1)
                      .enable_safe_overflow_for_requests(false)
                      .enable_safe_overflow_for_responses(false)
                      .open_or_create();
    if (!result.has_value())
        fail("iceoryx2: request_response open_or_create", name);
    return std::move(result.value());
}

using PubSub = std::remove_cvref_t<decltype(make_pubsub(std::declval<NodeT&>(), std::string(), Params(), 1))>;
using ReqRes = std::remove_cvref_t<decltype(make_reqres(std::declval<NodeT&>(), std::string(), Params()))>;
using Pub = std::remove_cvref_t<decltype(std::declval<PubSub&>().publisher_builder().create().value())>;
using Sub = std::remove_cvref_t<decltype(std::declval<PubSub&>().subscriber_builder().create().value())>;
using ClientT = std::remove_cvref_t<decltype(std::declval<ReqRes&>().client_builder().create().value())>;
using ServerT = std::remove_cvref_t<decltype(std::declval<ReqRes&>().server_builder().create().value())>;
using Pending =
    std::remove_cvref_t<decltype(send(assume_init(std::declval<ClientT&>().loan_slice_uninit(1).value())).value())>;
using Active = std::remove_cvref_t<decltype(std::declval<ServerT&>().receive().value().value())>;

// Polls `attempt` until it delivers or timeout_ms has passed, reading the clock once per 256 attempts.
template <typename F>
int poll_for(int timeout_ms, F&& attempt)
{
    if (attempt())
        return 1;
    if (timeout_ms == 0)
        return 0;
    uint64_t deadline = ticks() + static_cast<uint64_t>(ticks_per_second() * timeout_ms / 1000);
    for (;;)
    {
        for (int k = 0; k < 256; k++)
        {
            if (attempt())
                return 1;
        }
        if (ticks() >= deadline)
            return 0;
    }
}

template <typename Sample>
void copy_into(Sample& sample, const uint8_t* data, size_t len)
{
    std::memcpy(sample.payload_mut().data(), data, len);
}

struct Iceoryx2Endpoint : Endpoint
{
    NodeT node = make_node();
    std::optional<PubSub> out_service, in_service;
    std::optional<Pub> publisher;
    std::optional<Sub> subscriber;
    std::optional<ReqRes> reqres;
    std::optional<ClientT> client;
    std::optional<ServerT> server;
    std::optional<Pending> pending;
    std::optional<Active> active;
    std::vector<uint8_t> buffer;
    size_t size;

    explicit Iceoryx2Endpoint(size_t size) : buffer(size), size(size)
    {
    }

    ~Iceoryx2Endpoint() override
    {
        active.reset();
        pending.reset();
        server.reset();
        client.reset();
        subscriber.reset();
        publisher.reset();
        reqres.reset();
        in_service.reset();
        out_service.reset();
    }

    void open_publisher(const std::string& name, const Params& p, uint64_t subscribers)
    {
        out_service.emplace(make_pubsub(node, name, p, subscribers));
        auto strategy = p.lossy ? BackpressureStrategy::DiscardData : BackpressureStrategy::RetryUntilDelivered;
        auto result =
            out_service->publisher_builder().initial_max_slice_len(p.size).backpressure_strategy(strategy).create();
        if (!result.has_value())
            fail("iceoryx2: publisher create", name);
        publisher.emplace(std::move(result.value()));
        while (out_service->dynamic_config().number_of_subscribers() < subscribers)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void open_subscriber(const std::string& name, const Params& p, uint64_t subscribers)
    {
        in_service.emplace(make_pubsub(node, name, p, subscribers));
        auto result = in_service->subscriber_builder().create();
        if (!result.has_value())
            fail("iceoryx2: subscriber create", name);
        subscriber.emplace(std::move(result.value()));
    }

    void send(const uint8_t* data, size_t len) override
    {
        if (publisher)
        {
            auto sample = publisher->loan_slice_uninit(len);
            if (!sample.has_value())
                fail("iceoryx2: loan_slice_uninit");
            copy_into(sample.value(), data, len);
            if (!iox2::send(assume_init(std::move(sample.value()))).has_value())
                fail("iceoryx2: send");
        }
        else if (client)
        {
            auto request = client->loan_slice_uninit(len);
            if (!request.has_value())
                fail("iceoryx2: client loan_slice_uninit");
            copy_into(request.value(), data, len);
            auto result = iox2::send(assume_init(std::move(request.value())));
            if (!result.has_value())
                fail("iceoryx2: request send");
            pending.emplace(std::move(result.value()));
        }
        else
        {
            auto response = active->loan_slice_uninit(len);
            if (!response.has_value())
                fail("iceoryx2: response loan_slice_uninit");
            copy_into(response.value(), data, len);
            if (!iox2::send(assume_init(std::move(response.value()))).has_value())
                fail("iceoryx2: response send");
            active.reset();
        }
    }

    template <typename Payload>
    void deliver(Sink& sink, const Payload& payload)
    {
        size_t len = payload.number_of_bytes();
        std::memcpy(buffer.data(), payload.data(), len < size ? len : size);
        sink.on(buffer.data(), len);
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        if (subscriber)
        {
            return poll_for(
                timeout_ms,
                [&]
                {
                    auto sample = subscriber->receive();
                    if (!sample.has_value())
                        fail("iceoryx2: receive");
                    if (!sample.value().has_value())
                        return false;
                    deliver(sink, sample.value()->payload());
                    return true;
                }
            );
        }
        if (client)
        {
            return poll_for(
                timeout_ms,
                [&]
                {
                    auto response = pending->receive();
                    if (!response.has_value())
                        fail("iceoryx2: response receive");
                    if (!response.value().has_value())
                        return false;
                    deliver(sink, response.value()->payload());
                    pending.reset();
                    return true;
                }
            );
        }
        return poll_for(
            timeout_ms,
            [&]
            {
                auto request = server->receive();
                if (!request.has_value())
                    fail("iceoryx2: server receive");
                if (!request.value().has_value())
                    return false;
                active.emplace(std::move(request.value().value()));
                deliver(sink, active->payload());
                return true;
            }
        );
    }
};

bool supports(Pattern pattern, const Params& p)
{
    if (pattern == Pattern::queue)
        return false;
    if (pattern == Pattern::service)
        return p.servers == 1;
    return true;
}

std::unique_ptr<Endpoint> open(Pattern pattern, Role role, const Params& p)
{
    set_log_level_from_env_or(LogLevel::Error);
    auto endpoint = std::make_unique<Iceoryx2Endpoint>(p.size);
    std::string forward = p.name + "-ab", back = p.name + "-ba";
    uint64_t subscribers = pattern == Pattern::broadcast ? static_cast<uint64_t>(p.subscribers) : 1;
    switch (role)
    {
        case Role::tx:
        case Role::pub:
            endpoint->open_publisher(forward, p, subscribers);
            break;
        case Role::rx:
        case Role::sub:
            endpoint->open_subscriber(forward, p, subscribers);
            break;
        case Role::ping:
            endpoint->open_subscriber(back, p, 1);
            endpoint->open_publisher(forward, p, 1);
            break;
        case Role::echo:
            endpoint->open_subscriber(forward, p, 1);
            endpoint->open_publisher(back, p, 1);
            break;
        case Role::client:
        {
            endpoint->reqres.emplace(make_reqres(endpoint->node, p.name, p));
            auto result = endpoint->reqres->client_builder().initial_max_slice_len(p.size).create();
            if (!result.has_value())
                fail("iceoryx2: client create");
            endpoint->client.emplace(std::move(result.value()));
            while (endpoint->reqres->dynamic_config().number_of_servers() < 1)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            break;
        }
        case Role::server:
        {
            endpoint->reqres.emplace(make_reqres(endpoint->node, p.name, p));
            auto result = endpoint->reqres->server_builder().initial_max_slice_len(p.size).create();
            if (!result.has_value())
                fail("iceoryx2: server create");
            endpoint->server.emplace(std::move(result.value()));
            break;
        }
        default:
            fail("iceoryx2: no such role");
    }
    return endpoint;
}

}  // namespace

extern const Lib lib_iceoryx2{"iceoryx2", supports, open};

}  // namespace alt
