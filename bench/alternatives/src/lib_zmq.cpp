// ZeroMQ (libzmq) over its ipc:// transport (a Unix domain socket; on Windows, AF_UNIX too), or over TCP on loopback
// (zeromq-tcp), default options: one I/O thread, high-water marks of 1000 messages (with --buffer, its bytes of
// messages, at least 8: ZMQ_SNDHWM and ZMQ_RCVHWM).
//   stream     PAIR to PAIR
//   broadcast  PUB to SUBs (lossy: a PUB drops messages for a subscriber at its high-water mark)
//   queue      PUSH to PULL: each consumer binds, each producer connects to every consumer and deals to them in turn
//   service    REQ to REP: each server binds, each client connects to every server and deals its requests to them
#include <zmq.h>

#include <string>
#include <vector>

#include "harness.hpp"

namespace alt
{
namespace
{

constexpr int wait_ms = 100;

void* context()
{
    static void* const instance = zmq_ctx_new();
    return instance;
}

// ipc:// in the run's directory, or TCP over loopback (zeromq-tcp): one port per bound socket, from the run's port.
std::string endpoint_name(bool tcp, const Params& p, const std::string& suffix, int offset = 0)
{
    if (tcp)
        return "tcp://127.0.0.1:" + std::to_string(p.port + offset);
    return "ipc://" + p.dir + "/zmq-" + suffix;
}

struct ZmqEndpoint : Endpoint
{
    void* socket;
    std::vector<uint8_t> buffer;

    ZmqEndpoint(int type, const Params& p) : socket(zmq_socket(context(), type)), buffer(p.size)
    {
        if (!socket)
            fail("zmq_socket", zmq_strerror(zmq_errno()));
        int timeout = wait_ms;
        zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof timeout);
        if (p.buffer)
        {
            // Before bind or connect: a connection takes the marks its socket has then
            int depth = static_cast<int>(depth_for(p));
            zmq_setsockopt(socket, ZMQ_SNDHWM, &depth, sizeof depth);
            zmq_setsockopt(socket, ZMQ_RCVHWM, &depth, sizeof depth);
        }
    }

    ~ZmqEndpoint() override
    {
        int linger = 0;
        zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof linger);
        zmq_close(socket);
    }

    void bind(const std::string& address)
    {
        if (zmq_bind(socket, address.c_str()) != 0)
            fail("zmq_bind", address + ": " + zmq_strerror(zmq_errno()));
    }

    void connect(const std::string& address)
    {
        if (zmq_connect(socket, address.c_str()) != 0)
            fail("zmq_connect", address + ": " + zmq_strerror(zmq_errno()));
    }

    void send(const uint8_t* data, size_t len) override
    {
        while (zmq_send(socket, data, len, 0) < 0)
        {
            if (zmq_errno() != EINTR)
                fail("zmq_send", zmq_strerror(zmq_errno()));
        }
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        int len = zmq_recv(socket, buffer.data(), buffer.size(), timeout_ms == 0 ? ZMQ_DONTWAIT : 0);
        if (len < 0)
        {
            if (zmq_errno() == EAGAIN || zmq_errno() == EINTR)
                return 0;
            fail("zmq_recv", zmq_strerror(zmq_errno()));
        }
        sink.on(buffer.data(), static_cast<size_t>(len));
        return 1;
    }
};

bool supports(Pattern pattern, const Params& p)
{
    return pattern != Pattern::broadcast || p.lossy;
}

std::unique_ptr<Endpoint> open_with(bool tcp, Pattern pattern, Role role, const Params& p)
{
    switch (pattern)
    {
        case Pattern::stream:
        {
            auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_PAIR, p);
            if (role == Role::rx || role == Role::echo)
                endpoint->bind(endpoint_name(tcp, p, p.name));
            else
                endpoint->connect(endpoint_name(tcp, p, p.name));
            return endpoint;
        }
        case Pattern::broadcast:
        {
            if (role == Role::pub)
            {
                auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_PUB, p);
                endpoint->bind(endpoint_name(tcp, p, p.name));
                return endpoint;
            }
            auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_SUB, p);
            zmq_setsockopt(endpoint->socket, ZMQ_SUBSCRIBE, "", 0);
            endpoint->connect(endpoint_name(tcp, p, p.name));
            return endpoint;
        }
        case Pattern::queue:
        {
            if (role == Role::consumer)
            {
                auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_PULL, p);
                endpoint->bind(endpoint_name(tcp, p, p.name + "-c" + std::to_string(p.index), p.index));
                return endpoint;
            }
            auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_PUSH, p);
            for (int c = 0; c < p.consumers; c++)
                endpoint->connect(endpoint_name(tcp, p, p.name + "-c" + std::to_string(c), c));
            return endpoint;
        }
        case Pattern::service:
        {
            if (role == Role::server)
            {
                auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_REP, p);
                endpoint->bind(endpoint_name(tcp, p, p.name + "-s" + std::to_string(p.index), p.index));
                return endpoint;
            }
            auto endpoint = std::make_unique<ZmqEndpoint>(ZMQ_REQ, p);
            for (int s = 0; s < p.servers; s++)
                endpoint->connect(endpoint_name(tcp, p, p.name + "-s" + std::to_string(s), s));
            return endpoint;
        }
    }
    return nullptr;
}

std::unique_ptr<Endpoint> open_ipc(Pattern pattern, Role role, const Params& p)
{
    return open_with(false, pattern, role, p);
}

std::unique_ptr<Endpoint> open_tcp(Pattern pattern, Role role, const Params& p)
{
    return open_with(true, pattern, role, p);
}

}  // namespace

extern const Lib lib_zmq{"zeromq", supports, open_ipc};
extern const Lib lib_zmq_tcp{"zeromq-tcp", supports, open_tcp};

}  // namespace alt
