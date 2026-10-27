// NNG (nanomsg-next-gen) over its ipc:// transport (a Unix domain socket; on Windows, winpipe://, a named pipe), or
// over TCP on loopback (nng-tcp). Its sockets buffer no message by default (a send then waits for the receiver to
// take it), so each gets send and receive buffers of 512 KiB of messages (8 to 1024 of them), as the other libraries'
// queues, or with --buffer its bytes of messages (8 to 8192, NNG's most: a larger count isn't run); otherwise default
// options.
//   stream     pair1 to pair1
//   broadcast  pub0 to sub0s (lossy: a pub0 drops messages for a subscriber whose queue is full)
//   queue      push0 to pull0: each consumer listens, each producer dials every consumer and deals to them
//   service    req0 to rep0: each server listens, each client dials every server and deals its requests to them
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <nng/nng.h>

#include "harness.hpp"

namespace alt
{
namespace
{

constexpr int wait_ms = 100;

void check(int result, const char* what)
{
    if (result != 0)
        fail(what, nng_strerror(static_cast<nng_err>(result)));
}

// ipc:// (Windows: winpipe://), or TCP over loopback (nng-tcp): one port per listener, from the run's port.
std::string url(bool tcp, const Params& p, const std::string& suffix, int offset = 0)
{
    if (tcp)
        return "tcp://127.0.0.1:" + std::to_string(p.port + offset);
#if defined(_WIN32)
    return "winpipe://nng-" + suffix;
#else
    return "ipc://" + p.dir + "/nng-" + suffix;
#endif
}

struct NngEndpoint : Endpoint
{
    nng_socket socket = NNG_SOCKET_INITIALIZER;
    std::vector<uint8_t> buffer;

    NngEndpoint(int (*open)(nng_socket*), const Params& p) : buffer(p.size)
    {
        static std::once_flag once;
        std::call_once(once, [] { check(nng_init(nullptr), "nng_init"); });
        check(open(&socket), "nng socket");
        check(nng_socket_set_ms(socket, NNG_OPT_RECVTIMEO, wait_ms), "nng recv-timeout");
        // Not every protocol has both buffers (a pub0 has no receive buffer, a pull0 no send buffer).
        int depth = static_cast<int>(depth_for(p));
        nng_socket_set_int(socket, NNG_OPT_SENDBUF, depth);
        nng_socket_set_int(socket, NNG_OPT_RECVBUF, depth);
    }

    ~NngEndpoint() override
    {
        nng_socket_close(socket);
    }

    void listen(const std::string& address)
    {
        check(nng_listen(socket, address.c_str(), nullptr, 0), "nng_listen");
    }

    // Dials until the listener is there (it may start later), so that "ready" means connected.
    void dial(const std::string& address)
    {
        for (;;)
        {
            int result = nng_dial(socket, address.c_str(), nullptr, 0);
            if (result == 0)
                return;
            if (result != NNG_ECONNREFUSED && result != NNG_ENOENT)
                check(result, "nng_dial");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    void send(const uint8_t* data, size_t len) override
    {
        check(nng_send(socket, const_cast<uint8_t*>(data), len, 0), "nng_send");
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        size_t len = buffer.size();
        int result = nng_recv(socket, buffer.data(), &len, timeout_ms == 0 ? NNG_FLAG_NONBLOCK : 0);
        if (result == NNG_ETIMEDOUT || result == NNG_EAGAIN)
            return 0;
        check(result, "nng_recv");
        sink.on(buffer.data(), len);
        return 1;
    }
};

// NNG's buffers hold at most 8192 messages.
constexpr size_t most_messages = 8192;

bool supports(Pattern pattern, const Params& p)
{
    return (pattern != Pattern::broadcast || p.lossy) && depth_for(p) <= most_messages;
}

std::unique_ptr<Endpoint> open_with(bool tcp, Pattern pattern, Role role, const Params& p)
{
    switch (pattern)
    {
        case Pattern::stream:
        {
            auto endpoint = std::make_unique<NngEndpoint>(nng_pair1_open, p);
            if (role == Role::rx || role == Role::echo)
                endpoint->listen(url(tcp, p, p.name));
            else
                endpoint->dial(url(tcp, p, p.name));
            return endpoint;
        }
        case Pattern::broadcast:
        {
            if (role == Role::pub)
            {
                auto endpoint = std::make_unique<NngEndpoint>(nng_pub0_open, p);
                endpoint->listen(url(tcp, p, p.name));
                return endpoint;
            }
            auto endpoint = std::make_unique<NngEndpoint>(nng_sub0_open, p);
            check(nng_sub0_socket_subscribe(endpoint->socket, "", 0), "nng_sub0_socket_subscribe");
            endpoint->dial(url(tcp, p, p.name));
            return endpoint;
        }
        case Pattern::queue:
        {
            if (role == Role::consumer)
            {
                auto endpoint = std::make_unique<NngEndpoint>(nng_pull0_open, p);
                endpoint->listen(url(tcp, p, p.name + "-c" + std::to_string(p.index), p.index));
                return endpoint;
            }
            auto endpoint = std::make_unique<NngEndpoint>(nng_push0_open, p);
            for (int c = 0; c < p.consumers; c++)
                endpoint->dial(url(tcp, p, p.name + "-c" + std::to_string(c), c));
            return endpoint;
        }
        case Pattern::service:
        {
            if (role == Role::server)
            {
                auto endpoint = std::make_unique<NngEndpoint>(nng_rep0_open, p);
                endpoint->listen(url(tcp, p, p.name + "-s" + std::to_string(p.index), p.index));
                return endpoint;
            }
            auto endpoint = std::make_unique<NngEndpoint>(nng_req0_open, p);
            for (int s = 0; s < p.servers; s++)
                endpoint->dial(url(tcp, p, p.name + "-s" + std::to_string(s), s));
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

extern const Lib lib_nng{"nng", supports, open_ipc};
extern const Lib lib_nng_tcp{"nng-tcp", supports, open_tcp};

}  // namespace alt
