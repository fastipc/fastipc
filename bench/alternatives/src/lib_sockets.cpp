// The operating system's sockets, as a baseline: a Unix domain stream socket (on Windows too, AF_UNIX) and TCP over
// loopback (TCP_NODELAY). A message is framed by a 4-byte length; a send writes the frame with one call, a receive
// reads whatever is there (up to 256 KiB) and hands out the messages it holds. A stream only.
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "harness.hpp"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <winsock2.h>
// After winsock2.h, which declares what they use
#include <afunix.h>

#include <ws2tcpip.h>
using socket_t = SOCKET;
#define CLOSE_SOCKET closesocket
#define POLL WSAPoll
#define WOULD_BLOCK() (WSAGetLastError() == WSAEWOULDBLOCK)
#else
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <cerrno>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
using socket_t = int;
#define INVALID_SOCKET (-1)
#define CLOSE_SOCKET close
#define POLL poll
#define WOULD_BLOCK() (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
#endif

namespace alt
{
namespace
{

constexpr size_t read_size = 256 * 1024;

void startup()
{
#if defined(_WIN32)
    static bool started = []
    {
        WSADATA data;
        return WSAStartup(MAKEWORD(2, 2), &data) == 0;
    }();
    if (!started)
        fail("WSAStartup");
#endif
}

void set_nonblocking(socket_t s)
{
#if defined(_WIN32)
    u_long on = 1;
    ioctlsocket(s, FIONBIO, &on);
#else
    fcntl(s, F_SETFL, fcntl(s, F_GETFL) | O_NONBLOCK);
#endif
}

struct SocketEndpoint : Endpoint
{
    socket_t s = INVALID_SOCKET;
    std::vector<uint8_t> frame, in;
    size_t begin = 0, end = 0;

    explicit SocketEndpoint(size_t size) : frame(size + 4), in(read_size + size + 4)
    {
    }

    ~SocketEndpoint() override
    {
        if (s != INVALID_SOCKET)
            CLOSE_SOCKET(s);
    }

    void wait(short events, int timeout_ms)
    {
        pollfd fd{};
        fd.fd = s;
        fd.events = events;
        POLL(&fd, 1, timeout_ms);
    }

    void send(const uint8_t* data, size_t len) override
    {
        uint32_t length = static_cast<uint32_t>(len);
        std::memcpy(frame.data(), &length, 4);
        std::memcpy(frame.data() + 4, data, len);
        size_t sent = 0, total = len + 4;
        while (sent < total)
        {
            auto n = ::send(s, reinterpret_cast<const char*>(frame.data()) + sent, static_cast<int>(total - sent), 0);
            if (n > 0)
            {
                sent += static_cast<size_t>(n);
            }
            else if (n < 0 && WOULD_BLOCK())
            {
                wait(POLLOUT, 100);
            }
            else
            {
                fail("send");
            }
        }
    }

    // Reads more into `in`, waiting up to timeout_ms; false if nothing came.
    bool fill(int timeout_ms)
    {
        if (begin > 0)
        {
            std::memmove(in.data(), in.data() + begin, end - begin);
            end -= begin;
            begin = 0;
        }
        for (int attempt = 0; attempt < 2; attempt++)
        {
            auto n = ::recv(s, reinterpret_cast<char*>(in.data()) + end, static_cast<int>(in.size() - end), 0);
            if (n > 0)
            {
                end += static_cast<size_t>(n);
                return true;
            }
            if (n == 0)
                fail("recv: the peer closed");
            if (!WOULD_BLOCK())
                fail("recv");
            if (timeout_ms == 0 || attempt == 1)
                return false;
            wait(POLLIN, timeout_ms);
        }
        return false;
    }

    int recv(Sink& sink, int timeout_ms) override
    {
        int delivered = 0;
        for (;;)
        {
            while (end - begin >= 4)
            {
                uint32_t length;
                std::memcpy(&length, in.data() + begin, 4);
                if (end - begin < 4 + length)
                    break;
                sink.on(in.data() + begin + 4, length);
                begin += 4 + length;
                delivered++;
            }
            if (delivered > 0)
                return delivered;
            if (!fill(timeout_ms))
                return 0;
        }
    }
};

struct Address
{
    sockaddr_storage storage{};
    socklen_t length = 0;
    int family = 0;
};

Address address_of(bool tcp, const Params& p)
{
    Address a;
    if (tcp)
    {
        auto* in = reinterpret_cast<sockaddr_in*>(&a.storage);
        in->sin_family = AF_INET;
        in->sin_port = htons(static_cast<uint16_t>(p.port));
        in->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.length = sizeof(sockaddr_in);
        a.family = AF_INET;
    }
    else
    {
        auto* un = reinterpret_cast<sockaddr_un*>(&a.storage);
        un->sun_family = AF_UNIX;
        std::string path = p.dir + "/uds-" + p.name;
        if (path.size() >= sizeof un->sun_path)
            fail("a socket path too long", path);
        std::memcpy(un->sun_path, path.c_str(), path.size() + 1);
        a.length = sizeof(sockaddr_un);
        a.family = AF_UNIX;
    }
    return a;
}

std::unique_ptr<Endpoint> open_socket(bool tcp, Role role, const Params& p)
{
    startup();
    auto endpoint = std::make_unique<SocketEndpoint>(p.size);
    Address a = address_of(tcp, p);
    if (role == Role::rx || role == Role::echo)
    {
        socket_t listener = socket(a.family, SOCK_STREAM, 0);
        if (listener == INVALID_SOCKET)
            fail("socket");
        if (tcp)
        {
            int on = 1;
            setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&on), sizeof on);
        }
        if (bind(listener, reinterpret_cast<sockaddr*>(&a.storage), a.length) != 0)
            fail("bind");
        if (listen(listener, 1) != 0)
            fail("listen");
        endpoint->s = accept(listener, nullptr, nullptr);
        CLOSE_SOCKET(listener);
        if (endpoint->s == INVALID_SOCKET)
            fail("accept");
    }
    else
    {
        for (;;)
        {
            endpoint->s = socket(a.family, SOCK_STREAM, 0);
            if (connect(endpoint->s, reinterpret_cast<sockaddr*>(&a.storage), a.length) == 0)
                break;
            CLOSE_SOCKET(endpoint->s);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    if (tcp)
    {
        int on = 1;
        setsockopt(endpoint->s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&on), sizeof on);
    }
    set_nonblocking(endpoint->s);
    return endpoint;
}

bool supports(Pattern pattern, const Params&)
{
    return pattern == Pattern::stream;
}

std::unique_ptr<Endpoint> open_uds(Pattern, Role role, const Params& p)
{
    return open_socket(false, role, p);
}

std::unique_ptr<Endpoint> open_tcp(Pattern, Role role, const Params& p)
{
    return open_socket(true, role, p);
}

}  // namespace

extern const Lib lib_uds{"unix-socket", supports, open_uds};
extern const Lib lib_tcp{"tcp-loopback", supports, open_tcp};

}  // namespace alt
