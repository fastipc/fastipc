// client.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    auto conn = fipc::stream_connect("my_channel", std::chrono::seconds(5));
    if (!conn)
    {
        std::fprintf(stderr, "connect: %s\n", conn.error().name());
        return 1;
    }
    std::string reply;
    if (!conn->send("ping") || !conn->receive(reply))
        return 1;
    std::printf("%s\n", reply.c_str());  // PING
    return 0;
}
