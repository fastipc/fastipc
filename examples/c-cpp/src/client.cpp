// client.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    auto conn = fipc::connection::connect("my_channel", std::chrono::seconds(5));
    if (!conn)
    {
        std::fprintf(stderr, "connect: %s\n", conn.error().name());
        return 1;
    }
    std::string reply;
    if (!conn->rpc_submit(1, "ping") || !conn->rpc_receive(reply))  // opcode 1
        return 1;
    std::printf("%s\n", reply.c_str());  // PING
    return 0;
}
