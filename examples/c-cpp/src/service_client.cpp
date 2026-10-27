// service_client.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    auto client = fipc::service_join("my_service", fipc::service_role::client, std::chrono::seconds(5));
    if (!client)
    {
        std::fprintf(stderr, "join: %s\n", client.error().name());
        return 1;
    }
    std::string reply;
    for (int i = 1; i <= 3; i++)
    {
        // sends the request and receives its reply; errc::abandoned if the server ended before it replied
        auto called = client->call("ping " + std::to_string(i), reply);
        if (!called)
        {
            std::fprintf(stderr, "call: %s\n", called.error().name());
            return 1;
        }
        std::printf("%s\n", reply.c_str());  // PING 1, PING 2, PING 3
    }
    return 0;
}
