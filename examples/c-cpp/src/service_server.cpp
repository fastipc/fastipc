// service_server.cpp
#include <cctype>
#include <chrono>
#include <string>

#include <fipc.hpp>

int main()
{
    // 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
    // and is its server
    auto service = fipc::service_create("my_service", 4096, 256, 1, 1);
    if (!service)
        return 1;
    auto server = fipc::service_join("my_service", fipc::service_role::server, std::chrono::seconds(5));
    if (!server)
        return 1;
    std::string request;
    for (int i = 1; i <= 3; i++)  // three requests, then the end
    {
        if (!server->receive(request))  // the next request of any client
            return 1;
        for (char& c : request)
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        if (!server->send(request))  // the reply goes to its client, and never waits
            return 1;
    }
    server->close();
    return 0;  // destroying the service ends it: the client still receives the replies sent
}
