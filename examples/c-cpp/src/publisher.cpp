// publisher.cpp
#include <chrono>
#include <string>

#include <fipc.hpp>

int main()
{
    // 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
    auto publisher = fipc::broadcast_create("my_broadcast", 1 << 16, 8, std::chrono::seconds(5));
    if (!publisher)
        return 1;
    if (publisher->wait_for_members(1))  // waits for the first subscriber
    {
        for (int i = 1; i <= 3; i++)
            (void) publisher->send("tick " + std::to_string(i));
    }
    return 0;  // closing the publisher ends the broadcast: subscribers get FIPC_DISCONNECTED after the ticks
}
