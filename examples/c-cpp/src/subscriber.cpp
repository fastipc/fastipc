// subscriber.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    // gated: misses nothing (the publisher waits for it)
    auto subscriber = fipc::broadcast_join("my_broadcast", fipc::subscriber_role::gated, std::chrono::seconds(5));
    if (!subscriber)
    {
        std::fprintf(stderr, "join: %s\n", subscriber.error().name());
        return 1;
    }
    std::string tick;
    for (;;)
    {
        auto received = subscriber->receive(tick);
        if (!received)  // errc::disconnected: the publisher closed
            return received.error() == fipc::errc::disconnected ? 0 : 1;
        std::printf("%s\n", tick.c_str());  // tick 1, tick 2, tick 3
    }
}
