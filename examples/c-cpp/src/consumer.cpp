// consumer.cpp
#include <chrono>
#include <cstdio>
#include <string>

#include <fipc.hpp>

int main()
{
    auto consumer = fipc::queue_join("my_queue", fipc::queue_role::consumer, std::chrono::seconds(5));
    if (!consumer)
    {
        std::fprintf(stderr, "join: %s\n", consumer.error().name());
        return 1;
    }
    std::string job;
    for (;;)
    {
        auto received = consumer->receive(job);
        if (!received)  // errc::disconnected: the queue ended
            return received.error() == fipc::errc::disconnected ? 0 : 1;
        std::printf("%s\n", job.c_str());  // job 1, job 2, job 3
    }
}
