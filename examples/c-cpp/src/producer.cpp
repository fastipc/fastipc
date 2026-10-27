// producer.cpp
#include <chrono>
#include <string>

#include <fipc.hpp>

int main()
{
    // 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
    auto queue = fipc::queue_create("my_queue", 4096, 1, 1);
    if (!queue)
        return 1;
    auto producer = fipc::queue_join("my_queue", fipc::queue_role::producer, std::chrono::seconds(5));
    if (!producer)
        return 1;
    for (int i = 1; i <= 3; i++)
        (void) producer->send("job " + std::to_string(i));          // waits in the queue until a consumer takes it
    (void) queue->wait_for_members(fipc::queue_role::consumer, 1);  // waits for the consumer to join
    producer->close();
    return 0;  // closing the queue ends it: the consumer gets FIPC_DISCONNECTED after the jobs
}
