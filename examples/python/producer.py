# producer.py
import fipc

# 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
with fipc.queue_create("my_queue", 4096, 1, 1) as queue:
    with fipc.queue_join("my_queue", fipc.PRODUCER) as producer:
        for i in (1, 2, 3):
            producer.send(f"job {i}".encode())  # waits in the queue until a consumer takes it
        queue.wait_for_members(fipc.CONSUMER, 1)  # waits for the consumer to join
# closing the queue ends it: the consumer gets DISCONNECTED after the jobs
