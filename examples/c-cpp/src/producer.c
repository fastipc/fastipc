/* producer.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_queue_t* queue;
    fipc_conn_t* producer = NULL;
    /* 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer */
    if (fipc_queue_create("my_queue", 4096, 1, 1, &queue) != FIPC_OK)
        return 1;
    fipc_result_t result = fipc_queue_join("my_queue", FIPC_PRODUCER, &producer, 5000);
    for (int i = 1; i <= 3 && result == FIPC_OK; i++)
    {
        char job[16];
        int len = snprintf(job, sizeof job, "job %d", i);
        result = fipc_send(producer, job, (size_t) len, FIPC_FOREVER); /* waits in the queue for a consumer */
    }
    uint32_t count;
    /* waits for the consumer to join */
    if (result == FIPC_OK)
        result = fipc_queue_wait_for_members(queue, FIPC_CONSUMER, 1, &count, FIPC_FOREVER);
    fipc_close(producer);
    fipc_queue_close(queue); /* ends the queue: the consumer gets FIPC_DISCONNECTED after the jobs */
    return result == FIPC_OK ? 0 : 1;
}
