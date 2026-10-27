/* consumer.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* consumer;
    fipc_result_t result = fipc_queue_join("my_queue", FIPC_CONSUMER, &consumer, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "join: %s\n", fipc_result_str(result));
        return 1;
    }
    char buf[256];
    size_t len;
    while ((result = fipc_recv(consumer, buf, sizeof buf, &len, FIPC_FOREVER)) == FIPC_OK)
        printf("%.*s\n", (int) len, buf); /* job 1, job 2, job 3 */
    fipc_close(consumer);
    return result == FIPC_DISCONNECTED ? 0 : 1; /* the queue ended */
}
