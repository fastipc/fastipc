/* subscriber.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* subscriber;
    /* gated: misses nothing (the publisher waits for it) */
    fipc_result_t result = fipc_broadcast_join("my_broadcast", FIPC_SUBSCRIBER_GATED, &subscriber, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "join: %s\n", fipc_result_str(result));
        return 1;
    }
    char buf[256];
    size_t len;
    while ((result = fipc_recv(subscriber, buf, sizeof buf, &len, FIPC_FOREVER)) == FIPC_OK)
        printf("%.*s\n", (int) len, buf); /* tick 1, tick 2, tick 3 */
    fipc_close(subscriber);
    return result == FIPC_DISCONNECTED ? 0 : 1; /* the publisher closed */
}
