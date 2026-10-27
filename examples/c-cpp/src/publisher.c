/* publisher.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* publisher;
    /* 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped */
    if (fipc_broadcast_create("my_broadcast", 1 << 16, 8, 5000, &publisher) != FIPC_OK)
        return 1;
    uint32_t count;
    /* waits for the first subscriber */
    if (fipc_broadcast_wait_for_members(publisher, 1, &count, FIPC_FOREVER) == FIPC_OK)
    {
        for (int i = 1; i <= 3; i++)
        {
            char tick[16];
            int len = snprintf(tick, sizeof tick, "tick %d", i);
            fipc_send(publisher, tick, (size_t) len, FIPC_FOREVER);
        }
    }
    fipc_close(publisher); /* ends the broadcast: subscribers get FIPC_DISCONNECTED after the ticks */
    return 0;
}
