/* service_client.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* client;
    fipc_result_t result = fipc_service_join("my_service", FIPC_CLIENT, &client, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "join: %s\n", fipc_result_str(result));
        return 1;
    }
    for (int i = 1; i <= 3 && result == FIPC_OK; i++)
    {
        char buf[256];
        size_t len = (size_t) snprintf(buf, sizeof buf, "ping %d", i);
        result = fipc_send(client, buf, len, FIPC_FOREVER); /* a request: a server takes it */
        if (result == FIPC_OK)
            result = fipc_recv(client, buf, sizeof buf, &len, FIPC_FOREVER); /* its reply */
        if (result == FIPC_OK)
            printf("%.*s\n", (int) len, buf); /* PING 1, PING 2, PING 3 */
    }
    if (result != FIPC_OK) /* FIPC_ABANDONED: the server ended before it replied */
        fprintf(stderr, "request: %s\n", fipc_result_str(result));
    fipc_close(client);
    return result == FIPC_OK ? 0 : 1;
}
