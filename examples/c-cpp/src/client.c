/* client.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* conn;
    fipc_result_t result = fipc_stream_connect("my_channel", &conn, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "connect: %s\n", fipc_result_str(result));
        return 1;
    }
    char buf[256];
    size_t len;
    if (fipc_send(conn, "ping", 4, FIPC_FOREVER) == FIPC_OK
        && fipc_recv(conn, buf, sizeof buf, &len, FIPC_FOREVER) == FIPC_OK)
        printf("%.*s\n", (int) len, buf); /* PING */
    fipc_close(conn);
    return 0;
}
