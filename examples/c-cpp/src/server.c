/* server.c */
#include <ctype.h>
#include <fipc.h>

int main(void)
{
    fipc_listener_t* listener;
    if (fipc_stream_create("my_channel", 1 << 20, &listener) != FIPC_OK) /* rings of 1 MiB each way */
        return 1;
    fipc_conn_t* conn;
    if (fipc_stream_accept(listener, &conn, FIPC_FOREVER) == FIPC_OK) /* waits for a client */
    {
        char buf[256];
        size_t len;
        if (fipc_recv(conn, buf, sizeof buf, &len, FIPC_FOREVER) == FIPC_OK)
        {
            for (size_t i = 0; i < len; i++)
                buf[i] = (char) toupper((unsigned char) buf[i]);
            fipc_send(conn, buf, len, FIPC_FOREVER);
        }
        fipc_close(conn);
    }
    fipc_listener_close(listener);
    return 0;
}
