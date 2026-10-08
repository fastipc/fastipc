/* client.c */
#include <fipc.h>
#include <stdio.h>

int main(void)
{
    fipc_conn_t* conn;
    fipc_result_t result = fipc_connect("my_channel", &conn, 5000);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "connect: %s\n", fipc_result_str(result));
        return 1;
    }
    uint64_t id;
    char buf[256];
    fipc_rpc_msg_t response;
    if (fipc_rpc_submit(conn, 1, "ping", 4, &id, FIPC_FOREVER) == FIPC_OK /* opcode 1 */
        && fipc_rpc_recv(conn, buf, sizeof buf, &response, FIPC_FOREVER) == FIPC_OK)
        printf("%.*s\n", (int) response.len, buf); /* PING */
    fipc_close(conn);
    return 0;
}
