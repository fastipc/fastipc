/* service_server.c */
#include <ctype.h>
#include <fipc.h>

int main(void)
{
    fipc_service_t* service;
    fipc_conn_t* server = NULL;
    /* 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
       and is its server */
    if (fipc_service_create("my_service", 4096, 256, 1, 1, &service) != FIPC_OK)
        return 1;
    fipc_result_t result = fipc_service_join("my_service", FIPC_SERVER, &server, 5000);
    for (int i = 1; i <= 3 && result == FIPC_OK; i++) /* three requests, then the end */
    {
        char buf[256];
        size_t len;
        result = fipc_recv(server, buf, sizeof buf, &len, FIPC_FOREVER); /* the next request of any client */
        for (size_t j = 0; result == FIPC_OK && j < len; j++)
            buf[j] = (char) toupper((unsigned char) buf[j]);
        if (result == FIPC_OK)
            result = fipc_send(server, buf, len, FIPC_NO_WAIT); /* the reply goes to its client, and never waits */
    }
    fipc_close(server);
    fipc_service_close(service); /* ends the service: the client still receives the replies sent */
    return result == FIPC_OK ? 0 : 1;
}
