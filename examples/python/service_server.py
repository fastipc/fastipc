# service_server.py
import fipc

# 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
# and is its server
with fipc.service_create("my_service", 4096, 256, 1, 1):
    with fipc.service_join("my_service", fipc.SERVER, timeout_ms=5000) as server:
        for _ in range(3):  # three requests, then the end
            request = server.recv()  # the next request of any client
            server.send(request.upper())  # the reply goes to its client, and never waits
# closing the service ends it: the client still receives the replies sent
