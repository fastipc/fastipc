# service_client.py
import fipc

with fipc.service_join("my_service", fipc.CLIENT, timeout_ms=5000) as client:
    for i in (1, 2, 3):
        # sends the request and returns its reply; FipcError (ABANDONED) if the server ended before it replied
        reply = client.call(f"ping {i}".encode())
        print(reply.decode())  # PING 1, PING 2, PING 3
