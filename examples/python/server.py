# server.py
from fipc import Listener

# rings of 1 MiB each way
with Listener("my_channel", 1 << 20) as listener:
    with listener.accept() as conn:  # waits for a client
        request = conn.rpc_recv()
        conn.rpc_respond(request.id, request.opcode,
                         data=request.payload.upper())
