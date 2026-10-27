# server.py
import fipc

# rings of 1 MiB each way
with fipc.stream_create("my_channel", 1 << 20) as listener:
    with listener.accept() as conn:  # waits for a client
        message = conn.recv()
        conn.send(message.upper())
