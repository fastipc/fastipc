# client.py
import fipc

with fipc.stream_connect("my_channel", timeout_ms=5000) as conn:
    conn.send(b"ping")
    print(conn.recv())  # b'PING'
