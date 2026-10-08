# client.py
from fipc import Conn

with Conn.connect("my_channel", timeout_ms=5000) as conn:
    conn.rpc_submit(1, b"ping")     # opcode 1
    print(conn.rpc_recv().payload)  # b'PING'
