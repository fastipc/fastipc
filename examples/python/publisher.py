# publisher.py
import fipc

# 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
with fipc.broadcast_create("my_broadcast", 1 << 16, 8, 5000) as publisher:
    publisher.wait_for_members(1)  # waits for the first subscriber
    for i in (1, 2, 3):
        publisher.send(f"tick {i}".encode())
# closing the publisher ends the broadcast: subscribers get DISCONNECTED after the ticks
