# subscriber.py
import fipc

# gated: misses nothing (the publisher waits for it)
with fipc.broadcast_join("my_broadcast", fipc.SUBSCRIBER_GATED, timeout_ms=5000) as subscriber:
    try:
        while True:
            print(subscriber.recv().decode())  # tick 1, tick 2, tick 3
    except fipc.FipcError as e:
        if e.result != fipc.Result.DISCONNECTED:  # the publisher closed
            raise
