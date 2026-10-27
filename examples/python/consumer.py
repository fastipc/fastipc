# consumer.py
import fipc

with fipc.queue_join("my_queue", fipc.CONSUMER, timeout_ms=5000) as consumer:
    try:
        while True:
            print(consumer.recv().decode())  # job 1, job 2, job 3
    except fipc.FipcError as e:
        if e.result != fipc.Result.DISCONNECTED:  # the queue ended
            raise
