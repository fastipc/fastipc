// Producer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.QueueRole;
import io.github.fastipc.WorkQueue;

public class Producer {
    public static void main(String[] args) {
        // 4 KiB of messages per producer, 1 producer and 1
        // consumer: this process owns the queue and is its
        // producer
        try (WorkQueue queue = Fipc.createQueue("my_queue", 4096,
                 1, 1)) {
            try (Connection producer = Fipc.joinQueue("my_queue",
                     QueueRole.PRODUCER)) {
                for (int i = 1; i <= 3; i++) {
                    // waits in the queue until a consumer takes it
                    producer.send(("job " + i).getBytes());
                }
                // waits for the consumer to join
                queue.waitForMembers(QueueRole.CONSUMER, 1);
            }
        }
        // closing the queue ends it: the consumer gets
        // DISCONNECTED after the jobs
    }
}
