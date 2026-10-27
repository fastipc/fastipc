// Consumer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.FipcException;
import io.github.fastipc.QueueRole;
import io.github.fastipc.Result;
import java.time.Duration;

public class Consumer {
    public static void main(String[] args) {
        try (Connection consumer = Fipc.joinQueue("my_queue",
                 QueueRole.CONSUMER, Duration.ofSeconds(5))) {
            while (true) {
                byte[] job = consumer.receive();
                System.out.println(new String(job));  // job 1..3
            }
        } catch (FipcException e) {
            // DISCONNECTED: the queue ended
            if (e.result() != Result.DISCONNECTED) throw e;
        }
    }
}
