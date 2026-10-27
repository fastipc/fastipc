// Subscriber.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.FipcException;
import io.github.fastipc.Result;
import io.github.fastipc.SubscriberRole;
import java.time.Duration;

public class Subscriber {
    public static void main(String[] args) {
        // gated: misses nothing (the publisher waits for it)
        try (Connection sub = Fipc.joinBroadcast("my_broadcast",
                 SubscriberRole.GATED, Duration.ofSeconds(5))) {
            while (true) {
                byte[] tick = sub.receive();
                System.out.println(new String(tick));  // tick 1..3
            }
        } catch (FipcException e) {
            // DISCONNECTED: the publisher closed
            if (e.result() != Result.DISCONNECTED) throw e;
        }
    }
}
