// Publisher.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import java.time.Duration;

public class Publisher {
    public static void main(String[] args) {
        // 64 KiB of messages, up to 8 subscribers; one that
        // holds the publisher back 5 s is dropped
        try (Connection publisher = Fipc.createBroadcast(
                 "my_broadcast", 1 << 16, 8, Duration.ofSeconds(5))) {
            publisher.waitForMembers(1);  // waits for the first one
            for (int i = 1; i <= 3; i++) {
                publisher.send(("tick " + i).getBytes());
            }
        }
        // closing the publisher ends the broadcast: subscribers
        // get DISCONNECTED after the ticks
    }
}
