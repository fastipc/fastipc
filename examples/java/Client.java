// Client.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import java.time.Duration;

public class Client {
    public static void main(String[] args) {
        try (Connection conn = Fipc.connectStream("my_channel",
                 Duration.ofSeconds(5))) {
            conn.send("ping".getBytes());
            byte[] reply = conn.receive();
            System.out.println(new String(reply));  // PING
        }
    }
}
