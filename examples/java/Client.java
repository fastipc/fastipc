// Client.java
import io.github.fastipc.Connection;
import java.time.Duration;

public class Client {
    public static void main(String[] args) {
        try (Connection conn = Connection.connect("my_channel",
                 Duration.ofSeconds(5))) {
            conn.rpcSubmit(1, "ping".getBytes());  // opcode 1
            byte[] reply = conn.rpcReceive().payload();
            System.out.println(new String(reply));  // PING
        }
    }
}
