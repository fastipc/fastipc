// Server.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.Listener;

public class Server {
    public static void main(String[] args) {
        // rings of 1 MiB each way; accept() waits for a client
        try (Listener listener =
                 Fipc.createStream("my_channel", 1 << 20);
             Connection conn = listener.accept()) {
            String text = new String(conn.receive());
            conn.send(text.toUpperCase().getBytes());
        }
    }
}
