// Server.java
import io.github.fastipc.Connection;
import io.github.fastipc.Listener;
import io.github.fastipc.RpcMessage;

public class Server {
    public static void main(String[] args) {
        // rings of 1 MiB each way; accept() waits for a client
        try (Listener listener =
                 Listener.listen("my_channel", 1 << 20);
             Connection conn = listener.accept()) {
            RpcMessage request = conn.rpcReceive();
            String text = new String(request.payload());
            conn.rpcRespond(request.id(), request.opcode(), 0,
                            text.toUpperCase().getBytes());
        }
    }
}
