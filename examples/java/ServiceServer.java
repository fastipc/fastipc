// ServiceServer.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.Service;
import io.github.fastipc.ServiceRole;
import java.time.Duration;

public class ServiceServer {
    public static void main(String[] args) {
        // 4 KiB per client, requests and replies of up to 256
        // bytes, 1 client and 1 server: this process owns the
        // service and is its server
        try (Service service = Fipc.createService("my_service",
                 4096, 256, 1, 1)) {
            try (Connection server = Fipc.joinService("my_service",
                     ServiceRole.SERVER, Duration.ofSeconds(5))) {
                // three requests, then the end
                for (int i = 1; i <= 3; i++) {
                    // the next request of any client
                    String request = new String(server.receive());
                    // the reply goes to its client, and never waits
                    server.send(request.toUpperCase().getBytes());
                }
            }
        }
        // closing the service ends it: the client still
        // receives the replies sent
    }
}
