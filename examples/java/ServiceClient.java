// ServiceClient.java
import io.github.fastipc.Connection;
import io.github.fastipc.Fipc;
import io.github.fastipc.ServiceRole;
import java.time.Duration;

public class ServiceClient {
    public static void main(String[] args) {
        try (Connection client = Fipc.joinService("my_service",
                 ServiceRole.CLIENT, Duration.ofSeconds(5))) {
            for (int i = 1; i <= 3; i++) {
                // sends the request and returns its reply; throws
                // FipcException (ABANDONED) if the server ended
                // before it replied
                byte[] reply = client.call(("ping " + i).getBytes());
                System.out.println(new String(reply));  // PING 1..3
            }
        }
    }
}
