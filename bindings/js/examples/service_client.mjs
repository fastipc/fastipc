// service_client.mjs
import { CLIENT, joinService } from "fipc";

const client = await joinService("my_service", CLIENT, 5000);
for (let i = 1; i <= 3; i++) {
  // sends the request and resolves to its reply; FipcError FIPC_ABANDONED if the server ended before it replied
  const reply = await client.call(`ping ${i}`);
  console.log(reply.toString()); // PING 1, PING 2, PING 3
}
client.close();
