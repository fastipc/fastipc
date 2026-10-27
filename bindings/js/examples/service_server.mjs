// service_server.mjs
import { SERVER, createService, joinService } from "fipc";

// 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
// and is its server
const service = createService("my_service", 4096, 256, 1, 1);
const server = await joinService("my_service", SERVER, 5000); // this process admits it, as any member
for (let i = 0; i < 3; i++) { // three requests, then the end
  const request = await server.receive(); // the next request of any client
  await server.send(request.toString().toUpperCase()); // the reply goes to its client, and never waits
}
server.close();
service.close(); // ends the service: the client still receives the replies sent
