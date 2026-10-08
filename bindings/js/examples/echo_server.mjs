// echo_server.mjs
import { Listener } from "fipc";

// rings of 1 MiB each way: a power of two, 1 KiB to 2 GiB
const listener = Listener.listen("demo", 1 << 20);
const conn = await listener.accept(); // waits for a client
for (;;) {
  let message;
  try {
    message = await conn.receive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break; // the client is gone
    throw error;
  }
  await conn.send(message); // echo, any size
}
console.log("client gone");
conn.close();
listener.close();
