// server.mjs
import { Listener } from "fipc";

const listener = Listener.listen("my_channel", 1 << 20); // rings of 1 MiB each way
const conn = await listener.accept(); // waits for a client; the event loop runs meanwhile
const request = await conn.rpcReceive();
await conn.rpcRespond(request.id, request.opcode, 0, request.payload.toString().toUpperCase());
conn.close();
listener.close();
