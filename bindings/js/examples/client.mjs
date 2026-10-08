// client.mjs
import { Connection } from "fipc";

const conn = await Connection.connect("my_channel", 5000); // waits up to 5 s for the server
await conn.rpcSubmit(1, "ping"); // opcode 1
console.log((await conn.rpcReceive()).payload.toString()); // PING
conn.close();
