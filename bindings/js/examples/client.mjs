// client.mjs
import { connectStream } from "fipc-js";

const conn = await connectStream("my_channel", 5000); // waits up to 5 s for the server
await conn.send("ping");
console.log((await conn.receive()).toString()); // PING
conn.close();
