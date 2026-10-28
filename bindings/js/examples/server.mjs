// server.mjs
import { createStream } from "fipc-js";

const listener = createStream("my_channel", 1 << 20); // rings of 1 MiB each way
const conn = await listener.accept(); // waits for a client; the event loop runs meanwhile
const message = await conn.receive();
await conn.send(message.toString().toUpperCase());
conn.close();
listener.close();
