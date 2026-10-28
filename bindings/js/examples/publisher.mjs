// publisher.mjs
import { createBroadcast } from "fipc-js";

// 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
const publisher = createBroadcast("my_broadcast", 1 << 16, 8, 5000);
await publisher.waitForMembers(1); // waits for the first subscriber; the event loop runs meanwhile
for (let i = 1; i <= 3; i++) await publisher.send(`tick ${i}`);
publisher.close(); // ends the broadcast: subscribers get FIPC_DISCONNECTED after the ticks
