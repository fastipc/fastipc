// subscriber.mjs
import { SUBSCRIBER_GATED, joinBroadcast } from "fipc";

// gated: misses nothing (the publisher waits for it)
const subscriber = await joinBroadcast("my_broadcast", SUBSCRIBER_GATED, 5000);
for (;;) {
  let tick;
  try {
    tick = await subscriber.receive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break; // the publisher closed
    throw error;
  }
  console.log(tick.toString()); // tick 1, tick 2, tick 3
}
subscriber.close();
