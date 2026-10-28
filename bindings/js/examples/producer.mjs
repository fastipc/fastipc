// producer.mjs
import { CONSUMER, PRODUCER, createQueue, joinQueue } from "fipc-js";

// 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
const queue = createQueue("my_queue", 4096, 1, 1);
const producer = await joinQueue("my_queue", PRODUCER); // this process admits it, as any member
for (let i = 1; i <= 3; i++) await producer.send(`job ${i}`); // waits in the queue until a consumer takes it
await queue.waitForMembers(CONSUMER, 1); // waits for the consumer to join; the event loop runs meanwhile
producer.close();
queue.close(); // ends the queue: the consumer gets FIPC_DISCONNECTED after the jobs
