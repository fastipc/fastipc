// consumer.mjs
import { CONSUMER, joinQueue } from "fipc-js";

const consumer = await joinQueue("my_queue", CONSUMER, 5000);
for (;;) {
  let job;
  try {
    job = await consumer.receive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break; // the queue ended
    throw error;
  }
  console.log(job.toString()); // job 1, job 2, job 3
}
consumer.close();
