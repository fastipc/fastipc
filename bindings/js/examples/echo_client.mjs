// echo_client.mjs
import { connectStream } from "fipc-js";

const conn = await connectStream("demo", 5000);
for (const word of ["hello", "shared", "memory"]) {
  await conn.send(word);
  console.log((await conn.receive()).toString());
}
conn.close();
