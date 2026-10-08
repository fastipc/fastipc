// echo_client.mjs
import { Connection } from "fipc";

const conn = await Connection.connect("demo", 5000);
for (const word of ["hello", "shared", "memory"]) {
  await conn.send(word);
  console.log((await conn.receive()).toString());
}
conn.close();
