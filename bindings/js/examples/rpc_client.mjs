// rpc_client.mjs
import { Connection, RPC_RESPONSE } from "fipc";

const UPPER = 1;

const conn = await Connection.connect("my_channel", 5000);
const words = ["ping", "shared", "memory"];
// three requests in flight at once; the server answers them in order
const ids = await Promise.all(words.map((word) => conn.rpcSubmit(UPPER, word)));
for (const id of ids) {
  const reply = await conn.rpcReceive();
  console.log(reply.kind === RPC_RESPONSE && reply.id === id, reply.payload.toString()); // true PING, ...
}
conn.close();
