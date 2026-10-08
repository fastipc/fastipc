// rpc_server.mjs
import { Listener, RPC_REQUEST } from "fipc";

const UPPER = 1;

const listener = Listener.listen("my_channel", 1 << 20);
const conn = await listener.accept();
for (;;) {
  let request;
  try {
    request = await conn.rpcReceive();
  } catch (error) {
    if (error.code === "FIPC_DISCONNECTED") break;
    throw error;
  }
  if (request.kind === RPC_REQUEST && request.opcode === UPPER) {
    await conn.rpcRespond(request.id, request.opcode, 0, request.payload.toString().toUpperCase());
  }
}
conn.close();
listener.close();
