// The ES module entry of fipc: the same binding as index.cjs, with named exports.
import fipc from './index.cjs';

export const {
  Listener,
  Connection,
  FipcError,
  Result,
  resultStr,
  NO_WAIT,
  FOREVER,
  RPC_REQUEST,
  RPC_RESPONSE,
  version,
  library,
  addon,
} = fipc;
export default fipc;
