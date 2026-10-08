//! An RPC server: answers each UPPER request with its payload in upper case, until its client's end. Its client is
//! `rpc_client` here, or a client in any other language.

// rpc_server.rs
use fipc::{Error, Listener, RpcKind};

const UPPER: u32 = 1;

fn main() -> fipc::Result<()> {
    let mut listener = Listener::listen("my_channel", 1 << 20)?;
    let mut conn = listener.accept(None)?;
    loop {
        let req = match conn.rpc_receive(None) {
            Ok(req) => req,
            Err(Error::Disconnected) => return Ok(()),
            Err(e) => return Err(e),
        };
        if req.kind == RpcKind::Request && req.opcode == UPPER {
            conn.rpc_respond(req.id, req.opcode, 0, &req.payload.to_ascii_uppercase(), None)?;
        }
    }
}
