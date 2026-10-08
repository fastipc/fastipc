//! One UPPER request to `rpc_server` (or an RPC server in any other language).

// rpc_client.rs
use fipc::{Connection, RpcKind};
use std::time::Duration;

const UPPER: u32 = 1;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("my_channel", Duration::from_secs(5))?;
    let id = conn.rpc_submit(UPPER, b"ping", None)?;
    let reply = conn.rpc_receive(None)?;
    assert!(reply.kind == RpcKind::Response && reply.id == id);
    println!("{} {}", reply.status, String::from_utf8_lossy(&reply.payload)); // 0 PING
    Ok(())
}
