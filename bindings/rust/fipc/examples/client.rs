//! The client of the repository README's example: one RPC request to `server` (or any other language's server).

// client.rs
use fipc::Connection;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("my_channel", Duration::from_secs(5))?;
    conn.rpc_submit(1, b"ping", None)?; // opcode 1
    println!("{}", String::from_utf8_lossy(&conn.rpc_receive(None)?.payload)); // PING
    Ok(())
}
