//! The client of the repository README's example: one message to `server` (or any other language's server).

// client.rs
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = fipc::stream::connect("my_channel", Duration::from_secs(5))?;
    conn.send(b"ping", None)?;
    println!("{}", String::from_utf8_lossy(&conn.receive(None)?)); // PING
    Ok(())
}
