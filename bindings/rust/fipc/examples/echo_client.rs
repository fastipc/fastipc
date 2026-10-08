//! Sends three words to `echo_server` and prints what comes back.

// echo_client.rs
use fipc::Connection;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("demo", Duration::from_secs(5))?;
    for word in ["hello", "shared", "memory"] {
        conn.send(word.as_bytes(), None)?;
        println!("{}", String::from_utf8_lossy(&conn.receive(None)?));
    }
    Ok(())
}
