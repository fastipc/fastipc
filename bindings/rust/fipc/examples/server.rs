//! The server of the repository README's example: answers one message with its uppercase. Its client is `client`
//! here, or any other language's client.

// server.rs
fn main() -> fipc::Result<()> {
    let mut listener = fipc::stream::create("my_channel", 1 << 20)?; // rings of 1 MiB each way
    let mut conn = listener.accept(None)?; // waits for a client
    let message = conn.receive(None)?;
    conn.send(&message.to_ascii_uppercase(), None)?;
    Ok(())
}
