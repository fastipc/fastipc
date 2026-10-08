//! Echoes every message back until its client's end. Its client is `echo_client` or `game_loop` here, or a client in
//! any other language.

// echo_server.rs
use fipc::{Error, Listener};

fn main() -> fipc::Result<()> {
    // rings of 1 MiB each way: a power of two, 1 KiB to 2 GiB
    let mut listener = Listener::listen("demo", 1 << 20)?;
    let mut conn = listener.accept(None)?; // waits for a client
    loop {
        match conn.receive(None) {
            Ok(message) => conn.send(&message, None)?, // echo, any size
            Err(Error::Disconnected) => break,
            Err(e) => return Err(e),
        }
    }
    println!("client gone");
    Ok(())
}
