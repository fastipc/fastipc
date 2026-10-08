//! A frame loop that never blocks on the connection: each frame sends its update and takes whatever has arrived,
//! without waiting. Run it against `echo_server`.

// game_loop.rs
use fipc::{Connection, Error, NO_WAIT};
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut conn = Connection::connect("demo", Duration::from_secs(5))?;
    let mut echoes = 0;
    for frame in 0..60u32 {
        conn.send(&frame.to_le_bytes(), NO_WAIT)?; // Error::Timeout only if the ring is full
        loop {
            match conn.receive(NO_WAIT) {
                Ok(_echo) => echoes += 1,
                Err(Error::Timeout) => break, // nothing more this frame
                Err(e) => return Err(e),      // Error::Disconnected: the peer is gone
            }
        }
        std::thread::sleep(Duration::from_millis(16)); // the rest of the frame
    }
    println!("{echoes} echoes in 60 frames");
    Ok(())
}
