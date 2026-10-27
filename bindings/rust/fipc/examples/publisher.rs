//! The publisher of the broadcast example: waits for a subscriber, sends `tick 1` to `tick 3` on the broadcast
//! `my_broadcast`, then ends the broadcast. Its subscriber is `subscriber` here, or any other language's subscriber.

// publisher.rs
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
    let mut publisher = fipc::broadcast::create("my_broadcast", 1 << 16, 8, Duration::from_secs(5))?;
    publisher.wait_for_members(1, None)?; // waits for the first subscriber
    for i in 1..=3 {
        publisher.send(format!("tick {i}").as_bytes(), None)?;
    }
    Ok(())
} // dropping the publisher ends the broadcast: subscribers get Error::Disconnected after the ticks
