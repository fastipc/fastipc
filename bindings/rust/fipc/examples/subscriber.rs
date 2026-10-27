//! The subscriber of the broadcast example: joins `my_broadcast` gated and prints `tick 1` to `tick 3` from `publisher`
//! (or any other language's publisher), until the broadcast ends.

// subscriber.rs
use fipc::{Error, SubscriberRole};
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // gated: misses nothing (the publisher waits for it)
    let mut subscriber = fipc::broadcast::join("my_broadcast", SubscriberRole::Gated, Duration::from_secs(5))?;
    loop {
        match subscriber.receive(None) {
            Ok(tick) => println!("{}", String::from_utf8_lossy(&tick)), // tick 1, tick 2, tick 3
            Err(Error::Disconnected) => return Ok(()),                  // the publisher closed
            Err(e) => return Err(e),
        }
    }
}
