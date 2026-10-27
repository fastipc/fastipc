//! The consumer of the work queue example: joins `my_queue` as a consumer and prints `job 1` to `job 3` from
//! `producer` (or any other language's producer), until the queue ends.

// consumer.rs
use fipc::{Error, QueueRole};
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut consumer = fipc::queue::join("my_queue", QueueRole::Consumer, Duration::from_secs(5))?;
    loop {
        match consumer.receive(None) {
            Ok(job) => println!("{}", String::from_utf8_lossy(&job)), // job 1, job 2, job 3
            Err(Error::Disconnected) => return Ok(()),                // the queue ended
            Err(e) => return Err(e),
        }
    }
}
