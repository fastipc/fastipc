//! The producer of the work queue example: owns the queue `my_queue` and is its producer, sends `job 1` to `job 3`,
//! waits for a consumer to join, then ends the queue. Its consumer is `consumer` here, or any other language's
//! consumer.

// producer.rs
use fipc::QueueRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
    let queue = fipc::queue::create("my_queue", 4096, 1, 1)?;
    let mut producer = fipc::queue::join("my_queue", QueueRole::Producer, Duration::from_secs(5))?;
    for i in 1..=3 {
        producer.send(format!("job {i}").as_bytes(), None)?; // waits in the queue until a consumer takes it
    }
    queue.wait_for_members(QueueRole::Consumer, 1, None)?; // waits for the consumer to join
    Ok(())
} // dropping the producer, then the queue, ends the queue: the consumer gets Error::Disconnected after the jobs
