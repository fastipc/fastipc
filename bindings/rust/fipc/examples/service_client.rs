//! The client of the service example: joins `my_service` as a client and calls it three times, `ping 1` to `ping 3`,
//! printing each reply (`PING 1` to `PING 3`) from `service_server` (or any other language's service server). Any
//! error, `Error::Abandoned` included, ends it with exit code 1.

// service_client.rs
use fipc::ServiceRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    let mut client = fipc::service::join("my_service", ServiceRole::Client, Duration::from_secs(5))?;
    for i in 1..=3 {
        // sends the request and returns its reply; Error::Abandoned if the server ended before it replied
        let reply = client.call(format!("ping {i}").as_bytes(), None)?;
        println!("{}", String::from_utf8_lossy(&reply)); // PING 1, PING 2, PING 3
    }
    Ok(())
}
