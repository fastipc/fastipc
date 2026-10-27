//! The server of the service example: owns the service `my_service` and is its server, answers three requests, each
//! with its uppercase, then ends the service. Its client is `service_client` here, or any other language's service
//! client.

// service_server.rs
use fipc::ServiceRole;
use std::time::Duration;

fn main() -> fipc::Result<()> {
    // 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the
    // service and is its server
    let service = fipc::service::create("my_service", 4096, 256, 1, 1)?;
    let mut server = fipc::service::join("my_service", ServiceRole::Server, Duration::from_secs(5))?;
    for _ in 0..3 {
        // three requests, then the end
        let request = server.receive(None)?; // the next request of any client
        server.send(&request.to_ascii_uppercase(), None)?; // the reply goes to its client, and never waits
    }
    service.close(); // ends the service: the client still receives the replies sent
    Ok(())
}
