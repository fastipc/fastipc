//! The server of the repository README's example: answers one RPC request with its payload in upper case. Its client
//! is `client` here, or any other language's client.

// server.rs
use fipc::Listener;

fn main() -> fipc::Result<()> {
    let mut listener = Listener::listen("my_channel", 1 << 20)?; // rings of 1 MiB each way
    let mut conn = listener.accept(None)?; // waits for a client
    let request = conn.rpc_receive(None)?;
    conn.rpc_respond(request.id, request.opcode, 0, &request.payload.to_ascii_uppercase(), None)?;
    Ok(())
}
