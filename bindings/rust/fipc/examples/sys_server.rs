//! The server of fipc-sys's README, on the raw FFI: answers one message with its uppercase. Its client is
//! `sys_client` or `client` here, or a client in any other language.

// server.rs
use fipc_sys::*;
use std::ptr;

fn main() {
    let mut listener = ptr::null_mut();
    let mut conn = ptr::null_mut();
    let mut buf = [0u8; 256];
    let mut len = 0;
    // SAFETY: the pointers are valid, and each handle is closed once, by the thread that uses it.
    unsafe {
        assert_eq!(fipc_stream_create(c"my_channel".as_ptr(), 1 << 20, &mut listener), FIPC_OK); // rings of 1 MiB each way
        assert_eq!(fipc_stream_accept(listener, &mut conn, FIPC_FOREVER), FIPC_OK); // waits for a client
        if fipc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut len, FIPC_FOREVER) == FIPC_OK {
            let message = &mut buf[..len];
            message.make_ascii_uppercase();
            fipc_send(conn, message.as_ptr().cast(), message.len(), FIPC_FOREVER);
        }
        fipc_close(conn);
        fipc_listener_close(listener);
    }
}
