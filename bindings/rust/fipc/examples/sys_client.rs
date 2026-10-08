//! The client of fipc-sys's README, on the raw FFI: one RPC request to `sys_server` or `server` here, or to a
//! server in any other language.

// client.rs
use fipc_sys::*;
use std::ptr;

fn main() {
    let mut conn = ptr::null_mut();
    let mut id = 0;
    let mut buf = [0u8; 256];
    let mut response = fipc_rpc_msg_t::default();
    // SAFETY: the pointers are valid, and the handle is closed once, by the thread that uses it.
    unsafe {
        assert_eq!(fipc_connect(c"my_channel".as_ptr(), &mut conn, 5000), FIPC_OK);
        assert_eq!(fipc_rpc_submit(conn, 1, b"ping".as_ptr().cast(), 4, &mut id, FIPC_FOREVER), FIPC_OK); // opcode 1
        assert_eq!(fipc_rpc_recv(conn, buf.as_mut_ptr().cast(), buf.len(), &mut response, FIPC_FOREVER), FIPC_OK);
        fipc_close(conn);
    }
    println!("{}", String::from_utf8_lossy(&buf[..response.len as usize])); // PING
}
