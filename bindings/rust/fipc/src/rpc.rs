use fipc_sys as sys;

use crate::{Error, Result};

/// Whether an RPC message is a request or a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcKind {
    /// A request, sent by [`rpc_submit`](crate::Connection::rpc_submit): answer it with
    /// [`rpc_respond`](crate::Connection::rpc_respond).
    Request,
    /// The response to a request, sent by [`rpc_respond`](crate::Connection::rpc_respond).
    Response,
}

/// An RPC message's header, without its payload: what
/// [`rpc_receive_into`](crate::Connection::rpc_receive_into) returns, the payload being in the caller's buffer.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RpcHeader {
    /// The request's id, echoed in its response. A connection numbers the requests it submits from 1.
    pub id: u64,
    /// A request or a response.
    pub kind: RpcKind,
    /// The application's: what the request asks for, by convention echoed in its response.
    pub opcode: u32,
    /// The application's: 0 in requests, a response's outcome.
    pub status: i32,
    /// The payload's length in bytes (it may be 0).
    pub len: usize,
}

/// An RPC message, its payload included: what [`rpc_receive`](crate::Connection::rpc_receive) returns.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RpcMessage {
    /// The request's id, echoed in its response. A connection numbers the requests it submits from 1.
    pub id: u64,
    /// A request or a response.
    pub kind: RpcKind,
    /// The application's: what the request asks for, by convention echoed in its response.
    pub opcode: u32,
    /// The application's: 0 in requests, a response's outcome.
    pub status: i32,
    /// The payload (it may be empty).
    pub payload: Vec<u8>,
}

impl RpcHeader {
    /// The header the C API filled; [`Error::Invalid`] for a kind it doesn't define (the library drops such a
    /// message itself, so this doesn't happen).
    pub(crate) fn from_sys(msg: &sys::fipc_rpc_msg_t) -> Result<RpcHeader> {
        let kind = match msg.kind {
            sys::FIPC_RPC_REQUEST => RpcKind::Request,
            sys::FIPC_RPC_RESPONSE => RpcKind::Response,
            _ => return Err(Error::Invalid),
        };
        let len = usize::try_from(msg.len).map_err(|_| Error::Invalid)?;
        Ok(RpcHeader { id: msg.id, kind, opcode: msg.opcode, status: msg.status, len })
    }

    pub(crate) fn with_payload(self, payload: Vec<u8>) -> RpcMessage {
        RpcMessage { id: self.id, kind: self.kind, opcode: self.opcode, status: self.status, payload }
    }
}
