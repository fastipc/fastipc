//! Broadcasts: one publisher, many subscribers.
//!
//! The publisher [`create`]s the broadcast's name and gets its publisher, a [`Connection`] whose sends go to every
//! subscriber; a subscriber [`join`]s as a [`SubscriberRole`] and receives on its own connection. The publisher's
//! process admits each joiner on a thread of the library's: no thread of the publisher's waits for it.
//!
//! ```
//! use fipc::{Error, NO_WAIT, SubscriberRole};
//! use std::time::Duration;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut publisher = fipc::broadcast::create("fipc_doc_broadcast", 4096, 8, None)?;
//! let mut lossy = fipc::broadcast::join("fipc_doc_broadcast", SubscriberRole::Lossy, Duration::from_secs(5))?;
//! publisher.send(b"tick", None)?;
//! assert_eq!(lossy.receive(None)?, b"tick");
//! assert_eq!(lossy.receive(NO_WAIT), Err(Error::Timeout));
//! drop(publisher); // ends the broadcast
//! assert_eq!(lossy.receive(None), Err(Error::Disconnected));
//! # Ok(())
//! # }
//! ```

use std::ptr::{self, NonNull};
use std::time::Duration;

use fipc_sys as sys;

use crate::connection::Kind;
use crate::error::check;
use crate::{Connection, Result, SubscriberRole, c_name, millis};

/// Publisher: claims `name` for a broadcast and returns its publisher, the broadcast's one handle. Doesn't wait:
/// subscribers may [`join`] from now on, and the publisher's process admits each one.
///
/// The broadcast has one publisher and up to `max_subscribers` (1 to 1024) subscribers, in any processes of the user.
/// Every message the publisher sends goes to every subscriber, in one order, and is stored once, in `capacity` bytes (a
/// power of two, 4096 to 2^31) of memory every subscriber maps; a message is of one piece, 1 to a quarter of the
/// capacity ([`max_piece`](Connection::max_piece)). The broadcast holds up to `capacity / 64` messages: a gated
/// subscriber holds the publisher back, and a lossy one is lapped, once that much is unread.
///
/// A gated subscriber whose position stays still for `max_stall` from the first send it held back is unsubscribed (its
/// receives return [`Error::Unsubscribed`](crate::Error::Unsubscribed)) and the publisher goes on: `None`
/// ([`FOREVER`](crate::FOREVER)) never unsubscribes, [`NO_WAIT`](crate::NO_WAIT) refuses gated subscribers.
///
/// The publisher is a [`Connection`] whose sends go to every subscriber and wait only for gated ones; its receives
/// return [`Error::Invalid`](crate::Error::Invalid). [`wait_for_members`](Connection::wait_for_members) counts the
/// joined subscribers and waits for a number of them. Dropping the publisher ends the broadcast and frees the name:
/// each subscriber receives what was sent, then [`Error::Disconnected`](crate::Error::Disconnected).
///
/// # Errors
///
/// [`Error::AddrInUse`](crate::Error::AddrInUse) if another stream, broadcast, queue or service holds the name;
/// [`Error::Invalid`](crate::Error::Invalid) for a bad name, capacity or `max_subscribers`;
/// [`Error::NoMemory`](crate::Error::NoMemory) if the memory for the whole broadcast can't be had (it is set up now,
/// not as subscribers join).
pub fn create(
    name: &str,
    capacity: usize,
    max_subscribers: u32,
    max_stall: impl Into<Option<Duration>>,
) -> Result<Connection> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    let code =
        unsafe { sys::fipc_broadcast_create(c_name.as_ptr(), capacity, max_subscribers, millis(max_stall), &mut out) };
    check(code, 0)?;
    let ptr = NonNull::new(out).expect("fipc_broadcast_create returned FIPC_OK without a publisher");
    // SAFETY: a publisher fipc_broadcast_create just returned, owned by nothing else.
    Ok(unsafe { Connection::from_raw(ptr, name, Kind::Plain) })
}

/// Subscriber: joins the broadcast on `name` as `role` and returns its connection, waiting up to `timeout` for the
/// publisher to [`create`] the broadcast, to have a free slot (while `max_subscribers` are joined, a joiner waits for
/// one to leave) and for the publisher's process to admit it. The subscriber receives the messages sent from its
/// admission on, which comes before `join` returns, then [`Error::Disconnected`](crate::Error::Disconnected) once the
/// publisher is dropped or its process ended. Its sends return [`Error::Invalid`](crate::Error::Invalid); dropping it
/// leaves the broadcast.
///
/// # Errors
///
/// [`Error::Timeout`](crate::Error::Timeout); [`Error::Invalid`](crate::Error::Invalid) for a bad name, a name a work
/// queue or a service holds, a publisher with another version of the protocol or running as another user, or
/// [`SubscriberRole::Gated`] on a broadcast that refuses gated subscribers; [`Error::NoMemory`](crate::Error::NoMemory)
/// if the broadcast can't be mapped. On a name a stream holds, the join learns the kind when the stream's server
/// accepts it, which brings [`Error::Invalid`](crate::Error::Invalid); while no accept runs, it waits and returns
/// [`Error::Timeout`](crate::Error::Timeout).
pub fn join(name: &str, role: SubscriberRole, timeout: impl Into<Option<Duration>>) -> Result<Connection> {
    let c_name = c_name(name)?;
    let mut out = ptr::null_mut();
    // SAFETY: c_name is NUL-terminated; out is valid for a write.
    check(unsafe { sys::fipc_broadcast_join(c_name.as_ptr(), role.code(), &mut out, millis(timeout)) }, 0)?;
    let ptr = NonNull::new(out).expect("fipc_broadcast_join returned FIPC_OK without a connection");
    // SAFETY: a connection fipc_broadcast_join just returned, owned by nothing else; a gated subscriber's if role is
    // Gated.
    Ok(unsafe {
        Connection::from_raw(ptr, name, if role == SubscriberRole::Gated { Kind::Gated } else { Kind::Plain })
    })
}
