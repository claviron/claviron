//! TCP / UDP socket builders with explicit port-sharing policy.
//!
//! The caller decides whether multiple listeners may bind the same address.
//! Shared sockets opt into kernel-managed distribution with `SO_REUSEPORT`;
//! the exact distribution is OS-specific. This module knows no process roles.
//!
//! Lives in `claviron-sys` (not in a "claviron-net" crate) because
//! this is **socket-option plumbing**, not networking. Real network
//! abstractions (TLS termination, HTTP framing, vhost dispatch) live
//! one layer up in Edge's TCP/QUIC drivers or another listener owner.

use std::io;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::num::NonZeroU32;

use tokio::net::TcpListener;

mod accept;
pub use accept::{AcceptErrorClass, classify_accept_error};

/// Whether live sockets may share the same local bind address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortSharing {
    Exclusive,
    Shared,
}

/// Creates a non-blocking TCP listener with the caller's sharing policy.
/// `SO_REUSEADDR` remains enabled for restart after TCP TIME_WAIT; it does not
/// authorize a second listening socket. Only `Shared` enables `SO_REUSEPORT`.
/// The caller supplies the pending-connection backlog; sys chooses no default.
/// Values above the signed socket API range fail before any socket is opened.
/// The kernel may cap the requested backlog independently.
pub async fn create_tcp_listener(
    addr: SocketAddr,
    sharing: PortSharing,
    backlog: NonZeroU32,
) -> io::Result<TcpListener> {
    use std::net::TcpListener as StdTcpListener;

    use socket2::{Domain, Protocol, Socket, Type};

    let backlog = i32::try_from(backlog.get()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP listen backlog must be at most 2147483647",
        )
    })?;
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(sharing == PortSharing::Shared)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;
    socket.set_nonblocking(true)?;
    let std_listener: StdTcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Creates a non-blocking UDP socket for the caller's async I/O driver.
/// Exclusive UDP disables both reuse options: on some supported systems,
/// `SO_REUSEADDR` alone can also permit competing live UDP binds.
pub fn create_udp_socket(
    addr: SocketAddr,
    sharing: PortSharing,
) -> io::Result<StdUdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(sharing == PortSharing::Shared)?;
    socket.set_reuse_port(sharing == PortSharing::Shared)?;
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

#[cfg(test)]
#[path = "../tests/net.rs"]
mod tests;
