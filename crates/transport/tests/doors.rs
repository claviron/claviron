use std::num::NonZeroU32;

use claviron_http::config::{HeaderReadTimeout, ListenerSettings};

use super::*;

fn address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 8000))
}

/// What a door promises: the backlog it was given, and no bound of its own on
/// a slow client or a body.
#[test]
fn a_door_declares_only_what_it_was_given() {
    let backlog = NonZeroU32::new(7).unwrap();
    let doors =
        Doors::new(Vec::new(), Protocols::default()).tcp_backlog(backlog);
    let listener = doors.listener(Some(address()), false).unwrap();
    assert_eq!(listener.listen, [address()]);
    assert_eq!(listener.requests.max_body_size, None);
    assert_eq!(listener.requests.body_read_timeout, None);
    let ListenerSettings::Tcp(tcp) = &listener.settings else {
        panic!("a door is a TCP listener");
    };
    assert_eq!(tcp.tcp.backlog, backlog);
    assert_eq!(tcp.h1.header_read_timeout, HeaderReadTimeout::Disabled);
}

/// HTTP/1 options on a listener without HTTP/1 are a configuration error, so
/// a door accepting HTTP/2 alone must not write them.
#[cfg(feature = "tls")]
#[test]
fn a_tls_door_can_accept_http2_alone() {
    let protocols = Protocols { h1: false, h2: true };
    let doors = Doors::new(Vec::new(), protocols);
    let listener = doors.listener(Some(address()), true).unwrap();
    let ListenerSettings::Tcp(tcp) = &listener.settings else {
        panic!("a door is a TCP listener");
    };
    assert_eq!(tcp.protocols, [HttpProtocol::H2]);
}
