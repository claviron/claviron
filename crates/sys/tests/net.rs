use std::io::ErrorKind;

use socket2::SockRef;

use super::*;

#[tokio::test]
async fn exclusive_tcp_disables_reuseport_and_rejects_competing_listeners() {
    let backlog = NonZeroU32::new(8).unwrap();
    let listener = create_tcp_listener(
        "127.0.0.1:0".parse().unwrap(),
        PortSharing::Exclusive,
        backlog,
    )
    .await
    .unwrap();
    let options = SockRef::from(&listener);
    assert!(!options.reuse_port().unwrap());
    assert!(options.reuse_address().unwrap(), "TCP restart keeps SO_REUSEADDR");
    let addr = listener.local_addr().unwrap();
    for sharing in [PortSharing::Exclusive, PortSharing::Shared] {
        assert_eq!(
            create_tcp_listener(addr, sharing, backlog)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::AddrInUse
        );
    }
    // Keep the owner bound for every assertion: once released, an ephemeral
    // address is available to unrelated parallel tests/processes as well.
}

#[tokio::test]
async fn shared_tcp_allows_multiple_live_listeners() {
    let backlog = NonZeroU32::new(8).unwrap();
    let first = create_tcp_listener(
        "127.0.0.1:0".parse().unwrap(),
        PortSharing::Shared,
        backlog,
    )
    .await
    .unwrap();
    let addr = first.local_addr().unwrap();
    let second =
        create_tcp_listener(addr, PortSharing::Shared, backlog).await.unwrap();
    assert!(SockRef::from(&first).reuse_port().unwrap());
    assert!(SockRef::from(&second).reuse_port().unwrap());
    assert_eq!(
        create_tcp_listener(addr, PortSharing::Exclusive, backlog)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AddrInUse
    );
}

#[test]
fn tcp_backlog_outside_socket_range_fails_before_binding_or_entering_tokio() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let mut future = std::pin::pin!(create_tcp_listener(
        "192.0.2.1:1".parse().unwrap(),
        PortSharing::Exclusive,
        NonZeroU32::new(u32::MAX).unwrap(),
    ));
    let Poll::Ready(Err(error)) =
        future.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!(
            "invalid backlog must fail before opening a socket or needing a runtime"
        )
    };
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn exclusive_udp_disables_both_reuse_options_and_rejects_competing_sockets() {
    let socket = create_udp_socket(
        "127.0.0.1:0".parse().unwrap(),
        PortSharing::Exclusive,
    )
    .unwrap();
    let options = SockRef::from(&socket);
    assert!(!options.reuse_port().unwrap());
    assert!(!options.reuse_address().unwrap());
    let addr = socket.local_addr().unwrap();
    for sharing in [PortSharing::Exclusive, PortSharing::Shared] {
        assert_eq!(
            create_udp_socket(addr, sharing).unwrap_err().kind(),
            ErrorKind::AddrInUse
        );
    }
}

#[test]
fn shared_udp_allows_multiple_live_sockets() {
    let first =
        create_udp_socket("127.0.0.1:0".parse().unwrap(), PortSharing::Shared)
            .unwrap();
    let addr = first.local_addr().unwrap();
    let second = create_udp_socket(addr, PortSharing::Shared).unwrap();
    assert!(SockRef::from(&first).reuse_port().unwrap());
    assert!(SockRef::from(&second).reuse_port().unwrap());
    assert_eq!(
        create_udp_socket(addr, PortSharing::Exclusive).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
}
