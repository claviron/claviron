use std::io::ErrorKind;

use socket2::SockRef;

use super::*;

#[tokio::test]
async fn exclusive_tcp_disables_reuseport_and_rejects_competing_listeners() {
    let listener = create_tcp_listener(
        "127.0.0.1:0".parse().unwrap(),
        PortSharing::Exclusive,
    )
    .await
    .unwrap();
    let options = SockRef::from(&listener);
    assert!(!options.reuse_port().unwrap());
    assert!(options.reuse_address().unwrap(), "TCP restart keeps SO_REUSEADDR");
    let addr = listener.local_addr().unwrap();
    for sharing in [PortSharing::Exclusive, PortSharing::Shared] {
        assert_eq!(
            create_tcp_listener(addr, sharing).await.unwrap_err().kind(),
            ErrorKind::AddrInUse
        );
    }
    // Keep the owner bound for every assertion: once released, an ephemeral
    // address is available to unrelated parallel tests/processes as well.
}

#[tokio::test]
async fn shared_tcp_allows_multiple_live_listeners() {
    let first = create_tcp_listener(
        "127.0.0.1:0".parse().unwrap(),
        PortSharing::Shared,
    )
    .await
    .unwrap();
    let addr = first.local_addr().unwrap();
    let second = create_tcp_listener(addr, PortSharing::Shared).await.unwrap();
    assert!(SockRef::from(&first).reuse_port().unwrap());
    assert!(SockRef::from(&second).reuse_port().unwrap());
    assert_eq!(
        create_tcp_listener(addr, PortSharing::Exclusive)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AddrInUse
    );
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
