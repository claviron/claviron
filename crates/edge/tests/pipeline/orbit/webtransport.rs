use super::*;

#[tokio::test]
async fn orbit_webtransport_directions_and_streams_progress_independently() {
    let cancellation = CancellationToken::new();
    let exercise = |byte| {
        let cancellation = cancellation.clone();
        async move {
            let (network, client) = tokio::io::duplex(16);
            let (application, slot) = tokio::io::duplex(16);
            let (mut input, mut output) = tokio::io::split(slot);
            let (mut client_rx, mut client_tx) = tokio::io::split(client);
            let (mut app_rx, mut app_tx) = tokio::io::split(application);
            let forwarding =
                transfer(network, &mut output, &mut input, &cancellation);
            let peers = async {
                let upload = async {
                    client_tx.write_all(&[byte; 128]).await.unwrap();
                    client_tx.shutdown().await.unwrap();
                };
                let download = async {
                    app_tx.write_all(&[byte + 1; 128]).await.unwrap();
                    app_tx.shutdown().await.unwrap();
                };
                let receive_upload = async {
                    let mut bytes = Vec::new();
                    app_rx.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(bytes, [byte; 128]);
                };
                let receive_download = async {
                    let mut bytes = Vec::new();
                    client_rx.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(bytes, [byte + 1; 128]);
                };
                tokio::join!(
                    upload,
                    download,
                    receive_upload,
                    receive_download
                );
            };
            let (result, ()) = tokio::join!(forwarding, peers);
            result.unwrap();
        }
    };
    tokio::join!(exercise(1), exercise(3));
}

#[test]
fn orbit_webtransport_route_selection_respects_prefix_and_route_ownership() {
    let host: HostConfig = serde_yaml::from_str("orbit:\n  - { fleet_name: apps, application: root }\n  - { path: /chat, fleet_name: apps, application: chat }\nroutes:\n  /local: { static: {} }\n").unwrap();
    assert_eq!(route(&host, "/chat/room").unwrap().application, "chat");
    assert_eq!(route(&host, "/chatter").unwrap().application, "root");
    assert!(route(&host, "/local/session").is_none());
    assert!(route(&HostConfig::new("public"), "/chat").is_none());
}

#[test]
fn orbit_webtransport_cancelled_copy_does_not_forge_fin() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let (_, mut input) = tokio::io::duplex(16);
    let (mut output, mut peer) = tokio::io::duplex(16);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = std::pin::pin!(copy(&mut input, &mut output, &cancellation))
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        matches!(result, Poll::Ready(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    let mut buffer = [0; 1];
    assert!(
        std::pin::pin!(peer.read(&mut buffer))
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

#[test]
fn orbit_webtransport_shutdown_finishes_the_chunk_already_in_flight() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let (mut source, mut input) = tokio::io::duplex(32);
    let (mut output, mut peer) = tokio::io::duplex(1);
    let cancellation = CancellationToken::new();
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        std::pin::pin!(source.write_all(b"accepted"))
            .poll(&mut context)
            .is_ready()
    );
    let mut pump = Box::pin(copy(&mut input, &mut output, &cancellation));
    assert!(pump.as_mut().poll(&mut context).is_pending());
    cancellation.cancel();
    assert!(
        std::pin::pin!(source.write_all(b"later"))
            .poll(&mut context)
            .is_ready()
    );
    let mut received = Vec::new();
    for index in 0..8 {
        let mut byte = [0; 1];
        assert!(
            std::pin::pin!(peer.read_exact(&mut byte))
                .poll(&mut context)
                .is_ready()
        );
        received.push(byte[0]);
        let result = pump.as_mut().poll(&mut context);
        // The final byte can already complete the write before the reader
        // takes it. Cancellation stops the next read, not this write.
        if index < 6 {
            assert!(result.is_pending());
        } else {
            assert!(
                matches!(result, Poll::Ready(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted)
            );
            break;
        }
    }
    drop(pump);
    let mut final_byte = [0; 1];
    assert!(
        std::pin::pin!(peer.read_exact(&mut final_byte))
            .poll(&mut context)
            .is_ready()
    );
    received.push(final_byte[0]);
    assert_eq!(received, b"accepted");
    let mut unread = [0; 5];
    assert!(
        std::pin::pin!(input.read_exact(&mut unread))
            .poll(&mut context)
            .is_ready()
    );
    assert_eq!(&unread, b"later");
}
