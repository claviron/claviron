use super::*;

#[tokio::test]
async fn message_owner_receives_its_ingress_retirement_signal() {
    use std::future::poll_fn;
    use std::task::Poll;

    let token = CancellationToken::new();
    let exchange = Exchange {
        method: "CONNECT".into(),
        scheme: "https".into(),
        path: "/messages".into(),
        raw_path: b"/messages".to_vec(),
        version: claviron_orbit::transport::HttpVersion::Http3,
        ..Default::default()
    };
    let request =
        session_request(&exchange, SessionShutdown { token: token.clone() })
            .unwrap();
    let signal = request.extensions().get::<SessionShutdown>().unwrap().clone();
    let mut waiting = std::pin::pin!(signal.cancelled());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    token.cancel();
    waiting.await;
    assert_eq!(request.version(), http::Version::HTTP_3);
    assert_eq!(request.method(), http::Method::CONNECT);
}

#[test]
fn refused_reply_publication_resets_even_a_finished_response_direction() {
    use claviron_orbit::Fleet;
    use claviron_orbit::stream::{Incarnation, Streams};

    let fleet = Arc::new(Fleet::join("reply-reset", 2).unwrap());
    let streams = Streams::new(fleet, Incarnation::new(1)).unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (read, _write) = origin.split();
    let (_incoming, mut outgoing) = remote.split();
    outgoing.try_write(b"body before head").unwrap();
    outgoing.finish().unwrap();

    let name = format!("rr{:x}", std::process::id());
    let segment = TransportSegment::open(&name, 2).unwrap();
    struct Cleanup(TransportSegment);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let held = Cleanup(segment);
    // A lane with no consumer deterministically refuses publication. This
    // exercises the same terminal path as full-capacity/contended admission.
    assert!(
        publish_reply_head(held.0.inbox(), 0, b"head", Some(&mut outgoing))
            .is_err()
    );
    assert!(read.try_read(&mut [0; 1]).is_err());
}

#[test]
fn edge_shutdown_application_event_preserves_notice_identity() {
    let notice = EdgeShutdownInitiated {
        edge: "example-edge".to_owned(),
        fleet: FleetName::new("example"),
        application: "example-app".to_owned(),
    };
    let event = notice.application_event().unwrap();
    assert_eq!(event.topic().as_str(), "claviron.edge.shutdown.initiated.v1");
    assert!(event.target().is_none());
    assert_eq!(
        event.payload(),
        &EventValue::Array(vec![
            (
                EventArrayKey::String("edge".into()),
                EventValue::String(b"example-edge".to_vec())
            ),
            (
                EventArrayKey::String("fleet".into()),
                EventValue::String(b"example".to_vec())
            ),
            (
                EventArrayKey::String("application".into()),
                EventValue::String(b"example-app".to_vec())
            ),
        ])
    );
    assert_eq!(
        ApplicationEvent::decode_orbit(&event.encode_orbit()).unwrap(),
        event
    );
}
