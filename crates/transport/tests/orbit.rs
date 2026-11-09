use super::*;

fn cancellation_stream()
-> (claviron_orbit::stream::ReadHalf, claviron_orbit::stream::WriteHalf) {
    use claviron_orbit::Fleet;
    use claviron_orbit::stream::{Incarnation, Streams};

    let streams = Streams::new(
        Arc::new(Fleet::join("head-cancel", 2).unwrap()),
        Incarnation::new(1),
    )
    .unwrap();
    let (caller, ticket) = streams.create().unwrap();
    let runner = streams.open(ticket).unwrap();
    let (reader, mut upload) = caller.split();
    // An empty/completed request upload must not cancel a pending reply.
    upload.finish().unwrap();
    let (_incoming, outgoing) = runner.split();
    (reader, outgoing)
}

#[tokio::test]
async fn cancelled_head_releases_in_flight_work() {
    struct PendingOwner(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for PendingOwner {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }

    let (reader, mut outgoing) = cancellation_stream();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (dropped, released) = tokio::sync::oneshot::channel();
    let mut exchanges = JoinSet::new();
    exchanges.spawn(async move {
        before_head(Some(&mut outgoing), async {
            let _owner = PendingOwner(Some(dropped));
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        })
        .await
    });
    ready.await.unwrap();
    assert_eq!(exchanges.len(), 1);
    reader.stop_reading();
    assert!(exchanges.join_next().await.unwrap().unwrap().is_none());
    assert!(exchanges.is_empty());
    released.await.unwrap();
}

#[tokio::test]
async fn cancelled_buffered_body_is_dropped_before_the_head() {
    struct Buffered {
        polled: Option<tokio::sync::oneshot::Sender<()>>,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl http_body::Body for Buffered {
        type Data = Bytes;
        type Error = std::io::Error;

        fn size_hint(&self) -> http_body::SizeHint {
            http_body::SizeHint::with_exact(1)
        }

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
        {
            if let Some(polled) = self.polled.take() {
                polled.send(()).unwrap();
            }
            std::task::Poll::Pending
        }
    }
    impl Drop for Buffered {
        fn drop(&mut self) {
            let _ = self.dropped.take().unwrap().send(());
        }
    }
    let (reader, mut outgoing) = cancellation_stream();
    let (polled, ready) = tokio::sync::oneshot::channel();
    let (dropped, released) = tokio::sync::oneshot::channel();
    let body = Buffered { polled: Some(polled), dropped: Some(dropped) };
    let waiter = tokio::spawn(async move {
        before_head(Some(&mut outgoing), body.collect()).await
    });
    ready.await.unwrap();
    reader.stop_reading();
    assert!(waiter.await.unwrap().is_none());
    released.await.unwrap();
}

#[tokio::test]
async fn pre_head_cancellation_wins_before_work_starts_but_fin_does_not() {
    let (reader, mut outgoing) = cancellation_stream();
    assert_eq!(
        before_head(Some(&mut outgoing), async { 200 }).await,
        Some(200)
    );
    reader.stop_reading();
    assert!(
        before_head(Some(&mut outgoing), async {
            panic!("an already cancelled exchange must not enter its handler")
        })
        .await
        .is_none()
    );
    assert_eq!(before_head(None, async { 204 }).await, Some(204));
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
    let segment = ApplicationSegment::open(&name, 2).unwrap();
    struct Cleanup(ApplicationSegment);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let held = Cleanup(segment);
    // A lane with no consumer deterministically refuses publication. This
    // exercises the same terminal path as full-capacity/contended admission.
    assert!(
        publish_reply_head(held.0.inbox(), 0, &[b"head"], Some(&mut outgoing))
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

#[tokio::test]
async fn idle_stream_releases_its_body_when_the_response_reader_leaves() {
    struct Idle {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl http_body::Body for Idle {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
        {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            std::task::Poll::Pending
        }
    }
    impl Drop for Idle {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }
    let (reader, mut outgoing) = cancellation_stream();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (dropped, released) = tokio::sync::oneshot::channel();
    let body = Idle { entered: Some(entered), dropped: Some(dropped) };
    let serving =
        tokio::spawn(async move { stream_reply(&mut outgoing, body, 1).await });
    ready.await.unwrap();
    // The caller stays alive; only its response reader is abandoned.
    drop(reader);
    assert_eq!(serving.await.unwrap(), None);
    released.await.unwrap();
}

#[tokio::test]
async fn completed_upload_does_not_cancel_a_streaming_response() {
    let (mut reader, mut outgoing) = cancellation_stream();
    let bytes = Bytes::from_static(b"one event");
    let body = http_body_util::Full::new(bytes.clone());
    assert_eq!(stream_reply(&mut outgoing, body, 1).await, Some(bytes.len()));
    let mut received = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut received)
        .await
        .unwrap();
    assert_eq!(received, bytes);
}

#[tokio::test]
async fn admission_stays_owned_after_the_head_until_the_streamed_body_ends() {
    use claviron_orbit::link::application::{Error, Exchange, ExchangeBody};
    use std::sync::Mutex;
    struct StreamingOwner {
        ready: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        finish: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    struct Body {
        ready: Option<tokio::sync::oneshot::Sender<()>>,
        finish: tokio::sync::oneshot::Receiver<()>,
    }
    impl http_body::Body for Body {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
        {
            if let Some(ready) = self.ready.take() {
                ready.send(()).unwrap();
            }
            match Pin::new(&mut self.finish).poll(cx) {
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(_) => std::task::Poll::Ready(None),
            }
        }
    }
    impl Owner for StreamingOwner {
        fn call(
            &self,
            _: http::Request<claviron_http::RouteBody>,
            _: Option<&Arc<claviron_http::ext::ConnCtx>>,
            _: RequestArrival,
        ) -> impl Future<Output = claviron_http::RouteResponse> + Send + use<>
        {
            let body = Body {
                ready: self.ready.lock().unwrap().take(),
                finish: self.finish.lock().unwrap().take().unwrap(),
            };
            async move {
                http::Response::new(body.map_err(Into::into).boxed_unsync())
            }
        }
    }
    let name = format!("sb{:x}", std::process::id());
    let segment = Arc::new(ApplicationSegment::open(&name, 4).unwrap());
    let caller = segment.join("caller", "edge", 1).unwrap();
    let worker =
        segment.join_with_capacity("worker", "rust", 2, Some(1)).unwrap();
    let origin =
        ApplicationBodies::for_segment(&segment, caller.lane, 1).unwrap();
    let bodies = Arc::new(
        ApplicationBodies::for_segment(&segment, worker.lane, 2).unwrap(),
    );
    let (mut endpoint, stream) = origin.create().unwrap();
    endpoint.finish().unwrap();
    let exchange = Exchange {
        method: "GET".into(),
        path: "/".into(),
        raw_path: b"/".to_vec(),
        scheme: "http".into(),
        origin: caller.lane as u16,
        stream: Some(stream),
        body: ExchangeBody::Empty,
        ..Exchange::default()
    };
    let work = segment.reserve_work(worker.lane, caller.lane).unwrap().unwrap();
    let ticket = work.ticket();
    work.publish(&[&exchange.encode().unwrap()]).unwrap();
    let mut frame = Vec::new();
    assert_eq!(
        segment.inbox().read_frame(worker.lane, &mut frame).unwrap(),
        Some(Some(ticket))
    );
    let admission = segment
        .accept_frame(worker.lane, Received::Work(ticket), &frame)
        .unwrap();
    let (ready, head_sent) = tokio::sync::oneshot::channel();
    let (finish, body_end) = tokio::sync::oneshot::channel();
    let owner = Arc::new(StreamingOwner {
        ready: Mutex::new(Some(ready)),
        finish: Mutex::new(Some(body_end)),
    });
    let serving = tokio::spawn(serve(
        frame,
        worker.lane,
        owner,
        Arc::clone(&segment),
        Arc::clone(&bodies),
        SessionShutdown { token: CancellationToken::new() },
        admission,
    ));
    head_sent.await.unwrap();
    assert!(segment.inbox().read(caller.lane, &mut Vec::new()).unwrap());
    assert!(matches!(
        segment.reserve_work(worker.lane, caller.lane),
        Err(Error::CapacityFull)
    ));
    finish.send(()).unwrap();
    serving.await.unwrap();
    drop(segment.reserve_work(worker.lane, caller.lane).unwrap().unwrap());
    drop(endpoint);
    segment.inbox().release_lane(worker.lane).unwrap();
    segment.inbox().release_lane(caller.lane).unwrap();
    drop((worker.hold, caller.hold));
    let _ = origin.unlink();
    segment.unlink().unwrap();
}
