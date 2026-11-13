use super::*;

fn cancellation_stream() -> (claviron_orbit::stream::ReadHalf, ResponseWriter) {
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
    (reader, ResponseWriter::ByteStream(outgoing))
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

#[tokio::test(flavor = "current_thread")]
async fn refused_reply_publication_resets_even_a_finished_response_direction() {
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
        publish_reply_head(
            &held.0,
            (0, 0),
            0,
            &[b"head"],
            Some(&mut ResponseWriter::ByteStream(outgoing)),
        )
        .await
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
    let serving = tokio::spawn(async move {
        stream_reply(&mut outgoing, claviron_http::boxed_body(body)).await
    });
    ready.await.unwrap();
    // The caller stays alive; only its response reader is abandoned.
    drop(reader);
    assert!(serving.await.unwrap().is_err());
    released.await.unwrap();
}

#[tokio::test]
async fn completed_upload_does_not_cancel_a_streaming_response() {
    let (mut reader, mut outgoing) = cancellation_stream();
    let bytes = Bytes::from_static(b"one event");
    let body = claviron_http::bytes_body(bytes.clone());
    assert_eq!(stream_reply(&mut outgoing, body).await.unwrap(), bytes.len());
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
        fn execution(&self) -> claviron_runtime::ExecutorSpec {
            claviron_runtime::ExecutorSpec::Inherit
        }

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
    let worker = Arc::new(
        segment.join_with_capacity("worker", "rust", 2, Some(1)).unwrap(),
    );
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
        Arc::clone(&worker),
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
    drop((worker, caller));
    let _ = origin.unlink();
    segment.unlink().unwrap();
}

#[tokio::test]
async fn native_runner_publishes_the_head_before_a_shared_response_exceeds_its_window()
 {
    use claviron_orbit::config::LinkGeometry;
    use claviron_orbit::link::application::body::{
        FlowEvent, SHARED_RESPONSE_CONTROL_KIND,
    };
    use claviron_orbit::link::application::{
        Access, ApplicationClient, HttpPending,
    };
    use http_body::{Body, Frame};

    struct TwoFrames(usize);

    impl Body for TwoFrames {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>>
        {
            let frame = match self.0 {
                0 => Some(Ok(Frame::data(Bytes::from(vec![5; 768])))),
                1 => Some(Ok(Frame::data(Bytes::from_static(b"end")))),
                _ => None,
            };
            self.0 += 1;
            std::task::Poll::Ready(frame)
        }
    }

    struct StreamingOwner;

    impl Owner for StreamingOwner {
        fn execution(&self) -> claviron_runtime::ExecutorSpec {
            claviron_runtime::ExecutorSpec::Inherit
        }

        fn call(
            &self,
            request: http::Request<claviron_http::RouteBody>,
            _: Option<&Arc<claviron_http::ext::ConnCtx>>,
            _: RequestArrival,
        ) -> impl Future<Output = claviron_http::RouteResponse> + Send + use<>
        {
            assert!(request.into_body().is_end_stream());
            async {
                http::Response::new(claviron_http::boxed_body(TwoFrames(0)))
            }
        }
    }

    let name = format!("ps{:x}", std::process::id());
    let geometry = LinkGeometry {
        fleet_capacity: 4,
        inbox_capacity: 8,
        inbox_payload_capacity: 1024,
        lane_capacity: 2,
        buffer_bytes: 256,
    };
    let segment = Arc::new(
        ApplicationSegment::for_fleet_with_spec(
            &name,
            geometry.spec(),
            &Access::OwnerOnly,
        )
        .unwrap(),
    );
    let worker = Arc::new(
        segment.join_with_capacity("worker", "rust", 11, Some(1)).unwrap(),
    );
    let mut tables =
        ApplicationBodies::for_segment(&segment, worker.lane, 11).unwrap();
    tables.prepare_http(&segment, worker.lane, 11).unwrap();
    let tables = Arc::new(tables);
    let client = ApplicationClient::join_segment(
        Arc::clone(&segment),
        "edge",
        12,
        None,
        None,
    )
    .unwrap();
    let target = client.target("worker");
    let mut inbox = segment.receiver(worker.lane).unwrap();

    let test = async {
        let pending = target
            .send_http_wait(Exchange {
                method: "GET".into(),
                path: "/".into(),
                raw_path: b"/".to_vec(),
                scheme: "http".into(),
                ..Exchange::default()
            })
            .await
            .unwrap();
        let HttpPending::SharedResponse { mut body, reply, .. } = pending
        else {
            panic!("empty request must use the advertised shared response");
        };
        let mut frame = Vec::new();
        let received = loop {
            let received = inbox.next(&mut frame).await.unwrap();
            if matches!(received, Received::Work(_)) {
                break received;
            }
        };
        let request = Exchange::decode(Bytes::copy_from_slice(&frame)).unwrap();
        assert_eq!(
            request.stream.unwrap().id.kind(),
            SHARED_RESPONSE_CONTROL_KIND
        );
        let admission =
            segment.accept_frame(worker.lane, received, &frame).unwrap();
        let serving = tokio::spawn(serve(
            frame,
            Arc::clone(&worker),
            Arc::new(StreamingOwner),
            Arc::clone(&segment),
            Arc::clone(&tables),
            SessionShutdown { token: CancellationToken::new() },
            admission,
        ));
        assert!(matches!(
            reply.head().await.unwrap().body,
            ExchangeBody::Stream
        ));
        assert!(matches!(
            segment.reserve_work(worker.lane, client.lane()),
            Err(LinkError::CapacityFull)
        ));

        let mut data = Vec::new();
        let mut chunks = 0;
        loop {
            let event = poll_fn(|cx| {
                loop {
                    match body.try_next() {
                        Err(StreamError::WouldBlock) => {
                            match body.poll_readable(cx) {
                                std::task::Poll::Ready(Ok(())) => continue,
                                std::task::Poll::Ready(Err(error)) => {
                                    return std::task::Poll::Ready(Err(error));
                                }
                                std::task::Poll::Pending => {
                                    return std::task::Poll::Pending;
                                }
                            }
                        }
                        result => return std::task::Poll::Ready(result),
                    }
                }
            })
            .await
            .unwrap();
            match event {
                FlowEvent::Start { .. } => {}
                FlowEvent::Data(chunk) => {
                    assert!(chunk.len() <= 256);
                    data.extend_from_slice(&chunk);
                    chunks += 1;
                }
                FlowEvent::Fin => break,
                FlowEvent::Reset(_) => panic!("unexpected reset"),
            }
        }
        assert_eq!(&data[..768], &[5; 768]);
        assert_eq!(&data[768..], b"end");
        assert_eq!(chunks, 4);
        serving.await.unwrap();
        assert!(segment.reserve_work(worker.lane, client.lane()).is_ok());
    };
    let result =
        tokio::time::timeout(std::time::Duration::from_secs(5), test).await;
    client.leave();
    segment.inbox().release_lane(worker.lane).unwrap();
    drop(worker);
    tables.unlink().unwrap();
    segment.unlink().unwrap();
    result.expect("native runner must finish the shared response");
}

#[tokio::test]
async fn full_reply_inbox_wakes_the_publisher_and_fences_replacement() {
    use claviron_orbit::config::LinkGeometry;
    use claviron_orbit::link::application::Access;
    use std::task::{Context, Waker};

    let spec = LinkGeometry {
        fleet_capacity: 4,
        inbox_capacity: 4,
        inbox_payload_capacity: 1024,
        lane_capacity: 2,
        buffer_bytes: 1024,
    }
    .spec();
    let name = format!("rw{:x}", std::process::id());
    let open = || {
        ApplicationSegment::for_fleet_with_spec(&name, spec, &Access::OwnerOnly)
            .unwrap()
    };
    struct Cleanup(Arc<ApplicationSegment>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.unlink().unwrap();
        }
    }
    let cleanup = Cleanup(Arc::new(open()));
    let segment = &cleanup.0;
    let edge = segment.join("edge", EDGE_RUNTIME, 1).unwrap();
    let worker = segment.join("worker", "rust", 1).unwrap();
    let mut intake = segment.application_receiver(worker.lane).unwrap();
    let other = [open(), open()];
    let inbox = segment.inbox();
    let epoch = inbox.lane_epoch(edge.lane).unwrap();
    let lost_wake = std::time::Duration::from_secs(10);

    for replace in [false, true] {
        for (index, producer) in
            other.iter().cycle().take(spec.inbox.capacity).enumerate()
        {
            producer.publish_reply(edge.lane, &[&[index as u8]]).unwrap();
        }
        let publication = publish_reply_head(
            segment,
            (edge.lane, epoch),
            worker.lane,
            &[b"reply"],
            None,
        );
        let mut publication = std::pin::pin!(publication);
        assert!(
            publication
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut frame = Vec::new();
        if replace {
            inbox.release_lane(edge.lane).unwrap();
            inbox.claim_lane(edge.lane, "edge", EDGE_RUNTIME, 2).unwrap();
        } else {
            assert!(inbox.read(edge.lane, &mut frame).unwrap());
            assert_eq!(frame, [0]);
        }
        if replace {
            tokio::time::timeout(lost_wake, intake.drain_notification())
                .await
                .expect("a draining publisher must observe lane replacement")
                .unwrap();
        } else {
            assert!(matches!(
                tokio::time::timeout(lost_wake, intake.next(&mut frame))
                    .await
                    .expect(
                        "the other producer's frame must release inbox space"
                    )
                    .unwrap(),
                Received::Capacity(_)
            ));
        }
        let result = tokio::time::timeout(lost_wake, publication)
            .await
            .expect("publication must leave its wait");
        if replace {
            assert!(result.is_err());
        } else {
            result.unwrap();
            for index in 1..spec.inbox.capacity {
                assert!(inbox.read(edge.lane, &mut frame).unwrap());
                assert_eq!(frame, [index as u8]);
            }
            assert!(inbox.read(edge.lane, &mut frame).unwrap());
            assert_eq!(frame, b"reply");
        }
        assert!(!inbox.read(edge.lane, &mut frame).unwrap());
    }
}

#[tokio::test]
async fn inline_reply_cancellation_and_permanent_failure_with_full_inbox() {
    use claviron_orbit::config::LinkGeometry;
    use claviron_orbit::link::application::Access;
    use std::task::{Context, Waker};

    struct BufferedOwner(usize);
    impl Owner for BufferedOwner {
        fn execution(&self) -> claviron_runtime::ExecutorSpec {
            claviron_runtime::ExecutorSpec::Inherit
        }

        fn call(
            &self,
            _: Request<claviron_http::RouteBody>,
            _: Option<&Arc<claviron_http::ext::ConnCtx>>,
            _: RequestArrival,
        ) -> impl Future<Output = claviron_http::RouteResponse> + Send + use<>
        {
            let body =
                claviron_http::bytes_body(Bytes::from(vec![b'x'; self.0]));
            std::future::ready(http::Response::new(body))
        }
    }

    let geometry = LinkGeometry {
        fleet_capacity: 4,
        inbox_capacity: 4,
        inbox_payload_capacity: 256,
        lane_capacity: 2,
        buffer_bytes: 256,
    };
    let name = format!("irc{:x}", std::process::id());
    let open = || {
        ApplicationSegment::for_fleet_with_spec(
            &name,
            geometry.spec(),
            &Access::OwnerOnly,
        )
        .unwrap()
    };
    struct Cleanup(ApplicationSegment);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.unlink().unwrap();
        }
    }
    let cleanup = Cleanup(open());
    let edge = Arc::new(open());
    let runner = Arc::new(open());
    let caller = edge.join("edge", EDGE_RUNTIME, 1).unwrap();
    let joined = runner.join("runner", "rust", 1).unwrap();
    let controls = cleanup.0.exchange_controls().unwrap();
    runner.exchange_controls().unwrap();
    let mut edge_rx = edge.receiver(caller.lane).unwrap();
    let mut runner_rx = runner.application_receiver(joined.lane).unwrap();
    for _ in 0..geometry.inbox_capacity {
        cleanup.0.publish_reply(caller.lane, &[b"another producer"]).unwrap();
    }

    let exchange = Exchange {
        origin: caller.lane as u16,
        method: "GET".into(),
        path: "/".into(),
        raw_path: b"/".into(),
        ..Exchange::default()
    };
    let lost_wake = std::time::Duration::from_secs(5);
    let lease = controls
        .reserve(&caller, joined.lane, joined.epoch())
        .unwrap()
        .unwrap();
    let owner = BufferedOwner(2);
    let publishing =
        serve_inline(&exchange, lease.ticket(), &owner, &runner, &joined);
    let mut publishing = std::pin::pin!(publishing);
    assert!(
        publishing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    lease.cancel().unwrap();
    tokio::time::timeout(lost_wake, async {
        loop {
            let notification = runner_rx.drain_notification();
            tokio::pin!(notification);
            tokio::select! {
                biased;
                () = &mut publishing => break,
                result = &mut notification => result.unwrap(),
            }
        }
    })
    .await
    .expect("the publisher must observe cancellation without inbox space");
    assert_eq!(
        lease.caller_state().unwrap(),
        claviron_orbit::link::application::ExchangeControlState::Cancelled
    );
    drop(lease);

    // The same temporary backpressure may instead clear. Keep the body
    // prepared while a different producer's frame releases a turn.
    let lease = controls
        .reserve(&caller, joined.lane, joined.epoch())
        .unwrap()
        .unwrap();
    let mut frame = Vec::new();
    {
        let publishing =
            serve_inline(&exchange, lease.ticket(), &owner, &runner, &joined);
        let mut publishing = std::pin::pin!(publishing);
        assert!(
            publishing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(edge.inbox().read(caller.lane, &mut frame).unwrap());
        tokio::time::timeout(lost_wake, async {
            loop {
                tokio::select! {
                    biased;
                    () = &mut publishing => break,
                    result = runner_rx.drain_notification() => result.unwrap(),
                }
            }
        })
        .await
        .expect("freeing a turn must wake the inline publisher");
    }
    for _ in 1..geometry.inbox_capacity {
        assert!(edge.inbox().read(caller.lane, &mut frame).unwrap());
        assert_eq!(frame, b"another producer");
    }
    assert!(edge.inbox().read(caller.lane, &mut frame).unwrap());
    assert_eq!(
        ExchangeReply::decode(Bytes::from(frame)).unwrap().inline(),
        b"xx"
    );
    assert!(!edge.inbox().read(caller.lane, &mut Vec::new()).unwrap());
    lease.complete();
    for _ in 0..geometry.inbox_capacity {
        cleanup.0.publish_reply(caller.lane, &[b"another producer"]).unwrap();
    }

    let lease = controls
        .reserve(&caller, joined.lane, joined.epoch())
        .unwrap()
        .unwrap();
    let oversized = BufferedOwner(geometry.inbox_payload_capacity);
    let failing = async {
        loop {
            if matches!(
                edge_rx.next_notification().await.unwrap(),
                Received::Capacity(_)
            ) {
                break;
            }
        }
    };
    let ((), ()) = tokio::time::timeout(lost_wake, async {
        tokio::join!(
            failing,
            serve_inline(
                &exchange,
                lease.ticket(),
                &oversized,
                &runner,
                &joined
            )
        )
    })
    .await
    .expect(
        "permanent failure must wake Edge while the reply inbox stays full",
    );
    assert_eq!(
        lease.caller_state().unwrap(),
        claviron_orbit::link::application::ExchangeControlState::Failed
    );
    assert_eq!(
        edge.inbox().depth(caller.lane).unwrap(),
        geometry.inbox_capacity as u64
    );
}
