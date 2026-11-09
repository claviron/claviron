//! The whole protocol, end to end, with nothing mocked.
//!
//! Two handles on both tables, as an edge worker and an application worker
//! would hold them: a request frame into one lane, its body down a duplex
//! slot, a reply frame back into the other lane, and the reply body up the
//! other direction of the same slot.

use std::sync::Arc;

use bytes::Bytes;
use orbitive::{Fleet, NodeId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::link::application::{
    ApplicationBodies, ApplicationSegment, Exchange, ExchangeBody,
    ExchangeReply,
};

const EDGE: usize = 0;
const WORKER: usize = 1;
const FLEET_CAPACITY: usize = 4;

/// Both tables as one side holds them.
struct Side {
    inbox: ApplicationSegment,
    bodies: ApplicationBodies,
}

struct Wires {
    edge: Side,
    worker: Side,
}

impl Drop for Wires {
    fn drop(&mut self) {
        let _ = self.edge.inbox.unlink();
        let _ = self.edge.bodies.unlink();
    }
}

fn wires(tag: &str) -> Wires {
    let name = format!("te{tag}{:x}", std::process::id());
    let open = |node: u16| Side {
        inbox: ApplicationSegment::open(&name, FLEET_CAPACITY).expect("inbox"),
        bodies: ApplicationBodies::open(
            Arc::new(
                Fleet::join_shm_as(
                    &name,
                    FLEET_CAPACITY as u16,
                    NodeId::new(node),
                )
                .expect("fleet"),
            ),
            1,
        )
        .expect("bodies"),
    };
    let edge = open(EDGE as u16);
    edge.bodies.streams().reset_all();
    let worker = open(WORKER as u16);

    // Both sides advertise themselves the way a booting process does.
    edge.inbox
        .inbox()
        .claim_lane(EDGE, "checkout", "asgi", 1)
        .expect("edge lane");
    worker
        .inbox
        .inbox()
        .claim_lane(WORKER, "checkout", "asgi", 1)
        .expect("worker lane");
    Wires { edge, worker }
}

#[tokio::test]
async fn a_request_and_its_reply_cross_shared_memory() {
    let wires = wires("full");
    let edge = &wires.edge;
    let worker = &wires.worker;

    // ── the edge dispatches ──────────────────────────────────────────────
    //
    // It chooses by reading: the lanes serving this application, idlest
    // first. No registry was consulted and nobody reported a load.
    let targets = edge.inbox.inbox().targets("checkout");
    assert!(targets.iter().any(|(node, _)| *node == WORKER));

    let (mut client_end, ticket) = edge.bodies.create().expect("body slot");
    let request = Exchange {
        correlation: 7,
        origin: EDGE as u16,
        method: "POST".to_owned(),
        scheme: "https".to_owned(),
        version: HttpVersion::Http11,
        path: "/orders".to_owned(),
        raw_path: b"/orders".to_vec(),
        query: Vec::new(),
        headers: vec![(b"content-type".to_vec(), b"application/json".to_vec())],
        client: None,
        server: None,
        stream: Some(ticket),
        body: ExchangeBody::Stream,
        upgrade: false,
    };
    edge.inbox
        .inbox()
        .write(WORKER, &request.encode().expect("encode"))
        .expect("write frame");
    client_end.write_all(b"{\"sku\":\"abc\"}").await.expect("request body");
    client_end.shutdown().await.expect("finish request");

    // ── the application worker serves ────────────────────────────────────
    let mut frame = Vec::new();
    assert!(
        worker.inbox.inbox().read(WORKER, &mut frame).expect("read"),
        "the frame arrived"
    );
    assert_eq!(frame_kind(&frame).expect("kind"), FrameKind::Request);

    let arrived = Exchange::decode(Bytes::from(frame)).expect("decode");
    assert_eq!(arrived.method, "POST");
    assert_eq!(arrived.path, "/orders");
    assert_eq!(arrived.correlation, 7);

    let mut server_end =
        worker.bodies.accept(arrived.stream.expect("ticket")).expect("accept");
    let mut body = Vec::new();
    server_end.read_to_end(&mut body).await.expect("read request body");
    assert_eq!(body, b"{\"sku\":\"abc\"}");

    server_end.write_all(b"{\"id\":1}").await.expect("reply body");
    server_end.shutdown().await.expect("finish reply");

    let reply = ExchangeReply {
        correlation: arrived.correlation,
        status: 201,
        headers: vec![(b"content-type".to_vec(), b"application/json".to_vec())],
        body: ExchangeBody::Stream,
    };
    // Back to the lane the request named, not to whoever is idle: only the
    // worker holding the client's connection can use this reply.
    worker
        .inbox
        .inbox()
        .write(arrived.origin as usize, &reply.encode().expect("encode"))
        .expect("write reply");

    // ── the edge answers its client ──────────────────────────────────────
    let mut frame = Vec::new();
    assert!(
        edge.inbox.inbox().read(EDGE, &mut frame).expect("read"),
        "the reply arrived"
    );
    assert_eq!(frame_kind(&frame).expect("kind"), FrameKind::Reply);

    let answered = ExchangeReply::decode(Bytes::from(frame)).expect("decode");
    assert_eq!(answered.correlation, 7);
    assert_eq!(answered.status, 201);

    let mut body = Vec::new();
    client_end.read_to_end(&mut body).await.expect("read reply body");
    assert_eq!(body, b"{\"id\":1}");
}

/// A request with nothing to send still gets served, and the reply body still
/// has somewhere to go: the slot exists for the exchange, not for the request.
#[tokio::test]
async fn an_exchange_with_no_request_body_still_carries_a_reply() {
    let wires = wires("get");
    let edge = &wires.edge;
    let worker = &wires.worker;

    let (mut client_end, ticket) = edge.bodies.create().expect("body slot");
    let request = Exchange {
        correlation: 1,
        origin: EDGE as u16,
        method: "GET".to_owned(),
        scheme: "http".to_owned(),
        path: "/health".to_owned(),
        raw_path: b"/health".to_vec(),
        stream: Some(ticket),
        ..Default::default()
    };
    edge.inbox
        .inbox()
        .write(WORKER, &request.encode().expect("encode"))
        .expect("write");
    // Nothing to send, so the direction ends immediately.
    client_end.shutdown().await.expect("finish request");

    let mut frame = Vec::new();
    worker.inbox.inbox().read(WORKER, &mut frame).expect("read");
    let arrived = Exchange::decode(Bytes::from(frame)).expect("decode");
    let mut server_end =
        worker.bodies.accept(arrived.stream.expect("ticket")).expect("accept");

    let mut empty = Vec::new();
    server_end.read_to_end(&mut empty).await.expect("read request body");
    assert!(empty.is_empty());

    server_end.write_all(b"ok").await.expect("reply body");
    server_end.shutdown().await.expect("finish reply");

    let mut body = Vec::new();
    client_end.read_to_end(&mut body).await.expect("read reply body");
    assert_eq!(body, b"ok");
}

/// Writing into a mailbox nobody empties would leave a caller waiting for an
/// answer that cannot come. The link refuses at the door, and the caller
/// turns that into a 503 rather than a timeout.
#[test]
fn an_application_with_no_worker_is_refused_rather_than_queued() {
    let wires = wires("gone");
    let edge = &wires.edge;

    wires.worker.inbox.inbox().release_lane(WORKER).expect("release");

    assert!(
        edge.inbox
            .inbox()
            .targets("checkout")
            .iter()
            .all(|(node, _)| *node != WORKER)
    );
    assert!(matches!(
        edge.inbox.inbox().admits(WORKER),
        Ok(crate::link::application::Admission::NoConsumer)
    ));
    assert!(edge.inbox.inbox().write(WORKER, b"nobody home").is_err());
}

#[tokio::test]
async fn receiver_capacity_is_shared_and_waiting_does_not_take_a_stream() {
    use std::future::{Future, poll_fn};
    use std::task::{Context, Waker};

    let name = format!("ad{:x}", std::process::id());
    let segment = Arc::new(ApplicationSegment::for_fleet(&name).unwrap());
    struct Cleanup(Arc<ApplicationSegment>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let _cleanup = Cleanup(Arc::clone(&segment));
    let worker =
        segment.join_with_capacity("bounded", "rust", 1, Some(1)).unwrap();
    let mut receiver = segment.application_receiver(worker.lane).unwrap();
    let first = ApplicationClient::join(&name, "caller-a", 2).unwrap();
    let second = ApplicationClient::join(&name, "caller-b", 3).unwrap();
    let target = second.target("bounded");
    let pending = first.target("bounded").send(Exchange::default()).unwrap();
    let mut frame = Vec::new();
    let received = loop {
        let received = receiver.next(&mut frame).await.unwrap();
        if matches!(received, Received::Work(_)) {
            break received;
        }
    };
    let (_, execution) =
        segment.accept_frame(worker.lane, received, &frame).unwrap().unwrap();
    // Taking the inbox frame is not completion of the response/session.
    assert!(matches!(
        target.send(Exchange::default()),
        Err(Error::CapacityFull)
    ));

    let mut cancelled = Box::pin(target.send_wait(Exchange::default()));
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(cancelled);
    // No exchange correlation or body slot was acquired by either failed try.
    // The first successful send from this client must still be correlation 1.
    let (parked, entered) = tokio::sync::oneshot::channel();
    let waiting = tokio::spawn(async move {
        let sending = target.send_wait(Exchange::default());
        tokio::pin!(sending);
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = sending.as_mut().poll(cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
    });
    entered.await.unwrap();
    let command = ControlCommand {
        application: "bounded".into(),
        name: "status".into(),
        payload: vec![],
        target: None,
    };
    segment.inbox().write(worker.lane, &command.encode().unwrap()).unwrap();
    loop {
        let received = receiver.next(&mut frame).await.unwrap();
        if received == Received::Frame {
            assert!(
                segment
                    .accept_frame(worker.lane, received, &frame)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(frame_kind(&frame).unwrap(), FrameKind::Control);
            break;
        }
    }
    assert!(!waiting.is_finished());
    drop(execution);
    let resumed = waiting.await.unwrap().unwrap();
    assert_eq!(resumed.correlation, 1);
    let received = receiver.next(&mut frame).await.unwrap();
    let admitted = segment.accept_frame(worker.lane, received, &frame).unwrap();
    assert!(admitted.is_some());
    drop((admitted, pending, resumed));
    first.leave();
    second.leave();
    let _ =
        ApplicationBodies::for_fleet(&name, first.lane(), 2).unwrap().unlink();
    let _ =
        ApplicationBodies::for_fleet(&name, second.lane(), 3).unwrap().unlink();
    segment.inbox().release_lane(worker.lane).unwrap();
    drop(worker.hold);
}

#[tokio::test]
async fn bounded_intake_rejects_raw_requests_and_withdrawal_wakes_capacity_waiters()
 {
    use std::future::{Future, poll_fn};
    let name = format!("aw{:x}", std::process::id());
    let segment = Arc::new(ApplicationSegment::for_fleet(&name).unwrap());
    let worker =
        segment.join_with_capacity("bounded", "rust", 1, Some(1)).unwrap();
    let client = ApplicationClient::join(&name, "caller", 2).unwrap();
    let frame = Exchange::default().encode().unwrap();
    assert!(matches!(
        segment.accept_frame(worker.lane, Received::Frame, &frame),
        Err(Error::Malformed(_))
    ));
    let reservation =
        segment.reserve_work(worker.lane, client.lane()).unwrap().unwrap();
    let target = client.target("bounded");
    let (parked, entered) = tokio::sync::oneshot::channel();
    let waiting = tokio::spawn(async move {
        let sending = target.send_wait(Exchange::default());
        tokio::pin!(sending);
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = sending.as_mut().poll(cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
    });
    entered.await.unwrap();
    segment.inbox().release_lane(worker.lane).unwrap();
    assert!(matches!(waiting.await.unwrap(), Err(Error::NoConsumer(_))));
    drop(reservation);
    client.leave();
    let _ =
        ApplicationBodies::for_fleet(&name, client.lane(), 2).unwrap().unlink();
    drop(worker.hold);
    segment.unlink().unwrap();
}
