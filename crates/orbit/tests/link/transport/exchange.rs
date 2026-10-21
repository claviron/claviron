//! The whole protocol, end to end, with nothing mocked.
//!
//! Two handles on both tables, as an edge worker and an application worker
//! would hold them: a request frame into one lane, its body down a duplex
//! slot, a reply frame back into the other lane, and the reply body up the
//! other direction of the same slot.

use std::sync::Arc;

use orbitive::{Fleet, NodeId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::transport::{
    Exchange, ExchangeReply, TransportBodies, TransportSegment,
};

const EDGE: usize = 0;
const WORKER: usize = 1;
const FLEET_CAPACITY: usize = 4;

/// Both tables as one side holds them.
struct Side {
    inbox: TransportSegment,
    bodies: TransportBodies,
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
        inbox: TransportSegment::open(&name, FLEET_CAPACITY).expect("inbox"),
        bodies: TransportBodies::open(
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
        body: Some(ticket),
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

    let arrived = Exchange::decode(&frame).expect("decode");
    assert_eq!(arrived.method, "POST");
    assert_eq!(arrived.path, "/orders");
    assert_eq!(arrived.correlation, 7);

    let mut server_end =
        worker.bodies.accept(arrived.body.expect("ticket")).expect("accept");
    let mut body = Vec::new();
    server_end.read_to_end(&mut body).await.expect("read request body");
    assert_eq!(body, b"{\"sku\":\"abc\"}");

    server_end.write_all(b"{\"id\":1}").await.expect("reply body");
    server_end.shutdown().await.expect("finish reply");

    let reply = ExchangeReply {
        correlation: arrived.correlation,
        status: 201,
        headers: vec![(b"content-type".to_vec(), b"application/json".to_vec())],
        body: Some(ticket),
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

    let answered = ExchangeReply::decode(&frame).expect("decode");
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
        body: Some(ticket),
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
    let arrived = Exchange::decode(&frame).expect("decode");
    let mut server_end =
        worker.bodies.accept(arrived.body.expect("ticket")).expect("accept");

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
/// answer that cannot come. The transport refuses at the door, and the caller
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
        Ok(crate::transport::Admission::NoConsumer)
    ));
    assert!(edge.inbox.inbox().write(WORKER, b"nobody home").is_err());
}
