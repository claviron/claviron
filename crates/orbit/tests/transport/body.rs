use std::sync::Arc;

use orbitive::stream::Error as StreamError;
use orbitive::{Fleet, NodeId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::transport::{Error, TRANSPORT_BUFFER_BYTES};

fn fleet_name(tag: &str) -> String {
    format!("tb{tag}{:x}", std::process::id())
}

/// Two handles on one table, and the unlink that keeps a test run from
/// leaving an object behind. Shared memory outlives the process that made it,
/// so a test that forgot would cost every run after it.
struct Pair {
    edge: TransportBodies,
    worker: TransportBodies,
}

impl Drop for Pair {
    fn drop(&mut self) {
        let _ = self.edge.unlink();
    }
}

fn pair(tag: &str) -> Pair {
    let name = fleet_name(tag);
    let edge = TransportBodies::open(
        Arc::new(
            Fleet::join_shm_as(&name, 4, NodeId::ZERO).expect("edge fleet"),
        ),
        1,
    )
    .expect("edge bodies");
    edge.streams().reset_all();

    let worker = TransportBodies::open(
        Arc::new(
            Fleet::join_shm_as(&name, 4, NodeId::new(1)).expect("worker fleet"),
        ),
        1,
    )
    .expect("worker bodies");
    Pair { edge, worker }
}

/// One slot, both bodies. The request goes one way and the reply comes back
/// the other without a second slot being taken.
#[tokio::test]
async fn one_slot_carries_both_bodies() {
    let held = pair("both");
    let (edge, worker) = (&held.edge, &held.worker);

    let (mut originator, ticket) = edge.create().expect("create");
    let mut acceptor = worker.accept(ticket).expect("accept");

    originator.write_all(b"{\"lines\":2}").await.expect("request body");
    originator.shutdown().await.expect("finish request");

    let mut request = Vec::new();
    acceptor.read_to_end(&mut request).await.expect("read request");
    assert_eq!(request, b"{\"lines\":2}");

    acceptor.write_all(b"{\"id\":42}").await.expect("reply body");
    acceptor.shutdown().await.expect("finish reply");

    let mut reply = Vec::new();
    originator.read_to_end(&mut reply).await.expect("read reply");
    assert_eq!(reply, b"{\"id\":42}");
}

/// What SSE needs: a chunk is readable the moment it is written, not when
/// enough of them have accumulated. An event held back to fill a buffer is an
/// event delivered late.
#[tokio::test]
async fn a_chunk_is_readable_before_the_next_one_is_written() {
    let held = pair("sse");
    let (edge, worker) = (&held.edge, &held.worker);

    let (mut originator, ticket) = edge.create().expect("create");
    let mut acceptor = worker.accept(ticket).expect("accept");

    // Nothing is written after the first event until it has been read, so a
    // reader that sees it can only have seen it because writing published it.
    originator.write_all(b"data: one\n\n").await.expect("first event");

    let mut buffer = [0u8; 32];
    let read = acceptor.read(&mut buffer).await.expect("read first event");
    assert_eq!(&buffer[..read], b"data: one\n\n");

    originator.write_all(b"data: two\n\n").await.expect("second event");
    let read = acceptor.read(&mut buffer).await.expect("read second event");
    assert_eq!(&buffer[..read], b"data: two\n\n");
}

/// Full duplex is not a feature to add later: the reply may start while the
/// request is still arriving. That is what answers a large upload with 413
/// without reading it.
#[tokio::test]
async fn the_reply_may_start_before_the_request_finishes() {
    let held = pair("duplex");
    let (edge, worker) = (&held.edge, &held.worker);

    let (originator, ticket) = edge.create().expect("create");
    let acceptor = worker.accept(ticket).expect("accept");
    let (mut originator_read, mut originator_write) = originator.split();
    let (mut acceptor_read, mut acceptor_write) = acceptor.split();

    originator_write
        .write_all(b"first part of a long upload")
        .await
        .expect("partial body");

    // The far side answers without waiting for the rest.
    acceptor_write.write_all(b"413 too large").await.expect("early reply");
    acceptor_write.shutdown().await.expect("finish reply");

    let mut reply = Vec::new();
    originator_read.read_to_end(&mut reply).await.expect("read reply");
    assert_eq!(reply, b"413 too large");

    // And the request half is still its own: the exchange is not over because
    // one direction is.
    originator_write.shutdown().await.expect("finish request");
    let mut request = Vec::new();
    acceptor_read.read_to_end(&mut request).await.expect("read request");
    assert_eq!(request, b"first part of a long upload");
}

/// A body larger than the window still crosses: the ring is what a writer may
/// run ahead by, not what a body may weigh.
#[tokio::test]
async fn a_body_larger_than_the_window_crosses() {
    let held = pair("large");
    let (edge, worker) = (&held.edge, &held.worker);

    let (originator, ticket) = edge.create().expect("create");
    let acceptor = worker.accept(ticket).expect("accept");
    let (_, mut originator_write) = originator.split();
    let (mut acceptor_read, _) = acceptor.split();

    let size = TRANSPORT_BUFFER_BYTES * 3 + 1234;
    let sending = tokio::spawn(async move {
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        originator_write.write_all(&payload).await.expect("write body");
        originator_write.shutdown().await.expect("finish");
        payload
    });

    let mut received = Vec::new();
    acceptor_read.read_to_end(&mut received).await.expect("read body");
    assert_eq!(received, sending.await.expect("writer"));
}

/// A ticket read twice out of a frame must not open two endpoints: the claim
/// is a compare-and-swap, so the second attempt loses rather than aliasing
/// someone else's stream.
#[tokio::test]
async fn a_ticket_opens_exactly_once() {
    let held = pair("once");
    let (edge, worker) = (&held.edge, &held.worker);

    let (_originator, ticket) = edge.create().expect("create");
    let _first = worker.accept(ticket).expect("first accept");
    assert!(
        matches!(
            worker.accept(ticket),
            Err(Error::Stream(StreamError::AlreadyClaimed(_)))
        ),
        "a second accept must lose"
    );
}

/// After a reset the epoch moves, so a ticket minted before it addresses a
/// table that no longer exists. Refused, not followed into whatever occupies
/// the slot now.
#[tokio::test]
async fn a_ticket_from_before_a_reset_is_stale() {
    let held = pair("stale");
    let (edge, worker) = (&held.edge, &held.worker);

    let (_originator, ticket) = edge.create().expect("create");
    edge.streams().reset_all();

    assert!(matches!(
        worker.accept(ticket),
        Err(Error::Stream(StreamError::Stale(_)))
    ));
    assert!(!edge.is_live(&ticket));
}

/// The Claviron adapter must rearm the retained endpoint, not only its shared
/// slot. A bodiless request sends FIN before dispatch, and the same hot slot
/// still has to write the next request.
#[tokio::test]
async fn a_retained_slot_writes_again_after_sending_fin() {
    let held = pair("rearm");
    let (edge, worker) = (&held.edge, &held.worker);

    let (mut originator, ticket) = edge.create().expect("create");
    let mut acceptor = worker.accept(ticket).expect("first accept");
    originator.shutdown().await.expect("first fin");
    let mut empty = Vec::new();
    acceptor.read_to_end(&mut empty).await.expect("read first fin");
    drop(acceptor);

    edge.rearm(&mut originator).expect("rearm endpoint");
    let mut acceptor = worker.accept(ticket).expect("second accept");
    originator.write_all(b"next request").await.expect("write after rearm");
    let mut body = [0_u8; 12];
    acceptor.read_exact(&mut body).await.expect("read after rearm");
    assert_eq!(&body, b"next request");
}
