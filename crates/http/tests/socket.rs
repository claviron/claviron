use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// A door that commits by answering `200` and runs the session at once.
struct TestDoor(Arc<AtomicBool>);

impl Door for TestDoor {
    fn commit(
        self: Box<Self>,
        mut proposal: RouteResponse,
        deferred: Deferred,
    ) -> RouteResponse {
        let ran = self.0;
        let session = deferred(SocketIo::Native(Box::new(())));
        tokio::spawn(async move {
            session.await;
            ran.store(true, Ordering::SeqCst);
        });
        *proposal.status_mut() = StatusCode::OK;
        proposal
    }

    fn refuse(
        self: Box<Self>,
        response: RouteResponse,
    ) -> RouteResponse {
        refusal(response)
    }
}

fn offer() -> (Offer, Arc<AtomicBool>) {
    let ran = Arc::new(AtomicBool::new(false));
    let info = OfferInfo { kind: SocketKind::WebSocket, carrier: Carrier::H1 };
    (Offer::new(info, TestDoor(Arc::clone(&ran))), ran)
}

fn answered(status: StatusCode) -> RouteResponse {
    let mut response = http::Response::new(empty_body());
    *response.status_mut() = status;
    response
}

#[tokio::test]
async fn a_proposal_commits_and_its_session_runs_only_then() {
    let (offer, ran) = offer();
    let proposal = offer.claim().unwrap().accept_with(|_| async {});
    tokio::task::yield_now().await;
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(offer.finish(proposal).status(), StatusCode::OK);
    tokio::task::yield_now().await;
    assert!(ran.load(Ordering::SeqCst));
}

#[test]
fn one_attempt_is_claimed_once_whichever_clone_asks() {
    let (offer, _) = offer();
    let clone = offer.clone();
    let claim = offer.claim();
    assert!(claim.is_some());
    assert!(clone.claim().is_none());
    drop(claim);
    assert!(offer.claim().is_none(), "a dropped claim is not returned");
}

#[test]
fn no_status_manufactures_a_session() {
    for status in [StatusCode::OK, StatusCode::SWITCHING_PROTOCOLS] {
        let (offer, ran) = offer();
        assert_eq!(
            offer.finish(answered(status)).status(),
            StatusCode::FORBIDDEN
        );
        assert!(!ran.load(Ordering::SeqCst));
    }
    let (offer, _) = offer();
    let _claim = offer.claim();
    assert_eq!(
        offer.finish(answered(StatusCode::OK)).status(),
        StatusCode::FORBIDDEN
    );
}

#[test]
fn what_stood_in_front_keeps_its_answer() {
    for status in [StatusCode::TEMPORARY_REDIRECT, StatusCode::NOT_FOUND] {
        let (offer, _) = offer();
        assert_eq!(offer.finish(answered(status)).status(), status);
    }
}

#[test]
fn a_proposal_answers_only_its_own_attempt() {
    let (first, _) = offer();
    let (second, ran) = offer();
    let proposal = first.claim().unwrap().accept_with(|_| async {});
    assert_eq!(second.finish(proposal).status(), StatusCode::FORBIDDEN);
    assert!(!ran.load(Ordering::SeqCst));
}

#[test]
fn a_reply_hook_can_still_cancel_a_proposal() {
    let (offer, ran) = offer();
    let mut proposal = offer.claim().unwrap().accept_with(|_| async {});
    proposal.extensions_mut().clear();
    assert_eq!(offer.finish(proposal).status(), StatusCode::FORBIDDEN);
    assert!(!ran.load(Ordering::SeqCst));
}

#[test]
fn finishing_twice_is_one_decision() {
    let (offer, _) = offer();
    let first = offer.claim().unwrap().accept_with(|_| async {});
    let again = first.extensions().clone();
    assert_eq!(
        offer.finish(answered(StatusCode::NOT_FOUND)).status(),
        StatusCode::NOT_FOUND
    );
    let mut repeat = answered(StatusCode::SWITCHING_PROTOCOLS);
    *repeat.extensions_mut() = again;
    assert_eq!(offer.finish(repeat).status(), StatusCode::FORBIDDEN);
}

#[test]
fn an_h2_handshake_answers_200_with_only_the_owners_headers() {
    let mut request = http::Request::builder()
        .method(http::Method::CONNECT)
        .version(http::Version::HTTP_2)
        .uri("https://example.test/socket")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    request
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let handshake = Handshake::from_request(&request).unwrap().unwrap();

    let mut response = answered(StatusCode::SWITCHING_PROTOCOLS);
    for (name, value) in [
        ("connection", "Upgrade"),
        ("upgrade", "websocket"),
        ("sec-websocket-accept", "untrusted"),
        ("content-length", "0"),
        ("sec-websocket-protocol", "chat"),
        ("set-cookie", "session=example"),
    ] {
        response.headers_mut().insert(name, value.parse().unwrap());
    }
    handshake.commit(&mut response);

    assert_eq!(response.status(), StatusCode::OK);
    for header in
        ["connection", "upgrade", "sec-websocket-accept", "content-length"]
    {
        assert!(!response.headers().contains_key(header));
    }
    assert_eq!(response.headers()["sec-websocket-protocol"], "chat");
    assert_eq!(response.headers()["set-cookie"], "session=example");

    request.headers_mut().remove("sec-websocket-version");
    assert_eq!(
        Handshake::from_request(&request).unwrap_err(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn an_h1_handshake_answers_101_with_its_own_accept() {
    let request = http::Request::builder()
        .uri("/socket")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let mut response = answered(StatusCode::OK);
    response
        .headers_mut()
        .insert("sec-websocket-accept", "untrusted".parse().unwrap());
    Handshake::from_request(&request).unwrap().unwrap().commit(&mut response);
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()["sec-websocket-accept"],
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}

#[test]
fn an_upgrade_to_anything_else_is_an_ordinary_request() {
    let request = http::Request::builder()
        .uri("/")
        .header("connection", "Upgrade, HTTP2-Settings")
        .header("upgrade", "h2c")
        .body(())
        .unwrap();
    assert!(Handshake::from_request(&request).unwrap().is_none());
}
