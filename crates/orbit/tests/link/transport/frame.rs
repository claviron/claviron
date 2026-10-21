use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use orbitive::NetId64;
use orbitive::stream::{Side, StreamId, Ticket};

use super::*;

fn ticket() -> Ticket {
    Ticket {
        id: StreamId::from_net_id(NetId64::make(211, 7, 42)),
        side: Side::B,
        epoch: 5,
    }
}

fn request() -> Exchange {
    Exchange {
        correlation: 0x0123_4567_89ab_cdef,
        origin: 3,
        method: "POST".to_owned(),
        scheme: "https".to_owned(),
        version: HttpVersion::Http2,
        path: "/orders/42".to_owned(),
        raw_path: b"/orders/42".to_vec(),
        query: b"expand=lines&expand=tax".to_vec(),
        headers: vec![
            (b"host".to_vec(), b"checkout.example".to_vec()),
            (b"content-type".to_vec(), b"application/json".to_vec()),
        ],
        client: Some(SocketAddr::new(Ipv4Addr::new(10, 0, 0, 7).into(), 51234)),
        server: Some(SocketAddr::new(Ipv4Addr::new(10, 0, 0, 1).into(), 443)),
        body: Some(ticket()),
        upgrade: true,
    }
}

#[test]
fn a_request_survives_the_round_trip() {
    let original = request();
    let decoded = Exchange::decode(&original.encode().unwrap()).unwrap();
    assert_eq!(decoded, original);
}

#[test]
fn a_reply_survives_the_round_trip() {
    let original = ExchangeReply {
        correlation: 99,
        status: 201,
        headers: vec![(b"location".to_vec(), b"/orders/42".to_vec())],
        body: Some(ticket()),
    };
    let decoded = ExchangeReply::decode(&original.encode().unwrap()).unwrap();
    assert_eq!(decoded, original);
}

/// The reason headers cross as bytes rather than as anything typed: an
/// application may depend on what actually arrived. A `HeaderMap` would
/// canonicalise the casing and could reorder repeats; nothing here may.
#[test]
fn headers_arrive_exactly_as_they_were_sent() {
    let awkward = Exchange {
        headers: vec![
            (b"X-Forwarded-For".to_vec(), b"10.0.0.1".to_vec()),
            (b"x-forwarded-for".to_vec(), b"10.0.0.2".to_vec()),
            (b"Set-Cookie".to_vec(), b"a=1".to_vec()),
            (b"Set-Cookie".to_vec(), b"b=2".to_vec()),
            (b"empty".to_vec(), Vec::new()),
        ],
        ..request()
    };
    let decoded = Exchange::decode(&awkward.encode().unwrap()).unwrap();
    assert_eq!(
        decoded.headers, awkward.headers,
        "casing, repeats and order must all survive"
    );
}

/// `raw_path` exists in ASGI precisely because `path` has been decoded. A
/// transport that carried only one of them would be deciding for the
/// application which it is allowed to see.
#[test]
fn the_decoded_and_raw_paths_are_both_carried() {
    let encoded = Exchange {
        path: "/orders/a b".to_owned(),
        raw_path: b"/orders/a%20b".to_vec(),
        ..request()
    };
    let decoded = Exchange::decode(&encoded.encode().unwrap()).unwrap();
    assert_eq!(decoded.path, "/orders/a b");
    assert_eq!(decoded.raw_path, b"/orders/a%20b");
}

#[test]
fn an_exchange_without_a_body_carries_no_ticket() {
    let bodyless =
        Exchange { body: None, client: None, server: None, ..request() };
    let decoded = Exchange::decode(&bodyless.encode().unwrap()).unwrap();
    assert_eq!(
        decoded.body, None,
        "absent means no body, not an empty one to read"
    );
    assert_eq!(decoded.client, None);
    assert_eq!(decoded.server, None);
}

#[test]
fn an_ipv6_peer_round_trips() {
    let addr = SocketAddr::new(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).into(),
        8443,
    );
    let encoded = Exchange { client: Some(addr), ..request() };
    let decoded = Exchange::decode(&encoded.encode().unwrap()).unwrap();
    assert_eq!(decoded.client, Some(addr));
}

/// A lane takes one half of an exchange. A frame written to the wrong one must
/// be refused before its bytes are read as something they are not.
#[test]
fn a_reply_is_not_mistaken_for_a_request() {
    let reply =
        ExchangeReply { correlation: 1, status: 200, ..Default::default() };
    let bytes = reply.encode().unwrap();
    assert_eq!(frame_kind(&bytes).unwrap(), FrameKind::Reply);
    assert!(Exchange::decode(&bytes).is_err());

    let request = request().encode().unwrap();
    assert_eq!(frame_kind(&request).unwrap(), FrameKind::Request);
    assert!(ExchangeReply::decode(&request).is_err());
}

/// The bytes come from another process. Every length in them is a claim, and a
/// claim that does not hold is refused rather than trusted.
#[test]
fn a_truncated_frame_is_refused_rather_than_guessed_at() {
    let bytes = request().encode().unwrap();
    for cut in 0..bytes.len() {
        assert!(
            Exchange::decode(&bytes[..cut]).is_err(),
            "decoding {cut} of {} bytes should have failed",
            bytes.len()
        );
    }
    assert!(Exchange::decode(&bytes).is_ok(), "the whole frame still decodes");
}

#[test]
fn an_unknown_frame_kind_is_refused() {
    assert!(frame_kind(&[]).is_err());
    assert!(frame_kind(&[7]).is_err());
}

#[test]
fn control_command_has_a_distinct_strict_frame() {
    let command = ControlCommand {
        application: "example-app".to_owned(),
        name: "stop".to_owned(),
        payload: vec![1, 2, 3],
        target: Some(super::ControlTarget {
            runtime: "php".to_owned(),
            name: "shop".to_owned(),
        }),
    };
    let frame = command.encode().unwrap();
    assert_eq!(frame_kind(&frame).unwrap(), FrameKind::Control);
    assert_eq!(ControlCommand::decode(&frame).unwrap(), command);
    let status = ControlCommand {
        application: "example-app".to_owned(),
        name: "status".to_owned(),
        payload: Vec::new(),
        target: None,
    };
    assert_eq!(
        ControlCommand::decode(&status.encode().unwrap()).unwrap(),
        status
    );
    assert!(ControlCommand::decode(&[3]).is_err());
    assert!(ControlCommand::decode(&[3, 1, 0]).is_err());
    assert!(Exchange::decode(&frame).is_err());
}

#[test]
fn application_snapshot_row_preserves_batch_and_payload() {
    let notice = ApplicationStateNotice {
        application: "example-app".to_owned(),
        lane: 3,
        incarnation: 7,
        batch: 11,
        row_count: 2,
        row_index: 1,
        payload: vec![0, 1, 255],
    };
    let frame = notice.encode().unwrap();
    assert_eq!(frame_kind(&frame).unwrap(), FrameKind::ApplicationState);
    assert_eq!(ApplicationStateNotice::decode(&frame).unwrap(), notice);
    assert!(ApplicationStateNotice::decode(&frame[..frame.len() - 1]).is_err());
    assert!(
        ApplicationStateNotice { row_count: 0, ..notice }.encode().is_err()
    );
}

#[test]
fn shutdown_notice_is_separate_from_a_command() {
    let notice = ShutdownNotice { edge: "example-edge".to_owned() };
    let frame = notice.encode().unwrap();
    assert_eq!(frame_kind(&frame).unwrap(), FrameKind::ShutdownInitiated);
    assert_eq!(ShutdownNotice::decode(&frame).unwrap(), notice);
}

/// The version is an enum on the wire and a string in the scope, so the
/// mapping has to be exact in both directions.
#[test]
fn every_http_version_keeps_its_asgi_spelling() {
    for (version, spelling) in [
        (HttpVersion::Http09, "0.9"),
        (HttpVersion::Http10, "1.0"),
        (HttpVersion::Http11, "1.1"),
        (HttpVersion::Http2, "2"),
        (HttpVersion::Http3, "3"),
    ] {
        assert_eq!(version.as_str(), spelling);
        let decoded = Exchange::decode(
            &Exchange { version, ..request() }.encode().unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.version, version);
    }
    assert_eq!(HttpVersion::from(http::Version::HTTP_3), HttpVersion::Http3);
}

/// A header block larger than a slot cannot be written, and the caller needs
/// to learn that here rather than from a truncated frame.
#[test]
fn an_oversized_field_is_refused() {
    let huge = vec![b'x'; u16::MAX as usize + 1];
    let oversized = Exchange { raw_path: huge, ..request() };
    assert!(oversized.encode().is_err());
}
