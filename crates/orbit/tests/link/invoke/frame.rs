use super::*;

fn request() -> Frame {
    Frame {
        from: Peer { lane: 1, incarnation: 10 },
        to: Peer { lane: 2, incarnation: 20 },
        sequence: 7,
        reply_limit: 256,
        ticket: Some(Ticket {
            id: StreamId::from_net_id(NetId64::from_raw(7)),
            side: Side::B,
            epoch: 1,
        }),
        body: Body::Request {
            operation: "read".to_owned(),
            payload: vec![1, 2, 3],
        },
    }
}

#[test]
fn invoke_frame_roundtrips_endpoint_stamps_and_opaque_results() {
    let mut frame = request();
    assert_eq!(Frame::decode(&frame.encode().unwrap()).unwrap(), frame);
    for result in [
        Ok(vec![4, 5]),
        Err(Failure::Busy),
        Err(Failure::Closing),
        Err(Failure::Unsupported),
        Err(Failure::ResponseTooLarge),
        Err(Failure::Rejected(vec![6])),
    ] {
        frame.ticket = None;
        frame.body = Body::Reply(result);
        assert_eq!(Frame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }
}

#[test]
fn invoke_frame_rejects_truncation_trailing_bytes_and_unknown_versions() {
    let frame = request().encode().unwrap();
    for length in 0..frame.len() {
        assert!(Frame::decode(&frame[..length]).is_err());
    }
    let mut bytes = frame.clone();
    bytes.push(0);
    assert!(Frame::decode(&bytes).is_err());
    let mut bytes = frame;
    bytes[1] = 255;
    assert!(Frame::decode(&bytes).is_err());
    assert_ne!(TAG, 0xf1);
    assert!(!(1..=5).contains(&TAG));
}
