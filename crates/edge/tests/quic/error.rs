use super::*;

#[test]
fn normal_peer_close_does_not_hide_nonzero_failures() {
    for (code, normal) in [
        (0, true),
        (h3::error::Code::H3_NO_ERROR.value(), true),
        (h3::error::Code::H3_GENERAL_PROTOCOL_ERROR.value(), false),
        (h3::error::Code::H3_INTERNAL_ERROR.value(), false),
    ] {
        assert_eq!(
            normal_peer_close(&ConnectionErrorIncoming::ApplicationClose {
                error_code: code,
            }),
            normal
        );
    }
    assert!(normal_peer_close(&ConnectionErrorIncoming::ConnectionClosed {
        error_code: 0
    }));
    assert!(!normal_peer_close(&ConnectionErrorIncoming::ConnectionClosed {
        error_code: 1
    }));
    assert!(!normal_peer_close(&ConnectionErrorIncoming::Timeout));
    assert!(!normal_peer_close(&ConnectionErrorIncoming::InternalError(
        "test failure".into()
    )));
}
