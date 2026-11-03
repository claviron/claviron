use http_body_util::BodyExt;

use super::*;

#[tokio::test]
async fn warp_sync_body_keeps_data_trailers_and_end_state() {
    let mut full = SyncBody::new(crate::boxed_body(http_body_util::Full::new(
        Bytes::from_static(b"full"),
    )));
    assert!(!full.is_end_stream());
    assert_eq!(
        full.frame().await.unwrap().unwrap().data_ref().unwrap(),
        "full"
    );
    assert!(full.is_end_stream());
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-end", http::HeaderValue::from_static("done"));
    let body = http_body_util::Full::new(Bytes::from_static(b"data"))
        .with_trailers(std::future::ready(Some(Ok(trailers))));
    let mut body = SyncBody::new(crate::boxed_body(body));
    assert!(!body.is_end_stream());
    let frame = body.frame().await.unwrap().unwrap();
    assert_eq!(frame.data_ref().unwrap(), "data");
    assert!(!body.is_end_stream());
    let frame = body.frame().await.unwrap().unwrap();
    assert_eq!(frame.trailers_ref().unwrap()["x-end"], "done");
    assert!(body.frame().await.is_none());
}
