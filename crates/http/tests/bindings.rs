use http_body_util::BodyExt;

use super::*;

#[tokio::test]
async fn internal_error_response_hides_its_source() {
    let response =
        crate::Error::internal(std::io::Error::other("private detail"))
            .into_response();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&body).unwrap();

    assert_eq!(status, http::StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("internal_server_error"));
    assert!(!body.contains("private detail"));
}
