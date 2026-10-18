use super::*;

#[test]
fn internal_errors_keep_source_private() {
    let error = Error::internal(std::io::Error::other("private detail"));

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "internal_server_error");
    assert_eq!(error.message(), "internal server error");
    assert!(error.to_string().contains("private detail"));
}

#[test]
fn constructors_expose_expected_http_metadata() {
    let error = Error::not_found("widget");

    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.code(), "not_found");
    assert_eq!(error.message(), "widget");
}
