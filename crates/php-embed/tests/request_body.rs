use super::*;
use claviron_http::body::{BodyLimit, guard_size};
use futures_util::FutureExt;
use http_body_util::Full;

#[test]
fn sapi_body_collector_consumes_ingress_and_preserves_guard_errors() {
    let payload = Bytes::from_static(b"abcd");
    let body = claviron_http::boxed_body(Full::new(payload.clone()));
    assert_eq!(collect_body(body).now_or_never().unwrap().unwrap(), payload);

    let body = guard_size(claviron_http::boxed_body(Full::new(payload)), 3);
    let error = collect_body(body).now_or_never().unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<BodyLimit>(),
        Some(&BodyLimit::TooLarge { limit: 3 })
    );
}
