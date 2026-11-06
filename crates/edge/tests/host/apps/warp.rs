use claviron_http::ServiceResult;
use claviron_pipeline::HostRequest;
use claviron_pipeline::warp::application;
use http::Request;

#[tokio::test]
async fn warp_rejection_remains_a_handled_response() {
    use warp::Filter;
    let filter = warp::path("known").map(|| "warp");
    let router = application(claviron_http::warp::router(filter));
    let request = Request::builder()
        .uri("/missing")
        .body(claviron_http::empty_body())
        .unwrap();
    let result = router.call(HostRequest::new(request)).await;
    match result {
        ServiceResult::Handled(response) => {
            assert_eq!(response.status(), http::StatusCode::NOT_FOUND)
        }
        ServiceResult::Unhandled(_) => panic!("a Warp 404 is terminal"),
    }
}
