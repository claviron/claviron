//! Shared JSON workload; construction and validation are identical for both owners.

use std::fmt::Debug;
use std::hint::black_box;

use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, Uri, Version, header};
use http_body::Body;
use http_body_util::{BodyExt, Empty, Full};
use serde::Serialize;

pub type Input = Request<Empty<Bytes>>;
pub const HOST: &str = "benchmark.test";

const EXPECTED: &[u8] =
    br#"{"message":"Hello, World!","method":"GET","path":"/json"}"#;

#[derive(Serialize)]
pub struct Payload<'a> {
    message: &'static str,
    method: &'a str,
    path: &'a str,
}

pub fn request() -> Input {
    Request::builder()
        .method("GET")
        .version(Version::HTTP_11)
        .uri(Uri::from_static("/json"))
        .header(header::HOST, HeaderValue::from_static(HOST))
        .body(Empty::new())
        .unwrap()
}

pub fn payload<B>(request: &Request<B>) -> Payload<'_> {
    Payload {
        message: "Hello, World!",
        method: request.method().as_str(),
        path: request.uri().path(),
    }
}

pub fn response<B>(request: Request<B>) -> Response<Full<Bytes>> {
    let payload = payload(&request);
    let bytes = serde_json::to_vec(black_box(&payload)).unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .body(Full::new(Bytes::from(bytes)))
        .unwrap()
}

pub async fn consume<B>(response: Response<B>)
where
    B: Body<Data = Bytes>,
    B::Error: Debug,
{
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), EXPECTED);
    // Include output destruction, not just constructing a response head.
    drop(black_box(bytes));
}
