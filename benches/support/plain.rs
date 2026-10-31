//! Minimal host overhead on an already decoded GET, with no transport IO.
//!
//! `native` uses Hyper/Axum; `host` uses the registered Edge owner. Both execute
//! the same response function. Their difference includes preparation, ownership,
//! body adaptation and response completion, not JSON serialization.
//! `prepare` stops before async execution and never calls the application; it
//! measures synchronous admission, not the entire pre-handler path.
//! Request construction and prepared-capability destruction are outside timing.
//! Completed responses are consumed in both full calls.

use std::fmt::Debug;
use std::future::Future;
use std::hint::black_box;

use bytes::Bytes;
use claviron::edge::transport::{HostService, RequestArrival};
use claviron::http::boxed_body;
use criterion::{BatchSize, Criterion, Throughput};
use http::{Request, Response, StatusCode, header};
use http_body::Body;
use http_body_util::{BodyExt, Empty, Full};

pub type Input = Request<Empty<Bytes>>;
pub const PATH: &str = "/plain";
const TEXT: &[u8] = b"Hello, World!";

pub fn request() -> Input {
    Request::builder()
        .method("GET")
        .uri(PATH)
        .header(header::HOST, super::json::HOST)
        .body(Empty::new())
        .unwrap()
}

pub fn response<B>(request: Request<B>) -> Response<Full<Bytes>> {
    drop(request);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from_static(TEXT)))
        .unwrap()
}

async fn consume<B>(response: Response<B>)
where
    B: Body<Data = Bytes>,
    B::Error: Debug,
{
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "text/plain");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), TEXT);
    drop(black_box(bytes));
}

pub fn run<C, F, B>(
    criterion: &mut Criterion,
    name: &str,
    host: HostService,
    native: C,
) where
    C: Fn(Input) -> F,
    F: Future<Output = Response<B>>,
    B: Body<Data = Bytes>,
    B::Error: Debug,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();
    runtime.block_on(async {
        consume(native(request()).await).await;
        consume(host.prepare(request(), RequestArrival::Established)
            .expect("fixture must pass host admission")
            .map_body(boxed_body).dispatch().await).await;
    });
    let mut group = criterion.benchmark_group(name);
    group.throughput(Throughput::Elements(1));
    group.bench_function("native", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            request,
            |request| async { consume(native(black_box(request)).await).await },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("prepare", |bencher| {
        bencher.iter_batched(
            request,
            |request| host.prepare(black_box(request), RequestArrival::Established)
                .expect("fixture must pass host admission"),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("host", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            request,
            |request| async {
                let reply = host.prepare(black_box(request), RequestArrival::Established)
                    .expect("fixture must pass host admission")
                    .map_body(boxed_body).dispatch().await;
                consume(reply).await;
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}
