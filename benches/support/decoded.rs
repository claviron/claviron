//! Measure an already decoded request through consumption of its JSON response.

use std::convert::Infallible;
use std::fmt::Debug;
use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput};
use http::Response;
use http_body::Body;
use hyper::service::Service;

use super::json;

pub fn run<S, B>(
    criterion: &mut Criterion,
    name: &str,
    service: S,
) where
    S: Service<json::Input, Response = Response<B>, Error = Infallible>,
    B: Body<Data = Bytes>,
    B::Error: Debug,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        json::consume(service.call(json::request()).await.unwrap()).await;
    });
    let mut group = criterion.benchmark_group(name);
    group.throughput(Throughput::Elements(1));
    group.bench_function("request", |bencher| {
        bencher.to_async(&runtime).iter_batched(
            json::request,
            |request| async {
                json::consume(service.call(black_box(request)).await.unwrap())
                    .await;
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}
