//! Native Hyper HTTP/1.1 server over an isolated persistent loopback connection.

use std::convert::Infallible;
use std::future::ready;

use criterion::{Criterion, criterion_group, criterion_main};
use hyper::body::Incoming;
use http::Request;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    support::http1::run(criterion, "transport/http1/hyper", || {
        support::http1::hyper(hyper::service::service_fn(|request: Request<Incoming>| {
            ready(Ok::<_, Infallible>(support::json::response(request)))
        }))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
