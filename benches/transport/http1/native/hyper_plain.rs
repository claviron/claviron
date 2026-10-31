//! Native Hyper over an established HTTP/1.1 connection, with the plain workload.

use std::convert::Infallible;
use std::future::ready;

use criterion::{Criterion, criterion_group, criterion_main};
use http::Request;
use hyper::body::Incoming;

#[path = "../../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    support::http1::run_plain(
        criterion,
        "transport/http1/native/hyper/plain",
        || {
            support::http1::hyper(hyper::service::service_fn(
                |request: Request<Incoming>| {
                    ready(Ok::<_, Infallible>(support::plain::response(
                        request,
                    )))
                },
            ))
        },
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
