//! Hyper HTTP/1.1 plus the prepared configured Edge host plan.
//! This measures the HostService handoff, not Edge's native listener lifecycle.

use std::convert::Infallible;

use claviron::edge::transport::RequestArrival;
use claviron::http::boxed_body;
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use http::Request;
use hyper::body::Incoming;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let host = support::edge::pipeline();
    support::http1::run(criterion, "transport/http1/edge", || {
        support::http1::hyper(hyper::service::service_fn(move |request: Request<Incoming>| {
            host.prepare(request, RequestArrival::Established)
                .expect("fixture must pass host admission")
                .map_body(boxed_body)
                .dispatch()
                .map(Ok::<_, Infallible>)
        }))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
