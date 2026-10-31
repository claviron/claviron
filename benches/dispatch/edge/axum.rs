//! Prepared host admission followed by the application-owned Axum router.

use std::convert::Infallible;

use claviron::edge::transport::RequestArrival;
use claviron::http::boxed_body;
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let host = support::edge::axum();
    let service = hyper::service::service_fn(move |request: support::json::Input| {
        host.prepare(request, RequestArrival::Established)
            .expect("fixture must pass host admission")
            .map_body(boxed_body)
            .dispatch()
            .map(Ok::<_, Infallible>)
    });
    support::decoded::run(criterion, "dispatch/edge/axum", service);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
