//! Prepared Edge host admission and Other dispatch, without transport I/O.

use claviron::edge::transport::RequestArrival;
use claviron::http::boxed_body;
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use std::convert::Infallible;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let host = support::edge::other();
    let service =
        hyper::service::service_fn(move |request: support::json::Input| {
            let prepared = host
                .prepare(request, RequestArrival::Established)
                .expect("fixture must pass host admission")
                .map_body(boxed_body);
            prepared.dispatch().map(Ok::<_, Infallible>)
        });
    support::decoded::run(criterion, "dispatch/edge/other", service);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
