//! Native Hyper handler versus a config/Routable-compiled host plan.

use std::convert::Infallible;
use std::future::ready;

use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use hyper::service::Service;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let native = hyper::service::service_fn(|request: support::plain::Input| {
        ready(Ok::<_, Infallible>(support::plain::response(request)))
    });
    support::plain::run(
        criterion, "dispatch/edge/pipeline/plain", support::edge::pipeline(),
        |request| native.call(request).map(|Ok(response)| response),
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
