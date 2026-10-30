//! Bare Hyper service on a decoded HTTP/1.1 request; see support for boundaries.

use std::convert::Infallible;
use std::future::ready;

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let service =
        hyper::service::service_fn(|request: support::json::Input| {
            ready(Ok::<_, Infallible>(support::json::response(request)))
        });
    support::decoded::run(criterion, "dispatch/hyper", service);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
