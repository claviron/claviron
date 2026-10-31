//! One Axum router called natively and through its registered Edge binding.

use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use hyper::service::Service;
use hyper_util::service::TowerToHyperService;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let router = support::axum::plain_router();
    let host = support::edge::axum(router.clone());
    let native = TowerToHyperService::new(router);
    support::plain::run(
        criterion, "dispatch/edge/axum/plain", host,
        |request| native.call(request).map(|Ok(response)| response),
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
