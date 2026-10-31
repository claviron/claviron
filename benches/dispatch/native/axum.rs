//! Axum routing on a decoded request, without a listener or protocol driver.

use criterion::{Criterion, criterion_group, criterion_main};
use hyper_util::service::TowerToHyperService;

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let service = TowerToHyperService::new(support::axum::router());
    support::decoded::run(criterion, "dispatch/native/axum/json", service);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
