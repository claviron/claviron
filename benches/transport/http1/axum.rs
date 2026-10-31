//! Native axum::serve, its router and Json over persistent loopback HTTP/1.1.

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    support::http1::run(criterion, "transport/http1/axum", support::http1::axum);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
