//! Native axum::serve over an established HTTP/1.1 connection, with the plain workload.

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "../../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    support::http1::run(
        criterion,
        "transport/http1/native/axum/plain",
        support::http1::axum,
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
