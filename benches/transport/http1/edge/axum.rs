//! Native Edge HTTP/1.1 through host selection and the registered Axum owner.

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "../../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let connection = support::edge::connection(support::edge::Owner::Axum);
    support::http1::run(criterion, "transport/http1/edge/axum/plain", || {
        support::http1::edge(connection)
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
