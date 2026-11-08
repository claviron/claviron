//! Native Edge HTTP/1.1 through host selection and the registered Routable owner.

use criterion::{Criterion, criterion_group, criterion_main};

#[path = "../../../support/mod.rs"]
pub mod support;

fn benchmark(criterion: &mut Criterion) {
    let connection = support::edge::connection(support::edge::Owner::Routable);
    support::http1::run(
        criterion,
        "transport/http1/edge/routable/plain",
        || support::http1::edge(connection),
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
