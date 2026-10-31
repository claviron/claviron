//! Prepared Edge host admission and Other dispatch, without transport I/O.

use claviron::edge::transport::{
    RustRouter, OtherHandler, OtherService, RequestArrival,
};
use claviron::http::{RouteBody, RouteFuture, boxed_body};
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use http::Request;
use std::convert::Infallible;

#[path = "../../support/mod.rs"]
pub mod support;

struct Json;

impl OtherHandler for Json {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(
            async move { Ok(support::json::response(request).map(boxed_body)) },
        )
    }
}

fn benchmark(criterion: &mut Criterion) {
    let setup = support::setup::Hosts::load();
    let host = setup.plain_host(RustRouter::Other(OtherService::new(Json)));
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
