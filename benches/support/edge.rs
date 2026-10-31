//! Prepared Edge owners. No provider boot, engine, listener or fleet is started.
//! The fixture enables no optional host policies or backends. `pipeline` goes
//! through HostAppRegistry compilation and one native Routable contribution.

use std::sync::Arc;

use claviron::edge::pipeline::{HostAppRegistry, Routable};
use claviron::edge::pipeline::host::{HostHandler, HostHandlerFuture, HostRequest};
use claviron::edge::transport::{HostConfig, HostService, OtherHandler, OtherService, RustRouter};
use claviron::http::{RouteBody, RouteFuture, ServiceResult, boxed_body};
use claviron::runtime::{AppState, registry::{Component, Result}};
use claviron::edge::Host;
use http::Request;

use super::{json, setup::Hosts};

struct Json;

impl OtherHandler for Json {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(std::future::ready(Ok(json::response(request).map(boxed_body))))
    }
}

impl HostHandler for Json {
    fn call(&self, request: HostRequest) -> HostHandlerFuture {
        let response = json::response(request.into_request()).map(boxed_body);
        Box::pin(std::future::ready(ServiceResult::Handled(response)))
    }
}

struct JsonRoutes;

impl Component<AppState> for JsonRoutes {
    type Deps = ();

    fn name(&self) -> &'static str {
        "benchmark-json"
    }
}

impl Routable for JsonRoutes {
    fn match_host(&self, _: &Host, _: &HostConfig) -> bool {
        true
    }

    fn application(
        self: Arc<Self>,
        _: &AppState,
        _: &Host,
        _: Arc<HostConfig>,
    ) -> Result<Option<Arc<dyn HostHandler>>> {
        Ok(Some(Arc::new(Json)))
    }
}

pub fn other() -> HostService {
    Hosts::load().plain_host(RustRouter::Other(OtherService::new(Json)))
}

#[cfg(feature = "axum")]
pub fn axum() -> HostService {
    Hosts::load().plain_host(RustRouter::Axum(super::axum::router()))
}

pub fn pipeline() -> HostService {
    let hosts = Hosts::load();
    let state = AppState::standalone_in_fleet("dispatch-bench", "bdbench", ".");
    let registry = HostAppRegistry::default();
    registry.register_routable(Arc::new(JsonRoutes));
    let applications = registry.build(&state, &hosts.vhosts)
        .expect("compile the configured native host pipeline");
    let application = applications.resolve(&json::HOST.parse().unwrap())
        .expect("configured host has its prepared application");
    hosts.plain_host(application)
}
