//! Prepare registered owners outside timing, without booting providers/engines.
//! The fixture declares no optional host policies or backend execution.

use std::sync::Arc;

use claviron::edge::Host;
use claviron::edge::pipeline::host::{
    HostHandler, HostHandlerFuture, HostRequest,
};
use claviron::edge::pipeline::{HostAppRegistry, Routable};
use claviron::edge::transport::{
    HostConfig, HostService, OtherHandler, OtherService,
};
use claviron::http::{RouteBody, RouteFuture, ServiceResult, boxed_body};
use claviron::runtime::{
    AppState,
    registry::{Component, Result},
};
use http::Request;

use super::{json, plain, setup::Hosts};

struct Json;

impl HostHandler for Json {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let response = json::response(request.into_request()).map(boxed_body);
        Box::pin(std::future::ready(ServiceResult::Handled(response)))
    }
}

struct Plain;

impl OtherHandler for Plain {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(std::future::ready(Ok(
            plain::response(request).map(boxed_body)
        )))
    }
}

impl HostHandler for Plain {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let response = plain::response(request.into_request()).map(boxed_body);
        Box::pin(std::future::ready(ServiceResult::Handled(response)))
    }
}

struct Routes(Arc<dyn HostHandler>);

impl Component<AppState> for Routes {
    type Deps = ();

    fn name(&self) -> &'static str {
        "benchmark-routes"
    }
}

impl Routable for Routes {
    fn match_host(
        &self,
        _: &Host,
        _: &HostConfig,
    ) -> bool {
        true
    }

    fn application(
        self: Arc<Self>,
        _: &AppState,
        _: &Host,
        _: Arc<HostConfig>,
    ) -> Result<Option<Arc<dyn HostHandler>>> {
        Ok(Some(self.0.clone()))
    }
}

fn build(registry: HostAppRegistry) -> HostService {
    let hosts = Hosts::load();
    let state = AppState::standalone_in_fleet("dispatch-bench", "bdbench", ".");
    let applications = registry
        .build(&state, &hosts.vhosts)
        .expect("compile registered benchmark application");
    let application = applications
        .resolve(&json::HOST.parse().unwrap())
        .expect("configured host has its prepared application");
    hosts.plain_host(application)
}

pub fn other() -> HostService {
    let registry = HostAppRegistry::default();
    registry.register_other(json::HOST, |_, _, _| Ok(OtherService::new(Plain)));
    build(registry)
}

#[cfg(feature = "axum")]
pub fn axum(router: claviron::axum::Router) -> HostService {
    let registry = HostAppRegistry::default();
    registry.register_router(json::HOST, move |_| router.clone());
    build(registry)
}

pub fn pipeline() -> HostService {
    let registry = HostAppRegistry::default();
    registry.register_routable(Arc::new(Routes(Arc::new(Plain))));
    build(registry)
}

pub fn json_pipeline() -> HostService {
    let registry = HostAppRegistry::default();
    registry.register_routable(Arc::new(Routes(Arc::new(Json))));
    build(registry)
}

pub enum Owner {
    Other,
    #[cfg(feature = "axum")]
    Axum,
    Routable,
}

pub struct Connection {
    pub handler: claviron::transport::edge::tcp::plain::ConnectionHandler,
    pub listener: Arc<claviron::http::config::ListenerConfig>,
}

/// Register the HTTP defaults and the selected owner without provider boot.
fn publish(
    owner: Owner,
    http: &str,
) -> AppState {
    let state = AppState::standalone_in_fleet("http1-bench", "bh1bench", ".");
    let provider = Arc::new(claviron::edge::HttpConfigProvider::empty(
        state.config_dir().join("http.yaml"),
    ));
    provider
        .replace_from_yaml(http)
        .expect("prepare HTTP defaults from the listener fixture");
    state
        .registry_ref()
        .register(provider)
        .expect("register prepared HTTP defaults without provider boot");
    let registry = Arc::new(HostAppRegistry::default());
    state
        .registry_ref()
        .insert(registry.clone())
        .expect("register application factories without provider boot");
    match owner {
        Owner::Other => registry
            .register_other(json::HOST, |_, _, _| Ok(OtherService::new(Plain))),
        #[cfg(feature = "axum")]
        Owner::Axum => registry
            .register_router(json::HOST, |_| super::axum::plain_router()),
        Owner::Routable => {
            registry.register_routable(Arc::new(Routes(Arc::new(Plain))))
        }
    }
    state
}

/// Publish registered applications with their host table before accepting IO.
/// Only cold preparation is supplied here; request handling is the native driver.
pub fn connection(owner: Owner) -> Connection {
    use claviron::transport::edge::tcp::plain::{HttpServer, HttpServerConfig};

    let hosts = Hosts::http1();
    let state = publish(owner, include_str!("fixtures/http1.yaml"));
    let id = "plain".parse().unwrap();
    let listener = hosts.http.listeners.get(&id).unwrap().clone();
    let config = HttpServerConfig::try_from(hosts.http)
        .expect("valid cleartext listener configuration");
    let handler = HttpServer::empty(state)
        .connection_handler(&config, &id, hosts.vhosts)
        .expect("prepare native HTTP/1 connection handler");
    Connection { handler, listener }
}

#[cfg(feature = "tls")]
pub struct SecureConnection {
    pub handler: claviron::transport::edge::tcp::secure::ConnectionHandler,
    pub listener: Arc<claviron::http::config::ListenerConfig>,
    /// The fleet session table the listener requires. The caller unlinks it
    /// once every connection has finished.
    pub sessions: Arc<claviron::tls::orbit::FleetServerSessions>,
}

/// The same publication behind the native TLS listener. The certificate is
/// the caller's; the session table is the real shared-memory one, opened
/// under a fleet name of this process alone.
#[cfg(feature = "tls")]
pub fn secure_connection(
    owner: Owner,
    certificate: Arc<rustls::sign::CertifiedKey>,
) -> SecureConnection {
    use claviron::tls::orbit::FleetServerSessions;
    use claviron::transport::edge::tcp::secure::{
        HttpsServer, HttpsServerConfig,
    };

    let hosts = Hosts::http1_tls();
    let state = publish(owner, include_str!("fixtures/http1-tls.yaml"));
    let id = "secure".parse().unwrap();
    let listener = hosts.http.listeners.get(&id).unwrap().clone();
    let config = HttpsServerConfig::try_from(hosts.http)
        .expect("valid TLS listener configuration");
    let fleet = claviron::orbitive::Fleet::join(
        &format!("bh{}", std::process::id()),
        1,
    )
    .expect("join the benchmark process's own fleet");
    let sessions = Arc::new(
        FleetServerSessions::open(Arc::new(fleet))
            .expect("open the fleet TLS session table"),
    );
    let certificates = Arc::new(claviron::tls::CertResolver::new(
        [json::HOST, "alias.benchmark.test"]
            .map(|name| (name.to_owned(), certificate.clone()))
            .into(),
        certificate,
    ));
    let handler = HttpsServer::empty(state)
        .connection_handler(
            &config,
            &id,
            certificates,
            hosts.vhosts,
            sessions.clone(),
            Arc::default(),
        )
        .expect("prepare native TLS connection handler");
    SecureConnection { handler, listener, sessions }
}
