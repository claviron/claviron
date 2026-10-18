use lumenize::axum::extract::{FromRequest, Path, Request, State};
use lumenize::axum::handler::Handler;
use lumenize::axum::http::{StatusCode, header};
use lumenize::axum::response::{IntoResponse, Response};
use lumenize::axum::routing::get;
use lumenize::axum::{Json, Router};
use lumenize::claviron::congif::{ConfigFile, Configurable, Format};
use lumenize::claviron::http::ext::{ConnCtx, HostKey, RealIpExt};
use lumenize::claviron::http::trust::{Forwarded, Origin};
use lumenize::claviron::{AppState, Component, congif, registry};
use lumenize::{Register, Routable, register_routable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::UNIX_EPOCH;

#[derive(Default)]
pub struct DemoRoutableService {
    settings: OnceLock<DemoSettings>,
}

impl DemoRoutableService {
    fn bind<F, T>(self: &Arc<Self>, method: F) -> impl Handler<T, AppState>
    where
        F: DemoMethod<T>,
    {
        method.bind(self.clone())
    }

    fn prepared_settings(&self) -> Result<&DemoSettings, DemoError> {
        self.settings.get().ok_or(DemoError::ServiceUnavailable)
    }

    async fn scheme(self: Arc<Self>) -> Result<String, DemoError> {
        Ok(self.prepared_settings()?.scheme.clone())
    }

    async fn settings(self: Arc<Self>, _req: Request) -> Result<Response, DemoError> {
        Ok(Json(self.prepared_settings()?).into_response())
    }

    async fn scheme_for_host(
        self: Arc<Self>,
        Path(host): Path<String>,
    ) -> Result<String, DemoError> {
        Ok(format!("{}://{host}", self.prepared_settings()?.scheme))
    }

    async fn runtime(
        self: Arc<Self>,
        (State(state), request): (State<AppState>, Request),
    ) -> Response {
        let report = RuntimeReport {
            runtime: "rust",
            service: self.name(),
            application: env!("CARGO_PKG_NAME"),
            application_version: env!("CARGO_PKG_VERSION"),
            runtime_version: lumenize::claviron::runtime::VERSION,
            process: ProcessInfo {
                pid: std::process::id(),
                name: state.name().to_owned(),
                hostname: state.hostname().to_owned(),
                fleet: state.fleet_name().to_owned(),
                run_mode: state.run_mode().as_env_value(),
                worker_id: state.worker_id(),
                worker_count: state.worker_count(),
                node_id: state.fleet_node_id().raw(),
                os: std::env::consts::OS,
                architecture: std::env::consts::ARCH,
                debug_build: cfg!(debug_assertions),
                environment: state.environment().to_owned(),
                production: state.is_production(),
                started_at: state
                    .runtime_started_at()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|time| time.as_secs()),
                uptime_seconds: state.runtime_uptime().as_secs_f64(),
                reload_count: state.reload_count(),
            },
            request: RequestInfo::from_request(&request),
            // Deliberately not a dump of the captured environment: it contains secrets.
            environment: public_environment(state.env_snapshot().values()),
        };
        let mut response = Json(report).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
        response
    }
}

#[derive(Serialize)]
struct RuntimeReport {
    runtime: &'static str,
    service: &'static str,
    application: &'static str,
    application_version: &'static str,
    runtime_version: &'static str,
    process: ProcessInfo,
    request: RequestInfo,
    environment: BTreeMap<&'static str, String>,
}

#[derive(Serialize)]
struct ProcessInfo {
    pid: u32,
    name: String,
    hostname: String,
    fleet: String,
    run_mode: &'static str,
    worker_id: Option<u8>,
    worker_count: u8,
    node_id: u16,
    os: &'static str,
    architecture: &'static str,
    debug_build: bool,
    environment: String,
    production: bool,
    started_at: Option<u64>,
    uptime_seconds: f64,
    reload_count: usize,
}

#[derive(Serialize)]
struct RequestInfo {
    method: String,
    path: String,
    http_version: String,
    scheme: Option<String>,
    client_ip: Option<IpAddr>,
    client_address: Option<SocketAddr>,
    server_address: Option<SocketAddr>,
    country: Option<String>,
    host_key: Option<String>,
    forwarded_context: bool,
    connection: Option<ConnectionInfo>,
    headers: BTreeMap<&'static str, String>,
}

impl RequestInfo {
    fn from_request(request: &Request) -> Self {
        let extensions = request.extensions();
        let origin = Origin::of(extensions);
        let connection = extensions.get::<Arc<ConnCtx>>();
        let headers = [
            "host",
            "user-agent",
            "accept",
            "accept-language",
            "content-type",
            "content-length",
            "origin",
        ]
        .into_iter()
        .filter_map(|name| {
            request
                .headers()
                .get(name)?
                .to_str()
                .ok()
                .map(|value| (name, value.to_owned()))
        })
        .collect();
        Self {
            method: request.method().to_string(),
            path: request.uri().path().to_owned(),
            http_version: format!("{:?}", request.version()),
            scheme: origin.scheme.map(|scheme| scheme.to_string()),
            client_ip: extensions
                .get::<RealIpExt>()
                .map(|ip| ip.0)
                .or_else(|| origin.client.map(|client| client.ip())),
            client_address: origin.client,
            server_address: origin.server,
            country: connection.and_then(|connection| connection.country_code.clone()),
            host_key: extensions.get::<HostKey>().map(|host| host.0.to_string()),
            forwarded_context: extensions.get::<Forwarded>().is_some(),
            connection: connection.map(|connection| ConnectionInfo {
                listener: connection.listener.to_string(),
                protocol: connection.proto.map(|protocol| protocol.version()),
                remote: connection.remote,
                local: SocketAddr::new(connection.local_ip, connection.local_port),
                tls: connection.tls().map(|tls| TlsDetails {
                    protocol: tls.ssl_protocol(),
                    cipher: tls.ssl_cipher(),
                    client_verify: tls.ssl_client_verify(),
                }),
            }),
            headers,
        }
    }
}

#[derive(Serialize)]
struct ConnectionInfo {
    listener: String,
    protocol: Option<&'static str>,
    remote: SocketAddr,
    local: SocketAddr,
    tls: Option<TlsDetails>,
}

#[derive(Serialize)]
struct TlsDetails {
    protocol: Option<String>,
    cipher: Option<String>,
    client_verify: &'static str,
}

fn public_environment(
    values: &std::collections::HashMap<String, String>,
) -> BTreeMap<&'static str, String> {
    [
        "APP_NAME",
        "APP_ENV",
        "APP_DEBUG",
        "RUST_LOG",
        "TZ",
        "CLAVIRON_HOSTNAME",
    ]
    .into_iter()
    .filter_map(|name| values.get(name).map(|value| (name, value.clone())))
    .collect()
}

/// Local prototype: bind a service method while leaving extraction to Axum.
trait DemoMethod<T> {
    fn bind(self, service: Arc<DemoRoutableService>) -> impl Handler<T, AppState>;
}

impl<F, Fut, R> DemoMethod<((),)> for F
where
    F: Fn(Arc<DemoRoutableService>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
{
    fn bind(self, service: Arc<DemoRoutableService>) -> impl Handler<((),), AppState> {
        move || self(service.clone())
    }
}

impl<F, Fut, R, M, A> DemoMethod<(M, A)> for F
where
    F: Fn(Arc<DemoRoutableService>, A) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    A: FromRequest<AppState, M> + Send + 'static,
{
    fn bind(self, service: Arc<DemoRoutableService>) -> impl Handler<(M, A), AppState> {
        move |argument: A| self(service.clone(), argument)
    }
}

#[derive(Debug)]
enum DemoError {
    ServiceUnavailable,
}
impl IntoResponse for DemoError {
    fn into_response(self) -> Response {
        match self {
            DemoError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            // DemoError::Other => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, PartialEq, Clone)]
pub struct DemoSettings {
    scheme: String,
}

impl Component<AppState> for DemoRoutableService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "DemoService"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
}

impl Register<AppState> for DemoRoutableService {
    fn register(state: &AppState) -> registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        // Registers the component and adds its routes in front of PHP.
        register_routable(state, Arc::new(DemoRoutableService::default()))
    }
}

#[congif::async_trait]
impl Configurable for DemoRoutableService {
    type Config = DemoSettings;

    fn source(&self) -> ConfigFile {
        ConfigFile {
            path: PathBuf::from("config/demo.php"),
            format: Format::Custom(String::from("php")),
        }
    }

    async fn configure(&self, config: &Self::Config) -> congif::Result<()> {
        self.settings
            .set(config.clone())
            .map_err(|_| congif::Error::msg("DemoService is already configured"))
    }
}

impl Routable for DemoRoutableService {
    fn routes(self: Arc<Self>, _state: &AppState) -> registry::Result<Option<Router<AppState>>> {
        Ok(Some(
            Router::new()
                .route("/demo-service/settings", get(self.bind(Self::settings)))
                .route("/demo-service/runtime", get(self.bind(Self::runtime)))
                .route("/demo-service/scheme", get(self.bind(Self::scheme)))
                .route(
                    "/demo-service/scheme/{host}",
                    get(self.bind(Self::scheme_for_host)),
                ),
        ))
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/routable.rs"]
mod tests;
