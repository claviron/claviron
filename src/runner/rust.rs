//! A native Rust application using the shared runner launch and ingress.

use std::convert::Infallible;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use claviron_http::RouteBodyError;
use claviron_runner::launch::Launch;
use claviron_runner::rust::{ApplicationRegistration, Registration};
use claviron_runner::{AppRequest, RunnerOptions, launch};
use claviron_runtime::{AppState, Register, ServerName, registry};
use http::Response;
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::Server;

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Configuration {}

/// Build one Rust application without host selection or a language engine.
///
/// `runner.yaml` supplies the shared runner options. Programmatic options win
/// over the file. Worker processes inherit the primary's resolved launch.
#[derive(Default)]
pub struct Runner {
    options: RunnerOptions,
    config_path: Option<PathBuf>,
    registrations: Vec<Registration>,
    application: Option<ApplicationRegistration>,
}

impl Runner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(
        mut self,
        options: RunnerOptions,
    ) -> Self {
        self.options = options;
        self
    }

    /// Use this launch file instead of `runner.yaml` in the working directory.
    pub fn with_config_path(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// Register application services before the terminal service is composed.
    pub fn with_services<C: Register<AppState> + 'static>(self) -> Self {
        self.with_bootstrap(C::register)
    }

    pub fn with_bootstrap<F>(
        mut self,
        register: F,
    ) -> Self
    where
        F: FnOnce(&AppState) -> registry::Result<()> + Send + 'static,
    {
        self.registrations.push(Box::new(register));
        self
    }

    /// Select a host-free RustRouter after service registration.
    pub fn with_router<F, R>(
        mut self,
        router: F,
    ) -> Self
    where
        F: FnOnce(AppState) -> R + Send + 'static,
        R: Into<claviron_http::RustRouter> + 'static,
    {
        self.application = Some(claviron_runner::rust::router(router));
        self
    }

    /// Select another Rust HTTP service without an Axum dependency.
    pub fn with_service<S, B>(
        mut self,
        service: S,
    ) -> Self
    where
        S: Service<AppRequest, Response = Response<B>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        self.application = Some(claviron_runner::rust::other(service));
        self
    }

    /// Select Warp filters using the same application, policy and ingress path.
    #[cfg(feature = "warp")]
    pub fn with_warp<F>(
        mut self,
        filter: F,
    ) -> Self
    where
        F: warp::Filter + Clone + Send + Sync + 'static,
        F::Extract: warp::Reply,
        F::Future: 'static,
    {
        self.application = Some(claviron_runner::rust::warp::register(filter));
        self
    }

    pub async fn run(self) -> crate::Result<()> {
        let config_path =
            self.config_path.clone().unwrap_or_else(|| "runner.yaml".into());
        let options = self.resolve_options(&config_path).await?;
        let application =
            self.application.ok_or("register a Rust application service")?;
        let name = options.name.clone().ok_or("name is required")?;
        let server_name: ServerName = name.parse().map_err(registry_error)?;
        claviron_orbit::link::application::segment::check_application_name(
            &name,
        )
        .map_err(registry_error)?;
        if !options.names_a_socket() && !options.orbit {
            return Err("select orbit, tcp or uds ingress".into());
        }
        let production = options.production;
        let environment = options.environment.clone();
        let mut launch =
            Launch { runner: options, own: Configuration::default() };
        let prepared = launch.prepare(name)?;
        let definitions = launch.clone();
        let registrations = self.registrations;
        let server = if production {
            Server::new().production()
        } else if let Some(environment) = environment {
            Server::new().environment(environment)
        } else {
            Server::new()
        };
        #[cfg(feature = "supervisor")]
        let server = server.workers(prepared.workers);
        let server = server
            .name(server_name)
            .fleet_name(prepared.fleet_name)
            .launch(prepared.envelope)
            .config_dir(
                config_path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .with_definitions(move |state| {
                let definitions = definitions.inherited(state)?;
                #[cfg(feature = "supervisor")]
                claviron_runner::supervisor::register_policy(
                    state,
                    definitions.runner.supervisor.unwrap_or_default(),
                )?;
                #[cfg(not(feature = "supervisor"))]
                let _ = definitions;
                Ok(())
            })
            .register(move |state| {
                let launch = launch.inherited(state)?;
                let root = std::env::current_dir()?;
                launch::register_routing(state, &launch.runner, &root)?;
                #[cfg(feature = "module")]
                if let Some(sites) = state
                    .registry_ref()
                    .resolve::<claviron_pipeline::SiteRegistry>(
                ) {
                    claviron_module::register_routes(state, &sites)?;
                }
                for register in registrations {
                    register(state)?;
                }
                claviron_runner::rust::register(
                    state,
                    launch.runner,
                    application,
                )
            });
        server.serve().await
    }

    async fn resolve_options(
        &self,
        path: &Path,
    ) -> crate::Result<RunnerOptions> {
        #[cfg(feature = "supervisor")]
        if let claviron_supervisor::ProcessEntry::Worker(worker) =
            claviron_supervisor::ProcessEntry::detect()?
        {
            let inherited: Launch<Configuration> =
                serde_json::from_slice(worker.launch().as_bytes())?;
            return Ok(inherited.runner);
        }
        let environment = if self.options.production {
            claviron_runtime::env::PRODUCTION
        } else {
            self.options
                .environment
                .as_deref()
                .unwrap_or(claviron_runtime::env::LOCAL)
        };
        let environment = claviron_runtime::env::ConfigEnvironment::load(
            &claviron_sys::short_hostname(),
            environment,
            Path::new(".env"),
        )
        .map_err(registry_error)?;
        let reader = claviron_runtime::ConfigReader::new()
            .with_builtin_parsers()
            .map_err(registry_error)?;
        let written = reader
            .read_with_variables::<RunnerOptions>(
                path,
                environment.snapshot().values(),
            )
            .await
            .map_err(registry_error)?;
        let options = self.options.clone().over(written);
        options.validate().map_err(registry::Error::msg)?;
        Ok(options)
    }
}

fn registry_error(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}
