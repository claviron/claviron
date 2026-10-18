//! Owns Axum catch-all mounting and pre-upstream file probing.

use std::fmt;
use std::sync::Arc;

use axum::Router;
use axum::response::Response;
use axum::routing::any;
use claviron_files::try_files_probe;
use claviron_http::{ServiceResult, boxed_body, into_axum_response};
use claviron_runtime::AppState;
use claviron_upstream::UpstreamProvider;
use claviron_upstream::binding::UpstreamApp;
use claviron_upstream::proxy::service::UpstreamService;
use http::StatusCode;

use claviron_http::config::{files::FilesConfig, host::HostUpstreamConfig};
use claviron_upstream::proxy::service::UpstreamRequestPolicy;

#[derive(Clone)]
struct PreparedUpstream {
    app: UpstreamApp,
    files: Arc<FilesConfig>,
    policy: UpstreamRequestPolicy,
}

async fn upstream_axum_handler(
    axum::Extension(prepared): axum::Extension<PreparedUpstream>,
    req: http::Request<axum::body::Body>,
) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path();
    if let Some(service) = prepared.app.select_explicit(path) {
        return call_upstream_service(
            service,
            parts,
            body,
            prepared.policy.allow_h1_upgrade,
            prepared.policy.allow_connect_protocol,
        )
        .await;
    }

    if prepared.files.try_files {
        match try_files_probe(&prepared.files.root, &parts.uri, &parts.headers)
            .await
        {
            ServiceResult::Handled(resp) => return into_axum_response(resp),
            ServiceResult::Unhandled(_) => {}
        }
    }

    let Some(service) = prepared.app.select_root(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    call_upstream_service(
        service,
        parts,
        body,
        prepared.policy.allow_h1_upgrade,
        prepared.policy.allow_connect_protocol,
    )
    .await
}

async fn call_upstream_service(
    service: UpstreamService,
    parts: http::request::Parts,
    body: axum::body::Body,
    allow_h1_upgrade: bool,
    allow_connect_protocol: bool,
) -> Response {
    let req = http::Request::from_parts(parts, boxed_body(body));
    into_axum_response(
        claviron_upstream::binding::call_upstream_service(
            service,
            req,
            allow_h1_upgrade,
            allow_connect_protocol,
        )
        .await,
    )
}

/// Mount HTTP upstream as a low-priority Axum catch-all for one host.
///
/// This binding owns the Axum-specific route shape and the pre-proxy
/// `try_files_probe` decision. The vhost router only decides where this
/// layer sits in the host pipeline.
pub(in crate::pipeline::merged) fn prepare(
    base: Router,
    state: &AppState,
    host_key: &impl fmt::Display,
    config: Option<&HostUpstreamConfig>,
    files: Arc<FilesConfig>,
    policy: UpstreamRequestPolicy,
) -> Router {
    let Some(config) = config else {
        return base;
    };
    let upstream_name = config.name();

    let Some(provider) = state.registry_ref().resolve::<UpstreamProvider>()
    else {
        tracing::error!(
            host = %host_key,
            upstream = upstream_name,
            "host declares upstream but UpstreamProvider is not registered"
        );
        return base;
    };

    let Some(app) = provider.app_for_state(upstream_name, state) else {
        tracing::error!(
            host = %host_key,
            upstream = upstream_name,
            "host declares unknown upstream app/profile"
        );
        return base;
    };

    base.route("/", any(upstream_axum_handler))
        .route("/{*upstream_path}", any(upstream_axum_handler))
        .layer(axum::Extension(PreparedUpstream { app, files, policy }))
}

use axum::response::IntoResponse;
