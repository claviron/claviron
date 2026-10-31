use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use claviron::axum::Router;
use claviron::axum::extract::Request;
use claviron::axum::http::{HeaderValue, StatusCode};
use claviron::axum::response::{Html, IntoResponse, Response};
use claviron::axum::routing::{any, get};
use claviron::edge::{Host, HostConfig, Routable};
use claviron::edge::extension::RouteLayer;
use claviron::edge::pipeline::host::{HostHandler, HostHandlerFuture, HostLayerBuilder, HostRequest};
use claviron::http::ServiceResult;
use claviron::runtime::{AppState, Component, registry};
use http_body_util::BodyExt;
use serde::Serialize;

#[derive(Serialize)]
struct Answer<'a> {
    runtime: &'static str,
    engine: &'static str,
    method: &'a str,
    path: &'a str,
    body: Cow<'a, str>,
    requests: u64,
}

#[derive(Default)]
pub struct Native {
    requests: AtomicU64,
}

impl Component<AppState> for Native {
    type Deps = ();

    fn name(&self) -> &'static str {
        "Pentaglot Rust"
    }
}

impl Routable for Native {
    fn match_host(
        &self,
        _key: &Host,
        _host: &HostConfig,
    ) -> bool {
        true
    }

    fn application(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> registry::Result<Option<Arc<dyn HostHandler>>> {
        let root = Arc::clone(&self);
        let echo = Arc::clone(&self);
        let router = Router::new()
            .route("/", get(|| async { Html(include_str!("../../public/index.html")) }))
            .route("/rust", any(move |request| root.clone().answer(request)))
            .route("/rust/echo", any(move |request| echo.clone().answer(request)));
        Ok(Some(Arc::new(NativeRoutes(
            claviron::edge::pipeline::app::application(router),
        ))))
    }

    fn layers(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> registry::Result<Vec<Arc<dyn RouteLayer>>> {
        Ok(vec![Arc::new(WorkerPid(
            HeaderValue::from_str(&std::process::id().to_string()).unwrap(),
        ))])
    }
}

struct NativeRoutes(Arc<dyn HostHandler>);

impl HostHandler for NativeRoutes {
    fn call(&self, request: HostRequest) -> HostHandlerFuture {
        match request.head().uri.path() {
            "/" | "/rust" | "/rust/echo" => self.0.call(request),
            _ => Box::pin(std::future::ready(ServiceResult::Unhandled(request))),
        }
    }
}

struct WorkerPid(HeaderValue);

impl RouteLayer for WorkerPid {
    fn prepare(&self, layer: &mut HostLayerBuilder) -> claviron::edge::Result<()> {
        let pid = self.0.clone();
        layer.after(move |response, _| {
            response.headers.insert("x-pentaglot-pid", pid.clone());
        });
        Ok(())
    }
}

impl Native {
    async fn answer(
        self: Arc<Self>,
        request: Request,
    ) -> Response {
        let (parts, body) = request.into_parts();
        let body = match body.collect().await {
            Ok(body) => body.to_bytes(),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
        claviron::axum::Json(Answer {
            runtime: "rust",
            engine: "native",
            method: parts.method.as_str(),
            path: parts.uri.path(),
            body: String::from_utf8_lossy(&body),
            requests: self.requests.fetch_add(1, Ordering::Relaxed) + 1,
        })
        .into_response()
    }
}
