//! Axum invocation after framework-independent prepared route selection.

use axum::extract::{Request, State};
use axum::response::Response;

pub async fn serve(
    State(dispatch): State<RouteDispatch>,
    request: Request,
) -> Response {
    let pipeline = dispatch.select(&super::RuleContext {
        method: request.method(),
        uri: request.uri(),
        headers: request.headers(),
    });
    pipeline.call(request).await
}

/// One prepared application pipeline and its head-only admission conditions.
pub type ConfiguredRoute = crate::dispatch::ConfiguredRoute<super::AxumPipeline>;

/// An immutable prefix table with explicit miss and rejected-condition fallbacks.
pub type RouteDispatch = crate::dispatch::RouteDispatch<super::AxumPipeline>;
