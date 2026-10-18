//! The selected route owns dispatch; no engine scan or registry lookup here.

use axum::extract::{Request, State};
use axum::response::Response;

pub async fn serve(
    State(dispatch): State<RouteDispatch>,
    request: Request,
) -> Response {
    let pipeline = match dispatch.entries.find(request.uri().path()) {
        Some(route) => {
            if !route.conditions.iter().all(|condition| {
                condition.accepts(&super::RuleContext {
                    method: request.method(),
                    uri: request.uri(),
                    headers: request.headers(),
                })
            }) {
                return dispatch.rejected.call(request).await;
            }
            &route.pipeline
        }
        None => &dispatch.fallback,
    };
    pipeline.call(request).await
}

/// One prepared application pipeline and its head-only admission conditions.
pub struct ConfiguredRoute {
    pub pipeline: super::AxumPipeline,
    pub conditions: Vec<std::sync::Arc<dyn super::RouteCondition>>,
}

/// An immutable prefix table with explicit miss and rejected-condition fallbacks.
#[derive(Clone)]
pub struct RouteDispatch {
    pub entries: std::sync::Arc<super::RouteTable<ConfiguredRoute>>,
    pub fallback: super::AxumPipeline,
    pub rejected: super::AxumPipeline,
}
