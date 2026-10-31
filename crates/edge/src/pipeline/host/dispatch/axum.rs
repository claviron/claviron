//! The temporary application adapter over native dispatch.

use super::*;

pub(crate) fn dispatch_router<H>(owner: H) -> ::axum::Router
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    ::axum::Router::new().fallback(move |request: Request<::axum::body::Body>| {
        owner.clone().call(request.map(claviron_http::boxed_body))
            .map(claviron_http::into_axum_response)
    })
}

pub(crate) fn compose_dispatch_router(
    terminal: ::axum::Router,
    steps: Vec<Arc<dyn DispatchStep>>,
    applications: &[Arc<dyn ApplicationInterceptor>],
) -> ::axum::Router {
    if steps.is_empty() {
        return super::super::compose_router(terminal, applications);
    }
    let dispatch = DispatchChain::new(terminal, steps);
    if applications.is_empty() {
        dispatch_router(dispatch)
    } else {
        dispatch_router(ApplicationStage::new(dispatch, applications.to_vec()))
    }
}
