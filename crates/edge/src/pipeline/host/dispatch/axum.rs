//! Explicit Axum adaptation of a native application owner.

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
