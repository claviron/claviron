//! Warp's terminal service on the shared Rust runner.

use super::ApplicationRegistration;

pub fn register<F>(filter: F) -> ApplicationRegistration
where
    F: ::warp::Filter + Clone + Send + Sync + 'static,
    F::Extract: ::warp::Reply,
    F::Future: 'static,
{
    super::router(move |_| claviron_http::warp::router(filter))
}

#[cfg(test)]
#[path = "../../tests/rust/warp.rs"]
mod tests;
