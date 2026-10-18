use std::sync::Arc;

#[cfg(feature = "axum")]
use axum::response::{IntoResponse, Response};
use claviron_types::Host;
#[cfg(feature = "axum")]
use http::StatusCode;

#[cfg(feature = "axum")]
pub struct ServerRejection(StatusCode, &'static str);

#[cfg(feature = "axum")]
impl IntoResponse for ServerRejection {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

#[derive(Clone, Debug)]
pub struct HostName(pub Arc<Host>);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HostKey(pub Arc<Host>);
