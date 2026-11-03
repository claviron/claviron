use std::net::IpAddr;
use std::ops::Deref;

use axum::extract::FromRequestParts;

use axum::response::{IntoResponse, Response};

use http::StatusCode;

use super::RealIpExt;
use http::request::Parts;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct RealIp(pub IpAddr);

pub struct IpRejection(StatusCode, &'static str);

impl IntoResponse for IpRejection {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RealIp {
    type Rejection = IpRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ip =
            parts.extensions.get::<RealIpExt>().map(|k| k.0).ok_or(
                IpRejection(StatusCode::BAD_REQUEST, "Missing Real Ip"),
            )?;
        Ok(RealIp(ip))
    }
}

impl Deref for RealIp {
    type Target = IpAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
