//! Early host replies, separate from application dispatch and native framing.

use http::header::LOCATION;
use http::{HeaderValue, Response, StatusCode};

use super::RequestRejection;

/// A completed host decision, not necessarily an error. No request body or
/// transport IO is retained; Hyper/H3 own sending and finishing the reply.
#[derive(Debug, Eq, PartialEq)]
pub enum HostResponse {
    Rejected(RequestRejection),
    #[cfg(feature = "rate-limit")]
    Quota(claviron_http::rate_limit::Rejection),
    Redirect {
        status: StatusCode,
        location: HeaderValue,
    },
}

impl From<RequestRejection> for HostResponse {
    fn from(reason: RequestRejection) -> Self {
        Self::Rejected(reason)
    }
}

impl HostResponse {
    pub fn into_response(self) -> Response<()> {
        let mut response = Response::new(());
        *response.status_mut() = match self {
            #[cfg(feature = "rate-limit")]
            Self::Quota(reply) => return reply.into_response(),
            Self::Rejected(reason) => reason.status(),
            Self::Redirect { status, location } => {
                response.headers_mut().insert(LOCATION, location);
                status
            }
        };
        response
    }
}
