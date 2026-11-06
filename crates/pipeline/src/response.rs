//! Early host replies, separate from application dispatch and native framing.

use http::header::LOCATION;
use http::{HeaderValue, Response, StatusCode};

use claviron_http::authority::AuthorityError;

/// Shared decision; native transports retain their wire response/close mechanics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestRejection {
    MissingProtocol,
    MissingIdentity,
    InvalidAuthority(AuthorityError),
    Misdirected,
    AccessDenied,
    AmbiguousPath,
    InvalidRedirect,
    TooEarly,
}

impl RequestRejection {
    pub fn status(self) -> StatusCode {
        match self {
            Self::MissingProtocol
            | Self::MissingIdentity
            | Self::InvalidRedirect => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Misdirected => StatusCode::MISDIRECTED_REQUEST,
            Self::AccessDenied => StatusCode::FORBIDDEN,
            Self::AmbiguousPath | Self::InvalidAuthority(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::TooEarly => StatusCode::TOO_EARLY,
        }
    }
}

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

impl From<HostResponse> for claviron_http::RouteResponse {
    fn from(reply: HostResponse) -> Self {
        reply.into_response().map(|()| claviron_http::empty_body())
    }
}

impl From<RequestRejection> for claviron_http::RouteResponse {
    fn from(reason: RequestRejection) -> Self {
        HostResponse::from(reason).into()
    }
}
