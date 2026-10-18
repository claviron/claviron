use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, Response, StatusCode};

use crate::{ForeignError, ForeignResult};

#[derive(Clone, Debug)]
pub struct ForeignResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl ForeignResponse {
    pub fn new(
        status: StatusCode,
        body: impl Into<Bytes>,
    ) -> Self {
        Self { status, headers: HeaderMap::new(), body: body.into() }
    }

    pub fn empty(status: StatusCode) -> Self {
        Self::new(status, Bytes::new())
    }

    pub fn bytes(body: impl Into<Bytes>) -> Self {
        Self::new(StatusCode::OK, body)
    }

    pub fn json_bytes(body: impl Into<Bytes>) -> Self {
        Self::bytes(body).with_static_header(CONTENT_TYPE, "application/json")
    }

    pub fn text(body: impl Into<Bytes>) -> Self {
        Self::bytes(body)
            .with_static_header(CONTENT_TYPE, "text/plain; charset=utf-8")
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_parts(self) -> (StatusCode, HeaderMap, Bytes) {
        (self.status, self.headers, self.body)
    }

    pub fn with_header(
        mut self,
        name: HeaderName,
        value: HeaderValue,
    ) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_status(
        mut self,
        status: StatusCode,
    ) -> Self {
        self.status = status;
        self
    }

    pub fn try_with_header(
        self,
        name: impl TryInto<HeaderName>,
        value: impl TryInto<HeaderValue>,
    ) -> ForeignResult<Self> {
        let name = name.try_into().map_err(|_| {
            ForeignError::invalid_response("invalid response header name")
        })?;
        let value = value.try_into().map_err(|_| {
            ForeignError::invalid_response("invalid response header value")
        })?;
        Ok(self.with_header(name, value))
    }

    pub fn ensure_json_content_type(mut self) -> Self {
        self.headers
            .entry(CONTENT_TYPE)
            .or_insert(HeaderValue::from_static("application/json"));
        self
    }

    pub fn into_http_response(self) -> Response<Bytes> {
        let mut response = Response::new(self.body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }

    fn with_static_header(
        self,
        name: HeaderName,
        value: &'static str,
    ) -> Self {
        self.with_header(name, HeaderValue::from_static(value))
    }
}

impl From<Bytes> for ForeignResponse {
    fn from(body: Bytes) -> Self {
        Self::bytes(body)
    }
}

impl From<Vec<u8>> for ForeignResponse {
    fn from(body: Vec<u8>) -> Self {
        Self::bytes(body)
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for ForeignResponse {
    fn into_response(self) -> axum::response::Response {
        let (status, headers, body) = self.into_parts();
        (status, headers, body).into_response()
    }
}

#[cfg(feature = "http")]
impl From<ForeignResponse> for claviron_http::RouteResponse {
    fn from(response: ForeignResponse) -> Self {
        let (status, headers, body) = response.into_parts();
        let mut response = http::Response::new(claviron_http::bytes_body(body));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    }
}
