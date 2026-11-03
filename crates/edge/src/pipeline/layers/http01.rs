//! HTTP-01 token replies from a directory pinned during listener preparation.

use super::*;
use claviron_http::ServiceResult;
use http::{Method, StatusCode, Uri, header};
use std::path::PathBuf;
use std::sync::Arc;

// ACME HTTP-01 defines this URL prefix; it is not an operator routing default.
const PREFIX: &str = "/.well-known/acme-challenge/";

pub struct Http01Layer {
    directory: Arc<PathBuf>,
}

pub(crate) enum Http01Reply {
    Status(StatusCode),
    File { path: PathBuf, head: bool },
}

impl Http01Layer {
    pub fn new(directory: impl Into<Arc<PathBuf>>) -> Self {
        Self { directory: directory.into() }
    }

    pub(crate) fn select(
        &self,
        method: &Method,
        uri: &Uri,
    ) -> Option<Http01Reply> {
        let token = uri.path().strip_prefix(PREFIX)?;
        if token.is_empty()
            || !token.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
            })
        {
            return Some(Http01Reply::Status(StatusCode::FORBIDDEN));
        }
        if method != Method::GET && method != Method::HEAD {
            return Some(Http01Reply::Status(StatusCode::METHOD_NOT_ALLOWED));
        }
        Some(Http01Reply::File {
            path: self.directory.join(token),
            head: method == Method::HEAD,
        })
    }
}

impl Http01Reply {
    pub(crate) async fn respond(self) -> RouteResponse {
        match self {
            Self::Status(status) => {
                let mut response = claviron_http::err_status(status);
                if status == StatusCode::METHOD_NOT_ALLOWED {
                    response.headers_mut().insert(
                        header::ALLOW,
                        http::HeaderValue::from_static("GET, HEAD"),
                    );
                }
                response
            }
            Self::File { path, head } => match tokio::fs::read(path).await {
                Ok(bytes) => {
                    let length = bytes.len();
                    let body = if head {
                        claviron_http::empty_body()
                    } else {
                        claviron_http::bytes_body(bytes.into())
                    };
                    let mut response = Response::new(body);
                    let headers = response.headers_mut();
                    headers.insert(
                        header::CONTENT_LENGTH,
                        http::HeaderValue::from(length),
                    );
                    headers.insert(
                        header::CONTENT_TYPE,
                        http::HeaderValue::from_static("text/plain"),
                    );
                    headers.insert(
                        header::CACHE_CONTROL,
                        http::HeaderValue::from_static("no-store"),
                    );
                    response
                }
                Err(_) => claviron_http::err_status(StatusCode::NOT_FOUND),
            },
        }
    }
}

impl<S> HostLayer<S> for Http01Layer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request(move |request| {
            let Some(reply) = self.select(request.method(), request.uri())
            else {
                return DispatchCall::ready(ServiceResult::Unhandled(request));
            };
            drop(request);
            DispatchCall::new(async move {
                ServiceResult::Handled(reply.respond().await)
            })
        });
        Ok(())
    }
}
