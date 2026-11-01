//! HTTP-01 file replies, prepared only with an explicit challenge directory.

use std::path::PathBuf;
use claviron_http::ServiceResult;
use super::*;

// ACME HTTP-01 defines this URL prefix; it is not an operator routing default.
const PREFIX: &str = "/.well-known/acme-challenge/";

pub struct Http01Layer {
    directory: PathBuf,
}

impl Http01Layer {
    pub fn new(directory: PathBuf) -> Self { Self { directory } }
}

impl<S> HostLayer<S> for Http01Layer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| {
            let Some(token) = request.uri().path().strip_prefix(PREFIX) else {
                return DispatchFuture::ready(ServiceResult::Unhandled(request));
            };
            if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')) {
                return DispatchFuture::ready(ServiceResult::Handled(
                    claviron_http::err_status(http::StatusCode::FORBIDDEN),
                ));
            }
            let path = self.directory.join(token);
            drop(request);
            DispatchFuture::new(async move {
                let response = match tokio::fs::read(path).await {
                    Ok(bytes) => {
                        let mut response = Response::new(claviron_http::bytes_body(bytes.into()));
                        response.headers_mut().insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("text/plain"));
                        response.headers_mut().insert(http::header::CACHE_CONTROL, http::HeaderValue::from_static("no-store"));
                        response
                    }
                    Err(_) => claviron_http::err_status(http::StatusCode::NOT_FOUND),
                };
                ServiceResult::Handled(response)
            })
        });
        Ok(())
    }
}
