//! Cleartext HTTP/2 by prior knowledge: a client that speaks it from its first
//! byte, because the path to it is private and agreed in advance. There is no
//! ALPN to choose with; when HTTP/1.1 is accepted too the connection preface
//! decides.

use claviron_http::RouteResponse;
use claviron_http::config::{H1Config, H2Config};
use claviron_http::session::ConnectionScope;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::convert::Infallible;
use tokio::io::{AsyncRead, AsyncWrite};

/// What a cleartext listener accepts besides plain HTTP/1.1.
#[derive(Clone, Debug)]
pub struct PriorKnowledge {
    pub h2: H2Config,
    /// Accept HTTP/1.1 on the same socket, decided per connection.
    pub h1: bool,
}

type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Drive one connection until its peer completes or its scope drains.
pub(crate) async fn drive<I, S>(
    stream: I,
    service: S,
    prior: &PriorKnowledge,
    h1: &H1Config,
    scope: &ConnectionScope,
) -> Option<Result<(), Failure>>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    S: Service<
            http::Request<Incoming>,
            Response = RouteResponse,
            Error = Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    let io = TokioIo::new(stream);
    if !prior.h1 {
        let connection =
            super::protocol::h2(&prior.h2, false).serve_connection(io, service);
        tokio::pin!(connection);
        return tokio::select! {
            biased;
            _ = scope.permit().wait_graceful_shutdown() => {
                connection.as_mut().graceful_shutdown();
                tokio::select! {
                    result = connection.as_mut() => Some(result.map_err(Into::into)),
                    _ = scope.permit().wait_forced_shutdown() => None
                }
            }
            result = connection.as_mut() => Some(result.map_err(Into::into))
        };
    }
    let builder = auto(h1, &prior.h2);
    let connection = builder.serve_connection_with_upgrades(io, service);
    tokio::pin!(connection);
    tokio::select! {
        biased;
        _ = scope.permit().wait_graceful_shutdown() => {
            connection.as_mut().graceful_shutdown();
            tokio::select! {
                result = connection.as_mut() => Some(result),
                _ = scope.permit().wait_forced_shutdown() => None
            }
        }
        result = connection.as_mut() => Some(result)
    }
}

/// Either protocol with the same settings its own builder applies.
fn auto(
    h1: &H1Config,
    h2: &H2Config,
) -> hyper_util::server::conn::auto::Builder<TokioExecutor> {
    use claviron_http::config::HeaderReadTimeout;
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    {
        let mut http1 = builder.http1();
        http1
            .timer(TokioTimer::new())
            .keep_alive(h1.keep_alive)
            .half_close(h1.half_close)
            .pipeline_flush(h1.pipeline_flush)
            .max_headers(h1.max_headers)
            .auto_date_header(true);
        if let Some(max_buf_size) = h1.max_buf_size {
            http1.max_buf_size(max_buf_size);
        }
        match h1.header_read_timeout {
            HeaderReadTimeout::DriverDefault => {}
            HeaderReadTimeout::Disabled => {
                http1.header_read_timeout(None);
            }
            HeaderReadTimeout::After(duration) => {
                http1.header_read_timeout(duration);
            }
        }
    }
    builder
        .http2()
        .auto_date_header(true)
        .timer(TokioTimer::new())
        .keep_alive_timeout(h2.keep_alive_timeout)
        .keep_alive_interval(h2.keep_alive_interval)
        .max_concurrent_streams(h2.max_concurrent_streams)
        .adaptive_window(true)
        .max_frame_size(h2.max_frame_size)
        .max_header_list_size(h2.max_header_list_size)
        .max_pending_accept_reset_streams(h2.max_pending_accept_reset_streams)
        .max_local_error_reset_streams(h2.max_local_error_reset_streams)
        .max_send_buf_size(h2.max_send_buf_size);
    builder
}
