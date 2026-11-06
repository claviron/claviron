use claviron_http::config::{H1Config, H2Config, HeaderReadTimeout, TcpConfig};
use hyper::server::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::net::TcpStream;

pub fn h1(config: &H1Config) -> http1::Builder {
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .keep_alive(config.keep_alive)
        .half_close(config.half_close)
        .pipeline_flush(config.pipeline_flush)
        .max_headers(config.max_headers)
        .max_header_size(config.max_header_size)
        .preserve_header_case(false)
        .title_case_headers(false)
        .ignore_invalid_headers(false)
        .auto_date_header(true);
    if let Some(max_buf_size) = config.max_buf_size {
        builder.max_buf_size(max_buf_size);
    }
    match config.header_read_timeout {
        HeaderReadTimeout::DriverDefault => {}
        HeaderReadTimeout::Disabled => {
            builder.header_read_timeout(None);
        }
        HeaderReadTimeout::After(duration) => {
            builder.header_read_timeout(duration);
        }
    }
    builder
}

/// The HTTP/2 connection builder for one set of declared settings.
pub fn h2(
    config: &H2Config,
    enable_connect_protocol: bool,
) -> http2::Builder<TokioExecutor> {
    let mut builder = http2::Builder::new(TokioExecutor::new());
    builder
        .auto_date_header(true)
        .timer(TokioTimer::new())
        .keep_alive_timeout(config.keep_alive_timeout)
        .keep_alive_interval(config.keep_alive_interval)
        .max_concurrent_streams(config.max_concurrent_streams)
        .adaptive_window(true)
        .max_frame_size(config.max_frame_size)
        .max_header_list_size(config.max_header_list_size)
        .max_pending_accept_reset_streams(
            config.max_pending_accept_reset_streams,
        )
        .max_local_error_reset_streams(config.max_local_error_reset_streams)
        .max_send_buf_size(config.max_send_buf_size);
    if enable_connect_protocol {
        builder.enable_connect_protocol();
    }
    builder
}

pub fn socket(
    stream: &TcpStream,
    config: &TcpConfig,
) -> std::io::Result<()> {
    stream.set_nodelay(config.nodelay)?;
    let socket = socket2::SockRef::from(stream);
    match config.keepalive {
        Some(time) => socket
            .set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(time)),
        None => socket.set_keepalive(false),
    }
}

#[cfg(test)]
#[path = "../../tests/tcp/timeout.rs"]
mod timeout_tests;
