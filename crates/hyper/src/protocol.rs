use claviron_http::config::{H1Config, HeaderReadTimeout, TcpConfig};
use hyper::server::conn::http1;
use hyper_util::rt::TokioTimer;
use tokio::net::TcpStream;

pub(crate) fn h1(config: &H1Config) -> http1::Builder {
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

pub(crate) fn socket(
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
#[path = "../tests/unit/timeout.rs"]
mod timeout_tests;

#[cfg(test)]
#[path = "../tests/unit/head.rs"]
mod head_tests;
