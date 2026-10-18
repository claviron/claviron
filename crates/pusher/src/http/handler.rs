use std::time::Duration;

use claviron_http::forwarding::{
    ForwardedHeaderInfo, ForwardedRequestContext, apply_forwarded_headers,
    original_host_value, reject_request_smuggling, strip_hop_by_hop_headers,
};
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteBody, RouteResponse, boxed_body, empty_body};
use http::{HeaderValue, Request, Response, StatusCode, Uri, header};
use hyper::{self};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, trace};

use crate::HttpTargetConfig;

async fn connect_upstream(
    address: &str,
    connect_timeout: Duration,
) -> Result<TcpStream, StatusCode> {
    let stream = match timeout(connect_timeout, TcpStream::connect(address))
        .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            error!("upstream connect error: address={} error={:?}", address, e);
            return Err(StatusCode::BAD_GATEWAY);
        }
        Err(_) => {
            error!(
                "upstream connect timed out: address={} timeout={:?}",
                address, connect_timeout
            );
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    };
    if let Err(e) = stream.set_nodelay(true) {
        debug!("failed to set TCP_NODELAY: {:?}", e);
    }
    Ok(stream)
}

async fn send_request_with_timeout(
    sender: &mut hyper::client::conn::http1::SendRequest<RouteBody>,
    req: Request<RouteBody>,
    request_timeout: Duration,
) -> Result<RouteResponse, StatusCode> {
    match timeout(request_timeout, sender.send_request(req)).await {
        Ok(Ok(res)) => {
            let (parts, incoming) = res.into_parts();
            Ok(Response::from_parts(parts, boxed_body(incoming)))
        }
        Ok(Err(e)) => {
            error!(
                "upstream send_request error: timeout={:?} error={:?}",
                request_timeout, e
            );
            Err(StatusCode::BAD_GATEWAY)
        }
        Err(_) => {
            error!("upstream request timed out: timeout={:?}", request_timeout);
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}

fn is_benign_eof(err: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    match err.kind() {
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe => {
            true
        }
        _ => {
            let msg = err.to_string();
            msg.contains("close_notify")
                || msg.contains("unexpected eof")
                || msg.contains("UnexpectedEof")
        }
    }
}

async fn pipe_with_idle_timeout(
    client: TokioIo<hyper::upgrade::Upgraded>,
    upstream: TokioIo<hyper::upgrade::Upgraded>,
    idle: Duration,
    log_socket: &str,
    log_ws_path: &str,
) {
    let (mut cr, mut cw) = tokio::io::split(client);
    let (mut ur, mut uw) = tokio::io::split(upstream);

    let mut buf_cu = vec![0u8; 16 * 1024];
    let mut buf_uc = vec![0u8; 16 * 1024];

    loop {
        let idle_sleep = tokio::time::sleep(idle);
        tokio::pin!(idle_sleep);

        tokio::select! {
            _ = &mut idle_sleep => {
                error!(
                    "idle timeout in tunnel: socket={} ws_path={} idle={:?}",
                    log_socket,
                    log_ws_path,
                    idle
                );
                let _ = cw.shutdown().await;
                let _ = uw.shutdown().await;
                break;
            }

            r1 = cr.read(&mut buf_cu) => {
                match r1 {
                    Ok(0) => { let _ = uw.shutdown().await; break; }
                    Ok(n) => {
                        if let Err(e) = uw.write_all(&buf_cu[..n]).await {
                            error!(
                                "write to upstream failed: socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        if is_benign_eof(&e) {
                            trace!(
                                "client closed (benign EOF): socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                        } else {
                            error!(
                                "read from client failed: socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                        }
                        let _ = uw.shutdown().await;
                        break;
                    }
                }
            }

            r2 = ur.read(&mut buf_uc) => {
                match r2 {
                    Ok(0) => { let _ = cw.shutdown().await; break; }
                    Ok(n) => {
                        if let Err(e) = cw.write_all(&buf_uc[..n]).await {
                            error!(
                                "write to client failed: socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        if is_benign_eof(&e) {
                            trace!(
                                "upstream closed (benign EOF): socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                        } else {
                            error!(
                                "read from upstream failed: socket={} ws_path={} error={:?}",
                                log_socket,
                                log_ws_path,
                                e
                            );
                        }
                        let _ = cw.shutdown().await;
                        break;
                    }
                }
            }
        }
    }
}

pub async fn proxy_http(
    req: Request<RouteBody>,
    cfg: &HttpTargetConfig,
) -> Result<RouteResponse, StatusCode> {
    debug!(
        "🔀 handling pusher API request: address={} uri={}",
        cfg.address,
        req.uri()
    );

    let stream = connect_upstream(&cfg.address, cfg.connect_timeout()).await?;

    let io = TokioIo::new(stream);
    let (mut sender, conn) =
        match hyper::client::conn::http1::handshake(io).await {
            Ok(v) => v,
            Err(_) => return Err(StatusCode::BAD_GATEWAY),
        };

    tokio::spawn(async move {
        let _ = conn.await;
    });

    let forwarded = ForwardedRequestContext::from_request(&req);
    let (mut parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_owned();
    parts.uri =
        match Uri::builder().path_and_query(path_and_query.as_str()).build() {
            Ok(uri) => uri,
            Err(_) => return Err(StatusCode::BAD_REQUEST),
        };

    if let Err(err) = reject_request_smuggling(parts.version, &parts.headers) {
        error!("pusher_http rejected request headers: {}", err);
        return Err(StatusCode::BAD_REQUEST);
    }
    let forwarded_host = original_host_value(&parts.headers);
    strip_hop_by_hop_headers(&mut parts.headers);
    apply_forwarded_headers(
        &mut parts.headers,
        ForwardedHeaderInfo {
            real_ip: forwarded.real_ip,
            proto: forwarded.proto.as_ref().map(|scheme| scheme.as_str()),
            host: forwarded_host,
            via: Some("1.1 claviron"),
        },
    );
    parts.headers.remove(header::HOST);
    if let Ok(host) = HeaderValue::from_str(&cfg.address) {
        parts.headers.insert(header::HOST, host);
    }

    debug!(
        "pusher_http proxied request built: address={} path={} headers={:?}",
        cfg.address, path_and_query, parts.headers
    );

    let proxied = Request::from_parts(parts, body);
    let res =
        send_request_with_timeout(&mut sender, proxied, cfg.request_timeout())
            .await?;

    let (mut res_parts, res_body) = res.into_parts();
    strip_hop_by_hop_headers(&mut res_parts.headers);
    Ok(Response::from_parts(res_parts, res_body))
}

pub fn is_ws_upgrade(req: &Request<RouteBody>) -> bool {
    let h = req.headers();
    h.get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
        && h.get(header::CONNECTION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_ascii_lowercase().contains("upgrade"))
            .unwrap_or(false)
}

pub async fn proxy_upgrade(
    mut req: Request<RouteBody>,
    cfg: &HttpTargetConfig,
) -> Result<RouteResponse, StatusCode> {
    let sessions =
        req.extensions().get::<SessionSpawner>().cloned().ok_or_else(|| {
            error!("pusher upgrade request missing native session owner");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    debug!(
        "🔀 handling pusher WS upgrade request: address={} uri={}",
        cfg.address,
        req.uri()
    );

    let forwarded = ForwardedRequestContext::from_request(&req);
    let on_client = hyper::upgrade::on(&mut req);
    let orig_query =
        req.uri().query().map(|q| format!("?{}", q)).unwrap_or_default();
    let log_ws_path = req.uri().path().to_owned();
    let path_q = format!("{}{}", log_ws_path, orig_query);
    let uri = match Uri::builder().path_and_query(path_q.as_str()).build() {
        Ok(uri) => uri,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let stream =
        match connect_upstream(&cfg.address, cfg.connect_timeout()).await {
            Ok(stream) => stream,
            Err(status) => return Err(status),
        };

    let io = TokioIo::new(stream);
    let (mut sender, conn) =
        match hyper::client::conn::http1::handshake(io).await {
            Ok(v) => v,
            Err(_) => return Err(StatusCode::BAD_GATEWAY),
        };

    let (parts, _body) = req.into_parts();
    let mut up_builder = Request::builder()
        .method(parts.method.clone())
        .uri(uri)
        .version(hyper::Version::HTTP_11);
    {
        let hm = up_builder.headers_mut().expect("request builder headers");
        hm.clear();
        for (k, v) in parts.headers.iter() {
            hm.append(k, v.clone());
        }
        let forwarded_host = original_host_value(hm);
        apply_forwarded_headers(
            hm,
            ForwardedHeaderInfo {
                real_ip: forwarded.real_ip,
                proto: forwarded.proto.as_ref().map(|scheme| scheme.as_str()),
                host: forwarded_host,
                via: Some("1.1 claviron"),
            },
        );
        hm.remove(header::HOST);
        if let Ok(host) = HeaderValue::from_str(&cfg.address) {
            hm.insert(header::HOST, host);
        }
    }
    let up_req = up_builder.body(empty_body()).expect("pusher upgrade request");

    // Drive this one-off backend connection in the request future. Cancellation
    // drops its IO directly, and no detached driver survives a failed handshake.
    let send = async {
        let response = send_request_with_timeout(
            &mut sender,
            up_req,
            cfg.request_timeout(),
        )
        .await?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(StatusCode::BAD_GATEWAY);
        }
        Ok(response)
    };
    let driver = conn.with_upgrades();
    tokio::pin!(send, driver);
    let mut up_res = tokio::select! {
        response = &mut send => {
            let response = response?;
            driver.await.map_err(|_| StatusCode::BAD_GATEWAY)?;
            response
        }
        result = &mut driver => {
            result.map_err(|_| StatusCode::BAD_GATEWAY)?;
            send.await?
        }
    };

    let up_headers = up_res.headers().clone();

    let mut client_res = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .version(hyper::Version::HTTP_11);
    {
        let h = client_res.headers_mut().expect("switching protocols headers");
        for (k, v) in up_headers.iter() {
            h.insert(k.clone(), v.clone());
        }
        h.insert("X-Frame-Options", HeaderValue::from_static("SAMEORIGIN"));
        h.insert("X-XSS-Protection", HeaderValue::from_static("1; mode=block"));
        h.insert("X-Content-Type-Options", HeaderValue::from_static("nosniff"));
    }
    let client_res =
        client_res.body(empty_body()).expect("switching protocols response");

    let on_upstream = hyper::upgrade::on(&mut up_res);
    let idle_timeout = cfg.idle_timeout();
    let log_socket = cfg.address.clone();

    sessions.spawn(async move {
        match (on_client.await, on_upstream.await) {
            (Ok(c), Ok(u)) => {
                let c = TokioIo::new(c);
                let u = TokioIo::new(u);
                pipe_with_idle_timeout(c, u, idle_timeout, &log_socket, &log_ws_path).await;
            }
            (Err(e), Ok(u)) => {
                error!(
                    "client upgrade wait failed: socket={} ws_path={} error={:?}",
                    log_socket, log_ws_path, e
                );
                let _ = AsyncWriteExt::shutdown(&mut TokioIo::new(u)).await;
            }
            (Ok(c), Err(e)) => {
                error!(
                    "upstream upgrade wait failed: socket={} ws_path={} error={:?}",
                    log_socket, log_ws_path, e
                );
                let _ = AsyncWriteExt::shutdown(&mut TokioIo::new(c)).await;
            }
            (Err(e1), Err(e2)) => {
                error!(
                    "both upgrades failed: socket={} ws_path={} client_error={:?} upstream_error={:?}",
                    log_socket, log_ws_path, e1, e2
                );
            }
        }
    }).map_err(|error| {
        debug!(%error, "pusher session hand-off rejected");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    Ok(client_res)
}

pub async fn handle_pusher_request(
    cfg: &HttpTargetConfig,
    req: Request<RouteBody>,
) -> RouteResponse {
    let resp = if is_ws_upgrade(&req) {
        proxy_upgrade(req, cfg).await
    } else {
        proxy_http(req, cfg).await
    };

    resp.unwrap_or_else(|status| {
        Response::builder()
            .status(status)
            .body(empty_body())
            .expect("pusher status response")
    })
}
