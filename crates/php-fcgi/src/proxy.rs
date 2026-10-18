use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use claviron_http::ext::{ConnCtx, HostName, RealIpExt, TlsInfo};
use claviron_http::policy::BodyNeed;
use claviron_http::{
    HttpExchange, RouteBody, RouteResponse, ServiceResult, empty_body,
};
use claviron_php::PhpRuntime;
use claviron_runtime::RunMode;
use fastcgi_client::Params;
use futures_util::StreamExt;
use futures_util::future::poll_fn;
use http::request::Parts;
use http::{HeaderMap, Method, Request, Response, StatusCode, Version, header};
use http_body::Body as _;
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;
use tracing::{debug, error};

use crate::error::GatewayError;
use crate::service::{PhpFcgiService, Stdin};

pub async fn try_fcgi_proxy(
    svc: Arc<PhpFcgiService>,
    runtime: Arc<PhpRuntime>,
    req: Request<RouteBody>,
    exchange_tx: Option<mpsc::Sender<HttpExchange>>,
) -> Result<ServiceResult<RouteResponse, Request<RouteBody>>, GatewayError> {
    // path segment must be validated before  try_fcgi_proxy
    let path = req.uri().path();

    // `try_files=true` lets static files win before PHP. Dotted,
    // non-`.php` URLs normally fall through to file_handler and then
    // 404; operators can opt rare front-controller routes such as
    // `/users/avatar/{hash}.png` into entry-script fallback with
    // `entry_extensions`.
    if runtime.try_files
        && let Some(ext) =
            std::path::Path::new(path).extension().and_then(|e| e.to_str())
        && !ext.eq_ignore_ascii_case("php")
        && (!runtime.allows_entry_extension(ext)
            || try_files_candidate_exists(&runtime, path).await)
    {
        return Ok(ServiceResult::Unhandled(req));
    }

    let resolution = match determine_script_name(
        path,
        runtime.entry_script.as_ref(),
        &runtime.indexes,
    ) {
        Some(r) => r,
        None => return Ok(ServiceResult::Unhandled(req)),
    };

    // PhpRuntime carries an already-resolved `dir` (composed at boot from
    // the host's docroot when the per-route override was absent). We avoid
    // cloning the PathBuf here — `runtime` already owns it via Arc, and
    // `script_file` only needs a single owned `PathBuf`.
    let script_file =
        runtime.dir.join(resolution.script_name.trim_start_matches('/'));

    // INVARIANT `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS` — canonical text in
    // `crates/claviron-php/ARCHITECTURE.md`. Same reasoning as the embed
    // backend's copy of this check: structural for a folder with no pinned
    // entry, never applied to `entry_script`.
    //
    // URI-derived `.php` candidates are validated against disk before
    // FPM is invoked, otherwise FPM emits "No input file specified" /
    // "Failed opening required" pages back at the client (often with
    // 200 status) leaking DOCUMENT_ROOT and include_path. Returning
    // Unhandled lets the file_handler fall through to a clean 404.
    // `entry_script` is operator-pinned and not validated.
    if runtime.validate_script
        && !resolution.from_entry_script
        && !tokio::fs::metadata(&script_file)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
    {
        return Ok(ServiceResult::Unhandled(req));
    }

    let resp = execute_fcgi(
        svc,
        req,
        runtime,
        resolution.script_name,
        script_file,
        exchange_tx,
    )
    .await?;
    Ok(ServiceResult::Handled(resp))
}

/// See `claviron-php-embed::proxy::ScriptResolution` — same shape and
/// rationale (`from_entry_script` flags operator-pinned entries that
/// skip the validate-against-disk step).
struct ScriptResolution {
    script_name: String,
    from_entry_script: bool,
}

#[inline]
fn determine_script_name(
    path: &str,
    entry_script: Option<&String>,
    indexes: &[String],
) -> Option<ScriptResolution> {
    if path.ends_with(".php") {
        return Some(ScriptResolution {
            script_name: path.to_string(),
            from_entry_script: false,
        });
    }

    if let Some(script) = entry_script {
        return Some(ScriptResolution {
            script_name: format!("/{}", script.trim_start_matches('/')),
            from_entry_script: true,
        });
    }

    if path.ends_with('/') {
        if let Some(php_index) =
            indexes.iter().find(|name| name.ends_with(".php"))
        {
            let implicit = format!("{}{}", path, php_index);
            debug!("Using implicit PHP index for dir {} → {}", path, implicit);
            return Some(ScriptResolution {
                script_name: implicit,
                from_entry_script: false,
            });
        }
        return None;
    }

    None
}

pub async fn upstream_fcgi_gateway(
    svc: Arc<PhpFcgiService>,
    runtime: Arc<PhpRuntime>,
    req: Request<RouteBody>,
    exchange_tx: Option<mpsc::Sender<HttpExchange>>,
) -> RouteResponse {
    let path = req.uri().path();

    if let Some(prefix) = &runtime.path
        && !prefix.has_path_segment(path)
    {
        return status_response(StatusCode::NOT_FOUND);
    }

    match try_fcgi_proxy(svc, runtime, req, exchange_tx).await {
        Ok(ServiceResult::Handled(resp)) => resp,
        Ok(ServiceResult::Unhandled(_)) => {
            status_response(StatusCode::NOT_FOUND)
        }
        Err(err) => status_response(err.status_code()),
    }
}

async fn execute_fcgi(
    svc: Arc<PhpFcgiService>,
    req: Request<RouteBody>,
    runtime: Arc<PhpRuntime>,
    script_name: String,
    script_file: PathBuf,
    exchange_tx: Option<mpsc::Sender<HttpExchange>>,
) -> Result<RouteResponse, GatewayError> {
    let (parts, body) = req.into_parts();
    let ctx =
        RequestCtx::from_parts(&parts, &runtime, &script_name, &script_file)?;

    // Over HTTP/3 a client that dies without a word is seen only when QUIC's
    // idle timeout fires, so a streamed reply holds its FPM child that long:
    // the HTTP/3 listener's `quic.idle_timeout` is this lease's length.
    let started_at = Instant::now();

    // TODO(audit): `exchange_tx` is dropped because the reply flows
    // straight to the HTTP client and never lands in a `Bytes` we could
    // capture; it did only on the buffered path, which is gone. It feeds
    // the termination-middleware audit, a manual feature. When it is
    // needed again, tee the mpsc body pump (one arm to the HTTP body, one
    // to a collector).
    let _ = exchange_tx;
    exec_streaming(svc, parts, body, &runtime, &ctx, started_at).await
}

async fn try_files_candidate_exists(
    runtime: &PhpRuntime,
    path: &str,
) -> bool {
    let mut file = runtime.dir.clone();
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        if segment == "." || segment == ".." {
            return true;
        }
        file.push(segment);
    }
    tokio::fs::metadata(file).await.map(|m| m.is_file()).unwrap_or(false)
}

struct RequestCtx {
    method: Method,
    headers: HeaderMap,
    version: Version,
    request_uri: String,
    document_uri: String,
    document_root: PathBuf,
    query_string: String,
    path_info: Option<String>,
    request_host: String,
    remote_addr: String,
    server_addr: String,
    server_port: u16,
    remote_port: u16,
    remote_ip: String,
    country_code: String,
    tls: Option<TlsInfo>,
    scheme: http::uri::Scheme,
    script_name: String,
    script_file: PathBuf,
}

impl RequestCtx {
    fn from_parts(
        parts: &Parts,
        runtime: &PhpRuntime,
        script_name: &str,
        script_file: &std::path::Path,
    ) -> Result<Self, GatewayError> {
        let exts = &parts.extensions;
        let Some(conn) = exts.get::<Arc<ConnCtx>>().cloned() else {
            return Err(GatewayError::Internal(
                "ConnCtx required for php-fcgi".to_string(),
            ));
        };
        let Some(real_ip) = exts.get::<RealIpExt>().cloned() else {
            return Err(GatewayError::Internal(
                "RealIp required for php-fcgi".to_string(),
            ));
        };
        let Some(hostname) = exts.get::<HostName>().cloned() else {
            return Err(GatewayError::Internal(
                "Hostname required for php-fcgi".to_string(),
            ));
        };

        let uri = &parts.uri;
        let path = uri.path();
        let request_uri = uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| path.to_string());
        let query_string = uri.query().unwrap_or("").to_string();
        let document_uri = path.to_string();

        let path_info: Option<String> =
            if let Some(entry) = &runtime.entry_script {
                let pat = format!("/{}/", entry.trim_start_matches('/'));
                path.strip_prefix(&pat).map(|rest| format!("/{}", rest))
            } else {
                None
            };

        let (
            effective_request_uri,
            effective_document_uri,
            effective_query_string,
        ) = if path_info.is_some() {
            let doc = runtime
                .entry_script
                .as_ref()
                .map(|s| format!("/{}", s.trim_start_matches('/')))
                .unwrap_or_else(|| path.to_string());
            let q = query_string.clone();
            let req = if q.is_empty() {
                doc.clone()
            } else {
                format!("{}?{}", doc, q)
            };
            (req, doc, q)
        } else if !path.ends_with(".php") {
            if let Some(tpl) = &runtime.entry_query {
                let sn = script_name.to_string();
                let orig_args = uri.query().unwrap_or("");
                let qs = tpl.replace("$uri", path).replace("$args", orig_args);
                let doc = sn.clone();
                let req =
                    if qs.is_empty() { sn } else { format!("{}?{}", sn, qs) };
                (req, doc, qs)
            } else {
                (request_uri, document_uri.clone(), query_string)
            }
        } else {
            (request_uri, document_uri.clone(), query_string)
        };

        let request_host = hostname.0.to_string();
        let remote_addr = real_ip.0.to_string();

        Ok(Self {
            method: parts.method.clone(),
            headers: parts.headers.clone(),
            version: parts.version,
            request_uri: effective_request_uri,
            document_uri: effective_document_uri,
            document_root: runtime.dir.clone(),
            query_string: effective_query_string,
            path_info,
            request_host,
            remote_addr,
            server_addr: conn.local_ip().to_string(),
            server_port: conn.local_port(),
            remote_port: conn.remote_port(),
            remote_ip: conn.remote_ip().to_string(),
            country_code: conn.country_code().unwrap_or_default().to_string(),
            tls: conn.tls().cloned(),
            scheme: conn.scheme(),
            script_name: script_name.to_string(),
            script_file: script_file.to_path_buf(),
        })
    }
}

fn build_params(
    ctx: &RequestCtx,
    runtime: &PhpRuntime,
    content_length: usize,
) -> Params<'static> {
    let proto = match ctx.version {
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/1.1",
    };

    let mut params = Params::default()
        .request_method(ctx.method.as_str().to_owned())
        .script_name(ctx.script_name.clone())
        .script_filename(ctx.script_file.to_string_lossy().into_owned())
        .document_root(ctx.document_root.to_string_lossy().into_owned())
        .request_uri(ctx.request_uri.clone())
        .document_uri(ctx.document_uri.clone())
        .query_string(ctx.query_string.clone())
        .server_protocol(proto.to_owned())
        .server_name(ctx.request_host.clone())
        .server_addr(ctx.server_addr.clone())
        .server_port(ctx.server_port)
        .remote_addr(ctx.remote_addr.clone())
        .remote_port(ctx.remote_port)
        .content_length(content_length)
        .custom("HTTP_VIA", ctx.remote_ip.clone())
        .custom("CLIENT_COUNTRY", ctx.country_code.clone())
        .custom(
            "HTTPS",
            if ctx.scheme == http::uri::Scheme::HTTPS { "on" } else { "off" },
        )
        .custom("SERVER_SOFTWARE", runtime.server_software.clone())
        .custom("CLAVIRON_HOSTNAME", runtime.server_hostname.clone())
        .custom("CLAVIRON_RUNNER", run_identifier(runtime.server_run_mode))
        .custom(
            "CLAVIRON_WORKER_COUNT",
            runtime.server_worker_count.to_string(),
        )
        .custom("CLAVIRON_PHP_RUNNER", runtime.php_runner.as_str());

    params = add_tls_params(params, ctx.tls.as_ref());

    if let Some(pi) = &ctx.path_info {
        params = params.custom("PATH_INFO", pi.clone());
    }

    params = params
        .custom("HTTP_HOST", ctx.request_host.clone())
        .custom("REQUEST_SCHEME", ctx.scheme.as_str().to_owned())
        .custom("HTTP_X_FORWARDED_HOST", ctx.request_host.clone())
        .custom("HTTP_X_FORWARDED_PROTO", ctx.scheme.as_str().to_owned());

    if let Some(ct) =
        ctx.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok())
    {
        params = params.content_type(ct.to_owned());
    }

    for (k, v) in runtime.environment.iter() {
        params = params.custom(k.clone(), v.clone());
    }

    for (name, value) in ctx.headers.iter() {
        let name_str = name.as_str();
        if claviron_php::headers::is_request_denied(name_str) {
            continue;
        }
        if let Ok(val) = value.to_str() {
            params = params.custom(to_cgi_http_key(name_str), val.to_owned());
        }
    }

    // HTTP/2/3 split-Cookie folding — delegates to the shared HTTP
    // helper so embed and fcgi don't drift.
    if let Some(joined) =
        claviron_http::resolvers::join_cookie_header(&ctx.headers)
    {
        params = params.custom("HTTP_COOKIE", joined);
    }

    params
}

fn add_tls_params(
    mut params: Params<'static>,
    tls: Option<&TlsInfo>,
) -> Params<'static> {
    let Some(tls) = tls else {
        return params;
    };

    if let Some(protocol) = tls.ssl_protocol() {
        params = params.custom("SSL_PROTOCOL", protocol);
    }
    if let Some(cipher) = tls.ssl_cipher() {
        params = params.custom("SSL_CIPHER", cipher);
    }
    params =
        params.custom("SSL_CLIENT_VERIFY", tls.ssl_client_verify().to_string());
    if let Some(cert) = tls.client_cert.as_ref() {
        params = params
            .custom("SSL_CLIENT_S_DN", cert.subject_dn.clone())
            .custom("SSL_CLIENT_I_DN", cert.issuer_dn.clone());
    }

    params
}

fn to_cgi_http_key(name: &str) -> String {
    let up = name.to_ascii_uppercase().replace('-', "_");
    format!("HTTP_{}", up)
}

async fn exec_streaming(
    svc: Arc<PhpFcgiService>,
    parts: Parts,
    body: RouteBody,
    php_cfg: &PhpRuntime,
    ctx: &RequestCtx,
    started_at: Instant,
) -> Result<RouteResponse, GatewayError> {
    if parts.extensions.get::<claviron_http::policy::HttpPolicy>().is_none() {
        return Err(GatewayError::Internal(
            "HttpPolicy is required for PHP FastCGI execution".into(),
        ));
    }
    let policy = php_cfg.policy(&parts);
    if claviron_http::body::declared_length_within(
        &parts.headers,
        policy.requests.max_body_size,
    )
    .is_err()
    {
        return Ok(status_response(StatusCode::PAYLOAD_TOO_LARGE));
    }
    // FPM reads STDIN only up to `CONTENT_LENGTH`, so a body goes as it
    // arrives only when its length is declared; one of unknown length is
    // held and counted first. Held whole, it can also be sent again on a
    // stale keep-alive connection. Which of the two the policy chooses.
    let (stdin, content_length) = if body.is_end_stream() {
        (Stdin::Whole(Bytes::new()), 0)
    } else {
        let declared = body.size_hint().exact();
        let whole = policy
            .requests
            .delivery(BodyNeed::Length, declared, false)
            .takes_whole(declared);
        match declared {
            Some(len) if !whole => {
                let max_bytes = policy.requests.max_body_size;
                let mut total = 0_usize;
                let stream = http_body_util::BodyStream::new(Box::pin(body))
                    .map(move |frame| {
                        let data = frame
                            .map_err(|error| std::io::Error::other(format!("{error:?}")))?
                            .into_data()
                            .unwrap_or_default();
                        if let Some(limit) = max_bytes {
                            total = total.checked_add(data.len()).filter(|total| *total <= limit)
                                .ok_or_else(|| std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    claviron_http::body::BodyLimit::TooLarge { limit },
                                ))?;
                        }
                        Ok::<_, std::io::Error>(data)
                    });
                (Stdin::Live(Box::new(StreamReader::new(stream))), len as usize)
            }
            _ => {
                let bytes =
                    match collect_body(body, policy.requests.max_body_size)
                        .await
                    {
                        Ok(bytes) => bytes,
                        Err(error)
                            if matches!(
                            error
                                .downcast_ref::<claviron_http::body::BodyLimit>(
                                ),
                            Some(
                                claviron_http::body::BodyLimit::TooLarge { .. }
                            )
                        ) =>
                        {
                            return Ok(status_response(
                                StatusCode::PAYLOAD_TOO_LARGE,
                            ));
                        }
                        Err(error) => {
                            return Err(GatewayError::Internal(format!(
                                "request body read error: {error}"
                            )));
                        }
                    };
                let len = bytes.len();
                (Stdin::Whole(bytes), len)
            }
        }
    };

    let params = build_params(ctx, php_cfg, content_length);

    let (status, out_headers, route_body) =
        svc.fcgi_execute_stream(&php_cfg.fpm, params, stdin).await.map_err(|err| {
            error!(
                "PHP exec error (stream): host={} uri={} real_ip={} elapsed={}ms error={}",
                ctx.request_host,
                ctx.request_uri,
                ctx.remote_addr,
                started_at.elapsed().as_millis(),
                err
            );
            GatewayError::Execute(err)
        })?;

    let mut res = Response::new(route_body);
    *res.status_mut() = status;
    let dst = res.headers_mut();
    for (k, v) in out_headers.iter() {
        dst.append(k.clone(), v.clone());
    }

    Ok(claviron_http::body::ends_at_shutdown(
        res,
        svc.shutdown_token().cancelled_owned(),
    ))
}

fn status_response(status: StatusCode) -> RouteResponse {
    Response::builder().status(status).body(empty_body()).unwrap()
}

async fn collect_body(
    body: RouteBody,
    max_bytes: Option<usize>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Box::pin(body);
    let mut buf = BytesMut::new();

    loop {
        let frame_opt = poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
        let Some(frame_res) = frame_opt else {
            break;
        };

        let frame = frame_res?;

        if let Some(data) = frame.data_ref() {
            if let Some(limit) = max_bytes
                && buf.len().saturating_add(data.len()) > limit
            {
                return Err(Box::new(
                    claviron_http::body::BodyLimit::TooLarge { limit },
                ));
            }
            buf.extend_from_slice(data);
        } else if frame.is_trailers() {
            let _ = frame.into_trailers().ok();
        } else {
            return Err(
                std::io::Error::other("invalid request body frame").into()
            );
        }
    }

    Ok(buf.freeze())
}

#[inline]
/// Render a process-instance label for `CLAVIRON_RUNNER`. Stable
/// for the lifetime of the claviron process, lets PHP / observers tell
/// which claviron instance handled a given request — and combined with
/// `CLAVIRON_FCGI_CONN_ID` (pool-scoped) yields a globally unique
/// connection key across a supervised fleet.
///
/// Values:
/// * `"standalone"` — single-process mode (no supervisor, no children)
/// * `"worker_<N>"` — child process N spawned by the supervisor
/// * `"master"`     — supervisor itself (does not handle requests in
///   practice; emitted for completeness if a request
///   ever lands here)
fn run_identifier(run_mode: RunMode) -> String {
    match run_mode {
        RunMode::Standalone => "standalone".to_string(),
        RunMode::Master => "master".to_string(),
        RunMode::Worker(wid) => format!("worker_{}", wid.raw()),
    }
}

#[cfg(test)]
#[path = "../tests/proxy.rs"]
mod tests;
