use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use claviron_http::config::statics::{DYNAMIC_FILE_EXTS, StaticConfig};
use claviron_http::{
    RequestContext, RouteResponse, ServiceResult, bytes_body, empty_body,
};
use claviron_runtime::AppState;
use http::header::{ETAG, LAST_MODIFIED};
use http::{HeaderMap, HeaderValue, Response, StatusCode, Uri};
use percent_encoding::percent_decode_str;
use tracing::{debug, error};

use super::error::FileServeError;
use super::response::status_response;
use crate::caching::condition::{ConditionalRequest, PreconditionOutcome};
use crate::caching::metrics;
use crate::caching::policy::{
    CachePolicy, apply_client_cache_headers, compose_static_policy,
};
use crate::caching::service::{
    fast_cache_hit, maybe_read_and_cache, try_serve_from_cache,
};
use crate::mime::make_etag_from_meta;
use crate::site::FileSite;
use crate::stream::stream_from_disk;

pub async fn try_handle_files(
    _state: &AppState,
    cfg: &dyn FileSite,
    req: RequestContext<'_>,
) -> Result<ServiceResult<RouteResponse>, StatusCode> {
    let cfg = cfg.route_for(req.uri.path()).unwrap_or(cfg);
    let root = cfg.root();

    if req.uri.path().ends_with("/") && cfg.has_static_index() {
        return serve_index(cfg, req.uri).await.map(ServiceResult::Handled);
    }

    if let Some(resp) = serve_static_mounts(cfg, &req).await {
        return Ok(ServiceResult::Handled(resp));
    }

    // General filesystem fallback is opt-in and stays behind dynamic runners.
    if cfg.try_files() {
        match try_files_for_site(cfg, req.uri, req.headers).await {
            ServiceResult::Handled(resp) => {
                return Ok(ServiceResult::Handled(resp));
            }
            ServiceResult::Unhandled(_) => {}
        }
    }

    if let Some(spa) = cfg.spa() {
        let options = cfg.spa_config();
        if options.is_some_and(|options| options.reject_ajax)
            && is_ajax(req.headers)
        {
            error!(
                host = req
                    .uri
                    .host()
                    .or_else(|| req
                        .headers
                        .get(http::header::HOST)
                        .and_then(|value| value.to_str().ok()))
                    .unwrap_or(""),
                path = req.uri.path(),
                "AJAX request reached SPA fallback"
            );
            return Ok(ServiceResult::Handled(reject_spa_ajax()));
        }
        let path = root.join(spa.trim_start_matches('/'));
        let policy = CachePolicy::spa(
            options.and_then(|options| options.caching.as_ref()),
        );
        let resp = serve_spa(&root, path, policy).await;
        return Ok(ServiceResult::Handled(resp));
    }

    Ok(ServiceResult::Unhandled(()))
}

fn is_ajax(headers: &HeaderMap) -> bool {
    headers
        .get_all("x-requested-with")
        .iter()
        .any(|value| value.as_bytes().eq_ignore_ascii_case(b"XMLHttpRequest"))
}

fn reject_spa_ajax() -> RouteResponse {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::CACHE_CONTROL, "no-store")
        .body(bytes_body(Bytes::from_static(br#"{"message":"Not Found"}"#)))
        .expect("static SPA rejection response")
}

/// One mount already selected by the caller's routing table. The request URI
/// is unchanged: prefix removal, decoding and traversal protection stay here.
pub struct StaticMount {
    root: PathBuf,
    config: StaticConfig,
}

impl StaticMount {
    pub fn new(
        root: PathBuf,
        config: StaticConfig,
    ) -> Self {
        Self { root, config }
    }

    pub async fn try_handle(
        &self,
        req: RequestContext<'_>,
    ) -> ServiceResult<RouteResponse> {
        match serve_static(
            &self.root,
            req.uri.path(),
            &self.config,
            req.headers,
        )
        .await
        {
            Some(response) => ServiceResult::Handled(response),
            None => ServiceResult::Unhandled(()),
        }
    }
}

async fn serve_static_mounts(
    cfg: &dyn FileSite,
    req: &RequestContext<'_>,
) -> Option<RouteResponse> {
    let root = cfg.root();
    if let Some(statics) = cfg.statics() {
        let req_path = req.uri.path();
        for cfg in statics {
            if !cfg.path.has_path_segment(req_path) {
                continue;
            }

            if let Some(resp) =
                serve_static(&root, req_path, cfg, req.headers).await
            {
                return Some(resp);
            }
        }
    }

    None
}

/// Explicit mounts only: no index, redirect, try_files or SPA probe.
#[cfg(feature = "axum")]
pub(crate) async fn try_handle_static_mounts(
    cfg: &dyn FileSite,
    req: RequestContext<'_>,
) -> Result<ServiceResult<RouteResponse>, StatusCode> {
    let cfg = cfg.route_for(req.uri.path()).unwrap_or(cfg);
    Ok(match serve_static_mounts(cfg, &req).await {
        Some(response) => ServiceResult::Handled(response),
        None => ServiceResult::Unhandled(()),
    })
}

pub async fn try_files_probe(
    root: &Path,
    uri: &Uri,
    headers: &HeaderMap,
) -> ServiceResult<RouteResponse> {
    match try_files(root, uri.path(), headers).await {
        Some(resp) => ServiceResult::Handled(resp),
        None => ServiceResult::Unhandled(()),
    }
}

/// Probe the selected route's directory using the path below its URL prefix.
/// Both merged and bypass callers use this without rewriting the request URI.
pub async fn try_files_for_site(
    site: &dyn FileSite,
    uri: &Uri,
    headers: &HeaderMap,
) -> ServiceResult<RouteResponse> {
    let site = site.route_for(uri.path()).unwrap_or(site);
    let path = match site.mapping() {
        Some(mapping) => match mapping.relative_path(uri.path()) {
            Some(path) => path,
            None => return ServiceResult::Unhandled(()),
        },
        None => uri.path(),
    };
    match try_files(&site.root(), path, headers).await {
        Some(response) => ServiceResult::Handled(response),
        None => ServiceResult::Unhandled(()),
    }
}

async fn serve_spa(
    root: &Path,
    path: PathBuf,
    policy: CachePolicy,
) -> RouteResponse {
    serve_with_policy(&path, root, policy)
        .await
        .unwrap_or_else(|err| status_response(err.status_code()))
}

async fn serve_index(
    cfg: &dyn FileSite,
    uri: &Uri,
) -> Result<RouteResponse, StatusCode> {
    let root = cfg.root();

    // This path had no check at all before: unlike the other two entry points
    // it took `Uri::path()` to disk verbatim, so a plain (unencoded) `..` in a
    // directory request was resolved by the OS at open time and served any file
    // named by `indexes()` outside the root.
    let relative = match cfg.mapping() {
        Some(mapping) => {
            mapping.relative_path(uri.path()).ok_or(StatusCode::FORBIDDEN)?
        }
        None => uri.path(),
    };
    let Some(rel) = decode_relative_path(relative.trim_start_matches('/'))
    else {
        return Err(StatusCode::FORBIDDEN);
    };
    let base_dir = root.join(&rel);

    for rel in cfg.indexes().iter().map(|s| s.trim_start_matches('/')) {
        // INVARIANT `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS` — canonical text in
        // `crates/claviron-php/ARCHITECTURE.md`. A `.php` index belongs to the
        // PHP resolver, which finds it by name. Skip it here by name too,
        // without building a path or touching the disk for it.
        //
        // This used to fall through to `serve_with_policy` and come back
        // `DynamicFile`, which was treated as a logic error and answered 500.
        // `has_static_index()` admits a list as soon as *one* entry is static,
        // so the ordinary `index: [index.php, index.html]` reached here, hit
        // `index.php` first, and 500'd every directory request on the host —
        // the file service runs ahead of PHP in the bypass chain and its `Err`
        // ends the request there.
        if is_dynamic_file_path(Path::new(rel)) {
            continue;
        }

        let path = base_dir.join(rel);
        match serve_with_policy(&path, &root, CachePolicy::no_cache()).await {
            Ok(resp) => return Ok(resp),
            Err(FileServeError::NotFound)
            | Err(FileServeError::DynamicFile) => continue,
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }

    debug!("📄 No index file found for {:?}", base_dir);
    Err(StatusCode::NOT_FOUND)
}

async fn serve_static(
    root: &Path,
    req_path: &str,
    config: &StaticConfig,
    headers: &HeaderMap,
) -> Option<RouteResponse> {
    let mapping = claviron_http::config::mapping::RouteMapping::new(
        config.path.clone(),
        root.join(&config.dir),
    );
    let rel = mapping.relative_path(req_path)?;

    if rel.is_empty() {
        return None;
    }

    let Some(rel) = decode_relative_path(rel) else {
        return Some(status_response(StatusCode::FORBIDDEN));
    };

    let base = mapping.directory();

    let path = base.join(&rel);
    let policy = compose_static_policy(config, &rel);

    match serve_with_policy_and_condition(
        &path,
        base,
        policy,
        ConditionalRequest::from_headers(headers),
    )
    .await
    {
        Ok(resp) => Some(resp),
        Err(FileServeError::NotFound) if config.none_found => None,
        Err(FileServeError::DynamicFile) => None,
        Err(e) => Some(status_response(e.status_code())),
    }
}

async fn try_files(
    root: &Path,
    request_path: &str,
    headers: &HeaderMap,
) -> Option<RouteResponse> {
    let rel = request_path.trim_start_matches('/');
    if rel.is_empty() {
        return None;
    }

    let Some(rel) = decode_relative_path(rel) else {
        debug!("file service denied suspicious path (path={})", request_path);
        return Some(status_response(StatusCode::FORBIDDEN));
    };

    // The extension test belongs after decoding, like every other check here.
    Path::new(&rel).extension()?;

    let path = root.join(&rel);

    match serve_with_policy_and_condition(
        &path,
        root,
        CachePolicy::default_server_cache(),
        ConditionalRequest::from_headers(headers),
    )
    .await
    {
        Ok(resp) => Some(resp),
        Err(FileServeError::NotFound) => None,
        Err(FileServeError::DynamicFile) => None,
        Err(e) => Some(status_response(e.status_code())),
    }
}

/// INVARIANT `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS` — canonical text in
/// `crates/claviron-php/ARCHITECTURE.md`.
///
/// This is the name-only test that keeps a PHP path out of the file service
/// before any IO happens. It runs first in `serve_with_policy_and_condition`,
/// ahead of the cache lookup and ahead of `symlink_metadata`, and it gates the
/// index loop above. Do not move a `stat` in front of it: a path that belongs
/// to PHP would then be probed twice, once here and once in the resolver, for
/// an answer opcache already holds.
#[inline]
pub(crate) fn is_dynamic_file_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| {
            DYNAMIC_FILE_EXTS.iter().any(|d| ext.eq_ignore_ascii_case(d))
        })
        .unwrap_or(false)
}

/// Percent-decode a request-relative path and reject everything that must not
/// reach the filesystem, returning the decoded path on success.
///
/// `Uri::path()` is still percent-encoded and nothing upstream of this crate
/// decodes it, so checking the raw form is wrong in both directions: it does
/// not see the `..` in `%2e%2e%2f`, and it refuses to serve a file whose name
/// contains a space because on the wire that is `%20`.
///
/// Decoding happens per segment, so a decoded separator can never introduce
/// one. That matters beyond traversal: the mount prefix is stripped from the
/// *encoded* path, so if `%2f` could split a segment the router and the file
/// service would disagree about the shape of what was requested. Apache and
/// nginx refuse encoded slashes by default for the same reason.
fn decode_relative_path(rel: &str) -> Option<String> {
    let mut out = String::with_capacity(rel.len());

    for (i, segment) in rel.split('/').enumerate() {
        let decoded = percent_decode_str(segment).decode_utf8().ok()?;

        // `%2f`/`%5c` are not separators, and a NUL would truncate the path
        // inside any C API that eventually opens it.
        if decoded.contains('/')
            || decoded.contains('\\')
            || decoded.contains('\0')
        {
            return None;
        }

        // Now that this is the real name: traversal (`.`, `..`) and dotfiles.
        if decoded.starts_with('.') {
            return None;
        }

        if i > 0 {
            out.push('/');
        }
        out.push_str(&decoded);
    }

    Some(out)
}

async fn serve_with_policy(
    path: &PathBuf,
    confine_to: &Path,
    policy: CachePolicy,
) -> Result<RouteResponse, FileServeError> {
    serve_with_policy_and_condition(
        path,
        confine_to,
        policy,
        ConditionalRequest::default(),
    )
    .await
}

/// Does `path` still land inside `confine_to` once every symlink on it is
/// resolved?
///
/// Called only when the final component is a symlink, so the extra `realpath`
/// never touches an ordinary request.
async fn resolves_within(
    path: &Path,
    confine_to: &Path,
) -> bool {
    let (Ok(target), Ok(base)) = (
        tokio::fs::canonicalize(path).await,
        tokio::fs::canonicalize(confine_to).await,
    ) else {
        return false;
    };

    // Component-wise, so `/srv/wwwroot` does not pass as being under `/srv/www`.
    target.starts_with(&base)
}

async fn serve_with_policy_and_condition(
    path: &PathBuf,
    confine_to: &Path,
    policy: CachePolicy,
    cond: ConditionalRequest,
) -> Result<RouteResponse, FileServeError> {
    // #[cfg(debug_assertions)]
    // debug!("serving static file: {}, policy: {:?}", path.display(), policy);

    if is_dynamic_file_path(path) {
        return Err(FileServeError::DynamicFile);
    }

    if let Some(resp) = fast_cache_hit(path, &policy, &cond) {
        metrics::fast_hit();
        return Ok(resp);
    }

    match tokio::fs::symlink_metadata(path).await {
        Ok(mut meta) => {
            let ft = meta.file_type();
            if ft.is_symlink() {
                // Symlinks are followed, as they are in nginx and Apache by
                // default — but only to somewhere the operator already exposes.
                // A link out of the serving root is the one way a request can
                // leave it now that the decoder refuses `..` and encoded
                // separators.
                //
                // Limitation, stated rather than hidden: this resolves the
                // final component. A symlinked *directory* partway along the
                // path is still followed unchecked, the same way both of those
                // servers follow it.
                if !resolves_within(path, confine_to).await {
                    debug!(
                        "file service denied a symlink out of {}",
                        confine_to.display()
                    );
                    return Err(FileServeError::Forbidden);
                }
                match tokio::fs::metadata(path).await {
                    Ok(tmeta) if tmeta.is_file() => {
                        meta = tmeta;
                    }
                    _ => {
                        return Err(FileServeError::Forbidden);
                    }
                }
            } else if !meta.is_file() {
                return Err(FileServeError::Forbidden);
            }

            let modified = meta.modified().ok();
            let etag = make_etag_from_meta(&meta);
            match cond.pre.evaluate_get_or_head(etag.as_deref(), modified) {
                PreconditionOutcome::NotModified => {
                    let mut resp = Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .body(empty_body())
                        .unwrap();
                    if let Some(etag) = etag.as_deref() {
                        resp.headers_mut()
                            .insert(ETAG, HeaderValue::from_str(etag).unwrap());
                    }
                    if let Some(modified) = modified {
                        let last_modified = httpdate::fmt_http_date(modified);
                        resp.headers_mut().insert(
                            LAST_MODIFIED,
                            HeaderValue::from_str(&last_modified).unwrap(),
                        );
                    }
                    apply_client_cache_headers(resp.headers_mut(), &policy);
                    return Ok(resp);
                }
                PreconditionOutcome::Failed => {
                    return Ok(status_response(
                        StatusCode::PRECONDITION_FAILED,
                    ));
                }
                PreconditionOutcome::Proceed => {}
            }

            if policy.server_ttl_secs.is_some()
                && let Some(resp) =
                    try_serve_from_cache(path, &policy, &meta, &cond)
            {
                metrics::hit();
                return Ok(resp);
            }

            let last_modified = modified.map(httpdate::fmt_http_date);
            if !cond.honors_range(
                meta.len(),
                etag.as_deref(),
                last_modified.as_deref(),
            ) && let Some(resp) =
                maybe_read_and_cache(path, &meta, &policy, &cond).await
            {
                metrics::miss();
                return Ok(resp);
            }

            Ok(stream_from_disk(path, &meta, &policy, &cond).await)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            Err(FileServeError::NotFound)
        }
        Err(_) => Err(FileServeError::Internal),
    }
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
