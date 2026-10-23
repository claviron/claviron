use std::sync::Arc;

use claviron_http::{
    HttpExchange, RouteBody, RouteResponse, ServiceResult, empty_body,
};
use claviron_php::PhpRuntime;
use http::header::RETRY_AFTER;
use http::{Request, Response, StatusCode};
use tokio::sync::mpsc;

use crate::PhpScriptService;
use crate::engine::PhpSapiRequest;
use crate::error::SapiGatewayError;

pub async fn try_php_proxy(
    svc: Arc<PhpScriptService>,
    runtime: Arc<PhpRuntime>,
    req: Request<RouteBody>,
    exchange_tx: Option<mpsc::Sender<HttpExchange>>,
) -> Result<ServiceResult<RouteResponse, Request<RouteBody>>, SapiGatewayError>
{
    // path segment must be validated before  try_php_proxy
    let path = req.uri().path().to_string();
    let query_string = req.uri().query().unwrap_or("").to_string();

    if runtime.try_files
        && let Some(ext) =
            std::path::Path::new(&path).extension().and_then(|e| e.to_str())
        && !ext.eq_ignore_ascii_case("php")
        && (!runtime.allows_entry_extension(ext)
            || try_files_candidate_exists(&runtime, &path).await?)
    {
        return Ok(ServiceResult::Unhandled(req));
    }

    let Some(sapi_req) =
        resolve_request(&runtime, &path, &query_string).await?
    else {
        return Ok(ServiceResult::Unhandled(req));
    };

    let response = svc
        .execute(runtime, sapi_req, req, exchange_tx)
        .await
        .unwrap_or_else(|err| {
            let status = err.status_code();
            tracing::warn!(
                status = status.as_u16(),
                error = %err,
                "php sapi request failed; returning empty status response"
            );
            error_response(&err)
        });
    Ok(ServiceResult::Handled(response))
}

pub async fn upstream_sapi_gateway(
    svc: Arc<PhpScriptService>,
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

    match try_php_proxy(svc, runtime, req, exchange_tx).await {
        Ok(ServiceResult::Handled(resp)) => resp,
        Ok(ServiceResult::Unhandled(_)) => {
            status_response(StatusCode::NOT_FOUND)
        }
        Err(err) => error_response(&err),
    }
}

async fn resolve_request(
    runtime: &PhpRuntime,
    path: &str,
    query_string: &str,
) -> Result<Option<PhpSapiRequest>, SapiGatewayError> {
    let Some(resolution) = determine_script_name(runtime, path)? else {
        return Ok(None);
    };
    let ScriptResolution { script_name, path_info, from_entry_script } =
        resolution;

    let Some(relative) =
        runtime.script_relative_path(&script_name, from_entry_script)
    else {
        return Ok(None);
    };
    let script_file = safe_join(&runtime.dir, relative)?;

    // INVARIANT `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS` — canonical text in
    // `crates/claviron-php/ARCHITECTURE.md`. This stat is the mixed-folder
    // shape's, not a concession: a folder with no pinned entry can only
    // answer "does this script exist" from the disk. Keep it, and keep it
    // off for `entry_script`.
    //
    // URI-derived `.php` candidates (direct hit, dir-index, embedded
    // `.php/…` segment) are validated against the disk before PHP is
    // invoked. Without this, a request for a missing script reaches
    // libphp.so which emits a fatal `Failed opening required` page —
    // 200 status, full DOCUMENT_ROOT and include_path leaked to the
    // client. Returning `Unhandled` here lets the file_handler fall
    // through to a clean 404.
    //
    // `entry_script` is the operator's pinned entry — missing-file
    // there is a config error, not a request error, so we skip the
    // metadata check on that path.
    if runtime.validate_script
        && !from_entry_script
        && !tokio::fs::metadata(&script_file)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
    {
        return Ok(None);
    }

    // REQUEST_URI / DOCUMENT_URI / QUERY_STRING shape mirrors
    // claviron-php-fcgi's proxy so Symfony / Laravel see the same
    // `$_SERVER` on both runners. The previous unconditional rewrite of
    // REQUEST_URI to `script_name` collapsed `/session` → `/index.php`,
    // which made Symfony's `prepareBaseUrl()` treat SCRIPT_NAME as a
    // prefix of REQUEST_URI and strip it, leaving pathInfo = "/".
    //
    //   PATH_INFO present (`/index.php/foo`): client hit the entry
    //     script directly with extra path; REQUEST_URI = entry + qs,
    //     DOCUMENT_URI = entry.
    //   try_files-style with `entry_query` set: nginx-style rewrite —
    //     REQUEST_URI = entry?expanded_qs, DOCUMENT_URI = entry.
    //   try_files-style without `entry_query` (Laravel default):
    //     preserve the client-visible path so Symfony recovers
    //     pathInfo = `/session` from REQUEST_URI minus SCRIPT_NAME.
    //   Direct `*.php` hit: keep the original path as REQUEST_URI.
    let (document_uri, request_uri, effective_query_string) =
        if path_info.is_some() {
            let doc = script_name.clone();
            let req = if query_string.is_empty() {
                doc.clone()
            } else {
                format!("{doc}?{query_string}")
            };
            (doc, req, query_string.to_string())
        } else if !path.ends_with(".php")
            && let Some(tpl) = &runtime.entry_query
        {
            let qs = tpl.replace("$uri", path).replace("$args", query_string);
            let doc = script_name.clone();
            let req =
                if qs.is_empty() { doc.clone() } else { format!("{doc}?{qs}") };
            (doc, req, qs)
        } else {
            let doc = path.to_string();
            let req = if query_string.is_empty() {
                doc.clone()
            } else {
                format!("{doc}?{query_string}")
            };
            (doc, req, query_string.to_string())
        };

    Ok(Some(PhpSapiRequest {
        script_name,
        script_file,
        document_root: runtime.dir.clone(),
        document_uri,
        request_uri,
        query_string: effective_query_string,
        path_info,
    }))
}

/// Per-request script resolver — fs-free, parallel to
/// [`claviron_php_fcgi::proxy::determine_script_name`]. opcache owns
/// existence/freshness; staying out of the disk here lets
/// `validate_timestamps=0` deliver its full benefit.
///
/// Resolution order:
///   1. Path ending in `.php` — execute the literal script (returns
///      `Some((path, None))`).
///   2. Path containing `/<file>.php/<rest>` — split into script +
///      PATH_INFO without touching disk (string-only segmentation).
///   3. `entry_script` is configured — route extensionless paths and
///      configured `entry_extensions` to it; any remaining suffix
///      becomes PATH_INFO.
///   4. Path ends with `/` or `==/` — append the first `.php` entry
///      from `runtime.indexes` (no fs check; PHP returns 404 if the
///      file is missing).
///
/// `None` ⇒ path is not handled by PHP; the proxy returns Unhandled
/// to let the next layer (file_handler) take it.
/// Outcome of [`determine_script_name`]. `from_entry_script: true`
/// signals "operator-pinned, do not validate against disk"; the
/// other variants are URI-derived and the proxy may want to
/// fs::metadata-check them when `runtime.validate_script` is on.
struct ScriptResolution {
    script_name: String,
    path_info: Option<String>,
    from_entry_script: bool,
}

fn determine_script_name(
    runtime: &PhpRuntime,
    path: &str,
) -> Result<Option<ScriptResolution>, SapiGatewayError> {
    // 1. Direct `.php` hit.
    if path.ends_with(".php") {
        return Ok(Some(ScriptResolution {
            script_name: path.to_string(),
            path_info: None,
            from_entry_script: false,
        }));
    }

    // 2. Embedded `.php/…` segment → split into script + PATH_INFO.
    if let Some((script_name, path_info)) = split_php_segment(path) {
        return Ok(Some(ScriptResolution {
            script_name,
            path_info: Some(path_info),
            from_entry_script: false,
        }));
    }

    // 3. Entry script catches everything else (Laravel/Symfony front
    //    controller pattern). Any remaining suffix becomes PATH_INFO.
    if let Some(script) = &runtime.entry_script {
        let script_name = format!("/{}", script.trim_start_matches('/'));
        let path_info = entry_script_path_info(path, &script_name);
        return Ok(Some(ScriptResolution {
            script_name,
            path_info,
            from_entry_script: true,
        }));
    }

    // 4. Directory request → first `.php` index. URI-derived, so
    //    validate_script applies if the proxy turns it on.
    if path.ends_with('/')
        && let Some(php_index) =
            runtime.indexes.iter().find(|name| name.ends_with(".php"))
    {
        return Ok(Some(ScriptResolution {
            script_name: format!("{path}{php_index}"),
            path_info: None,
            from_entry_script: false,
        }));
    }

    Ok(None)
}

fn entry_script_path_info(
    path: &str,
    script_name: &str,
) -> Option<String> {
    let prefix = format!("{}/", script_name.trim_end_matches('/'));
    path.strip_prefix(&prefix).map(|rest| format!("/{rest}"))
}

/// Split a path of the form `/.../foo.php/extra` into `("/.../foo.php",
/// "/extra")`. Returns `None` if no segment ends with `.php`. fs-free.
fn split_php_segment(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_end_matches('/');
    let segments: Vec<&str> =
        trimmed.split('/').filter(|s| !s.is_empty()).collect();

    for idx in (1..segments.len()).rev() {
        if segments[idx - 1].ends_with(".php") {
            let script = format!("/{}", segments[..idx].join("/"));
            let rest = segments[idx..].join("/");
            let path_info = if rest.is_empty() {
                String::new()
            } else {
                format!("/{rest}")
            };
            return Some((script, path_info));
        }
    }

    None
}

fn safe_join(
    document_root: &std::path::Path,
    script_name: &str,
) -> Result<std::path::PathBuf, SapiGatewayError> {
    let mut path = document_root.to_path_buf();
    for segment in script_name.split('/').filter(|s| !s.is_empty()) {
        if segment == "." || segment == ".." {
            return Err(SapiGatewayError::BadRequest(
                "invalid php script path".to_string(),
            ));
        }
        path.push(segment);
    }
    Ok(path)
}

async fn try_files_candidate_exists(
    runtime: &PhpRuntime,
    path: &str,
) -> Result<bool, SapiGatewayError> {
    let Some(relative) = runtime.relative_path(path) else {
        return Ok(false);
    };
    let file = match safe_join(&runtime.dir, relative) {
        Ok(file) => file,
        Err(SapiGatewayError::BadRequest(_)) => return Ok(true),
        Err(err) => return Err(err),
    };
    Ok(tokio::fs::metadata(file).await.map(|m| m.is_file()).unwrap_or(false))
}

fn status_response(status: StatusCode) -> RouteResponse {
    Response::builder().status(status).body(empty_body()).unwrap()
}

fn error_response(err: &SapiGatewayError) -> RouteResponse {
    let mut builder = Response::builder().status(err.status_code());
    if matches!(err, SapiGatewayError::ServiceUnavailable(_)) {
        builder = builder.header(RETRY_AFTER, "1");
    }
    builder.body(empty_body()).unwrap()
}

#[cfg(test)]
#[path = "../../tests/mapping.rs"]
mod mapping_tests;
