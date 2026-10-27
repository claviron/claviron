use std::fs::{File as StdFile, Metadata};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use claviron_http::{
    RouteBody, RouteResponse, boxed_body, bytes_body, empty_body,
};
use futures_util::TryStreamExt;
use http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
    LAST_MODIFIED,
};
use http::{HeaderValue, Response, StatusCode};
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;
use tracing::debug;

use super::condition::{ConditionalRequest, PreconditionOutcome};
use super::policy::CachePolicy;
use super::{
    CachedEntry, ContentEncoding, FILE_CACHE, FileCachedEntry, MEMORY_CACHE,
    TOTAL_CACHE_BYTES, compute_file_hash, file_cache_path, metrics,
    unreserve_cache_bytes,
};
use crate::config;
use crate::mime::{make_etag_from_meta, resolve_content_type};
use crate::response::{
    build_ok_response, cached_not_modified_response, status_response,
};

#[inline]
fn try_reserve_cache_bytes(
    len: u64,
    max_bytes: u64,
) -> bool {
    loop {
        let current = TOTAL_CACHE_BYTES.load(Ordering::Relaxed);
        let next = current.saturating_add(len);

        if next > max_bytes {
            return false;
        }

        if TOTAL_CACHE_BYTES
            .compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            metrics::sync_memory_cache();
            return true;
        }
    }
}

#[inline]
fn cached_response_body(
    cond: &ConditionalRequest,
    entry: &CachedEntry,
) -> (RouteBody, Option<ContentEncoding>) {
    if let (Some(encoding), Some(encoded_body)) =
        (entry.encoding, entry.encoded_body.as_ref())
        && cond.accepts_encoding(encoding)
    {
        (bytes_body(encoded_body.as_ref().clone()), Some(encoding))
    } else {
        (bytes_body(entry.body.as_ref().clone()), None)
    }
}

#[inline]
fn accepted_policy_encoding(
    policy: &CachePolicy,
    cond: &ConditionalRequest,
) -> Option<ContentEncoding> {
    let comp_cfg = policy.compression.as_ref()?;
    let encoding = ContentEncoding::from_algorithm(comp_cfg.algorithm);
    cond.accepts_encoding(encoding).then_some(encoding)
}

#[inline]
pub(crate) fn fast_cache_hit(
    path: &PathBuf,
    policy: &CachePolicy,
    cond: &ConditionalRequest,
) -> Option<RouteResponse> {
    #[cfg(feature = "orbit")]
    if policy.server_ttl_secs.is_some()
        && !policy.validate_timestamps
        && let Some(resp) = super::arena::hit(path, policy, cond, None)
    {
        return Some(resp);
    }

    if policy.server_ttl_secs.is_some()
        && !policy.validate_timestamps
        && let Some(cached) = MEMORY_CACHE.get(path)
    {
        let c = cached.value();
        if c.expires_at > Instant::now() {
            match cond.pre.evaluate_get_or_head(Some(&c.etag), Some(c.mtime)) {
                PreconditionOutcome::NotModified => {
                    return Some(cached_not_modified_response(
                        c.etag.as_str(),
                        c.last_modified.as_str(),
                    ));
                }
                PreconditionOutcome::Failed => {
                    return Some(status_response(
                        StatusCode::PRECONDITION_FAILED,
                    ));
                }
                PreconditionOutcome::Proceed => {}
            }
            if cond.honors_range(Some(&c.etag), Some(&c.last_modified)) {
                return match cond.range_bounds(c.len) {
                    Some((start, end)) => {
                        Some(build_cached_partial_response(c, start, end))
                    }
                    None => {
                        let mut resp = Response::builder()
                            .status(StatusCode::RANGE_NOT_SATISFIABLE)
                            .body(empty_body())
                            .unwrap();
                        resp.headers_mut().insert(
                            CONTENT_RANGE,
                            HeaderValue::from_str(&format!(
                                "bytes */{}",
                                c.len
                            ))
                            .unwrap(),
                        );
                        resp.headers_mut().insert(
                            ACCEPT_RANGES,
                            HeaderValue::from_static("bytes"),
                        );
                        resp.headers_mut().insert(
                            ETAG,
                            HeaderValue::from_str(&c.etag).unwrap(),
                        );
                        Some(resp)
                    }
                };
            }
            let (body, encoding) = cached_response_body(cond, c);

            let mut resp = build_ok_response(
                policy,
                &c.content_type,
                &c.last_modified,
                body,
            );
            resp.headers_mut()
                .insert("x-cache", HeaderValue::from_static("HIT+"));

            if let Some(encoding) = encoding {
                resp.headers_mut().insert(
                    http::header::CONTENT_ENCODING,
                    HeaderValue::from_static(encoding.token()),
                );
            }

            resp.headers_mut()
                .insert(ETAG, HeaderValue::from_str(&c.etag).unwrap());
            return Some(resp);
        } else {
            // Expired: treat as a miss and let the caller rebuild/stream.
            // Do not remove here; GC or subsequent insert will replace.
            drop(cached);
        }
    }

    // Check file cache (large files)
    if policy.server_ttl_secs.is_some()
        && !policy.validate_timestamps
        && let Some(cached) = FILE_CACHE.get(path)
    {
        let c = cached.value();
        if c.expires_at > Instant::now() && cond.accepts_encoding(c.encoding) {
            match cond.pre.evaluate_get_or_head(Some(&c.etag), Some(c.mtime)) {
                PreconditionOutcome::NotModified => {
                    return Some(cached_not_modified_response(
                        c.etag.as_str(),
                        c.last_modified.as_str(),
                    ));
                }
                PreconditionOutcome::Failed => {
                    return Some(status_response(
                        StatusCode::PRECONDITION_FAILED,
                    ));
                }
                PreconditionOutcome::Proceed => {}
            }
            // TODO: Handle Range requests
            return Some(serve_file_cache_sync(
                &c.encoded_file,
                &c.content_type,
                &c.last_modified,
                &c.etag,
                c.encoding,
                policy,
            ));
        }
    }

    None
}

/// Internal implementation of try_serve_from_cache with optional If-None-Match header.
#[inline]
pub(crate) fn try_serve_from_cache(
    path: &PathBuf,
    policy: &CachePolicy,
    meta: &Metadata,
    cond: &ConditionalRequest,
) -> Option<RouteResponse> {
    #[cfg(feature = "orbit")]
    if let Some(resp) = super::arena::hit(path, policy, cond, Some(meta)) {
        return Some(resp);
    }

    if let Some(cached) = MEMORY_CACHE.get(path) {
        let c = cached.value();
        if c.expires_at > Instant::now() {
            if let Ok(m) = meta.modified() {
                // validate via mtime when metadata is provided (opcache-style)
                if c.mtime == m {
                    if cond.honors_range(Some(&c.etag), Some(&c.last_modified))
                    {
                        return match cond.range_bounds(c.len) {
                            Some((start, end)) => Some(
                                build_cached_partial_response(c, start, end),
                            ),
                            None => {
                                let mut resp = Response::builder()
                                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                    .body(empty_body())
                                    .unwrap();
                                resp.headers_mut().insert(
                                    CONTENT_RANGE,
                                    HeaderValue::from_str(&format!(
                                        "bytes */{}",
                                        c.len
                                    ))
                                    .unwrap(),
                                );
                                resp.headers_mut().insert(
                                    ACCEPT_RANGES,
                                    HeaderValue::from_static("bytes"),
                                );
                                resp.headers_mut().insert(
                                    ETAG,
                                    HeaderValue::from_str(&c.etag).unwrap(),
                                );
                                Some(resp)
                            }
                        };
                    }
                    let (body, encoding) = cached_response_body(cond, c);

                    let mut resp = build_ok_response(
                        policy,
                        &c.content_type,
                        &c.last_modified,
                        body,
                    );
                    resp.headers_mut()
                        .insert("x-cache", HeaderValue::from_static("HIT"));

                    if let Some(encoding) = encoding {
                        resp.headers_mut().insert(
                            http::header::CONTENT_ENCODING,
                            HeaderValue::from_static(encoding.token()),
                        );
                    }

                    resp.headers_mut()
                        .insert(ETAG, HeaderValue::from_str(&c.etag).unwrap());
                    return Some(resp);
                }
            }
        } else {
            // Expired: treat as a miss and let the caller rebuild/stream.
            // Do not remove here; GC or subsequent insert will replace.
            drop(cached);
        }
    }

    // Check file cache (large files) with timestamp validation
    if let Some(cached) = FILE_CACHE.get(path) {
        let c = cached.value();
        if c.expires_at > Instant::now() {
            // Validate mtime if policy requires
            if let Ok(m) = meta.modified()
                && m == c.mtime
                && cond.accepts_encoding(c.encoding)
            {
                return Some(serve_file_cache_sync(
                    &c.encoded_file,
                    &c.content_type,
                    &c.last_modified,
                    &c.etag,
                    c.encoding,
                    policy,
                ));
            }
        }
    }

    None
}

/// RAII guard to ensure cache budget is released even if future is cancelled
struct CacheBudgetGuard(u64);

impl Drop for CacheBudgetGuard {
    fn drop(&mut self) {
        unreserve_cache_bytes(self.0);
    }
}

/// Read full file into memory and cache it, returning the ready Response. Skips if too large or budget unavailable.
///
/// Uses RAII guard to prevent cache budget leaks if:
/// - tokio::fs::read is cancelled (client disconnect)
/// - File read fails
/// - Metadata read fails
#[inline]
pub(crate) async fn maybe_read_and_cache(
    path: &PathBuf,
    meta: &Metadata,
    policy: &CachePolicy,
    cond: &ConditionalRequest,
) -> Option<RouteResponse> {
    let config = config::active();
    let max_body_size = config.memory.max_body_size;
    // The fleet tier takes every small body when it is there; the
    // per-process tier below is what a standalone process is left with.
    #[cfg(feature = "orbit")]
    if policy.server_ttl_secs.is_some_and(|ttl| ttl > 0)
        && meta.len() < max_body_size
        && super::arena::active().is_some()
    {
        return super::arena::read_and_store(path, meta, policy).await;
    }

    if let Some(ttl) = policy.server_ttl_secs
        && ttl > 0
        && meta.len() < max_body_size
        && try_reserve_cache_bytes(meta.len(), config.memory.max_bytes)
    {
        // Guard ensures unreserve is called on drop (cancel/error)
        let guard = CacheBudgetGuard(meta.len());

        // blocking ops must be awaited by caller; we perform async read here
        // SAFETY: called from async context
        return match tokio::fs::read(path).await {
            Ok(buf) => {
                debug!("⚡️ caching: path = {}", path.display());
                let body_bytes = Arc::new(Bytes::from(buf));
                let ct = resolve_content_type(path.as_path());
                match meta.modified() {
                    Ok(modified) => {
                        let since = httpdate::fmt_http_date(modified);
                        let etag =
                            make_etag_from_meta(meta).unwrap_or_else(|| {
                                format!("W/\"{:x}-0\"", meta.len())
                            });

                        let (encoded_body, encoding, encoded_len) =
                            if let Some(comp_cfg) = &policy.compression {
                                if meta.len() >= comp_cfg.min_size.as_u64() {
                                    let encoding =
                                        ContentEncoding::from_algorithm(
                                            comp_cfg.algorithm,
                                        );
                                    match compress_body(
                                        &body_bytes,
                                        comp_cfg.level,
                                        encoding,
                                    ) {
                                        Ok(compressed) => {
                                            let compressed_len =
                                                compressed.len() as u64;
                                            debug!(
                                                "🗜️  {}: {} -> {} bytes ({:.1}%)",
                                                encoding.label(),
                                                meta.len(),
                                                compressed_len,
                                                (compressed_len as f64
                                                    / meta.len() as f64)
                                                    * 100.0
                                            );
                                            (
                                                Some(Arc::new(compressed)),
                                                Some(encoding),
                                                compressed_len,
                                            )
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "{} compression failed: {}",
                                                encoding.label(),
                                                e
                                            );
                                            (None, None, 0)
                                        }
                                    }
                                } else {
                                    (None, None, 0)
                                }
                            } else {
                                (None, None, 0)
                            };

                        let entry = Arc::new(CachedEntry {
                            mtime: modified,
                            len: meta.len(),
                            body: body_bytes.clone(),
                            content_type: ct.clone(),
                            last_modified: since.clone(),
                            expires_at: Instant::now()
                                + Duration::from_secs(ttl),
                            etag: etag.clone(),
                            encoded_body,
                            encoding,
                            encoded_len,
                        });
                        if let Some(old) =
                            MEMORY_CACHE.insert(path.clone(), entry)
                        {
                            unreserve_cache_bytes(old.len);
                        }
                        metrics::sync_memory_cache();

                        // Success - prevent guard from unreserving (entry now owns the budget)
                        std::mem::forget(guard);

                        let body = bytes_body(body_bytes.as_ref().clone());
                        let mut resp =
                            build_ok_response(policy, &ct, &since, body);
                        resp.headers_mut().insert(
                            "x-cache",
                            HeaderValue::from_static("MISS"),
                        );
                        resp.headers_mut().insert(
                            ETAG,
                            HeaderValue::from_str(&etag).unwrap(),
                        );
                        Some(resp)
                    }
                    Err(_) => {
                        // Guard will unreserve on drop
                        None
                    }
                }
            }
            Err(_) => {
                // Guard will unreserve on drop
                None
            }
        };
    }

    // Large file path: use file cache if compression enabled
    // No server_ttl required - file cache is disk-based, cheap, invalidated by mtime
    if meta.len() >= max_body_size
        && accepted_policy_encoding(policy, cond).is_some()
    {
        return maybe_read_and_file_cache(path, meta, policy, &config.disk)
            .await;
    }

    None
}

/// Large file cache: compress and write to disk, serve via mmap
/// No TTL required - invalidated by mtime check
async fn maybe_read_and_file_cache(
    path: &PathBuf,
    meta: &Metadata,
    policy: &CachePolicy,
    disk: &crate::config::DiskCacheConfig,
) -> Option<RouteResponse> {
    let comp_cfg = policy.compression.as_ref()?;
    let encoding = ContentEncoding::from_algorithm(comp_cfg.algorithm);

    // Read file
    let buf = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("Failed to read file for file cache: {}", e);
            return None;
        }
    };

    debug!(
        "💾 file caching: path = {} ({}MB)",
        path.display(),
        meta.len() / 1024 / 1024
    );

    // Compute hash for cache key
    // #[cfg(feature = "auth")]
    let file_hash = compute_file_hash(&buf);
    // #[cfg(not(feature = "auth"))]
    // let file_hash = format!("{:x}", meta.len()); // Fallback if blake3 not available

    let cache_file = file_cache_path(&file_hash, encoding, &disk.directory);

    // Compress and write to disk if not exists
    let encoded_len = if !cache_file.exists() {
        match compress_body(&buf, comp_cfg.level, encoding) {
            Ok(compressed) => {
                let compressed_len = compressed.len() as u64;
                debug!(
                    "🗜️  {} (file): {} -> {} bytes ({:.1}%)",
                    encoding.label(),
                    meta.len(),
                    compressed_len,
                    (compressed_len as f64 / meta.len() as f64) * 100.0
                );

                // Ensure cache directory exists
                if let Some(parent) = cache_file.parent()
                    && let Err(e) = std::fs::create_dir_all(parent)
                {
                    tracing::error!("Failed to create cache directory: {}", e);
                    return None;
                }

                // Write compressed file
                if let Err(e) = std::fs::write(&cache_file, compressed.as_ref())
                {
                    tracing::error!("Failed to write cache file: {}", e);
                    return None;
                }

                compressed_len
            }
            Err(e) => {
                tracing::warn!(
                    "{} compression failed for large file: {}",
                    encoding.label(),
                    e
                );
                return None;
            }
        }
    } else {
        // Cache file exists, get its size
        match std::fs::metadata(&cache_file) {
            Ok(m) => m.len(),
            Err(_) => return None,
        }
    };

    let ct = resolve_content_type(path.as_path());
    let modified = meta.modified().ok()?;
    let since = httpdate::fmt_http_date(modified);
    let etag = make_etag_from_meta(meta)
        .unwrap_or_else(|| format!("W/\"{:x}-0\"", meta.len()));

    // Create file cache entry (only metadata in RAM)
    // No expires_at - file cache never expires, only invalidated by mtime change
    let entry = Arc::new(FileCachedEntry {
        mtime: modified,
        len: meta.len(),
        content_type: ct.clone(),
        last_modified: since.clone(),
        expires_at: Instant::now() + disk.retention,
        etag: etag.clone(),
        encoded_file: cache_file.clone(),
        encoding,
        encoded_hash: file_hash,
        encoded_len,
    });

    FILE_CACHE.insert(path.clone(), entry);

    // Serve from file cache
    serve_from_file_cache(&cache_file, &ct, &since, &etag, encoding, policy)
        .await
}

/// Serve file cache synchronously (for fast_cache_hit path)
fn serve_file_cache_sync(
    cache_file: &PathBuf,
    content_type: &str,
    last_modified: &str,
    etag: &str,
    encoding: ContentEncoding,
    policy: &CachePolicy,
) -> RouteResponse {
    let file = match StdFile::open(cache_file) {
        Ok(file) => file,
        Err(e) => {
            tracing::error!("Failed to open cache file (sync): {}", e);
            return status_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let len = file.metadata().ok().map(|m| m.len());

    let stream =
        ReaderStream::new(TokioFile::from_std(file)).map_ok(Frame::data);
    build_file_cache_response(
        boxed_body(StreamBody::new(stream)),
        FileCacheResponseHeaders {
            content_type,
            last_modified,
            etag,
            encoding,
            policy,
            cache_status: "FILE-HIT+",
            content_length: len,
        },
    )
}

/// Serve compressed file from disk cache
async fn serve_from_file_cache(
    cache_file: &PathBuf,
    content_type: &str,
    last_modified: &str,
    etag: &str,
    encoding: ContentEncoding,
    policy: &CachePolicy,
) -> Option<RouteResponse> {
    let file = match TokioFile::open(cache_file).await {
        Ok(file) => file,
        Err(e) => {
            tracing::error!("Failed to open cache file: {}", e);
            return None;
        }
    };
    let len = file.metadata().await.ok().map(|m| m.len());

    let stream = ReaderStream::new(file).map_ok(Frame::data);
    Some(build_file_cache_response(
        boxed_body(StreamBody::new(stream)),
        FileCacheResponseHeaders {
            content_type,
            last_modified,
            etag,
            encoding,
            policy,
            cache_status: "FILE-MISS",
            content_length: len,
        },
    ))
}

struct FileCacheResponseHeaders<'a> {
    content_type: &'a str,
    last_modified: &'a str,
    etag: &'a str,
    encoding: ContentEncoding,
    policy: &'a CachePolicy,
    cache_status: &'static str,
    content_length: Option<u64>,
}

fn build_file_cache_response(
    body: RouteBody,
    headers: FileCacheResponseHeaders<'_>,
) -> RouteResponse {
    let mut resp = build_ok_response(
        headers.policy,
        headers.content_type,
        headers.last_modified,
        body,
    );

    resp.headers_mut()
        .insert("x-cache", HeaderValue::from_static(headers.cache_status));
    resp.headers_mut().insert(
        http::header::CONTENT_ENCODING,
        HeaderValue::from_static(headers.encoding.token()),
    );
    resp.headers_mut()
        .insert(ETAG, HeaderValue::from_str(headers.etag).unwrap());
    if let Some(len) = headers.content_length {
        resp.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&len.to_string()).unwrap(),
        );
    }

    resp
}

#[inline]
pub fn build_cached_partial_response(
    c: &CachedEntry,
    start: u64,
    end: u64,
) -> RouteResponse {
    let start_usize = start as usize;
    let end_excl = (end + 1) as usize; // Bytes::slice is [start..end)
    let part = c.body.slice(start_usize..end_excl);
    let mut resp = http::Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .body(bytes_body(part))
        .unwrap();
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_str(&c.content_type).unwrap());
    resp.headers_mut().insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    resp.headers_mut().insert(
        CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, c.len))
            .unwrap(),
    );
    resp.headers_mut().insert(ETAG, HeaderValue::from_str(&c.etag).unwrap());
    resp.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&c.last_modified).unwrap(),
    );
    resp.headers_mut().insert("x-cache", HeaderValue::from_static("HIT"));
    resp
}

pub(super) fn compress_body(
    data: &[u8],
    quality: u8,
    encoding: ContentEncoding,
) -> Result<Bytes, std::io::Error> {
    match encoding {
        ContentEncoding::Brotli => compress_brotli(data, quality),
        ContentEncoding::Zstd => {
            let compressed =
                zstd::stream::encode_all(data, i32::from(quality.min(22)))?;
            Ok(Bytes::from(compressed))
        }
    }
}

fn compress_brotli(
    data: &[u8],
    quality: u8,
) -> Result<Bytes, std::io::Error> {
    let mut compressed = Vec::new();
    let mut encoder = brotli::CompressorWriter::new(
        &mut compressed,
        4096,                   // buffer size
        quality.min(11) as u32, // quality (0-11)
        22,                     // lgwin (window size, 22 is default)
    );
    encoder.write_all(data)?;
    encoder.flush()?;
    drop(encoder); // Ensure all data is flushed
    Ok(Bytes::from(compressed))
}

#[cfg(test)]
#[path = "../../tests/caching/service.rs"]
mod tests;
