//! The fleet tier of the static cache: bodies in the Orbit arena, one copy
//! per fleet, read in place by every worker.
//!
//! Present when the crate is built with `orbit` and the runtime registered an
//! `ArenaProvider`; until then every call here says so and the per-process
//! tiers carry on. Identity and encoded bodies go in together under a digest
//! of the real path, with the file's modification time and length beside
//! them, so a validating policy can compare against the file without reading
//! it and every other field of the response is derived, never stored.

use std::fs::Metadata;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use claviron_http::{RouteResponse, bytes_body, empty_body};
use claviron_orbit::arena::{ARENA_BYTES, Arena, Key, Record};
use http::header::{
    ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_RANGE, CONTENT_TYPE, ETAG,
    LAST_MODIFIED,
};
use http::{HeaderValue, Response, StatusCode};

use super::ContentEncoding;
use super::condition::{ConditionalRequest, PreconditionOutcome};
use super::policy::CachePolicy;
use super::service::compress_body;
use crate::mime::resolve_content_type;
use crate::response::{
    build_ok_response, cached_not_modified_response, status_response,
};

static ARENA: OnceLock<Arc<Arena>> = OnceLock::new();

/// Installed once at boot by the file cache provider.
pub(crate) fn install(arena: Arc<Arena>) {
    let _ = ARENA.set(arena);
}

pub(crate) fn active() -> Option<&'static Arc<Arena>> {
    ARENA.get()
}

/// Said once at boot by the process that owns the fleet: what the ring
/// holds for the nodes it will serve. The build ties the threshold to the
/// ring; only boot knows how many nodes will pin at once, and the worker
/// count is the number that changes between runs while the capacity does not.
pub(crate) fn report_budget(worker_count: u8) {
    let nodes = u64::from(worker_count).max(1);
    let ring = ARENA_BYTES as u64;
    // Body and its encoded twin: the most one response can pin.
    let record = 2 * crate::config::active().memory.max_body_size;
    let per_node = ring / nodes;
    let records_per_node = per_node / record;
    if records_per_node < 4 {
        tracing::debug!(
            "fleet arena: {} MiB across {nodes} node(s) is {} MiB each, room for {records_per_node} \
             largest record(s) in flight per node; raise ORBIT_ARENA_BYTES or lower \
             CLAVIRON_FILES_MAX_CACHE_BODY_BYTES",
            ring >> 20,
            per_node >> 20
        );
    } else {
        tracing::debug!(
            "🗂️  fleet arena: {} MiB across {nodes} node(s), up to {records_per_node} largest records \
             in flight per node before a put finds no room",
            ring >> 20
        );
    }
}

/// Empties the fleet tier; how many records it held.
pub(crate) fn reset() -> usize {
    match active().map(|arena| arena.reset()) {
        Some(Ok(count)) => count,
        Some(Err(error)) => {
            tracing::warn!("fleet file cache reset failed: {error}");
            0
        }
        None => 0,
    }
}

fn key(path: &Path) -> Key {
    let digest = blake3::hash(path.to_string_lossy().as_bytes());
    let mut first = [0_u8; 16];
    first.copy_from_slice(&digest.as_bytes()[..16]);
    Key::from_bytes(first)
}

fn stamp(modified: SystemTime) -> u64 {
    modified
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}

/// Same shape as `mime::make_etag_from_meta`, from what the arena kept.
fn etag(
    len: u64,
    stamp: u64,
) -> String {
    format!("\"{:x}-{:x}\"", len, stamp / 1_000_000_000)
}

/// A hit from the fleet tier. With `meta`, the record is served only if it
/// still describes the file on disk; without, it is served as filed.
pub(crate) fn hit(
    path: &Path,
    policy: &CachePolicy,
    cond: &ConditionalRequest,
    meta: Option<&Metadata>,
) -> Option<RouteResponse> {
    let arena = active()?;
    let entry = arena.get(key(path))?;
    if let Some(meta) = meta
        && (meta.modified().ok().map(stamp) != Some(entry.stamp())
            || meta.len() != entry.extra())
    {
        return None;
    }

    let len = entry.extra();
    let modified = UNIX_EPOCH + Duration::from_nanos(entry.stamp());
    let etag = etag(len, entry.stamp());
    let last_modified = httpdate::fmt_http_date(modified);
    match cond.pre.evaluate_get_or_head(Some(&etag), Some(modified)) {
        PreconditionOutcome::NotModified => {
            return Some(cached_not_modified_response(&etag, &last_modified));
        }
        PreconditionOutcome::Failed => {
            return Some(status_response(StatusCode::PRECONDITION_FAILED));
        }
        PreconditionOutcome::Proceed => {}
    }

    let content_type = resolve_content_type(path);
    if cond.honors_range(Some(&etag), Some(&last_modified)) {
        return Some(match cond.range_bounds(len) {
            Some((start, end)) => {
                let part = Bytes::from_owner(entry.into_body())
                    .slice(start as usize..(end + 1) as usize);
                let mut resp = Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .body(bytes_body(part))
                    .unwrap();
                let headers = resp.headers_mut();
                headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_str(&content_type).unwrap(),
                );
                headers
                    .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                headers.insert(
                    CONTENT_RANGE,
                    HeaderValue::from_str(&format!(
                        "bytes {start}-{end}/{len}"
                    ))
                    .unwrap(),
                );
                headers.insert(ETAG, HeaderValue::from_str(&etag).unwrap());
                headers.insert(
                    LAST_MODIFIED,
                    HeaderValue::from_str(&last_modified).unwrap(),
                );
                headers.insert("x-cache", HeaderValue::from_static("HIT"));
                resp
            }
            None => {
                let mut resp = Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .body(empty_body())
                    .unwrap();
                let headers = resp.headers_mut();
                headers.insert(
                    CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes */{len}")).unwrap(),
                );
                headers
                    .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                headers.insert(ETAG, HeaderValue::from_str(&etag).unwrap());
                resp
            }
        });
    }

    let encoding = entry
        .encoded()
        .and_then(|(_, tag)| ContentEncoding::from_tag(tag))
        .filter(|encoding| cond.accepts_encoding(*encoding));
    let (body, encoding) = match encoding {
        Some(encoding) => match entry.into_encoded() {
            Ok(view) => (Bytes::from_owner(view), Some(encoding)),
            Err(entry) => (Bytes::from_owner(entry.into_body()), None),
        },
        None => (Bytes::from_owner(entry.into_body()), None),
    };

    let mut resp = build_ok_response(
        policy,
        &content_type,
        &last_modified,
        bytes_body(body),
    );
    resp.headers_mut().insert(
        "x-cache",
        HeaderValue::from_static(if meta.is_some() { "HIT" } else { "HIT+" }),
    );
    if let Some(encoding) = encoding {
        resp.headers_mut().insert(
            CONTENT_ENCODING,
            HeaderValue::from_static(encoding.token()),
        );
    }
    resp.headers_mut().insert(ETAG, HeaderValue::from_str(&etag).unwrap());
    Some(resp)
}

/// Read the file, encode it as policy says, file both in the arena, and
/// answer this request from what was read. A put the arena refuses is
/// logged and the response still goes out; the next request tries again.
pub(crate) async fn read_and_store(
    path: &Path,
    meta: &Metadata,
    policy: &CachePolicy,
) -> Option<RouteResponse> {
    let arena = active()?;
    let modified = meta.modified().ok()?;
    let buf = tokio::fs::read(path).await.ok()?;

    let encoded = policy
        .compression
        .as_ref()
        .filter(|compression| meta.len() >= compression.min_size.as_u64())
        .and_then(|compression| {
            let encoding =
                ContentEncoding::from_algorithm(compression.algorithm);
            match compress_body(&buf, compression.level, encoding) {
                Ok(bytes) => {
                    tracing::debug!(
                        "🗜️  {}: {} -> {} bytes ({:.1}%)",
                        encoding.label(),
                        meta.len(),
                        bytes.len(),
                        (bytes.len() as f64 / meta.len() as f64) * 100.0
                    );
                    Some((bytes, encoding))
                }
                Err(error) => {
                    tracing::warn!(
                        "{} compression failed: {error}",
                        encoding.label()
                    );
                    None
                }
            }
        });

    let record = Record::body(&buf).stamp(stamp(modified)).extra(meta.len());
    let record = match &encoded {
        Some((bytes, encoding)) => record.encoded(bytes, encoding.tag()),
        None => record,
    };
    match arena.put(key(path), record) {
        Ok(()) => {
            tracing::debug!("⚡️ fleet caching: path = {}", path.display())
        }
        Err(error) => {
            tracing::debug!("fleet cache skipped {}: {error}", path.display())
        }
    }

    let content_type = resolve_content_type(path);
    let last_modified = httpdate::fmt_http_date(modified);
    let etag = etag(meta.len(), stamp(modified));
    let mut resp = build_ok_response(
        policy,
        &content_type,
        &last_modified,
        bytes_body(Bytes::from(buf)),
    );
    resp.headers_mut().insert("x-cache", HeaderValue::from_static("MISS"));
    resp.headers_mut().insert(ETAG, HeaderValue::from_str(&etag).unwrap());
    Some(resp)
}
