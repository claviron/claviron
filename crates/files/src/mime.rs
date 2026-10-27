use std::fs::Metadata;
use std::path::Path;
use std::time::{Duration, SystemTime};

pub fn resolve_content_type(path: &Path) -> String {
    // 1) Fast map (by extension)
    if let Some(ext) = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        && let Some(hit) = fast_mime_by_ext(&ext)
    {
        let mut ct = hit.to_string();
        if needs_utf8_charset(&ct) && !ct.to_lowercase().contains("charset=") {
            ct.push_str("; charset=utf-8");
        }
        return ct;
    }

    // 2) mime_guess, 3) infer fallback
    let guessed = mime_guess::from_path(path).first_or_octet_stream();
    let mut ct_ref = guessed.as_ref();
    if ct_ref == "application/octet-stream" {
        // Only sniff content when there is no extension, to avoid extra syscall
        if path.extension().is_none()
            && let Ok(opt) = infer::get_from_path(path)
            && let Some(kind) = opt
        {
            ct_ref = kind.mime_type();
        }
    }
    let mut ct = ct_ref.to_string();
    if needs_utf8_charset(&ct) && !ct.to_lowercase().contains("charset=") {
        ct.push_str("; charset=utf-8");
    }
    ct
}

#[inline]
pub fn fast_mime_by_ext(ext: &str) -> Option<&'static str> {
    match ext {
        // Text & web documents
        "html" | "htm" => Some("text/html"),
        "css" => Some("text/css"),
        "js" | "mjs" => Some("application/javascript"),
        "json" => Some("application/json"),
        "map" => Some("application/json"), // source maps
        "txt" | "text" | "log" => Some("text/plain"),
        "xml" => Some("application/xml"),
        "csv" => Some("text/csv"),
        "svg" => Some("image/svg+xml"),

        // Images
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "avif" => Some("image/avif"),
        "bmp" => Some("image/bmp"),
        "ico" => Some("image/x-icon"),

        // Fonts
        "woff" => Some("font/woff"),
        "woff2" => Some("font/woff2"),
        "ttf" => Some("font/ttf"),
        "otf" => Some("font/otf"),

        // Media
        "mp4" => Some("video/mp4"),
        "m4v" => Some("video/x-m4v"),
        "webm" => Some("video/webm"),
        "ogv" => Some("video/ogg"),
        "mp3" => Some("audio/mpeg"),
        "ogg" => Some("audio/ogg"),
        "wav" => Some("audio/wav"),

        // Other common
        "pdf" => Some("application/pdf"),
        "wasm" => Some("application/wasm"),
        "zip" => Some("application/zip"),
        "tar" => Some("application/x-tar"),
        "gz" => Some("application/gzip"),
        _ => None,
    }
}

/// Magic-byte sniff for the four raster image formats commonly served
/// from DB-backed CDNs (png/jpeg/gif/webp). Returns `None` for unknown
/// signatures so callers can fall back to the URL extension — fine for
/// SVG / custom payloads / anything sniffing-resistant.
///
/// Intentionally narrow: we don't pull `infer` for this because the
/// hot path is "looked up by hash, sniff just to confirm" — adding 50+
/// signatures would be wasted work. If you need broader detection
/// (file uploads, untrusted content), use `infer::get` directly.
#[inline]
pub fn sniff_image_mime(bytes: &[u8]) -> Option<&'static str> {
    match bytes {
        [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, ..] => {
            Some("image/png")
        }
        [0xff, 0xd8, 0xff, ..] => Some("image/jpeg"),
        [b'G', b'I', b'F', b'8', b'7' | b'9', b'a', ..] => Some("image/gif"),
        [b'R', b'I', b'F', b'F', _, _, _, _, b'W', b'E', b'B', b'P', ..] => {
            Some("image/webp")
        }
        _ => None,
    }
}

#[inline]
fn needs_utf8_charset(ct: &str) -> bool {
    ct.starts_with("text/")
        || ct == "application/javascript"
        || ct == "application/json"
        || ct == "application/xml"
}

#[inline]
pub(crate) fn make_etag_from_meta(meta: &Metadata) -> Option<String> {
    meta.modified()
        .ok()
        .and_then(|m| m.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|modified| make_etag(meta.len(), modified))
}

/// Metadata is not a content digest. Preserve timestamp precision without
/// advertising byte-for-byte strength or reading/hashing the file per request.
pub(crate) fn make_etag(
    len: u64,
    modified: Duration,
) -> String {
    format!("W/\"{:x}-{:x}\"", len, modified.as_nanos())
}
