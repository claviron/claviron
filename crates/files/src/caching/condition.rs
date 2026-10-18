use std::time::{SystemTime, UNIX_EPOCH};

use http::header::IF_NONE_MATCH;

use super::ContentEncoding;

#[derive(Clone, Debug, Default)]
pub struct ConditionalRequest {
    pub pre: Preconditions,
    pub range: Option<ByteRange>, // parsed from Range
    accept_br: bool,
    accept_zstd: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Preconditions {
    pub if_none_match: Option<String>,
    pub if_match: Option<String>,
    pub if_modified_since: Option<String>, // keep as raw HTTP-date for now
    pub if_unmodified_since: Option<String>,
    pub if_range: Option<IfRange>, // ETag or HTTP-date
}

impl Preconditions {
    pub(crate) fn evaluate_get_or_head(
        &self,
        etag: Option<&str>,
        last_modified: Option<SystemTime>,
    ) -> PreconditionOutcome {
        if let Some(tag_list) = self.if_match.as_deref()
            && !etag_list_matches(tag_list, etag, ETagComparison::Strong)
        {
            return PreconditionOutcome::Failed;
        }

        if self.if_match.is_none()
            && let (Some(value), Some(modified)) =
                (self.if_unmodified_since.as_deref(), last_modified)
            && let Ok(since) = httpdate::parse_http_date(value)
            && system_time_seconds(modified) > system_time_seconds(since)
        {
            return PreconditionOutcome::Failed;
        }

        if let Some(tag_list) = self.if_none_match.as_deref() {
            return if etag_list_matches(tag_list, etag, ETagComparison::Weak) {
                PreconditionOutcome::NotModified
            } else {
                PreconditionOutcome::Proceed
            };
        }

        if let (Some(value), Some(modified)) =
            (self.if_modified_since.as_deref(), last_modified)
            && let Ok(since) = httpdate::parse_http_date(value)
            && system_time_seconds(modified) <= system_time_seconds(since)
        {
            return PreconditionOutcome::NotModified;
        }

        PreconditionOutcome::Proceed
    }

    fn if_range_matches(
        &self,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> bool {
        match self.if_range.as_ref() {
            None => true,
            Some(IfRange::ETag(candidate)) => {
                etag.is_some_and(|etag| strong_compare_etags(candidate, etag))
            }
            Some(IfRange::Date(candidate)) => {
                let Some(last_modified) = last_modified else {
                    return false;
                };
                httpdate::parse_http_date(candidate)
                    .ok()
                    .zip(httpdate::parse_http_date(last_modified).ok())
                    .is_some_and(|(candidate, last_modified)| {
                        candidate == last_modified
                    })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreconditionOutcome {
    Proceed,
    NotModified,
    Failed,
}

#[derive(Clone, Debug)]
pub enum IfRange {
    ETag(String),
    Date(String),
}

#[derive(Clone, Debug)]
pub enum ByteRange {
    From(u64),
    Suffix(u64),
    Between(u64, u64),
}

impl ByteRange {
    /// Convert this parsed range into concrete (start, end) byte bounds
    /// within a file of size `total`. Returns None if invalid for `total`.
    pub(crate) fn bounds(
        &self,
        total: u64,
    ) -> Option<(u64, u64)> {
        match *self {
            ByteRange::From(start) => {
                if start >= total {
                    None
                } else {
                    Some((start, total - 1))
                }
            }
            ByteRange::Suffix(n) => {
                if n == 0 {
                    return None;
                }
                if n >= total {
                    Some((0, total.saturating_sub(1)))
                } else {
                    Some((total - n, total - 1))
                }
            }
            ByteRange::Between(a, b) => {
                if a > b || a >= total {
                    return None;
                }
                let end = b.min(total - 1);
                Some((a, end))
            }
        }
    }
}

impl ConditionalRequest {
    pub fn from_headers(h: &http::HeaderMap) -> Self {
        let pre = Preconditions {
            if_none_match: req_if_none_match(h).map(|s| s.to_string()),
            if_match: req_if_match(h),
            if_modified_since: req_if_modified_since(h),
            if_unmodified_since: req_if_unmodified_since(h),
            if_range: req_if_range(h),
        };
        let range = req_range(h);
        let accept_br = req_accept_encoding(h, ContentEncoding::Brotli);
        let accept_zstd = req_accept_encoding(h, ContentEncoding::Zstd);
        Self { pre, range, accept_br, accept_zstd }
    }

    pub fn range_bounds(
        &self,
        total: u64,
    ) -> Option<(u64, u64)> {
        self.range.as_ref().and_then(|r| r.bounds(total))
    }

    pub(crate) fn honors_range(
        &self,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> bool {
        self.range.is_some() && self.pre.if_range_matches(etag, last_modified)
    }

    pub(crate) fn accepts_encoding(
        &self,
        encoding: ContentEncoding,
    ) -> bool {
        match encoding {
            ContentEncoding::Brotli => self.accept_br,
            ContentEncoding::Zstd => self.accept_zstd,
        }
    }
}

#[inline]
fn req_if_none_match(headers: &http::HeaderMap) -> Option<&str> {
    headers.get(IF_NONE_MATCH).and_then(|h| h.to_str().ok())
}

// Helper to parse If-Match header
fn req_if_match(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get(http::header::IF_MATCH)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

// Helper to parse If-Modified-Since header
fn req_if_modified_since(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get(http::header::IF_MODIFIED_SINCE)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

// Helper to parse If-Unmodified-Since header
fn req_if_unmodified_since(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get(http::header::IF_UNMODIFIED_SINCE)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

// Helper to parse If-Range header
fn req_if_range(headers: &http::HeaderMap) -> Option<IfRange> {
    headers.get(http::header::IF_RANGE).and_then(|h| h.to_str().ok()).map(|s| {
        let s = s.trim();
        if (s.starts_with('"') && s.ends_with('"'))
            || (s.starts_with("W/\"") && s.ends_with('"'))
        {
            IfRange::ETag(s.to_string())
        } else {
            IfRange::Date(s.to_string())
        }
    })
}

// Helper to parse Range header (only basic bytes ranges)
fn req_range(headers: &http::HeaderMap) -> Option<ByteRange> {
    headers.get(http::header::RANGE).and_then(|h| h.to_str().ok()).and_then(
        |s| {
            // Only support bytes= ranges, e.g. "bytes=0-499", "bytes=500-", "bytes=-500"
            let s = s.trim();
            if let Some(rest) = s.strip_prefix("bytes=") {
                let parts: Vec<_> = rest.split(',').collect();
                if let Some(first) = parts.first() {
                    let r = first.trim();
                    if let Some(idx) = r.find('-') {
                        let (start, end) = r.split_at(idx);
                        let end = &end[1..]; // skip '-'
                        match (start.is_empty(), end.is_empty()) {
                            (false, false) => {
                                // bytes=from-to
                                if let (Ok(from), Ok(to)) =
                                    (start.parse::<u64>(), end.parse::<u64>())
                                    && from <= to
                                {
                                    return Some(ByteRange::Between(from, to));
                                }
                            }
                            (false, true) => {
                                // bytes=from-
                                if let Ok(from) = start.parse::<u64>() {
                                    return Some(ByteRange::From(from));
                                }
                            }
                            (true, false) => {
                                // bytes=-suffixlen
                                if let Ok(suffix) = end.parse::<u64>() {
                                    return Some(ByteRange::Suffix(suffix));
                                }
                            }
                            (true, true) => {}
                        }
                    }
                }
            }
            None
        },
    )
}

fn req_accept_encoding(
    headers: &http::HeaderMap,
    encoding: ContentEncoding,
) -> bool {
    headers
        .get(http::header::ACCEPT_ENCODING)
        .and_then(|h| h.to_str().ok())
        .map(|s| accepts_encoding_token(s, encoding.token()))
        .unwrap_or(false)
}

fn accepts_encoding_token(
    header: &str,
    token: &str,
) -> bool {
    let mut wildcard = false;
    for part in header.split(',') {
        let mut segments = part.split(';').map(str::trim);
        let Some(candidate) = segments.next() else {
            continue;
        };
        let accepted = segments.all(|segment| {
            let Some(q) = segment.strip_prefix("q=") else {
                return true;
            };
            q.parse::<f32>().map(|q| q > 0.0).unwrap_or(true)
        });
        if candidate.eq_ignore_ascii_case(token) {
            return accepted;
        }
        if candidate == "*" {
            wildcard = accepted;
        }
    }
    wildcard
}

fn weak_compare_etags(
    left: &str,
    right: &str,
) -> bool {
    etag_opaque_tag(left).is_some_and(|left_tag| {
        etag_opaque_tag(right).is_some_and(|right_tag| left_tag == right_tag)
    })
}

fn strong_compare_etags(
    left: &str,
    right: &str,
) -> bool {
    !left.starts_with("W/")
        && !right.starts_with("W/")
        && etag_opaque_tag(left).is_some_and(|left_tag| {
            etag_opaque_tag(right)
                .is_some_and(|right_tag| left_tag == right_tag)
        })
}

#[derive(Clone, Copy)]
enum ETagComparison {
    Strong,
    Weak,
}

fn etag_list_matches(
    tag_list: &str,
    etag: Option<&str>,
    comparison: ETagComparison,
) -> bool {
    tag_list.split(',').map(str::trim).any(|candidate| {
        if candidate == "*" {
            return true;
        }
        etag.is_some_and(|etag| match comparison {
            ETagComparison::Strong => strong_compare_etags(candidate, etag),
            ETagComparison::Weak => weak_compare_etags(candidate, etag),
        })
    })
}

fn system_time_seconds(value: SystemTime) -> i128 {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration.as_secs()),
        Err(error) => -i128::from(error.duration().as_secs()),
    }
}

fn etag_opaque_tag(value: &str) -> Option<&str> {
    let tag = value.strip_prefix("W/").unwrap_or(value);
    (tag.len() >= 2 && tag.starts_with('"') && tag.ends_with('"'))
        .then_some(tag)
}

#[cfg(test)]
#[path = "../../tests/caching/condition.rs"]
mod tests;
