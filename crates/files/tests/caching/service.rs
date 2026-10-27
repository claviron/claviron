use byte_unit::Byte;
use claviron_http::config::caching::{CompressionAlgorithm, CompressionConfig};
use http::header::ACCEPT_ENCODING;

use super::*;

fn compression_policy(algorithm: CompressionAlgorithm) -> CachePolicy {
    CachePolicy {
        client_ttl_secs: None,
        server_ttl_secs: None,
        validate_timestamps: true,
        immutable: false,
        compression: Some(CompressionConfig {
            algorithm,
            level: 5,
            min_size: Byte::from_u64(1),
        }),
    }
}

fn cond(accept_encoding: Option<&str>) -> ConditionalRequest {
    let mut headers = http::HeaderMap::new();
    if let Some(value) = accept_encoding {
        headers.insert(ACCEPT_ENCODING, value.parse().unwrap());
    }
    ConditionalRequest::from_headers(&headers)
}

#[test]
fn policy_encoding_requires_client_accept_encoding() {
    let zstd_policy = compression_policy(CompressionAlgorithm::Zstd);
    assert_eq!(
        accepted_policy_encoding(&zstd_policy, &cond(Some("gzip, zstd"))),
        Some(ContentEncoding::Zstd)
    );
    assert_eq!(
        accepted_policy_encoding(&zstd_policy, &cond(Some("gzip"))),
        None
    );
    assert_eq!(accepted_policy_encoding(&zstd_policy, &cond(None)), None);

    let brotli_policy = compression_policy(CompressionAlgorithm::Brotli);
    assert_eq!(
        accepted_policy_encoding(&brotli_policy, &cond(Some("gzip, *;q=0.5"))),
        Some(ContentEncoding::Brotli)
    );
    assert_eq!(
        accepted_policy_encoding(&brotli_policy, &cond(Some("gzip, br;q=0"))),
        None
    );
}

#[tokio::test]
async fn regression_empty_cached_suffix_matches_disk_semantics() {
    use http::header::{CONTENT_RANGE, RANGE};
    use http_body_util::BodyExt;
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    let meta = file.as_file().metadata().unwrap();
    let mut policy = CachePolicy::dynamic_server_cache();
    MEMORY_CACHE.insert(
        path.clone(),
        Arc::new(CachedEntry {
            mtime: meta.modified().unwrap(),
            len: 0,
            body: Arc::new(Bytes::new()),
            content_type: "application/octet-stream".into(),
            last_modified: httpdate::fmt_http_date(meta.modified().unwrap()),
            expires_at: Instant::now()
                + Duration::from_secs(policy.server_ttl_secs.unwrap()),
            etag: make_etag_from_meta(&meta).unwrap(),
            encoded_body: None,
            encoding: None,
            encoded_len: 0,
        }),
    );
    let mut headers = http::HeaderMap::new();
    headers.insert(RANGE, "bytes=-1".parse().unwrap());
    let condition = ConditionalRequest::from_headers(&headers);
    let validated =
        try_serve_from_cache(&path, &policy, &meta, &condition).unwrap();
    policy.validate_timestamps = false;
    let fast = fast_cache_hit(&path, &policy, &condition).unwrap();
    MEMORY_CACHE.remove(&path);
    for response in [validated, fast] {
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key(CONTENT_RANGE));
        assert!(
            response.into_body().collect().await.unwrap().to_bytes().is_empty()
        );
    }
}
