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
