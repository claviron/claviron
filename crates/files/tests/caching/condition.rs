use std::time::{Duration, UNIX_EPOCH};

use http::header::RANGE;

use super::{
    ConditionalRequest, ContentEncoding, IfRange, PreconditionOutcome,
    Preconditions, accepts_encoding_token,
};

#[test]
fn if_none_match_accepts_lists_wildcard_and_weak_tags() {
    let pre = Preconditions {
        if_none_match: Some("\"old\", W/\"abc\", \"other\"".to_string()),
        ..Preconditions::default()
    };

    assert_eq!(
        pre.evaluate_get_or_head(Some("\"abc\""), None),
        PreconditionOutcome::NotModified
    );
    assert_eq!(
        pre.evaluate_get_or_head(Some("W/\"abc\""), None),
        PreconditionOutcome::NotModified
    );
    assert_eq!(
        pre.evaluate_get_or_head(Some("\"missing\""), None),
        PreconditionOutcome::Proceed
    );

    let wildcard = Preconditions {
        if_none_match: Some("*".to_string()),
        ..Preconditions::default()
    };
    assert_eq!(
        wildcard.evaluate_get_or_head(Some("\"anything\""), None),
        PreconditionOutcome::NotModified
    );
}

#[test]
fn if_match_uses_strong_comparison_and_takes_precedence() {
    let pre = Preconditions {
        if_match: Some("W/\"current\"".to_string()),
        if_none_match: Some("\"different\"".to_string()),
        ..Preconditions::default()
    };

    assert_eq!(
        pre.evaluate_get_or_head(Some("\"current\""), None),
        PreconditionOutcome::Failed
    );

    let matching = Preconditions {
        if_match: Some("\"old\", \"current\"".to_string()),
        if_unmodified_since: Some(httpdate::fmt_http_date(UNIX_EPOCH)),
        ..Preconditions::default()
    };
    assert_eq!(
        matching.evaluate_get_or_head(
            Some("\"current\""),
            Some(UNIX_EPOCH + Duration::from_secs(1_000))
        ),
        PreconditionOutcome::Proceed
    );

    let wildcard = Preconditions {
        if_match: Some("*".to_string()),
        ..Preconditions::default()
    };
    assert_eq!(
        wildcard.evaluate_get_or_head(Some("\"current\""), None),
        PreconditionOutcome::Proceed
    );
}

#[test]
fn date_preconditions_use_http_second_precision_and_rfc_precedence() {
    let modified =
        UNIX_EPOCH + Duration::from_secs(1_000) + Duration::from_millis(900);
    let same_second =
        httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(1_000));
    let older = httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(999));

    let modified_since = Preconditions {
        if_modified_since: Some(same_second.clone()),
        ..Preconditions::default()
    };
    assert_eq!(
        modified_since.evaluate_get_or_head(None, Some(modified)),
        PreconditionOutcome::NotModified
    );

    let unmodified_since = Preconditions {
        if_unmodified_since: Some(older),
        ..Preconditions::default()
    };
    assert_eq!(
        unmodified_since.evaluate_get_or_head(None, Some(modified)),
        PreconditionOutcome::Failed
    );

    let etag_wins = Preconditions {
        if_none_match: Some("\"different\"".to_string()),
        if_modified_since: Some(same_second),
        ..Preconditions::default()
    };
    assert_eq!(
        etag_wins.evaluate_get_or_head(Some("\"current\""), Some(modified)),
        PreconditionOutcome::Proceed
    );
}

#[test]
fn invalid_http_dates_are_ignored() {
    let pre = Preconditions {
        if_modified_since: Some("not a date".to_string()),
        if_unmodified_since: Some("not a date".to_string()),
        ..Preconditions::default()
    };

    assert_eq!(
        pre.evaluate_get_or_head(Some("\"current\""), Some(UNIX_EPOCH)),
        PreconditionOutcome::Proceed
    );
}

#[test]
fn if_range_requires_a_strong_etag_or_matching_date() {
    let mut headers = http::HeaderMap::new();
    headers.insert(RANGE, "bytes=10-20".parse().unwrap());
    let mut request = ConditionalRequest::from_headers(&headers);

    request.pre.if_range = Some(IfRange::ETag("\"current\"".to_string()));
    assert!(request.honors_range(100, Some("\"current\""), None));

    request.pre.if_range = Some(IfRange::ETag("W/\"current\"".to_string()));
    assert!(!request.honors_range(100, Some("\"current\""), None));

    request.pre.if_range =
        Some(IfRange::Date("Thu, 01 Jan 1970 00:16:40 GMT".to_string()));
    assert!(request.honors_range(
        100,
        None,
        Some("Thu, 01 Jan 1970 00:16:40 GMT")
    ));
    assert!(!request.honors_range(
        100,
        None,
        Some("Thu, 01 Jan 1970 00:16:41 GMT")
    ));
}

#[test]
fn accept_encoding_matches_supported_tokens_and_respects_zero_q() {
    assert!(accepts_encoding_token("gzip, br, zstd", "br"));
    assert!(accepts_encoding_token("gzip, br;q=1, zstd;q=0.7", "zstd"));
    assert!(!accepts_encoding_token("gzip, br;q=0, zstd", "br"));
    assert!(accepts_encoding_token("gzip, *;q=0.5", "zstd"));
    assert!(!accepts_encoding_token("gzip, zstd;q=0, *;q=0.5", "zstd"));

    let req = ConditionalRequest {
        accept_br: true,
        accept_zstd: false,
        ..ConditionalRequest::default()
    };
    assert!(req.accepts_encoding(ContentEncoding::Brotli));
    assert!(!req.accepts_encoding(ContentEncoding::Zstd));
}

#[test]
fn regression_repeated_list_fields_match_the_combined_form() {
    use http::header::{ACCEPT_ENCODING, IF_MATCH, IF_NONE_MATCH, IF_RANGE};
    for (name, expected) in [
        (IF_NONE_MATCH, PreconditionOutcome::NotModified),
        (IF_MATCH, PreconditionOutcome::Proceed),
    ] {
        let mut repeated = http::HeaderMap::new();
        repeated.append(&name, "\"old\"".parse().unwrap());
        repeated.append(&name, "\"current\"".parse().unwrap());
        let mut combined = http::HeaderMap::new();
        combined.insert(name, "\"old\",\"current\"".parse().unwrap());
        for headers in [repeated, combined] {
            assert_eq!(
                ConditionalRequest::from_headers(&headers)
                    .pre
                    .evaluate_get_or_head(Some("\"current\""), None),
                expected
            );
        }
    }
    let mut headers = http::HeaderMap::new();
    headers.append(ACCEPT_ENCODING, "*;q=1".parse().unwrap());
    headers.append(ACCEPT_ENCODING, "br;q=0, zstd".parse().unwrap());
    headers.insert(IF_RANGE, "Thu, 01 Jan 1970 00:16:40 GMT".parse().unwrap());
    let condition = ConditionalRequest::from_headers(&headers);
    assert!(!condition.accepts_encoding(ContentEncoding::Brotli));
    assert!(condition.accepts_encoding(ContentEncoding::Zstd));
    assert!(matches!(condition.pre.if_range, Some(IfRange::Date(_))));
}

#[test]
fn regression_empty_suffix_has_no_inclusive_byte_interval() {
    use super::ByteRange;
    assert_eq!(ByteRange::Suffix(1).bounds(0), None);
    assert_eq!(ByteRange::Suffix(0).bounds(10), None);
    assert_eq!(ByteRange::Suffix(20).bounds(10), Some((0, 9)));
    assert_eq!(ByteRange::Suffix(2).bounds(10), Some((8, 9)));
}
