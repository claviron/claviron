#![no_main]

use claviron::http::forwarding::{
    RequestSmugglingError, is_hop_by_hop_name, reject_request_smuggling,
    strip_hop_by_hop_headers,
};
use http::{
    HeaderMap, HeaderName, HeaderValue, Version,
    header::{CONNECTION, CONTENT_LENGTH, TRANSFER_ENCODING},
};
use libfuzzer_sys::fuzz_target;

// Each line is name:value; repeated names remain separate field values.
fuzz_target!(|data: &[u8]| {
    let mut headers = HeaderMap::new();
    for line in data.split(|byte| *byte == b'\n') {
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(&line[..colon]),
            HeaderValue::from_bytes(&line[colon + 1..]),
        ) {
            headers.append(name, value);
        }
    }
    // HTTP/1 ingress has already validated framing; this guard must defer to it.
    for version in [Version::HTTP_10, Version::HTTP_11] {
        assert!(reject_request_smuggling(version, &headers).is_ok());
    }
    let h2 = reject_request_smuggling(Version::HTTP_2, &headers);
    let h3 = reject_request_smuggling(Version::HTTP_3, &headers);
    if headers.contains_key(CONTENT_LENGTH)
        && headers.contains_key(TRANSFER_ENCODING)
    {
        assert_eq!(
            h3,
            Err(RequestSmugglingError::ContentLengthWithTransferEncoding)
        );
    } else {
        assert_eq!(h2, h3);
    }

    let original = headers.clone();
    strip_hop_by_hop_headers(&mut headers);
    for name in original.keys() {
        let connection_named = original
            .get_all(CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case(name.as_str()));
        if is_hop_by_hop_name(name.as_str()) || connection_named {
            assert!(!headers.contains_key(name));
        } else {
            assert!(
                headers.get_all(name).iter().eq(original.get_all(name).iter())
            );
        }
    }
    let once = headers.clone();
    strip_hop_by_hop_headers(&mut headers);
    assert_eq!(headers, once);
});
