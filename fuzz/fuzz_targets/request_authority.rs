#![no_main]

use claviron::http::authority::{AuthorityError, RequestAuthority};
use http::{HeaderValue, Request, Uri, Version, header::HOST};
use libfuzzer_sys::fuzz_target;

// First line: request target. Remaining lines: separate Host field values.
fuzz_target!(|data: &[u8]| {
    let mut lines = data.split(|byte| *byte == b'\n');
    let Ok(uri) = Uri::try_from(lines.next().unwrap_or_default()) else {
        return;
    };
    let mut request = Request::new(());
    *request.uri_mut() = uri;
    for line in lines {
        let Ok(value) = HeaderValue::from_bytes(line) else { return };
        request.headers_mut().append(HOST, value);
    }
    let original_headers = request.headers().clone();
    let original_uri = request.uri().clone();

    for version in
        [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2, Version::HTTP_3]
    {
        *request.version_mut() = version;
        let result = RequestAuthority::from_request(&request);
        let cleartext = RequestAuthority::from_cleartext(&request);
        assert_eq!(request.headers(), &original_headers);
        assert_eq!(request.uri(), &original_uri);

        if request.headers().get_all(HOST).iter().count() > 1 {
            assert_eq!(
                result.as_ref().err(),
                Some(&AuthorityError::Conflicting)
            );
        }
        if version == Version::HTTP_11 && !request.headers().contains_key(HOST)
        {
            assert_eq!(result.as_ref().err(), Some(&AuthorityError::Missing));
        }
        if let Ok(None) = &result {
            assert_eq!(version, Version::HTTP_10);
        }
        if let Ok(Some(selected)) = &result {
            // Reuse the accepted wire authority alone: identity must be stable.
            let canonical = Request::builder()
                .header(HOST, selected.authority().as_str())
                .body(())
                .unwrap();
            let reparsed =
                RequestAuthority::from_request(&canonical).unwrap().unwrap();
            assert_eq!(selected.host(), reparsed.host());
        }
        if request.uri().scheme_str().is_some_and(|scheme| scheme != "http") {
            assert_eq!(cleartext.err(), Some(AuthorityError::WrongScheme));
        } else {
            assert_eq!(
                cleartext.as_ref().err().copied(),
                match &result {
                    Ok(Some(_)) => None,
                    Ok(None) => Some(AuthorityError::Missing),
                    Err(error) => Some(*error),
                }
            );
        }
    }
});
